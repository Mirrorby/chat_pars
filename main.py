"""
TG Parser — GitHub Actions парсер.
Запускается по крону, делает один обход всех каналов, завершается.

Состояние (last_id каждого канала) хранится в листе «Состояние» каждой таблицы.
Дедупликация — по листу «Посты» за последние 3 дня (в RAM на время запуска).

Переменные окружения (GitHub Secrets):
  TG_API_ID                — Telegram API id
  TG_API_HASH              — Telegram API hash
  TG_SESSION               — Telethon StringSession
  GOOGLE_CREDENTIALS_BASE64 — сервисный аккаунт Google в base64
  SPREADSHEET_IDS          — ID таблиц через запятую: id1,id2,id3
  MESSAGES_LIMIT           — сколько последних сообщений читать за раз (default: 10)

Структура каждой таблицы:
  Лист «Настройки»:
    B2   — токен TG-бота
    A4+  — chat_id куда слать посты
    E1   — чекбокс TRUE/FALSE (включить фильтр ключей)
    D2+  — ключевые слова
    G1   — чекбокс TRUE/FALSE (включить фильтр негативов)
    F2+  — негативные слова
  Лист «Каналы»:    A2+ — username/ссылка/числовой ID канала
  Лист «Посты»:     A=дата B=канал C=автор D=ссылка_автора E=ссылка F=текст
  Лист «Логи»:      A=дата B=уровень C=сообщение
  Лист «Кеш»:       A=username B=entity_id C=chat_name
  Лист «Состояние»: A=username B=last_id  (создаётся автоматически)
"""

import asyncio
import base64
import io
import json
import logging
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.sessions import StringSession

# ── Конфиг ────────────────────────────────────────────────────────────────────

API_ID                 = int(os.environ.get('TG_API_ID', '0'))
API_HASH               = os.environ.get('TG_API_HASH', '')
SESSION_STRING         = os.environ.get('TG_SESSION', '')
GOOGLE_CREDENTIALS_B64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
MESSAGES_LIMIT         = int(os.environ.get('MESSAGES_LIMIT', '10'))
SPREADSHEET_IDS        = [
    s.strip() for s in os.environ.get('SPREADSHEET_IDS', '').split(',') if s.strip()
]

# Пауза между get_messages запросами — защита от FloodWait
REQUEST_DELAY = 1.5  # секунд

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=4)

# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets
# ══════════════════════════════════════════════════════════════════════════════

def _open_spreadsheet(spreadsheet_id: str):
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_B64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)


def _read_settings(ss, label: str) -> dict | None:
    try:
        data = ss.worksheet('Настройки').get_all_values()

        token = ''
        if len(data) > 1 and len(data[1]) > 1:
            token = str(data[1][1]).strip()

        dest_chats = [
            str(row[0]).strip() for row in data[3:]
            if row and str(row[0]).strip()
        ]

        keywords_enabled = (
            len(data) > 0 and len(data[0]) > 4
            and str(data[0][4]).upper() == 'TRUE'
        )
        keywords = [
            str(row[3]).strip() for row in data[1:]
            if len(row) > 3 and str(row[3]).strip()
        ]

        negatives_enabled = (
            len(data) > 0 and len(data[0]) > 6
            and str(data[0][6]).upper() == 'TRUE'
        )
        negatives = [
            str(row[5]).strip() for row in data[1:]
            if len(row) > 5 and str(row[5]).strip()
        ]

        log.info(
            f'[{label}] Настройки | токен: {"есть" if token else "НЕТ"} | '
            f'чаты: {len(dest_chats)} | '
            f'ключи: {len(keywords)} ({"ВКЛ" if keywords_enabled else "ВЫКЛ"}) | '
            f'негативы: {len(negatives)} ({"ВКЛ" if negatives_enabled else "ВЫКЛ"})'
        )
        return {
            'tg_token':          token,
            'dest_chats':        dest_chats,
            'keywords_enabled':  keywords_enabled,
            'keywords':          keywords,
            'negatives_enabled': negatives_enabled,
            'negatives':         negatives,
        }
    except Exception as e:
        log.error(f'[{label}] Ошибка чтения настроек: {e}')
        return None


def _read_channels_raw(ss, label: str) -> list[str]:
    try:
        data = ss.worksheet('Каналы').get_all_values()
        return [row[0].strip() for row in data[1:] if row and row[0].strip()]
    except Exception as e:
        log.error(f'[{label}] Ошибка чтения каналов: {e}')
        return []


def _read_entity_cache(ss, label: str) -> dict:
    """username -> {entity_id, chat_name}"""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet(title='Кеш', rows=1000, cols=3)
            ws.append_row(['username', 'entity_id', 'chat_name'])
            return {}

        cache = {}
        for row in ws.get_all_values()[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                try:
                    cache[row[0].strip()] = {
                        'entity_id': int(row[1].strip()),
                        'chat_name': row[2].strip() if len(row) > 2 else row[0].strip(),
                    }
                except ValueError:
                    pass
        log.info(f'[{label}] Кеш загружен: {len(cache)} записей')
        return cache
    except Exception as e:
        log.error(f'[{label}] Ошибка чтения кеша: {e}')
        return {}


def _write_entity_cache(ss, username: str, entity_id: int, chat_name: str):
    try:
        ss.worksheet('Кеш').append_row(
            [username, str(entity_id), chat_name],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error(f'Ошибка записи кеша: {e}')


def _read_state(ss, label: str) -> dict:
    """Читает лист Состояние. Возвращает username -> last_id (int)."""
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet(title='Состояние', rows=1000, cols=2)
            ws.append_row(['username', 'last_id'])
            return {}

        state = {}
        for row in ws.get_all_values()[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                try:
                    state[row[0].strip()] = int(row[1].strip())
                except ValueError:
                    pass
        log.info(f'[{label}] Состояние загружено: {len(state)} каналов')
        return state
    except Exception as e:
        log.error(f'[{label}] Ошибка чтения состояния: {e}')
        return {}


def _write_state(ss, label: str, state: dict):
    """Перезаписывает лист Состояние целиком. state: username -> last_id"""
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet(title='Состояние', rows=1000, cols=2)

        rows = [['username', 'last_id']]
        rows += [[u, str(lid)] for u, lid in state.items()]
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'[{label}] Состояние сохранено: {len(state)} каналов')
    except Exception as e:
        log.error(f'[{label}] Ошибка записи состояния: {e}')


def _read_recent_posts(ss, label: str) -> set:
    """Нормализованные тексты постов за последние 3 дня для дедупликации."""
    try:
        rows = ss.worksheet('Посты').get_all_values()
        cutoff = datetime.now() - timedelta(days=3)
        texts = set()
        for row in rows[1:]:
            if len(row) < 6:
                continue
            try:
                dt = datetime.strptime(row[0].strip(), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
            if dt >= cutoff and row[5].strip():
                texts.add(_normalize_text(row[5].strip()))
        log.info(f'[{label}] Загружено {len(texts)} текстов за 3 дня для дедупликации')
        return texts
    except Exception as e:
        log.error(f'[{label}] Ошибка чтения постов для дедупликации: {e}')
        return set()


def _write_post(ss, post: dict):
    try:
        ss.worksheet('Посты').append_row([
            post['date'].strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['author_name'],
            post['author_link'],
            post['link'],
            post['text'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error(f'Ошибка записи поста: {e}')


def _write_log(ss, label: str, level: str, message: str):
    try:
        safe = str(message)
        if safe and safe[0] in '=+-@':
            safe = "'" + safe
        ss.worksheet('Логи').append_row(
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, safe],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error(f'[{label}] Ошибка записи лога: {e}')

# ══════════════════════════════════════════════════════════════════════════════
# Утилиты
# ══════════════════════════════════════════════════════════════════════════════

def _extract_username(raw: str) -> str | None:
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^-100\d+$', raw):
        return raw
    if re.match(r'^-?\d+$', raw):
        return raw
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    return None


def _normalize_text(text: str) -> str:
    return ' '.join(text.lower().split())


def _should_send(text: str, settings: dict) -> bool:
    if settings['keywords_enabled']:
        if not text.strip():
            return False
        lower = text.lower()
        if not any(kw.lower().strip() in lower for kw in settings['keywords'] if kw.strip()):
            return False
    if settings['negatives_enabled'] and settings['negatives']:
        lower = text.lower()
        if any(neg.lower().strip() in lower for neg in settings['negatives'] if neg.strip()):
            return False
    return True


def _build_link(chat, msg_id: int) -> str:
    username = getattr(chat, 'username', None)
    if username:
        return f'https://t.me/{username}/{msg_id}'
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    return f'https://t.me/c/{chat_id}/{msg_id}'


def _get_author_info(msg):
    try:
        sender = msg.sender
        if not sender:
            return '', ''
        first = getattr(sender, 'first_name', '') or ''
        last  = getattr(sender, 'last_name',  '') or ''
        uname = getattr(sender, 'username',   '') or ''
        name  = (first + ' ' + last).strip()
        link  = f'https://t.me/{uname}' if uname else ''
        return name, link
    except Exception:
        return '', ''

# ══════════════════════════════════════════════════════════════════════════════
# Отправка через Bot API
# ══════════════════════════════════════════════════════════════════════════════

def _send_telegram_sync(body: str, tg_token: str, chats: list):
    for chat_id in chats:
        try:
            url  = f'https://api.telegram.org/bot{tg_token}/sendMessage'
            data = json.dumps({
                'chat_id': chat_id,
                'text':    body[:4096],
                'disable_web_page_preview': False,
            }).encode('utf-8')
            req = urllib.request.Request(
                url, data=data,
                headers={'Content-Type': 'application/json'},
            )
            urllib.request.urlopen(req, timeout=10)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'Ошибка отправки TG в {chat_id}: {e}')


def _send_single_photo_sync(caption: str, tg_token: str, chats: list, photo: bytes):
    for chat_id in chats:
        try:
            url      = f'https://api.telegram.org/bot{tg_token}/sendPhoto'
            boundary = '----TGBoundary' + str(int(time.time()))
            parts    = []
            parts.append(
                (f'--{boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n'
                 f'{chat_id}').encode()
            )
            if caption:
                parts.append(
                    (f'--{boundary}\r\nContent-Disposition: form-data; name="caption"\r\n\r\n'
                     f'{caption[:1024]}').encode()
                )
            parts.append(
                (f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; '
                 f'filename="photo.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode() + photo
            )
            parts.append(f'--{boundary}--'.encode())
            body = b'\r\n'.join(parts)
            req  = urllib.request.Request(
                url, data=body,
                headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
            )
            urllib.request.urlopen(req, timeout=30)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'Ошибка отправки фото в {chat_id}: {e}')
            _send_telegram_sync(caption, tg_token, [chat_id])


def _send_media_group_sync(caption: str, tg_token: str, chats: list, photos: list):
    if not photos:
        _send_telegram_sync(caption, tg_token, chats)
        return
    photos = photos[:10]
    for chat_id in chats:
        try:
            url        = f'https://api.telegram.org/bot{tg_token}/sendMediaGroup'
            media_list = []
            for i, _ in enumerate(photos):
                item = {'type': 'photo', 'media': f'attach://photo_{i}'}
                if i == 0 and caption:
                    item['caption'] = caption[:1024]
                media_list.append(item)

            boundary = '----TGBoundary' + str(int(time.time()))
            parts    = []
            parts.append(
                (f'--{boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n'
                 f'{chat_id}').encode()
            )
            parts.append(
                (f'--{boundary}\r\nContent-Disposition: form-data; name="media"\r\n'
                 f'Content-Type: application/json\r\n\r\n'
                 f'{json.dumps(media_list)}').encode()
            )
            for i, photo_bytes in enumerate(photos):
                parts.append(
                    (f'--{boundary}\r\nContent-Disposition: form-data; name="photo_{i}"; '
                     f'filename="photo_{i}.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode()
                    + photo_bytes
                )
            parts.append(f'--{boundary}--'.encode())
            body = b'\r\n'.join(parts)
            req  = urllib.request.Request(
                url, data=body,
                headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
            )
            urllib.request.urlopen(req, timeout=30)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'Ошибка отправки медиагруппы в {chat_id}: {e}')
            _send_telegram_sync(caption, tg_token, [chat_id])

# ══════════════════════════════════════════════════════════════════════════════
# Резолв каналов
# ══════════════════════════════════════════════════════════════════════════════

async def _resolve_channel(
    client: TelegramClient,
    username: str,
    ss,
    label: str,
    entity_cache: dict,
) -> tuple[int, str, object] | None:
    """Возвращает (abs_entity_id, chat_name, peer) или None."""
    loop = asyncio.get_event_loop()

    # Из кеша
    if username in entity_cache:
        ec   = entity_cache[username]
        peer = int(username) if re.match(r'^-100\d+$', username) else username
        return ec['entity_id'], ec['chat_name'], peer

    # Через Telegram API
    try:
        peer   = int(username) if re.match(r'^-100\d+$', username) else username
        entity = await client.get_entity(peer)
        eid    = abs(entity.id)
        name   = getattr(entity, 'title', None) or username
        entity_cache[username] = {'entity_id': eid, 'chat_name': name}
        await loop.run_in_executor(_executor, _write_entity_cache, ss, username, eid, name)
        log.info(f'[{label}] Резолв: {username} -> {eid} ({name})')
        await asyncio.sleep(2.0)
        return eid, name, peer
    except FloodWaitError as e:
        log.warning(f'[{label}] FloodWait при резолве {username}: жду {e.seconds}s')
        await asyncio.sleep(e.seconds + 2)
        return None
    except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
        log.warning(f'[{label}] Недоступен {username}: {e}')
        return None
    except Exception as e:
        log.error(f'[{label}] Ошибка резолва {username}: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════════════
# Обход одной таблицы
# ══════════════════════════════════════════════════════════════════════════════

async def _process_spreadsheet(client: TelegramClient, spreadsheet_id: str, index: int):
    label = f'SS{index + 1}'
    loop  = asyncio.get_event_loop()

    log.info(f'[{label}] Подключение к таблице {spreadsheet_id}...')
    try:
        ss = await loop.run_in_executor(_executor, _open_spreadsheet, spreadsheet_id)
    except Exception as e:
        log.error(f'[{label}] Не удалось открыть таблицу: {e}')
        return

    settings = await loop.run_in_executor(_executor, _read_settings, ss, label)
    if not settings:
        log.error(f'[{label}] Не удалось прочитать настройки — пропускаю')
        return

    raw_channels  = await loop.run_in_executor(_executor, _read_channels_raw, ss, label)
    entity_cache  = await loop.run_in_executor(_executor, _read_entity_cache, ss, label)
    channel_state = await loop.run_in_executor(_executor, _read_state, ss, label)
    recent_texts  = await loop.run_in_executor(_executor, _read_recent_posts, ss, label)

    dedup: set[str] = set(recent_texts)
    tg_token        = settings['tg_token']
    dest_chats      = settings['dest_chats']
    new_state: dict = dict(channel_state)
    total_sent      = 0

    for raw in raw_channels:
        username = _extract_username(raw)
        if not username:
            continue

        resolved = await _resolve_channel(client, username, ss, label, entity_cache)
        if not resolved:
            continue

        eid, chat_name, peer = resolved
        known_last = channel_state.get(username, 0)

        # Получаем сообщения
        try:
            messages = await client.get_messages(peer, limit=MESSAGES_LIMIT)
        except FloodWaitError as e:
            log.warning(f'[{label}] FloodWait {username}: жду {e.seconds}s')
            await asyncio.sleep(e.seconds + 2)
            try:
                messages = await client.get_messages(peer, limit=MESSAGES_LIMIT)
            except Exception as e2:
                log.error(f'[{label}] Повторная ошибка {username}: {e2}')
                await asyncio.sleep(REQUEST_DELAY)
                continue
        except Exception as e:
            log.error(f'[{label}] Ошибка get_messages {username}: {e}')
            await asyncio.sleep(REQUEST_DELAY)
            continue

        if not messages:
            await asyncio.sleep(REQUEST_DELAY)
            continue

        # Первый запуск — просто запоминаем ID, ничего не шлём
        if known_last == 0:
            new_state[username] = messages[0].id
            log.info(f'[{label}] [{chat_name}] первый запуск, last_id={messages[0].id}')
            await asyncio.sleep(REQUEST_DELAY)
            continue

        new_msgs = sorted(
            [m for m in messages if m.id > known_last],
            key=lambda m: m.id,
        )

        if not new_msgs:
            await asyncio.sleep(REQUEST_DELAY)
            continue

        # Разделяем на медиагруппы и одиночные
        groups: dict[int, list] = {}
        singles: list = []
        for msg in new_msgs:
            if msg.action is not None:
                continue
            gid = getattr(msg, 'grouped_id', None)
            if gid is not None:
                groups.setdefault(gid, []).append(msg)
            else:
                singles.append(msg)

        # Одиночные сообщения
        for msg in singles:
            text = msg.text or msg.message or ''
            if hasattr(msg, 'caption') and msg.caption:
                text = msg.caption
            text = ' '.join(text.split())

            if not _should_send(text, settings):
                continue

            norm = _normalize_text(text)
            if norm in dedup:
                log.info(f'[{label}] [{chat_name}] дубль — пропускаю')
                continue

            photos: list[bytes] = []
            if msg.photo:
                try:
                    buf_io = io.BytesIO()
                    await client.download_media(msg, file=buf_io)
                    photos = [buf_io.getvalue()]
                except Exception as e:
                    log.warning(f'[{label}] Не удалось скачать фото {msg.id}: {e}')

            try:
                chat = await client.get_entity(peer)
                link = _build_link(chat, msg.id)
            except Exception:
                link = ''

            author_name, author_link = _get_author_info(msg)

            post = {
                'date':        msg.date.replace(tzinfo=None),
                'chat_name':   chat_name,
                'author_name': author_name,
                'author_link': author_link,
                'link':        link,
                'text':        text,
            }
            await loop.run_in_executor(_executor, _write_post, ss, post)
            dedup.add(norm)
            total_sent += 1
            log.info(f'[{label}] ✅ [{chat_name}] -> {link}')

            if tg_token and dest_chats:
                parts = [f'📢 {chat_name}']
                if author_name:
                    s = author_name + (f' — {author_link}' if author_link else '')
                    parts.append(f'👤 {s}')
                parts += ['', text, '', f'🔗 {link}']
                body = '\n'.join(parts)
                if photos:
                    await loop.run_in_executor(
                        _executor, _send_single_photo_sync,
                        body, tg_token, dest_chats, photos[0],
                    )
                else:
                    await loop.run_in_executor(
                        _executor, _send_telegram_sync, body, tg_token, dest_chats,
                    )

        # Медиагруппы
        for gid, msgs in groups.items():
            msgs = sorted(msgs, key=lambda m: m.id)
            first_msg = msgs[0]

            text = ''
            for m in msgs:
                t = m.text or m.message or ''
                if hasattr(m, 'caption') and m.caption:
                    t = m.caption
                t = ' '.join(t.split())
                if t:
                    text = t
                    break

            if not _should_send(text, settings):
                log.info(f'[{label}] [{chat_name}] медиагруппа {gid} — отклонена фильтром')
                continue

            norm = _normalize_text(text)
            if norm in dedup:
                log.info(f'[{label}] [{chat_name}] медиагруппа {gid} — дубль, пропускаю')
                continue

            photos: list[bytes] = []
            for m in msgs:
                if m.photo or m.document:
                    try:
                        buf_io = io.BytesIO()
                        await client.download_media(m, file=buf_io)
                        photos.append(buf_io.getvalue())
                    except Exception as e:
                        log.warning(f'[{label}] Не удалось скачать медиа {m.id}: {e}')

            try:
                chat = await client.get_entity(peer)
                link = _build_link(chat, first_msg.id)
            except Exception:
                link = ''

            author_name, author_link = _get_author_info(first_msg)

            post = {
                'date':        first_msg.date.replace(tzinfo=None),
                'chat_name':   chat_name,
                'author_name': author_name,
                'author_link': author_link,
                'link':        link,
                'text':        text + (f' [📷 {len(photos)} фото]' if photos else ''),
            }
            await loop.run_in_executor(_executor, _write_post, ss, post)
            dedup.add(norm)
            total_sent += 1
            log.info(f'[{label}] ✅ [{chat_name}] медиагруппа {gid} ({len(photos)} фото) -> {link}')

            if tg_token and dest_chats:
                parts = [f'📢 {chat_name}']
                if author_name:
                    s = author_name + (f' — {author_link}' if author_link else '')
                    parts.append(f'👤 {s}')
                parts += ['', text, '', f'🔗 {link}']
                caption = '\n'.join(parts)
                if photos:
                    await loop.run_in_executor(
                        _executor, _send_media_group_sync,
                        caption, tg_token, dest_chats, photos,
                    )
                else:
                    await loop.run_in_executor(
                        _executor, _send_telegram_sync, caption, tg_token, dest_chats,
                    )

        new_state[username] = new_msgs[-1].id
        await asyncio.sleep(REQUEST_DELAY)

    # Сохраняем состояние
    await loop.run_in_executor(_executor, _write_state, ss, label, new_state)
    await loop.run_in_executor(
        _executor, _write_log, ss, label, 'INFO',
        f'Обход завершён | отправлено: {total_sent}'
    )
    log.info(f'[{label}] Обход завершён. Отправлено: {total_sent}')

# ══════════════════════════════════════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info('═══ TG Parser стартует (GitHub Actions) ═══')

    if not SPREADSHEET_IDS:
        log.error('SPREADSHEET_IDS не задан')
        return

    log.info(f'Таблиц: {len(SPREADSHEET_IDS)}')

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    try:
        await client.start()
    except Exception as e:
        if 'AuthKeyDuplicated' in type(e).__name__:
            log.error('AuthKeyDuplicatedError — пересоздай TG_SESSION.')
            return
        raise

    log.info('Telegram: подключён')

    try:
        for i, ss_id in enumerate(SPREADSHEET_IDS):
            await _process_spreadsheet(client, ss_id, i)
    finally:
        await client.disconnect()
        log.info('Telegram: отключён')

    log.info('═══ Готово ═══')


if __name__ == '__main__':
    asyncio.run(main())
