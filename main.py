import asyncio
import base64
import json
import logging
import os
import re
import urllib.request
from datetime import datetime, timezone, timedelta
import time

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError

API_ID = int(os.environ.get('TG_API_ID', '0'))
API_HASH = os.environ.get('TG_API_HASH', '')
SESSION_STRING = os.environ.get('TG_SESSION', '')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDENTIALS_BASE64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
LOOKBACK_MINUTES = int(os.environ.get('LOOKBACK_MINUTES', '65'))  # чуть больше интервала запуска

# Задержка между чатами (секунды) — главная защита от флудбана
DELAY_BETWEEN_CHATS = float(os.environ.get('DELAY_BETWEEN_CHATS', '2.5'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# ── Google Sheets ──────────────────────────────────────────────────────────────

def get_spreadsheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def get_settings(ss):
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()
        keywords_enabled = str(data[0][4]).upper() == 'TRUE' if data and len(data[0]) > 4 else False
        keywords = []
        for row in data[1:]:
            if len(row) > 3 and row[3].strip():
                keywords.append(row[3].strip())
        token = str(data[1][1]).strip() if len(data) > 1 and len(data[1]) > 1 else ''
        topic_chats = {}
        for row in data[3:]:
            chat_id = str(row[0]).strip() if len(row) > 0 else ''
            topic = str(row[1]).strip() if len(row) > 1 else ''
            if not chat_id:
                continue
            topic_chats.setdefault(topic, []).append(chat_id)
        return keywords_enabled, keywords, token, topic_chats
    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e))
        return False, [], '', {}


def get_channels(ss):
    try:
        sheet = ss.worksheet('Каналы')
        data = sheet.get_all_values()
        channels = []
        for i, row in enumerate(data[1:], start=2):
            if not row or not row[0].strip():
                continue
            username = extract_username(row[0].strip())
            if not username:
                continue
            last_link = row[1].strip() if len(row) > 1 else ''
            topic = row[3].strip() if len(row) > 3 else ''
            channels.append({'username': username, 'last_link': last_link, 'row': i, 'topic': topic})
        return channels
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e))
        return []


def batch_update_channels(ss, updates):
    """Обновляет все каналы за один запрос к Sheets вместо N запросов."""
    if not updates:
        return
    try:
        sheet = ss.worksheet('Каналы')
        data = [None] * (max(u['row'] for u in updates) + 1)
        requests = []
        for u in updates:
            requests.append({
                'range': f"B{u['row']}:C{u['row']}",
                'values': [[u['last_link'], u['status']]]
            })
        sheet.batch_update(requests)
    except Exception as e:
        log.error('Ошибка batch_update_channels: ' + str(e))


def write_posts(ss, posts):
    if not posts:
        return
    try:
        sheet = ss.worksheet('Посты')
        rows = [[
            p['date'].strftime('%Y-%m-%d %H:%M:%S'),
            p['chat_name'],
            p['topic'],
            p['author_name'],
            p['author_link'],
            p['link'],
            p['text']
        ] for p in posts]
        sheet.append_rows(rows, value_input_option='USER_ENTERED')
        log.info('Записано постов: ' + str(len(rows)))
    except Exception as e:
        log.error('Ошибка записи постов: ' + str(e))


def write_log(ss, level, message):
    try:
        sheet = ss.worksheet('Логи')
        safe = str(message)
        if safe and safe[0] in '=+-@':
            safe = "'" + safe
        sheet.append_row(
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, safe],
            value_input_option='USER_ENTERED'
        )
    except Exception as e:
        log.error('Ошибка записи лога: ' + str(e))


# ── Telegram отправка ──────────────────────────────────────────────────────────

def send_to_telegram(posts, tg_token, topic_chats):
    if not posts or not tg_token or not topic_chats:
        return
    for p in posts:
        parts = ['📢 ' + p['chat_name']]
        if p.get('topic'):
            parts.append('🏷 ' + p['topic'])
        if p.get('author_name'):
            author_str = p['author_name']
            if p.get('author_link'):
                author_str += ' — ' + p['author_link']
            parts.append('👤 ' + author_str)
        parts.extend(['', p['text'], '', '🔗 ' + p['link']])
        body = '\n'.join(parts)
        if len(body) > 4000:
            body = body[:4000] + '...'

        chats = topic_chats.get(p.get('topic', '')) or topic_chats.get('') or []
        for chat_id in chats:
            try:
                url = 'https://api.telegram.org/bot' + tg_token + '/sendMessage'
                data = json.dumps({
                    'chat_id': chat_id, 'text': body, 'disable_web_page_preview': False
                }).encode('utf-8')
                req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req, timeout=10)
                time.sleep(0.3)
            except Exception as e:
                log.error('Ошибка отправки TG в ' + str(chat_id) + ': ' + str(e))


# ── Утилиты ────────────────────────────────────────────────────────────────────

def extract_username(raw):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    if re.match(r'^-?\d+$', raw):
        return raw
    return None


def extract_post_id(link):
    m = re.search(r'/(\d+)$', link)
    return int(m.group(1)) if m else 0


def build_link(chat, msg_id):
    username = getattr(chat, 'username', None)
    if username:
        return 'https://t.me/' + username + '/' + str(msg_id)
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    return 'https://t.me/c/' + chat_id + '/' + str(msg_id)


def get_author_info(msg):
    try:
        if not msg.sender:
            return '', ''
        sender = msg.sender
        first = getattr(sender, 'first_name', '') or ''
        last = getattr(sender, 'last_name', '') or ''
        username = getattr(sender, 'username', '') or ''
        full_name = (first + ' ' + last).strip()
        author_link = ('https://t.me/' + username) if username else ''
        return full_name, author_link
    except Exception:
        return '', ''


def matches_keywords(text, keywords):
    if not text or not keywords:
        return False
    text_lower = text.lower()
    for kw in keywords:
        kw_lower = kw.lower().strip().rstrip('*')
        if kw_lower and kw_lower in text_lower:
            return True
    return False


# ── Главная логика ─────────────────────────────────────────────────────────────

async def process_chat(client, ch, keywords_enabled, keywords, since):
    """Обрабатывает один чат. Возвращает (posts, last_link, status, error)."""
    chat_username = ch['username']
    last_link = ch['last_link']
    last_post_id = extract_post_id(last_link) if last_link else 0
    topic = ch['topic']

    try:
        chat = await client.get_entity(chat_username)
        chat_name = getattr(chat, 'title', None) or getattr(chat, 'username', None) or str(chat_username)

        messages = []
        async for msg in client.iter_messages(chat_username, limit=50):
            if last_post_id > 0:
                if msg.id <= last_post_id:
                    break
            else:
                if msg.date < since:
                    break
            messages.append(msg)

        messages.sort(key=lambda m: m.id)
        new_last_link = last_link
        saved = []

        for msg in messages:
            if msg.action is not None:
                continue
            text = msg.text or msg.message or ''
            if hasattr(msg, 'caption') and msg.caption:
                text = msg.caption
            text = ' '.join(text.split())

            link = build_link(chat, msg.id)
            new_last_link = link

            if keywords_enabled and keywords:
                if not text.strip() or not matches_keywords(text, keywords):
                    continue

            author_name, author_link = get_author_info(msg)
            saved.append({
                'date': msg.date.replace(tzinfo=None),
                'chat_name': chat_name,
                'topic': topic,
                'author_name': author_name,
                'author_link': author_link,
                'link': link,
                'text': text
            })

        new_count = len(messages)
        status = f'✅ Новых: {new_count} | Записано: {len(saved)}' if new_count > 0 else '✅ Нет новых'
        log.info(f'{chat_username} [{topic or "—"}] | новых: {new_count} | в таблицу: {len(saved)}')
        return saved, new_last_link, status, None

    except FloodWaitError as e:
        wait = e.seconds + 5
        log.warning(f'{chat_username} | FloodWait {e.seconds}s — жду {wait}s')
        await asyncio.sleep(wait)
        # После ожидания возвращаем пустой результат, не падаем
        return [], last_link, f'⏳ FloodWait {e.seconds}s', None

    except (ChannelPrivateError, UsernameNotOccupiedError) as e:
        log.warning(f'{chat_username} | недоступен: {e}')
        return [], last_link, '🔒 Недоступен', str(e)

    except Exception as e:
        log.error(f'{chat_username} | ОШИБКА: {e}')
        return [], last_link, f'❌ {str(e)[:50]}', str(e)


async def main():
    log.info('Запуск прогона...')

    try:
        ss = get_spreadsheet()
        log.info('Google Sheets подключён')
    except Exception as e:
        log.error('Ошибка Google Sheets: ' + str(e))
        return

    keywords_enabled, keywords, tg_token, topic_chats = get_settings(ss)
    channels = get_channels(ss)

    if not channels:
        log.warning('Нет каналов в листе Каналы')
        write_log(ss, 'WARN', 'Нет каналов в листе Каналы')
        return

    log.info(f'Чатов: {len(channels)} | Ключи: {"ВКЛ (" + str(len(keywords)) + " шт)" if keywords_enabled else "ВЫКЛ"}')

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    log.info('Telegram подключён')

    since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
    all_posts = []
    channel_updates = []
    total_new = 0
    total_saved = 0

    write_log(ss, 'INFO', f'ПРОГОН НАЧАТ | чатов: {len(channels)}')

    for ch in channels:
        posts, new_last_link, status, error = await process_chat(
            client, ch, keywords_enabled, keywords, since
        )

        all_posts.extend(posts)
        total_saved += len(posts)

        channel_updates.append({
            'row': ch['row'],
            'last_link': new_last_link,
            'status': status
        })

        if error:
            write_log(ss, 'ERROR', f"{ch['username']} | {error[:100]}")

        # Задержка между чатами — ключевая защита от флудбана
        await asyncio.sleep(DELAY_BETWEEN_CHATS)

    # Все записи в Sheets за минимум запросов
    write_posts(ss, all_posts)
    batch_update_channels(ss, channel_updates)

    summary = f'ПРОГОН ЗАВЕРШЁН | чатов: {len(channels)} | записано: {total_saved}'
    log.info(summary)
    write_log(ss, 'INFO', summary)

    if all_posts and tg_token and topic_chats:
        log.info(f'Отправляю {len(all_posts)} постов в TG...')
        send_to_telegram(all_posts, tg_token, topic_chats)

    await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
