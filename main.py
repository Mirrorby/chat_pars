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
from telethon.errors import FloodWaitError

API_ID = int(os.environ.get('TG_API_ID', '0'))
API_HASH = os.environ.get('TG_API_HASH', '')
SESSION_STRING = os.environ.get('TG_SESSION', '')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDENTIALS_BASE64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
LOOKBACK_MINUTES = int(os.environ.get('LOOKBACK_MINUTES', '35'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)


def get_spreadsheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def get_settings(ss):
    """
    Лист Настройки:
      A1: Настройки бота  B1:         D1: Ключевые слова  E1: Тема  F1: [чекбокс вкл/выкл]
      A2: TG-бот          B2: токен   D2: ключ1           E2: тема1
      A3: Чаты            B3: тема    D3: ключ2           E3: (пусто = все темы)
      A4: chat_id1        B4: тема    D4: (пусто)         E4: тема2 (берём всё из темы2)
      ...

    Возвращает:
      keywords_enabled  — bool (F1)
      keyword_rules     — список {'kw': str, 'topic': str} (тема '' = все темы)
      tg_token          — str
      topic_chats       — {'тема': ['chat_id', ...], ...}
    """
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()

        # F1 — чекбокс включения фильтра
        keywords_enabled = data[0][5] == True or str(data[0][5]).upper() == 'TRUE' if data and len(data[0]) > 5 else False

        # Токен — B2
        tg_token = str(data[1][1]).strip() if len(data) > 1 and len(data[1]) > 1 else ''

        # Правила ключевых слов: D=ключ, E=тема (пусто = все темы)
        keyword_rules = []
        for row in data[1:]:
            kw = str(row[3]).strip() if len(row) > 3 else ''
            topic = str(row[4]).strip() if len(row) > 4 else ''
            if kw:
                keyword_rules.append({'kw': kw.lower().rstrip('*'), 'topic': topic.lower()})
            elif topic:
                # Тема без ключа — берём всё из этой темы (специальный маркер)
                keyword_rules.append({'kw': '', 'topic': topic.lower()})

        # Чаты по темам: A=chat_id, B=тема (строки с 4-й, индекс 3)
        topic_chats = {}
        for row in data[3:]:
            chat_id = str(row[0]).strip() if len(row) > 0 else ''
            topic = str(row[1]).strip() if len(row) > 1 else ''
            if not chat_id:
                continue
            key = topic.lower()
            if key not in topic_chats:
                topic_chats[key] = []
            topic_chats[key].append(chat_id)

        return keywords_enabled, keyword_rules, tg_token, topic_chats

    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e))
        return False, [], '', {}


def get_channels(ss):
    """
    Лист Каналы: Канал | Последний пост | Статус | Тема
    """
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
            topic = row[3].strip().lower() if len(row) > 3 else ''
            channels.append({'username': username, 'last_link': last_link, 'row': i, 'topic': topic})
        return channels
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e))
        return []


def should_save_post(text, topic, keywords_enabled, keyword_rules):
    """
    Решает — сохранять пост или нет.

    Логика (F1 ВКЛ):
    1. Собираем правила применимые к теме поста:
       - правила с совпадающей темой
       - правила без темы (применяются ко всем)
    2. Если среди применимых есть правило с пустым ключом (тема без ключа)
       → берём всё из этой темы, возвращаем True
    3. Иначе проверяем ключи — если хоть один найден в тексте → True
    4. Если применимых правил нет → пропускаем пост
    """
    if not keywords_enabled:
        return True

    if not text.strip():
        return False

    text_lower = text.lower()

    # Правила применимые к данной теме
    applicable = [r for r in keyword_rules if r['topic'] == '' or r['topic'] == topic]

    if not applicable:
        return False

    # Есть правило "тема без ключа" — берём всё
    if any(r['kw'] == '' for r in applicable):
        return True

    # Проверяем ключи
    for r in applicable:
        if r['kw'] and r['kw'] in text_lower:
            return True

    return False


def update_channel(ss, row, last_link, status):
    try:
        sheet = ss.worksheet('Каналы')
        sheet.update([[last_link, status]], 'B' + str(row) + ':C' + str(row))
    except Exception as e:
        log.error('Ошибка обновления канала row=' + str(row) + ': ' + str(e))


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
        sheet.append_row([datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, safe], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи лога: ' + str(e))


def send_to_telegram(posts, tg_token, topic_chats):
    """Отправляет каждый пост в чаты его темы."""
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
        parts.append('')
        parts.append(p['text'])
        parts.append('')
        parts.append('🔗 ' + p['link'])
        body = '\n'.join(parts)
        if len(body) > 4000:
            body = body[:4000] + '...'

        post_topic = p.get('topic', '').lower()
        chats = topic_chats.get(post_topic) or topic_chats.get('') or []

        for chat_id in chats:
            try:
                url = 'https://api.telegram.org/bot' + tg_token + '/sendMessage'
                data = json.dumps({'chat_id': chat_id, 'text': body, 'disable_web_page_preview': False}).encode('utf-8')
                req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req, timeout=10)
                time.sleep(0.3)
            except Exception as e:
                log.error('Ошибка отправки TG в ' + str(chat_id) + ': ' + str(e) + ' | текст: ' + body[:150])
        time.sleep(0.3)


def extract_username(raw):
    """Возвращает peer для Telethon: username строкой или числовой id '-100XXXX'.

    Поддерживаемые форматы в листе Каналы:
      https://t.me/username           → 'username'
      https://t.me/c/3062996124/2     → '-1003062996124'
      https://t.me/c/3062996124       → '-1003062996124'
      @username                       → 'username'
      username                        → 'username'
      -1001234567890                  → '-1001234567890'
    """
    if not raw:
        return None
    # Приватная ссылка: t.me/c/CHAT_ID[/MSG_ID]
    m = re.match(r'(?:https?://)?t\.me/c/(\d+)(?:/\d+)?', raw)
    if m:
        return '-100' + m.group(1)
    # Публичная ссылка: t.me/username
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


async def main():
    log.info('Запуск прогона...')
    try:
        ss = get_spreadsheet()
        log.info('Google Sheets подключён')
    except Exception as e:
        log.error('Ошибка Google Sheets: ' + str(e))
        return

    keywords_enabled, keyword_rules, tg_token, topic_chats = get_settings(ss)
    channels = get_channels(ss)

    kw_count = len([r for r in keyword_rules if r['kw']])
    log.info('Чатов: ' + str(len(channels)) + ' | Ключи: ' + ('ВКЛ (' + str(kw_count) + ' шт)' if keywords_enabled else 'ВЫКЛ') + ' | Тем: ' + str(len(topic_chats)))

    if not channels:
        log.warning('Нет каналов в листе Каналы')
        write_log(ss, 'WARN', 'Нет каналов в листе Каналы')
        return

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    log.info('Telegram подключён')

    all_new_posts = []
    total_new = 0
    total_saved = 0

    write_log(ss, 'INFO', 'ПРОГОН НАЧАТ | чатов: ' + str(len(channels)) + ' | ключи: ' + ('ВКЛ (' + str(kw_count) + ' шт)' if keywords_enabled else 'ВЫКЛ'))

    for ch in channels:
        chat_username = ch['username']
        # Числовые peer_id передаём как int — иначе Telethon не резолвит приватные чаты
        chat_peer = int(chat_username) if re.match(r'^-?\d+$', str(chat_username)) else chat_username
        last_link = ch['last_link']
        last_post_id = extract_post_id(last_link) if last_link else 0
        row = ch['row']
        topic = ch['topic']

        try:
            since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
            messages = []

            async for msg in client.iter_messages(chat_peer, limit=100):
                if last_post_id > 0:
                    if msg.id <= last_post_id:
                        break
                else:
                    if msg.date < since:
                        break
                messages.append(msg)

            if not messages:
                update_channel(ss, row, last_link, '✅ Нет новых сообщений')
                log.info(chat_username + ' [' + (topic or 'без темы') + '] | новых: 0')
                await asyncio.sleep(2)
                continue

            # Берём chat из первого сообщения — без get_entity (защита от FloodWait)
            chat = await messages[0].get_chat()
            chat_name = getattr(chat, 'title', None) or getattr(chat, 'username', None) or str(chat_username)

            messages.sort(key=lambda m: m.id)
            new_msgs_count = len(messages)
            saved_msgs = []
            new_last_link = last_link

            for msg in messages:
                if msg.action is not None:
                    continue

                text = msg.text or msg.message or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption
                text = ' '.join(text.split())

                author_name, author_link = get_author_info(msg)
                link = build_link(chat, msg.id)
                date = msg.date.replace(tzinfo=None)
                new_last_link = link

                if not should_save_post(text, topic, keywords_enabled, keyword_rules):
                    continue

                saved_msgs.append({
                    'date': date,
                    'chat_name': chat_name,
                    'topic': topic,
                    'author_name': author_name,
                    'author_link': author_link,
                    'link': link,
                    'text': text
                })

            total_new += new_msgs_count
            total_saved += len(saved_msgs)
            all_new_posts.extend(saved_msgs)

            update_channel(ss, row, new_last_link, '✅ Новых: ' + str(new_msgs_count) + ' | Записано: ' + str(len(saved_msgs)))
            log.info(chat_username + ' [' + (topic or 'без темы') + '] | новых: ' + str(new_msgs_count) + ' | в таблицу: ' + str(len(saved_msgs)) + ' | lastId: ' + (str(last_post_id) if last_post_id else 'пусто'))

        except FloodWaitError as e:
            wait = e.seconds + 5
            log.warning(chat_username + ' | FloodWait ' + str(wait) + 's — жду...')
            write_log(ss, 'WARN', chat_username + ' | FloodWait ' + str(wait) + 's')
            update_channel(ss, row, last_link, '⏳ FloodWait ' + str(wait) + 's')
            await asyncio.sleep(wait)

        except Exception as e:
            log.error(chat_username + ' | ОШИБКА: ' + str(e))
            update_channel(ss, row, last_link, '❌ Ошибка: ' + str(e)[:50])
            write_log(ss, 'ERROR', chat_username + ' | ' + str(e)[:100])

        await asyncio.sleep(2)

    write_posts(ss, all_new_posts)

    if all_new_posts and tg_token and topic_chats:
        log.info('Отправляю ' + str(len(all_new_posts)) + ' постов в TG...')
        send_to_telegram(all_new_posts, tg_token, topic_chats)

    summary = 'ПРОГОН ЗАВЕРШЁН | чатов: ' + str(len(channels)) + ' | новых: ' + str(total_new) + ' | записано: ' + str(total_saved)
    log.info(summary)
    write_log(ss, 'INFO', summary)

    await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
