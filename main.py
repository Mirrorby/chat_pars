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
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()
        keywords_enabled = str(data[0][4]).upper() == 'TRUE' if data and len(data[0]) > 4 else False
        keywords = []
        for row in data[1:]:
            if len(row) > 3 and row[3].strip():
                keywords.append(row[3].strip())
        return keywords_enabled, keywords
    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e))
        return False, []

def get_tg_settings(ss):
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()
        token = str(data[1][1]).strip() if len(data) > 1 and len(data[1]) > 1 else ''
        chats = []
        for row in data[2:]:
            if len(row) > 1 and str(row[1]).strip():
                chats.append(str(row[1]).strip())
        return token, chats
    except Exception as e:
        log.error('Ошибка чтения TG настроек: ' + str(e))
        return '', []

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
            channels.append({'username': username, 'last_link': last_link, 'row': i})
        return channels
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e))
        return []

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
            p['author'],
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

def send_to_telegram(posts, tg_token, tg_chats):
    if not posts or not tg_token or not tg_chats:
        return
    for p in posts:
        author = p.get('author', '')
        parts = ['📢 ' + p['chat_name']]
        if author:
            parts.append('👤 ' + author)
        parts.append('')
        parts.append(p['text'])
        parts.append('')
        parts.append('🔗 ' + p['link'])
        body = '\n'.join(parts)
        if len(body) > 4000:
            body = body[:4000] + '...'
        for chat_id in tg_chats:
            try:
                url = 'https://api.telegram.org/bot' + tg_token + '/sendMessage'
                data = json.dumps({'chat_id': chat_id, 'text': body, 'disable_web_page_preview': False}).encode('utf-8')
                req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req, timeout=10)
                time.sleep(0.3)
            except Exception as e:
                log.error('Ошибка отправки TG в ' + str(chat_id) + ': ' + str(e) + ' | текст: ' + body[:200])
        time.sleep(0.3)

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

def get_author(msg):
    try:
        if not msg.sender:
            return ''
        sender = msg.sender
        first = getattr(sender, 'first_name', '') or ''
        last = getattr(sender, 'last_name', '') or ''
        username = getattr(sender, 'username', '') or ''
        full_name = (first + ' ' + last).strip()
        if username:
            return (full_name + ' (@' + username + ')').strip() if full_name else '@' + username
        return full_name or str(getattr(sender, 'id', ''))
    except Exception:
        return ''

def matches_keywords(text, keywords):
    if not text or not keywords:
        return False
    text_lower = text.lower()
    for kw in keywords:
        kw_lower = kw.lower().strip()
        if not kw_lower:
            continue
        if kw_lower.endswith('*'):
            if kw_lower[:-1] in text_lower:
                return True
            continue
        escaped = re.escape(kw_lower)
        if re.search(r'\b' + escaped + r'\b', text_lower):
            return True
        if len(kw_lower) > 4:
            root = escaped[:-2]
            suffixes = r'(ть|л|ла|ли|ло|ет|ешь|ем|ете|ут|ют|ит|ишь|им|ите|ат|ят|у|ю|а|я|е|и|ой|ей|ого|его|ому|ему|ом|ем|ых|их|ов|ами|ями)?'
            if re.search(r'\b' + root + suffixes + r'\b', text_lower):
                return True
    return False

async def main():
    log.info('Запуск прогона...')

    try:
        ss = get_spreadsheet()
        log.info('Google Sheets подключён')
    except Exception as e:
        log.error('Ошибка Google Sheets: ' + str(e))
        return

    keywords_enabled, keywords = get_settings(ss)
    tg_token, tg_chats = get_tg_settings(ss)
    channels = get_channels(ss)

    log.info('Чатов: ' + str(len(channels)) + ' | Ключи: ' + ('ВКЛ (' + str(len(keywords)) + ' шт)' if keywords_enabled else 'ВЫКЛ'))

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

    write_log(ss, 'INFO', 'ПРОГОН НАЧАТ | чатов: ' + str(len(channels)) + ' | ключи: ' + ('ВКЛ (' + str(len(keywords)) + ' шт)' if keywords_enabled else 'ВЫКЛ'))

    for ch in channels:
        chat_username = ch['username']
        last_link = ch['last_link']
        last_post_id = extract_post_id(last_link) if last_link else 0
        row = ch['row']

        try:
            chat = await client.get_entity(chat_username)
            chat_name = getattr(chat, 'title', None) or getattr(chat, 'username', None) or str(chat_username)

            since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
            messages = []

            async for msg in client.iter_messages(chat_username, limit=100):
                if last_post_id > 0:
                    if msg.id <= last_post_id:
                        break
                else:
                    if msg.date < since:
                        break
                messages.append(msg)

            messages.sort(key=lambda m: m.id)

            new_msgs_count = len(messages)
            saved_msgs = []
            new_last_link = last_link

            for msg in messages:
                text = msg.text or msg.message or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption

                # Текст в одну строку без переносов и лишних пробелов
                text = ' '.join(text.split())

                author = get_author(msg)
                link = build_link(chat, msg.id)
                date = msg.date.replace(tzinfo=None)
                new_last_link = link

                # Фильтр ключевых слов только для постов с текстом
                if keywords_enabled and keywords and text.strip():
                    if not matches_keywords(text, keywords):
                        continue

                saved_msgs.append({
                    'date': date,
                    'chat_name': chat_name,
                    'author': author,
                    'link': link,
                    'text': text
                })

            total_new += new_msgs_count
            total_saved += len(saved_msgs)
            all_new_posts.extend(saved_msgs)

            if new_msgs_count > 0:
                update_channel(ss, row, new_last_link, '✅ Новых: ' + str(new_msgs_count) + ' | Записано: ' + str(len(saved_msgs)))
            else:
                update_channel(ss, row, last_link, '✅ Нет новых сообщений')

            log.info(chat_username + ' | новых: ' + str(new_msgs_count) + ' | в таблицу: ' + str(len(saved_msgs)) + ' | lastId: ' + (str(last_post_id) if last_post_id else 'пусто'))

        except Exception as e:
            log.error(chat_username + ' | ОШИБКА: ' + str(e))
            update_channel(ss, row, last_link, '❌ Ошибка: ' + str(e)[:50])
            write_log(ss, 'ERROR', chat_username + ' | ' + str(e)[:100])

        await asyncio.sleep(1)

    write_posts(ss, all_new_posts)

    if all_new_posts and tg_token and tg_chats:
        log.info('Отправляю ' + str(len(all_new_posts)) + ' постов в TG...')
        send_to_telegram(all_new_posts, tg_token, tg_chats)

    summary = 'ПРОГОН ЗАВЕРШЁН | чатов: ' + str(len(channels)) + ' | новых: ' + str(total_new) + ' | записано: ' + str(total_saved)
    log.info(summary)
    write_log(ss, 'INFO', summary)

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
