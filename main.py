import asyncio
import logging
import os
import re
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient, events
from telethon.tl.types import Message, Channel, Chat

# ============================================================
# КОНФИГУРАЦИЯ — заполни своими данными
# ============================================================
API_ID = int(os.environ.get('TG_API_ID', '0'))
API_HASH = os.environ.get('TG_API_HASH', '')
SESSION_NAME = 'parser_session'

SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', 'credentials.json')

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ============================================================
# GOOGLE SHEETS
# ============================================================
def get_sheets_client():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
    return gspread.authorize(creds)

def get_spreadsheet():
    gc = get_sheets_client()
    return gc.open_by_key(SPREADSHEET_ID)

def get_keywords(ss):
    """Читает ключевые слова и флаг из листа Настройки."""
    try:
        sheet = ss.worksheet('Настройки')
        data = sheet.get_all_values()
        
        # Чекбокс в E1 — TRUE/FALSE
        keywords_enabled = str(data[0][4]).upper() == 'TRUE' if len(data) > 0 and len(data[0]) > 4 else False
        
        # Ключевые слова в колонке D начиная со строки 2
        keywords = []
        for row in data[1:]:
            if len(row) > 3 and row[3].strip():
                keywords.append(row[3].strip())
        
        return keywords_enabled, keywords
    except Exception as e:
        log.error(f'Ошибка чтения настроек: {e}')
        return False, []

def get_allowed_chats(ss):
    """Читает список разрешённых чатов из листа Каналы."""
    try:
        sheet = ss.worksheet('Каналы')
        data = sheet.get_all_values()
        chats = set()
        for row in data[1:]:
            if row and row[0].strip():
                raw = row[0].strip()
                # Извлекаем username или chat_id
                username = extract_username(raw)
                if username:
                    chats.add(username.lower())
        return chats
    except Exception as e:
        log.error(f'Ошибка чтения каналов: {e}')
        return set()

def write_post(ss, date, chat_name, link, text):
    """Записывает пост в лист Посты."""
    try:
        sheet = ss.worksheet('Посты')
        sheet.append_row(
            [date.strftime('%Y-%m-%d %H:%M:%S'), chat_name, link, text],
            value_input_option='USER_ENTERED'
        )
        log.info(f'Записан пост: {chat_name} | {link}')
    except Exception as e:
        log.error(f'Ошибка записи поста: {e}')

def write_log(ss, level, message):
    """Пишет в лист Логи."""
    try:
        sheet = ss.worksheet('Логи')
        sheet.append_row(
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, message],
            value_input_option='USER_ENTERED'
        )
    except Exception as e:
        log.error(f'Ошибка записи лога: {e}')

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================
def extract_username(raw):
    """Извлекает username из любого формата ссылки."""
    if not raw:
        return None
    # https://t.me/username или t.me/username
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    # @username
    if raw.startswith('@'):
        return raw[1:]
    # просто username
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    # chat_id числовой
    if re.match(r'^-?\d+$', raw):
        return raw
    return None

def matches_keywords(text, keywords):
    """Проверяет текст на соответствие ключевым словам."""
    if not text or not keywords:
        return False
    text_lower = text.lower()
    
    for kw in keywords:
        kw_lower = kw.lower().strip()
        if not kw_lower:
            continue
        
        # Режим корня: купи* → всё начинающееся с "купи"
        if kw_lower.endswith('*'):
            if kw_lower[:-1] in text_lower:
                return True
            continue
        
        # Точное слово
        escaped = re.escape(kw_lower)
        if re.search(r'\b' + escaped + r'\b', text_lower):
            return True
        
        # Автоформы (корень минус 2 буквы + окончания)
        if len(kw_lower) > 4:
            root = escaped[:-2]
            suffixes = r'(ть|л|ла|ли|ло|ет|ешь|ем|ете|ут|ют|ит|ишь|им|ите|ат|ят|у|ю|а|я|е|и|ой|ей|ого|его|ому|ему|ом|ем|ых|их|ов|ами|ями)?'
            if re.search(r'\b' + root + suffixes + r'\b', text_lower):
                return True
    
    return False

def build_link(chat, msg_id):
    """Строит ссылку на сообщение."""
    if hasattr(chat, 'username') and chat.username:
        return f'https://t.me/{chat.username}/{msg_id}'
    # Для приватных чатов
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    return f'https://t.me/c/{chat_id}/{msg_id}'

# ============================================================
# ОСНОВНОЙ КОД
# ============================================================
async def main():
    log.info('Запуск парсера...')
    
    # Подключаемся к Google Sheets
    try:
        ss = get_spreadsheet()
        log.info('Google Sheets подключён')
        write_log(ss, 'INFO', 'Парсер запущен')
    except Exception as e:
        log.error(f'Ошибка подключения к Google Sheets: {e}')
        return
    
    # Подключаемся к Telegram
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    log.info('Telegram подключён')
    
    # Получаем список разрешённых чатов
    allowed_chats = get_allowed_chats(ss)
    log.info(f'Разрешённых чатов: {len(allowed_chats)}')
    
    @client.on(events.NewMessage)
    async def handler(event):
        try:
            msg = event.message
            chat = await event.get_chat()
            
            # Определяем идентификатор чата
            chat_username = getattr(chat, 'username', None)
            chat_id = str(chat.id)
            
            # Проверяем что чат в списке разрешённых
            in_allowed = (
                (chat_username and chat_username.lower() in allowed_chats) or
                chat_id in allowed_chats or
                chat_id.lstrip('-') in allowed_chats
            )
            
            if not in_allowed:
                return
            
            # Имя чата для записи
            chat_name = getattr(chat, 'title', None) or chat_username or chat_id
            
            # Текст сообщения
            text = msg.text or msg.message or ''
            if hasattr(msg, 'caption') and msg.caption:
                text = msg.caption
            
            # Ссылка на сообщение
            link = build_link(chat, msg.id)
            
            # Дата
            date = msg.date.replace(tzinfo=None) if msg.date.tzinfo else msg.date
            
            # Читаем актуальные настройки (каждый раз чтобы подхватывать изменения)
            keywords_enabled, keywords = get_keywords(ss)
            
            # Фильтр по ключевым словам
            if keywords_enabled and keywords and text.strip():
                if not matches_keywords(text, keywords):
                    return
            
            # Записываем в таблицу
            write_post(ss, date, chat_name, link, text)
            
        except Exception as e:
            log.error(f'Ошибка обработки сообщения: {e}')
    
    log.info('Слушаю сообщения...')
    write_log(ss, 'INFO', f'Слушаю {len(allowed_chats)} чатов')
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
