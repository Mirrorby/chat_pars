"""
TG Parser — multi-account, GitHub Actions.

Secrets (GitHub):
  TG_API_ID_1, TG_API_HASH_1      ← аккаунт 1 (свой app на my.telegram.org)
  TG_API_ID_2, TG_API_HASH_2      ← аккаунт 2
  TG_API_ID_3, TG_API_HASH_3      ← аккаунт 3
  TG_SESSION_1 ... TG_SESSION_3   ← строковые сессии (по одной на каждый API_ID)
  GOOGLE_CREDENTIALS_BASE64
  SPREADSHEET_IDS                 ← id1,id2,...
  MESSAGES_LIMIT                  ← сообщений с канала за раз (default: 10)

Структура таблицы (каждая):
  Каналы:    A2+=  username / https://t.me/xxx / числовой id
  Настройки: B2=токен бота, A4+=chat_id назначения,
             E1=TRUE/FALSE ключи вкл, D2+=ключевые слова,
             G1=TRUE/FALSE негативы вкл, F2+=негативные слова
  Кеш:       A=username  B=entity_id  C=chat_name      ← ЧИТАЕТСЯ и ПИШЕТСЯ
  Посты:     A=дата B=канал C=тема D=автор E=аккаунт F=ссылка G=текст
  Логи:      A=дата B=уровень C=сообщение
  Состояние: A=username B=last_id

Распределение каналов по аккаунтам — по hash(username), статично.
Кеш entity_id пишется в лист Кеш — при следующем запуске get_entity не вызывается.
State сохраняется каждые STATE_SAVE_INTERVAL каналов + в конце.
"""

import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import random
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError, FloodWaitError,
    UsernameInvalidError, UsernameNotOccupiedError,
)
from telethon.sessions import StringSession

# ── config ─────────────────────────────────────────────────────────────────────
GS_B64    = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
SS_IDS    = [s.strip() for s in os.environ.get('SPREADSHEET_IDS', '').split(',') if s.strip()]
MSG_LIMIT = int(os.environ.get('MESSAGES_LIMIT', '10'))

DELAY_MIN = 2.0   # минимальная пауза между каналами (сек)
DELAY_MAX = 4.0   # максимальная пауза (джиттер)
STATE_SAVE_INTERVAL = 20  # сохранять state каждые N каналов

# Собираем аккаунты: каждый — своя тройка API_ID / API_HASH / SESSION
ACCOUNTS = []
for _i in range(1, 10):
    _id   = os.environ.get(f'TG_API_ID_{_i}', '').strip()
    _hash = os.environ.get(f'TG_API_HASH_{_i}', '').strip()
    _sess = os.environ.get(f'TG_SESSION_{_i}', '').strip()
    if _id and _hash and _sess:
        ACCOUNTS.append(dict(api_id=int(_id), api_hash=_hash, session=_sess))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)
pool = ThreadPoolExecutor(max_workers=8)


# ── google sheets ──────────────────────────────────────────────────────────────

def _gs_open(ss_id):
    creds = Credentials.from_service_account_info(
        json.loads(base64.b64decode(GS_B64)),
        scopes=['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'],
    )
    return gspread.authorize(creds).open_by_key(ss_id)


def _gs_settings(ss, label):
    try:
        d = ss.worksheet('Настройки').get_all_values()
        token  = d[1][1].strip() if len(d) > 1 and len(d[1]) > 1 else ''
        chats  = [r[0].strip() for r in d[3:] if r and r[0].strip()]
        kw_on  = len(d) > 0 and len(d[0]) > 4 and str(d[0][4]).upper() == 'TRUE'
        kws    = [r[3].strip() for r in d[1:] if len(r) > 3 and r[3].strip()]
        neg_on = len(d) > 0 and len(d[0]) > 6 and str(d[0][6]).upper() == 'TRUE'
        negs   = [r[5].strip() for r in d[1:] if len(r) > 5 and r[5].strip()]
        kw_str  = f'ключи: ВКЛ ({len(kws)})' if kw_on  else f'ключи: ВЫКЛ'
        neg_str = f'негативы: ВКЛ ({len(negs)})' if neg_on else f'негативы: ВЫКЛ'
        log.info(f'[{label}] token:{"OK" if token else "NO"} chats:{len(chats)} {kw_str} {neg_str}')
        return dict(token=token, chats=chats,
                    kw_on=kw_on, kws=kws, neg_on=neg_on, negs=negs)
    except Exception as e:
        log.error(f'[{label}] settings: {e}')
        return None


def _gs_channels(ss, label):
    try:
        return [r[0].strip() for r in ss.worksheet('Каналы').get_all_values()[1:]
                if r and r[0].strip()]
    except Exception as e:
        log.error(f'[{label}] channels: {e}')
        return []


def _gs_read_cache(ss, label):
    """Читает лист Кеш → {username: entity_id}"""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
            ws.append_row(['username', 'entity_id', 'chat_name'])
            return {}
        cache = {}
        for r in ws.get_all_values()[1:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                try:
                    cache[r[0].strip()] = int(float(r[1].strip()))
                except ValueError:
                    pass
        log.info(f'[{label}] entity cache: {len(cache)} entries')
        return cache
    except Exception as e:
        log.error(f'[{label}] read_cache: {e}')
        return {}


def _gs_write_cache(ss, label, cache: dict):
    """Перезаписывает лист Кеш полностью."""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
        rows = [['username', 'entity_id', 'chat_name']]
        rows += [[u, str(eid), name] for u, (eid, name) in cache.items()]
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'[{label}] entity cache saved: {len(cache)}')
    except Exception as e:
        log.error(f'[{label}] write_cache: {e}')


def _gs_read_state(ss, label):
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet('Состояние', 1000, 2)
            ws.append_row(['username', 'last_id'])
            return {}
        state = {}
        for r in ws.get_all_values()[1:]:
            if len(r) >= 2 and r[0].strip() and r[1].strip():
                try:
                    state[r[0].strip()] = int(float(r[1].strip()))
                except ValueError:
                    pass
        log.info(f'[{label}] state: {len(state)} channels')
        return state
    except Exception as e:
        log.error(f'[{label}] read_state: {e}')
        return {}


def _gs_write_state(ss, label, state):
    try:
        try:
            ws = ss.worksheet('Состояние')
        except Exception:
            ws = ss.add_worksheet('Состояние', 1000, 2)
        rows = [['username', 'last_id']] + [[u, str(v)] for u, v in state.items()]
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'[{label}] state saved: {len(state)}')
    except Exception as e:
        log.error(f'[{label}] write_state: {e}')


def _gs_write_post(ss, date, channel, link, text):
    try:
        ss.worksheet('Посты').append_row(
            [date.strftime('%Y-%m-%d %H:%M:%S'), channel, '', '', '', link, text],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error(f'write_post: {e}')


def _gs_read_recent(ss, label):
    try:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=3)
        texts = set()
        for r in ss.worksheet('Посты').get_all_values()[1:]:
            if len(r) < 7:
                continue
            try:
                dt = datetime.strptime(r[0].strip(), '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                if dt >= cutoff:
                    texts.add(' '.join(r[6].lower().split()))
            except ValueError:
                pass
        log.info(f'[{label}] dedup: {len(texts)} posts')
        return texts
    except Exception as e:
        log.error(f'[{label}] read_recent: {e}')
        return set()


def _gs_log(ss, label, level, msg):
    try:
        ss.worksheet('Логи').append_row(
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, str(msg)],
            value_input_option='USER_ENTERED',
        )
    except Exception:
        pass


# ── utils ──────────────────────────────────────────────────────────────────────

def _parse_username(raw):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([A-Za-z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^-?100\d+$', raw) or re.match(r'^-?\d+$', raw):
        return raw
    if re.match(r'^[A-Za-z0-9_]+$', raw):
        return raw
    return None


def _assign_account(uname: str, n: int) -> int:
    """Стабильно привязывает канал к аккаунту по hash(username)."""
    return int(hashlib.md5(uname.encode()).hexdigest(), 16) % n


def _should_send(text, cfg):
    if cfg['kw_on']:
        if not text.strip():
            return False
        lo = text.lower()
        if not any(k.lower() in lo for k in cfg['kws'] if k):
            return False
    if cfg['neg_on']:
        lo = text.lower()
        if any(n.lower() in lo for n in cfg['negs'] if n):
            return False
    return True


def _make_link(uname, entity_id, msg_id):
    if re.match(r'^-?100\d+$', str(uname)):
        return f'https://t.me/c/{entity_id}/{msg_id}'
    return f'https://t.me/{uname}/{msg_id}'


# ── telegram send ──────────────────────────────────────────────────────────────

def _tg_text(token, chats, text):
    for chat in chats:
        try:
            data = json.dumps({
                'chat_id': chat,
                'text': text[:4096],
                'disable_web_page_preview': False,
            }).encode()
            req = urllib.request.Request(
                f'https://api.telegram.org/bot{token}/sendMessage', data=data,
                headers={'Content-Type': 'application/json'})
            urllib.request.urlopen(req, timeout=10)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'tg_text {chat}: {e}')


def _tg_photo(token, chats, caption, photo: bytes):
    for chat in chats:
        try:
            boundary = 'B' + str(int(time.time()))
            parts = [
                f'--{boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n{chat}'.encode(),
                f'--{boundary}\r\nContent-Disposition: form-data; name="caption"\r\n\r\n{caption[:1024]}'.encode(),
                (f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; '
                 f'filename="p.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode() + photo,
                f'--{boundary}--'.encode(),
            ]
            body = b'\r\n'.join(parts)
            req = urllib.request.Request(
                f'https://api.telegram.org/bot{token}/sendPhoto', data=body,
                headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
            urllib.request.urlopen(req, timeout=30)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'tg_photo {chat}: {e}')
            _tg_text(token, [chat], caption)


def _tg_album(token, chats, caption, photos: list):
    if not photos:
        _tg_text(token, chats, caption)
        return
    photos = photos[:10]
    for chat in chats:
        try:
            media = []
            for i in range(len(photos)):
                item = {'type': 'photo', 'media': f'attach://p{i}'}
                if i == 0:
                    item['caption'] = caption[:1024]
                media.append(item)
            boundary = 'B' + str(int(time.time()))
            parts = [
                f'--{boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n{chat}'.encode(),
                (f'--{boundary}\r\nContent-Disposition: form-data; name="media"\r\n'
                 f'Content-Type: application/json\r\n\r\n{json.dumps(media)}').encode(),
            ]
            for i, pb in enumerate(photos):
                parts.append(
                    (f'--{boundary}\r\nContent-Disposition: form-data; name="p{i}"; '
                     f'filename="p{i}.jpg"\r\nContent-Type: image/jpeg\r\n\r\n').encode() + pb
                )
            parts.append(f'--{boundary}--'.encode())
            body = b'\r\n'.join(parts)
            req = urllib.request.Request(
                f'https://api.telegram.org/bot{token}/sendMediaGroup', data=body,
                headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
            urllib.request.urlopen(req, timeout=30)
            time.sleep(0.3)
        except Exception as e:
            log.error(f'tg_album {chat}: {e}')
            _tg_text(token, [chat], caption)


# ── safe TG call ───────────────────────────────────────────────────────────────

async def _tg_call(fn, *args, label='', **kwargs):
    """FloodWait <= 120s: ждёт и повторяет. > 120s: возвращает None."""
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except FloodWaitError as e:
            if e.seconds > 120:
                log.warning(f'[{label}] FloodWait {e.seconds}s > 120 — skip channel')
                return None
            log.warning(f'[{label}] FloodWait {e.seconds}s — waiting...')
            await asyncio.sleep(e.seconds + 3)
        except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
            log.warning(f'[{label}] unavailable: {e}')
            return None
        except Exception as e:
            log.error(f'[{label}] error attempt {attempt + 1}: {e}')
            if attempt < 2:
                await asyncio.sleep(5)
    return None


# ── account worker ─────────────────────────────────────────────────────────────

async def run_account(account: dict, acc_idx: int, channel_list: list,
                      ss_list, settings_list, state_list, cache_list,
                      dedup_list, cache_dirty):
    """
    channel_list: [(raw_str, ss_idx), ...]
    cache_dirty: set() — ss_idx'ы у которых cache был обновлён (нужно записать)
    """
    label = f'ACC{acc_idx + 1}'
    loop  = asyncio.get_event_loop()

    client = TelegramClient(
        StringSession(account['session']),
        account['api_id'],
        account['api_hash'],
    )
    try:
        await client.start()
    except Exception as e:
        log.error(f'[{label}] connect failed: {e}')
        return
    log.info(f'[{label}] connected, channels: {len(channel_list)}')

    processed = 0

    try:
        for raw, ss_idx in channel_list:
            uname = _parse_username(raw)
            if not uname:
                continue

            ss     = ss_list[ss_idx]
            cfg    = settings_list[ss_idx]
            state  = state_list[ss_idx]
            cache  = cache_list[ss_idx]   # {uname: (entity_id, chat_name)}
            dedup  = dedup_list[ss_idx]

            # ── resolve entity (из кеша или get_entity) ────────────────────
            if uname in cache:
                eid, _name = cache[uname]
                peer = int(uname) if re.match(r'^-?100\d+$', uname) else uname
            else:
                peer = int(uname) if re.match(r'^-?100\d+$', uname) else uname
                entity = await _tg_call(client.get_entity, peer, label=label)
                if entity is None:
                    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    continue
                eid = abs(entity.id)
                chat_name = getattr(entity, 'title', uname)
                cache[uname] = (eid, chat_name)
                cache_dirty.add(ss_idx)
                log.info(f'[{label}] [{uname}] entity resolved: {eid}')
                await asyncio.sleep(2.0)

            known_last = state.get(uname, 0)

            messages = await _tg_call(client.get_messages, peer,
                                      limit=MSG_LIMIT, label=label)
            if not messages:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                continue

            # ── первый запуск — запомнить last_id, ничего не слать ─────────
            if known_last == 0:
                state[uname] = messages[0].id
                log.info(f'[{label}] [{uname}] first run, last_id={messages[0].id}')
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                processed += 1
                continue

            new_msgs = sorted(
                [m for m in messages if m.id > known_last and m.action is None],
                key=lambda m: m.id,
            )
            if not new_msgs:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                processed += 1
                continue

            groups: dict[int, list] = {}
            singles = []
            for m in new_msgs:
                gid = getattr(m, 'grouped_id', None)
                if gid:
                    groups.setdefault(gid, []).append(m)
                else:
                    singles.append(m)

            token = cfg['token']
            dest  = cfg['chats']

            # ── одиночные сообщения ────────────────────────────────────────
            for m in singles:
                text = m.text or m.message or ''
                if hasattr(m, 'caption') and m.caption:
                    text = m.caption
                text = ' '.join(text.split())

                if not _should_send(text, cfg):
                    continue
                norm = ' '.join(text.lower().split())
                if norm in dedup:
                    continue

                link = _make_link(uname, eid, m.id)
                body = f'📢 {uname}\n\n{text}\n\n🔗 {link}'

                photos = []
                if m.photo:
                    try:
                        buf = io.BytesIO()
                        await client.download_media(m, file=buf)
                        photos = [buf.getvalue()]
                    except Exception:
                        pass

                if token and dest:
                    if photos:
                        await loop.run_in_executor(
                            pool, _tg_photo, token, dest, body, photos[0])
                    else:
                        await loop.run_in_executor(
                            pool, _tg_text, token, dest, body)

                await loop.run_in_executor(
                    pool, _gs_write_post, ss,
                    m.date.replace(tzinfo=None), uname, link, text)
                dedup.add(norm)
                log.info(f'[{label}] sent {uname} {link}')

            # ── альбомы ───────────────────────────────────────────────────
            for gid, msgs in groups.items():
                msgs = sorted(msgs, key=lambda m: m.id)
                text = ''
                for m in msgs:
                    t = m.text or m.message or ''
                    if hasattr(m, 'caption') and m.caption:
                        t = m.caption
                    t = ' '.join(t.split())
                    if t:
                        text = t
                        break

                if not _should_send(text, cfg):
                    continue
                norm = ' '.join(text.lower().split())
                if norm in dedup:
                    continue

                link = _make_link(uname, eid, msgs[0].id)
                body = f'📢 {uname}\n\n{text}\n\n🔗 {link}'

                photos = []
                for m in msgs:
                    if m.photo or m.document:
                        try:
                            buf = io.BytesIO()
                            await client.download_media(m, file=buf)
                            photos.append(buf.getvalue())
                        except Exception:
                            pass

                if token and dest:
                    if photos:
                        await loop.run_in_executor(
                            pool, _tg_album, token, dest, body, photos)
                    else:
                        await loop.run_in_executor(
                            pool, _tg_text, token, dest, body)

                post_text = text + (f' [photo:{len(photos)}]' if photos else '')
                await loop.run_in_executor(
                    pool, _gs_write_post, ss,
                    msgs[0].date.replace(tzinfo=None), uname, link, post_text)
                dedup.add(norm)
                log.info(f'[{label}] sent {uname} album({len(photos)}) {link}')

            state[uname] = new_msgs[-1].id
            processed += 1

            # ── периодическое сохранение state ────────────────────────────
            if processed % STATE_SAVE_INTERVAL == 0:
                for i, ss_ in enumerate(ss_list):
                    lbl_ = f'SS{i + 1}'
                    await loop.run_in_executor(
                        pool, _gs_write_state, ss_, lbl_, state_list[i])
                log.info(f'[{label}] periodic state save at {processed} channels')

            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    finally:
        await client.disconnect()
        log.info(f'[{label}] disconnected')


# ── main ───────────────────────────────────────────────────────────────────────

async def main():
    if not ACCOUNTS:
        log.error('No accounts. Set TG_API_ID_1/TG_API_HASH_1/TG_SESSION_1 etc.')
        return
    if not SS_IDS:
        log.error('SPREADSHEET_IDS not set')
        return

    n = len(ACCOUNTS)
    log.info(f'accounts: {n}, spreadsheets: {len(SS_IDS)}')
    loop = asyncio.get_event_loop()

    ss_list, settings_list, state_list, cache_list, dedup_list = [], [], [], [], []
    all_channels = []  # [(raw_str, ss_idx)]

    for i, ss_id in enumerate(SS_IDS):
        lbl = f'SS{i + 1}'
        try:
            ss = await loop.run_in_executor(pool, _gs_open, ss_id)
        except Exception as e:
            log.error(f'[{lbl}] open failed: {e}')
            continue

        cfg = await loop.run_in_executor(pool, _gs_settings, ss, lbl)
        if not cfg:
            continue

        state = await loop.run_in_executor(pool, _gs_read_state, ss, lbl)
        cache = await loop.run_in_executor(pool, _gs_read_cache, ss, lbl)
        # cache: {uname: entity_id} → конвертируем в {uname: (entity_id, '')}
        cache = {u: (eid, '') for u, eid in cache.items()}
        dedup = await loop.run_in_executor(pool, _gs_read_recent, ss, lbl)
        raws  = await loop.run_in_executor(pool, _gs_channels, ss, lbl)

        si = len(ss_list)
        ss_list.append(ss)
        settings_list.append(cfg)
        state_list.append(state)
        cache_list.append(cache)
        dedup_list.append(dedup)

        for raw in raws:
            all_channels.append((raw, si))

    if not all_channels:
        log.error('no channels')
        return

    log.info(f'total channels: {len(all_channels)}')

    # ── статичное распределение по hash(username) ─────────────────────────
    chunks = [[] for _ in range(n)]
    for raw, ss_idx in all_channels:
        uname = _parse_username(raw)
        if uname:
            acc = _assign_account(uname, n)
            chunks[acc].append((raw, ss_idx))

    for i, chunk in enumerate(chunks):
        log.info(f'ACC{i + 1}: {len(chunk)} channels')

    cache_dirty = set()  # ss_idx'ы с обновлённым кешем

    # ── параллельный запуск аккаунтов ──────────────────────────────────────
    await asyncio.gather(*[
        run_account(
            account=ACCOUNTS[i],
            acc_idx=i,
            channel_list=chunks[i],
            ss_list=ss_list,
            settings_list=settings_list,
            state_list=state_list,
            cache_list=cache_list,
            dedup_list=dedup_list,
            cache_dirty=cache_dirty,
        )
        for i in range(n)
    ])

    # ── финальное сохранение state и кеша ────────────────────────────────
    for i, ss in enumerate(ss_list):
        lbl = f'SS{i + 1}'
        await loop.run_in_executor(pool, _gs_write_state, ss, lbl, state_list[i])
        if i in cache_dirty:
            # конвертируем обратно для записи
            cache_to_write = {u: (eid, name) for u, (eid, name) in cache_list[i].items()}
            await loop.run_in_executor(pool, _gs_write_cache, ss, lbl, cache_to_write)
        await loop.run_in_executor(pool, _gs_log, ss, lbl, 'INFO',
                                   f'done | каналов: {len(all_channels)}')

    log.info('all done')


if __name__ == '__main__':
    asyncio.run(main())
