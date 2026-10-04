# -*- coding: utf-8 -*-
"""
BOLT — однофайловый Telegram-бот на Turso с транзакционными pipeline-запросами.

Переменные окружения:
    BOT_TOKEN    — токен бота от @BotFather       (обязательно)
    OWNER_ID     — Telegram ID владельца          (обязательно)
    TURSO_URL    — URL базы Turso                 (обязательно)
    TURSO_TOKEN  — токен доступа Turso            (обязательно)

ВАЖНО: upsert пользователя НИКОГДА не трогает balance и last_bonus.
"""
import importlib
import os
import subprocess
import sys

REQUIRED = [
    ("aiogram", "aiogram>=3.7,<4"),
    ("requests", "requests"),
]


def _ensure_deps():
    for mod, spec in REQUIRED:
        try:
            importlib.import_module(mod)
            continue
        except ImportError:
            pass
        base = [sys.executable, "-m", "pip", "install", "--quiet",
                "--disable-pip-version-check", spec]
        for extra in ([], ["--break-system-packages"], ["--user"]):
            if subprocess.call(base + extra) == 0:
                break
        else:
            raise SystemExit(f"Не удалось установить зависимость: {spec}")
        importlib.invalidate_caches()
        importlib.import_module(mod)


_ensure_deps()

import asyncio  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import re  # noqa: E402
import secrets  # noqa: E402
import time  # noqa: E402
from datetime import date, datetime, timedelta, timezone  # noqa: E402
from html import escape as esc  # noqa: E402

import requests  # noqa: E402

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router  # noqa: E402
from aiogram.client.default import DefaultBotProperties  # noqa: E402
from aiogram.enums import ChatType, ParseMode  # noqa: E402
from aiogram.exceptions import TelegramBadRequest  # noqa: E402
from aiogram.filters import Command, CommandObject  # noqa: E402
from aiogram.types import (  # noqa: E402
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup, User,
)

# ───────────────────────────── Конфигурация ─────────────────────────────

TOKEN = os.getenv("BOT_TOKEN", "").strip()
try:
    OWNER_ID = int(os.getenv("OWNER_ID", "0").strip() or 0)
except ValueError:
    OWNER_ID = 0
TURSO_URL = os.getenv("TURSO_URL", "").strip()
TURSO_TOKEN = os.getenv("TURSO_TOKEN", "").strip()

CUR = "BOLT"
SCALE = 100
BONUS = 500 * SCALE
BONUS_COOLDOWN = 24 * 3600
REF_PREMIUM = 1000 * SCALE
FEE_MILLE = 5
CREATOR_PART = (2, 5)
HISTORY_DAYS = 10
MIN_BET = 1 * SCALE
MSK = timezone(timedelta(hours=3))

GAME_TIMEOUT = 30 * 60
PAGE_SIZE = 10

MINES_N, MINES_COUNT, MINES_EDGE = 25, 3, 0.97
HACK_LEVELS, HACK_STEP = 5, 1.45

log = logging.getLogger("bolt")
RNG = secrets.SystemRandom()
BOT_USERNAME = ""

# ─────────────────────── Кэш meta в памяти (только чтение) ──────────────

META_CACHE = {"creator_balance": "0", "total_burned": "0", "last_deflation_day": ""}

# ─────────────────────────────── База данных ────────────────────────────

TABLES = {
    "users": [
        ("id", "INTEGER PRIMARY KEY"),
        ("name", "TEXT NOT NULL DEFAULT ''"),
        ("username", "TEXT NOT NULL DEFAULT ''"),
        ("balance", "INTEGER NOT NULL DEFAULT 0"),
        ("last_bonus", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "referrals": [
        ("invited_id", "INTEGER PRIMARY KEY"),
        ("inviter_id", "INTEGER NOT NULL DEFAULT 0"),
        ("rewarded", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "checks": [
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("code", "TEXT NOT NULL DEFAULT ''"),
        ("creator_id", "INTEGER NOT NULL DEFAULT 0"),
        ("amount", "INTEGER NOT NULL DEFAULT 0"),
        ("total", "INTEGER NOT NULL DEFAULT 0"),
        ("used", "INTEGER NOT NULL DEFAULT 0"),
        ("status", "TEXT NOT NULL DEFAULT 'active'"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "check_claims": [
        ("check_id", "INTEGER NOT NULL DEFAULT 0"),
        ("user_id", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "transfers": [
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("from_id", "INTEGER NOT NULL DEFAULT 0"),
        ("to_id", "INTEGER NOT NULL DEFAULT 0"),
        ("amount", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "deflation_history": [
        ("day", "TEXT PRIMARY KEY"),
        ("supply", "INTEGER NOT NULL DEFAULT 0"),
        ("burned", "INTEGER NOT NULL DEFAULT 0"),
        ("creator", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "games": [
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("user_id", "INTEGER NOT NULL DEFAULT 0"),
        ("chat_id", "INTEGER NOT NULL DEFAULT 0"),
        ("kind", "TEXT NOT NULL DEFAULT ''"),
        ("bet", "INTEGER NOT NULL DEFAULT 0"),
        ("data", "TEXT NOT NULL DEFAULT '{}'"),
        ("state", "TEXT NOT NULL DEFAULT 'active'"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "pending": [
        ("user_id", "INTEGER PRIMARY KEY"),
        ("kind", "TEXT NOT NULL DEFAULT ''"),
        ("data", "TEXT NOT NULL DEFAULT ''"),
        ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "meta": [
        ("key", "TEXT PRIMARY KEY"),
        ("value", "TEXT NOT NULL DEFAULT ''"),
    ],
}
INDEXES = [
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_checks_code ON checks(code)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_claims ON check_claims(check_id, user_id)",
    "CREATE INDEX IF NOT EXISTS ix_checks_creator ON checks(creator_id)",
    "CREATE INDEX IF NOT EXISTS ix_ref_inviter ON referrals(inviter_id)",
    "CREATE INDEX IF NOT EXISTS ix_games_user ON games(user_id)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_from ON transfers(from_id)",
    "CREATE INDEX IF NOT EXISTS ix_transfers_to ON transfers(to_id)",
]


def _to_int(v, default=0):
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        try:
            return int(float(str(v).strip()))
        except (TypeError, ValueError):
            return default


class Row(dict):
    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._values = list(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


class Cursor:
    def __init__(self, rows, columns, lastrowid=None):
        self._rows = list(rows)
        self._columns = list(columns)
        self._idx = 0
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            return Row(self._columns, row)
        return None

    def fetchall(self):
        result = [Row(self._columns, r) for r in self._rows[self._idx:]]
        self._idx = len(self._rows)
        return result

    def __len__(self):
        return len(self._rows) - self._idx


def _normalize_url(u):
    u = u.strip().rstrip("/")
    if u.startswith("libsql://"):
        u = "https://" + u[len("libsql://"):]
    if u.startswith("turso://"):
        u = "https://" + u[len("turso://"):]
    if not u.startswith("http"):
        u = "https://" + u
    return u + "/v2/pipeline"


class TursoClient:
    def __init__(self, url, token):
        self.endpoint = _normalize_url(url)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    @staticmethod
    def _arg(v):
        if v is None:
            return {"type": "null"}
        if isinstance(v, bool):
            return {"type": "integer", "value": "1" if v else "0"}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "float", "value": v}
        return {"type": "text", "value": str(v)}

    @staticmethod
    def _cell(c):
        t = c.get("type")
        v = c.get("value")
        if t == "null" or v is None:
            return None
        if t in ("integer", "float"):
            try:
                if t == "integer":
                    return int(v)
                return float(v)
            except (TypeError, ValueError):
                pass
        if isinstance(v, str):
            s = v.strip()
            if s.lstrip("-").isdigit():
                try:
                    return int(s)
                except ValueError:
                    pass
            try:
                return float(s)
            except ValueError:
                pass
        return v

    def _post(self, reqs):
        last = None
        for attempt in range(3):
            try:
                r = self.session.post(self.endpoint, json={"requests": reqs},
                                      timeout=20)
                if r.status_code != 200:
                    last = f"HTTP {r.status_code}: {r.text[:200]}"
                    time.sleep(0.3 * (attempt + 1))
                    continue
                return r.json()
            except Exception as e:
                last = str(e)
                time.sleep(0.3 * (attempt + 1))
        raise RuntimeError(f"Turso error: {last}")

    @staticmethod
    def _parse_result(res):
        if res.get("type") == "error":
            raise RuntimeError(f"Turso query error: {res}")
        resp = res.get("response", {}).get("result", {})
        cols = [c.get("name") for c in resp.get("cols", [])]
        rows = [tuple(TursoClient._cell(c) for c in row)
                for row in resp.get("rows", [])]
        lastrowid = None
        lri = resp.get("last_insert_rowid")
        if lri is not None:
            try:
                lastrowid = int(lri)
            except (TypeError, ValueError):
                lastrowid = None
        return Cursor(rows, cols, lastrowid)

    def _sync_execute(self, sql, params=None):
        upper = sql.strip().upper()
        if upper in ("BEGIN", "BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"):
            return Cursor([], [])
        stmt = {"sql": sql, "args": [self._arg(p) for p in (params or [])]}
        body = self._post([{"type": "execute", "stmt": stmt}, {"type": "close"}])
        results = body.get("results", [])
        if not results:
            return Cursor([], [])
        return self._parse_result(results[0])

    def _sync_tx(self, stmts):
        """BEGIN + все запросы + COMMIT в одном HTTP-вызове."""
        reqs = [{"type": "execute", "stmt": {"sql": "BEGIN", "args": []}}]
        for sql, params in stmts:
            reqs.append({
                "type": "execute",
                "stmt": {"sql": sql,
                         "args": [self._arg(p) for p in (params or [])]},
            })
        reqs.append({"type": "execute", "stmt": {"sql": "COMMIT", "args": []}})
        reqs.append({"type": "close"})
        body = self._post(reqs)
        results = body.get("results", [])
        out = []
        for res in results:
            if res.get("type") in ("close",):
                continue
            if res.get("type") == "error":
                log.error("Tx error: %s", res)
                return out
            if res.get("type") == "execute":
                out.append(self._parse_result(res))
        return out

    async def execute(self, sql, params=None):
        return await asyncio.to_thread(self._sync_execute, sql, params)

    async def tx(self, stmts):
        return await asyncio.to_thread(self._sync_tx, stmts)


DB: TursoClient = None  # type: ignore


async def init_db():
    global DB
    if not (TURSO_URL and TURSO_TOKEN):
        raise SystemExit("TURSO_URL и TURSO_TOKEN обязательны")
    DB = TursoClient(TURSO_URL, TURSO_TOKEN)
    ddl_stmts = []
    for table, cols in TABLES.items():
        ddl = ", ".join(f"{c} {d}" for c, d in cols)
        ddl_stmts.append((f"CREATE TABLE IF NOT EXISTS {table} ({ddl})", None))
    for sql in INDEXES:
        ddl_stmts.append((sql, None))
    ddl_stmts.append(
        ("INSERT OR IGNORE INTO meta(key, value) VALUES('creator_balance', '0')", None))
    ddl_stmts.append(
        ("INSERT OR IGNORE INTO meta(key, value) VALUES('total_burned', '0')", None))
    ddl_stmts.append(
        ("INSERT OR IGNORE INTO meta(key, value) VALUES('last_deflation_day', '')", None))
    for sql, params in ddl_stmts:
        try:
            await DB.execute(sql, params)
        except Exception:
            log.exception("ddl failed: %s", sql)
    try:
        rows = (await DB.execute("SELECT key, value FROM meta")).fetchall()
        for r in rows:
            META_CACHE[str(r["key"])] = str(r["value"] or "0")
    except Exception:
        log.exception("meta cache load failed")
    log.info("Turso готов, meta cache: %s", META_CACHE)


async def meta_get(key, default="0"):
    if key in META_CACHE and META_CACHE[key] != "":
        return META_CACHE[key]
    r = (await DB.execute("SELECT value FROM meta WHERE key=?", (key,))).fetchone()
    val = r["value"] if r else default
    META_CACHE[key] = str(val)
    return val


async def meta_set(key, value):
    META_CACHE[key] = str(value)
    await DB.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)",
                     (key, str(value)))


async def upsert_user(u: User) -> bool:
    """БЕЗОПАСНЫЙ upsert. Никогда не трогает balance и last_bonus.

    Возвращает True, если пользователь создан впервые.
    """
    name = (u.full_name or "").strip()
    uname = u.username or ""
    now = int(time.time())
    r = (await DB.execute("SELECT 1 FROM users WHERE id=?", (u.id,))).fetchone()
    if r:
        # Юзер есть — обновляем только name/username, остальное не трогаем
        await DB.execute(
            "UPDATE users SET name=?, username=? WHERE id=?",
            (name, uname, u.id))
        return False
    # Новый юзер — создаём с нуля
    await DB.execute(
        "INSERT INTO users(id, name, username, balance, last_bonus, created_at) "
        "VALUES(?,?,?,0,0,?)",
        (u.id, name, uname, now))
    return True


async def get_user(uid):
    return (await DB.execute("SELECT * FROM users WHERE id=?", (uid,))).fetchone()


async def balance_of(uid):
    r = await get_user(uid)
    if not r:
        return 0
    return _to_int(r["balance"])


async def pend_set(uid, kind, data=""):
    await DB.execute(
        "INSERT OR REPLACE INTO pending(user_id, kind, data, created_at) VALUES(?,?,?,?)",
        (uid, kind, data, int(time.time())))


async def pend_get(uid):
    r = (await DB.execute("SELECT * FROM pending WHERE user_id=?", (uid,))).fetchone()
    if r and time.time() - _to_int(r["created_at"]) > 600:
        await pend_clear(uid)
        return None
    return r


async def pend_clear(uid):
    await DB.execute("DELETE FROM pending WHERE user_id=?", (uid,))


# ──────────────────────────────── Утилиты ───────────────────────────────

AMT_RE = re.compile(r"^\d{1,10}(?:[.,]\d{1,2})?$")


def parse_amount(s):
    if not s or not AMT_RE.match(s):
        return None
    whole, _, frac = s.replace(",", ".").partition(".")
    v = int(whole) * SCALE + int((frac + "00")[:2])
    return v if v > 0 else None


def fmt(c):
    c = _to_int(c)
    w, f = divmod(c, SCALE)
    s = f"{w:,}".replace(",", "\u00a0")
    return s + (f",{f:02d}" if f else "")


def money(c):
    return f"{fmt(c)} {CUR}"


def q(*lines):
    return "<blockquote>" + "\n".join(lines) + "</blockquote>"


def fmt_left(sec):
    sec = max(int(sec), 60)
    h, m = divmod(sec // 60, 60)
    return f"{h} ч {m} мин" if h else f"{m} мин"


def mention(uid, name):
    return f'<a href="tg://user?id={uid}">{esc(name or "Пользователь")}</a>'


def ikb(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows])


def is_owner(uid):
    return OWNER_ID != 0 and uid == OWNER_ID


async def say(m: Message, text, kb=None):
    if m.chat.type == ChatType.PRIVATE:
        return await m.answer(text, reply_markup=kb)
    return await m.reply(text, reply_markup=kb)


async def edit(cb: CallbackQuery, text, kb=None):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            log.warning("edit failed: %s", e)


# ───────────────────────────── Reply-клавиатуры ─────────────────────────

L_ACC, L_BAL, L_PAR = "Аккаунт", "Счет", "Партнеры"
L_ECO, L_BON, L_MAN = "Экономика", "Бонус", "Мануал"


def main_kb():
    rows = [
        [KeyboardButton(text=L_ACC), KeyboardButton(text=L_PAR)],
        [KeyboardButton(text=L_BAL), KeyboardButton(text=L_ECO)],
        [KeyboardButton(text=L_BON), KeyboardButton(text=L_MAN)],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


# ───────────────────────────────── Тексты ───────────────────────────────

def manual_menu_text():
    return "<b>📖 Мануал</b>\n\nВыбери раздел:"


def manual_games_text():
    return "<b>🎮 Игры</b>\n\nВыбери игру, чтобы посмотреть правила:"


def mines_text():
    return (
        "<b>💣 Мины</b>\n\n"
        "Поле 5×5, спрятано 3 мины. Открывай ячейки — каждая безопасная "
        "повышает множитель. Забери выигрыш в любой момент, пока не попал "
        "на мину.\n\n"
        "<b>Множитель</b>\n"
        f"• Растёт до x{mines_mult(MINES_N - MINES_COUNT):.0f}\n"
        "• Ставка списывается сразу\n"
        "• Проигранная ставка сгорает\n"
        "• Бездействие 30 минут — ставка возвращается\n\n"
        "<b>Запуск</b>\n"
        "• <code>Мины {сумма}</code>"
    )


def hack_text():
    return (
        "<b>🔓 Взлом</b>\n\n"
        "5 уровней защиты. На каждом выбери один из трёх узлов — в одном "
        "спрятана ловушка. Каждый пройденный уровень умножает ставку. "
        "Забрать выигрыш можно после любого уровня.\n\n"
        "<b>Множитель</b>\n"
        f"• +x{HACK_STEP} за каждый уровень\n"
        f"• Максимум x{HACK_STEP ** HACK_LEVELS:.2f}\n"
        "• Ставка списывается сразу\n"
        "• Бездействие 30 минут — ставка возвращается\n\n"
        "<b>Запуск</b>\n"
        "• <code>Взлом {сумма}</code>"
    )


def commands_text():
    return (
        "<b>💸 Переводы</b>\n"
        "• <code>П {сумма}</code> — ответом на сообщение\n"
        "• <code>П {ID} {сумма}</code> — по ID пользователя\n\n"
        "<b>🎮 Игры</b>\n"
        "• <code>Игры</code> — список всех игр и гайды\n"
        "• <code>Мины {сумма}</code> — игра Мины\n"
        "• <code>Взлом {сумма}</code> — игра Взлом\n\n"
        "<b>👤 Профиль</b>\n"
        "• <code>Аккаунт</code> — твой профиль\n"
        "• <code>Счет</code> — текущий и создательский счёт\n"
        "• <code>Бонус</code> — ежедневный бонус\n\n"
        "<b>💬 В чатах</b>\n"
        "• <code>счет</code> — показать свой баланс\n"
        "• <code>мой счет</code> — показать свой баланс"
    )


def policy_text():
    return (
        "<b>📋 Правила использования бота</b>\n\n"
        "1. Используя бота, ты подтверждаешь согласие с данными правилами.\n\n"
        "2. Бот обрабатывает только Telegram ID, имя и username для сохранения "
        "прогресса, статистики и рейтингов.\n\n"
        "3. Администрация не передаёт пользовательские данные третьим лицам "
        "и использует их только для работы бота.\n\n"
        "4. Запрещено использование читов, скриптов, багов и любого стороннего ПО "
        "для получения преимущества.\n\n"
        "5. Купля, продажа и передача игровой валюты между пользователями "
        "запрещена.\n\n"
        "6. За нарушение правил администрация вправе обнулить баланс или "
        "заблокировать аккаунт без возможности восстановления.\n\n"
        "7. Администрация вправе изменять правила без предварительного уведомления."
    )


def burn_text():
    return (
        "<b>🔥 Сжигание валюты</b>\n\n"
        "Каждый день в 00:00 (МСК) со всех счетов списывается "
        "ровно 0,5% от суммы валюты. Это и есть дефляция.\n\n"
        "Списанные 0,5% сгорают безвозвратно — их больше нельзя получить "
        "или вернуть.\n\n"
        "<b>Зачем это нужно</b>\n"
        f"• Сгорающая валюта уменьшает общий объём и поддерживает ценность {CUR}.\n"
        "• Дефляция стимулирует активность: бонусы, игры и чеки позволяют "
        "не терять накопленное."
    )


# ─────────────────────────────── Экраны (views) ─────────────────────────

async def view_account(uid):
    u = await get_user(uid)
    uname = f"@{esc(u['username'])}" if u["username"] else "—"
    return q(
        f"ID: <code>{uid}</code>",
        f"Name: {esc(u['name'] or '—')}",
        f"Username: {uname}")


async def view_balance(uid):
    if is_owner(uid):
        res = await DB.tx([
            ("SELECT balance FROM users WHERE id=?", (uid,)),
        ])
        bal = 0
        if res and res[0] and len(res[0]) > 0:
            row = res[0].fetchone()
            bal = _to_int(row["balance"]) if row else 0
        cb = _to_int(META_CACHE.get("creator_balance", "0"))
        lines = [f"Текущий счет: {money(bal)}",
                 f"Создательский счет: {money(cb)}"]
    else:
        r = (await DB.execute("SELECT balance FROM users WHERE id=?", (uid,))).fetchone()
        bal = _to_int(r["balance"]) if r else 0
        lines = [f"Текущий счет: {money(bal)}"]
    text = q(*lines)
    kb = [[("Чеки", "nav:chk"), ("История", "nav:hist")]]
    if is_owner(uid):
        kb.append([("Вывести", "nav:wd")])
    return text, ikb(*kb)


async def view_history(uid, page=0):
    r = (await DB.execute(
        "SELECT COUNT(*) AS c FROM transfers WHERE from_id=? OR to_id=?",
        (uid, uid))).fetchone()
    total = _to_int(r["c"]) if r else 0
    if total == 0:
        return q("Переводов пока не было."), ikb([("← Счет", "nav:bal")])
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, pages - 1))

    rows = (await DB.execute(
        "SELECT t.amount, t.created_at, "
        "  CASE WHEN t.from_id = ? THEN t.to_id ELSE t.from_id END AS other_id, "
        "  CASE WHEN t.from_id = ? THEN 1 ELSE 0 END AS outgoing "
        "FROM transfers t WHERE t.from_id = ? OR t.to_id = ? "
        "ORDER BY t.id DESC LIMIT ? OFFSET ?",
        (uid, uid, uid, uid, PAGE_SIZE, page * PAGE_SIZE))).fetchall()

    other_ids = list({_to_int(r["other_id"]) for r in rows if r["other_id"]})
    names = {}
    if other_ids:
        placeholders = ",".join(["?"] * len(other_ids))
        urows = (await DB.execute(
            f"SELECT id, name FROM users WHERE id IN ({placeholders})",
            tuple(other_ids))).fetchall()
        for u in urows:
            names[_to_int(u["id"])] = u["name"] or "—"

    lines = []
    for tr in rows:
        dt = datetime.fromtimestamp(_to_int(tr["created_at"]), MSK).strftime("%d.%m.%Y %H:%M")
        amount = _to_int(tr["amount"])
        who = esc(names.get(_to_int(tr["other_id"]), "—"))
        if _to_int(tr["outgoing"]):
            lines.append(f"⬆️ −{money(amount)} → {who}\n     <i>{dt}</i>")
        else:
            lines.append(f"⬇️ +{money(amount)} ← {who}\n     <i>{dt}</i>")
    text = q(*lines) + f"\n<i>Страница {page + 1} / {pages} · всего {total}</i>"
    nav = []
    if page > 0:
        nav.append(("← Назад", f"hist:{page - 1}"))
    if page < pages - 1:
        nav.append(("Вперёд →", f"hist:{page + 1}"))
    rows_kb = []
    if nav:
        rows_kb.append(nav)
    rows_kb.append([("← Счет", "nav:bal")])
    return text, ikb(*rows_kb)


def check_link(code):
    return f"https://t.me/{BOT_USERNAME}?start=c_{code}"


async def view_checks(uid):
    rows = (await DB.execute(
        "SELECT * FROM checks WHERE creator_id=? AND status='active' "
        "ORDER BY id DESC LIMIT 5", (uid,))).fetchall()
    text = q("Создай чек и получи ссылку. Каждый, кто её откроет, получит валюту.")
    kb = [[("Создать", "nav:new")]]
    for c in rows:
        amount = _to_int(c["amount"])
        total = _to_int(c["total"])
        used = _to_int(c["used"])
        text += ("\n\n" + q(
            f"#{c['id']} · {money(amount)} · "
            f"осталось {total - used} из {total}")
            + f"\n<code>{check_link(c['code'])}</code>")
        kb.append([(f"Отозвать #{c['id']}", f"chk:rev:{c['id']}")])
    kb.append([("← Счет", "nav:bal")])
    return text, ikb(*kb)


async def view_partners(uid):
    r = (await DB.execute("SELECT COUNT(*) AS c FROM referrals WHERE inviter_id=?",
                          (uid,))).fetchone()
    n = _to_int(r["c"]) if r else 0
    return (
        "Твоя партнёрская ссылка:\n"
        f"<code>https://t.me/{BOT_USERNAME}?start=r_{uid}</code>\n\n"
        + q(f"Ты пригласил: {n}",
            f"Премия: {money(REF_PREMIUM)}")
        + "\n"
        + q("Чтобы получить премию, приглашённый должен быть новым "
            "пользователем в боте и получить Бонус."))


async def view_bonus(uid):
    u = await get_user(uid)
    last_bonus = _to_int(u["last_bonus"]) if u else 0
    left = last_bonus + BONUS_COOLDOWN - time.time()
    if left > 0:
        return q(f"+{money(BONUS)} каждые 24 часа",
                 f"Следующий бонус через {fmt_left(left)}"), None
    return q(f"+{money(BONUS)} каждые 24 часа",
             "Бонус доступен"), \
        ikb([("Активировать", "bonus:claim")])


async def view_economy():
    res = await DB.tx([
        ("SELECT COALESCE(SUM(balance),0) AS s FROM users", None),
        ("SELECT day, burned FROM deflation_history ORDER BY day DESC LIMIT ?",
         (HISTORY_DAYS,)),
    ])
    supply = 0
    if res and res[0]:
        row = res[0].fetchone()
        supply = _to_int(row["s"]) if row else 0
    supply += _to_int(META_CACHE.get("creator_balance", "0"))
    burned_total = _to_int(META_CACHE.get("total_burned", "0"))

    hist_lines = []
    if len(res) > 1 and res[1]:
        for h in res[1].fetchall():
            d = date.fromisoformat(h["day"])
            hist_lines.append(f"{d.day:02d}.{d.month:02d} · {money(_to_int(h['burned']))}")

    text = (
        q(f"Валюты на счетах: {money(supply)}",
          f"Дефляция сожгла: {money(burned_total)}")
        + "\n"
        + q("Текущая дефляция: 0.5%")
        + f"\n<b>История дефляции · {HISTORY_DAYS} дней</b>\n"
        + q(*(hist_lines or ["Пока нет данных"]))
    )
    return text, ikb([("Сжигание валюты", "nav:burn")])


# ────────────────────────────── Дефляция ────────────────────────────────

DEFLATION_LOCK = {"day": None}


async def apply_deflation(day: str):
    if DEFLATION_LOCK["day"] == day:
        return
    last = await meta_get("last_deflation_day", "")
    if last and last >= day:
        DEFLATION_LOCK["day"] = last
        return
    cp_n, cp_d = CREATOR_PART
    res = await DB.tx([
        (f"SELECT COALESCE(SUM(balance*{FEE_MILLE}/1000),0) AS fee, "
         f"COALESCE(SUM((balance*{FEE_MILLE}/1000)*{cp_n}/{cp_d}),0) AS cre, "
         f"COALESCE(SUM(balance),0) AS s FROM users WHERE balance>0", None),
    ])
    fee_sum = cre_sum = supply = 0
    if res and res[0]:
        row = res[0].fetchone()
        if row:
            fee_sum = _to_int(row["fee"])
            cre_sum = _to_int(row["cre"])
            supply = _to_int(row["s"])
    burned = fee_sum - cre_sum
    cur_cb = _to_int(META_CACHE.get("creator_balance", "0"))
    cur_burned = _to_int(META_CACHE.get("total_burned", "0"))
    await DB.tx([
        (f"UPDATE users SET balance = balance - balance*{FEE_MILLE}/1000 WHERE balance>0",
         None),
        ("INSERT OR REPLACE INTO meta(key, value) VALUES('creator_balance', ?)",
         (str(cur_cb + cre_sum),)),
        ("INSERT OR REPLACE INTO meta(key, value) VALUES('total_burned', ?)",
         (str(cur_burned + burned),)),
        ("INSERT OR REPLACE INTO deflation_history(day, supply, burned, creator, created_at) "
         "VALUES(?,?,?,?,?)", (day, supply, burned, cre_sum, int(time.time()))),
        ("INSERT OR REPLACE INTO meta(key, value) VALUES('last_deflation_day', ?)",
         (day,)),
    ])
    META_CACHE["creator_balance"] = str(cur_cb + cre_sum)
    META_CACHE["total_burned"] = str(cur_burned + burned)
    META_CACHE["last_deflation_day"] = day
    DEFLATION_LOCK["day"] = day
    log.info("Дефляция %s: сожжено %s, создателю %s", day, burned, cre_sum)


async def deflation_loop():
    while True:
        try:
            today = datetime.now(MSK).date().isoformat()
            last = await meta_get("last_deflation_day", "")
            if not last:
                await meta_set("last_deflation_day", today)
            elif last < today:
                d = date.fromisoformat(last)
                td = date.fromisoformat(today)
                if (td - d).days > 30:
                    d = td - timedelta(days=30)
                while d < td:
                    d += timedelta(days=1)
                    await apply_deflation(d.isoformat())
        except Exception:
            log.exception("deflation failed")
        await asyncio.sleep(30)


async def idle_games_loop():
    while True:
        try:
            rows = (await DB.execute(
                "SELECT id, user_id, bet FROM games "
                "WHERE state='active' AND created_at < ?",
                (int(time.time()) - GAME_TIMEOUT,))).fetchall()
            if rows:
                stmts = []
                for g in rows:
                    stmts.append(
                        ("UPDATE games SET state='expired' WHERE id=? AND state='active'",
                         (g["id"],)))
                    stmts.append(
                        ("UPDATE users SET balance=balance+? WHERE id=?",
                         (_to_int(g["bet"]), g["user_id"])))
                if stmts:
                    await DB.tx(stmts)
        except Exception:
            log.exception("idle games failed")
        await asyncio.sleep(60)


# ──────────────────────────────── Игры ──────────────────────────────────

def mines_mult(k):
    m = MINES_EDGE
    for i in range(k):
        m *= (MINES_N - i) / (MINES_N - MINES_COUNT - i)
    return m


async def get_game(gid):
    return (await DB.execute("SELECT * FROM games WHERE id=?", (gid,))).fetchone()


async def save_game(gid, data):
    await DB.execute("UPDATE games SET data=?, created_at=? WHERE id=?",
                     (json.dumps(data), int(time.time()), gid))


async def finish_game(gid, state, data, payout, uid):
    if payout:
        await DB.tx([
            ("UPDATE games SET state=?, data=? WHERE id=? AND state='active'",
             (state, json.dumps(data), gid)),
            ("UPDATE users SET balance=balance+? WHERE id=?", (payout, uid)),
        ])
    else:
        await DB.execute(
            "UPDATE games SET state=?, data=? WHERE id=? AND state='active'",
            (state, json.dumps(data), gid))


async def mines_view(g):
    d = json.loads(g["data"])
    opened, st = d["opened"], g["state"]
    k = len(opened)
    mult = mines_mult(k) if k else 1.0
    bet = _to_int(g["bet"])
    pay = 0 if st == "lost" else int(bet * mult)
    u = await get_user(g["user_id"])
    who = esc((u["name"] if u else "") or "—")
    text = f"<b>Мины</b> · {who}\n\n" + q(
        f"Ставка: {money(bet)}",
        f"Открыто: {k} из {MINES_N - MINES_COUNT}",
        f"Множитель: x{mult if st != 'lost' else 0:.2f}",
        f"Выигрыш: {money(pay)}")
    if st == "active":
        text += f"\n<i>Следующая ячейка — x{mines_mult(k + 1):.2f}</i>"
    elif st == "cashed":
        text += f"\n💰 <b>Забрано {money(pay)}</b>"
    elif st == "expired":
        text += "\n⏳ <b>Игра истекла, ставка возвращена.</b>"
    else:
        text += "\n💥 <b>Мина.</b> Ставка сгорела."
    mines, hit = set(d["mines"]), d.get("hit")
    rows = []
    for r in range(5):
        row = []
        for c in range(5):
            i = r * 5 + c
            if st in ("lost",) and i in mines:
                t = "💥" if i == hit else "💣"
            elif i in opened:
                t = "💎"
            else:
                t = "⬜"
            row.append(InlineKeyboardButton(
                text=t, callback_data=f"m:{g['id']}:{i}" if st == "active" else "noop"))
        rows.append(row)
    if st == "active" and k:
        rows.append([InlineKeyboardButton(text=f"Забрать {money(pay)}",
                                          callback_data=f"m:{g['id']}:c")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def hack_view(g):
    d = json.loads(g["data"])
    lvl, st = d["level"], g["state"]
    mult = HACK_STEP ** lvl
    bet = _to_int(g["bet"])
    pay = 0 if st == "lost" else int(bet * mult)
    u = await get_user(g["user_id"])
    who = esc((u["name"] if u else "") or "—")
    bar = "▰" * lvl + "▱" * (HACK_LEVELS - lvl)
    text = f"<b>Взлом</b> · {who}\n\n" + q(
        f"Ставка: {money(bet)}",
        f"Защита: {lvl} / {HACK_LEVELS}  {bar}",
        f"Множитель: x{mult if st != 'lost' else 0:.2f}",
        f"Выигрыш: {money(pay)}")
    kb = None
    if st == "active":
        text += "\n<i>Выбери узел — в одном из трёх спрятана ловушка.</i>"
        rows = [[InlineKeyboardButton(text=f"{n}", callback_data=f"h:{g['id']}:{i}")
                 for i, n in enumerate("ABC")]]
        if lvl:
            rows.append([InlineKeyboardButton(text=f"Забрать {money(pay)}",
                                              callback_data=f"h:{g['id']}:c")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
    elif st == "cashed":
        text += f"\n🔓 <b>Взлом удался. Забрано {money(pay)}</b>"
    elif st == "expired":
        text += "\n⏳ <b>Игра истекла, ставка возвращена.</b>"
    else:
        text += (f"\n🚨 <b>Сработала защита.</b> Ловушка была в узле "
                 f"{'ABC'[d['traps'][lvl]]}. Ставка сгорела.")
    return text, kb


async def take_bet(uid, bet, kind, data, chat_id):
    res = await DB.tx([
        ("SELECT balance FROM users WHERE id=?", (uid,)),
    ])
    have = 0
    if res and res[0]:
        row = res[0].fetchone()
        have = _to_int(row["balance"]) if row else 0
    if have < bet:
        return None
    await DB.tx([
        ("UPDATE users SET balance=balance-? WHERE id=?", (bet, uid)),
        ("INSERT INTO games(user_id, chat_id, kind, bet, data, created_at) "
         "VALUES(?,?,?,?,?,?)",
         (uid, chat_id, kind, bet, json.dumps(data), int(time.time()))),
    ])
    r = (await DB.execute(
        "SELECT id FROM games WHERE user_id=? ORDER BY id DESC LIMIT 1",
        (uid,))).fetchone()
    return _to_int(r["id"]) if r else None


async def cmd_mines(m: Message, arg):
    bet = parse_amount(arg)
    if bet is None or bet < MIN_BET:
        if m.chat.type == ChatType.PRIVATE:
            await say(m, q("<code>Мины {сумма}</code>",
                           f"Минимальная ставка: {money(MIN_BET)}"))
        return
    uid = m.from_user.id
    data = {"mines": RNG.sample(range(MINES_N), MINES_COUNT), "opened": []}
    gid = await take_bet(uid, bet, "mines", data, m.chat.id)
    if gid is None:
        return await say(m, q("Недостаточно средств."))
    text, kb = await mines_view(await get_game(gid))
    await say(m, text, kb)


async def cmd_hack(m: Message, arg):
    bet = parse_amount(arg)
    if bet is None or bet < MIN_BET:
        if m.chat.type == ChatType.PRIVATE:
            await say(m, q("<code>Взлом {сумма}</code>",
                           f"Минимальная ставка: {money(MIN_BET)}"))
        return
    uid = m.from_user.id
    data = {"traps": [RNG.randrange(3) for _ in range(HACK_LEVELS)], "level": 0}
    gid = await take_bet(uid, bet, "hack", data, m.chat.id)
    if gid is None:
        return await say(m, q("Недостаточно средств."))
    text, kb = await hack_view(await get_game(gid))
    await say(m, text, kb)


# ──────────────────────────── Команда баланса в чате ────────────────────

async def cmd_balance_chat(m: Message, uid: int):
    r = (await DB.execute(
        "SELECT name, balance FROM users WHERE id=?", (uid,))).fetchone()
    if not r:
        return
    name = esc(r["name"] or "—")
    await say(m, q(f"{name}: {money(_to_int(r['balance']))}"))


# ─────────────────────────────── Переводы ───────────────────────────────

TRANSFER_USAGE = q(
    "<code>П {сумма}</code> — ответом на сообщение",
    "<code>П {ID} {сумма}</code> — по ID")


async def cmd_transfer(m: Message, args):
    private = m.chat.type == ChatType.PRIVATE
    sender = m.from_user
    target_id = None
    target_name = ""
    if len(args) == 1:
        amt = parse_amount(args[0])
        r = m.reply_to_message
        if amt is None or not r or not r.from_user or r.from_user.is_bot:
            if private or (amt is not None and not r):
                await say(m, TRANSFER_USAGE)
            return
        await upsert_user(r.from_user)
        target_id, target_name = r.from_user.id, r.from_user.full_name
    elif len(args) == 2 and args[0].isdigit():
        amt = parse_amount(args[1])
        if amt is None:
            if private:
                await say(m, TRANSFER_USAGE)
            return
        target_id = int(args[0])
        t = await get_user(target_id)
        if not t:
            return await say(m, q("Пользователь с таким ID не найден."))
        target_name = t["name"]
    else:
        if private:
            await say(m, TRANSFER_USAGE)
        return

    if target_id == sender.id:
        return await say(m, q("Нельзя переводить самому себе."))

    res = await DB.tx([
        ("SELECT balance FROM users WHERE id=?", (sender.id,)),
    ])
    have = 0
    if res and res[0]:
        row = res[0].fetchone()
        have = _to_int(row["balance"]) if row else 0
    if have < amt:
        return await say(m, q("Недостаточно средств."))

    await DB.tx([
        ("UPDATE users SET balance=balance-? WHERE id=?", (amt, sender.id)),
        ("UPDATE users SET balance=balance+? WHERE id=?", (amt, target_id)),
        ("INSERT INTO transfers(from_id, to_id, amount, created_at) "
         "VALUES(?,?,?,?)", (sender.id, target_id, amt, int(time.time()))),
    ])

    await say(m, "Перевод выполнен.\n\n" + q(
        f"Отправлено: {money(amt)}",
        f"Получатель: {mention(target_id, target_name)}"))
    try:
        await m.bot.send_message(
            target_id,
            "Входящий перевод.\n\n" + q(
                f"+{money(amt)} от {mention(sender.id, sender.full_name)}"))
    except Exception:
        pass


# ───────────────────────────── Чеки / вывод ─────────────────────────────

async def claim_check(m: Message, code):
    uid = m.from_user.id
    c = (await DB.execute("SELECT * FROM checks WHERE code=?", (code,))).fetchone()
    if not c:
        return await say(m, q("Чек не найден."), main_kb())
    if c["status"] == "revoked":
        return await say(m, q("Чек отозван."), main_kb())
    used = _to_int(c["used"])
    total = _to_int(c["total"])
    if c["status"] != "active" or used >= total:
        return await say(m, q("Чек уже активирован полностью."), main_kb())
    already = (await DB.execute(
        "SELECT 1 FROM check_claims WHERE check_id=? AND user_id=?",
        (c["id"], uid))).fetchone()
    if already:
        return await say(m, q("Ты уже активировал этот чек."), main_kb())

    got = _to_int(c["amount"])
    await DB.tx([
        ("INSERT INTO check_claims(check_id, user_id, created_at) VALUES(?,?,?)",
         (c["id"], uid, int(time.time()))),
        ("UPDATE checks SET used=used+?, status=CASE WHEN used+1>=total "
         "THEN 'done' ELSE status END WHERE id=?", (1, c["id"])),
        ("UPDATE users SET balance=balance+? WHERE id=?", (got, uid)),
    ])

    await say(m, "Чек активирован.\n\n" + q(
        f"Получено: +{money(got)}"), main_kb())


async def handle_pending(m: Message, p):
    uid = m.from_user.id
    txt = (m.text or "").strip()
    if p["kind"] == "check":
        t = txt.split()
        amt = parse_amount(t[0]) if len(t) == 2 else None
        cnt = int(t[1]) if len(t) == 2 and t[1].isdigit() else 0
        if amt is None or not 1 <= cnt <= 1000:
            return await say(m, q(
                "Формат: сумма на одну активацию и количество активаций.",
                "Пример: <code>100 5</code>"), ikb([("Отмена", "nav:chk")]))
        total = amt * cnt
        r = (await DB.execute("SELECT balance FROM users WHERE id=?",
                              (uid,))).fetchone()
        have = _to_int(r["balance"]) if r else 0
        if have < total:
            return await say(m, q("Недостаточно средств."),
                             ikb([("Отмена", "nav:chk")]))
        code = secrets.token_urlsafe(9)
        await DB.tx([
            ("UPDATE users SET balance=balance-? WHERE id=?", (total, uid)),
            ("INSERT INTO checks(code, creator_id, amount, total, created_at) "
             "VALUES(?,?,?,?,?)", (code, uid, amt, cnt, int(time.time()))),
        ])
        await pend_clear(uid)
        return await say(m, "Чек создан.\n\n" + q(
            f"Сумма: {money(amt)} × {cnt} активаций",
            f"Списано: {money(total)}")
            + f"\n<code>{check_link(code)}</code>", ikb([("Мои чеки", "nav:chk")]))

    if p["kind"] == "withdraw" and is_owner(uid):
        cb = _to_int(META_CACHE.get("creator_balance", "0"))
        amt = cb if txt.lower() in ("всё", "все", "all") else parse_amount(txt)
        if amt is None or amt <= 0:
            return await say(m, q("Введи сумму числом или «всё»."),
                             ikb([("Отмена", "nav:bal")]))
        if cb < amt:
            return await say(m, q(
                f"Недостаточно средств. На Создательском счете {money(cb)}."),
                ikb([("Отмена", "nav:bal")]))
        await DB.tx([
            ("INSERT OR REPLACE INTO meta(key, value) VALUES('creator_balance', ?)",
             (str(cb - amt),)),
            ("UPDATE users SET balance=balance+? WHERE id=?", (amt, uid)),
        ])
        META_CACHE["creator_balance"] = str(cb - amt)
        await pend_clear(uid)
        return await say(m, "Вывод выполнен.\n\n" + q(
            f"Переведено: {money(amt)}"), ikb([("Счет", "nav:bal")]))
    await pend_clear(uid)


# ─────────────────────────────── Хендлеры ───────────────────────────────

router = Router()


class Reg(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        data["is_new"] = False
        if u and not u.is_bot:
            try:
                data["is_new"] = await upsert_user(u)
            except Exception:
                log.exception("upsert_user failed")
        return await handler(event, data)


router.message.middleware(Reg())
router.callback_query.middleware(Reg())


@router.message(Command("start"), F.chat.type == ChatType.PRIVATE)
async def cmd_start(m: Message, command: CommandObject, is_new: bool):
    uid = m.from_user.id
    await pend_clear(uid)
    arg = (command.args or "").strip()
    if arg.startswith("c_"):
        return await claim_check(m, arg[2:])
    if arg.startswith("r_") and is_new and arg[2:].isdigit():
        inviter = int(arg[2:])
        if inviter != uid and await get_user(inviter):
            await DB.execute(
                "INSERT OR IGNORE INTO referrals(invited_id, inviter_id, created_at) "
                "VALUES(?,?,?)", (uid, inviter, int(time.time())))
    name = esc(m.from_user.first_name or "друг")
    await m.answer(f"О, привет, {name}.\n\n"
                   + q("Счет, переводы, и игры прямо в Telegram."),
                   reply_markup=main_kb())


@router.message(Command("games"))
async def cmd_games(m: Message):
    await m.answer(manual_games_text(),
                   reply_markup=ikb([("Мины", "man:miny")],
                                    [("Взлом", "man:vzlom")],
                                    [("Назад", "man:back")]))


async def menu_account(m: Message):
    await m.answer(await view_account(m.from_user.id), reply_markup=main_kb())


async def menu_balance(m: Message):
    t, k = await view_balance(m.from_user.id)
    await m.answer(t, reply_markup=k)


async def menu_partners(m: Message):
    await m.answer(await view_partners(m.from_user.id), reply_markup=main_kb())


async def menu_economy(m: Message):
    t, k = await view_economy()
    await m.answer(t, reply_markup=k)


async def menu_bonus(m: Message):
    t, k = await view_bonus(m.from_user.id)
    await m.answer(t, reply_markup=k or main_kb())


async def menu_manual(m: Message):
    await m.answer(
        manual_menu_text(),
        reply_markup=ikb(
            [("Игры", "man:games")],
            [("Команды", "man:cmd")],
            [("Политика", "man:pol")],
        ))


MENU = {}
for _label, _fn in [(L_ACC, menu_account), (L_BAL, menu_balance), (L_PAR, menu_partners),
                    (L_ECO, menu_economy), (L_BON, menu_bonus), (L_MAN, menu_manual)]:
    MENU[_label] = _fn
    MENU[_label.lower()] = _fn


@router.message(F.text)
async def on_text(m: Message):
    if not m.from_user or m.from_user.is_bot:
        return
    txt = (m.text or "").strip()
    low = txt.lower()
    parts = low.split()
    if not parts or txt.startswith("/"):
        return
    uid = m.from_user.id
    private = m.chat.type == ChatType.PRIVATE

    if private:
        fn = MENU.get(txt) or MENU.get(low)
        if fn:
            await pend_clear(uid)
            return await fn(m)

    head = parts[0]
    if head == "п" and 2 <= len(parts) <= 3:
        await pend_clear(uid)
        return await cmd_transfer(m, parts[1:])
    if head == "мины" and len(parts) == 2:
        return await cmd_mines(m, parts[1])
    if head == "взлом" and len(parts) == 2:
        return await cmd_hack(m, parts[1])
    if head == "игры" and len(parts) == 1:
        return await cmd_games(m)

    if head == "счет" and len(parts) == 1:
        return await cmd_balance_chat(m, uid)
    if len(parts) == 2 and head == "мой" and parts[1] == "счет":
        return await cmd_balance_chat(m, uid)

    if private:
        p = await pend_get(uid)
        if p:
            return await handle_pending(m, p)
        if head in ("п", "мины", "взлом", "счет"):
            return await say(m, commands_text(), main_kb())
        await m.answer("Выбери раздел в меню ниже.", reply_markup=main_kb())


# ──────────────────────────────── Callbacks ─────────────────────────────

@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@router.callback_query(F.data.startswith("man:"))
async def cb_manual(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    act = cb.data[4:]
    back = ikb([("Назад", "man:back")])
    back_games = ikb([("Назад", "man:games")])
    if act == "games":
        await edit(cb, manual_games_text(),
                   ikb([("Мины", "man:miny")],
                       [("Взлом", "man:vzlom")],
                       [("Назад", "man:back")]))
    elif act == "miny":
        await edit(cb, mines_text(), back_games)
    elif act == "vzlom":
        await edit(cb, hack_text(), back_games)
    elif act == "cmd":
        await edit(cb, commands_text(), back)
    elif act == "pol":
        await edit(cb, policy_text(), back)
    elif act == "back":
        await edit(cb, manual_menu_text(),
                   ikb([("Игры", "man:games")],
                       [("Команды", "man:cmd")],
                       [("Политика", "man:pol")]))
    await cb.answer()


@router.callback_query(F.data.startswith("hist:"))
async def cb_hist(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    try:
        page = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        return await cb.answer()
    t, k = await view_history(cb.from_user.id, page)
    await edit(cb, t, k)
    await cb.answer()


@router.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    uid, act = cb.from_user.id, cb.data[4:]
    await pend_clear(uid)
    if act == "bal":
        t, k = await view_balance(uid)
    elif act == "hist":
        t, k = await view_history(uid, 0)
    elif act == "chk":
        t, k = await view_checks(uid)
    elif act == "eco":
        t, k = await view_economy()
    elif act == "burn":
        t, k = burn_text(), ikb([("Назад", "nav:eco")])
    elif act == "new":
        await pend_set(uid, "check")
        t = q("Отправь сумму на одну активацию и количество активаций через пробел.",
              "Пример: <code>100 5</code>")
        k = ikb([("Отмена", "nav:chk")])
    elif act == "wd":
        if not is_owner(uid):
            return await cb.answer("Доступно только владельцу", show_alert=True)
        await pend_set(uid, "withdraw")
        cb_val = _to_int(META_CACHE.get("creator_balance", "0"))
        t = q(f"Создательский счет: {money(cb_val)}",
              "Отправь сумму для вывода на основной счет или «всё».")
        k = ikb([("Отмена", "nav:bal")])
    else:
        return await cb.answer()
    await edit(cb, t, k)
    await cb.answer()


@router.callback_query(F.data.startswith("chk:rev:"))
async def cb_revoke(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    uid = cb.from_user.id
    try:
        cid = int(cb.data.split(":")[2])
    except ValueError:
        return await cb.answer()
    refund = 0
    c = (await DB.execute(
        "SELECT * FROM checks WHERE id=? AND creator_id=? AND status='active'",
        (cid, uid))).fetchone()
    if c:
        refund = (_to_int(c["total"]) - _to_int(c["used"])) * _to_int(c["amount"])
        await DB.tx([
            ("UPDATE checks SET status='revoked' WHERE id=?", (cid,)),
            ("UPDATE users SET balance=balance+? WHERE id=?", (refund, uid)),
        ])
    await cb.answer(f"Возвращено: {money(refund)}" if c else "Чек недоступен")
    t, k = await view_checks(uid)
    await edit(cb, t, k)


@router.callback_query(F.data == "bonus:claim")
async def cb_bonus(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    uid, now = cb.from_user.id, int(time.time())
    res = await DB.tx([
        ("SELECT last_bonus FROM users WHERE id=?", (uid,)),
        ("SELECT inviter_id FROM referrals WHERE invited_id=? AND rewarded=0", (uid,)),
    ])
    last_bonus = 0
    if res and res[0]:
        row = res[0].fetchone()
        last_bonus = _to_int(row["last_bonus"]) if row else 0
    inviter_id = None
    if len(res) > 1 and res[1]:
        row = res[1].fetchone()
        if row:
            inviter_id = _to_int(row["inviter_id"])

    if last_bonus + BONUS_COOLDOWN > now:
        await cb.answer("Бонус пока недоступен", show_alert=True)
        t, k = await view_bonus(uid)
        return await edit(cb, t, k)

    stmts = [
        ("UPDATE users SET balance=balance+?, last_bonus=? WHERE id=?",
         (BONUS, now, uid)),
    ]
    if inviter_id:
        stmts.append(("UPDATE referrals SET rewarded=1 WHERE invited_id=?", (uid,)))
        stmts.append(("UPDATE users SET balance=balance+? WHERE id=?",
                      (REF_PREMIUM, inviter_id)))
    await DB.tx(stmts)

    await edit(cb, "Бонус получен.\n\n" + q(
        f"+{money(BONUS)}",
        "Следующий бонус через 24 ч"))
    await cb.answer()
    if inviter_id:
        try:
            await cb.bot.send_message(
                inviter_id,
                "Новый реферал.\n\n" + q(
                    f"Приглашённый: {mention(uid, cb.from_user.full_name)}",
                    "Активировал Бонус",
                    f"Начислено: +{money(REF_PREMIUM)}"))
        except Exception:
            pass


@router.callback_query(F.data.startswith("m:"))
async def cb_mines(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    try:
        _, gid, act = cb.data.split(":")
        gid = int(gid)
        cell = None if act == "c" else int(act)
    except ValueError:
        return await cb.answer()
    g = await get_game(gid)
    if not g or g["kind"] != "mines":
        return await cb.answer("Игра не найдена")
    uid = g["user_id"]
    if cb.from_user.id != uid:
        return await cb.answer("Это чужая игра", show_alert=True)
    if g["state"] != "active":
        return await cb.answer("Игра завершена")
    d = json.loads(g["data"])
    opened = d["opened"]
    bet = _to_int(g["bet"])
    if cell is None:
        if not opened:
            return await cb.answer("Открой хотя бы одну ячейку")
        await finish_game(gid, "cashed", d, int(bet * mines_mult(len(opened))), uid)
    else:
        if not 0 <= cell < MINES_N or cell in opened:
            return await cb.answer()
        if cell in d["mines"]:
            d["hit"] = cell
            await finish_game(gid, "lost", d, 0, uid)
        else:
            opened.append(cell)
            if len(opened) >= MINES_N - MINES_COUNT:
                await finish_game(gid, "cashed", d,
                                  int(bet * mines_mult(len(opened))), uid)
            else:
                await save_game(gid, d)
    t, k = await mines_view(await get_game(gid))
    await edit(cb, t, k)
    await cb.answer()


@router.callback_query(F.data.startswith("h:"))
async def cb_hack(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    try:
        _, gid, act = cb.data.split(":")
        gid = int(gid)
        node = None if act == "c" else int(act)
    except ValueError:
        return await cb.answer()
    g = await get_game(gid)
    if not g or g["kind"] != "hack":
        return await cb.answer("Игра не найдена")
    uid = g["user_id"]
    if cb.from_user.id != uid:
        return await cb.answer("Это чужая игра", show_alert=True)
    if g["state"] != "active":
        return await cb.answer("Игра завершена")
    d = json.loads(g["data"])
    bet = _to_int(g["bet"])
    if node is None:
        if d["level"] == 0:
            return await cb.answer("Пройди хотя бы один уровень")
        await finish_game(gid, "cashed", d, int(bet * HACK_STEP ** d["level"]), uid)
    else:
        if node not in (0, 1, 2):
            return await cb.answer()
        if node == d["traps"][d["level"]]:
            await finish_game(gid, "lost", d, 0, uid)
        else:
            d["level"] += 1
            if d["level"] >= HACK_LEVELS:
                await finish_game(gid, "cashed", d,
                                  int(bet * HACK_STEP ** d["level"]), uid)
            else:
                await save_game(gid, d)
    t, k = await hack_view(await get_game(gid))
    await edit(cb, t, k)
    await cb.answer()


# ────────────────────────────────── Запуск ──────────────────────────────

async def main():
    global BOT_USERNAME
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not TOKEN:
        raise SystemExit("Не задана переменная окружения BOT_TOKEN")
    if not OWNER_ID:
        raise SystemExit("Не задана (или неверна) переменная окружения OWNER_ID")
    await init_db()
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    BOT_USERNAME = (await bot.get_me()).username
    dp = Dispatcher()
    dp.include_router(router)
    await bot.set_my_commands([BotCommand(command="start", description="Главное меню")])
    await bot.delete_webhook(drop_pending_updates=True)
    t_defl = asyncio.create_task(deflation_loop())
    t_idle = asyncio.create_task(idle_games_loop())
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        t_defl.cancel()
        t_idle.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
