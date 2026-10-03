# -*- coding: utf-8 -*-
"""
GLINT — однофайловый Telegram-бот с внутренней валютой.

Переменные окружения (задаются на хостинге):
    BOT_TOKEN  — токен бота от @BotFather            (обязательно)
    OWNER_ID   — Telegram ID владельца               (обязательно)
    DB_PATH    — путь к базе, по умолчанию baze.db   (необязательно)

Зависимости ставятся автоматически при первом запуске (aiogram 3.x).
Для работы команд в группах отключи у бота Privacy Mode (@BotFather → /setprivacy → Disable).
"""
import importlib
import os
import subprocess
import sys

REQUIRED = [("aiogram", "aiogram>=3.7,<4")]


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
import sqlite3  # noqa: E402
import time  # noqa: E402
from contextlib import contextmanager  # noqa: E402
from datetime import date, datetime, timedelta, timezone  # noqa: E402
from html import escape as esc  # noqa: E402

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
DB_PATH = os.getenv("DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "baze.db")

CUR = "GLINT"
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

log = logging.getLogger("glint")
RNG = secrets.SystemRandom()
BOT_USERNAME = ""

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

DB: sqlite3.Connection = None  # type: ignore


def init_db():
    global DB
    DB = sqlite3.connect(DB_PATH, isolation_level=None)
    DB.row_factory = sqlite3.Row
    DB.execute("PRAGMA journal_mode=DELETE")
    DB.execute("PRAGMA synchronous=FULL")
    for table, cols in TABLES.items():
        ddl = ", ".join(f"{c} {d}" for c, d in cols)
        DB.execute(f"CREATE TABLE IF NOT EXISTS {table} ({ddl})")
        have = {r["name"] for r in DB.execute(f"PRAGMA table_info({table})")}
        for c, d in cols:
            if c not in have:
                plain = d.replace("PRIMARY KEY", "").replace("AUTOINCREMENT", "").strip()
                DB.execute(f"ALTER TABLE {table} ADD COLUMN {c} {plain}")
    for sql in INDEXES:
        try:
            DB.execute(sql)
        except sqlite3.DatabaseError:
            log.exception("index failed: %s", sql)
    DB.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('creator_balance', '0')")
    DB.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('total_burned', '0')")


@contextmanager
def tx():
    DB.execute("BEGIN IMMEDIATE")
    try:
        yield DB
        DB.execute("COMMIT")
    except BaseException:
        DB.execute("ROLLBACK")
        raise


def meta_get(key, default="0"):
    r = DB.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def meta_set(key, value):
    DB.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, str(value)))


def meta_add(key, delta):
    meta_set(key, int(meta_get(key)) + delta)


def upsert_user(u: User) -> bool:
    name = (u.full_name or "").strip()
    uname = u.username or ""
    if DB.execute("SELECT 1 FROM users WHERE id=?", (u.id,)).fetchone():
        DB.execute("UPDATE users SET name=?, username=? WHERE id=?", (name, uname, u.id))
        return False
    DB.execute("INSERT INTO users(id, name, username, created_at) VALUES(?,?,?,?)",
               (u.id, name, uname, int(time.time())))
    return True


def get_user(uid):
    return DB.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()


def balance_of(uid):
    r = get_user(uid)
    return r["balance"] if r else 0


def pend_set(uid, kind, data=""):
    DB.execute("INSERT OR REPLACE INTO pending(user_id, kind, data, created_at) VALUES(?,?,?,?)",
               (uid, kind, data, int(time.time())))


def pend_get(uid):
    r = DB.execute("SELECT * FROM pending WHERE user_id=?", (uid,)).fetchone()
    if r and time.time() - r["created_at"] > 600:
        pend_clear(uid)
        return None
    return r


def pend_clear(uid):
    DB.execute("DELETE FROM pending WHERE user_id=?", (uid,))


# ──────────────────────────────── Утилиты ───────────────────────────────

AMT_RE = re.compile(r"^\d{1,10}(?:[.,]\d{1,2})?$")


def parse_amount(s):
    if not s or not AMT_RE.match(s):
        return None
    whole, _, frac = s.replace(",", ".").partition(".")
    v = int(whole) * SCALE + int((frac + "00")[:2])
    return v if v > 0 else None


def fmt(c):
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

L_ACC, L_BAL, L_PAR = "👤 Аккаунт", "💳 Счет", "🤝 Партнеры"
L_ECO, L_BON, L_MAN = "📊 Экономика", "🎁 Бонус", "📖 Мануал"


def main_kb():
    rows = [
        [KeyboardButton(text=L_ACC), KeyboardButton(text=L_PAR)],
        [KeyboardButton(text=L_BAL), KeyboardButton(text=L_ECO)],
        [KeyboardButton(text=L_BON), KeyboardButton(text=L_MAN)],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


# ───────────────────────────────── Тексты ───────────────────────────────

def games_text():
    return (
        "<b>🎮 Игры</b>\n\n"
        + q("<b>💣 Мины</b>",
            "Команда: <code>Мины {сумма}</code>",
            f"Поле 5×5, спрятано {MINES_COUNT} мины. Открывай ячейки — каждая "
            "безопасная повышает множитель. Забери выигрыш, пока не попал на мину.")
        + "\n"
        + q("<b>🔓 Взлом</b>",
            "Команда: <code>Взлом {сумма}</code>",
            f"{HACK_LEVELS} уровней защиты. На каждом выбери один из трёх узлов — "
            "в одном спрятана ловушка. Каждый пройденный уровень умножает ставку "
            f"на x{HACK_STEP}, максимум x{HACK_STEP ** HACK_LEVELS:.2f}.")
        + "\n"
        + q("Ставка списывается сразу. Выигрыш выплачивает бот, "
            "проигранная ставка сгорает.",
            "Если 30 минут не делать ход — ставка вернётся автоматически.")
    )


def commands_text():
    return (
        "<b>⌨️ Команды</b>\n\n"
        + q("<b>Переводы</b>",
            "<code>П {сумма}</code> — ответом на сообщение",
            "<code>П {ID} {сумма}</code> — по ID")
        + "\n"
        + q("<b>Игры</b>",
            "<code>Игры</code> — список игр и гайды",
            "<code>Мины {сумма}</code>",
            "<code>Взлом {сумма}</code>")
        + "\n"
        + q("<b>В чатах</b>",
            "<code>счет</code> / <code>мой счет</code> — показать баланс")
        + "\n<i>Суммы — целые или с точностью до сотых: 100, 12.5.\n"
          "Переводы и игры работают в ЛС и в чатах.</i>"
    )


def policy_text():
    return (
        "<b>📜 Политика</b>\n\n"
        + q("<b>Валюта</b>",
            f"{CUR} — внутренняя игровая единица бота. Это не деньги "
            "и не ценная бумага: её нельзя купить, продать или вывести "
            "за пределы бота.")
        + "\n"
        + q("<b>Дефляция</b>",
            "Ежедневно со всех счетов списывается 0,5% — "
            "эти средства сгорают безвозвратно.")
        + "\n"
        + q("<b>Переводы, чеки и игры</b>",
            "Переводы и активации чеков необратимы. Игры построены на случайности, "
            "результат не гарантирован. Если игрок бездействует 30 минут — "
            "ставка возвращается.")
        + "\n"
        + q("<b>Данные</b>",
            "Бот хранит Telegram ID, имя, username, баланс и историю операций — "
            "только для работы функций.")
        + "\n"
        + q("<b>Условия</b>",
            "Правила и параметры могут меняться. Используя бот, "
            "ты принимаешь эту политику.")
    )


def burn_text():
    return (
        "<b>🔥 Сжигание валюты</b>\n\n"
        + q("Каждый день в 00:00 (МСК) со всех счетов списывается "
            "ровно 0,5% от суммы валюты. Это и есть дефляция.",
            "Списанные 0,5% сгорают безвозвратно — "
            "их больше нельзя получить или вернуть.")
        + "\n<b>Зачем это нужно</b>\n"
        + q(f"Сгорающая валюта уменьшает общий объём и поддерживает "
            f"ценность {CUR}. Дефляция стимулирует активность: бонусы, "
            "игры и чеки позволяют не терять накопленное.")
    )


# ─────────────────────────────── Экраны (views) ─────────────────────────

def view_account(uid):
    u = get_user(uid)
    uname = f"@{esc(u['username'])}" if u["username"] else "—"
    return ("<b>👤 Аккаунт</b>\n\n" + q(
        f"ID: <code>{uid}</code>",
        f"Name: {esc(u['name'] or '—')}",
        f"Username: {uname}"))


def view_balance(uid):
    lines = [f"Текущий счет: {money(balance_of(uid))}"]
    if is_owner(uid):
        lines.append(f"Создательский счет: {money(int(meta_get('creator_balance')))}")
    text = "<b>💳 Счет</b>\n\n" + q(*lines)
    kb = [[("🧾 Чеки", "nav:chk"), ("📜 История", "nav:hist")]]
    if is_owner(uid):
        kb.append([("📤 Вывести", "nav:wd")])
    return text, ikb(*kb)


def view_history(uid, page=0):
    total = DB.execute(
        "SELECT COUNT(*) FROM transfers WHERE from_id=? OR to_id=?",
        (uid, uid)).fetchone()[0]
    head = "<b>📜 История</b>\n\n"
    if total == 0:
        return head + q("Переводов пока не было."), ikb([("← Счет", "nav:bal")])
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, pages - 1))
    rows = DB.execute(
        "SELECT * FROM transfers WHERE from_id=? OR to_id=? "
        "ORDER BY id DESC LIMIT ? OFFSET ?",
        (uid, uid, PAGE_SIZE, page * PAGE_SIZE)).fetchall()
    lines = []
    for r in rows:
        dt = datetime.fromtimestamp(r["created_at"], MSK).strftime("%d.%m.%Y %H:%M")
        if r["from_id"] == uid:
            other = get_user(r["to_id"])
            who = (other["name"] if other else "") or "—"
            lines.append(f"⬆️ −{money(r['amount'])} → {esc(who)}\n     <i>{dt}</i>")
        else:
            other = get_user(r["from_id"])
            who = (other["name"] if other else "") or "—"
            lines.append(f"⬇️ +{money(r['amount'])} ← {esc(who)}\n     <i>{dt}</i>")
    text = head + q(*lines) + f"\n<i>Страница {page + 1} / {pages} · всего {total}</i>"
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


def view_checks(uid):
    rows = DB.execute(
        "SELECT * FROM checks WHERE creator_id=? AND status='active' ORDER BY id DESC LIMIT 5",
        (uid,)).fetchall()
    text = ("<b>🧾 Чеки</b>\n\n"
            + q("Создай чек и получи ссылку. Каждый, кто её откроет, получит валюту."))
    kb = [[("➕ Создать", "nav:new")]]
    for c in rows:
        text += ("\n\n" + q(
            f"#{c['id']} · {money(c['amount'])} · "
            f"осталось {c['total'] - c['used']} из {c['total']}")
            + f"\n<code>{check_link(c['code'])}</code>")
        kb.append([(f"↩️ Отозвать #{c['id']}", f"chk:rev:{c['id']}")])
    kb.append([("← Счет", "nav:bal")])
    return text, ikb(*kb)


def view_partners(uid):
    n = DB.execute("SELECT COUNT(*) FROM referrals WHERE inviter_id=?", (uid,)).fetchone()[0]
    return (
        "<b>🤝 Партнеры</b>\n\n"
        "Твоя партнёрская ссылка:\n"
        f"<code>https://t.me/{BOT_USERNAME}?start=r_{uid}</code>\n\n"
        + q(f"Ты пригласил: {n}",
            f"Премия: {money(REF_PREMIUM)}")
        + "\n"
        + q("Чтобы получить премию, приглашённый должен быть новым "
            "пользователем в боте и получить Бонус.")
    )


def view_bonus(uid):
    left = get_user(uid)["last_bonus"] + BONUS_COOLDOWN - time.time()
    head = "<b>🎁 Бонус</b>\n\n"
    if left > 0:
        return head + q(f"+{money(BONUS)} каждые 24 часа",
                        f"Следующий бонус через {fmt_left(left)}"), None
    return head + q(f"+{money(BONUS)} каждые 24 часа",
                    "Бонус доступен"), \
        ikb([("🎁 Активировать", "bonus:claim")])


def view_economy():
    supply = DB.execute("SELECT COALESCE(SUM(balance),0) FROM users").fetchone()[0] \
        + int(meta_get("creator_balance"))
    hist = DB.execute("SELECT day, burned FROM deflation_history ORDER BY day DESC LIMIT ?",
                      (HISTORY_DAYS,)).fetchall()
    lines = []
    for h in hist:
        d = date.fromisoformat(h["day"])
        lines.append(f"{d.day:02d}.{d.month:02d} · {money(h['burned'])}")
    text = ("<b>📊 Экономика</b>\n\n"
            + q(f"Валюты на счетах: {money(supply)}",
                f"Дефляция сожгла: {money(int(meta_get('total_burned')))}",
                "Текущая дефляция: 0.5%")
            + f"\n<b>История дефляции · {HISTORY_DAYS} дней</b>\n"
            + q(*(lines or ["Пока нет данных"])))
    return text, ikb([("🔥 Сжигание валюты", "nav:burn")])


# ────────────────────────────── Дефляция ────────────────────────────────

def apply_deflation(day: str):
    cp_n, cp_d = CREATOR_PART
    with tx():
        fee_sum, cre_sum = DB.execute(
            f"SELECT COALESCE(SUM(balance*{FEE_MILLE}/1000),0), "
            f"COALESCE(SUM((balance*{FEE_MILLE}/1000)*{cp_n}/{cp_d}),0) "
            f"FROM users WHERE balance>0").fetchone()
        supply = DB.execute("SELECT COALESCE(SUM(balance),0) FROM users").fetchone()[0]
        DB.execute(f"UPDATE users SET balance = balance - balance*{FEE_MILLE}/1000 WHERE balance>0")
        burned = fee_sum - cre_sum
        meta_add("creator_balance", cre_sum)
        meta_add("total_burned", burned)
        DB.execute("INSERT OR REPLACE INTO deflation_history(day, supply, burned, creator, created_at) "
                   "VALUES(?,?,?,?,?)", (day, supply, burned, cre_sum, int(time.time())))
        meta_set("last_deflation_day", day)


async def deflation_loop():
    while True:
        try:
            today = datetime.now(MSK).date()
            last = meta_get("last_deflation_day", "")
            if not last:
                meta_set("last_deflation_day", today.isoformat())
            else:
                d = date.fromisoformat(last)
                if (today - d).days > 30:
                    d = today - timedelta(days=30)
                while d < today:
                    d += timedelta(days=1)
                    apply_deflation(d.isoformat())
        except Exception:
            log.exception("deflation failed")
        await asyncio.sleep(30)


# ──────────────────────────── Автовозврат ставок ────────────────────────

async def idle_games_loop():
    while True:
        try:
            with tx():
                rows = DB.execute(
                    "SELECT id, user_id, bet FROM games "
                    "WHERE state='active' AND created_at < ?",
                    (int(time.time()) - GAME_TIMEOUT,)).fetchall()
                for g in rows:
                    DB.execute("UPDATE games SET state='expired' WHERE id=? AND state='active'",
                               (g["id"],))
                    DB.execute("UPDATE users SET balance=balance+? WHERE id=?",
                               (g["bet"], g["user_id"]))
        except Exception:
            log.exception("idle games failed")
        await asyncio.sleep(60)


# ──────────────────────────────── Игры ──────────────────────────────────

def mines_mult(k):
    m = MINES_EDGE
    for i in range(k):
        m *= (MINES_N - i) / (MINES_N - MINES_COUNT - i)
    return m


def get_game(gid):
    return DB.execute("SELECT * FROM games WHERE id=?", (gid,)).fetchone()


def save_game(gid, data):
    DB.execute("UPDATE games SET data=?, created_at=? WHERE id=?",
               (json.dumps(data), int(time.time()), gid))


def finish_game(gid, state, data, payout, uid):
    with tx():
        DB.execute("UPDATE games SET state=?, data=? WHERE id=? AND state='active'",
                   (state, json.dumps(data), gid))
        if payout:
            DB.execute("UPDATE users SET balance=balance+? WHERE id=?", (payout, uid))


def mines_view(g):
    d = json.loads(g["data"])
    opened, st = d["opened"], g["state"]
    k = len(opened)
    mult = mines_mult(k) if k else 1.0
    pay = 0 if st == "lost" else int(g["bet"] * mult)
    who = esc(get_user(g["user_id"])["name"] or "—")
    text = f"<b>💣 Мины</b> · {who}\n\n" + q(
        f"Ставка: {money(g['bet'])}",
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
        rows.append([InlineKeyboardButton(text=f"💰 Забрать {money(pay)}",
                                          callback_data=f"m:{g['id']}:c")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def hack_view(g):
    d = json.loads(g["data"])
    lvl, st = d["level"], g["state"]
    mult = HACK_STEP ** lvl
    pay = 0 if st == "lost" else int(g["bet"] * mult)
    who = esc(get_user(g["user_id"])["name"] or "—")
    bar = "▰" * lvl + "▱" * (HACK_LEVELS - lvl)
    text = f"<b>🔓 Взлом</b> · {who}\n\n" + q(
        f"Ставка: {money(g['bet'])}",
        f"Защита: {lvl} / {HACK_LEVELS}  {bar}",
        f"Множитель: x{mult if st != 'lost' else 0:.2f}",
        f"Выигрыш: {money(pay)}")
    kb = None
    if st == "active":
        text += "\n<i>Выбери узел — в одном из трёх спрятана ловушка.</i>"
        rows = [[InlineKeyboardButton(text=f"🔹 {n}", callback_data=f"h:{g['id']}:{i}")
                 for i, n in enumerate("ABC")]]
        if lvl:
            rows.append([InlineKeyboardButton(text=f"💰 Забрать {money(pay)}",
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


def take_bet(uid, bet, kind, data, chat_id):
    with tx():
        if balance_of(uid) < bet:
            return None
        DB.execute("UPDATE users SET balance=balance-? WHERE id=?", (bet, uid))
        cur = DB.execute(
            "INSERT INTO games(user_id, chat_id, kind, bet, data, created_at) VALUES(?,?,?,?,?,?)",
            (uid, chat_id, kind, bet, json.dumps(data), int(time.time())))
        return cur.lastrowid


async def cmd_mines(m: Message, arg):
    bet = parse_amount(arg)
    if bet is None or bet < MIN_BET:
        if m.chat.type == ChatType.PRIVATE:
            await say(m, "<b>💣 Мины</b>\n\n" + q(
                "<code>Мины {сумма}</code>",
                f"Минимальная ставка: {money(MIN_BET)}"))
        return
    uid = m.from_user.id
    data = {"mines": RNG.sample(range(MINES_N), MINES_COUNT), "opened": []}
    gid = take_bet(uid, bet, "mines", data, m.chat.id)
    if gid is None:
        return await say(m, "<b>💣 Мины</b>\n\n" + q("Недостаточно средств."))
    text, kb = mines_view(get_game(gid))
    await say(m, text, kb)


async def cmd_hack(m: Message, arg):
    bet = parse_amount(arg)
    if bet is None or bet < MIN_BET:
        if m.chat.type == ChatType.PRIVATE:
            await say(m, "<b>🔓 Взлом</b>\n\n" + q(
                "<code>Взлом {сумма}</code>",
                f"Минимальная ставка: {money(MIN_BET)}"))
        return
    uid = m.from_user.id
    data = {"traps": [RNG.randrange(3) for _ in range(HACK_LEVELS)], "level": 0}
    gid = take_bet(uid, bet, "hack", data, m.chat.id)
    if gid is None:
        return await say(m, "<b>🔓 Взлом</b>\n\n" + q("Недостаточно средств."))
    text, kb = hack_view(get_game(gid))
    await say(m, text, kb)


# ──────────────────────────── Команда баланса в чате ────────────────────

async def cmd_balance_chat(m: Message, uid: int):
    u = get_user(uid)
    name = esc(u["name"] or "—")
    await say(m, "<b>💳 Счет</b>\n\n" + q(f"{name}: {money(u['balance'])}"))


# ─────────────────────────────── Переводы ───────────────────────────────

TRANSFER_USAGE = "<b>💸 Перевод</b>\n\n" + q(
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
        upsert_user(r.from_user)
        target_id, target_name = r.from_user.id, r.from_user.full_name
    elif len(args) == 2 and args[0].isdigit():
        amt = parse_amount(args[1])
        if amt is None:
            if private:
                await say(m, TRANSFER_USAGE)
            return
        target_id = int(args[0])
        t = get_user(target_id)
        if not t:
            return await say(m, "<b>💸 Перевод</b>\n\n"
                                + q("Пользователь с таким ID не найден."))
        target_name = t["name"]
    else:
        if private:
            await say(m, TRANSFER_USAGE)
        return

    if target_id == sender.id:
        return await say(m, "<b>💸 Перевод</b>\n\n" + q("Нельзя переводить самому себе."))

    with tx():
        have = balance_of(sender.id)
        ok = have >= amt
        if ok:
            DB.execute("UPDATE users SET balance=balance-? WHERE id=?", (amt, sender.id))
            DB.execute("UPDATE users SET balance=balance+? WHERE id=?", (amt, target_id))
            DB.execute("INSERT INTO transfers(from_id, to_id, amount, created_at) VALUES(?,?,?,?)",
                       (sender.id, target_id, amt, int(time.time())))
    if not ok:
        return await say(m, "<b>💸 Перевод</b>\n\n" + q("Недостаточно средств."))

    await say(m, "<b>💸 Перевод выполнен</b>\n\n" + q(
        f"Отправлено: {money(amt)}",
        f"Получатель: {mention(target_id, target_name)}"))
    try:
        await m.bot.send_message(
            target_id,
            "<b>💸 Входящий перевод</b>\n\n" + q(
                f"+{money(amt)} от {mention(sender.id, sender.full_name)}"))
    except Exception:
        pass


# ───────────────────────────── Чеки / вывод ─────────────────────────────

async def claim_check(m: Message, code):
    uid = m.from_user.id
    err = None
    got = 0
    with tx():
        c = DB.execute("SELECT * FROM checks WHERE code=?", (code,)).fetchone()
        if not c:
            err = "Чек не найден."
        elif c["status"] == "revoked":
            err = "Чек отозван."
        elif c["status"] != "active" or c["used"] >= c["total"]:
            err = "Чек уже активирован полностью."
        elif DB.execute("SELECT 1 FROM check_claims WHERE check_id=? AND user_id=?",
                        (c["id"], uid)).fetchone():
            err = "Ты уже активировал этот чек."
        else:
            got = c["amount"]
            DB.execute("INSERT INTO check_claims(check_id, user_id, created_at) VALUES(?,?,?)",
                       (c["id"], uid, int(time.time())))
            DB.execute("UPDATE checks SET used=used+?, status=CASE WHEN used+1>=total "
                       "THEN 'done' ELSE status END WHERE id=?", (1, c["id"]))
            DB.execute("UPDATE users SET balance=balance+? WHERE id=?", (got, uid))
    if err:
        return await say(m, "<b>🧾 Чек</b>\n\n" + q(err), main_kb())
    await say(m, "<b>🧾 Чек активирован</b>\n\n" + q(
        f"Получено: +{money(got)}"), main_kb())


async def handle_pending(m: Message, p):
    uid = m.from_user.id
    txt = (m.text or "").strip()
    if p["kind"] == "check":
        t = txt.split()
        amt = parse_amount(t[0]) if len(t) == 2 else None
        cnt = int(t[1]) if len(t) == 2 and t[1].isdigit() else 0
        if amt is None or not 1 <= cnt <= 1000:
            return await say(m, "<b>🧾 Новый чек</b>\n\n" + q(
                "Формат: сумма на одну активацию и количество активаций.",
                "Пример: <code>100 5</code>"), ikb([("← Отмена", "nav:chk")]))
        total = amt * cnt
        code = secrets.token_urlsafe(9)
        with tx():
            have = balance_of(uid)
            ok = have >= total
            if ok:
                DB.execute("UPDATE users SET balance=balance-? WHERE id=?", (total, uid))
                DB.execute("INSERT INTO checks(code, creator_id, amount, total, created_at) "
                           "VALUES(?,?,?,?,?)", (code, uid, amt, cnt, int(time.time())))
        if not ok:
            return await say(m, "<b>🧾 Новый чек</b>\n\n" + q("Недостаточно средств."),
                             ikb([("← Отмена", "nav:chk")]))
        pend_clear(uid)
        return await say(m, "<b>🧾 Чек создан</b>\n\n" + q(
            f"Сумма: {money(amt)} × {cnt} активаций",
            f"Списано: {money(total)}")
            + f"\n<code>{check_link(code)}</code>", ikb([("🧾 Мои чеки", "nav:chk")]))

    if p["kind"] == "withdraw" and is_owner(uid):
        cb = int(meta_get("creator_balance"))
        amt = cb if txt.lower() in ("всё", "все", "all") else parse_amount(txt)
        if amt is None or amt <= 0:
            return await say(m, "<b>📤 Вывод</b>\n\n" + q(
                "Введи сумму числом или «всё»."), ikb([("← Отмена", "nav:bal")]))
        with tx():
            cb = int(meta_get("creator_balance"))
            ok = cb >= amt
            if ok:
                meta_set("creator_balance", cb - amt)
                DB.execute("UPDATE users SET balance=balance+? WHERE id=?", (amt, uid))
        if not ok:
            return await say(m, "<b>📤 Вывод</b>\n\n" + q(
                f"Недостаточно средств. На Создательском счете {money(cb)}."),
                ikb([("← Отмена", "nav:bal")]))
        pend_clear(uid)
        return await say(m, "<b>📤 Вывод выполнен</b>\n\n" + q(
            f"Переведено: {money(amt)}"), ikb([("← Счет", "nav:bal")]))
    pend_clear(uid)


# ─────────────────────────────── Хендлеры ───────────────────────────────

router = Router()


class Reg(BaseMiddleware):
    async def __call__(self, handler, event, data):
        u = data.get("event_from_user")
        data["is_new"] = bool(u and not u.is_bot and upsert_user(u))
        return await handler(event, data)


router.message.middleware(Reg())
router.callback_query.middleware(Reg())


@router.message(Command("start"), F.chat.type == ChatType.PRIVATE)
async def cmd_start(m: Message, command: CommandObject, is_new: bool):
    uid = m.from_user.id
    pend_clear(uid)
    arg = (command.args or "").strip()
    if arg.startswith("c_"):
        return await claim_check(m, arg[2:])
    if arg.startswith("r_") and is_new and arg[2:].isdigit():
        inviter = int(arg[2:])
        if inviter != uid and get_user(inviter):
            DB.execute("INSERT OR IGNORE INTO referrals(invited_id, inviter_id, created_at) "
                       "VALUES(?,?,?)", (uid, inviter, int(time.time())))
    name = esc(m.from_user.first_name or "друг")
    await m.answer(
        f"<b>😄 О, привет, {name}.</b>\n\n"
        + q("Счет, переводы, и игры прямо в Telegram."),
        reply_markup=main_kb())


@router.message(Command("games"))
async def cmd_games(m: Message):
    await games_guide(m)


async def games_guide(m: Message):
    await say(m, games_text())


async def menu_account(m: Message):
    await m.answer(view_account(m.from_user.id), reply_markup=main_kb())


async def menu_balance(m: Message):
    t, k = view_balance(m.from_user.id)
    await m.answer(t, reply_markup=k)


async def menu_partners(m: Message):
    await m.answer(view_partners(m.from_user.id), reply_markup=main_kb())


async def menu_economy(m: Message):
    t, k = view_economy()
    await m.answer(t, reply_markup=k)


async def menu_bonus(m: Message):
    t, k = view_bonus(m.from_user.id)
    await m.answer(t, reply_markup=k or main_kb())


async def menu_manual(m: Message):
    await m.answer(
        "<b>📖 Мануал</b>\n\nВыбери раздел:",
        reply_markup=ikb(
            [("🎮 Игры", "man:games")],
            [("⌨️ Команды", "man:cmd")],
            [("📜 Политика", "man:pol")],
        ))


MENU = {}
for _label, _fn in [(L_ACC, menu_account), (L_BAL, menu_balance), (L_PAR, menu_partners),
                    (L_ECO, menu_economy), (L_BON, menu_bonus), (L_MAN, menu_manual)]:
    MENU[_label] = _fn
    MENU[_label.split(" ", 1)[1].lower()] = _fn


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
            pend_clear(uid)
            return await fn(m)

    head = parts[0]
    if head == "п" and 2 <= len(parts) <= 3:
        pend_clear(uid)
        return await cmd_transfer(m, parts[1:])
    if head == "мины" and len(parts) == 2:
        return await cmd_mines(m, parts[1])
    if head == "взлом" and len(parts) == 2:
        return await cmd_hack(m, parts[1])
    if head == "игры" and len(parts) == 1:
        return await games_guide(m)

    if head == "счет" and len(parts) == 1:
        return await cmd_balance_chat(m, uid)
    if len(parts) == 2 and head == "мой" and parts[1] == "счет":
        return await cmd_balance_chat(m, uid)

    if private:
        p = pend_get(uid)
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
    back = ikb([("← Мануал", "man:back")])
    if act == "games":
        await edit(cb, games_text(), back)
    elif act == "cmd":
        await edit(cb, commands_text(), back)
    elif act == "pol":
        await edit(cb, policy_text(), back)
    elif act == "back":
        await edit(cb, "<b>📖 Мануал</b>\n\nВыбери раздел:",
                   ikb([("🎮 Игры", "man:games")],
                       [("⌨️ Команды", "man:cmd")],
                       [("📜 Политика", "man:pol")]))
    await cb.answer()


@router.callback_query(F.data.startswith("hist:"))
async def cb_hist(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    try:
        page = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        return await cb.answer()
    t, k = view_history(cb.from_user.id, page)
    await edit(cb, t, k)
    await cb.answer()


@router.callback_query(F.data.startswith("nav:"))
async def cb_nav(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    uid, act = cb.from_user.id, cb.data[4:]
    pend_clear(uid)
    if act == "bal":
        t, k = view_balance(uid)
    elif act == "hist":
        t, k = view_history(uid, 0)
    elif act == "chk":
        t, k = view_checks(uid)
    elif act == "eco":
        t, k = view_economy()
    elif act == "burn":
        t, k = burn_text(), ikb([("← Экономика", "nav:eco")])
    elif act == "new":
        pend_set(uid, "check")
        t = "<b>🧾 Новый чек</b>\n\n" + q(
            "Отправь сумму на одну активацию и количество активаций через пробел.",
            "Пример: <code>100 5</code>")
        k = ikb([("← Отмена", "nav:chk")])
    elif act == "wd":
        if not is_owner(uid):
            return await cb.answer("Доступно только владельцу", show_alert=True)
        pend_set(uid, "withdraw")
        t = "<b>📤 Вывод</b>\n\n" + q(
            f"Создательский счет: {money(int(meta_get('creator_balance')))}",
            "Отправь сумму для вывода на основной счет или «всё».")
        k = ikb([("← Отмена", "nav:bal")])
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
    with tx():
        c = DB.execute("SELECT * FROM checks WHERE id=? AND creator_id=? AND status='active'",
                       (cid, uid)).fetchone()
        if c:
            refund = (c["total"] - c["used"]) * c["amount"]
            DB.execute("UPDATE checks SET status='revoked' WHERE id=?", (cid,))
            DB.execute("UPDATE users SET balance=balance+? WHERE id=?", (refund, uid))
    await cb.answer(f"Возвращено: {money(refund)}" if c else "Чек недоступен")
    t, k = view_checks(uid)
    await edit(cb, t, k)


@router.callback_query(F.data == "bonus:claim")
async def cb_bonus(cb: CallbackQuery):
    if not isinstance(cb.message, Message):
        return await cb.answer()
    uid, now = cb.from_user.id, int(time.time())
    ok = False
    inviter_id = None
    with tx():
        u = get_user(uid)
        if u["last_bonus"] + BONUS_COOLDOWN <= now:
            ok = True
            DB.execute("UPDATE users SET balance=balance+?, last_bonus=? WHERE id=?",
                       (BONUS, now, uid))
            r = DB.execute("SELECT inviter_id FROM referrals WHERE invited_id=? AND rewarded=0",
                           (uid,)).fetchone()
            if r:
                inviter_id = r["inviter_id"]
                DB.execute("UPDATE referrals SET rewarded=1 WHERE invited_id=?", (uid,))
                DB.execute("UPDATE users SET balance=balance+? WHERE id=?",
                           (REF_PREMIUM, inviter_id))
    if not ok:
        await cb.answer("Бонус пока недоступен", show_alert=True)
        t, k = view_bonus(uid)
        return await edit(cb, t, k)
    await edit(cb, "<b>🎁 Бонус получен</b>\n\n" + q(
        f"+{money(BONUS)}",
        "Следующий бонус через 24 ч"))
    await cb.answer()
    if inviter_id:
        try:
            await cb.bot.send_message(
                inviter_id,
                "<b>🤝 Новый реферал</b>\n\n" + q(
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
    g = get_game(gid)
    if not g or g["kind"] != "mines":
        return await cb.answer("Игра не найдена")
    uid = g["user_id"]
    if cb.from_user.id != uid:
        return await cb.answer("Это чужая игра", show_alert=True)
    if g["state"] != "active":
        return await cb.answer("Игра завершена")
    d = json.loads(g["data"])
    opened = d["opened"]
    if cell is None:
        if not opened:
            return await cb.answer("Открой хотя бы одну ячейку")
        finish_game(gid, "cashed", d, int(g["bet"] * mines_mult(len(opened))), uid)
    else:
        if not 0 <= cell < MINES_N or cell in opened:
            return await cb.answer()
        if cell in d["mines"]:
            d["hit"] = cell
            finish_game(gid, "lost", d, 0, uid)
        else:
            opened.append(cell)
            if len(opened) >= MINES_N - MINES_COUNT:
                finish_game(gid, "cashed", d, int(g["bet"] * mines_mult(len(opened))), uid)
            else:
                save_game(gid, d)
    t, k = mines_view(get_game(gid))
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
    g = get_game(gid)
    if not g or g["kind"] != "hack":
        return await cb.answer("Игра не найдена")
    uid = g["user_id"]
    if cb.from_user.id != uid:
        return await cb.answer("Это чужая игра", show_alert=True)
    if g["state"] != "active":
        return await cb.answer("Игра завершена")
    d = json.loads(g["data"])
    if node is None:
        if d["level"] == 0:
            return await cb.answer("Пройди хотя бы один уровень")
        finish_game(gid, "cashed", d, int(g["bet"] * HACK_STEP ** d["level"]), uid)
    else:
        if node not in (0, 1, 2):
            return await cb.answer()
        if node == d["traps"][d["level"]]:
            finish_game(gid, "lost", d, 0, uid)
        else:
            d["level"] += 1
            if d["level"] >= HACK_LEVELS:
                finish_game(gid, "cashed", d, int(g["bet"] * HACK_STEP ** d["level"]), uid)
            else:
                save_game(gid, d)
    t, k = hack_view(get_game(gid))
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
    init_db()
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
