import asyncio
import hmac
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatType
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DB_PATH = DATA_DIR / "bot.db"

PIARFLOW_URL = "https://piarflow.com/v1"
REWARD = 3
HOLD_HOURS = 48
MAX_SPONSORS = 5
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("referral_bot")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")
if not PIARFLOW_API_KEY:
    raise RuntimeError("Missing PIARFLOW_API_KEY environment variable")
if not WEBHOOK_SECRET:
    raise RuntimeError("Missing WEBHOOK_SECRET environment variable")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

http: aiohttp.ClientSession | None = None
db_lock = asyncio.Lock()
bot_username: str | None = None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# =========================================================
# DATABASE
# =========================================================

async def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")

        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            referred_by INTEGER,
            balance INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            CHECK (referred_by IS NULL OR referred_by != user_id)
        );

        CREATE TABLE IF NOT EXISTS user_tasks (
            user_id INTEGER NOT NULL,
            link TEXT NOT NULL,
            PRIMARY KEY (user_id, link)
        );

        CREATE TABLE IF NOT EXISTS rewards (
            referred_id INTEGER PRIMARY KEY,
            referrer_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            credited_at TEXT NOT NULL,
            revoked_at TEXT,
            revoke_reason TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_rewards_referrer
        ON rewards(referrer_id);

        CREATE INDEX IF NOT EXISTS idx_rewards_status
        ON rewards(status);
        """)
        await db.commit()


async def register_user(user_id: int, username: str | None,
                        first_name: str, referrer_id: int | None):
    if referrer_id == user_id:
        referrer_id = None

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users
                (user_id, username, first_name, referred_by, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            user_id, username, first_name, referrer_id, dt_string(now_utc())
        ))

        await db.execute("""
            UPDATE users SET username = ?, first_name = ?
            WHERE user_id = ?
        """, (username, first_name, user_id))

        await db.commit()


async def user_exists(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
        )
        return await cur.fetchone() is not None


async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT user_id, referred_by, balance
            FROM users WHERE user_id = ?
        """, (user_id,))
        return await cur.fetchone()


async def get_stats(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT
                COUNT(*),
                COALESCE(SUM(amount), 0)
            FROM rewards
            WHERE referrer_id = ? AND status = 'active'
        """, (user_id,))
        active_count, active_amount = await cur.fetchone()

        cur = await db.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM rewards
            WHERE referrer_id = ? AND status = 'revoked'
        """, (user_id,))
        revoked_count, revoked_amount = await cur.fetchone()

        cur = await db.execute(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()

        return {
            "active_count": active_count,
            "active_amount": active_amount,
            "revoked_count": revoked_count,
            "revoked_amount": revoked_amount,
            "balance": row[0] if row else 0,
        }


async def save_tasks(user_id: int, links: list[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM user_tasks WHERE user_id = ?", (user_id,)
        )
        await db.executemany("""
            INSERT OR IGNORE INTO user_tasks (user_id, link)
            VALUES (?, ?)
        """, [(user_id, link) for link in links])
        await db.commit()


async def get_tasks(user_id: int) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT link FROM user_tasks WHERE user_id = ? ORDER BY link",
            (user_id,),
        )
        return [row[0] for row in await cur.fetchall()]


# =========================================================
# PIARFLOW API
# =========================================================

async def piarflow_post(path: str, payload: dict) -> dict:
    if http is None:
        raise RuntimeError("HTTP client is not initialized")

    async with http.post(
        f"{PIARFLOW_URL}{path}",
        json=payload,
        headers={
            "Authorization": f"Bearer {PIARFLOW_API_KEY}",
            "Content-Type": "application/json",
        },
    ) as response:
        try:
            data = await response.json(content_type=None)
        except (ValueError, aiohttp.ContentTypeError):
            data = {}

        if response.status >= 400:
            message = data.get("message", "PiarFlow API error")
            raise RuntimeError(f"PiarFlow HTTP {response.status}: {message}")

        if not isinstance(data, dict) or data.get("status") == "error":
            raise RuntimeError(
                str(data.get("message", "Invalid PiarFlow response"))
            )

        return data


async def fetch_sponsors(user_id: int, chat_id: int,
                         first_name: str) -> list[dict]:
    data = await piarflow_post("/sponsors", {
        "user_id": user_id,
        "chat_id": chat_id,
        "max_sponsors": MAX_SPONSORS,
    })

    sponsors = data.get("sponsors") or []
    valid = []

    for sponsor in sponsors:
        link = sponsor.get("link")
        if isinstance(link, str) and link.startswith("https://"):
            valid.append({
                "link": link,
                "status": sponsor.get("status", "unsubscribed"),
            })

    return valid


async def check_sponsors(user_id: int, links: list[str]) -> list[dict]:
    if not links:
        return []

    data = await piarflow_post("/sponsors/check", {
        "user_id": user_id,
        "links": links,
    })

    return data.get("sponsors") or []


# =========================================================
# REWARDS
# =========================================================

async def credit_referrer(referred_id: int) -> tuple[bool, int | None]:
    """
    Credits once per referred user.
    The SQLite transaction prevents duplicate rewards.
    """
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("BEGIN IMMEDIATE")

            cur = await db.execute("""
                SELECT referred_by FROM users WHERE user_id = ?
            """, (referred_id,))
            row = await cur.fetchone()

            if not row or row[0] is None:
                await db.rollback()
                return False, None

            referrer_id = row[0]

            cur = await db.execute("""
                SELECT 1 FROM rewards WHERE referred_id = ?
            """, (referred_id,))
            if await cur.fetchone():
                await db.rollback()
                return False, referrer_id

            cur = await db.execute("""
                SELECT 1 FROM users WHERE user_id = ?
            """, (referrer_id,))
            if not await cur.fetchone():
                await db.rollback()
                return False, referrer_id

            await db.execute("""
                INSERT INTO rewards
                    (referred_id, referrer_id, amount, status, credited_at)
                VALUES (?, ?, ?, 'active', ?)
            """, (
                referred_id,
                referrer_id,
                REWARD,
                dt_string(now_utc()),
            ))

            await db.execute("""
                UPDATE users SET balance = balance + ?
                WHERE user_id = ?
            """, (REWARD, referrer_id))

            await db.commit()
            return True, referrer_id


async def revoke_reward(referred_id: int, reason: str) -> tuple[bool, int | None]:
    """
    Revokes the full reward if an unsubscribe webhook arrives
    within the 48-hour holding period. Repeated events are ignored.
    """
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("BEGIN IMMEDIATE")

            cur = await db.execute("""
                SELECT referrer_id, amount, status, credited_at
                FROM rewards WHERE referred_id = ?
            """, (referred_id,))
            reward = await cur.fetchone()

            if not reward:
                await db.rollback()
                return False, None

            referrer_id, amount, status, credited_at = reward

            if status != "active":
                await db.rollback()
                return False, referrer_id

            credited_time = parse_dt(credited_at)
            elapsed = now_utc() - credited_time

            if elapsed < timedelta(0) or elapsed >= timedelta(hours=HOLD_HOURS):
                await db.rollback()
                return False, referrer_id

            await db.execute("""
                UPDATE rewards
                SET status = 'revoked',
                    revoked_at = ?,
                    revoke_reason = ?
                WHERE referred_id = ? AND status = 'active'
            """, (dt_string(now_utc()), reason[:500], referred_id))

            await db.execute("""
                UPDATE users
                SET balance = MAX(0, balance - ?)
                WHERE user_id = ?
            """, (amount, referrer_id))

            await db.commit()
            return True, referrer_id


# =========================================================
# KEYBOARDS / MENUS
# =========================================================

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📢 Задания спонсоров",
            callback_data="tasks",
        )],
        [InlineKeyboardButton(
            text="👥 Пригласить друзей",
            callback_data="referral",
        )],
        [InlineKeyboardButton(
            text="⭐ Баланс",
            callback_data="balance",
        )],
        [InlineKeyboardButton(
            text="📊 Статистика",
            callback_data="stats",
        )],
    ])


def tasks_keyboard(sponsors: list[dict]) -> InlineKeyboardMarkup:
    rows = []

    for index, sponsor in enumerate(sponsors, 1):
        rows.append([InlineKeyboardButton(
            text=f"📢 Задание {index}",
            url=sponsor["link"],
        )])

    rows.append([InlineKeyboardButton(
        text="✅ Проверить подписки",
        callback_data="check_tasks",
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================================================
# COMMANDS
# =========================================================

@dp.message(CommandStart())
async def start(message: Message):
    if message.chat.type != ChatType.PRIVATE or not message.from_user:
        return

    user = message.from_user
    parts = (message.text or "").split(maxsplit=1)
    referrer_id = None

    if len(parts) == 2 and parts[1].isdigit():
        candidate = int(parts[1])
        if candidate != user.id and await user_exists(candidate):
            referrer_id = candidate

    # INSERT OR IGNORE means existing users cannot change referrer.
    await register_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name or "",
        referrer_id=referrer_id,
    )

    await message.answer(
        "⭐ <b>Реферальный бот</b>\n\n"
        f"Приглашай новых пользователей и получай +{REWARD} Stars "
        "за каждого, кто выполнит задания PiarFlow.\n\n"
        "Если реферал отпишется от задания в течение 48 часов, "
        "награда за него будет отозвана.\n\n"
        "Выбери действие:",
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    )


@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "<b>Как это работает</b>\n\n"
        "1. Открой задания спонсоров.\n"
        "2. Выполни все задания.\n"
        "3. Нажми «Проверить подписки».\n"
        "4. За каждого нового подтверждённого реферала "
        f"пригласителю начисляется +{REWARD} Stars.\n\n"
        "Отписка в первые 48 часов приводит к отзыву награды.",
        parse_mode="HTML",
    )


@dp.callback_query(F.data == "tasks")
async def tasks_handler(callback: CallbackQuery):
    user = callback.from_user

    try:
        sponsors = await fetch_sponsors(
            user_id=user.id,
            chat_id=user.id,
            first_name=user.first_name or "",
        )

        if not sponsors:
            await callback.message.answer(
                "Сейчас PiarFlow не выдал доступных заданий. "
                "Попробуй позже.",
                reply_markup=main_keyboard(),
            )
            await callback.answer()
            return

        links = list(dict.fromkeys(
            sponsor["link"] for sponsor in sponsors
        ))
        await save_tasks(user.id, links)

        # Display tasks returned by PiarFlow.
        visible_sponsors = [{"link": link} for link in links]

        await callback.message.answer(
            "📢 <b>Задания спонсоров</b>\n\n"
            "Открой каждую ссылку, выполни задание, "
            "затем нажми «Проверить подписки».",
            reply_markup=tasks_keyboard(visible_sponsors),
            parse_mode="HTML",
        )

    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
        log.exception("Could not fetch PiarFlow sponsors")
        await callback.message.answer(
            "Не удалось загрузить задания. Попробуй позже."
        )

    await callback.answer()


@dp.callback_query(F.data == "check_tasks")
async def check_tasks_handler(callback: CallbackQuery):
    user_id = callback.from_user.id

    if not await user_exists(user_id):
        await callback.answer("Сначала нажми /start.", show_alert=True)
        return

    links = await get_tasks(user_id)

    if not links:
        await callback.answer(
            "Сначала открой задания спонсоров.",
            show_alert=True,
        )
        return

    try:
        results = await check_sponsors(user_id, links)

        # Require a result for every requested link.
        statuses = {
            item.get("link"): item.get("status")
            for item in results
            if isinstance(item, dict)
        }

        complete = (
            len(statuses) == len(set(links))
            and all(statuses.get(link) == "subscribed" for link in links)
        )

        if not complete:
            await callback.message.answer(
                "❌ Не все задания подтверждены.\n"
                "Выполни оставшиеся задания и попробуй ещё раз."
            )
            await callback.answer()
            return

        credited, referrer_id = await credit_referrer(user_id)

        if credited and referrer_id is not None:
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Реферал выполнил задания!\n"
                    f"Начислено +{REWARD} Stars.\n"
                    "Награда может быть отозвана при отписке "
                    "в течение 48 часов.",
                )
            except Exception:
                log.exception("Could not notify referrer %s", referrer_id)

            await callback.message.answer(
                "✅ Все задания выполнены!\n"
                "Пригласителю начислена награда."
            )
        else:
            user_row = await get_user(user_id)

            if user_row and user_row[1] is None:
                text = (
                    "✅ Все задания выполнены!\n"
                    "Ты пришёл без реферальной ссылки, "
                    "поэтому награда пригласителю не начисляется."
                )
            else:
                text = (
                    "✅ Все задания выполнены!\n"
                    "Награда за этого пользователя уже обработана "
                    "или не может быть начислена."
                )

            await callback.message.answer(text)

    except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
        log.exception("Could not check PiarFlow tasks")
        await callback.message.answer(
            "Не удалось проверить задания. Попробуй позже."
        )

    await callback.answer()


@dp.callback_query(F.data == "referral")
async def referral_handler(callback: CallbackQuery):
    link = f"https://t.me/{bot_username}?start={callback.from_user.id}"
    stats = await get_stats(callback.from_user.id)

    await callback.message.answer(
        "👥 <b>Твоя реферальная ссылка</b>\n\n"
        f"<code>{link}</code>\n\n"
        f"Награда: +{REWARD} Stars за нового реферала.\n"
        f"Активных наград: {stats['active_count']}\n"
        f"Баланс: {stats['balance']} Stars",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data == "balance")
async def balance_handler(callback: CallbackQuery):
    stats = await get_stats(callback.from_user.id)

    await callback.message.answer(
        f"⭐ <b>Баланс: {stats['balance']} Stars</b>\n\n"
        "Это внутренний баланс бота, а не баланс Telegram Stars.",
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data == "stats")
async def stats_handler(callback: CallbackQuery):
    stats = await get_stats(callback.from_user.id)

    await callback.message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"Активных наград: {stats['active_count']}\n"
        f"Начислено по активным наградам: {stats['active_amount']} Stars\n"
        f"Отозвано наград: {stats['revoked_count']}\n"
        f"Сумма отозванных наград: {stats['revoked_amount']} Stars\n"
        f"Текущий баланс: {stats['balance']} Stars",
        parse_mode="HTML",
    )
    await callback.answer()


# =========================================================
# PIARFLOW WEBHOOK
# =========================================================

async def piarflow_webhook(request: web.Request) -> web.Response:
    # The secret path is an additional shared-secret check.
    supplied_secret = request.match_info.get("secret", "")
    if not hmac.compare_digest(supplied_secret, WEBHOOK_SECRET):
        raise web.HTTPNotFound()

    try:
        payload = await request.json()
    except Exception:
        return web.json_response(
            {"ok": False, "error": "invalid_json"},
            status=400,
        )

    # PiarFlow can send a test webhook.
    if payload.get("test"):
        return web.json_response({"ok": True})

    if payload.get("status") != "unsubscribed":
        return web.json_response({"ok": True})

    try:
        referred_id = int(payload["tg_user_id"])
    except (KeyError, TypeError, ValueError):
        return web.json_response(
            {"ok": False, "error": "missing_tg_user_id"},
            status=400,
        )

    offer_link = str(payload.get("offer_link", "unknown"))

    revoked, referrer_id = await revoke_reward(
        referred_id,
        reason=f"Unsubscribed from {offer_link}",
    )

    if revoked and referrer_id is not None:
        try:
            await bot.send_message(
                referrer_id,
                "⚠️ <b>Награда отозвана</b>\n\n"
                "Реферал отписался от задания в течение 48 часов.\n"
                f"Списано: {REWARD} Stars.",
                parse_mode="HTML",
            )
        except Exception:
            log.exception("Could not notify referrer %s", referrer_id)

        log.info(
            "Reward revoked: referred=%s referrer=%s",
            referred_id,
            referrer_id,
        )

    return web.json_response({"ok": True})


async def start_webhook_server():
    app = web.Application()
    app.router.add_post(
        "/piarflow/webhook/{secret}",
        piarflow_webhook,
    )

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    log.info("Webhook server started on port %s", PORT)
    return runner


# =========================================================
# START / SHUTDOWN
# =========================================================

async def main():
    global http, bot_username

    await init_db()

    me = await bot.get_me()
    bot_username = me.username

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:
        http = session
        runner = await start_webhook_server()

        try:
            log.info("Bot started: @%s", bot_username)
            await dp.start_polling(bot)
        finally:
            await runner.cleanup()
            await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
