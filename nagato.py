# -*- coding: utf-8 -*-
import asyncio
import io
import json
import logging
import os
import shutil
import urllib.parse
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
import aiosqlite
import segno
from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramConflictError, TelegramRetryAfter
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Template

# ==========================================
# CONFIG & AUTHENTICATION
# ==========================================
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
DEFAULT_PASS = os.getenv("ADMIN_PASS", "supersecret123")
AUTH_COOKIE_NAME = "session_token"
AUTH_SECRET = "admin_authenticated_session_key_99"

# Persistent path handling for Railway volumes
DATA_DIR = os.getenv("DATA_DIR", "/app/data" if os.path.exists("/app/data") else ".")
DB_NAME = os.path.join(DATA_DIR, "name_database.db")

# Ensure BOTH static and uploads directories exist before mounting
STATIC_DIR = os.path.join(os.getcwd(), "static")
UPLOAD_DIR = os.path.join(STATIC_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

INITIAL_BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
logging.basicConfig(level=logging.INFO)


async def get_admin_ids() -> list[int]:
    chat_id_str = await get_setting("admin_chat_id")
    ids = []
    if chat_id_str:
        for x in chat_id_str.split(","):
            x = x.strip()
            if x.lstrip("-").isdigit():
                ids.append(int(x))
    return ids


async def require_admin(request: Request):
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token != AUTH_SECRET:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    return True


# ==========================================
# DYNAMIC BOT CONTROLLER
# ==========================================
class BotManager:
    def __init__(self):
        self.bot: Bot | None = None
        self.dp: Dispatcher = Dispatcher(storage=MemoryStorage())
        self.polling_task: asyncio.Task | None = None
        self.session: AiohttpSession | None = None

    async def start(self, token: str):
        if not token or token == "YOUR_BOT_TOKEN_HERE":
            logging.warning("[BotManager] No valid token set. Bot is idle.")
            return

        self.session = AiohttpSession(timeout=20.0)
        self.bot = Bot(
            token=token,
            session=self.session,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )

        try:
            await self.bot.delete_webhook(drop_pending_updates=True)
            await self.bot.session.close()
            self.session = AiohttpSession(timeout=20.0)
            self.bot.session = self.session
        except Exception as e:
            logging.warning(f"[BotManager] Initial reset error: {e}")

        async def runner():
            while True:
                try:
                    logging.info("[BotManager] Starting isolated polling loop...")
                    await self.dp.start_polling(
                        self.bot,
                        drop_pending_updates=True,
                        allowed_updates=["message", "callback_query"],
                    )
                    break
                except TelegramConflictError:
                    logging.warning("[BotManager] Polling collision: duplicate session detected. Retrying in 5s...")
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logging.error(f"[BotManager] Polling error: {e}. Retrying in 3s...")
                    await asyncio.sleep(3)

        self.polling_task = asyncio.create_task(runner())

    async def stop(self):
        if self.polling_task and not self.polling_task.done():
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
            self.polling_task = None

        if self.dp:
            try:
                await self.dp.stop_polling()
            except Exception:
                pass

        if self.bot:
            try:
                if self.bot.session:
                    await self.bot.session.close()
            except Exception:
                pass
            self.bot = None

        logging.info("[BotManager] Bot stopped cleanly.")

    async def restart(self, new_token: str):
        await self.stop()
        await self.start(new_token)


manager = BotManager()

# ==========================================
# MESSAGE TRACKING SYSTEM
# ==========================================
user_messages = defaultdict(list)


def track(chat_id: int, message_id: int):
    if message_id not in user_messages[chat_id]:
        user_messages[chat_id].append(message_id)


async def delete_old_messages(chat_id: int, exclude_ids: list[int] | None = None):
    if not manager.bot:
        return
    exclude = set(exclude_ids or [])
    all_ids = [mid for mid in user_messages.get(chat_id, []) if mid not in exclude]
    user_messages[chat_id] = [mid for mid in user_messages.get(chat_id, []) if mid in exclude]

    if not all_ids:
        return

    for i in range(0, len(all_ids), 100):
        chunk = all_ids[i : i + 100]
        try:
            await manager.bot.delete_messages(chat_id=chat_id, message_ids=chunk)
        except TelegramBadRequest:
            for mid in chunk:
                try:
                    await manager.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception:
                    pass
        except Exception as e:
            logging.debug(f"Failed to delete message chunk: {e}")


class MessageTrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.TelegramObject, data: dict):
        if isinstance(event, types.Message):
            track(event.chat.id, event.message_id)
        return await handler(event, data)


# ==========================================
# DATABASE LAYER (WITH BUSY TIMEOUT & PRAGMAS)
# ==========================================
async def get_db_connection():
    db = await aiosqlite.connect(DB_NAME, timeout=10.0)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA busy_timeout = 10000;")
    return db


async def init_db():
    db = await get_db_connection()
    try:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                username TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                premium_status TEXT DEFAULT 'Free',
                is_banned INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                plan_name TEXT,
                amount REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS plans (
                plan_id TEXT PRIMARY KEY,
                name TEXT,
                amount REAL,
                validity TEXT,
                access_link TEXT DEFAULT ''
            )
        """)

        try:
            await db.execute("ALTER TABLE plans ADD COLUMN access_link TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass

        defaults = {
            "bot_token": INITIAL_BOT_TOKEN,
            "admin_password": DEFAULT_PASS,
            "admin_chat_id": "6528792525",
            "maintenance": "off",
            "upi_id": "paytm.s21dj6b@pty",
            "payee_name": "NAZIYA NASRIN",
            "welcome_photo": "https://picsum.photos/800/450",
            "welcome_text": (
                "👋 Welcome to Our Bot!\n\n"
                "✨ Explore features, view demos, check subscriptions, "
                "or manage your account using the buttons below."
            ),
            "plans_text": (
                "📦 Choose Your Membership Plan\n\n"
                "👉 Select any plan below to get an instant UPI QR payment card:"
            ),
            "demo_video": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4",
        }
        for k, v in defaults.items():
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

        default_plans = [
            ("plan_1", "INDIAN WEBSERIES", 99.0, "30 Days", ""),
            ("plan_2", "3 MONTHS SPECIAL", 249.0, "90 Days", ""),
            ("plan_3", "6 MONTHS VIP", 449.0, "180 Days", ""),
            ("plan_4", "1 YEAR ACCESS", 799.0, "365 Days", ""),
            ("plan_5", "LIFETIME PASS", 1299.0, "Lifetime", ""),
            ("plan_6", "4K ULTRA STREAM", 199.0, "30 Days", ""),
            ("plan_7", "PRO PASS", 349.0, "60 Days", ""),
            ("plan_8", "EXCLUSIVE HUB", 599.0, "90 Days", ""),
        ]
        for p in default_plans:
            await db.execute(
                "INSERT OR IGNORE INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)",
                p,
            )
        await db.commit()
    finally:
        await db.close()


async def get_setting(key: str) -> str:
    db = await get_db_connection()
    try:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""
    finally:
        await db.close()


async def update_setting(key: str, value: str):
    db = await get_db_connection()
    try:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()
    finally:
        await db.close()


async def get_demo_video_list() -> list[str]:
    raw = await get_setting("demo_video")
    if not raw:
        return []
    return [line.strip() for line in raw.replace(",", "\n").splitlines() if line.strip()]


async def get_all_plans():
    db = await get_db_connection()
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans ORDER BY plan_id ASC") as cur:
            return await cur.fetchall()
    finally:
        await db.close()


async def get_plan(plan_id: str):
    db = await get_db_connection()
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans WHERE plan_id = ?", (plan_id,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()


async def get_plan_by_name(name: str):
    db = await get_db_connection()
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans WHERE name = ?", (name,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()


async def update_plan(plan_id: str, name: str, amount: float, validity: str, access_link: str = ""):
    db = await get_db_connection()
    try:
        await db.execute(
            "UPDATE plans SET name = ?, amount = ?, validity = ?, access_link = ? WHERE plan_id = ?",
            (name, amount, validity, access_link.strip(), plan_id),
        )
        await db.commit()
    finally:
        await db.close()


async def add_new_plan(plan_id: str, name: str, amount: float, validity: str, access_link: str = ""):
    db = await get_db_connection()
    try:
        await db.execute(
            "INSERT INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)",
            (plan_id, name, amount, validity, access_link.strip()),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_plan(plan_id: str):
    db = await get_db_connection()
    try:
        await db.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
        await db.commit()
    finally:
        await db.close()


async def add_or_update_user(user: types.User):
    db = await get_db_connection()
    try:
        await db.execute(
            """
            INSERT INTO users (user_id, full_name, username)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET full_name = excluded.full_name, username = excluded.username
            """,
            (user.id, user.full_name, user.username or "N/A"),
        )
        await db.commit()
    finally:
        await db.close()


async def get_user(user_id: int):
    db = await get_db_connection()
    try:
        async with db.execute(
            "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users WHERE user_id = ?",
            (user_id,),
        ) as cur:
            return await cur.fetchone()
    finally:
        await db.close()


async def get_all_users_detailed():
    db = await get_db_connection()
    try:
        async with db.execute(
            "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users ORDER BY joined_at DESC"
        ) as cur:
            return await cur.fetchall()
    finally:
        await db.close()


async def update_user_subscription(user_id: int, plan_name: str):
    db = await get_db_connection()
    try:
        await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
        await db.commit()
    finally:
        await db.close()


async def get_dashboard_metrics():
    db = await get_db_connection()
    try:
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM payments WHERE status='approved'") as cur:
            row = await cur.fetchone()
            paid_orders = row[0] if row else 0
            revenue = row[1] if row else 0.0

        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            total_users = (await cur.fetchone())[0]

        async with db.execute("""
            SELECT p.id, p.user_id, COALESCE(u.username, 'N/A'), p.plan_name, p.amount, p.status, p.created_at
            FROM payments p
            LEFT JOIN users u ON p.user_id = u.user_id
            ORDER BY p.id DESC
        """) as cur:
            all_orders = await cur.fetchall()

        return {
            "paid_orders": paid_orders,
            "revenue": f"{revenue:,.2f}",
            "total_users": total_users,
            "recent_orders": all_orders[:20],
            "all_orders": all_orders,
        }
    finally:
        await db.close()


async def get_user_payment_stats(user_id: int):
    db = await get_db_connection()
    try:
        async with db.execute(
            """
            SELECT 
                COUNT(CASE WHEN status = 'approved' THEN 1 END),
                COUNT(CASE WHEN status = 'pending' THEN 1 END),
                COUNT(*)
            FROM payments WHERE user_id = ?
            """,
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
            return {
                "approved": row[0] or 0,
                "pending": row[1] or 0,
                "total": row[2] or 0,
            }
    finally:
        await db.close()


async def set_user_ban_status(user_id: int, is_banned: int):
    db = await get_db_connection()
    try:
        await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (int(is_banned), int(user_id)))
        await db.commit()
    finally:
        await db.close()


async def generate_upi_qr(plan_name: str, amount: float) -> io.BytesIO:
    upi_id = await get_setting("upi_id")
    payee_name = await get_setting("payee_name")
    upi_params = {
        "pa": upi_id,
        "pn": payee_name,
        "am": f"{amount:.2f}",
        "cu": "INR",
        "tn": f"Payment for {plan_name}",
    }
    upi_url = "upi://pay?" + urllib.parse.urlencode(upi_params)
    qr = segno.make(upi_url, error="m")
    buffer = io.BytesIO()
    qr.save(buffer, kind="png", scale=8, border=2)
    buffer.seek(0)
    return buffer


async def detect_payee_name_from_upi(upi_id: str) -> str:
    upi_id = upi_id.strip()
    if "@" not in upi_id:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }

    try:
        url = f"https://upier.vercel.app/api/check?vpa={urllib.parse.quote(upi_id)}"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.5), headers=headers) as s:
            async with s.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    name = data.get("name") or data.get("payeeAccountName")
                    if name and name.strip():
                        return name.strip()
    except Exception:
        pass

    return ""


# ==========================================
# MIDDLEWARE & SECURITY
# ==========================================
class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.TelegramObject, data: dict):
        user = data.get("event_from_user")
        admin_ids = await get_admin_ids()
        if not user or user.id in admin_ids:
            return await handler(event, data)
        user_record = await get_user(user.id)
        if user_record and user_record[5] == 1:
            if isinstance(event, types.Message):
                await event.answer("You are banned from using this bot.")
            return
        if await get_setting("maintenance") == "on":
            if isinstance(event, types.Message):
                await event.answer("Bot is under maintenance. Please try again later.")
            return
        return await handler(event, data)


manager.dp.message.outer_middleware(MessageTrackerMiddleware())
manager.dp.message.outer_middleware(SecurityMiddleware())
manager.dp.callback_query.outer_middleware(SecurityMiddleware())


# ==========================================
# USER-FACING STATES & KEYBOARDS
# ==========================================
class PaymentStates(StatesGroup):
    waiting_for_screenshot = State()


def get_home_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🎬 View Demo", callback_data="btn_view_demo:0")
    builder.button(text="⭐ My Premium", callback_data="btn_my_premium")
    builder.button(text="👤 My Profile", callback_data="btn_my_profile")
    builder.adjust(1)
    return builder.as_markup()


def get_demo_keyboard(current_idx: int, total_videos: int):
    builder = InlineKeyboardBuilder()

    nav_row = []
    if current_idx > 0:
        nav_row.append(types.InlineKeyboardButton(text="◀️ Previous", callback_data=f"btn_view_demo:{current_idx - 1}"))
    if total_videos > 1:
        nav_row.append(types.InlineKeyboardButton(text=f"[{current_idx + 1}/{total_videos}]", callback_data="noop"))
    if current_idx < total_videos - 1:
        nav_row.append(types.InlineKeyboardButton(text="Next ▶️", callback_data=f"btn_view_demo:{current_idx + 1}"))

    if nav_row:
        builder.row(*nav_row)

    builder.row(
        types.InlineKeyboardButton(text="💎 Get Premium", callback_data="btn_get_premium"),
        types.InlineKeyboardButton(text="🏠 Home", callback_data="btn_home"),
    )
    return builder.as_markup()


async def get_plans_keyboard():
    builder = InlineKeyboardBuilder()
    plans = await get_all_plans()
    for pid, name, price, _, _ in plans:
        builder.button(text=f"🔥 {name} (Rs.{int(price)})", callback_data=f"buy_plan:{pid}")
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(*(1 for _ in range(len(plans) + 1)))
    return builder.as_markup()


def get_upi_card_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 CHECK PAYMENT", callback_data="check_payment")
    builder.button(text="🔙 BACK TO PLANS", callback_data="btn_get_premium")
    builder.adjust(1)
    return builder.as_markup()


async def notify_payment_approved(user_id: int, plan_name: str):
    if not manager.bot:
        return
    plan_info = await get_plan_by_name(plan_name)
    access_link = plan_info[4] if plan_info and len(plan_info) > 4 else ""

    builder = InlineKeyboardBuilder()
    if access_link and access_link.strip().startswith(("http://", "https://", "t.me/")):
        link_url = access_link.strip()
        if link_url.startswith("t.me/"):
            link_url = "https://" + link_url
        builder.button(text="🔗 Join VIP Channel / Access Link", url=link_url)
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(1)

    caption = (
        f"🎉 <b>Payment Approved!</b>\n\n"
        f"Your subscription for <b>{plan_name}</b> is now active!\n"
    )
    if access_link:
        caption += "\n👉 Click the button below to claim your access:"

    try:
        await manager.bot.send_message(
            chat_id=user_id,
            text=caption,
            reply_markup=builder.as_markup(),
        )
    except Exception as e:
        logging.warning(f"Could not deliver approval message to {user_id}: {e}")


# ==========================================
# TELEGRAM BOT FLOW HANDLERS
# ==========================================
async def send_welcome_flow(chat_id: int):
    if not manager.bot:
        return
    photo_url = await get_setting("welcome_photo")
    caption = await get_setting("welcome_text")

    sent_photo = False
    if photo_url and photo_url.startswith(("http://", "https://", "AgAC")):
        try:
            async with asyncio.timeout(2.5):
                m1 = await manager.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo_url,
                    caption=caption,
                    reply_markup=get_home_keyboard(),
                )
                track(chat_id, m1.message_id)
                sent_photo = True
        except Exception as e:
            logging.warning(f"[Welcome] Fallback to text: {e}")

    if not sent_photo:
        m1 = await manager.bot.send_message(
            chat_id=chat_id,
            text=caption,
            reply_markup=get_home_keyboard()
        )
        track(chat_id, m1.message_id)

    plans_txt = await get_setting("plans_text")
    m2 = await manager.bot.send_message(
        chat_id=chat_id,
        text=plans_txt,
        reply_markup=await get_plans_keyboard(),
    )
    track(chat_id, m2.message_id)


@manager.dp.message(CommandStart())
async def handle_start(message: types.Message):
    await add_or_update_user(message.from_user)
    await send_welcome_flow(message.chat.id)


@manager.dp.callback_query(F.data == "btn_home")
async def nav_home(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer()
    await delete_old_messages(callback.message.chat.id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_welcome_flow(callback.message.chat.id)


@manager.dp.callback_query(F.data == "btn_get_premium")
async def nav_plans(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return
    text = await get_setting("plans_text")
    msg = await manager.bot.send_message(
        chat_id=callback.message.chat.id,
        text=text,
        reply_markup=await get_plans_keyboard(),
    )
    track(callback.message.chat.id, msg.message_id)


@manager.dp.callback_query(F.data == "noop")
async def handle_noop(callback: types.CallbackQuery):
    await callback.answer()


@manager.dp.callback_query(F.data.startswith("btn_view_demo"))
async def nav_demo(callback: types.CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return

    parts = callback.data.split(":")
    idx = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

    videos = await get_demo_video_list()
    if not videos:
        msg = await manager.bot.send_message(
            chat_id=callback.message.chat.id,
            text="📺 No demo videos available right now.",
            reply_markup=get_demo_keyboard(0, 0),
        )
        track(callback.message.chat.id, msg.message_id)
        return

    idx = max(0, min(idx, len(videos) - 1))
    video_url = videos[idx]

    try:
        async with asyncio.timeout(3.5):
            msg = await manager.bot.send_video(
                chat_id=callback.message.chat.id,
                video=video_url,
                caption=f"📺 <b>Demo Video</b> ({idx + 1}/{len(videos)})",
                reply_markup=get_demo_keyboard(idx, len(videos)),
            )
            track(callback.message.chat.id, msg.message_id)
    except Exception:
        msg = await manager.bot.send_message(
            chat_id=callback.message.chat.id,
            text=f"📺 <b>Demo Video</b> ({idx + 1}/{len(videos)})\n\n🔗 {video_url}",
            reply_markup=get_demo_keyboard(idx, len(videos)),
        )
        track(callback.message.chat.id, msg.message_id)


@manager.dp.callback_query(F.data == "btn_my_premium")
async def nav_my_premium(callback: types.CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return
    user = await get_user(callback.from_user.id)
    plan = user[4] if user else "Free"

    builder = InlineKeyboardBuilder()
    if plan != "Free":
        p_info = await get_plan_by_name(plan)
        if p_info and p_info[4]:
            link = p_info[4].strip()
            if link.startswith("t.me/"):
                link = "https://" + link
            builder.button(text="🔗 Open Premium Channel / Link", url=link)
    builder.button(text="💎 Upgrade", callback_data="btn_get_premium")
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(1)

    text = f"⭐ My Premium Membership\n\nPlan: <b>{plan}</b>\nStatus: <b>{'Active' if plan != 'Free' else 'Free Tier'}</b>"
    msg = await manager.bot.send_message(
        chat_id=callback.message.chat.id,
        text=text,
        reply_markup=builder.as_markup(),
    )
    track(callback.message.chat.id, msg.message_id)


@manager.dp.callback_query(F.data == "btn_my_profile")
async def nav_my_profile(callback: types.CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return
    user = await get_user(callback.from_user.id)
    if not user:
        return
    uid, full_name, username, joined_at, plan, _ = user
    payments = await get_user_payment_stats(uid)
    text = (
        f"👤 MY PROFILE\n"
        f"Name: {full_name}\n"
        f"Username: @{username}\n"
        f"ID: {uid}\n"
        f"Joined: {joined_at}\n"
        f"Plan: {plan}\n"
        f"Payments: Approved: {payments['approved']} | Pending: {payments['pending']}"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="💎 Get Premium", callback_data="btn_get_premium")
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(2)
    msg = await manager.bot.send_message(
        chat_id=callback.message.chat.id,
        text=text,
        reply_markup=builder.as_markup(),
    )
    track(callback.message.chat.id, msg.message_id)


@manager.dp.callback_query(F.data.startswith("buy_plan:"))
async def process_plan_selection(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return

    pid = callback.data.split(":")[1]
    plan = await get_plan(pid)
    if not plan:
        return
    _, plan_name, amount, validity, _ = plan
    qr_buf = await generate_upi_qr(plan_name, amount)
    photo_file = BufferedInputFile(qr_buf.getvalue(), filename="qr.png")
    upi_id = await get_setting("upi_id")
    payee = await get_setting("payee_name")

    formatted_price = f"₹{amount:.2f}"

    caption = (
        "📲 UPI PAYMENT\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        f"📦 PLAN: {plan_name}\n"
        f"💰 AMOUNT: {formatted_price}\n"
        f"⏳ VALITY: {validity}\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        f"👤 NAME: {payee}\n"
        f"📱 UPI ID: <code>{upi_id}</code>\n\n"
        "📋 STEPS:\n"
        "1️⃣ SCAN THE QR CODE ABOVE\n"
        f"2️⃣ {formatted_price} AMOUNT AND NOTE WILL BE BILLED AUTOMATICALLY\n"
        "3️⃣ ENTER YOUR PIN AND COMPLETE PAYMENT\n"
        "4️⃣ TAKE A SCREENSHOT AND CLICK CHECK PAYMENT ✅"
    )

    await state.update_data(current_plan=plan_name, current_amount=amount)
    msg = await manager.bot.send_photo(
        chat_id=callback.message.chat.id,
        photo=photo_file,
        caption=caption,
        parse_mode="HTML",
        reply_markup=get_upi_card_keyboard(),
    )
    track(callback.message.chat.id, msg.message_id)


@manager.dp.callback_query(F.data == "check_payment")
async def handle_check_payment(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return
    await state.set_state(PaymentStates.waiting_for_screenshot)

    text = (
        "📸 SEND PAYMENT SCREENSHOT\n\n"
        "✅ SEND THE SCREENSHOT HERE AFTER COMPLITING UPI PAYMENT.\n\n"
        "⚠️ ONLINE AN IMAGE OR SCREENSHOT IS ACCEPTED\n\n"
        "Type /cancel to abort."
    )

    msg = await manager.bot.send_message(
        chat_id=callback.message.chat.id,
        text=text,
    )
    track(callback.message.chat.id, msg.message_id)


@manager.dp.message(PaymentStates.waiting_for_screenshot, F.photo)
async def process_payment_proof(message: types.Message, state: FSMContext):
    if not manager.bot:
        return
    data = await state.get_data()
    plan_name = data.get("current_plan", "Unknown Plan")
    amount = data.get("current_amount", 0)

    db = await get_db_connection()
    try:
        cur = await db.execute(
            "INSERT INTO payments (user_id, plan_name, amount) VALUES (?, ?, ?)",
            (message.from_user.id, plan_name, amount),
        )
        pid = cur.lastrowid
        await db.commit()
    finally:
        await db.close()

    await state.clear()
    msg = await message.answer(
        "✅ Screenshot Received! Verification is in progress.",
        reply_markup=get_home_keyboard(),
    )
    track(message.chat.id, msg.message_id)

    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Approve", callback_data=f"adm_pay:{pid}:approved")
    builder.button(text="❌ Reject", callback_data=f"adm_pay:{pid}:rejected")
    builder.adjust(2)

    caption = (
        f"🔔 <b>New Payment Screenshot Received</b>\n\n"
        f"<b>Order ID:</b> #{pid}\n"
        f"<b>User ID:</b> <code>{message.from_user.id}</code>\n"
        f"<b>Username:</b> @{message.from_user.username or 'N/A'}\n"
        f"<b>Plan:</b> {plan_name}\n"
        f"<b>Amount:</b> Rs.{amount}"
    )

    photo_file_id = message.photo[-1].file_id
    admin_ids = await get_admin_ids()

    for admin_id in admin_ids:
        try:
            await manager.bot.send_photo(
                chat_id=admin_id,
                photo=photo_file_id,
                caption=caption,
                reply_markup=builder.as_markup(),
            )
        except Exception as e:
            logging.error(f"Failed to send proof to admin {admin_id}: {e}")
            try:
                await manager.bot.send_message(
                    chat_id=admin_id,
                    text=f"{caption}\n\n⚠️ <i>(Screenshot could not be loaded directly)</i>",
                    reply_markup=builder.as_markup(),
                )
            except Exception:
                pass


@manager.dp.message(PaymentStates.waiting_for_screenshot)
async def invalid_proof(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await send_welcome_flow(message.chat.id)
        return
    msg = await message.answer("⚠️ ONLINE AN IMAGE OR SCREENSHOT IS ACCEPTED\n\nPlease upload your payment screenshot image.")
    track(message.chat.id, message.message_id)


# Admin payment approval listener from Telegram notification channel
@manager.dp.callback_query(F.data.startswith("adm_pay:"))
async def handle_admin_pay_approval(callback: types.CallbackQuery):
    if not manager.bot:
        return
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        _, pid, act = callback.data.split(":")
        db = await get_db_connection()
        try:
            async with db.execute(
                "SELECT user_id, plan_name FROM payments WHERE id=?", (int(pid),)
            ) as cur:
                p = await cur.fetchone()
            if not p:
                await callback.answer("Not found.")
                return
            t_uid, pl = p
            if act == "approved":
                await db.execute("UPDATE payments SET status='approved' WHERE id=?", (int(pid),))
                await db.execute("UPDATE users SET premium_status=? WHERE user_id=?", (pl, t_uid))
                await db.commit()

                await notify_payment_approved(t_uid, pl)

                await callback.message.edit_caption(
                    caption=callback.message.caption + "\n\nSTATUS: APPROVED"
                )
            else:
                await db.execute("UPDATE payments SET status='rejected' WHERE id=?", (int(pid),))
                await db.commit()
                try:
                    await manager.bot.send_message(t_uid, "Payment verification failed.")
                except Exception:
                    pass
                await callback.message.edit_caption(
                    caption=callback.message.caption + "\n\nSTATUS: REJECTED"
                )
        finally:
            await db.close()
        await callback.answer("Status updated.")


# ==========================================
# FASTAPI LIFECYCLE & WEB DASHBOARD
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    token = await get_setting("bot_token")
    if token and token != "YOUR_BOT_TOKEN_HERE":
        await manager.start(token)
    yield
    await manager.stop()


app = FastAPI(lifespan=lifespan)
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ==========================================
# TEMPLATES (INCLUDES RESTYLED RESET MODAL + ORDER ACTION CONTROLS)
# ==========================================
LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Admin Login &mdash; Nagato Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;800;900&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #030108; }
        .font-tech { font-family: 'Orbitron', monospace; }
        .exact-login-card {
            background: linear-gradient(180deg, rgba(16, 12, 34, 0.94) 0%, rgba(10, 8, 22, 0.96) 100%);
            border: 1px solid rgba(168, 85, 247, 0.5);
            box-shadow: 0 0 28px rgba(168, 85, 247, 0.35), 0 0 70px rgba(168, 85, 247, 0.15);
            border-radius: 26px;
        }
        .custom-input {
            background-color: #080613;
            border: 1px solid rgba(147, 51, 234, 0.25);
            transition: all 0.2s ease;
        }
        .custom-input:focus {
            outline: none;
            border-color: #38bdf8;
            box-shadow: 0 0 12px rgba(56, 189, 248, 0.3);
        }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex items-center justify-center p-4 relative overflow-hidden">
    <div class="w-full max-w-[370px] relative z-10">
        <div class="exact-login-card p-8 space-y-6">
            <div class="space-y-1">
                <div class="flex items-center gap-2.5">
                    <svg class="w-6 h-6 text-fuchsia-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>
                        <path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>
                        <path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>
                        <path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>
                    </svg>
                    <h1 class="text-xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-purple-300 via-fuchsia-300 to-cyan-300">
                        Nagato Panel
                    </h1>
                </div>
                <div class="font-tech text-[10px] tracking-[0.25em] text-cyan-400/90 font-bold uppercase pl-8">
                    ADMIN PANEL
                </div>
            </div>

            {% if error %}
            <div class="p-3 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-400 text-xs font-mono">
                {{ error }}
            </div>
            {% endif %}

            <form method="POST" action="/login" class="space-y-4 pt-1">
                <div>
                    <label class="block text-xs font-medium text-slate-300 mb-2">Username</label>
                    <input type="text" name="username" required autofocus placeholder=""
                           class="custom-input w-full h-11 rounded-xl px-4 text-sm text-white">
                </div>

                <div>
                    <label class="block text-xs font-medium text-slate-300 mb-2">Password</label>
                    <input type="password" name="password" required placeholder=""
                           class="custom-input w-full h-11 rounded-xl px-4 text-sm text-white">
                </div>

                <button type="submit"
                        class="w-full h-11 mt-3 bg-gradient-to-r from-purple-500 via-fuchsia-500 to-cyan-400 hover:opacity-95 text-white font-semibold rounded-xl text-sm transition shadow-lg shadow-purple-600/30">
                    Sign in &rarr;
                </button>
            </form>

            <div class="pt-2 text-center">
                <a href="https://t.me/NAGATOxOWNER" target="_blank" rel="noopener noreferrer"
                   class="inline-flex items-center gap-1.5 text-xs text-cyan-400/80 hover:text-cyan-300 transition font-mono">
                    <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24">
                        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/>
                    </svg>
                    Contact Developer
                </a>
            </div>
        </div>
    </div>
</body>
</html>"""

DASHBOARD_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard &mdash; Nagato Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #06040c; margin: 0; padding: 0; overflow-x: hidden; }
        .font-tech { font-family: 'Orbitron', monospace; }
        .glass-card {
            background: linear-gradient(135deg, rgba(22, 12, 42, 0.85) 0%, rgba(13, 8, 25, 0.92) 100%);
            border: 1px solid rgba(139, 92, 246, 0.25);
        }
        .neon-border-pink {
            border-color: rgba(255, 0, 127, 0.5) !important;
            box-shadow: 0 0 15px rgba(255, 0, 127, 0.2);
        }
        #sidebar {
            position: fixed;
            top: 0;
            left: 0;
            bottom: 0;
            width: 260px;
            background-color: #090614;
            border-right: 1px solid rgba(139, 92, 246, 0.25);
            z-index: 50;
            transition: transform 0.25s ease;
            transform: translateX(-100%);
        }
        #sidebar.open {
            transform: translateX(0);
        }
        @media (min-width: 768px) {
            #sidebar {
                position: static;
                transform: translateX(0) !important;
                height: 100vh;
            }
        }
        #sidebarBackdrop {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,0.7);
            backdrop-filter: blur(4px);
            z-index: 40;
        }
        #sidebarBackdrop.open {
            display: block;
        }

        .modal-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.75);
            backdrop-filter: blur(6px);
            z-index: 99;
            align-items: center;
            justify-content: center;
            padding: 1rem;
        }
        .modal-overlay.active {
            display: flex;
        }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex">
    <div id="sidebarBackdrop" onclick="toggleSidebar()"></div>

    <!-- Designed Cyberpunk Revenue Reset Modal -->
    <div id="confirmResetModal" class="modal-overlay">
        <div class="glass-card max-w-sm w-full p-6 rounded-2xl border border-amber-500/40 shadow-[0_0_30px_rgba(245,158,11,0.25)] space-y-5">
            <div class="flex items-center gap-3 border-b border-purple-900/40 pb-3">
                <span class="p-2 rounded-xl bg-amber-500/10 text-amber-400 border border-amber-500/30">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>
                </span>
                <div>
                    <h4 class="font-tech text-sm font-bold text-white tracking-wide">Reset Revenue</h4>
                    <p class="text-[11px] text-purple-400/80 font-mono">Zero out financial archive</p>
                </div>
            </div>

            <p class="text-xs text-slate-300 leading-relaxed font-sans">
                Are you sure you want to reset all revenue counters? This will clear all recorded payment logs permanently.
            </p>

            <form method="POST" action="/admin/revenue/reset" class="flex gap-3 pt-2">
                <button type="button" onclick="closeResetModal()" class="flex-1 bg-purple-900/40 hover:bg-purple-900/60 text-slate-300 font-tech text-xs py-2.5 rounded-xl transition">
                    Cancel
                </button>
                <button type="submit" class="flex-1 bg-gradient-to-r from-amber-600 to-yellow-600 hover:from-amber-500 hover:to-yellow-500 text-white font-tech font-bold text-xs py-2.5 rounded-xl uppercase tracking-wider shadow-lg shadow-amber-600/30 transition">
                    Confirm Reset
                </button>
            </form>
        </div>
    </div>

    <!-- Designed Cyberpunk Delete Plan Modal -->
    <div id="confirmDeleteModal" class="modal-overlay">
        <div class="glass-card max-w-sm w-full p-6 rounded-2xl border border-rose-500/40 shadow-[0_0_30px_rgba(244,63,94,0.25)] space-y-5">
            <div class="flex items-center gap-3 border-b border-purple-900/40 pb-3">
                <span class="p-2 rounded-xl bg-rose-500/10 text-rose-400 border border-rose-500/30">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                </span>
                <div>
                    <h4 class="font-tech text-sm font-bold text-white tracking-wide">Confirm Deletion</h4>
                    <p class="text-[11px] text-purple-400/80 font-mono">This action is permanent.</p>
                </div>
            </div>

            <p class="text-xs text-slate-300 leading-relaxed font-sans">
                Are you sure you want to delete <span id="modalPlanName" class="text-cyan-400 font-semibold"></span> (<span id="modalPlanId" class="text-purple-300 font-mono"></span>)?
            </p>

            <form id="modalDeleteForm" method="POST" action="/admin/plans/delete" class="flex gap-3 pt-2">
                <input type="hidden" id="modalPlanIdInput" name="plan_id" value="">
                <button type="button" onclick="closeDeleteModal()" class="flex-1 bg-purple-900/40 hover:bg-purple-900/60 text-slate-300 font-tech text-xs py-2.5 rounded-xl transition">
                    Cancel
                </button>
                <button type="submit" class="flex-1 bg-gradient-to-r from-rose-600 to-red-600 hover:from-rose-500 hover:to-red-500 text-white font-tech font-bold text-xs py-2.5 rounded-xl uppercase tracking-wider shadow-lg shadow-rose-600/30 transition">
                    Delete
                </button>
            </form>
        </div>
    </div>

    <!-- Navigation Drawer with Restored Icons -->
    <aside id="sidebar" class="p-5 flex flex-col justify-between overflow-y-auto">
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                    <svg class="w-6 h-6 text-fuchsia-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>
                        <path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>
                        <path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>
                        <path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>
                    </svg>
                    <div>
                        <span class="font-tech font-bold text-sm text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300 tracking-wider block">Nagato Panel</span>
                        <span class="font-tech text-[10px] tracking-wider text-cyan-300 uppercase block">Pom Pom Bot</span>
                    </div>
                </div>
                <button type="button" onclick="toggleSidebar()" class="md:hidden text-purple-400 hover:text-white p-1">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>

            <nav class="space-y-1.5 text-xs">
                <button type="button" onclick="switchTab('tab-dashboard')" id="nav-tab-dashboard" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-semibold bg-purple-900/40 border border-fuchsia-500/30 text-cyan-400 shadow-[0_0_12px_rgba(0,240,255,0.15)] transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
                    Dashboard
                </button>
                <button type="button" onclick="switchTab('tab-orders')" id="nav-tab-orders" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/></svg>
                    Orders
                </button>
                <button type="button" onclick="switchTab('tab-users')" id="nav-tab-users" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/></svg>
                    Manage Users
                </button>
                <button type="button" onclick="switchTab('tab-broadcast')" id="nav-tab-broadcast" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z"/></svg>
                    Broadcast
                </button>
                <button type="button" onclick="switchTab('tab-settings')" id="nav-tab-settings" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
                    Setting &amp; UPI
                </button>
                <button type="button" onclick="switchTab('tab-bot')" id="nav-tab-bot" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"/></svg>
                    Bot Token Config
                </button>
                <button type="button" onclick="switchTab('tab-media')" id="nav-tab-media" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
                    Media &amp; Greetings
                </button>
                <button type="button" onclick="switchTab('tab-plans')" id="nav-tab-plans" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                    Subscription Plans
                </button>
            </nav>
        </div>

        <div class="pt-4 border-t border-purple-900/40 space-y-3">
            <a href="/logout" class="w-full flex items-center justify-center gap-2 py-2 rounded-xl text-xs font-mono font-semibold text-rose-400 hover:bg-rose-500/10 transition">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/></svg>
                Sign Out
            </a>
        </div>
    </aside>

    <!-- Main Content Container -->
    <main class="flex-1 flex flex-col min-w-0 h-screen overflow-y-auto">
        <header class="sticky top-0 z-20 bg-[#06040c]/95 backdrop-blur-md border-b border-purple-900/40 px-4 md:px-8 py-3.5 flex items-center justify-between">
            <div class="flex items-center gap-3">
                <button type="button" onclick="toggleSidebar()" class="p-2 rounded-lg bg-[#120b22] border border-purple-900/40 text-purple-300 hover:text-white focus:outline-none">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>
                </button>
                <h2 id="sectionTitle" class="font-tech text-base md:text-lg font-bold text-white tracking-wider">Dashboard</h2>
            </div>
            
            <div class="flex items-center gap-3">
                <span class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#120b22] border border-cyan-500/40 text-xs font-mono text-cyan-300">
                    <span class="w-2 h-2 rounded-full {{ 'bg-cyan-400' if is_online else 'bg-amber-400' }} animate-pulse inline-block"></span>
                    {{ 'ONLINE' if is_online else 'STANDBY' }}
                </span>
            </div>
        </header>

        <div class="p-4 md:p-8 max-w-5xl w-full mx-auto space-y-6">
            {% if message %}
            <div class="p-4 rounded-xl bg-cyan-500/10 border border-cyan-500/40 text-cyan-300 text-xs font-mono">
                {{ message }}
            </div>
            {% endif %}

            <!-- TAB: DASHBOARD -->
            <div id="tab-dashboard" class="tab-content space-y-6">
                <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">{{ paid_orders }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Paid orders</p>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <div class="flex justify-between items-center">
                            <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">&#8377;{{ revenue }}</span>
                            <button type="button" onclick="openResetModal()" class="text-[10px] text-purple-400 hover:text-fuchsia-300 font-mono transition">
                                Reset
                            </button>
                        </div>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Revenue</p>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">{{ total_users }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Total users</p>
                    </div>

                    <!-- CARD: ADMIN PANEL USERNAME -->
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between neon-border-pink">
                        <div class="flex justify-between items-center mb-1">
                            <span class="p-1.5 rounded-lg bg-fuchsia-500/10 text-fuchsia-400 border border-fuchsia-500/30">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5.121 17.804A13.937 13.937 0 0112 16c2.5 0 4.847.655 6.879 1.804M15 10a3 3 0 11-6 0 3 3 0 016 0zm6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                                </svg>
                            </span>
                            <span class="text-[10px] font-tech px-2 py-0.5 rounded-md bg-fuchsia-500/20 text-fuchsia-300 border border-fuchsia-500/40">
                                ADMIN
                            </span>
                        </div>
                        <div>
                            <span class="font-tech text-base md:text-lg font-black text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300 truncate block">
                                {{ admin_username }}
                            </span>
                            <p class="text-[10px] text-purple-400 mt-0.5 font-mono">Control Panel</p>
                        </div>
                    </div>
                </div>

                <!-- Recent Orders Table -->
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="flex items-center justify-between border-b border-purple-900/40 pb-3">
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Recent orders</h3>
                        <button type="button" onclick="switchTab('tab-orders')" class="text-xs text-fuchsia-400 hover:text-fuchsia-300 font-mono">View All &rarr;</button>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">Order</th>
                                    <th class="py-2.5 px-3">User</th>
                                    <th class="py-2.5 px-3">Item</th>
                                    <th class="py-2.5 px-3">Amount</th>
                                    <th class="py-2.5 px-3 text-right">Status</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for oid, uid, uname, item, amt, st, dt in recent_orders %}
                                <tr>
                                    <td class="py-2.5 px-3 text-cyan-400">FT{{ oid }}ORD</td>
                                    <td class="py-2.5 px-3">{{ uid }}<span class="block text-[10px] text-purple-400">@{{ uname }}</span></td>
                                    <td class="py-2.5 px-3 text-white">{{ item }}</td>
                                    <td class="py-2.5 px-3 text-white font-semibold">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-2.5 px-3 text-right">
                                        <span class="px-2 py-0.5 rounded text-[10px] uppercase font-tech {{ 'text-emerald-400 bg-emerald-500/10' if st == 'approved' else ('text-amber-400 bg-amber-500/10' if st == 'pending' else 'text-rose-400 bg-rose-500/10') }}">{{ st }}</span>
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="5" class="py-6 text-center text-purple-400">No orders recorded yet.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: CUSTOMER ORDERS (WITH DIRECT ACCEPT / REJECT) -->
            <div id="tab-orders" class="tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white tracking-wide border-b border-purple-900/40 pb-3">All Customer Orders</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono text-slate-300 border-collapse">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">Order</th>
                                    <th class="py-2.5 px-3">User</th>
                                    <th class="py-2.5 px-3">Item</th>
                                    <th class="py-2.5 px-3">Amount</th>
                                    <th class="py-2.5 px-3">Date</th>
                                    <th class="py-2.5 px-3">Status</th>
                                    <th class="py-2.5 px-3 text-right">Action</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30">
                                {% for oid, uid, uname, item, amt, st, dt in all_orders %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-2.5 px-3 text-cyan-400 align-middle">FT{{ oid }}ORD</td>
                                    <td class="py-2.5 px-3 align-middle">{{ uid }}<span class="block text-[10px] text-purple-400">@{{ uname }}</span></td>
                                    <td class="py-2.5 px-3 text-white align-middle">{{ item }}</td>
                                    <td class="py-2.5 px-3 font-semibold text-white align-middle">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-2.5 px-3 text-purple-300/80 text-[11px] align-middle">{{ dt }}</td>
                                    <td class="py-2.5 px-3 align-middle">
                                        <span class="px-2 py-0.5 rounded text-[10px] uppercase font-tech {{ 'text-emerald-400 bg-emerald-500/10' if st == 'approved' else ('text-amber-400 bg-amber-500/10' if st == 'pending' else 'text-rose-400 bg-rose-500/10') }}">
                                            {{ st }}
                                        </span>
                                    </td>
                                    <td class="py-2.5 px-3 text-right align-middle">
                                        {% if st == 'pending' %}
                                        <div class="flex items-center justify-end gap-1.5">
                                            <form method="POST" action="/admin/orders/status">
                                                <input type="hidden" name="order_id" value="{{ oid }}">
                                                <input type="hidden" name="action" value="approved">
                                                <button type="submit" class="bg-emerald-500/20 hover:bg-emerald-600 text-emerald-400 hover:text-white font-tech text-[10px] px-2.5 py-1 rounded-lg uppercase tracking-wider transition">
                                                    Accept
                                                </button>
                                            </form>
                                            <form method="POST" action="/admin/orders/status">
                                                <input type="hidden" name="order_id" value="{{ oid }}">
                                                <input type="hidden" name="action" value="rejected">
                                                <button type="submit" class="bg-rose-500/20 hover:bg-rose-600 text-rose-400 hover:text-white font-tech text-[10px] px-2.5 py-1 rounded-lg uppercase tracking-wider transition">
                                                    Reject
                                                </button>
                                            </form>
                                        </div>
                                        {% else %}
                                        <span class="text-purple-400/50 text-[11px] uppercase font-tech font-bold">Processed</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="7" class="py-8 text-center text-purple-400">No orders recorded in database.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: MANAGE USERS -->
            <div id="tab-users" class="tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white tracking-wide border-b border-purple-900/40 pb-3">Registered Users</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">User ID</th>
                                    <th class="py-2.5 px-3">Username</th>
                                    <th class="py-2.5 px-3">Subscription</th>
                                    <th class="py-2.5 px-3 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for u_id, u_fname, u_uname, u_joined, u_status, u_banned in users_list %}
                                <tr>
                                    <td class="py-2.5 px-3 text-cyan-400">{{ u_id }}<span class="block text-[10px] text-white">{{ u_fname }}</span></td>
                                    <td class="py-2.5 px-3"><a href="https://t.me/{{ u_uname }}" target="_blank" class="text-fuchsia-400">@{{ u_uname }}</a></td>
                                    <td class="py-2.5 px-3">
                                        <form method="POST" action="/admin/users/subscription" class="flex items-center gap-1.5">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <select name="plan_name" class="bg-[#070410] border border-purple-900/60 rounded px-2 py-1 text-[11px] text-white">
                                                <option value="Free" {{ 'selected' if u_status == 'Free' else '' }}>Free</option>
                                                {% for p_id, p_name, _, _, _ in plans %}
                                                <option value="{{ p_name }}" {{ 'selected' if u_status == p_name else '' }}>{{ p_name }}</option>
                                                {% endfor %}
                                            </select>
                                            <button type="submit" class="bg-purple-900/60 px-2 py-1 rounded text-[10px] font-tech text-white">SET</button>
                                        </form>
                                    </td>
                                    <td class="py-2.5 px-3 text-right">
                                        <form method="POST" action="/admin/users/ban">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <input type="hidden" name="status" value="{{ 0 if u_banned else 1 }}">
                                            <button type="submit" class="px-2.5 py-1 rounded text-[10px] font-tech {{ 'bg-emerald-500/20 text-emerald-400' if u_banned else 'bg-rose-500/20 text-rose-400' }}">
                                                {{ 'UNBAN' if u_banned else 'BAN' }}
                                            </button>
                                        </form>
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="4" class="py-8 text-center text-purple-400">No users found.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: BROADCAST -->
            <div id="tab-broadcast" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/broadcast/send" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Transmit Broadcast</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Message Payload</label>
                        <textarea name="broadcast_message" rows="4" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white focus:outline-none"></textarea>
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Photo URL (Optional)</label>
                        <input type="url" name="broadcast_photo" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 text-white font-tech font-bold px-6 py-2.5 rounded-xl text-xs uppercase">Send Broadcast</button>
                </form>
            </div>

            <!-- TAB: SETTINGS & UPI -->
            <div id="tab-settings" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/settings/upi" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">UPI Settings</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">UPI ID</label>
                        <input type="text" id="upi_id_input" name="upi_id" value="{{ upi_id }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white font-mono focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Payee Name</label>
                        <input type="text" id="payee_name_input" name="payee_name" value="{{ payee_name }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Admin Telegram Chat ID</label>
                        <input type="text" name="admin_chat_id" value="{{ admin_chat_id }}" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white font-mono focus:outline-none">
                    </div>
                    <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 text-white font-tech font-bold px-5 py-2.5 rounded-xl text-xs uppercase">Save UPI</button>
                </form>

                <form method="POST" action="/admin/password/update" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Security Key</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Current Password</label>
                        <input type="password" name="current_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">New Password</label>
                        <input type="password" name="new_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <button type="submit" class="bg-purple-900/60 text-white font-tech px-5 py-2 rounded-xl text-xs">Update Password</button>
                </form>
            </div>

            <!-- TAB: BOT TOKEN -->
            <form method="POST" action="/admin/save">
                <div id="tab-bot" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-4">
                        <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Telegram Bot API</h3>
                        <div>
                            <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Bot Token</label>
                            <input type="text" name="bot_token" value="{{ bot_token }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none">
                        </div>
                    </div>
                </div>

                <!-- TAB: MEDIA & GREETINGS -->
                <div id="tab-media" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-4">
                        <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Media &amp; Interface Messages</h3>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Welcome Greeting</label>
                            <textarea name="welcome_text" rows="3" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white">{{ welcome_text }}</textarea>
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Plans Header Text</label>
                            <textarea name="plans_text" rows="2" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white">{{ plans_text }}</textarea>
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Welcome Image URL</label>
                            <input type="url" name="welcome_photo" value="{{ welcome_photo }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white">
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Demo Videos (1 URL per line)</label>
                            <textarea name="demo_video" rows="4" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white font-mono">{{ demo_video }}</textarea>
                        </div>
                    </div>

                    <!-- Direct Video File Upload -->
                    <div class="glass-card rounded-2xl p-6 space-y-3">
                        <h4 class="font-tech text-xs font-bold text-cyan-300 uppercase">Direct Video Upload</h4>
                        <div class="flex items-center gap-3">
                            <input type="file" id="demo_file_input" accept="video/mp4,video/*" class="text-xs text-purple-300">
                            <button type="button" onclick="uploadDemoVideoFile()" class="bg-cyan-600 text-white font-tech text-xs px-4 py-2 rounded-xl">Upload</button>
                        </div>
                        <p id="uploadStatus" class="text-xs font-mono"></p>
                    </div>
                </div>

                <div id="saveBar" class="pt-4 hidden">
                    <button type="submit" class="w-full bg-gradient-to-r from-fuchsia-600 via-purple-600 to-cyan-600 text-white font-tech font-bold py-3 rounded-xl uppercase tracking-wider">
                        Save Interface Parameters
                    </button>
                </div>
            </form>

            <!-- TAB: SUBSCRIPTION PLANS -->
            <div id="tab-plans" class="tab-content space-y-5 hidden">
                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Add Subscription Tier</h3>
                    <form method="POST" action="/admin/plans/add" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 text-xs font-mono">
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Plan ID</label>
                            <input type="text" name="plan_id" required placeholder="plan_9" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Tier Name</label>
                            <input type="text" name="name" required placeholder="VIP ACCESS" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Price (Rs.)</label>
                            <input type="number" step="any" name="amount" required placeholder="499" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Validity</label>
                            <input type="text" name="validity" required placeholder="60 Days" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Channel Link (Optional)</label>
                            <input type="text" name="access_link" placeholder="https://t.me/+..." class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div class="sm:col-span-2 lg:col-span-5">
                            <button type="submit" class="bg-cyan-600 text-white font-tech font-bold py-2.5 px-6 rounded-xl uppercase tracking-wider">+ Add Plan</button>
                        </div>
                    </form>
                </div>

                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Active Plans</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono text-slate-300 border-collapse">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="p-3">Plan Key</th>
                                    <th class="p-3">Title</th>
                                    <th class="p-3">Price (Rs.)</th>
                                    <th class="p-3">Validity</th>
                                    <th class="p-3">Premium Link</th>
                                    <th class="p-3 text-center">Update Action</th>
                                    <th class="p-3 text-center">Delete Action</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30">
                                {% for pid, name, amount, validity, access_link in plans %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="p-3 text-cyan-400 align-middle">{{ pid }}</td>
                                    
                                    <form method="POST" action="/admin/plans/update">
                                        <input type="hidden" name="plan_id" value="{{ pid }}">
                                        <td class="p-3 align-middle">
                                            <input type="text" name="name" value="{{ name }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white min-w-[130px]">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="number" step="any" name="amount" value="{{ amount }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white w-20">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="text" name="validity" value="{{ validity }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white w-24">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="text" name="access_link" value="{{ access_link }}" placeholder="https://t.me/+..." class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white min-w-[140px]">
                                        </td>
                                        <td class="p-3 text-center align-middle">
                                            <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white font-tech text-[10px] px-3.5 py-1.5 rounded-lg uppercase tracking-wider transition">
                                                Update
                                            </button>
                                        </td>
                                    </form>

                                    <!-- Trigger custom modal -->
                                    <td class="p-3 text-center align-middle">
                                        <button type="button" onclick="openDeleteModal('{{ pid }}', '{{ name }}')" class="bg-rose-500/20 hover:bg-rose-600 text-rose-400 hover:text-white font-tech text-[10px] px-3.5 py-1.5 rounded-lg uppercase tracking-wider transition">
                                            Delete
                                        </button>
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

        </div>
    </main>

    <!-- Reliable Vanilla Scripts -->
    <script>
        function toggleSidebar() {
            var sidebar = document.getElementById('sidebar');
            var backdrop = document.getElementById('sidebarBackdrop');
            if (!sidebar) return;
            sidebar.classList.toggle('open');
            if (backdrop) backdrop.classList.toggle('open');
        }

        var titles = {
            'tab-dashboard': 'Dashboard',
            'tab-orders': 'Customer Orders',
            'tab-users': 'Manage Users',
            'tab-broadcast': 'Mass Broadcast',
            'tab-settings': 'Setting & UPI',
            'tab-bot': 'Bot Token Config',
            'tab-media': 'Media & Greetings',
            'tab-plans': 'Subscription Plans'
        };

        function switchTab(tabId) {
            var contents = document.querySelectorAll('.tab-content');
            for (var i = 0; i < contents.length; i++) {
                contents[i].classList.add('hidden');
            }

            var target = document.getElementById(tabId);
            if (target) target.classList.remove('hidden');

            var saveBar = document.getElementById('saveBar');
            if (saveBar) {
                if (tabId === 'tab-bot' || tabId === 'tab-media') {
                    saveBar.classList.remove('hidden');
                } else {
                    saveBar.classList.add('hidden');
                }
            }

            var titleEl = document.getElementById('sectionTitle');
            if (titleEl) titleEl.innerText = titles[tabId] || 'Nagato Panel';

            var navBtns = document.querySelectorAll('.nav-btn');
            for (var j = 0; j < navBtns.length; j++) {
                navBtns[j].classList.remove('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                navBtns[j].classList.add('text-slate-400');
            }

            var activeNav = document.getElementById('nav-' + tabId);
            if (activeNav) {
                activeNav.classList.add('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                activeNav.classList.remove('text-slate-400');
            }

            if (window.innerWidth < 768) {
                var sidebar = document.getElementById('sidebar');
                if (sidebar && sidebar.classList.contains('open')) {
                    toggleSidebar();
                }
            }
        }

        var urlParams = new URLSearchParams(window.location.search);
        var requestedTab = urlParams.get('tab');
        if (requestedTab && titles[requestedTab]) {
            switchTab(requestedTab);
        }

        function openDeleteModal(planId, planName) {
            document.getElementById('modalPlanIdInput').value = planId;
            document.getElementById('modalPlanId').innerText = planId;
            document.getElementById('modalPlanName').innerText = planName;
            document.getElementById('confirmDeleteModal').classList.add('active');
        }

        function closeDeleteModal() {
            document.getElementById('confirmDeleteModal').classList.remove('active');
        }

        function openResetModal() {
            document.getElementById('confirmResetModal').classList.add('active');
        }

        function closeResetModal() {
            document.getElementById('confirmResetModal').classList.remove('active');
        }

        async function uploadDemoVideoFile() {
            var input = document.getElementById('demo_file_input');
            var statusEl = document.getElementById('uploadStatus');
            if (!input.files || input.files.length === 0) {
                statusEl.innerText = "Please select a file first.";
                statusEl.className = "text-xs text-rose-400 font-mono";
                return;
            }
            statusEl.innerText = "Uploading...";
            statusEl.className = "text-xs text-cyan-400 font-mono";

            var formData = new FormData();
            formData.append("file", input.files[0]);

            try {
                var res = await fetch('/admin/upload-demo-video', {
                    method: 'POST',
                    body: formData
                });
                var data = await res.json();
                if (data.status === 'success') {
                    statusEl.innerText = "Uploaded!";
                    statusEl.className = "text-xs text-emerald-400 font-mono";
                    var txtArea = document.querySelector('textarea[name="demo_video"]');
                    txtArea.value = (txtArea.value.trim() + "\\n" + data.url).trim();
                } else {
                    statusEl.innerText = "Error: " + data.message;
                    statusEl.className = "text-xs text-rose-400 font-mono";
                }
            } catch(e) {
                statusEl.innerText = "Upload failed: " + e;
                statusEl.className = "text-xs text-rose-400 font-mono";
            }
        }
    </script>
</body>
</html>"""


# ==========================================
# FASTAPI ROUTES & API ENDPOINTS
# ==========================================
@app.get("/login", response_class=HTMLResponse)
async def login_form(request: Request, error: str | None = None):
    if request.cookies.get(AUTH_COOKIE_NAME) == AUTH_SECRET:
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    tmpl = Template(LOGIN_PAGE)
    return HTMLResponse(content=tmpl.render(error=error))


@app.post("/login")
async def process_login(username: str = Form(...), password: str = Form(...)):
    stored_password = await get_setting("admin_password") or DEFAULT_PASS
    if username == ADMIN_USER and password == stored_password:
        response = RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
        response.set_cookie(key=AUTH_COOKIE_NAME, value=AUTH_SECRET, httponly=True, max_age=86400 * 7)
        return response
    tmpl = Template(LOGIN_PAGE)
    return HTMLResponse(content=tmpl.render(error="Invalid administrator credentials."), status_code=status.HTTP_401_UNAUTHORIZED)


@app.get("/logout")
async def logout_admin():
    response = RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(key=AUTH_COOKIE_NAME)
    return response


@app.post("/admin/upload-demo-video")
async def upload_demo_video_file(
    request: Request,
    file: UploadFile = File(...),
    is_auth: bool = Depends(require_admin),
):
    try:
        clean_name = f"{int(datetime.now().timestamp())}_{file.filename.replace(' ', '_')}"
        dest_path = os.path.join(UPLOAD_DIR, clean_name)
        with open(dest_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        base_url = str(request.base_url).rstrip("/")
        file_url = f"{base_url}/static/uploads/{clean_name}"

        curr = await get_demo_video_list()
        curr.append(file_url)
        await update_setting("demo_video", "\n".join(curr))

        return JSONResponse({"status": "success", "url": file_url})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/api/detect-upi-name")
async def api_detect_upi_name(upi_id: str, is_auth: bool = Depends(require_admin)):
    name = await detect_payee_name_from_upi(upi_id)
    if name:
        return JSONResponse(content={"status": "success", "upi_id": upi_id, "name": name})
    return JSONResponse(
        content={
            "status": "failed",
            "message": "Live NPCI lookup blocked on cloud server. Please enter your name manually.",
        }
    )


@app.get("/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, message: str | None = None, is_auth: bool = Depends(require_admin)):
    try:
        metrics = await get_dashboard_metrics()
        users_list = await get_all_users_detailed()
        tmpl = Template(DASHBOARD_PAGE)
        html = tmpl.render(
            message=message,
            admin_username=ADMIN_USER,
            is_online=manager.bot is not None,
            paid_orders=metrics.get("paid_orders", 0),
            revenue=metrics.get("revenue", "0.00"),
            total_users=metrics.get("total_users", 0),
            recent_orders=metrics.get("recent_orders", []),
            all_orders=metrics.get("all_orders", []),
            users_list=users_list or [],
            bot_token=await get_setting("bot_token"),
            admin_chat_id=await get_setting("admin_chat_id"),
            upi_id=await get_setting("upi_id"),
            payee_name=await get_setting("payee_name"),
            maintenance=await get_setting("maintenance"),
            welcome_text=await get_setting("welcome_text"),
            plans_text=await get_setting("plans_text"),
            welcome_photo=await get_setting("welcome_photo"),
            demo_video=await get_setting("demo_video"),
            plans=await get_all_plans(),
        )
        return HTMLResponse(content=html)
    except Exception as err:
        logging.error(f"Dashboard render error: {err}")
        return HTMLResponse(f"<h3>Dashboard Error: {err}</h3>", status_code=500)


@app.post("/admin/broadcast/send")
async def handle_admin_broadcast(
    broadcast_message: str = Form(...),
    broadcast_photo: str = Form(""),
    is_auth: bool = Depends(require_admin),
):
    if not manager.bot:
        return RedirectResponse(
            url="/?message=Error:+Bot+is+offline.+Add+token+first.&tab=tab-broadcast",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    db = await get_db_connection()
    try:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as cur:
            users = await cur.fetchall()
    finally:
        await db.close()

    sent = 0
    cleaned_photo = broadcast_photo.strip()

    for (uid,) in users:
        try:
            if cleaned_photo:
                await manager.bot.send_photo(chat_id=uid, photo=cleaned_photo, caption=broadcast_message)
            else:
                await manager.bot.send_message(chat_id=uid, text=broadcast_message)
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                if cleaned_photo:
                    await manager.bot.send_photo(chat_id=uid, photo=cleaned_photo, caption=broadcast_message)
                else:
                    await manager.bot.send_message(chat_id=uid, text=broadcast_message)
                sent += 1
            except Exception:
                pass
        except Exception:
            pass

    return RedirectResponse(
        url=f"/?message=Broadcast+successfully+sent+to+{sent}+users!&tab=tab-broadcast",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.post("/admin/users/ban")
async def handle_user_ban_toggle(
    request: Request,
    is_auth: bool = Depends(require_admin),
):
    try:
        form = await request.form()
        user_id_raw = form.get("user_id")
        status_raw = form.get("status")

        if not user_id_raw or status_raw is None:
            raise ValueError("Missing form fields")

        user_id = int(user_id_raw)
        status_int = int(status_raw)

        await set_user_ban_status(user_id, status_int)
        action_text = "banned" if status_int == 1 else "unbanned"
        msg = f"User {user_id} {action_text} successfully"
    except Exception as e:
        logging.error(f"Ban toggle error: {e}")
        msg = "Error updating user status"

    return RedirectResponse(
        url=f"/?message={urllib.parse.quote_plus(msg)}&tab=tab-users",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.post("/admin/users/subscription")
async def handle_user_subscription_change(
    request: Request,
    is_auth: bool = Depends(require_admin),
):
    try:
        form = await request.form()
        user_id_raw = form.get("user_id")
        plan_name = str(form.get("plan_name", "Free")).strip()

        if not user_id_raw:
            raise ValueError("Missing user_id")

        user_id = int(user_id_raw)
        await update_user_subscription(user_id, plan_name)
        if manager.bot:
            if plan_name == "Free":
                try:
                    await manager.bot.send_message(user_id, "Your premium subscription has ended. You are now on the Free tier.")
                except Exception:
                    pass
            else:
                await notify_payment_approved(user_id, plan_name)
        msg = f"User {user_id} subscription updated to {plan_name}"
    except Exception as e:
        logging.error(f"Subscription update error: {e}")
        msg = "Error updating subscription"

    return RedirectResponse(
        url=f"/?message={urllib.parse.quote_plus(msg)}&tab=tab-users",
        status_code=status.HTTP_303_SEE_OTHER,
    )


# NEW: Direct Accept / Reject for Orders via Admin Panel
@app.post("/admin/orders/status")
async def handle_admin_order_status_change(
    order_id: int = Form(...),
    action: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    db = await get_db_connection()
    try:
        async with db.execute("SELECT user_id, plan_name FROM payments WHERE id = ?", (int(order_id),)) as cur:
            row = await cur.fetchone()
        if not row:
            return RedirectResponse(url="/?message=Order+not+found&tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

        user_id, plan_name = row

        if action == "approved":
            await db.execute("UPDATE payments SET status = 'approved' WHERE id = ?", (int(order_id),))
            await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
            await db.commit()

            await notify_payment_approved(user_id, plan_name)
            msg = f"Order #{order_id} approved and user upgraded!"
        else:
            await db.execute("UPDATE payments SET status = 'rejected' WHERE id = ?", (int(order_id),))
            await db.commit()
            if manager.bot:
                try:
                    await manager.bot.send_message(user_id, "Payment verification failed. Your order has been rejected.")
                except Exception:
                    pass
            msg = f"Order #{order_id} rejected."
    except Exception as e:
        logging.error(f"Order status change error: {e}")
        msg = "Error changing order status"
    finally:
        await db.close()

    return RedirectResponse(url=f"/?message={urllib.parse.quote_plus(msg)}&tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/settings/upi")
async def save_upi_setup(
    upi_id: str = Form(...),
    payee_name: str = Form(...),
    admin_chat_id: str = Form(""),
    is_auth: bool = Depends(require_admin),
):
    cleaned_upi = upi_id.strip()
    cleaned_name = payee_name.strip()

    if not cleaned_name:
        detected = await detect_payee_name_from_upi(cleaned_upi)
        cleaned_name = detected if detected else "Merchant"

    await update_setting("upi_id", cleaned_upi)
    await update_setting("payee_name", cleaned_name)
    await update_setting("admin_chat_id", admin_chat_id.strip())

    return RedirectResponse(url="/?message=UPI+and+payee+details+saved+successfully&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/password/update")
async def update_admin_password(
    current_password: str = Form(...),
    new_password: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    stored_password = await get_setting("admin_password") or DEFAULT_PASS
    if current_password != stored_password:
        return RedirectResponse(url="/?message=Error:+Current+password+does+not+match&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

    await update_setting("admin_password", new_password.strip())
    return RedirectResponse(url="/?message=Password+updated+successfully!&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/save")
async def save_general_settings(
    bot_token: str = Form(...),
    welcome_text: str = Form(...),
    plans_text: str = Form(...),
    welcome_photo: str = Form(...),
    demo_video: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    old_token = await get_setting("bot_token")
    cleaned_token = bot_token.strip()

    await update_setting("bot_token", cleaned_token)
    await update_setting("welcome_text", welcome_text.strip())
    await update_setting("plans_text", plans_text.strip())
    await update_setting("welcome_photo", welcome_photo.strip())
    await update_setting("demo_video", demo_video.strip())

    if cleaned_token and (cleaned_token != old_token or not manager.bot):
        await manager.restart(cleaned_token)

    return RedirectResponse(url="/?message=Configurations+applied+and+saved&tab=tab-bot", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/revenue/reset")
async def reset_revenue_stats(is_auth: bool = Depends(require_admin)):
    db = await get_db_connection()
    try:
        await db.execute("DELETE FROM payments")
        await db.commit()
    finally:
        await db.close()
    return RedirectResponse(url="/?message=Revenue+counters+reset+to+zero&tab=tab-dashboard", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/plans/add")
async def handle_add_plan(
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    access_link: str = Form(""),
    is_auth: bool = Depends(require_admin),
):
    await add_new_plan(plan_id.strip(), name.strip(), amount, validity.strip(), access_link.strip())
    return RedirectResponse(url="/?message=New+plan+added+successfully&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/plans/update")
async def update_plan_details(
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    access_link: str = Form(""),
    is_auth: bool = Depends(require_admin),
):
    await update_plan(plan_id, name.strip(), amount, validity.strip(), access_link.strip())
    return RedirectResponse(url="/?message=Plan+details+updated&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/plans/delete")
async def delete_plan_details(
    plan_id: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    await delete_plan(plan_id.strip())
    return RedirectResponse(url="/?message=Plan+deleted+successfully&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/health")
async def health_check():
    return {"status": "ok", "bot_online": manager.bot is not None}
