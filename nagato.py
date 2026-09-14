# -*- coding: utf-8 -*-
import asyncio
import io
import json
import logging
import os
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
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from jinja2 import Template

# ==========================================
# CONFIG & AUTHENTICATION
# ==========================================
ADMIN_USER = os.getenv("ADMIN_USER", "nagato")
DEFAULT_PASS = os.getenv("ADMIN_PASS", "nagato@123")
AUTH_COOKIE_NAME = "session_token"
AUTH_SECRET = "admin_authenticated_session_key_99"
DB_NAME = "nagato_database.db"

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
# DYNAMIC BOT CONTROLLER (ISOLATED & RESILIENT)
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
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
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
                        allowed_updates=["message", "callback_query"]
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
# DATABASE LAYER
# ==========================================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
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
                validity TEXT
            )
        """)

        default_expiry = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        defaults = {
            "bot_token": INITIAL_BOT_TOKEN,
            "admin_password": DEFAULT_PASS,
            "admin_chat_id": "6528792525",
            "maintenance": "off",
            "upi_id": "paytm.s21dj6b@pty",
            "payee_name": "NAZIYA NASRIN",
            "panel_expiry": default_expiry,
            "welcome_photo": "https://kommodo.ai/i/vMd2KH7PZC8bgMH9mGWm",
            "welcome_text": (
                "👋 Welcome to Our Bot!\n\n"
                "✨ Explore features, view demos, check subscriptions, "
                "or manage your account using the buttons below."
            ),
            "plans_text": (
                "📦 Choose Your Membership Plan\n\n"
                "👉 Select any plan below to get an instant UPI QR payment card:"
            ),
            "demo_video": "https://www.image2url.com/r2/default/videos/1788689350793-635df470-449c-4fd5-8ed3-96f503e8b88b.mp4",
        }
        for k, v in defaults.items():
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

        default_plans = [
            ("plan_1", "INDIAN WEBSERIES", 99.0, "30 Days"),
            ("plan_2", "3 MONTHS SPECIAL", 249.0, "90 Days"),
            ("plan_3", "6 MONTHS VIP", 449.0, "180 Days"),
            ("plan_4", "1 YEAR ACCESS", 799.0, "365 Days"),
            ("plan_5", "LIFETIME PASS", 1299.0, "Lifetime"),
            ("plan_6", "4K ULTRA STREAM", 199.0, "30 Days"),
            ("plan_7", "PRO PASS", 349.0, "60 Days"),
            ("plan_8", "EXCLUSIVE HUB", 599.0, "90 Days"),
        ]
        for p in default_plans:
            await db.execute(
                "INSERT OR IGNORE INTO plans (plan_id, name, amount, validity) VALUES (?, ?, ?, ?)",
                p,
            )
        await db.commit()


async def get_setting(key: str) -> str:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""


async def update_setting(key: str, value: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()


async def get_all_plans():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT plan_id, name, amount, validity FROM plans ORDER BY plan_id ASC") as cur:
            return await cur.fetchall()


async def get_plan(plan_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT plan_id, name, amount, validity FROM plans WHERE plan_id = ?", (plan_id,)) as cur:
            return await cur.fetchone()


async def update_plan(plan_id: str, name: str, amount: float, validity: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE plans SET name = ?, amount = ?, validity = ? WHERE plan_id = ?",
            (name, amount, validity, plan_id),
        )
        await db.commit()


async def add_new_plan(plan_id: str, name: str, amount: float, validity: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO plans (plan_id, name, amount, validity) VALUES (?, ?, ?, ?)",
            (plan_id, name, amount, validity),
        )
        await db.commit()


async def delete_plan(plan_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
        await db.commit()


async def add_or_update_user(user: types.User):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, full_name, username)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET full_name = excluded.full_name, username = excluded.username
            """,
            (user.id, user.full_name, user.username or "N/A"),
        )
        await db.commit()


async def get_user(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users WHERE user_id = ?",
            (user_id,),
        ) as cur:
            return await cur.fetchone()


async def get_all_users_detailed():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users ORDER BY joined_at DESC"
        ) as cur:
            return await cur.fetchall()


async def update_user_subscription(user_id: int, plan_name: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
        await db.commit()


async def get_dashboard_metrics():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM payments WHERE status='approved'") as cur:
            row = await cur.fetchone()
            paid_orders = row[0]
            revenue = row[1]

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


async def get_user_payment_stats(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
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


async def set_user_ban_status(user_id: int, is_banned: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (is_banned, user_id))
        await db.commit()


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


# ==========================================
# AUTOMATIC UPI PAYEE NAME DETECTOR
# ==========================================
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
# FSM STATES & KEYBOARDS
# ==========================================
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_lookup = State()
    waiting_for_welcome_photo = State()
    waiting_for_welcome_text = State()
    waiting_for_plans_text = State()
    waiting_for_demo_video = State()
    waiting_for_upi_id = State()
    waiting_for_payee_name = State()
    waiting_for_plan_name = State()
    waiting_for_plan_price = State()
    waiting_for_plan_validity = State()


class PaymentStates(StatesGroup):
    waiting_for_screenshot = State()


def get_home_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🎬 View Demo", callback_data="btn_view_demo")
    builder.button(text="⭐ My Premium", callback_data="btn_my_premium")
    builder.button(text="👤 My Profile", callback_data="btn_my_profile")
    builder.adjust(1)
    return builder.as_markup()


def get_demo_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="💎 Get Premium", callback_data="btn_get_premium")
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(2)
    return builder.as_markup()


async def get_plans_keyboard():
    builder = InlineKeyboardBuilder()
    plans = await get_all_plans()
    for pid, name, price, _ in plans:
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


async def get_admin_menu():
    maint = await get_setting("maintenance")
    builder = InlineKeyboardBuilder()
    builder.button(text="Stats", callback_data="admin_stats")
    builder.button(text="User Lookup", callback_data="admin_lookup")
    builder.button(text="Broadcast", callback_data="admin_broadcast")
    builder.button(text=f"Maint: {maint.upper()}", callback_data="admin_toggle_maint")
    builder.button(text="Edit Photo", callback_data="adm_edit_photo")
    builder.button(text="Edit Welcome Msg", callback_data="adm_edit_wtext")
    builder.button(text="Edit Plans Msg", callback_data="adm_edit_ptext")
    builder.button(text="Edit Demo Video", callback_data="adm_edit_video")
    builder.button(text="Edit Payment UPI", callback_data="adm_edit_upi")
    builder.button(text="Edit Plan Buttons", callback_data="adm_edit_plans_list")
    builder.button(text="Close", callback_data="admin_close")
    builder.adjust(2, 2, 2, 2, 2, 1)
    return builder.as_markup()


def get_admin_user_card_keyboard(target_id: int, is_banned: int):
    builder = InlineKeyboardBuilder()
    builder.button(
        text="Unban User" if is_banned else "Ban User",
        callback_data=f"adm_ban:{target_id}:{0 if is_banned else 1}",
    )
    builder.button(text="Back", callback_data="admin_home")
    builder.adjust(1)
    return builder.as_markup()


# ==========================================
# NON-BLOCKING TELEGRAM FLOW HANDLERS
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


@manager.dp.callback_query(F.data == "btn_view_demo")
async def nav_demo(callback: types.CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not manager.bot:
        return
    video_url = await get_setting("demo_video")
    try:
        async with asyncio.timeout(3.0):
            msg = await manager.bot.send_video(
                chat_id=callback.message.chat.id,
                video=video_url,
                caption="📺 Demo Video",
                reply_markup=get_demo_keyboard(),
            )
            track(callback.message.chat.id, msg.message_id)
    except Exception:
        msg = await manager.bot.send_message(
            chat_id=callback.message.chat.id,
            text="📺 Demo video temporarily unavailable.",
            reply_markup=get_demo_keyboard(),
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
    text = f"⭐ My Premium Membership\n\nPlan: {plan}\nStatus: {'Active' if plan != 'Free' else 'Free Tier'}"
    msg = await manager.bot.send_message(
        chat_id=callback.message.chat.id,
        text=text,
        reply_markup=get_demo_keyboard(),
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
    _, plan_name, amount, validity = plan
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

    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "INSERT INTO payments (user_id, plan_name, amount) VALUES (?, ?, ?)",
            (message.from_user.id, plan_name, amount),
        )
        pid = cur.lastrowid
        await db.commit()

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


# ==========================================
# BOT ADMIN COMMANDS & HANDLERS
# ==========================================
@manager.dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    admin_ids = await get_admin_ids()
    if message.from_user.id in admin_ids:
        await message.answer("Admin Control Panel", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "admin_home")
async def nav_admin_home(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await callback.message.edit_text("Admin Control Panel", reply_markup=await get_admin_menu())
        await callback.answer()


@manager.dp.callback_query(F.data == "admin_close")
async def close_admin_panel(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer()


@manager.dp.callback_query(F.data == "admin_toggle_maint")
async def handle_maintenance_toggle(callback: types.CallbackQuery):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        curr = await get_setting("maintenance")
        await update_setting("maintenance", "off" if curr == "on" else "on")
        await callback.message.edit_reply_markup(reply_markup=await get_admin_menu())
        await callback.answer("Maintenance toggled.")


@manager.dp.callback_query(F.data == "adm_edit_photo")
async def start_edit_welcome_photo(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_welcome_photo)
        await callback.message.edit_text("Send the new photo or URL:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_welcome_photo)
async def process_new_welcome_photo(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    target = message.photo[-1].file_id if message.photo else message.text
    await update_setting("welcome_photo", target)
    await state.clear()
    await message.answer("Welcome photo updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "adm_edit_wtext")
async def start_edit_welcome_text(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_welcome_text)
        await callback.message.edit_text("Send the new welcome text:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_welcome_text)
async def process_new_welcome_text(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    await update_setting("welcome_text", message.text)
    await state.clear()
    await message.answer("Welcome text updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "adm_edit_ptext")
async def start_edit_plans_text(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_plans_text)
        await callback.message.edit_text("Send the new plans text:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_plans_text)
async def process_new_plans_text(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    await update_setting("plans_text", message.text)
    await state.clear()
    await message.answer("Plans message text updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "adm_edit_video")
async def start_edit_demo_video(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_demo_video)
        await callback.message.edit_text("Send the new video or URL:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_demo_video)
async def process_new_demo_video(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    target = message.video.file_id if message.video else message.text
    await update_setting("demo_video", target)
    await state.clear()
    await message.answer("Demo video updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "adm_edit_upi")
async def start_edit_upi(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_upi_id)
        await callback.message.edit_text("Step 1: Enter new UPI ID:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_upi_id)
async def process_new_upi_id(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    await state.update_data(new_upi_id=message.text.strip())
    await state.set_state(AdminStates.waiting_for_payee_name)
    await message.answer("Step 2: Enter new Payee Name:")


@manager.dp.message(AdminStates.waiting_for_payee_name)
async def process_new_payee_name(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    d = await state.get_data()
    await update_setting("upi_id", d["new_upi_id"])
    await update_setting("payee_name", message.text.strip())
    await state.clear()
    await message.answer("UPI details updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "adm_edit_plans_list")
async def show_plans_for_editing(callback: types.CallbackQuery):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        plans = await get_all_plans()
        builder = InlineKeyboardBuilder()
        for pid, name, price, _ in plans:
            builder.button(text=f"{name} (Rs.{int(price)})", callback_data=f"adm_psel:{pid}")
        builder.button(text="Back", callback_data="admin_home")
        builder.adjust(1)
        await callback.message.edit_text("Select a button/plan to edit:", reply_markup=builder.as_markup())
        await callback.answer()


@manager.dp.callback_query(F.data.startswith("adm_psel:"))
async def select_plan_to_edit(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        pid = callback.data.split(":")[1]
        await state.update_data(target_pid=pid)
        await state.set_state(AdminStates.waiting_for_plan_name)
        await callback.message.edit_text(f"Step 1: Send new Plan Name for `{pid}`:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_plan_name)
async def process_edit_plan_name(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    await state.update_data(new_pname=message.text.strip())
    await state.set_state(AdminStates.waiting_for_plan_price)
    await message.answer("Step 2: Enter new Price (e.g. 199):")


@manager.dp.message(AdminStates.waiting_for_plan_price)
async def process_edit_plan_price(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    try:
        price = float(message.text.strip())
        await state.update_data(new_pprice=price)
        await state.set_state(AdminStates.waiting_for_plan_validity)
        await message.answer("Step 3: Enter new Validity (e.g. 30 Days):")
    except ValueError:
        await message.answer("Please enter a numeric price.")


@manager.dp.message(AdminStates.waiting_for_plan_validity)
async def process_edit_plan_validity(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    d = await state.get_data()
    await update_plan(d["target_pid"], d["new_pname"], d["new_pprice"], message.text.strip())
    await state.clear()
    await message.answer("Plan updated!", reply_markup=await get_admin_menu())


@manager.dp.callback_query(F.data == "admin_stats")
async def handle_admin_stats(callback: types.CallbackQuery):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        metrics = await get_dashboard_metrics()
        builder = InlineKeyboardBuilder()
        builder.button(text="Back", callback_data="admin_home")
        await callback.message.edit_text(
            f"Analytics Overview:\n"
            f"Paid Orders: {metrics['paid_orders']}\n"
            f"Total Revenue: Rs.{metrics['revenue']}\n"
            f"Registered Users: {metrics['total_users']}",
            reply_markup=builder.as_markup(),
        )
        await callback.answer()


@manager.dp.callback_query(F.data == "admin_lookup")
async def start_user_lookup(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_lookup)
        await callback.message.edit_text("Send numeric Telegram ID:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_lookup)
async def process_user_lookup(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    if not message.text.isdigit():
        await message.answer("Send digits only.")
        return
    u = await get_user(int(message.text))
    await state.clear()
    if not u:
        await message.answer("User not found.", reply_markup=await get_admin_menu())
        return
    uid, name, uname, joined, plan, ban = u
    await message.answer(
        f"ID: {uid}\nName: {name}\nUsername: @{uname}\nPlan: {plan}\nStatus: {'Banned' if ban else 'Active'}",
        reply_markup=get_admin_user_card_keyboard(uid, ban),
    )


@manager.dp.callback_query(F.data.startswith("adm_ban:"))
async def handle_admin_ban(callback: types.CallbackQuery):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        _, uid, st = callback.data.split(":")
        await set_user_ban_status(int(uid), int(st))
        u = await get_user(int(uid))
        await callback.message.edit_text(
            f"User {uid} updated. Status: {'Banned' if u[5] else 'Active'}",
            reply_markup=get_admin_user_card_keyboard(int(uid), u[5]),
        )
        await callback.answer("Updated.")


@manager.dp.callback_query(F.data == "admin_broadcast")
async def start_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        await state.set_state(AdminStates.waiting_for_broadcast)
        await callback.message.edit_text("Send message to broadcast:\n/cancel to abort.")
        await callback.answer()


@manager.dp.message(AdminStates.waiting_for_broadcast)
async def process_admin_broadcast(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Canceled.", reply_markup=await get_admin_menu())
        return
    await state.clear()
    status_msg = await message.answer("Broadcasting...")
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as cur:
            users = await cur.fetchall()
    sent = 0
    for (uid,) in users:
        try:
            await message.copy_to(chat_id=uid)
            sent += 1
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await message.copy_to(chat_id=uid)
                sent += 1
            except Exception:
                pass
        except Exception:
            pass
    await status_msg.edit_text(
        f"Broadcast finished. Sent: {sent}", reply_markup=await get_admin_menu()
    )


@manager.dp.callback_query(F.data.startswith("adm_pay:"))
async def handle_admin_pay_approval(callback: types.CallbackQuery):
    if not manager.bot:
        return
    admin_ids = await get_admin_ids()
    if callback.from_user.id in admin_ids:
        _, pid, act = callback.data.split(":")
        async with aiosqlite.connect(DB_NAME) as db:
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
                try:
                    await manager.bot.send_message(
                        t_uid, f"Payment Approved! Your plan {pl} is active!"
                    )
                except Exception:
                    pass
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

# ==========================================
# TEMPLATES (EXACT NOISY/NAGATO LOGIN & CYBERPUNK PANEL)
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
        #cyberCanvas {
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            z-index: 1;
            pointer-events: none;
        }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex items-center justify-center p-4 relative overflow-hidden">
    <canvas id="cyberCanvas"></canvas>

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

    <script>
        const canvas = document.getElementById('cyberCanvas');
        const ctx = canvas.getContext('2d');
        let width = canvas.width = window.innerWidth;
        let height = canvas.height = window.innerHeight;

        window.addEventListener('resize', () => {
            width = canvas.width = window.innerWidth;
            height = canvas.height = window.innerHeight;
        });

        const particles = [];
        const count = 38;
        const colors = ['#a855f7', '#00f0ff', '#ff007f'];

        for (let i = 0; i < count; i++) {
            particles.push({
                x: Math.random() * width,
                y: Math.random() * height,
                vx: (Math.random() - 0.5) * 0.5,
                vy: (Math.random() - 0.5) * 0.5,
                radius: Math.random() * 1.8 + 0.8,
                color: colors[Math.floor(Math.random() * colors.length)]
            });
        }

        function render() {
            ctx.clearRect(0, 0, width, height);

            for (let i = 0; i < count; i++) {
                let p = particles[i];
                p.x += p.vx;
                p.y += p.vy;

                if (p.x < 0 || p.x > width) p.vx *= -1;
                if (p.y < 0 || p.y > height) p.vy *= -1;

                ctx.beginPath();
                ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
                ctx.fillStyle = p.color;
                ctx.shadowBlur = 6;
                ctx.shadowColor = p.color;
                ctx.fill();
                ctx.shadowBlur = 0;

                for (let j = i + 1; j < count; j++) {
                    let p2 = particles[j];
                    let dist = Math.hypot(p.x - p2.x, p.y - p2.y);
                    if (dist < 110) {
                        ctx.beginPath();
                        ctx.moveTo(p.x, p.y);
                        ctx.lineTo(p2.x, p2.y);
                        ctx.strokeStyle = p.color;
                        ctx.globalAlpha = 1 - (dist / 110);
                        ctx.lineWidth = 0.4;
                        ctx.stroke();
                        ctx.globalAlpha = 1;
                    }
                }
            }
            requestAnimationFrame(render);
        }
        render();
    </script>
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
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #06040c; }
        .font-tech { font-family: 'Orbitron', monospace; }
        .glass-card {
            background: linear-gradient(135deg, rgba(22, 12, 42, 0.75) 0%, rgba(13, 8, 25, 0.85) 100%);
            backdrop-filter: blur(16px);
            border: 1px solid rgba(139, 92, 246, 0.25);
        }
        .neon-border-pink {
            box-shadow: 0 0 15px rgba(255, 0, 127, 0.2), inset 0 0 15px rgba(255, 0, 127, 0.05);
            border-color: rgba(255, 0, 127, 0.4);
        }
        .neon-border-cyan {
            box-shadow: 0 0 15px rgba(0, 240, 255, 0.2), inset 0 0 15px rgba(0, 240, 255, 0.05);
            border-color: rgba(0, 240, 255, 0.4);
        }
        #cyberCanvas {
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            z-index: 0;
            pointer-events: none;
        }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex overflow-x-hidden relative">
    <canvas id="cyberCanvas"></canvas>

    <div id="sidebarBackdrop" onclick="toggleSidebar()" class="fixed inset-0 bg-black/70 z-30 backdrop-blur-sm hidden md:hidden"></div>

    <aside id="sidebar" class="fixed inset-y-0 left-0 z-40 w-64 bg-[#090614] border-r border-purple-900/40 p-5 flex flex-col justify-between -translate-x-full md:translate-x-0 transition-transform duration-200 ease-in-out md:static md:h-screen">
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                    <span class="text-lg">🚀</span>
                    <div>
                        <span class="font-tech font-bold text-sm text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300 tracking-wider block">Nagato Panel</span>
                        <span class="font-tech text-[10px] tracking-wider text-cyan-300 uppercase block">Pom Pom Bot</span>
                    </div>
                </div>
                <button onclick="toggleSidebar()" class="md:hidden text-purple-400 hover:text-white p-1">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>

            <nav class="space-y-1 text-xs">
                <button onclick="switchTab('tab-dashboard')" id="nav-tab-dashboard" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-semibold bg-purple-900/40 border border-fuchsia-500/30 text-cyan-400 shadow-[0_0_12px_rgba(0,240,255,0.15)] transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
                    Dashboard
                </button>
                <button onclick="switchTab('tab-orders')" id="nav-tab-orders" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/></svg>
                    Orders
                </button>
                <button onclick="switchTab('tab-users')" id="nav-tab-users" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/></svg>
                    Manage Users
                </button>
                <button onclick="switchTab('tab-broadcast')" id="nav-tab-broadcast" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z"/></svg>
                    Broadcast
                </button>
                <button onclick="switchTab('tab-settings')" id="nav-tab-settings" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
                    Setting &amp; UPI
                </button>
                <button onclick="switchTab('tab-bot')" id="nav-tab-bot" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"/></svg>
                    Bot Token Config
                </button>
                <button onclick="switchTab('tab-media')" id="nav-tab-media" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
                    Media &amp; Greetings
                </button>
                <button onclick="switchTab('tab-plans')" id="nav-tab-plans" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
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

    <main class="flex-1 flex flex-col min-w-0 h-screen overflow-y-auto relative z-10">
        <header class="sticky top-0 z-20 bg-[#06040c]/90 backdrop-blur-md border-b border-purple-900/40 px-4 md:px-8 py-3.5 flex items-center justify-between">
            <div class="flex items-center gap-3">
                <button onclick="toggleSidebar()" class="p-2 rounded-lg bg-[#120b22] border border-purple-900/40 text-purple-300 hover:text-white">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>
                </button>
                <h2 id="sectionTitle" class="font-tech text-base md:text-lg font-bold text-white tracking-wider">Dashboard</h2>
            </div>
            
            <div class="flex items-center gap-3">
                <span class="text-xs text-purple-300 hidden sm:inline font-mono flex items-center gap-1">
                    <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>
                    Nagato
                </span>
                <span class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#120b22] border border-cyan-500/40 text-xs font-mono text-cyan-300 shadow-[0_0_10px_rgba(0,240,255,0.2)]">
                    <span class="w-2 h-2 rounded-full {{ 'bg-cyan-400' if is_online else 'bg-amber-400' }} animate-pulse inline-block"></span>
                    {{ 'ONLINE' if is_online else 'STANDBY' }}
                </span>
            </div>
        </header>

        <div class="p-4 md:p-8 max-w-5xl w-full mx-auto space-y-6">
            {% if message %}
            <div class="p-4 rounded-xl bg-cyan-500/10 border border-cyan-500/40 text-cyan-300 text-xs font-mono flex items-center gap-2 shadow-[0_0_15px_rgba(0,240,255,0.15)]">
                <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
                <span>{{ message }}</span>
            </div>
            {% endif %}

            <div id="tab-dashboard" class="tab-content space-y-6">
                <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between hover:border-cyan-500/40 transition">
                        <div class="flex items-center justify-between mb-2">
                            <span class="p-2 rounded-xl bg-cyan-500/10 text-cyan-400 border border-cyan-500/30">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                            </span>
                        </div>
                        <div>
                            <span class="font-tech text-2xl md:text-3xl font-extrabold text-white tracking-tight">{{ paid_orders }}</span>
                            <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Paid orders</p>
                        </div>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between hover:border-fuchsia-500/40 transition">
                        <div class="flex items-center justify-between mb-2">
                            <span class="p-2 rounded-xl bg-fuchsia-500/10 text-fuchsia-400 border border-fuchsia-500/30">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 9V7a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2m2 4h10a2 2 0 002-2v-6a2 2 0 00-2-2H9a2 2 0 00-2 2v6a2 2 0 002 2zm7-5a2 2 0 11-4 0 2 2 0 014 0z"/></svg>
                            </span>
                            <form method="POST" action="/admin/revenue/reset" onsubmit="return confirm('Reset all revenue counters?');">
                                <button type="submit" class="text-[10px] text-purple-400 hover:text-fuchsia-300 flex items-center gap-1 font-mono">
                                    Reset
                                </button>
                            </form>
                        </div>
                        <div>
                            <span class="font-tech text-2xl md:text-3xl font-extrabold text-white tracking-tight">&#8377;{{ revenue }}</span>
                            <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Revenue</p>
                        </div>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between hover:border-purple-500/40 transition">
                        <div class="flex items-center justify-between mb-2">
                            <span class="p-2 rounded-xl bg-purple-500/10 text-purple-300 border border-purple-500/30">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/></svg>
                            </span>
                        </div>
                        <div>
                            <span class="font-tech text-2xl md:text-3xl font-extrabold text-white tracking-tight">{{ total_users }}</span>
                            <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Total users</p>
                        </div>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between neon-border-pink relative overflow-hidden">
                        <div class="absolute -right-6 -bottom-6 w-24 h-24 bg-fuchsia-500/20 rounded-full blur-xl pointer-events-none"></div>
                        <div class="flex items-center justify-between mb-2">
                            <span class="p-2 rounded-xl bg-fuchsia-500/10 text-fuchsia-400 border border-fuchsia-500/30">
                                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                            </span>
                            <span id="expiryBadge" class="text-[10px] font-tech px-2 py-0.5 rounded-md bg-fuchsia-500/20 text-fuchsia-300 border border-fuchsia-500/40">
                                ACTIVE
                            </span>
                        </div>
                        <div>
                            <span id="countdownTimer" class="font-tech text-lg md:text-xl font-black text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300">
                                Calculating...
                            </span>
                            <p class="text-[11px] text-purple-400 mt-1 font-mono">
                                Valid till: <span class="text-slate-300">{{ panel_expiry }}</span>
                            </p>
                        </div>
                    </div>
                </div>

                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-purple-900/40 pb-3">
                        <div>
                            <span class="text-xs text-cyan-400 font-mono">GATEWAY: DIRECT UPI QR</span>
                            <h3 class="font-tech text-base font-bold text-white tracking-wide">Recent orders</h3>
                        </div>
                        <button onclick="switchTab('tab-orders')" class="text-xs text-fuchsia-400 hover:text-fuchsia-300 font-mono">View All Orders &rarr;</button>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-3 px-3">Code</th>
                                    <th class="py-3 px-3">UID</th>
                                    <th class="py-3 px-3">Item</th>
                                    <th class="py-3 px-3">Amount</th>
                                    <th class="py-3 px-3 text-right">Status</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for oid, uid, uname, item, amt, st, dt in recent_orders %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-3 px-3 text-cyan-400">FT{{ oid }}ORD</td>
                                    <td class="py-3 px-3">
                                        {{ uid }}
                                        <span class="block text-[10px] text-purple-400">@{{ uname }}</span>
                                    </td>
                                    <td class="py-3 px-3 text-white font-sans">{{ item }}</td>
                                    <td class="py-3 px-3 font-semibold text-white">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-3 px-3 text-right">
                                        {% if st == 'approved' %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-emerald-500/10 border border-emerald-500/40 text-emerald-400">PAID</span>
                                        {% elif st == 'pending' %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-amber-500/10 border border-amber-500/40 text-amber-400">PENDING</span>
                                        {% else %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-rose-500/10 border border-rose-500/40 text-rose-400">REJECTED</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% else %}
                                <tr>
                                    <td colspan="5" class="py-8 text-center text-purple-400 text-xs">No orders recorded yet.</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: CUSTOMER ORDERS -->
            <div id="tab-orders" class="tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="border-b border-purple-900/40 pb-3">
                        <span class="text-xs text-cyan-400 font-mono">NAGATO TRANSACTION ARCHIVE</span>
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Customer Orders</h3>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-3 px-3">Order Code</th>
                                    <th class="py-3 px-3">Telegram User</th>
                                    <th class="py-3 px-3">Subscription Item</th>
                                    <th class="py-3 px-3">Amount</th>
                                    <th class="py-3 px-3">Timestamp</th>
                                    <th class="py-3 px-3 text-right">Status</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for oid, uid, uname, item, amt, st, dt in all_orders %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-3 px-3 text-cyan-400">FT{{ oid }}ORD</td>
                                    <td class="py-3 px-3">
                                        {{ uid }}
                                        <span class="block text-[10px] text-purple-400">@{{ uname }}</span>
                                    </td>
                                    <td class="py-3 px-3 text-white font-sans">{{ item }}</td>
                                    <td class="py-3 px-3 font-semibold text-white">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-3 px-3 text-purple-300/80 text-[11px]">{{ dt }}</td>
                                    <td class="py-3 px-3 text-right">
                                        {% if st == 'approved' %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-emerald-500/10 border border-emerald-500/40 text-emerald-400">PAID</span>
                                        {% elif st == 'pending' %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-amber-500/10 border border-amber-500/40 text-amber-400">PENDING</span>
                                        {% else %}
                                        <span class="px-2.5 py-1 rounded-md text-[10px] font-tech bg-rose-500/10 border border-rose-500/40 text-rose-400">REJECTED</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% else %}
                                <tr>
                                    <td colspan="6" class="py-12 text-center text-purple-400 text-xs">No orders recorded in database.</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: MANAGE USERS -->
            <div id="tab-users" class="tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="border-b border-purple-900/40 pb-3">
                        <span class="text-xs text-cyan-400 font-mono">DATABASE DIRECTORY</span>
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Registered Users</h3>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-3 px-3">User &amp; Chat ID</th>
                                    <th class="py-3 px-3">Username</th>
                                    <th class="py-3 px-3">Joined Date</th>
                                    <th class="py-3 px-3">Subscription</th>
                                    <th class="py-3 px-3 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for u_id, u_fname, u_uname, u_joined, u_status, u_banned in users_list %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-3 px-3 text-cyan-400">
                                        {{ u_id }}
                                        <span class="block text-[10px] text-white font-sans">{{ u_fname }}</span>
                                    </td>
                                    <td class="py-3 px-3">
                                        {% if u_uname != 'N/A' %}
                                        <a href="https://t.me/{{ u_uname }}" target="_blank" class="text-fuchsia-400 hover:underline">@{{ u_uname }}</a>
                                        {% else %}
                                        <span class="text-purple-400/60">None</span>
                                        {% endif %}
                                    </td>
                                    <td class="py-3 px-3 text-purple-300/80 text-[11px]">{{ u_joined }}</td>
                                    <td class="py-3 px-3">
                                        <form method="POST" action="/admin/users/subscription" class="flex items-center gap-1.5">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <select name="plan_name" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2 py-1 text-[11px] text-white focus:outline-none focus:border-cyan-400">
                                                <option value="Free" {{ 'selected' if u_status == 'Free' else '' }}>Free Tier</option>
                                                {% for p_id, p_name, _, _ in plans %}
                                                <option value="{{ p_name }}" {{ 'selected' if u_status == p_name else '' }}>{{ p_name }}</option>
                                                {% endfor %}
                                            </select>
                                            <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white px-2 py-1 rounded-lg text-[10px] font-tech transition">SET</button>
                                        </form>
                                    </td>
                                    <td class="py-3 px-3 text-right">
                                        <form method="POST" action="/admin/users/ban">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <input type="hidden" name="status" value="{{ 0 if u_banned else 1 }}">
                                            {% if u_banned %}
                                            <button type="submit" class="bg-emerald-500/10 border border-emerald-500/40 hover:bg-emerald-500 text-emerald-400 hover:text-white px-2.5 py-1 rounded-lg text-[10px] font-tech transition">UNBAN</button>
                                            {% else %}
                                            <button type="submit" class="bg-rose-500/10 border border-rose-500/40 hover:bg-rose-500 text-rose-400 hover:text-white px-2.5 py-1 rounded-lg text-[10px] font-tech transition">BAN</button>
                                            {% endif %}
                                        </form>
                                    </td>
                                </tr>
                                {% else %}
                                <tr>
                                    <td colspan="5" class="py-12 text-center text-purple-400 text-xs">No registered users in database yet.</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB: BROADCAST -->
            <div id="tab-broadcast" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/broadcast/send" class="glass-card rounded-2xl p-6 space-y-5">
                    <div class="flex items-center gap-3 border-b border-purple-900/40 pb-4">
                        <div class="w-9 h-9 rounded-xl bg-cyan-500/15 text-cyan-400 border border-cyan-500/30 flex items-center justify-center">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z"/></svg>
                        </div>
                        <div>
                            <h3 class="font-tech text-base font-bold text-white tracking-wide">Mass Broadcast</h3>
                            <p class="text-xs text-purple-400/80">Dispatch immediate system announcement to all bot users.</p>
                        </div>
                    </div>

                    <div class="space-y-4">
                        <div>
                            <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Message Payload</label>
                            <textarea name="broadcast_message" rows="5" required placeholder="Type your broadcast transmission..."
                                      class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-cyan-400 transition"></textarea>
                        </div>

                        <div>
                            <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Banner Image URL (Optional)</label>
                            <input type="url" name="broadcast_photo" placeholder="https://..."
                                   class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-cyan-400 transition">
                        </div>
                    </div>

                    <button type="submit" onclick="return confirm('Transmit broadcast message?');"
                            class="bg-gradient-to-r from-fuchsia-600 to-purple-600 hover:from-fuchsia-500 hover:to-purple-500 text-white font-tech font-bold px-6 py-2.5 rounded-xl text-xs uppercase tracking-wider shadow-lg shadow-fuchsia-600/30 transition">
                        📢 Transmit Broadcast
                    </button>
                </form>
            </div>

            <!-- TAB: SETTINGS & UPI -->
            <div id="tab-settings" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/settings/upi" class="glass-card rounded-2xl p-6 space-y-5">
                    <div class="flex items-center gap-3 border-b border-purple-900/40 pb-4">
                        <div class="w-9 h-9 rounded-xl bg-cyan-500/15 text-cyan-400 border border-cyan-500/30 flex items-center justify-center">
                            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z"/></svg>
                        </div>
                        <div>
                            <h3 class="font-tech text-base font-bold text-white tracking-wide">UPI Payment Configuration</h3>
                            <p class="text-xs text-purple-400/80">Configure payout UPI ID and merchant descriptor.</p>
                        </div>
                    </div>

                    <div class="space-y-4">
                        <div>
                            <div class="flex items-center justify-between mb-2">
                                <label class="block text-xs font-semibold text-purple-300 font-mono uppercase">UPI ID *</label>
                                <button type="button" onclick="autoDetectName()" class="text-xs text-cyan-400 hover:text-cyan-300 font-mono flex items-center gap-1">
                                    <svg id="detectSpinner" class="w-3.5 h-3.5 hidden animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path></svg>
                                    Auto-detect
                                </button>
                            </div>
                            <input type="text" id="upi_id_input" name="upi_id" value="{{ upi_id }}" required placeholder="yourname@okhdfcbank"
                                   class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none focus:border-cyan-400 transition">
                        </div>

                        <div>
                            <label class="block text-xs font-semibold text-purple-300 mb-2 font-mono uppercase">Payee Registered Name</label>
                            <input type="text" id="payee_name_input" name="payee_name" value="{{ payee_name }}" required placeholder="Account Holder Name"
                                   class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-cyan-400 transition">
                            <p id="detectStatus" class="text-[11px] text-purple-400/80 mt-1.5 font-mono">Provide the registered account name matching your UPI.</p>
                        </div>

                        <div class="pt-2">
                            <label class="block text-xs font-semibold text-purple-300 mb-2 font-mono uppercase">Admin Telegram ID (Order Alert Dispatch)</label>
                            <input type="text" name="admin_chat_id" value="{{ admin_chat_id }}" placeholder="e.g. 6528792525"
                                   class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white font-mono focus:outline-none focus:border-cyan-400 transition">
                        </div>
                    </div>

                    <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 hover:from-fuchsia-500 hover:to-purple-500 text-white font-tech font-bold px-5 py-2.5 rounded-xl text-xs uppercase tracking-wider shadow-lg shadow-fuchsia-600/30 transition">
                        Save UPI Config
                    </button>
                </form>

                <form method="POST" action="/admin/password/update" class="glass-card rounded-2xl p-6 space-y-4">
                    <div class="flex items-center gap-3 border-b border-purple-900/40 pb-3">
                        <div class="w-8 h-8 rounded-xl bg-purple-500/15 text-purple-300 border border-purple-500/30 flex items-center justify-center">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/></svg>
                        </div>
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Update Security Key</h3>
                    </div>

                    <div>
                        <label class="block text-xs font-semibold text-purple-300 mb-2 font-mono uppercase">Current Password</label>
                        <input type="password" name="current_password" required placeholder="********"
                               class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-fuchsia-500 transition">
                    </div>

                    <div>
                        <label class="block text-xs font-semibold text-purple-300 mb-2 font-mono uppercase">New Password</label>
                        <input type="password" name="new_password" required placeholder="********"
                               class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-fuchsia-500 transition">
                    </div>

                    <div class="pt-2">
                        <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 hover:from-fuchsia-500 hover:to-purple-500 text-white font-tech font-bold px-5 py-2.5 rounded-xl text-xs uppercase tracking-wider shadow-lg shadow-fuchsia-600/30 transition">
                            Change Key
                        </button>
                    </div>
                </form>
            </div>

            <!-- TAB: BOT TOKEN -->
            <form method="POST" action="/admin/save">
                <div id="tab-bot" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-4">
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Bot Core Authentication</h3>
                        <div>
                            <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Telegram Bot Token</label>
                            <input type="text" name="bot_token" value="{{ bot_token }}" required
                                   class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none focus:border-cyan-400 transition">
                            <p class="text-xs text-purple-400/80 mt-2 font-mono">Re-initializes polling runtime immediately.</p>
                        </div>
                    </div>
                </div>

                <!-- TAB: MEDIA & TEXTS -->
                <div id="tab-media" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-5">
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Interface Templates</h3>
                        <div>
                            <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Welcome Greeting</label>
                            <textarea name="welcome_text" rows="4" required
                                      class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-cyan-400 transition">{{ welcome_text }}</textarea>
                        </div>
                        <div>
                            <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Plans Header Caption</label>
                            <textarea name="plans_text" rows="3" required
                                      class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white focus:outline-none focus:border-cyan-400 transition">{{ plans_text }}</textarea>
                        </div>
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div>
                                <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Welcome Image URL</label>
                                <input type="url" name="welcome_photo" value="{{ welcome_photo }}" required
                                       class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-cyan-400 transition">
                            </div>
                            <div>
                                <label class="block text-xs font-semibold text-purple-300 uppercase tracking-wider mb-2 font-mono">Demo Video URL</label>
                                <input type="url" name="demo_video" value="{{ demo_video }}" required
                                       class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white focus:outline-none focus:border-cyan-400 transition">
                            </div>
                        </div>
                    </div>
                </div>

                <div id="saveBar" class="pt-4 hidden">
                    <button type="submit" class="w-full bg-gradient-to-r from-fuchsia-600 via-purple-600 to-cyan-600 hover:from-fuchsia-500 hover:to-cyan-500 text-white font-tech font-bold py-3.5 rounded-xl shadow-lg shadow-fuchsia-500/25 tracking-wider uppercase transition">
                        Save Interface Parameters
                    </button>
                </div>
            </form>

            <!-- TAB: PLANS -->
            <div id="tab-plans" class="tab-content space-y-5 hidden">
                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white tracking-wide">Add Subscription Tier</h3>
                    <form method="POST" action="/admin/plans/add" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 items-end font-mono text-xs">
                        <div>
                            <label class="block text-[11px] font-semibold text-purple-300 mb-1.5 uppercase">Plan ID</label>
                            <input type="text" name="plan_id" required placeholder="plan_9" class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-3 py-2 text-white focus:outline-none focus:border-cyan-400">
                        </div>
                        <div>
                            <label class="block text-[11px] font-semibold text-purple-300 mb-1.5 uppercase">Tier Name</label>
                            <input type="text" name="name" required placeholder="VIP ACCESS" class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-3 py-2 text-white focus:outline-none focus:border-cyan-400">
                        </div>
                        <div>
                            <label class="block text-[11px] font-semibold text-purple-300 mb-1.5 uppercase">Price (Rs.)</label>
                            <input type="number" step="any" name="amount" required placeholder="499" class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-3 py-2 text-white focus:outline-none focus:border-cyan-400">
                        </div>
                        <div>
                            <label class="block text-[11px] font-semibold text-purple-300 mb-1.5 uppercase">Validity</label>
                            <input type="text" name="validity" required placeholder="60 Days" class="w-full bg-[#070410]/90 border border-purple-900/60 rounded-xl px-3 py-2 text-white focus:outline-none focus:border-cyan-400">
                        </div>
                        <div>
                            <button type="submit" class="w-full bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-tech font-bold py-2 rounded-xl uppercase tracking-wider transition">
                                + Add Plan
                            </button>
                        </div>
                    </form>
                </div>

                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white tracking-wide">Active Membership Plans</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono text-slate-300">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="p-3">Plan Key</th>
                                    <th class="p-3">Title</th>
                                    <th class="p-3">Price (Rs.)</th>
                                    <th class="p-3">Validity</th>
                                    <th class="p-3 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30">
                                {% for pid, name, amount, validity in plans %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <form method="POST" action="/admin/plans/update">
                                        <input type="hidden" name="plan_id" value="{{ pid }}">
                                        <td class="p-3 text-cyan-400">{{ pid }}</td>
                                        <td class="p-3"><input type="text" name="name" value="{{ name }}" class="bg-[#070410]/90 border border-purple-900/60 rounded-lg px-2 py-1 text-white"></td>
                                        <td class="p-3"><input type="number" step="any" name="amount" value="{{ amount }}" class="bg-[#070410]/90 border border-purple-900/60 rounded-lg px-2 py-1 text-white w-20"></td>
                                        <td class="p-3"><input type="text" name="validity" value="{{ validity }}" class="bg-[#070410]/90 border border-purple-900/60 rounded-lg px-2 py-1 text-white w-24"></td>
                                        <td class="p-3 text-right flex items-center justify-end gap-2">
                                            <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white px-3 py-1 rounded-lg font-tech text-[10px] transition">UPDATE</button>
                                    </form>
                                            <form method="POST" action="/admin/plans/delete" onsubmit="return confirm('Delete this plan?');">
                                                <input type="hidden" name="plan_id" value="{{ pid }}">
                                                <button type="submit" class="bg-rose-500/10 border border-rose-500/40 hover:bg-rose-500 text-rose-400 hover:text-white px-2.5 py-1 rounded-lg font-tech text-[10px] transition">DELETE</button>
                                            </form>
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

    <script>
        const canvas = document.getElementById('cyberCanvas');
        const ctx = canvas.getContext('2d');
        let width = canvas.width = window.innerWidth;
        let height = canvas.height = window.innerHeight;

        window.addEventListener('resize', () => {
            width = canvas.width = window.innerWidth;
            height = canvas.height = window.innerHeight;
        });

        const particles = [];
        const count = 45;
        const colors = ['#ff007f', '#00f0ff', '#a855f7'];

        for (let i = 0; i < count; i++) {
            particles.push({
                x: Math.random() * width,
                y: Math.random() * height,
                vx: (Math.random() - 0.5) * 0.7,
                vy: (Math.random() - 0.5) * 0.7,
                radius: Math.random() * 2 + 1,
                color: colors[Math.floor(Math.random() * colors.length)]
            });
        }

        function render() {
            ctx.clearRect(0, 0, width, height);

            for (let i = 0; i < count; i++) {
                let p = particles[i];
                p.x += p.vx;
                p.y += p.vy;

                if (p.x < 0 || p.x > width) p.vx *= -1;
                if (p.y < 0 || p.y > height) p.vy *= -1;

                ctx.beginPath();
                ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
                ctx.fillStyle = p.color;
                ctx.shadowBlur = 8;
                ctx.shadowColor = p.color;
                ctx.fill();
                ctx.shadowBlur = 0;

                for (let j = i + 1; j < count; j++) {
                    let p2 = particles[j];
                    let dist = Math.hypot(p.x - p2.x, p.y - p2.y);
                    if (dist < 130) {
                        ctx.beginPath();
                        ctx.moveTo(p.x, p.y);
                        ctx.lineTo(p2.x, p2.y);
                        ctx.strokeStyle = p.color;
                        ctx.globalAlpha = 1 - (dist / 130);
                        ctx.lineWidth = 0.5;
                        ctx.stroke();
                        ctx.globalAlpha = 1;
                    }
                }
            }
            requestAnimationFrame(render);
        }
        render();

        function toggleSidebar() {
            const sidebar = document.getElementById('sidebar');
            const backdrop = document.getElementById('sidebarBackdrop');
            sidebar.classList.toggle('-translate-x-full');
            backdrop.classList.toggle('hidden');
        }

        const titles = {
            'tab-dashboard': 'Dashboard',
            'tab-orders': 'Customer Orders',
            'tab-users': 'Manage Users',
            'tab-broadcast': 'Mass Broadcast',
            'tab-settings': 'Setting & UPI',
            'tab-bot': 'Bot API & Polling',
            'tab-media': 'Media & Greeting Texts',
            'tab-plans': 'Subscription Pricing'
        };

        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
            
            const target = document.getElementById(tabId);
            if (target) target.classList.remove('hidden');

            const saveBar = document.getElementById('saveBar');
            if (['tab-bot', 'tab-media'].includes(tabId)) {
                saveBar.classList.remove('hidden');
            } else {
                saveBar.classList.add('hidden');
            }

            document.getElementById('sectionTitle').innerText = titles[tabId] || 'Nagato Panel';

            document.querySelectorAll('.nav-btn').forEach(btn => {
                btn.classList.remove('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400', 'shadow-[0_0_12px_rgba(0,240,255,0.15)]');
                btn.classList.add('text-slate-400');
            });
            const activeNav = document.getElementById('nav-' + tabId);
            if (activeNav) {
                activeNav.classList.add('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400', 'shadow-[0_0_12px_rgba(0,240,255,0.15)]');
                activeNav.classList.remove('text-slate-400');
            }

            if (window.innerWidth < 768) {
                const sidebar = document.getElementById('sidebar');
                if (!sidebar.classList.contains('-translate-x-full')) {
                    toggleSidebar();
                }
            }
        }

        const urlParams = new URLSearchParams(window.location.search);
        const requestedTab = urlParams.get('tab');
        if (requestedTab && titles[requestedTab]) {
            switchTab(requestedTab);
        }

        async function autoDetectName() {
            const upiId = document.getElementById('upi_id_input').value.trim();
            const statusEl = document.getElementById('detectStatus');
            const spinner = document.getElementById('detectSpinner');
            const nameInput = document.getElementById('payee_name_input');

            if (!upiId || !upiId.includes('@')) {
                statusEl.innerText = "Please enter a valid UPI ID first (e.g., name@bank).";
                statusEl.className = "text-[11px] text-rose-400 mt-1.5 font-mono";
                return;
            }

            spinner.classList.remove('hidden');
            statusEl.innerText = "Querying directory for registered account name...";
            statusEl.className = "text-[11px] text-cyan-400 mt-1.5 font-mono";

            try {
                const response = await fetch('/api/detect-upi-name?upi_id=' + encodeURIComponent(upiId));
                const data = await response.json();
                spinner.classList.add('hidden');

                if (data.status === "success" && data.name) {
                    nameInput.value = data.name;
                    statusEl.innerText = "Payee name successfully detected: " + data.name;
                    statusEl.className = "text-[11px] text-emerald-400 mt-1.5 font-mono";
                } else {
                    statusEl.innerText = "Bank registry lookup restricted on foreign cloud IP. Please type your name manually above.";
                    statusEl.className = "text-[11px] text-amber-400 mt-1.5 font-mono";
                }
            } catch (err) {
                spinner.classList.add('hidden');
                statusEl.innerText = "Lookup service unreachable. Please type your name manually.";
                statusEl.className = "text-[11px] text-amber-400 mt-1.5 font-mono";
            }
        }

        const expiryDateStr = "{{ panel_expiry }}";
        const expiryDate = new Date(expiryDateStr.replace(' ', 'T')).getTime();

        function updateCountdown() {
            const now = new Date().getTime();
            const distance = expiryDate - now;

            const timerEl = document.getElementById("countdownTimer");
            const badgeEl = document.getElementById("expiryBadge");

            if (!timerEl || !badgeEl) return;

            if (isNaN(distance) || distance <= 0) {
                timerEl.innerText = "EXPIRED";
                timerEl.className = "font-tech text-lg md:text-xl font-extrabold text-rose-400 tracking-tight";
                badgeEl.innerText = "EXPIRED";
                badgeEl.className = "text-[10px] font-tech px-2 py-0.5 rounded-md bg-rose-500/20 text-rose-400 border border-rose-500/40";
                return;
            }

            const days = Math.floor(distance / (1000 * 60 * 60 * 24));
            const hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            const minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
            const seconds = Math.floor((distance % (1000 * 60)) / 1000);

            timerEl.innerText = days + "d " + hours + "h " + minutes + "m " + seconds + "s";
        }

        setInterval(updateCountdown, 1000);
        updateCountdown();
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


@app.get("/api/detect-upi-name")
async def api_detect_upi_name(upi_id: str, is_auth: bool = Depends(require_admin)):
    name = await detect_payee_name_from_upi(upi_id)
    if name:
        return JSONResponse(content={"status": "success", "upi_id": upi_id, "name": name})
    return JSONResponse(
        content={
            "status": "failed",
            "message": "Live NPCI lookup blocked on cloud server. Please enter your name manually."
        }
    )


@app.get("/", response_class=HTMLResponse)
async def admin_dashboard(request: Request, message: str | None = None, is_auth: bool = Depends(require_admin)):
    metrics = await get_dashboard_metrics()
    users_list = await get_all_users_detailed()
    tmpl = Template(DASHBOARD_PAGE)
    html = tmpl.render(
        message=message,
        username="Nagato",
        is_online=manager.bot is not None,
        paid_orders=metrics["paid_orders"],
        revenue=metrics["revenue"],
        total_users=metrics["total_users"],
        recent_orders=metrics["recent_orders"],
        all_orders=metrics["all_orders"],
        users_list=users_list,
        bot_token=await get_setting("bot_token"),
        admin_chat_id=await get_setting("admin_chat_id"),
        upi_id=await get_setting("upi_id"),
        payee_name=await get_setting("payee_name"),
        panel_expiry=await get_setting("panel_expiry"),
        maintenance=await get_setting("maintenance"),
        welcome_text=await get_setting("welcome_text"),
        plans_text=await get_setting("plans_text"),
        welcome_photo=await get_setting("welcome_photo"),
        demo_video=await get_setting("demo_video"),
        plans=await get_all_plans(),
    )
    return HTMLResponse(content=html)


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

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as cur:
            users = await cur.fetchall()

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
    user_id: int = Form(...),
    status: int = Form(...),
    is_auth: bool = Depends(require_admin),
):
    await set_user_ban_status(user_id, status)
    action_text = "banned" if status == 1 else "unbanned"
    return RedirectResponse(
        url=f"/?message=User+{user_id}+{action_text}+successfully&tab=tab-users",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@app.post("/admin/users/subscription")
async def handle_user_subscription_change(
    user_id: int = Form(...),
    plan_name: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    await update_user_subscription(user_id, plan_name)
    if manager.bot:
        try:
            if plan_name == "Free":
                await manager.bot.send_message(user_id, "Your premium subscription has ended. You are now on the Free tier.")
            else:
                await manager.bot.send_message(user_id, f"Congratulations! You have been granted access to {plan_name}!")
        except Exception:
            pass

    return RedirectResponse(
        url=f"/?message=User+{user_id}+subscription+updated+to+{plan_name}&tab=tab-users",
        status_code=status.HTTP_303_SEE_OTHER,
    )


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
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM payments")
        await db.commit()
    return RedirectResponse(url="/?message=Revenue+counters+reset+to+zero&tab=tab-dashboard", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/plans/add")
async def handle_add_plan(
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    await add_new_plan(plan_id.strip(), name.strip(), amount, validity.strip())
    return RedirectResponse(url="/?message=New+plan+added+successfully&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/admin/plans/update")
async def update_plan_details(
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    is_auth: bool = Depends(require_admin),
):
    await update_plan(plan_id, name.strip(), amount, validity.strip())
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
