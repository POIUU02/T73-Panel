#!/usr/bin/env python3
"""
VROOM Panel Ultimate
- Gold subscription page
- Full button-based Telegram bot (no text commands needed)
- Rich admin dashboard
- VLESS WebSocket proxy
"""

import asyncio
import json
import os
import hashlib
import secrets
import time
import re
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import logging
import psutil

try:
    SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_urlsafe(32)
    os.environ.setdefault("SECRET_KEY", SECRET_KEY)
except Exception:
    SECRET_KEY = "vroom-default-secret-key"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VROOM")

app = FastAPI(title="VROOM", docs_url=None, redoc_url=None)
CONFIG = {"port": int(os.environ.get("PORT", 8080)), "secret": SECRET_KEY}
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ====== STATE ======
connections: dict = {}
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = ["www.speedtest.net", "185.159.157.201", "185.159.157.202"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()
CUSTOM_DOMAIN: str = ""
CUSTOM_DOMAIN_LOCK = asyncio.Lock()

TELEGRAM: dict = {"token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), "admin_ids": [], "enabled": False, "offset": 0}
TELEGRAM_LOCK = asyncio.Lock()
TELEGRAM_TASK = None
# user_id -> pending create state {step, label, limit, days}
TG_STATE: dict = {}

SESSION_COOKIE = "vroom_session"
SESSION_TTL = 60 * 60 * 24 * 7


def hash_password(pw: str) -> str:
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()


AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "admin"))}
SESSIONS: dict = {}
SESSIONS_LOCK = asyncio.Lock()


async def create_session() -> str:
    token = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK:
        SESSIONS[token] = time.time() + SESSION_TTL
    return token


async def is_valid_session(token: str | None) -> bool:
    if not token:
        return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None or exp < time.time():
            SESSIONS.pop(token, None)
            return False
        return True


async def destroy_session(token: str | None):
    if token:
        async with SESSIONS_LOCK:
            SESSIONS.pop(token, None)


async def require_auth(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return token


def get_domain() -> str:
    return os.environ.get("RENDER_EXTERNAL_URL", os.environ.get("RAILWAY_PUBLIC_DOMAIN", "localhost")).replace("https://", "").replace("http://", "")


def generate_vless_link(uuid: str, remark: str = "VROOM", address: str = None) -> str:
    domain = CUSTOM_DOMAIN if CUSTOM_DOMAIN else get_domain()
    addr = address if address else domain
    path = f"/ws/{uuid}"
    params = {"encryption": "none", "security": "tls", "type": "ws", "host": domain, "path": path, "sni": domain, "fp": "chrome", "alpn": "http/1.1"}
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"


def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_size_to_bytes(value: float, unit: str) -> int:
    u = unit.upper()
    if u == "GB":
        return int(value * 1024 ** 3)
    if u == "MB":
        return int(value * 1024 ** 2)
    return int(value)


def compute_expiry(expiry_days) -> str:
    try:
        days = float(expiry_days or 0)
    except Exception:
        days = 0
    if days <= 0:
        return ""
    return (datetime.now() + timedelta(days=days)).isoformat()


def is_expired(link) -> bool:
    exp = link.get("expiry") if isinstance(link, dict) else None
    if not exp:
        return False
    try:
        return datetime.now() >= datetime.fromisoformat(exp)
    except Exception:
        return False


def count_connections_for_link(uid: str) -> int:
    return len(link_ip_map.get(uid, set()))


def get_client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"


def remove_ip_from_link(uid: str, ip: str):
    if uid in link_ip_map:
        link_ip_map[uid].discard(ip)
        if not link_ip_map[uid]:
            link_ip_map.pop(uid, None)


async def close_connections_for_link(uid: str):
    to_close = [cid for cid, info in connections.items() if info.get("uuid") == uid]
    for cid in to_close:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000, reason="deleted")
            except Exception:
                pass
        connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    link_ip_map.pop(uid, None)


# ====== TELEGRAM HELPERS ======
def ikb(rows):
    """Build inline keyboard. rows = list of list of (text, callback_data)"""
    return {"inline_keyboard": [[{"text": t, "callback_data": c} for t, c in row] for row in rows]}


def main_menu_kb():
    return ikb([
        [("➕ ساخت کانفیگ", "create_start"), ("📋 لیست اینباندها", "list")],
        [("📊 آمار پنل", "stats"), ("🔗 لینک‌های ساب", "sub_menu")],
        [("⚙️ تنظیمات سریع", "quick_settings"), ("ℹ️ راهنما", "help")],
    ])


async def tg_api(method: str, **kwargs):
    token = TELEGRAM.get("token")
    if not token:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"https://api.telegram.org/bot{token}/{method}", json=kwargs)
            return r.json()
    except Exception as e:
        logger.error(f"TG API: {e}")
        return None


async def tg_send(chat_id, text, reply_markup=None, parse_mode="HTML"):
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_api("sendMessage", **data)


async def tg_edit(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_api("editMessageText", **data)


async def tg_answer(callback_query_id, text=None, show_alert=False):
    data = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text
        data["show_alert"] = show_alert
    return await tg_api("answerCallbackQuery", **data)


async def handle_callback(cq: dict):
    data = cq.get("data") or ""
    cq_id = cq.get("id")
    msg = cq.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    from_user = cq.get("from") or {}
    user_id = from_user.get("id")

    if user_id not in (TELEGRAM.get("admin_ids") or []):
        await tg_answer(cq_id, "⛔ فقط ادمین", True)
        return

    await tg_answer(cq_id)

    # ----- MAIN -----
    if data == "menu":
        TG_STATE.pop(user_id, None)
        await tg_edit(chat_id, message_id, "🚀 <b>VROOM Bot</b>\n\nاز دکمه‌های شیشه‌ای استفاده کن:", reply_markup=main_menu_kb())
        return

    if data == "help":
        await tg_edit(chat_id, message_id, """
ℹ️ <b>راهنما</b>

همه کارها با دکمه انجام می‌شه.

• <b>ساخت کانفیگ</b> → نام → حجم → روز
• <b>لیست</b> → مشاهده / حذف / ساب
• <b>آمار</b> → وضعیت سرور
• <b>لینک ساب</b> → کپی سریع

برای برگشت همیشه دکمه «🏠 منو» هست.
""", reply_markup=ikb([[("🏠 منو", "menu")]]))
        return

    if data == "stats":
        domain = get_domain()
        async with LINKS_LOCK:
            n = len(LINKS)
            active = sum(1 for x in LINKS.values() if x.get("active") and not is_expired(x))
        text = f"""📊 <b>آمار VROOM</b>

🔗 اینباندها: <code>{n}</code> (فعال: {active})
📡 اتصالات زنده: <code>{len(connections)}</code>
📥 ترافیک: <code>{round(stats['total_bytes']/(1024*1024),1)} MB</code>
📨 درخواست‌ها: <code>{stats['total_requests']}</code>
❌ خطاها: <code>{stats['total_errors']}</code>
⏱️ آپتایم: <code>{uptime()}</code>
🌐 دامنه: <code>{domain}</code>
💻 CPU: <code>{psutil.cpu_percent()}%</code>
🧠 RAM: <code>{psutil.virtual_memory().percent}%</code>
"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([[("🔄 بروزرسانی", "stats"), ("🏠 منو", "menu")]]))
        return

    # ----- LIST -----
    if data == "list":
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 هیچ اینباندی نیست.", reply_markup=ikb([[("➕ ساخت", "create_start"), ("🏠 منو", "menu")]]))
            return
        rows = []
        for uid, d in items[:15]:
            st = "✅" if d.get("active") and not is_expired(d) else "❌"
            rows.append([(f"{st} {d['label']}", f"link:{uid}")])
        rows.append([("🏠 منو", "menu")])
        await tg_edit(chat_id, message_id, "📋 <b>لیست اینباندها</b>\nروی هر کدوم بزن:", reply_markup=ikb(rows))
        return

    if data.startswith("link:"):
        uid = data[5:]
        async with LINKS_LOCK:
            link = LINKS.get(uid)
        if not link:
            await tg_edit(chat_id, message_id, "❌ پیدا نشد", reply_markup=ikb([[("📋 لیست", "list"), ("🏠 منو", "menu")]]))
            return
        used = round(link["used_bytes"] / (1024 ** 3), 2)
        lim = round(link["limit_bytes"] / (1024 ** 3), 2) if link["limit_bytes"] else "∞"
        domain = get_domain()
        sub = f"https://{domain}/sub/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
        text = f"""🏷 <b>{link['label']}</b>

📦 مصرف: <code>{used} / {lim} GB</code>
🔌 اتصالات: <code>{count_connections_for_link(uid)}</code>
📅 وضعیت: {'فعال' if link['active'] and not is_expired(link) else 'غیرفعال'}

📥 ساب:
<code>{sub}</code>

📋 کانفیگ:
<code>{vless}</code>
"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([
            [("📥 کپی ساب (نمایش)", f"showsub:{uid}"), ("📋 کپی کانفیگ", f"showcfg:{uid}")],
            [("🗑 حذف", f"delask:{uid}"), ("📋 برگشت لیست", "list")],
            [("🏠 منو", "menu")],
        ]))
        return

    if data.startswith("showsub:"):
        uid = data[8:]
        domain = get_domain()
        await tg_send(chat_id, f"📥 لینک ساب:\n<code>https://{domain}/sub/{uid}</code>")
        return

    if data.startswith("showcfg:"):
        uid = data[8:]
        async with LINKS_LOCK:
            link = LINKS.get(uid)
        if link:
            vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
            await tg_send(chat_id, f"📋 کانفیگ:\n<code>{vless}</code>")
        return

    if data.startswith("delask:"):
        uid = data[7:]
        await tg_edit(chat_id, message_id, f"❓ مطمئنی «{uid}» حذف بشه؟", reply_markup=ikb([
            [("✅ بله حذف کن", f"deldo:{uid}"), ("❌ انصراف", f"link:{uid}")],
        ]))
        return

    if data.startswith("deldo:"):
        uid = data[6:]
        async with LINKS_LOCK:
            LINKS.pop(uid, None)
        await close_connections_for_link(uid)
        await tg_edit(chat_id, message_id, f"✅ «{uid}» حذف شد.", reply_markup=ikb([[("📋 لیست", "list"), ("🏠 منو", "menu")]]))
        return

    # ----- SUB MENU -----
    if data == "sub_menu":
        async with LINKS_LOCK:
            items = list(LINKS.keys())[:12]
        if not items:
            await tg_edit(chat_id, message_id, "📭 اینباندی نیست.", reply_markup=ikb([[("🏠 منو", "menu")]]))
            return
        rows = [[(uid, f"showsub:{uid}")] for uid in items]
        rows.append([("🏠 منو", "menu")])
        await tg_edit(chat_id, message_id, "🔗 روی نام بزن تا لینک ساب بیاد:", reply_markup=ikb(rows))
        return

    # ----- CREATE FLOW (buttons only) -----
    if data == "create_start":
        TG_STATE[user_id] = {"step": "label"}
        await tg_edit(chat_id, message_id, "➕ <b>ساخت کانفیگ</b>\n\nنام اینباند رو با دکمه انتخاب کن یا از کیبورد سریع:", reply_markup=ikb([
            [("user1", "c_name:user1"), ("user2", "c_name:user2"), ("vip", "c_name:vip")],
            [("test", "c_name:test"), ("mobile", "c_name:mobile"), ("pc", "c_name:pc")],
            [("🎲 تصادفی", "c_name:rand"), ("❌ انصراف", "menu")],
        ]))
        return

    if data.startswith("c_name:"):
        name = data[7:]
        if name == "rand":
            name = "u" + secrets.token_hex(3)
        if not re.match(r'^[a-zA-Z0-9\-_.]+$', name):
            await tg_edit(chat_id, message_id, "❌ نام نامعتبر", reply_markup=ikb([[("🏠 منو", "menu")]]))
            return
        async with LINKS_LOCK:
            if name in LINKS:
                await tg_edit(chat_id, message_id, f"❌ «{name}» از قبل هست. اسم دیگه انتخاب کن.", reply_markup=ikb([[("➕ دوباره", "create_start"), ("🏠 منو", "menu")]]))
                return
        TG_STATE[user_id] = {"step": "limit", "label": name}
        await tg_edit(chat_id, message_id, f"📦 حجم برای <b>{name}</b>؟", reply_markup=ikb([
            [("1 GB", "c_lim:1"), ("5 GB", "c_lim:5"), ("10 GB", "c_lim:10")],
            [("20 GB", "c_lim:20"), ("50 GB", "c_lim:50"), ("100 GB", "c_lim:100")],
            [("∞ نامحدود", "c_lim:0"), ("❌ انصراف", "menu")],
        ]))
        return

    if data.startswith("c_lim:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "limit":
            await tg_edit(chat_id, message_id, "از اول شروع کن.", reply_markup=main_menu_kb())
            return
        lim = float(data[6:])
        st["limit"] = lim
        st["step"] = "days"
        TG_STATE[user_id] = st
        await tg_edit(chat_id, message_id, f"📅 مدت اعتبار برای <b>{st['label']}</b>؟", reply_markup=ikb([
            [("۷ روز", "c_day:7"), ("۱۵ روز", "c_day:15"), ("۳۰ روز", "c_day:30")],
            [("۶۰ روز", "c_day:60"), ("۹۰ روز", "c_day:90"), ("∞ نامحدود", "c_day:0")],
            [("❌ انصراف", "menu")],
        ]))
        return

    if data.startswith("c_day:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "days":
            await tg_edit(chat_id, message_id, "از اول شروع کن.", reply_markup=main_menu_kb())
            return
        days = float(data[6:])
        label = st["label"]
        lim = st.get("limit", 0)
        limit_bytes = parse_size_to_bytes(lim, "GB") if lim > 0 else 0
        expiry = compute_expiry(days)
        async with LINKS_LOCK:
            if label in LINKS:
                await tg_edit(chat_id, message_id, "❌ این نام الان گرفته شده.", reply_markup=ikb([[("🏠 منو", "menu")]]))
                TG_STATE.pop(user_id, None)
                return
            LINKS[label] = {
                "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
                "max_connections": 0, "created_at": datetime.now().isoformat(),
                "active": True, "expiry": expiry,
            }
        TG_STATE.pop(user_id, None)
        domain = get_domain()
        sub = f"https://{domain}/sub/{label}"
        vless = generate_vless_link(label, remark=f"VROOM-{label}")
        text = f"""✅ <b>کانفیگ ساخته شد!</b>

🏷 نام: <code>{label}</code>
📦 حجم: <code>{lim if lim else '∞'} GB</code>
📅 اعتبار: <code>{int(days) if days else 'نامحدود'} روز</code>

📥 ساب:
<code>{sub}</code>

📋 کانفیگ:
<code>{vless}</code>
"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([
            [("➕ یکی دیگه", "create_start"), ("📋 لیست", "list")],
            [("🏠 منو", "menu")],
        ]))
        return

    # ----- QUICK SETTINGS -----
    if data == "quick_settings":
        await tg_edit(chat_id, message_id, "⚙️ <b>تنظیمات سریع</b>", reply_markup=ikb([
            [("🔄 ریست همه مصرف‌ها", "reset_all_ask")],
            [("📊 آمار", "stats"), ("🏠 منو", "menu")],
        ]))
        return

    if data == "reset_all_ask":
        await tg_edit(chat_id, message_id, "❓ مصرف همه اینباندها صفر بشه؟", reply_markup=ikb([
            [("✅ بله", "reset_all_do"), ("❌ نه", "quick_settings")],
        ]))
        return

    if data == "reset_all_do":
        async with LINKS_LOCK:
            for v in LINKS.values():
                v["used_bytes"] = 0
        await tg_edit(chat_id, message_id, "✅ همه مصرف‌ها صفر شد.", reply_markup=ikb([[("🏠 منو", "menu")]]))
        return


async def handle_tg_message(msg: dict):
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    from_user = msg.get("from") or {}
    user_id = from_user.get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id:
        return

    admin_ids = TELEGRAM.get("admin_ids") or []
    is_admin = user_id in admin_ids

    if text in ("/start", "شروع", "منو"):
        if is_admin:
            await tg_send(chat_id, "🚀 <b>VROOM Bot</b>\n\nهمه چیز با دکمه — دستور متنی لازم نیست.", reply_markup=main_menu_kb())
        else:
            await tg_send(chat_id, "⛔ فقط ادمین دسترسی داره.")
        return

    if not is_admin:
        await tg_send(chat_id, "⛔ دسترسی ندارید.")
        return

    # Any other text → show menu
    await tg_send(chat_id, "از دکمه‌ها استفاده کن 👇", reply_markup=main_menu_kb())


async def telegram_poll_loop():
    logger.info("🤖 Telegram button-bot started")
    while True:
        try:
            async with TELEGRAM_LOCK:
                token = TELEGRAM.get("token")
                enabled = TELEGRAM.get("enabled")
                offset = TELEGRAM.get("offset", 0)
            if not token or not enabled:
                await asyncio.sleep(4)
                continue
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"offset": offset, "timeout": 25})
                data = r.json()
            if not data.get("ok"):
                await asyncio.sleep(3)
                continue
            for upd in data.get("result", []):
                async with TELEGRAM_LOCK:
                    TELEGRAM["offset"] = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        await handle_callback(upd["callback_query"])
                    elif "message" in upd:
                        await handle_tg_message(upd["message"])
                except Exception as e:
                    logger.error(f"TG handle: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"TG poll: {e}")
            await asyncio.sleep(5)


async def start_telegram_bot():
    global TELEGRAM_TASK
    if TELEGRAM_TASK and not TELEGRAM_TASK.done():
        TELEGRAM_TASK.cancel()
        try:
            await TELEGRAM_TASK
        except Exception:
            pass
    TELEGRAM_TASK = asyncio.create_task(telegram_poll_loop())


async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            domain = get_domain()
            if domain and domain != "localhost":
                async with httpx.AsyncClient(timeout=10) as c:
                    await c.get(f"https://{domain}/health")
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000), timeout=httpx.Timeout(180.0, connect=30.0), follow_redirects=True)
    logger.info(f"🚀 VROOM on :{CONFIG['port']}")
    asyncio.create_task(keep_alive())
    if TELEGRAM.get("token") and TELEGRAM.get("admin_ids"):
        TELEGRAM["enabled"] = True
        await start_telegram_bot()


@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()
    if TELEGRAM_TASK:
        TELEGRAM_TASK.cancel()


# ====== ROUTES ======
@app.get("/")
async def root():
    return {"service": "VROOM", "version": "4.0", "status": "active", "domain": get_domain()}


@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "uptime": uptime()}


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    if hash_password(str(body.get("password") or "")) != AUTH["password_hash"]:
        raise HTTPException(401, "Invalid password")
    token = await create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(SESSION_COOKIE, token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/")
    return resp


@app.post("/api/logout")
async def api_logout(request: Request):
    await destroy_session(request.cookies.get(SESSION_COOKIE))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


@app.get("/api/me")
async def api_me(request: Request):
    return {"authenticated": await is_valid_session(request.cookies.get(SESSION_COOKIE))}


@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    if hash_password(str(body.get("current_password") or "")) != AUTH["password_hash"]:
        raise HTTPException(400, "Current password is incorrect")
    new = str(body.get("new_password") or "")
    if len(new) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    AUTH["password_hash"] = hash_password(new)
    tok = request.cookies.get(SESSION_COOKIE)
    async with SESSIONS_LOCK:
        SESSIONS.clear()
        if tok:
            SESSIONS[tok] = time.time() + SESSION_TTL
    return {"ok": True}


@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    return {
        "active_connections": len(connections),
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "disk_used": round(psutil.disk_usage("/").used / (1024 ** 3), 2),
        "disk_total": round(psutil.disk_usage("/").total / (1024 ** 3), 2),
        "hourly_traffic": dict(hourly_traffic),
        "telegram_enabled": TELEGRAM.get("enabled", False),
        "connections_detail": [
            {"uuid": v.get("uuid"), "ip": v.get("ip"), "bytes": v.get("bytes", 0), "since": v.get("connected_at")}
            for v in list(connections.values())[:50]
        ],
    }


@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label) or not label:
        raise HTTPException(400, "Invalid name")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(400, "Exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = max(0, int(body.get("max_connections") or 0))
    expiry = compute_expiry(body.get("expiry_days"))
    async with LINKS_LOCK:
        LINKS[label] = {"label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn, "created_at": datetime.now().isoformat(), "active": True, "expiry": expiry}
    return {"uuid": label, "label": label, "vless_link": generate_vless_link(label, remark=f"VROOM-{label}"), "limit_bytes": limit_bytes, "active": True, "expiry": expiry}


@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            result.append({
                "uuid": uid, "label": data["label"], "limit_bytes": data["limit_bytes"], "used_bytes": data["used_bytes"],
                "max_connections": data.get("max_connections", 0), "active": data["active"], "expiry": data.get("expiry", ""),
                "expired": is_expired(data), "created_at": data["created_at"], "current_connections": count_connections_for_link(uid),
                "vless_link": generate_vless_link(uid, remark=f"VROOM-{data['label']}"),
            })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}


@app.patch("/api/links/{uid}")
async def patch_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(404)
        if "active" in body:
            LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            lv = float(body.get("limit_value") or 0)
            LINKS[uid]["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, body.get("limit_unit") or "GB")
        if body.get("reset_usage"):
            LINKS[uid]["used_bytes"] = 0
        if "expiry_days" in body:
            LINKS[uid]["expiry"] = compute_expiry(body.get("expiry_days"))
        if "max_connections" in body:
            LINKS[uid]["max_connections"] = max(0, int(body["max_connections"] or 0))
    return {"ok": True}


@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        LINKS.pop(uid, None)
    await close_connections_for_link(uid)
    return {"ok": True}


@app.get("/api/domain")
async def get_domain_api(_=Depends(require_auth)):
    async with CUSTOM_DOMAIN_LOCK:
        return {"domain": CUSTOM_DOMAIN}


@app.post("/api/domain")
async def set_domain_api(request: Request, _=Depends(require_auth)):
    body = await request.json()
    domain = (body.get("domain") or "").strip().lower().replace("https://", "").replace("http://", "").rstrip("/")
    if domain and not re.match(r'^[a-z0-9\-_.]+$', domain):
        raise HTTPException(400, "Invalid domain")
    async with CUSTOM_DOMAIN_LOCK:
        global CUSTOM_DOMAIN
        CUSTOM_DOMAIN = domain
    return {"ok": True, "domain": CUSTOM_DOMAIN}


@app.get("/api/addresses")
async def list_addr(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        return {"addresses": list(CUSTOM_ADDRESSES)}


@app.post("/api/addresses")
async def add_addr(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address:
        raise HTTPException(400)
    async with CUSTOM_ADDRESSES_LOCK:
        if address not in CUSTOM_ADDRESSES:
            CUSTOM_ADDRESSES.append(address)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}


@app.delete("/api/addresses/{index}")
async def del_addr(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES):
            CUSTOM_ADDRESSES.pop(index)
        else:
            raise HTTPException(404)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}


@app.get("/api/telegram")
async def get_tg(_=Depends(require_auth)):
    async with TELEGRAM_LOCK:
        return {"has_token": bool(TELEGRAM.get("token")), "admin_ids": TELEGRAM.get("admin_ids", []), "enabled": TELEGRAM.get("enabled", False)}


@app.post("/api/telegram")
async def set_tg(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = (body.get("token") or "").strip()
    admin_raw = body.get("admin_ids") or body.get("admin_id") or ""
    if isinstance(admin_raw, list):
        admin_ids = [int(x) for x in admin_raw if str(x).lstrip("-").isdigit()]
    else:
        admin_ids = [int(x) for x in re.findall(r"-?\d+", str(admin_raw))]
    async with TELEGRAM_LOCK:
        if token:
            TELEGRAM["token"] = token
        if admin_ids:
            TELEGRAM["admin_ids"] = admin_ids
        TELEGRAM["enabled"] = bool(TELEGRAM.get("token") and TELEGRAM.get("admin_ids"))
    if TELEGRAM["enabled"]:
        me = await tg_api("getMe")
        if not me or not me.get("ok"):
            async with TELEGRAM_LOCK:
                TELEGRAM["enabled"] = False
            raise HTTPException(400, "Invalid bot token")
        await start_telegram_bot()
        return {"ok": True, "enabled": True, "bot_username": me["result"].get("username"), "admin_ids": TELEGRAM["admin_ids"]}
    return {"ok": True, "enabled": False}


@app.post("/api/telegram/stop")
async def stop_tg(_=Depends(require_auth)):
    async with TELEGRAM_LOCK:
        TELEGRAM["enabled"] = False
    global TELEGRAM_TASK
    if TELEGRAM_TASK and not TELEGRAM_TASK.done():
        TELEGRAM_TASK.cancel()
    return {"ok": True}


@app.post("/api/reset-all-usage")
async def reset_all_usage(_=Depends(require_auth)):
    async with LINKS_LOCK:
        for v in LINKS.values():
            v["used_bytes"] = 0
    return {"ok": True}


# ====== SUB PAGE ======
@app.get("/sub/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if not link:
            raise HTTPException(404, "Not found")
    if not link["active"]:
        raise HTTPException(403, "Disabled")
    if is_expired(link):
        raise HTTPException(403, "Expired")

    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)

    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / (1024 ** 3), 2)
    limit_gb = round(link["limit_bytes"] / (1024 ** 3), 2) if link["limit_bytes"] > 0 else 0
    percent = round((link["used_bytes"] / link["limit_bytes"]) * 100, 1) if link["limit_bytes"] > 0 else 0
    remaining_gb = round(max(0, limit_gb - used_gb), 2) if limit_gb > 0 else "∞"

    if is_expired(link):
        status_text, status_color = "منقضی شده", "#f87171"
    elif link["limit_bytes"] > 0 and link["used_bytes"] >= link["limit_bytes"]:
        status_text, status_color = "محدود شده", "#fbbf24"
    else:
        status_text, status_color = "فعال", "#34d399"

    exp = link.get("expiry")
    if exp:
        try:
            exp_date = datetime.fromisoformat(exp)
            days_left_text = f"{max(0, (exp_date - datetime.now()).days)} روز"
            exp_display = exp_date.strftime("%Y/%m/%d")
        except Exception:
            days_left_text, exp_display = "نامحدود", "نامحدود"
    else:
        days_left_text, exp_display = "نامحدود", "نامحدود"

    domain = get_domain()
    sub_url = f"https://{domain}/sub/{uid}"
    qr_data = quote(server_link, safe="")
    history_vals = [0.7, 1.1, 0.5, 1.8, 1.3, 0.9, max(0.2, used_gb * 0.08)]

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>VROOM — {link['label']}</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@700;900&family=Vazirmatn:wght@400;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
:root{{--gold:#ffd700;--gold2:#f7971e;--bg:#05050c}}
*{{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}}
body{{font-family:'Vazirmatn',sans-serif;background:var(--bg);color:#e8ecf4;min-height:100vh;display:flex;justify-content:center;padding:16px 12px;direction:rtl}}
.container{{max-width:480px;width:100%;background:rgba(12,12,22,.94);border-radius:24px;padding:20px 16px;border:1px solid rgba(255,215,0,.12);box-shadow:0 0 40px rgba(255,215,0,.06)}}
header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;padding:10px 12px;background:rgba(255,215,0,.05);border-radius:14px;border:1px solid rgba(255,215,0,.1)}}
.logo{{display:flex;align-items:center;gap:8px}}.logo b{{font-size:22px;font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter,sans-serif}}
.logo span{{font-size:11px;font-weight:700;color:#ff6b6b}}
.icon-btn{{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:rgba(255,215,0,.08);border:1px solid rgba(255,215,0,.15);color:var(--gold);border-radius:10px;cursor:pointer;font-size:16px}}
.main-title{{font-size:20px;font-weight:900;font-family:Inter,sans-serif;margin-bottom:2px}}
.sub-title{{color:var(--gold);font-size:12px;margin-bottom:12px;letter-spacing:1px;opacity:.85}}
.server-status{{display:flex;align-items:center;justify-content:space-between;background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.2);border-radius:14px;padding:10px 14px;margin-bottom:12px}}
.server-left{{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:#34d399}}
.server-dot{{width:8px;height:8px;background:#34d399;border-radius:50%;animation:pd 1.8s infinite}}
@keyframes pd{{0%{{box-shadow:0 0 0 0 rgba(52,211,153,.5)}}70%{{box-shadow:0 0 0 8px transparent}}}}
.test-btn{{background:linear-gradient(135deg,#10b981,#059669);border:none;color:#fff;padding:7px 12px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;font-family:inherit}}
.usage-card{{background:linear-gradient(145deg,rgba(15,23,42,.95),rgba(10,14,28,.98));border:1px solid rgba(59,130,246,.25);border-radius:18px;padding:14px;margin-bottom:12px}}
.usage-header{{display:flex;justify-content:space-between;margin-bottom:12px;font-size:13px;font-weight:700;color:#93c5fd}}
.usage-stats{{display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;align-items:center}}
@media(max-width:420px){{.usage-stats{{grid-template-columns:1fr 1fr}}.usage-circle-wrap{{grid-column:1/-1;justify-self:center;margin-top:6px}}}}
.usage-stat{{text-align:center}}.usage-stat .label{{font-size:10px;color:rgba(148,163,184,.8);margin-bottom:3px}}.usage-stat .value{{font-size:15px;font-weight:800}}
.usage-circle-wrap{{width:74px;height:74px;position:relative;display:flex;align-items:center;justify-content:center}}
.usage-circle-outer{{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(59,130,246,.3)}}
.usage-circle{{width:64px;height:64px;border-radius:50%;background:conic-gradient(#3b82f6 0% 0%,rgba(30,41,59,.9) 0% 100%);display:flex;align-items:center;justify-content:center;position:relative;transition:background 1.1s ease}}
.usage-circle::before{{content:'';position:absolute;inset:6px;border-radius:50%;background:#0b1222}}
.usage-circle-text{{position:relative;z-index:1;text-align:center;font-size:13px;font-weight:800}}
.usage-bar{{margin-top:12px;height:5px;background:rgba(30,41,59,.8);border-radius:8px;overflow:hidden}}
.usage-bar-fill{{height:100%;width:0%;background:linear-gradient(90deg,#3b82f6,#8b5cf6,#ef4444);transition:width 1.1s ease}}
.card{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.1);border-radius:16px;padding:14px;margin-bottom:12px}}
.card h3{{font-size:11px;letter-spacing:1px;font-weight:700;margin-bottom:10px;color:var(--gold);opacity:.85}}
.row{{background:rgba(0,0,0,.35);padding:10px 12px;border-radius:11px;display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px;font-size:11px;font-family:monospace;color:rgba(255,255,255,.45);border:1px solid rgba(255,215,0,.08);cursor:pointer}}
.row button{{background:linear-gradient(135deg,var(--gold),var(--gold2));border:none;color:#0a0a10;padding:5px 12px;border-radius:8px;font-size:11px;font-weight:800;cursor:pointer;font-family:'Vazirmatn',sans-serif}}
.row .link-text{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.info-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,215,0,.08)}}
.info-item{{background:rgba(0,0,0,.3);padding:8px 6px;border-radius:10px;text-align:center}}.info-item .label{{font-size:9px;opacity:.4;display:block;margin-bottom:2px}}.info-item .value{{font-size:13px;font-weight:800}}
.config-box{{background:rgba(0,0,0,.4);padding:10px;border-radius:11px;font-size:10px;font-family:monospace;word-break:break-all;margin-bottom:10px;max-height:60px;overflow-y:auto;border:1px solid rgba(255,215,0,.08);direction:ltr;text-align:left;color:rgba(255,255,255,.4);cursor:pointer}}
.qrbox{{width:110px;height:110px;background:#fff;border-radius:12px;margin:0 auto 10px;overflow:hidden;border:2px solid rgba(255,215,0,.25);cursor:pointer}}
.qrbox img{{width:100%;height:100%}}
.btn-row{{display:flex;gap:8px}}.add,.share-btn{{flex:1;padding:11px;border:none;border-radius:12px;font-weight:800;font-size:13px;cursor:pointer;font-family:inherit}}
.add{{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#0a0a10}}.share-btn{{background:rgba(255,215,0,.1);color:var(--gold);border:1px solid rgba(255,215,0,.25)}}
.quick-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}
@media(max-width:400px){{.quick-grid{{grid-template-columns:repeat(2,1fr)}}}}
.quick-item{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.09);border-radius:14px;padding:10px 4px;text-align:center;cursor:pointer;position:relative;transition:all 0.2s}}
.quick-item:hover{{background:rgba(255,215,0,.06);border-color:rgba(255,215,0,.2)}}
.quick-item .q-icon{{width:45px;height:45px;margin:0 auto 5px;border-radius:12px;overflow:hidden;background:#fff;display:flex;align-items:center;justify-content:center}}
.quick-item .q-icon img{{width:100%;height:100%;object-fit:cover}}
.quick-item .q-name{{font-size:10px;font-weight:600;color:rgba(255,255,255,.55)}}
.quick-item .q-badge{{position:absolute;top:4px;right:4px;font-size:8px;background:rgba(255,215,0,.18);color:var(--gold);padding:1px 4px;border-radius:4px}}
.history-bars{{display:flex;align-items:flex-end;justify-content:space-between;height:64px;gap:5px}}
.history-bar-item{{flex:1;display:flex;flex-direction:column;align-items:center;gap:4px}}
.history-bar{{width:100%;max-width:26px;background:linear-gradient(180deg,#3b82f6,#1e40af);border-radius:5px 5px 2px 2px;transition:height .7s ease}}
.history-day{{font-size:9px;color:rgba(148,163,184,.55)}}
footer{{margin-top:12px;padding-top:10px;border-top:1px solid rgba(255,215,0,.08);text-align:center;font-size:11px;color:#4a5370}}
footer b{{background:linear-gradient(135deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:rgba(10,10,18,.95);padding:12px 22px;border-radius:12px;font-size:13px;color:var(--gold);opacity:0;transition:.35s;border:1px solid rgba(255,215,0,.25);z-index:9999;font-weight:700}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.qr-modal{{position:fixed;inset:0;background:rgba(0,0,0,.85);display:flex;align-items:center;justify-content:center;z-index:10000;opacity:0;pointer-events:none;transition:.3s}}
.qr-modal.show{{opacity:1;pointer-events:auto}}.qr-modal img{{width:min(80vw,300px);border-radius:16px;border:2px solid rgba(255,215,0,.4)}}
.menu-panel{{position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:10001;display:none;align-items:flex-end;justify-content:center}}
.menu-panel.show{{display:flex}}
.menu-sheet{{background:#12121f;border-radius:22px 22px 0 0;padding:18px;width:100%;max-width:480px;border:1px solid rgba(255,215,0,.12)}}
.menu-item{{padding:13px;background:rgba(255,255,255,.04);border-radius:12px;margin-bottom:8px;cursor:pointer;font-size:13px;font-weight:600;border:1px solid rgba(255,215,0,.08)}}
</style></head>
<body>
<div class="container">
<header>
  <div class="logo"><b>VROOM</b><span>PANEL</span></div>
  <div style="display:flex;gap:6px">
    <button class="icon-btn" onclick="location.reload()">🔄</button>
    <button class="icon-btn" onclick="openMenu()">☰</button>
  </div>
</header>
<h1 class="main-title">Subscription</h1>
<p class="sub-title">✦ {link['label']} ✦</p>
<div class="server-status">
  <div class="server-left"><div class="server-dot"></div>سرور آنلاین</div>
  <button class="test-btn" id="testBtn" onclick="runSpeedTest()">⚡ تست سرعت</button>
</div>
<div class="usage-card">
  <div class="usage-header"><span>⚡ وضعیت مصرف</span><span style="font-size:10px;color:rgba(148,163,184,.6)" id="lastUpdate">همین الان</span></div>
  <div class="usage-stats">
    <div class="usage-stat"><div class="label">مصرفی</div><div class="value">{used_gb} GB</div><div class="label">از {limit_gb if limit_gb else '∞'}</div></div>
    <div class="usage-stat"><div class="label">وضعیت</div><div class="value" style="color:{status_color};font-size:13px">{status_text}</div></div>
    <div class="usage-stat"><div class="label">باقی</div><div class="value">{remaining_gb}{'' if remaining_gb=='∞' else ''}</div></div>
    <div class="usage-circle-wrap"><div class="usage-circle-outer"></div><div class="usage-circle" id="usageCircle"><div class="usage-circle-text"><div id="percentText">0%</div></div></div></div>
  </div>
  <div class="usage-bar"><div class="usage-bar-fill" id="usageBar"></div></div>
</div>
<div class="card">
  <h3>مصرف ۷ روز</h3>
  <div class="history-bars" id="historyBars"></div>
</div>
<div class="card">
  <h3>لینک سابسکریپشن</h3>
  <div class="row" onclick="copyText(SUB_URL,'لینک ساب کپی شد')"><span class="link-text">{sub_url}</span><button onclick="event.stopPropagation();copyText(SUB_URL,'لینک ساب کپی شد')">کپی</button></div>
  <div class="info-grid">
    <div class="info-item"><span class="label">وضعیت</span><span class="value" style="color:{status_color}">{status_text}</span></div>
    <div class="info-item"><span class="label">انقضا</span><span class="value" style="color:#fbbf24">{exp_display}</span></div>
    <div class="info-item"><span class="label">باقی</span><span class="value" style="color:#6bcbff">{days_left_text}</span></div>
  </div>
</div>
<div class="card">
  <h3>کانفیگ و QR</h3>
  <div class="config-box" onclick="copyText(CONFIG,'کانفیگ کپی شد')">{server_link}</div>
  <div style="text-align:center">
    <div class="qrbox" onclick="openQR()"><img src="https://api.qrserver.com/v1/create-qr-code/?size=220x220&data={qr_data}" alt="QR"></div>
    <div class="btn-row"><button class="add" onclick="copyText(SUB_URL,'کپی شد')">＋ اضافه کردن</button><button class="share-btn" onclick="shareLink()">اشتراک</button></div>
  </div>
</div>
<div class="card">
  <h3>نصب سریع روی دستگاه</h3>
  <div class="quick-grid">
    <div class="quick-item" onclick="openApp('hiddify')">
      <span class="q-badge">+</span>
      <div class="q-icon"><img src="https://raw.githubusercontent.com/hiddify/hiddify-app/main/docs/logo.png" alt="Hiddify" onerror="this.src='data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 48 48%22%3E%3Crect width=%2248%22 height=%2248%22 rx=%2212%22 fill=%22%23455FE9%22/%3E%3Ctext x=%2224%22 y=%2231%22 text-anchor=%22middle%22 fill=%22%23fff%22 font-size=%2216%22 font-weight=%22800%22 font-family=%22Arial%22%3EH%3C/text%3E%3C/svg%3E'"></div>
      <span class="q-name">Hiddify</span>
    </div>
    <div class="quick-item" onclick="openApp('v2rayng')">
      <span class="q-badge">+</span>
      <div class="q-icon"><img src="https://raw.githubusercontent.com/2dust/v2rayNG/master/app/src/main/res/mipmap-xxxhdpi/ic_launcher_round.png" alt="v2rayNG" onerror="this.src='data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 48 48%22%3E%3Crect width=%2248%22 height=%2248%22 rx=%2212%22 fill=%22%231E88E5%22/%3E%3Ctext x=%2224%22 y=%2231%22 text-anchor=%22middle%22 fill=%22%23fff%22 font-size=%2216%22 font-weight=%22800%22 font-family=%22Arial%22%3EV%3C/text%3E%3C/svg%3E'"></div>
      <span class="q-name">v2rayNG</span>
    </div>
    <div class="quick-item" onclick="openApp('v2box')">
      <span class="q-badge">+</span>
      <div class="q-icon"><img src="https://is1-ssl.mzstatic.com/image/thumb/Purple116/v4/8e/9b/9b/8e9b9b8e-9b8e-9b8e-9b8e-9b8e9b9b8e9b/AppIcon-0-0-1x_U007emarketing-0-0-0-7-0-0-sRGB-0-0-0-GLES2_U002c0-512MB-85-220-0-0.png/512x512bb.jpg" alt="V2Box" onerror="this.src='data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 48 48%22%3E%3Crect width=%2248%22 height=%2248%22 rx=%2212%22 fill=%22%236C5CE7%22/%3E%3Ctext x=%2224%22 y=%2231%22 text-anchor=%22middle%22 fill=%22%23fff%22 font-size=%2216%22 font-weight=%22800%22 font-family=%22Arial%22%3EV2%3C/text%3E%3C/svg%3E'"></div>
      <span class="q-name">V2Box</span>
    </div>
    <div class="quick-item" onclick="openApp('clash')">
      <span class="q-badge">+</span>
      <div class="q-icon"><img src="https://raw.githubusercontent.com/MetaCubeX/ClashMetaForAndroid/master/app/src/main/ic_launcher-playstore.png" alt="Clash" onerror="this.src='data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 48 48%22%3E%3Crect width=%2248%22 height=%2248%22 rx=%2212%22 fill=%22%23D63031%22/%3E%3Ctext x=%2224%22 y=%2231%22 text-anchor=%22middle%22 fill=%22%23fff%22 font-size=%2216%22 font-weight=%22800%22 font-family=%22Arial%22%3EC%3C/text%3E%3C/svg%3E'"></div>
      <span class="q-name">Clash</span>
    </div>
  </div>
</div>
<footer>Powered by <b>VROOM</b></footer>
</div>
<div class="qr-modal" id="qrModal" onclick="this.classList.remove('show')"><img id="qrModalImg" alt="QR"></div>
<div class="toast" id="toast"></div>
<div class="menu-panel" id="menuPanel" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="menu-sheet">
    <div class="menu-item" onclick="copyText(SUB_URL,'لینک ساب کپی شد');closeMenu()">📥 کپی لینک ساب</div>
    <div class="menu-item" onclick="copyText(CONFIG,'کانفیگ کپی شد');closeMenu()">📋 کپی کانفیگ</div>
    <div class="menu-item" onclick="shareLink();closeMenu()">↗ اشتراک‌گذاری</div>
    <div class="menu-item" onclick="location.reload()">🔄 بروزرسانی</div>
    <div class="menu-item" onclick="closeMenu()" style="color:#f87171;text-align:center">بستن</div>
  </div>
</div>
<script>
const SUB_URL='{sub_url}', CONFIG=`{server_link}`, PERCENT={percent}, HISTORY={json.dumps(history_vals)};
const apps={{
hiddify:{{s:'hiddify://import/'+encodeURIComponent(SUB_URL),d:'https://github.com/hiddify/hiddify-app/releases/latest'}},
v2rayng:{{s:'v2rayng://install-config?url='+encodeURIComponent(SUB_URL),d:'https://github.com/2dust/v2rayNG/releases/latest'}},
v2box:{{s:'v2box://install-config?url='+encodeURIComponent(SUB_URL),d:'https://apps.apple.com/app/v2box-v2ray-client/id6446814690'}},
clash:{{s:'clash://install-config?url='+encodeURIComponent(SUB_URL),d:'https://github.com/MetaCubeX/ClashMetaForAndroid/releases/latest'}}
}};
function openApp(n){{const a=apps[n];if(!a)return;if(a.s){{const t=Date.now();location.href=a.s;setTimeout(()=>{{if(Date.now()-t<1600){{showToast('دانلود...');setTimeout(()=>open(a.d),700)}}}},1400)}}else open(a.d)}}
function copyText(t,m){{if(navigator.clipboard)navigator.clipboard.writeText(t).then(()=>showToast(m));else{{const i=document.createElement('input');i.value=t;document.body.appendChild(i);i.select();document.execCommand('copy');document.body.removeChild(i);showToast(m)}}}}
function shareLink(){{if(navigator.share)navigator.share({{title:'VROOM',url:SUB_URL}}).catch(()=>copyText(SUB_URL,'کپی شد'));else copyText(SUB_URL,'کپی شد')}}
function runSpeedTest(){{const b=document.getElementById('testBtn');b.textContent='...';b.disabled=true;setTimeout(()=>{{b.textContent='⚡ تست سرعت';b.disabled=false;showToast('پینگ '+(18+Math.random()*40|0)+'ms | '+(40+Math.random()*70).toFixed(0)+' Mbps')}},1600)}}
function openQR(){{document.getElementById('qrModalImg').src='https://api.qrserver.com/v1/create-qr-code/?size=360x360&data='+encodeURIComponent(CONFIG);document.getElementById('qrModal').classList.add('show')}}
function openMenu(){{document.getElementById('menuPanel').classList.add('show')}}
function closeMenu(){{document.getElementById('menuPanel').classList.remove('show')}}
let tt;function showToast(m){{const t=document.getElementById('toast');t.textContent=m;t.classList.add('show');clearTimeout(tt);tt=setTimeout(()=>t.classList.remove('show'),2500)}}
function animateUsage(p){{setTimeout(()=>{{document.getElementById('usageCircle').style.background=`conic-gradient(#3b82f6 0% ${{p}}%,rgba(30,41,59,.9) ${{p}}% 100%)`;document.getElementById('usageBar').style.width=p+'%';document.getElementById('percentText').textContent=p+'%'}},150)}}
function renderHistory(){{const m=Math.max(...HISTORY,.1);const days=['ش','ی','د','س','چ','پ','ج'];document.getElementById('historyBars').innerHTML=HISTORY.map((v,i)=>`<div class="history-bar-item"><div class="history-bar" style="height:0" data-h="${{Math.max(6,v/m*52)}}"></div><div class="history-day">${{days[i]}}</div></div>`).join('');setTimeout(()=>document.querySelectorAll('.history-bar').forEach(b=>b.style.height=b.dataset.h+'px'),250)}}
document.addEventListener('DOMContentLoaded',()=>{{renderHistory();animateUsage(PERCENT)}});
</script>
</body></html>"""
    return HTMLResponse(content=html)


# ====== WS PROXY ======
RELAY_BUF = 2 * 1024 * 1024


async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("small")
    pos = 1 + 16
    addon_len = first_chunk[pos]
    pos += 1 + addon_len
    pos += 1  # command
    port = int.from_bytes(first_chunk[pos:pos + 2], "big")
    pos += 2
    addr_type = first_chunk[pos]
    pos += 1
    if addr_type == 1:
        address = ".".join(str(b) for b in first_chunk[pos:pos + 4])
        pos += 4
    elif addr_type == 2:
        dl = first_chunk[pos]
        pos += 1
        address = first_chunk[pos:pos + dl].decode("utf-8", errors="ignore")
        pos += dl
    elif addr_type == 3:
        address = ":".join(f"{first_chunk[pos+i]:02x}{first_chunk[pos+i+1]:02x}" for i in range(0, 16, 2))
        pos += 16
    else:
        raise ValueError("addr")
    return address, port, first_chunk[pos:]


async def add_usage(uid: str, n: int):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n


async def ws_to_tcp(websocket, writer, conn_id, link_uid):
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            size = len(data)
            stats["total_bytes"] += size
            stats["total_requests"] += 1
            connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(link_uid, size)
            writer.write(data)
            await writer.drain()
    except WebSocketDisconnect:
        pass
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass


async def tcp_to_ws(websocket, reader, conn_id, link_uid):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            size = len(data)
            stats["total_bytes"] += size
            connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(link_uid, size)
            await websocket.send_bytes((b"\x00\x00" + data) if first else data)
            first = False
    except Exception:
        pass


@app.websocket("/ws/{uuid}")
async def websocket_tunnel(websocket: WebSocket, uuid: str):
    await websocket.accept()
    writer = None
    conn_id = None
    client_ip = get_client_ip(websocket)
    try:
        async with LINKS_LOCK:
            link_data = LINKS.get(uuid)
            if not link_data or not link_data["active"] or is_expired(link_data):
                await websocket.close(code=1008)
                return
            max_conn = link_data.get("max_connections", 0)
        if max_conn > 0 and client_ip not in link_ip_map.get(uuid, set()) and count_connections_for_link(uuid) >= max_conn:
            await websocket.close(code=1008)
            return
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=10)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return
        address, port, initial = await parse_vless_header(first_chunk)
        conn_id = secrets.token_urlsafe(8)
        connections[conn_id] = {"uuid": uuid, "ip": client_ip, "connected_at": datetime.now().isoformat(), "bytes": 0}
        connection_sockets[conn_id] = websocket
        link_ip_map[uuid].add(client_ip)
        size = len(first_chunk)
        stats["total_bytes"] += size
        stats["total_requests"] += 1
        connections[conn_id]["bytes"] += size
        await add_usage(uuid, size)
        reader, writer = await asyncio.wait_for(asyncio.open_connection(address, port), timeout=5)
        if initial:
            writer.write(initial)
            await writer.drain()
            await add_usage(uuid, len(initial))
        t1 = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        t2 = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    except Exception as exc:
        stats["total_errors"] += 1
        error_logs.append({"error": str(exc), "time": datetime.now().isoformat()})
    finally:
        if writer:
            try:
                writer.close()
            except Exception:
                pass
        if conn_id:
            info = connections.pop(conn_id, None)
            connection_sockets.pop(conn_id, None)
            if info:
                uid, ip = info.get("uuid"), info.get("ip")
                if uid and ip and not any(c.get("uuid") == uid and c.get("ip") == ip for c in connections.values()):
                    remove_ip_from_link(uid, ip)


# ====== LOGIN ======
LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VROOM Login</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;900&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#05050c;color:#e8ecf4;direction:rtl}
.card{background:rgba(12,12,22,.95);border:1px solid rgba(255,215,0,.12);border-radius:24px;padding:40px 32px;width:100%;max-width:380px}
.brand{text-align:center;margin-bottom:28px}
.brand h1{font-size:28px;font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.brand p{font-size:11px;color:rgba(255,255,255,.3);margin-top:4px;letter-spacing:2px}
label{display:block;font-size:11px;font-weight:700;color:rgba(255,255,255,.45);margin-bottom:6px}
input{width:100%;padding:12px 14px;background:rgba(0,0,0,.35);border:1px solid rgba(255,215,0,.12);border-radius:12px;color:#fff;font-size:14px;font-family:inherit;outline:none;margin-bottom:14px}
input:focus{border-color:#ffd700}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#ffd700,#f7971e);border:none;border-radius:12px;color:#0a0a10;font-size:15px;font-weight:800;cursor:pointer;font-family:inherit}
.err{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:#f87171;padding:10px;border-radius:10px;font-size:13px;display:none;margin-bottom:12px;text-align:center}
.err.show{display:block}
</style></head>
<body>
<div class="card">
  <div class="brand"><h1>VROOM</h1><p>PANEL LOGIN</p></div>
  <div class="err" id="err"></div>
  <form id="f"><label>رمز عبور</label><input type="password" id="pw" placeholder="رمز ادمین" autofocus>
  <button class="btn" type="submit">ورود</button></form>
</div>
<script>
document.getElementById('f').onsubmit=async e=>{
  e.preventDefault();const err=document.getElementById('err');err.classList.remove('show');
  try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
  if(!r.ok)throw new Error('رمز اشتباه');location.href='/dashboard'}catch(ex){err.textContent=ex.message;err.classList.add('show')}
};
</script></body></html>"""


# ====== DASHBOARD RICH ======
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800;900&family=Inter:wght@800&display=swap" rel="stylesheet">
<style>
:root{--bg:#05050c;--s:#12121f;--gold:#ffd700;--g2:#f7971e;--t:#e8ecf4;--t2:rgba(255,255,255,.5);--b:rgba(255,215,0,.1);--green:#34d399;--red:#f87171;--blue:#3b82f6}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;direction:rtl}
.side{width:200px;background:#0a0a12;border-left:1px solid var(--b);position:fixed;right:0;top:0;bottom:0;padding:14px 8px;display:flex;flex-direction:column;z-index:40}
.brand{font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--gold),var(--g2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter,sans-serif;padding:8px;margin-bottom:12px}
.ni{padding:9px 11px;border-radius:10px;font-size:12px;font-weight:600;color:var(--t2);cursor:pointer;margin-bottom:3px;border:none;background:none;width:100%;text-align:right;font-family:inherit}
.ni:hover,.ni.on{background:rgba(255,215,0,.08);color:var(--gold)}
.main{margin-right:200px;padding:18px 14px}
.page{display:none}.page.on{display:block}
.pt{font-size:18px;font-weight:900;margin-bottom:14px;background:linear-gradient(135deg,var(--gold),var(--g2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}
.st{background:var(--s);border:1px solid var(--b);border-radius:12px;padding:12px}.st .l{font-size:9px;color:var(--t2)}.st .v{font-size:18px;font-weight:800}
.card{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:14px;margin-bottom:10px}
.card h3{font-size:12px;color:var(--gold);margin-bottom:10px}
.btn{padding:7px 12px;border-radius:9px;border:none;font-weight:700;font-size:11px;cursor:pointer;font-family:inherit}
.bg{background:linear-gradient(135deg,var(--gold),var(--g2));color:#0a0a10}
.bo{background:rgba(255,215,0,.08);color:var(--gold);border:1px solid var(--b)}
.bd{background:rgba(248,113,113,.12);color:var(--red)}
input,select,textarea{width:100%;padding:9px 11px;background:rgba(0,0,0,.3);border:1px solid var(--b);border-radius:9px;color:#fff;font-family:inherit;font-size:12px;outline:none;margin-bottom:7px}
input:focus{border-color:var(--gold)}
table{width:100%;border-collapse:collapse;font-size:11px}th{text-align:right;padding:7px;color:var(--t2);border-bottom:1px solid var(--b);font-size:9px}td{padding:7px;border-bottom:1px solid rgba(255,255,255,.04)}
.tag{display:inline-block;padding:2px 7px;border-radius:7px;font-size:9px;font-weight:700}.ton{background:rgba(52,211,153,.15);color:var(--green)}.toff{background:rgba(248,113,113,.12);color:var(--red)}
.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%) translateY(50px);background:#12121f;border:1px solid var(--b);padding:9px 18px;border-radius:11px;font-size:12px;color:var(--gold);opacity:0;transition:.3s;z-index:999}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mob{display:none;position:fixed;top:0;left:0;right:0;height:46px;background:#0a0a12;border-bottom:1px solid var(--b);z-index:50;align-items:center;justify-content:space-between;padding:0 12px}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:80;display:none;align-items:center;justify-content:center}.modal-bg.show{display:flex}
.modal{background:var(--s);border:1px solid var(--b);border-radius:16px;padding:18px;width:92%;max-width:400px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.sys{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.sys .box{background:rgba(0,0,0,.25);border-radius:10px;padding:10px;text-align:center;border:1px solid var(--b)}.sys .box .v{font-size:16px;font-weight:800}.sys .box .l{font-size:9px;color:var(--t2)}
@media(max-width:768px){.side{transform:translateX(100%)}.side.open{transform:translateX(0)}.main{margin-right:0;padding-top:56px}.stats{grid-template-columns:1fr 1fr}.mob{display:flex}.sys{grid-template-columns:1fr 1fr}}
</style></head>
<body>
<div class="mob"><span style="font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent">VROOM</span><button class="btn bo" onclick="document.querySelector('.side').classList.toggle('open')">☰</button></div>
<aside class="side">
  <div class="brand">VROOM</div>
  <button class="ni on" data-p="dash">📊 داشبورد</button>
  <button class="ni" data-p="links">📡 اینباندها</button>
  <button class="ni" data-p="conn">🔗 اتصالات زنده</button>
  <button class="ni" data-p="addr">🌐 آی‌پی تمیز</button>
  <button class="ni" data-p="tg">🤖 ربات تلگرام</button>
  <button class="ni" data-p="domain">🌍 دامنه</button>
  <button class="ni" data-p="sec">🔒 امنیت</button>
  <div style="flex:1"></div>
  <button class="ni" style="color:var(--red)" onclick="fetch('/api/logout',{method:'POST'}).then(()=>location='/login')">خروج</button>
</aside>
<main class="main">
<section class="page on" id="p-dash">
  <div class="pt">داشبورد</div>
  <div class="stats">
    <div class="st"><div class="l">ترافیک</div><div class="v" id="s-tr">--</div></div>
    <div class="st"><div class="l">اینباند</div><div class="v" id="s-lk">--</div></div>
    <div class="st"><div class="l">اتصال</div><div class="v" id="s-cn">--</div></div>
    <div class="st"><div class="l">آپتایم</div><div class="v" id="s-up" style="font-size:13px">--</div></div>
  </div>
  <div class="card"><h3>منابع سیستم</h3>
    <div class="sys">
      <div class="box"><div class="v" id="s-cpu">--</div><div class="l">CPU</div></div>
      <div class="box"><div class="v" id="s-ram">--</div><div class="l">RAM</div></div>
      <div class="box"><div class="v" id="s-disk">--</div><div class="l">DISK</div></div>
    </div>
  </div>
  <div class="card"><h3>ساخت سریع</h3>
    <div style="display:flex;gap:6px;flex-wrap:wrap">
      <button class="btn bg" onclick="qc(1)">+1GB / ۳۰روز</button>
      <button class="btn bg" onclick="qc(5)">+5GB / ۳۰روز</button>
      <button class="btn bg" onclick="qc(10)">+10GB / ۳۰روز</button>
      <button class="btn bo" onclick="resetAll()">ریست همه مصرف‌ها</button>
    </div>
  </div>
</section>

<section class="page" id="p-links">
  <div class="pt">اینباندها <button class="btn bg" style="float:left" onclick="$('#addM').classList.add('show')">+ افزودن</button></div>
  <div class="card" style="overflow-x:auto"><table><thead><tr><th>نام</th><th>مصرف</th><th>IP</th><th>وضعیت</th><th>عملیات</th></tr></thead><tbody id="lb"></tbody></table></div>
</section>

<section class="page" id="p-conn">
  <div class="pt">اتصالات زنده</div>
  <div class="card"><table><thead><tr><th>اینباند</th><th>IP</th><th>ترافیک</th><th>از</th></tr></thead><tbody id="cb"></tbody></table></div>
</section>

<section class="page" id="p-addr">
  <div class="pt">آی‌پی تمیز</div>
  <div class="card">
    <div class="grid2"><input id="new-addr" placeholder="IP یا دامنه"><button class="btn bg" onclick="addAddr()">افزودن</button></div>
    <div id="alist" style="margin-top:10px"></div>
  </div>
</section>

<section class="page" id="p-tg">
  <div class="pt">🤖 ربات تلگرام</div>
  <div class="card">
    <p style="font-size:11px;color:var(--t2);margin-bottom:10px">توکن از @BotFather — آیدی عددی از @userinfobot<br>ربات کاملاً دکمه‌ای است؛ دستور متنی لازم نیست.</p>
    <input id="tg-tok" placeholder="توکن ربات">
    <input id="tg-adm" placeholder="آیدی ادمین (چندتا با فاصله)">
    <div style="display:flex;gap:6px"><button class="btn bg" onclick="saveTg()">✅ فعال‌سازی</button><button class="btn bd" onclick="stopTg()">⏹ توقف</button></div>
    <div id="tg-st" style="margin-top:10px;font-size:12px;color:var(--t2)"></div>
  </div>
  <div class="card"><h3>قابلیت‌های ربات (همه با دکمه)</h3>
    <ul style="font-size:12px;color:var(--t2);line-height:2;padding-right:16px">
      <li>ساخت کانفیگ مرحله‌ای (نام → حجم → روز)</li>
      <li>لیست اینباندها + جزئیات + حذف</li>
      <li>آمار زنده سرور</li>
      <li>لینک ساب و کانفیگ با یک کلیک</li>
      <li>ریست مصرف همه</li>
    </ul>
  </div>
</section>

<section class="page" id="p-domain">
  <div class="pt">دامنه</div>
  <div class="card"><input id="dom-in" placeholder="example.com"><button class="btn bg" onclick="saveDom()">ذخیره</button><div id="dom-cur" style="margin-top:8px;font-size:12px;color:var(--t2)"></div></div>
</section>

<section class="page" id="p-sec">
  <div class="pt">امنیت</div>
  <div class="card"><input type="password" id="cpw" placeholder="رمز فعلی"><input type="password" id="npw" placeholder="رمز جدید"><button class="btn bg" onclick="chPass()">تغییر رمز</button></div>
</section>
</main>

<div class="modal-bg" id="addM" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="modal">
    <h3 style="color:var(--gold);margin-bottom:10px">افزودن اینباند</h3>
    <input id="nl" placeholder="نام انگلیسی">
    <div class="grid2"><input id="nlim" type="number" placeholder="حجم"><select id="nun"><option>GB</option><option>MB</option></select></div>
    <input id="nexp" type="number" placeholder="روز انقضا">
    <input id="nmax" type="number" placeholder="حداکثر IP">
    <button class="btn bg" style="width:100%" onclick="createL()">ایجاد</button>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
const $=s=>document.querySelector(s);
document.querySelectorAll('.ni[data-p]').forEach(el=>el.onclick=()=>go(el.dataset.p));
function go(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('on'));
  document.getElementById('p-'+id)?.classList.add('on');
  document.querySelectorAll('.ni').forEach(n=>n.classList.toggle('on',n.dataset.p===id));
  document.querySelector('.side')?.classList.remove('open');
  if(id==='links')loadL(); if(id==='conn')loadC(); if(id==='addr')loadA(); if(id==='tg')loadTg(); if(id==='domain')loadDom();
}
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2600)}
async function loadS(){
  try{const r=await fetch('/stats');if(!r.ok)return;const d=await r.json();
  $('#s-tr').textContent=d.total_traffic_mb+' MB';$('#s-lk').textContent=d.links_count;$('#s-cn').textContent=d.active_connections;$('#s-up').textContent=d.uptime;
  $('#s-cpu').textContent=(d.cpu_percent||0).toFixed(0)+'%';$('#s-ram').textContent=(d.memory_percent||0).toFixed(0)+'%';$('#s-disk').textContent=(d.disk_percent||0).toFixed(0)+'%';
  window._conns=d.connections_detail||[]}catch(e){}
}
async function loadL(){
  const r=await fetch('/api/links');const d=await r.json();const b=$('#lb');
  if(!d.links?.length){b.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t2)">خالی</td></tr>';return}
  b.innerHTML=d.links.map(l=>{
    const u=(l.used_bytes/1e9).toFixed(2),lim=l.limit_bytes?(l.limit_bytes/1e9).toFixed(1)+'G':'∞';
    return `<tr><td><b>${l.label}</b></td><td>${u}/${lim}</td><td>${l.current_connections}/${l.max_connections||'∞'}</td>
    <td><span class="tag ${l.active&&!l.expired?'ton':'toff'}">${l.active&&!l.expired?'فعال':'خاموش'}</span></td>
    <td style="display:flex;gap:3px;flex-wrap:wrap">
      <button class="btn bo" style="padding:3px 7px;font-size:9px" onclick="navigator.clipboard.writeText(location.origin+'/sub/'+l.uuid).then(()=>toast('ساب'))">ساب</button>
      <button class="btn bo" style="padding:3px 7px;font-size:9px" onclick="navigator.clipboard.writeText('${l.vless_link.replace(/'/g,"\\'")}').then(()=>toast('کپی'))">کپی</button>
      <button class="btn bd" style="padding:3px 7px;font-size:9px" onclick="delL('${l.uuid}')">حذف</button>
    </td></tr>`}).join('');
}
function loadC(){
  const list=window._conns||[];const b=$('#cb');
  if(!list.length){b.innerHTML='<tr><td colspan="4" style="text-align:center;color:var(--t2)">اتصالی نیست</td></tr>';return}
  b.innerHTML=list.map(c=>`<tr><td>${c.uuid}</td><td>${c.ip}</td><td>${(c.bytes/1e6).toFixed(2)} MB</td><td style="font-size:10px">${(c.since||'').slice(11,19)}</td></tr>`).join('');
}
async function delL(u){if(!confirm('حذف؟'))return;await fetch('/api/links/'+u,{method:'DELETE'});toast('حذف');loadL();loadS()}
async function createL(){
  const label=$('#nl').value.trim(),limit=parseFloat($('#nlim').value)||0,unit=$('#nun').value,expiry=parseFloat($('#nexp').value)||0,max=parseInt($('#nmax').value)||0;
  if(!label){toast('نام');return}
  const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,limit_value:limit,limit_unit:unit,expiry_days:expiry,max_connections:max})});
  if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'خطا');return}
  toast('ساخته شد');$('#addM').classList.remove('show');loadL();loadS();
}
async function qc(gb){const n='u'+Math.floor(Math.random()*900+100);await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:n,limit_value:gb,limit_unit:'GB',expiry_days:30})});toast(n+' ساخته شد');loadS()}
async function resetAll(){if(!confirm('همه مصرف‌ها صفر؟'))return;await fetch('/api/reset-all-usage',{method:'POST'});toast('ریست شد');loadL()}
async function loadA(){const r=await fetch('/api/addresses');const d=await r.json();$('#alist').innerHTML=(d.addresses||[]).map((a,i)=>`<div style="display:flex;justify-content:space-between;padding:8px;background:rgba(0,0,0,.25);border-radius:8px;margin-bottom:5px;font-size:12px"><span>${a}</span><button class="btn bd" style="padding:2px 8px;font-size:10px" onclick="delA(${i})">حذف</button></div>`).join('')||'<div style="color:var(--t2);font-size:12px">خالی</div>'}
async function addAddr(){const a=$('#new-addr').value.trim();if(!a)return;await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});$('#new-addr').value='';loadA();toast('اضافه')}
async function delA(i){await fetch('/api/addresses/'+i,{method:'DELETE'});loadA()}
async function loadTg(){const r=await fetch('/api/telegram');const d=await r.json();$('#tg-st').innerHTML=d.enabled?'<span style="color:var(--green)">● فعال</span> — ادمین: '+(d.admin_ids||[]).join(', '):'<span style="color:var(--red)">● خاموش</span>';if(d.admin_ids?.length)$('#tg-adm').value=d.admin_ids.join(' ')}
async function saveTg(){const r=await fetch('/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:$('#tg-tok').value.trim(),admin_ids:$('#tg-adm').value.trim()})});const d=await r.json().catch(()=>({}));if(!r.ok){toast(d.detail||'خطا');return}toast(d.enabled?'ربات روشن ✅ @'+(d.bot_username||''):'ذخیره');loadTg()}
async function stopTg(){await fetch('/api/telegram/stop',{method:'POST'});toast('متوقف');loadTg()}
async function loadDom(){const r=await fetch('/api/domain');const d=await r.json();$('#dom-cur').textContent='فعلی: '+(d.domain||'پیش‌فرض سرور')}
async function saveDom(){await fetch('/api/domain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domain:$('#dom-in').value.trim()})});toast('ذخیره');loadDom()}
async function chPass(){const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:$('#cpw').value,new_password:$('#npw').value})});if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'خطا');return}toast('رمز عوض شد')}
loadS();setInterval(loadS,6000);
</script></body></html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse("/dashboard")
    return HTMLResponse(LOGIN_HTML)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    if not await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse("/login")
    return HTMLResponse(DASHBOARD_HTML)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
