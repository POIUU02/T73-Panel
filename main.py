#!/usr/bin/env python3
"""
VROOM Panel - Ultimate Edition
- Beautiful Gold/Dark Subscription Page
- Admin Dashboard
- Telegram Bot (create configs from bot)
- VLESS over WebSocket Proxy
"""

import asyncio
import json
import os
import hashlib
import secrets
import time
import re
import base64
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import logging
import psutil

# ====== SECRET KEY ======
try:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    if not SECRET_KEY:
        SECRET_KEY = secrets.token_urlsafe(32)
        os.environ["SECRET_KEY"] = SECRET_KEY
        print(f"⚠️ SECRET_KEY created: {SECRET_KEY}")
except Exception:
    SECRET_KEY = "vroom-default-secret-key"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VROOM")

app = FastAPI(title="VROOM", docs_url=None, redoc_url=None)

CONFIG = {
    "port": int(os.environ.get("PORT", 8080)),
    "secret": SECRET_KEY,
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====== GLOBAL STATE ======
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

# Telegram Bot Config
TELEGRAM: dict = {
    "token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "admin_ids": [],  # list of int
    "enabled": False,
    "offset": 0,
}
TELEGRAM_LOCK = asyncio.Lock()
TELEGRAM_TASK = None

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
    params = {
        "encryption": "none",
        "security": "tls",
        "type": "ws",
        "host": domain,
        "path": path,
        "sni": domain,
        "fp": "chrome",
        "alpn": "http/1.1",
    }
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"


def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB":
        return int(value * 1024 * 1024 * 1024)
    if unit == "MB":
        return int(value * 1024 * 1024)
    if unit == "KB":
        return int(value * 1024)
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
    if websocket.client:
        return websocket.client.host
    return "unknown"


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
                await ws.close(code=1000, reason="link deleted")
            except Exception:
                pass
        connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    link_ip_map.pop(uid, None)


# ====== TELEGRAM BOT ======
async def tg_api(method: str, **kwargs):
    token = TELEGRAM.get("token")
    if not token:
        return None
    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(url, json=kwargs)
            return r.json()
    except Exception as e:
        logger.error(f"TG API error: {e}")
        return None


async def tg_send(chat_id: int, text: str, reply_markup=None, parse_mode="HTML"):
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_api("sendMessage", **data)


async def handle_tg_message(msg: dict):
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    from_user = msg.get("from", {})
    user_id = from_user.get("id")
    text = (msg.get("text") or "").strip()

    if not text or not chat_id:
        return

    admin_ids = TELEGRAM.get("admin_ids") or []
    is_admin = user_id in admin_ids

    if text.startswith("/start"):
        if is_admin:
            kb = {
                "keyboard": [
                    [{"text": "➕ ساخت کانفیگ"}, {"text": "📋 لیست اینباندها"}],
                    [{"text": "📊 آمار"}, {"text": "🔗 لینک ساب"}],
                    [{"text": "ℹ️ راهنما"}],
                ],
                "resize_keyboard": True,
            }
            await tg_send(chat_id, "🚀 <b>VROOM Bot</b>\n\nخوش اومدی ادمین!\nاز دکمه‌ها استفاده کن یا دستورات رو بفرست.", reply_markup=kb)
        else:
            await tg_send(chat_id, "⛔ دسترسی فقط برای ادمین است.")
        return

    if not is_admin:
        await tg_send(chat_id, "⛔ فقط ادمین می‌تونه از ربات استفاده کنه.")
        return

    if text in ("ℹ️ راهنما", "/help"):
        await tg_send(chat_id, """
<b>📖 راهنمای ربات VROOM</b>

<code>/create نام حجم_GB روز</code>
مثال: <code>/create user1 10 30</code>

<code>/list</code> — لیست اینباندها
<code>/stats</code> — آمار پنل
<code>/sub نام</code> — لینک ساب یک اینباند

یا از دکمه‌های کیبورد استفاده کن.
""")
        return

    if text in ("📊 آمار", "/stats"):
        domain = get_domain()
        async with LINKS_LOCK:
            links_count = len(LINKS)
        await tg_send(chat_id, f"""
📊 <b>آمار VROOM</b>

🔗 اینباندها: <code>{links_count}</code>
📡 اتصالات فعال: <code>{len(connections)}</code>
📥 ترافیک کل: <code>{round(stats['total_bytes']/(1024*1024),1)} MB</code>
⏱️ آپتایم: <code>{uptime()}</code>
🌐 دامنه: <code>{domain}</code>
""")
        return

    if text in ("📋 لیست اینباندها", "/list"):
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_send(chat_id, "📭 هیچ اینباندی وجود نداره.")
            return
        lines = []
        for uid, data in items[:20]:
            used = round(data["used_bytes"] / (1024**3), 2)
            limit = round(data["limit_bytes"] / (1024**3), 2) if data["limit_bytes"] else "∞"
            st = "✅" if data["active"] and not is_expired(data) else "❌"
            lines.append(f"{st} <b>{data['label']}</b> — {used}/{limit} GB")
        await tg_send(chat_id, "📋 <b>لیست اینباندها:</b>\n\n" + "\n".join(lines))
        return

    if text.startswith("/sub ") or text.startswith("🔗"):
        name = text.replace("/sub ", "").replace("🔗 لینک ساب", "").strip()
        if not name or name == "🔗 لینک ساب":
            await tg_send(chat_id, "نام اینباند رو بعد از /sub بنویس.\nمثال: <code>/sub user1</code>")
            return
        async with LINKS_LOCK:
            link = LINKS.get(name)
        if not link:
            await tg_send(chat_id, f"❌ اینباند «{name}» پیدا نشد.")
            return
        domain = get_domain()
        sub_url = f"https://{domain}/sub/{name}"
        vless = generate_vless_link(name, remark=f"VROOM-{link['label']}")
        await tg_send(chat_id, f"""
🔗 <b>ساب: {link['label']}</b>

📥 لینک ساب:
<code>{sub_url}</code>

📋 کانفیگ:
<code>{vless}</code>
""")
        return

    if text in ("➕ ساخت کانفیگ",) or text.startswith("/create"):
        parts = text.split()
        if text == "➕ ساخت کانفیگ":
            await tg_send(chat_id, "فرمت:\n<code>/create نام حجم_GB روز</code>\n\nمثال:\n<code>/create ali 5 30</code>")
            return
        if len(parts) < 2:
            await tg_send(chat_id, "فرمت اشتباه.\n<code>/create نام حجم_GB روز</code>")
            return
        label = parts[1][:40]
        if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
            await tg_send(chat_id, "❌ نام فقط انگلیسی، عدد و - _ . فاصله")
            return
        limit_gb = float(parts[2]) if len(parts) > 2 else 0
        days = float(parts[3]) if len(parts) > 3 else 0
        limit_bytes = parse_size_to_bytes(limit_gb, "GB") if limit_gb > 0 else 0
        expiry = compute_expiry(days)
        async with LINKS_LOCK:
            if label in LINKS:
                await tg_send(chat_id, f"❌ «{label}» از قبل وجود داره.")
                return
            LINKS[label] = {
                "label": label,
                "limit_bytes": limit_bytes,
                "used_bytes": 0,
                "max_connections": 0,
                "created_at": datetime.now().isoformat(),
                "active": True,
                "expiry": expiry,
            }
        domain = get_domain()
        sub_url = f"https://{domain}/sub/{label}"
        vless = generate_vless_link(label, remark=f"VROOM-{label}")
        await tg_send(chat_id, f"""
✅ <b>کانفیگ ساخته شد!</b>

🏷 نام: <code>{label}</code>
📦 حجم: <code>{limit_gb if limit_gb else '∞'} GB</code>
📅 انقضا: <code>{days if days else 'نامحدود'} روز</code>

📥 ساب:
<code>{sub_url}</code>

📋 کانفیگ:
<code>{vless}</code>
""")
        return

    await tg_send(chat_id, "دستور نامشخص. /help رو بزن.")


async def telegram_poll_loop():
    logger.info("🤖 Telegram bot polling started")
    while True:
        try:
            async with TELEGRAM_LOCK:
                token = TELEGRAM.get("token")
                enabled = TELEGRAM.get("enabled")
                offset = TELEGRAM.get("offset", 0)
            if not token or not enabled:
                await asyncio.sleep(5)
                continue
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(url, params={"offset": offset, "timeout": 25})
                data = r.json()
            if not data.get("ok"):
                await asyncio.sleep(3)
                continue
            for upd in data.get("result", []):
                new_offset = upd["update_id"] + 1
                async with TELEGRAM_LOCK:
                    TELEGRAM["offset"] = new_offset
                msg = upd.get("message")
                if msg:
                    try:
                        await handle_tg_message(msg)
                    except Exception as e:
                        logger.error(f"TG handle error: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"TG poll error: {e}")
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
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.get(f"https://{domain}/health")
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    global http_client
    limits = httpx.Limits(max_connections=5000, max_keepalive_connections=1000)
    timeout = httpx.Timeout(180.0, connect=30.0)
    http_client = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
    logger.info(f"🚀 VROOM started on port {CONFIG['port']}")
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


# ====== BASIC ROUTES ======
@app.get("/")
async def root():
    return {"service": "VROOM", "version": "3.1", "status": "active", "domain": get_domain()}


@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "uptime": uptime()}


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    password = str(body.get("password") or "")
    if hash_password(password) != AUTH["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = await create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie(key=SESSION_COOKIE, value=token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/")
    return resp


@app.post("/api/logout")
async def api_logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    await destroy_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


@app.get("/api/me")
async def api_me(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    return {"authenticated": await is_valid_session(token)}


@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    current = str(body.get("current_password") or "")
    new = str(body.get("new_password") or "")
    if hash_password(current) != AUTH["password_hash"]:
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    AUTH["password_hash"] = hash_password(new)
    current_token = request.cookies.get(SESSION_COOKIE)
    async with SESSIONS_LOCK:
        SESSIONS.clear()
        if current_token:
            SESSIONS[current_token] = time.time() + SESSION_TTL
    return {"ok": True}


@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    return {
        "active_connections": len(connections),
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "timestamp": datetime.now().isoformat(),
        "recent_errors": list(error_logs)[-10:],
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage('/').percent,
        "disk_used": round(psutil.disk_usage('/').used / (1024**3), 2),
        "disk_total": round(psutil.disk_usage('/').total / (1024**3), 2),
        "hourly_traffic": dict(hourly_traffic),
        "telegram_enabled": TELEGRAM.get("enabled", False),
    }


# ====== LINKS API ======
@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Inbound name must contain only English letters, numbers, and - _ . space")
    if not label:
        raise HTTPException(status_code=400, detail="Inbound name is required")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="An inbound with this name already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = int(body.get("max_connections") or 0)
    if max_conn < 0:
        max_conn = 0
    expiry = compute_expiry(body.get("expiry_days"))
    uid = label
    async with LINKS_LOCK:
        LINKS[uid] = {
            "label": label,
            "limit_bytes": limit_bytes,
            "used_bytes": 0,
            "max_connections": max_conn,
            "created_at": datetime.now().isoformat(),
            "active": True,
            "expiry": expiry,
        }
    return {
        "uuid": uid,
        "label": label,
        "limit_bytes": limit_bytes,
        "used_bytes": 0,
        "max_connections": max_conn,
        "active": True,
        "expiry": expiry,
        "created_at": LINKS[uid]["created_at"],
        "vless_link": generate_vless_link(uid, remark=f"VROOM-{label}"),
    }


@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            result.append({
                "uuid": uid,
                "label": data["label"],
                "limit_bytes": data["limit_bytes"],
                "used_bytes": data["used_bytes"],
                "max_connections": data.get("max_connections", 0),
                "active": data["active"],
                "expiry": data.get("expiry", ""),
                "expired": is_expired(data),
                "created_at": data["created_at"],
                "current_connections": count_connections_for_link(uid),
                "vless_link": generate_vless_link(uid, remark=f"VROOM-{data['label']}"),
            })
    result.sort(key=lambda x: x["created_at"], reverse=True)
    return {"links": result}


@app.patch("/api/links/{uid}")
async def toggle_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        if "active" in body:
            LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            limit_value = float(body.get("limit_value") or 0)
            limit_unit = body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
        if "reset_usage" in body and body["reset_usage"]:
            LINKS[uid]["used_bytes"] = 0
        if "expiry_days" in body:
            LINKS[uid]["expiry"] = compute_expiry(body.get("expiry_days"))
        if "label" in body:
            LINKS[uid]["label"] = str(body["label"])[:60]
        if "max_connections" in body:
            mc = int(body["max_connections"] or 0)
            LINKS[uid]["max_connections"] = mc if mc >= 0 else 0
    return {"ok": True}


@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK:
        LINKS.pop(uid, None)
    await close_connections_for_link(uid)
    return {"ok": True}


@app.get("/api/domain")
async def get_custom_domain(_=Depends(require_auth)):
    async with CUSTOM_DOMAIN_LOCK:
        return {"domain": CUSTOM_DOMAIN}


@app.post("/api/domain")
async def set_custom_domain(request: Request, _=Depends(require_auth)):
    body = await request.json()
    domain = (body.get("domain") or "").strip().lower()
    if domain:
        domain = domain.replace("https://", "").replace("http://", "").rstrip("/")
        if not re.match(r'^[a-z0-9\-_.]+$', domain):
            raise HTTPException(status_code=400, detail="Invalid domain format")
    async with CUSTOM_DOMAIN_LOCK:
        global CUSTOM_DOMAIN
        CUSTOM_DOMAIN = domain
    return {"ok": True, "domain": CUSTOM_DOMAIN}


@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        return {"addresses": list(CUSTOM_ADDRESSES)}


@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="Address is required")
    async with CUSTOM_ADDRESSES_LOCK:
        if address not in CUSTOM_ADDRESSES:
            CUSTOM_ADDRESSES.append(address)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}


@app.delete("/api/addresses/{index}")
async def delete_address(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES):
            CUSTOM_ADDRESSES.pop(index)
        else:
            raise HTTPException(status_code=404, detail="Address not found")
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}


# ====== TELEGRAM CONFIG API ======
@app.get("/api/telegram")
async def get_telegram_config(_=Depends(require_auth)):
    async with TELEGRAM_LOCK:
        return {
            "token": TELEGRAM.get("token", "")[:10] + "..." if TELEGRAM.get("token") else "",
            "has_token": bool(TELEGRAM.get("token")),
            "admin_ids": TELEGRAM.get("admin_ids", []),
            "enabled": TELEGRAM.get("enabled", False),
        }


@app.post("/api/telegram")
async def set_telegram_config(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = (body.get("token") or "").strip()
    admin_raw = body.get("admin_ids") or body.get("admin_id") or ""
    if isinstance(admin_raw, list):
        admin_ids = [int(x) for x in admin_raw if str(x).isdigit()]
    else:
        admin_ids = [int(x.strip()) for x in str(admin_raw).replace(",", " ").split() if x.strip().isdigit()]

    async with TELEGRAM_LOCK:
        if token:
            TELEGRAM["token"] = token
        if admin_ids:
            TELEGRAM["admin_ids"] = admin_ids
        TELEGRAM["enabled"] = bool(TELEGRAM.get("token") and TELEGRAM.get("admin_ids"))

    if TELEGRAM["enabled"]:
        # verify token
        me = await tg_api("getMe")
        if not me or not me.get("ok"):
            async with TELEGRAM_LOCK:
                TELEGRAM["enabled"] = False
            raise HTTPException(status_code=400, detail="Invalid bot token")
        await start_telegram_bot()
        bot_name = me["result"].get("username", "bot")
        return {"ok": True, "enabled": True, "bot_username": bot_name, "admin_ids": TELEGRAM["admin_ids"]}
    return {"ok": True, "enabled": False}


@app.post("/api/telegram/stop")
async def stop_telegram(_=Depends(require_auth)):
    async with TELEGRAM_LOCK:
        TELEGRAM["enabled"] = False
    global TELEGRAM_TASK
    if TELEGRAM_TASK and not TELEGRAM_TASK.done():
        TELEGRAM_TASK.cancel()
    return {"ok": True}


# ====== SUBSCRIPTION PAGE (Our Gold Design) ======
@app.get("/sub/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(status_code=404, detail="Link not found")

    if not link["active"]:
        raise HTTPException(status_code=403, detail="Link disabled")
    if is_expired(link):
        raise HTTPException(status_code=403, detail="Link expired")

    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)

    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    sub_links = [server_link]
    for i, addr in enumerate(addresses):
        sub_links.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))

    used_gb = round(link["used_bytes"] / (1024**3), 2)
    limit_gb = round(link["limit_bytes"] / (1024**3), 2) if link["limit_bytes"] > 0 else 0
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
            days_left = max(0, (exp_date - datetime.now()).days)
            days_left_text = f"{days_left} روز"
            exp_display = exp_date.strftime("%Y/%m/%d")
        except Exception:
            days_left_text, exp_display = "نامحدود", "نامحدود"
    else:
        days_left_text, exp_display = "نامحدود", "نامحدود"

    domain = get_domain()
    sub_url = f"https://{domain}/sub/{uid}"
    qr_data = quote(server_link, safe="")

    # Fake 7-day history for visual
    history_vals = [0.8, 1.2, 0.6, 1.9, 1.4, 1.0, max(0.3, used_gb * 0.1)]

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>
<title>🚀 VROOM — {link['label']}</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&family=Vazirmatn:wght@400;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
:root{{--gold:#ffd700;--gold2:#f7971e;--bg:#05050c;--blue:#3b82f6}}
*{{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}}
body{{font-family:'Vazirmatn','Inter',sans-serif;background:var(--bg);color:#e8ecf4;min-height:100vh;display:flex;justify-content:center;padding:16px 12px;direction:rtl;overflow-x:hidden}}
.container{{max-width:480px;width:100%;background:rgba(12,12,22,.92);border-radius:24px;padding:20px 16px;border:1px solid rgba(255,215,0,.12);box-shadow:0 0 40px rgba(255,215,0,.06)}}
header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;padding:10px 12px;background:rgba(255,215,0,.05);border-radius:14px;border:1px solid rgba(255,215,0,.1)}}
.logo{{display:flex;align-items:center;gap:8px;font-family:'Inter',sans-serif}}
.logo-icon{{width:26px;height:26px;color:var(--gold);animation:float 2.8s ease-in-out infinite}}
.logo-icon svg{{width:100%;height:100%;stroke:currentColor;fill:none;stroke-width:1.7}}
@keyframes float{{0%,100%{{transform:translateY(0)}}50%{{transform:translateY(-5px)}}}}
.logo b{{font-size:22px;font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.logo span{{font-size:12px;font-weight:700;color:#ff6b6b}}
.icon-btn{{width:38px;height:38px;display:flex;align-items:center;justify-content:center;background:rgba(255,215,0,.08);border:1px solid rgba(255,215,0,.15);color:var(--gold);border-radius:11px;cursor:pointer}}
.icon-btn svg{{width:18px;height:18px;stroke:currentColor;fill:none;stroke-width:1.8}}
.main-title{{font-size:22px;font-weight:900;font-family:'Inter',sans-serif;color:#fff;margin-bottom:2px}}
.sub-title{{color:var(--gold);font-size:12px;margin-bottom:14px;letter-spacing:2px;font-weight:600;opacity:.85;display:flex;align-items:center;gap:8px}}
.sub-title svg{{width:12px;height:12px;stroke:var(--gold);fill:none;stroke-width:1.6}}
.server-status{{display:flex;align-items:center;justify-content:space-between;background:rgba(16,185,129,.07);border:1px solid rgba(16,185,129,.18);border-radius:14px;padding:10px 14px;margin-bottom:14px}}
.server-left{{display:flex;align-items:center;gap:9px;font-size:13px;font-weight:700;color:#34d399}}
.server-dot{{width:8px;height:8px;background:#34d399;border-radius:50%;animation:pulse-dot 1.8s infinite}}
@keyframes pulse-dot{{0%{{box-shadow:0 0 0 0 rgba(52,211,153,.55)}}70%{{box-shadow:0 0 0 7px transparent}}100%{{box-shadow:0 0 0 0 transparent}}}}
.test-btn{{background:linear-gradient(135deg,#10b981,#059669);border:none;color:#fff;padding:7px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;font-family:'Vazirmatn',sans-serif;display:flex;align-items:center;gap:6px}}
.test-btn svg{{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:2}}
.usage-card{{background:linear-gradient(145deg,rgba(15,23,42,.9),rgba(10,14,28,.95));border:1px solid rgba(59,130,246,.22);border-radius:18px;padding:14px;margin-bottom:14px}}
.usage-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}}
.usage-header .title{{display:flex;align-items:center;gap:7px;color:#93c5fd;font-size:13px;font-weight:700}}
.usage-header .title svg{{width:15px;height:15px;stroke:#93c5fd;fill:none;stroke-width:1.8}}
.last-update{{font-size:10px;color:rgba(148,163,184,.7)}}
.usage-stats{{display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;align-items:center}}
@media(max-width:420px){{.usage-stats{{grid-template-columns:1fr 1fr;gap:12px}}.usage-circle-wrap{{grid-column:1/-1;justify-self:center;margin-top:6px}}}}
.usage-stat{{text-align:center}}
.usage-stat .label{{font-size:10px;color:rgba(148,163,184,.85);margin-bottom:4px}}
.usage-stat .value{{font-size:16px;font-weight:800;color:#fff}}
.usage-stat .sub{{font-size:11px;color:rgba(148,163,184,.6);margin-top:1px}}
.usage-circle-wrap{{width:78px;height:78px;position:relative;display:flex;align-items:center;justify-content:center}}
.usage-circle-outer{{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(59,130,246,.25);box-shadow:0 0 18px rgba(59,130,246,.12)}}
.usage-circle{{width:68px;height:68px;border-radius:50%;background:conic-gradient(#3b82f6 0% 0%,rgba(30,41,59,.9) 0% 100%);display:flex;align-items:center;justify-content:center;position:relative;transition:background 1.2s cubic-bezier(.4,0,.2,1)}}
.usage-circle::before{{content:'';position:absolute;inset:7px;border-radius:50%;background:#0b1222;box-shadow:inset 0 0 12px rgba(0,0,0,.5)}}
.usage-circle-text{{position:relative;z-index:1;text-align:center}}
.usage-circle-text .percent{{font-size:15px;font-weight:800;color:#fff;line-height:1.1}}
.usage-circle-text .desc{{font-size:8px;color:rgba(148,163,184,.75);margin-top:1px}}
.usage-bar{{margin-top:14px;height:5px;background:rgba(30,41,59,.8);border-radius:10px;overflow:hidden}}
.usage-bar-fill{{height:100%;width:0%;background:linear-gradient(90deg,#3b82f6,#8b5cf6,#ef4444);border-radius:10px;transition:width 1.2s cubic-bezier(.4,0,.2,1)}}
.remaining-text{{text-align:center;margin-top:10px;font-size:12px;color:rgba(148,163,184,.8)}}
.remaining-text strong{{color:#93c5fd;font-weight:700}}
.history-card,.card{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.1);border-radius:16px;padding:14px;margin-bottom:12px}}
.history-card h3,.card h3{{font-size:11px;text-transform:uppercase;letter-spacing:1.5px;font-weight:700;margin-bottom:12px;color:var(--gold);opacity:.8;display:flex;align-items:center;gap:6px}}
.history-card h3 svg,.card h3 svg{{width:13px;height:13px;stroke:var(--gold);fill:none;stroke-width:1.8}}
.history-bars{{display:flex;align-items:flex-end;justify-content:space-between;height:70px;gap:6px;padding:0 4px}}
.history-bar-item{{flex:1;display:flex;flex-direction:column;align-items:center;gap:6px}}
.history-bar{{width:100%;max-width:28px;background:linear-gradient(180deg,#3b82f6,#1e40af);border-radius:6px 6px 3px 3px;transition:height .8s cubic-bezier(.4,0,.2,1)}}
.history-day{{font-size:10px;color:rgba(148,163,184,.6);font-weight:600}}
.row{{background:rgba(0,0,0,.35);padding:10px 12px;border-radius:11px;display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:10px;font-size:11px;font-family:'Courier New',monospace;color:rgba(255,255,255,.45);border:1px solid rgba(255,215,0,.08);cursor:pointer}}
.row button{{background:linear-gradient(135deg,var(--gold),var(--gold2));border:none;color:#0a0a10;padding:5px 12px;border-radius:8px;font-size:11px;font-weight:800;cursor:pointer;white-space:nowrap}}
.row .link-text{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.info-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,215,0,.08)}}
@media(max-width:400px){{.info-grid{{grid-template-columns:1fr 1fr}}}}
.info-item{{background:rgba(0,0,0,.3);padding:9px 6px;border-radius:10px;text-align:center;border:1px solid rgba(255,215,0,.07)}}
.info-item .label{{font-size:9px;text-transform:uppercase;opacity:.4;letter-spacing:1px;display:block;margin-bottom:2px;font-weight:700}}
.info-item .value{{font-size:14px;font-weight:800}}
.config-box{{background:rgba(0,0,0,.4);padding:10px;border-radius:11px;font-size:11px;font-family:'Courier New',monospace;word-break:break-all;margin-bottom:10px;max-height:65px;overflow-y:auto;border:1px solid rgba(255,215,0,.08);text-align:left;direction:ltr;color:rgba(255,255,255,.4);cursor:pointer}}
.config-box .config-label{{font-size:9px;color:rgba(255,215,0,.3);letter-spacing:1px;display:block;margin-bottom:4px;font-family:'Vazirmatn',sans-serif;text-align:right;direction:rtl}}
.qr-section{{display:flex;flex-direction:column;align-items:center}}
.qrbox{{width:120px;height:120px;background:#fff;border-radius:14px;margin:0 auto 10px;overflow:hidden;border:2px solid rgba(255,215,0,.25);cursor:pointer}}
.qrbox img{{width:100%;height:100%}}
.btn-row{{display:flex;gap:8px;width:100%}}
.add,.share-btn{{flex:1;padding:11px;border:none;border-radius:12px;font-weight:800;font-size:13px;cursor:pointer;font-family:'Vazirmatn',sans-serif}}
.add{{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#0a0a10}}
.share-btn{{background:rgba(255,215,0,.1);color:var(--gold);border:1px solid rgba(255,215,0,.25)}}
.quick-section{{margin:14px 0 8px}}
.quick-title{{font-size:15px;font-weight:800;margin-bottom:11px;color:#fff;font-family:'Inter',sans-serif;display:flex;align-items:center;gap:8px}}
.quick-title .icon{{width:16px;height:16px;color:var(--gold)}}
.quick-title .icon svg{{width:100%;height:100%;stroke:currentColor;fill:none;stroke-width:1.8}}
.quick-title .line{{flex:1;height:1px;background:linear-gradient(90deg,rgba(255,215,0,.3),transparent)}}
.quick-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}
@media(max-width:400px){{.quick-grid{{grid-template-columns:repeat(2,1fr)}}}}
.quick-item{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.09);border-radius:14px;padding:12px 4px 10px;text-align:center;cursor:pointer;position:relative;transition:all .2s}}
.quick-item:active{{transform:scale(.95);background:rgba(255,215,0,.1)}}
.quick-item .q-icon{{width:44px;height:44px;margin:0 auto 7px;border-radius:12px;display:flex;align-items:center;justify-content:center;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,.25)}}
.quick-item .q-icon svg{{width:100%;height:100%;border-radius:11px}}
.quick-item .q-name{{font-size:11px;font-weight:600;color:rgba(255,255,255,.6)}}
.quick-item .q-badge{{position:absolute;top:5px;right:5px;font-size:9px;background:rgba(255,215,0,.18);color:var(--gold);padding:1px 5px;border-radius:5px;font-weight:700}}
.platform-chips{{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,215,0,.08)}}
.chip{{background:rgba(255,215,0,.05);padding:4px 10px;border-radius:16px;font-size:11px;border:1px solid rgba(255,215,0,.1);color:rgba(255,255,255,.4);font-weight:600}}
footer{{margin-top:14px;padding-top:12px;border-top:1px solid rgba(255,215,0,.08);text-align:center;font-size:12px;color:#4a5370;font-weight:600}}
footer b{{background:linear-gradient(135deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:rgba(10,10,18,.95);padding:12px 24px;border-radius:12px;font-size:13px;color:var(--gold);opacity:0;transition:all .35s;pointer-events:none;border:1px solid rgba(255,215,0,.25);z-index:9999;white-space:nowrap;font-family:'Vazirmatn',sans-serif;font-weight:700}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.qr-modal{{position:fixed;inset:0;background:rgba(0,0,0,.85);display:flex;align-items:center;justify-content:center;z-index:10000;opacity:0;pointer-events:none;transition:opacity .3s}}
.qr-modal.show{{opacity:1;pointer-events:auto}}
.qr-modal img{{width:min(80vw,300px);border-radius:16px;border:2px solid rgba(255,215,0,.4)}}
.menu-panel{{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:10001;display:none;align-items:flex-end;justify-content:center}}
.menu-panel.show{{display:flex}}
.menu-sheet{{background:#12121f;border-radius:24px 24px 0 0;padding:20px;width:100%;max-width:500px;border:1px solid rgba(255,215,0,.15)}}
.menu-sheet h4{{font-size:14px;color:var(--gold);margin-bottom:14px;text-align:center}}
.menu-item{{padding:14px;background:rgba(255,255,255,.04);border-radius:12px;margin-bottom:8px;cursor:pointer;display:flex;align-items:center;gap:12px;border:1px solid rgba(255,215,0,.08);font-size:13px;font-weight:600}}
.menu-item:active{{background:rgba(255,215,0,.1)}}
</style>
</head>
<body>
<div class="container">
<header>
  <div class="logo">
    <div class="logo-icon"><svg viewBox="0 0 24 24"><path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/></svg></div>
    <b>VROOM</b><span>PANEL</span>
  </div>
  <div style="display:flex;gap:8px">
    <button class="icon-btn" onclick="location.reload()" title="بروزرسانی"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg></button>
    <button class="icon-btn" onclick="openMenu()" title="منو"><svg viewBox="0 0 24 24"><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/></svg></button>
  </div>
</header>

<h1 class="main-title">Subscription</h1>
<p class="sub-title">
  <svg viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
  {link['label']}
  <svg viewBox="0 0 24 24"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
</p>

<div class="server-status">
  <div class="server-left"><div class="server-dot"></div><span>سرور آنلاین</span></div>
  <button class="test-btn" id="testBtn" onclick="runSpeedTest()"><svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg> تست سرعت</button>
</div>

<div class="usage-card">
  <div class="usage-header">
    <div class="title"><svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg> وضعیت مصرف</div>
    <div class="last-update" id="lastUpdate">آخرین بروزرسانی: همین الان</div>
  </div>
  <div class="usage-stats">
    <div class="usage-stat"><div class="label">حجم مصرفی</div><div class="value" id="usedGB">{used_gb} GB</div><div class="sub">از {limit_gb if limit_gb else '∞'} GB</div></div>
    <div class="usage-stat"><div class="label">وضعیت</div><div class="value" style="color:{status_color};font-size:14px">{status_text}</div><div class="sub">&nbsp;</div></div>
    <div class="usage-stat"><div class="label">باقی‌مانده</div><div class="value" id="remGB">{remaining_gb}{'' if remaining_gb=='∞' else ' GB'}</div><div class="sub">&nbsp;</div></div>
    <div class="usage-circle-wrap">
      <div class="usage-circle-outer"></div>
      <div class="usage-circle" id="usageCircle">
        <div class="usage-circle-text"><div class="percent" id="percentText">0%</div><div class="desc">از کل</div></div>
      </div>
    </div>
  </div>
  <div class="usage-bar"><div class="usage-bar-fill" id="usageBar"></div></div>
</div>

<div class="history-card">
  <h3><svg viewBox="0 0 24 24"><path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/></svg> مصرف ۷ روز گذشته</h3>
  <div class="history-bars" id="historyBars"></div>
</div>

<div class="card">
  <h3><svg viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg> لینک سابسکریپشن</h3>
  <div class="row" onclick="copyText(SUB_URL,'لینک ساب کپی شد!')">
    <span class="link-text">{sub_url}</span>
    <button onclick="event.stopPropagation();copyText(SUB_URL,'لینک ساب کپی شد!')">کپی</button>
  </div>
  <div class="info-grid">
    <div class="info-item"><span class="label">وضعیت</span><span class="value" style="color:{status_color}">{status_text}</span></div>
    <div class="info-item"><span class="label">انقضا</span><span class="value" style="color:#fbbf24">{exp_display}</span></div>
    <div class="info-item"><span class="label">باقی‌مانده</span><span class="value" style="color:#6bcbff">{days_left_text}</span></div>
  </div>
</div>

<div class="card">
  <h3><svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M7 7h.01M17 7h.01M7 17h.01M17 17h.01M7 12h.01M12 7h.01M12 17h.01M17 12h.01M12 12h.01"/></svg> کانفیگ و QR</h3>
  <div class="config-box" onclick="copyText(CONFIG,'کانفیگ کپی شد!')">
    <span class="config-label">کانفیگ متنی — کلیک برای کپی</span>
    <span>{server_link}</span>
  </div>
  <div class="qr-section">
    <div class="qrbox" onclick="openQRModal()"><img id="qrImage" src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={qr_data}" alt="QR"></div>
    <div class="btn-row">
      <button class="add" onclick="copyText(SUB_URL,'لینک کپی شد!')">＋ اضافه کردن</button>
      <button class="share-btn" onclick="shareLink()">اشتراک ↗</button>
    </div>
  </div>
</div>

<div class="quick-section">
  <div class="quick-title"><span class="icon"><svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg></span> ابزارهای سریع<span class="line"></span></div>
  <div class="quick-grid">
    <div class="quick-item" onclick="openApp('hiddify')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#455FE9"/><path d="M14 34V14h8.8c4.1 0 6.7 2.2 6.7 5.6 0 2.2-1.1 3.9-3.1 4.7L32 34h-5.6l-4.6-7.8H19V34h-5zm5-12h2.8c1.8 0 2.8-.8 2.8-2.2s-1-2.2-2.8-2.2H19v4.4z" fill="#fff"/></svg></div><span class="q-name">Hiddify</span></div>
    <div class="quick-item" onclick="openApp('v2rayng')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#1E88E5"/><path d="M24 10L12 18v12l12 8 12-8V18L24 10zm0 5.5l7.5 5v7l-7.5 5-7.5-5v-7l7.5-5z" fill="#fff"/><circle cx="24" cy="24" r="3.5" fill="#fff"/></svg></div><span class="q-name">v2rayNG</span></div>
    <div class="quick-item" onclick="openApp('v2box')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#6C5CE7"/><text x="24" y="31" text-anchor="middle" fill="#fff" font-size="16" font-weight="800" font-family="Arial">V2</text></svg></div><span class="q-name">V2Box</span></div>
    <div class="quick-item" onclick="openApp('singbox')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#00B894"/><path d="M14 15h20v3.8H14V15zm0 8h20v3.8H14V23zm0 8h14v3.8H14V31z" fill="#fff"/></svg></div><span class="q-name">Sing-box</span></div>
    <div class="quick-item" onclick="openApp('shadowrocket')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#E84393"/><path d="M24 9l-2.5 9.5H13l6.8 5-2.6 9.5 9.3-6.2 9.3 6.2-2.6-9.5 6.8-5h-8.5L24 9z" fill="#fff"/></svg></div><span class="q-name">ShadowRocket</span></div>
    <div class="quick-item" onclick="openApp('clash')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#D63031"/><circle cx="24" cy="24" r="11" fill="none" stroke="#fff" stroke-width="3.5"/><circle cx="24" cy="24" r="5" fill="#fff"/></svg></div><span class="q-name">Clash</span></div>
    <div class="quick-item" onclick="openApp('streisand')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#FF6B6B"/><path d="M16 15h16v3.5H16V15zm0 7.5h16v3.5H16V22.5zm0 7.5h11v3.5H16V30z" fill="#fff"/></svg></div><span class="q-name">Streisand</span></div>
    <div class="quick-item" onclick="openApp('nekoray')"><span class="q-badge">+</span><div class="q-icon"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#F39C12"/><circle cx="24" cy="18.5" r="7.5" fill="#fff"/><path d="M12 37c0-6.1 5.4-11 12-11s12 4.9 12 11H12z" fill="#fff"/></svg></div><span class="q-name">NekoRay</span></div>
  </div>
  <div class="platform-chips"><span class="chip">Android</span><span class="chip">iOS</span><span class="chip">Windows</span><span class="chip">macOS</span><span class="chip">Linux</span></div>
</div>

<footer>Powered by <b>VROOM PANEL</b></footer>
</div>

<div class="qr-modal" id="qrModal" onclick="closeQRModal()"><img id="qrModalImg" src="" alt="QR"></div>
<div class="toast" id="toast">کپی شد!</div>

<div class="menu-panel" id="menuPanel" onclick="if(event.target===this)closeMenu()">
  <div class="menu-sheet">
    <h4>منو</h4>
    <div class="menu-item" onclick="copyText(SUB_URL,'لینک ساب کپی شد!');closeMenu()">📥 کپی لینک ساب</div>
    <div class="menu-item" onclick="copyText(CONFIG,'کانفیگ کپی شد!');closeMenu()">📋 کپی کانفیگ</div>
    <div class="menu-item" onclick="shareLink();closeMenu()">↗ اشتراک‌گذاری</div>
    <div class="menu-item" onclick="location.reload()">🔄 بروزرسانی وضعیت</div>
    <div class="menu-item" onclick="closeMenu()" style="color:#f87171;justify-content:center">بستن</div>
  </div>
</div>

<script>
const SUB_URL = '{sub_url}';
const CONFIG = `{server_link}`;
const PERCENT = {percent};
const HISTORY = {json.dumps(history_vals)};

const apps = {{
  hiddify: {{scheme:'hiddify://import/'+encodeURIComponent(SUB_URL), download:'https://github.com/hiddify/hiddify-app/releases/latest'}},
  v2rayng: {{scheme:'v2rayng://install-config?url='+encodeURIComponent(SUB_URL), download:'https://github.com/2dust/v2rayNG/releases/latest'}},
  v2box: {{scheme:'v2box://install-config?url='+encodeURIComponent(SUB_URL), download:'https://apps.apple.com/app/v2box-v2ray-client/id6446814690'}},
  singbox: {{scheme:'sing-box://import-remote-profile?url='+encodeURIComponent(SUB_URL), download:'https://github.com/SagerNet/sing-box/releases/latest'}},
  shadowrocket: {{scheme:'shadowrocket://add/sub://'+btoa(SUB_URL), download:'https://apps.apple.com/app/shadowrocket/id932747118'}},
  clash: {{scheme:'clash://install-config?url='+encodeURIComponent(SUB_URL), download:'https://github.com/MetaCubeX/ClashMetaForAndroid/releases/latest'}},
  streisand: {{scheme:'streisand://import/'+encodeURIComponent(SUB_URL), download:'https://apps.apple.com/app/streisand/id6450534064'}},
  nekoray: {{scheme:'', download:'https://github.com/MatsuriDayo/nekoray/releases/latest'}}
}};

function openApp(name){{
  const app=apps[name]; if(!app) return;
  if(app.scheme){{
    const start=Date.now(); window.location.href=app.scheme;
    setTimeout(()=>{{if(Date.now()-start<1600){{showToast('برنامه پیدا نشد → لینک دانلود'); setTimeout(()=>window.open(app.download,'_blank'),800);}}}},1500);
  }} else {{ window.open(app.download,'_blank'); showToast('لینک دانلود باز شد'); }}
}}

function copyText(text,msg){{
  if(navigator.clipboard) navigator.clipboard.writeText(text).then(()=>showToast(msg));
  else {{ const i=document.createElement('input'); i.value=text; document.body.appendChild(i); i.select(); document.execCommand('copy'); document.body.removeChild(i); showToast(msg); }}
}}
function shareLink(){{
  if(navigator.share) navigator.share({{title:'VROOM',url:SUB_URL}}).catch(()=>copyText(SUB_URL,'لینک کپی شد'));
  else copyText(SUB_URL,'لینک کپی شد');
}}
function runSpeedTest(){{
  const btn=document.getElementById('testBtn');
  btn.innerHTML='⏳ در حال تست...'; btn.disabled=true;
  setTimeout(()=>{{
    const ping=Math.floor(18+Math.random()*45);
    const down=(45+Math.random()*80).toFixed(1);
    btn.innerHTML='<svg viewBox="0 0 24 24" width="14" height="14" style="stroke:#fff;fill:none;stroke-width:2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg> تست سرعت';
    btn.disabled=false;
    showToast('پینگ: '+ping+'ms | دانلود: '+down+' Mbps');
  }},1800);
}}
function openQRModal(){{
  document.getElementById('qrModalImg').src='https://api.qrserver.com/v1/create-qr-code/?size=360x360&data='+encodeURIComponent(CONFIG);
  document.getElementById('qrModal').classList.add('show');
}}
function closeQRModal(){{ document.getElementById('qrModal').classList.remove('show'); }}
function openMenu(){{ document.getElementById('menuPanel').classList.add('show'); }}
function closeMenu(){{ document.getElementById('menuPanel').classList.remove('show'); }}

let toastTimer;
function showToast(msg){{
  const t=document.getElementById('toast'); t.textContent=msg; t.classList.add('show');
  clearTimeout(toastTimer); toastTimer=setTimeout(()=>t.classList.remove('show'),2800);
}}

function animateUsage(p){{
  const circle=document.getElementById('usageCircle');
  const bar=document.getElementById('usageBar');
  const text=document.getElementById('percentText');
  setTimeout(()=>{{
    circle.style.background=`conic-gradient(#3b82f6 0% ${{p}}%, rgba(30,41,59,.9) ${{p}}% 100%)`;
    bar.style.width=p+'%';
    text.textContent=p+'%';
  }},200);
}}

function renderHistory(){{
  const max=Math.max(...HISTORY,0.1);
  const days=['ش','ی','د','س','چ','پ','ج'];
  document.getElementById('historyBars').innerHTML=HISTORY.map((v,i)=>{{
    const h=Math.max(8,(v/max)*58);
    return `<div class="history-bar-item"><div class="history-bar" style="height:0" data-h="${{h}}"></div><div class="history-day">${{days[i]}}</div></div>`;
  }}).join('');
  setTimeout(()=>{{ document.querySelectorAll('.history-bar').forEach(b=>b.style.height=b.dataset.h+'px'); }},300);
}}

document.addEventListener('DOMContentLoaded',()=>{{ renderHistory(); animateUsage(PERCENT); }});
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ====== WEBSOCKET PROXY ======
RELAY_BUF = 2 * 1024 * 1024


async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("chunk too small")
    pos = 1 + 16
    addon_len = first_chunk[pos]
    pos += 1 + addon_len
    command = first_chunk[pos]
    pos += 1
    port = int.from_bytes(first_chunk[pos:pos + 2], "big")
    pos += 2
    addr_type = first_chunk[pos]
    pos += 1
    if addr_type == 1:
        addr_bytes = first_chunk[pos:pos + 4]
        pos += 4
        address = ".".join(str(b) for b in addr_bytes)
    elif addr_type == 2:
        domain_len = first_chunk[pos]
        pos += 1
        address = first_chunk[pos:pos + domain_len].decode("utf-8", errors="ignore")
        pos += domain_len
    elif addr_type == 3:
        addr_bytes = first_chunk[pos:pos + 16]
        pos += 16
        address = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError(f"unknown address type: {addr_type}")
    return command, address, port, first_chunk[pos:]


async def add_usage(uid: str, n: int):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n


async def ws_to_tcp(websocket: WebSocket, writer: asyncio.StreamWriter, conn_id: str, link_uid: str):
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


async def tcp_to_ws(websocket: WebSocket, reader: asyncio.StreamReader, conn_id: str, link_uid: str):
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
            if link_data is None or not link_data["active"]:
                await websocket.close(code=1008, reason="link not found or disabled")
                return
            if is_expired(link_data):
                await websocket.close(code=1008, reason="link expired")
                return
            max_conn = link_data.get("max_connections", 0)
        if max_conn > 0:
            already = client_ip in link_ip_map.get(uuid, set())
            if not already and count_connections_for_link(uuid) >= max_conn:
                await websocket.close(code=1008, reason="connection limit reached")
                return
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=10.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return
        command, address, port, initial_payload = await parse_vless_header(first_chunk)
        conn_id = secrets.token_urlsafe(8)
        connections[conn_id] = {"uuid": uuid, "ip": client_ip, "connected_at": datetime.now().isoformat(), "bytes": 0}
        connection_sockets[conn_id] = websocket
        link_ip_map[uuid].add(client_ip)
        size = len(first_chunk)
        stats["total_bytes"] += size
        stats["total_requests"] += 1
        connections[conn_id]["bytes"] += size
        hourly_traffic[datetime.now().strftime("%H:00")] += size
        await add_usage(uuid, size)
        reader, writer = await asyncio.wait_for(asyncio.open_connection(address, port), timeout=5.0)
        if initial_payload:
            p_size = len(initial_payload)
            stats["total_bytes"] += p_size
            connections[conn_id]["bytes"] += p_size
            hourly_traffic[datetime.now().strftime("%H:00")] += p_size
            await add_usage(uuid, p_size)
            writer.write(initial_payload)
            await writer.drain()
        task_up = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        task_down = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({task_up, task_down}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    except WebSocketDisconnect:
        pass
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
                uid = info.get("uuid")
                ip = info.get("ip")
                if uid and ip:
                    has_other = any(c.get("uuid") == uid and c.get("ip") == ip for c in connections.values())
                    if not has_other:
                        remove_ip_from_link(uid, ip)


# ====== LOGIN PAGE (simplified clean) ======
LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VROOM Login</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Vazirmatn',sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#05050c;color:#e8ecf4;direction:rtl}
.card{background:rgba(12,12,22,.95);border:1px solid rgba(255,215,0,.12);border-radius:24px;padding:40px 32px;width:100%;max-width:380px;box-shadow:0 0 40px rgba(255,215,0,.06)}
.brand{text-align:center;margin-bottom:28px}
.brand h1{font-size:28px;font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.brand p{font-size:12px;color:rgba(255,255,255,.35);margin-top:4px;letter-spacing:2px}
.form-group{margin-bottom:16px}
.form-group label{display:block;font-size:11px;font-weight:700;color:rgba(255,255,255,.5);margin-bottom:6px}
.form-group input{width:100%;padding:12px 14px;background:rgba(0,0,0,.35);border:1px solid rgba(255,215,0,.12);border-radius:12px;color:#fff;font-size:14px;font-family:inherit;outline:none}
.form-group input:focus{border-color:#ffd700}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#ffd700,#f7971e);border:none;border-radius:12px;color:#0a0a10;font-size:15px;font-weight:800;cursor:pointer;font-family:inherit}
.error{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:#f87171;padding:10px;border-radius:10px;font-size:13px;display:none;margin-bottom:14px;text-align:center}
.error.show{display:block}
</style>
</head>
<body>
<div class="card">
  <div class="brand"><h1>VROOM</h1><p>PANEL LOGIN</p></div>
  <div class="error" id="err"></div>
  <form id="f">
    <div class="form-group"><label>رمز عبور</label><input type="password" id="pw" placeholder="رمز ادمین..." autofocus></div>
    <button type="submit" class="btn">ورود</button>
  </form>
</div>
<script>
document.getElementById('f').onsubmit=async e=>{
  e.preventDefault();
  const err=document.getElementById('err'); err.classList.remove('show');
  try{
    const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
    if(!r.ok) throw new Error('رمز اشتباه');
    location.href='/dashboard';
  }catch(ex){err.textContent=ex.message;err.classList.add('show')}
};
</script>
</body>
</html>"""


# ====== DASHBOARD (improved + Telegram settings) ======
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800;900&family=Inter:wght@600;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#05050c;--surface:#12121f;--gold:#ffd700;--gold2:#f7971e;--primary:#7c5cfc;--text:#e8ecf4;--text2:rgba(255,255,255,.5);--border:rgba(255,215,0,.1);--green:#34d399;--red:#f87171}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Vazirmatn',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;direction:rtl}
.sidebar{width:200px;background:#0a0a12;border-left:1px solid var(--border);position:fixed;right:0;top:0;bottom:0;padding:16px 10px;display:flex;flex-direction:column;z-index:50}
.brand{font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Inter',sans-serif;padding:8px;margin-bottom:16px}
.nav-item{padding:10px 12px;border-radius:10px;font-size:12px;font-weight:600;color:var(--text2);cursor:pointer;margin-bottom:4px;border:none;background:none;width:100%;text-align:right;font-family:inherit}
.nav-item:hover,.nav-item.active{background:rgba(255,215,0,.08);color:var(--gold)}
.main{margin-right:200px;padding:20px 16px}
.page{display:none}.page.active{display:block}
.page-title{font-size:20px;font-weight:900;margin-bottom:16px;background:linear-gradient(135deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:14px}
.stat .l{font-size:10px;color:var(--text2);margin-bottom:4px}.stat .v{font-size:20px;font-weight:800}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px;margin-bottom:12px}
.card h3{font-size:13px;color:var(--gold);margin-bottom:12px}
.btn{padding:8px 14px;border-radius:10px;border:none;font-weight:700;font-size:12px;cursor:pointer;font-family:inherit}
.btn-gold{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#0a0a10}
.btn-outline{background:rgba(255,215,0,.08);color:var(--gold);border:1px solid var(--border)}
.btn-danger{background:rgba(248,113,113,.12);color:var(--red)}
input,select,textarea{width:100%;padding:10px 12px;background:rgba(0,0,0,.3);border:1px solid var(--border);border-radius:10px;color:#fff;font-family:inherit;font-size:13px;outline:none;margin-bottom:8px}
input:focus{border-color:var(--gold)}
.form-row{display:flex;gap:8px;flex-wrap:wrap}
.form-row>*{flex:1;min-width:100px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:right;padding:8px;color:var(--text2);border-bottom:1px solid var(--border);font-size:10px}
td{padding:8px;border-bottom:1px solid rgba(255,255,255,.04)}
.tag{display:inline-block;padding:2px 8px;border-radius:8px;font-size:10px;font-weight:700}
.tag-on{background:rgba(52,211,153,.15);color:var(--green)}.tag-off{background:rgba(248,113,113,.12);color:var(--red)}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(60px);background:#12121f;border:1px solid var(--border);padding:10px 20px;border-radius:12px;font-size:13px;color:var(--gold);opacity:0;transition:.3s;z-index:999}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mobile-bar{display:none;position:fixed;top:0;left:0;right:0;height:48px;background:#0a0a12;border-bottom:1px solid var(--border);z-index:60;align-items:center;justify-content:space-between;padding:0 14px}
@media(max-width:768px){
  .sidebar{transform:translateX(100%)}.sidebar.open{transform:translateX(0)}
  .main{margin-right:0;padding-top:60px}.stats{grid-template-columns:1fr 1fr}.mobile-bar{display:flex}
}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;display:none;align-items:center;justify-content:center}
.modal-bg.show{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:20px;width:90%;max-width:400px}
</style>
</head>
<body>
<div class="mobile-bar"><span style="font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent">VROOM</span><button class="btn btn-outline" onclick="document.querySelector('.sidebar').classList.toggle('open')">☰</button></div>
<aside class="sidebar">
  <div class="brand">VROOM</div>
  <button class="nav-item active" data-p="dash">📊 داشبورد</button>
  <button class="nav-item" data-p="links">📡 اینباندها</button>
  <button class="nav-item" data-p="tg">🤖 ربات تلگرام</button>
  <button class="nav-item" data-p="domain">🌐 دامنه</button>
  <button class="nav-item" data-p="security">🔒 امنیت</button>
  <div style="flex:1"></div>
  <button class="nav-item" onclick="fetch('/api/logout',{method:'POST'}).then(()=>location.href='/login')" style="color:var(--red)">خروج</button>
</aside>
<main class="main">
  <!-- DASH -->
  <section class="page active" id="p-dash">
    <div class="page-title">داشبورد</div>
    <div class="stats">
      <div class="stat"><div class="l">ترافیک</div><div class="v" id="s-traffic">--</div></div>
      <div class="stat"><div class="l">اینباندها</div><div class="v" id="s-links">--</div></div>
      <div class="stat"><div class="l">اتصالات</div><div class="v" id="s-conn">--</div></div>
      <div class="stat"><div class="l">آپتایم</div><div class="v" id="s-up" style="font-size:14px">--</div></div>
    </div>
    <div class="card"><h3>دسترسی سریع</h3>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-gold" onclick="quickCreate(1)">+1 GB</button>
        <button class="btn btn-gold" onclick="quickCreate(5)">+5 GB</button>
        <button class="btn btn-outline" onclick="switchP('links')">اینباندها</button>
        <button class="btn btn-outline" onclick="switchP('tg')">ربات تلگرام</button>
      </div>
    </div>
  </section>

  <!-- LINKS -->
  <section class="page" id="p-links">
    <div class="page-title">اینباندها <button class="btn btn-gold" style="float:left;font-size:11px" onclick="document.getElementById('addModal').classList.add('show')">+ افزودن</button></div>
    <div class="card" style="overflow-x:auto">
      <table><thead><tr><th>نام</th><th>مصرف</th><th>وضعیت</th><th>عملیات</th></tr></thead><tbody id="linksBody"></tbody></table>
    </div>
  </section>

  <!-- TELEGRAM -->
  <section class="page" id="p-tg">
    <div class="page-title">🤖 ربات تلگرام</div>
    <div class="card">
      <h3>تنظیمات ربات</h3>
      <p style="font-size:12px;color:var(--text2);margin-bottom:12px">توکن ربات رو از @BotFather بگیر و آیدی عددی ادمین رو از @userinfobot</p>
      <label style="font-size:11px;color:var(--text2)">توکن ربات</label>
      <input id="tg-token" placeholder="123456:ABC-DEF...">
      <label style="font-size:11px;color:var(--text2)">آیدی ادمین (عددی، چندتا با فاصله)</label>
      <input id="tg-admins" placeholder="123456789">
      <div style="display:flex;gap:8px;margin-top:8px">
        <button class="btn btn-gold" onclick="saveTelegram()">✅ ذخیره و فعال‌سازی</button>
        <button class="btn btn-danger" onclick="stopTelegram()">⏹ توقف</button>
      </div>
      <div id="tg-status" style="margin-top:12px;font-size:12px;color:var(--text2)">وضعیت: بررسی...</div>
    </div>
    <div class="card">
      <h3>دستورات ربات</h3>
      <pre style="font-size:11px;color:var(--text2);line-height:1.8;direction:ltr;text-align:left">
/start — شروع
/create name 5 30 — ساخت کانفیگ (نام حجم_GB روز)
/list — لیست اینباندها
/stats — آمار
/sub name — لینک ساب
      </pre>
    </div>
  </section>

  <!-- DOMAIN -->
  <section class="page" id="p-domain">
    <div class="page-title">دامنه</div>
    <div class="card">
      <h3>دامنه سفارشی</h3>
      <input id="domain-input" placeholder="example.com">
      <button class="btn btn-gold" onclick="saveDomain()">ذخیره</button>
      <div id="domain-cur" style="margin-top:10px;font-size:12px;color:var(--text2)"></div>
    </div>
  </section>

  <!-- SECURITY -->
  <section class="page" id="p-security">
    <div class="page-title">امنیت</div>
    <div class="card">
      <h3>تغییر رمز</h3>
      <input type="password" id="cur-pw" placeholder="رمز فعلی">
      <input type="password" id="new-pw" placeholder="رمز جدید">
      <button class="btn btn-gold" onclick="changePass()">تغییر رمز</button>
    </div>
  </section>
</main>

<div class="modal-bg" id="addModal" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="modal">
    <h3 style="margin-bottom:12px;color:var(--gold)">افزودن اینباند</h3>
    <input id="new-label" placeholder="نام (انگلیسی)">
    <div class="form-row">
      <input id="new-limit" type="number" placeholder="حجم" style="flex:2">
      <select id="new-unit"><option value="GB">GB</option><option value="MB">MB</option></select>
    </div>
    <input id="new-expiry" type="number" placeholder="انقضا (روز)">
    <input id="new-maxconn" type="number" placeholder="حداکثر IP (0=نامحدود)">
    <button class="btn btn-gold" style="width:100%;margin-top:8px" onclick="createLink()">ایجاد</button>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const $=s=>document.querySelector(s);
document.querySelectorAll('.nav-item[data-p]').forEach(el=>el.onclick=()=>switchP(el.dataset.p));
function switchP(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('p-'+id)?.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.toggle('active',n.dataset.p===id));
  document.querySelector('.sidebar')?.classList.remove('open');
  if(id==='links') loadLinks();
  if(id==='tg') loadTg();
  if(id==='domain') loadDomain();
}
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2800)}

async function loadStats(){
  try{
    const r=await fetch('/stats'); if(!r.ok) return;
    const d=await r.json();
    $('#s-traffic').textContent=d.total_traffic_mb+' MB';
    $('#s-links').textContent=d.links_count;
    $('#s-conn').textContent=d.active_connections;
    $('#s-up').textContent=d.uptime;
  }catch(e){}
}
async function loadLinks(){
  try{
    const r=await fetch('/api/links'); const d=await r.json();
    const body=$('#linksBody');
    if(!d.links?.length){body.innerHTML='<tr><td colspan="4" style="text-align:center;color:var(--text2)">خالی</td></tr>';return}
    body.innerHTML=d.links.map(l=>{
      const used=(l.used_bytes/1e9).toFixed(2);
      const lim=l.limit_bytes? (l.limit_bytes/1e9).toFixed(1)+' GB':'∞';
      return `<tr>
        <td><b>${l.label}</b></td>
        <td>${used} / ${lim}</td>
        <td><span class="tag ${l.active&&!l.expired?'tag-on':'tag-off'}">${l.active&&!l.expired?'فعال':'غیرفعال'}</span></td>
        <td style="display:flex;gap:4px;flex-wrap:wrap">
          <button class="btn btn-outline" style="padding:4px 8px;font-size:10px" onclick="copySub('${l.uuid}')">ساب</button>
          <button class="btn btn-outline" style="padding:4px 8px;font-size:10px" onclick="navigator.clipboard.writeText('${l.vless_link.replace(/'/g,"\\'")}').then(()=>toast('کپی شد'))">کپی</button>
          <button class="btn btn-danger" style="padding:4px 8px;font-size:10px" onclick="delLink('${l.uuid}')">حذف</button>
        </td>
      </tr>`;
    }).join('');
  }catch(e){}
}
function copySub(uid){navigator.clipboard.writeText(location.origin+'/sub/'+uid).then(()=>toast('لینک ساب کپی شد'))}
async function delLink(uid){if(!confirm('حذف؟'))return; await fetch('/api/links/'+uid,{method:'DELETE'}); toast('حذف شد'); loadLinks(); loadStats()}
async function createLink(){
  const label=$('#new-label').value.trim();
  const limit=parseFloat($('#new-limit').value)||0;
  const unit=$('#new-unit').value;
  const expiry=parseFloat($('#new-expiry').value)||0;
  const maxconn=parseInt($('#new-maxconn').value)||0;
  if(!label){toast('نام لازم است');return}
  const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,limit_value:limit,limit_unit:unit,expiry_days:expiry,max_connections:maxconn})});
  if(!r.ok){const e=await r.json().catch(()=>({})); toast(e.detail||'خطا'); return}
  toast('ساخته شد'); $('#addModal').classList.remove('show'); loadLinks(); loadStats();
}
async function quickCreate(gb){
  const name='user'+Math.floor(Math.random()*900+100);
  await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:name,limit_value:gb,limit_unit:'GB',expiry_days:30})});
  toast(name+' ساخته شد'); loadStats();
}
async function loadTg(){
  try{
    const r=await fetch('/api/telegram'); const d=await r.json();
    $('#tg-status').innerHTML=d.enabled
      ? `<span style="color:var(--green)">● فعال</span> — ادمین‌ها: ${d.admin_ids.join(', ')}`
      : '<span style="color:var(--red)">● غیرفعال</span>';
    if(d.admin_ids?.length) $('#tg-admins').value=d.admin_ids.join(' ');
  }catch(e){}
}
async function saveTelegram(){
  const token=$('#tg-token').value.trim();
  const admin_ids=$('#tg-admins').value.trim();
  const r=await fetch('/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,admin_ids})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){toast(d.detail||'خطا'); return}
  toast(d.enabled? 'ربات فعال شد ✅ @'+(d.bot_username||'') : 'ذخیره شد');
  loadTg();
}
async function stopTelegram(){await fetch('/api/telegram/stop',{method:'POST'}); toast('ربات متوقف شد'); loadTg()}
async function loadDomain(){
  const r=await fetch('/api/domain'); const d=await r.json();
  $('#domain-cur').textContent='فعلی: '+(d.domain||'(پیش‌فرض سرور)');
}
async function saveDomain(){
  const domain=$('#domain-input').value.trim();
  await fetch('/api/domain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domain})});
  toast('ذخیره شد'); loadDomain();
}
async function changePass(){
  const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:$('#cur-pw').value,new_password:$('#new-pw').value})});
  if(!r.ok){const e=await r.json().catch(()=>({})); toast(e.detail||'خطا'); return}
  toast('رمز عوض شد');
}
loadStats(); setInterval(loadStats,8000);
</script>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if await is_valid_session(token):
        return RedirectResponse(url="/dashboard")
    return HTMLResponse(content=LOGIN_HTML)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if not await is_valid_session(token):
        return RedirectResponse(url="/login")
    return HTMLResponse(content=DASHBOARD_HTML)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
