
#!/usr/bin/env python3
"""
VROOM Panel v4.0 — Full Button Telegram Bot + Live Dashboard
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

# ====== SECRET ======
try:
    SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_urlsafe(32)
    os.environ["SECRET_KEY"] = SECRET_KEY
except Exception:
    SECRET_KEY = "vroom-default-secret"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VROOM")

app = FastAPI(title="VROOM", docs_url=None, redoc_url=None)
CONFIG = {"port": int(os.environ.get("PORT", 8080)), "secret": SECRET_KEY}

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ====== STATE ======
connections: dict = {}
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {
    "total_bytes": 0,
    "download_bytes": 0,  # server → client
    "upload_bytes": 0,    # client → server
    "total_requests": 0,
    "total_errors": 0,
    "start_time": time.time(),
}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
http_client = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = ["www.speedtest.net"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()
CUSTOM_DOMAIN = ""
CUSTOM_DOMAIN_LOCK = asyncio.Lock()

TELEGRAM = {"token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), "admin_ids": [], "enabled": False, "offset": 0}
TELEGRAM_LOCK = asyncio.Lock()
TELEGRAM_TASK = None
# user_id -> pending action state for multi-step flows
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
    params = {
        "encryption": "none", "security": "tls", "type": "ws",
        "host": domain, "path": path, "sni": domain, "fp": "chrome", "alpn": "http/1.1",
    }
    query = "&".join(f"{k}={quote(str(v))}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"


def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = (unit or "GB").upper()
    if unit == "GB":
        return int(value * 1024 ** 3)
    if unit == "MB":
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


def fmt_bytes(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024**3:.2f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024**2:.1f} MB"
    return f"{b / 1024:.0f} KB"


# ====== TELEGRAM (FULL BUTTON UI) ======
def kb_main():
    return {
        "inline_keyboard": [
            [{"text": "➕ ساخت کانفیگ", "callback_data": "create_menu"}],
            [{"text": "📋 لیست اینباندها", "callback_data": "list"}, {"text": "📊 آمار زنده", "callback_data": "stats"}],
            [{"text": "🔗 گرفتن لینک ساب", "callback_data": "sub_menu"}],
            [{"text": "👥 اتصالات فعال", "callback_data": "conns"}, {"text": "🔄 رفرش", "callback_data": "home"}],
        ]
    }


def kb_create_presets():
    return {
        "inline_keyboard": [
            [{"text": "1 GB / 7 روز", "callback_data": "cpre_1_7"}, {"text": "5 GB / 30 روز", "callback_data": "cpre_5_30"}],
            [{"text": "10 GB / 30 روز", "callback_data": "cpre_10_30"}, {"text": "50 GB / 90 روز", "callback_data": "cpre_50_90"}],
            [{"text": "نامحدود / ۳۰ روز", "callback_data": "cpre_0_30"}],
            [{"text": "⬅️ بازگشت", "callback_data": "home"}],
        ]
    }


def kb_back():
    return {"inline_keyboard": [[{"text": "⬅️ منوی اصلی", "callback_data": "home"}]]}


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


async def tg_send(chat_id: int, text: str, reply_markup=None, parse_mode="HTML"):
    data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_api("sendMessage", **data)


async def tg_edit(chat_id: int, message_id: int, text: str, reply_markup=None):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        data["reply_markup"] = reply_markup
    return await tg_api("editMessageText", **data)


async def tg_answer(callback_id: str, text: str = ""):
    return await tg_api("answerCallbackQuery", callback_query_id=callback_id, text=text)


def is_admin(user_id: int) -> bool:
    return user_id in (TELEGRAM.get("admin_ids") or [])


async def handle_callback(cq: dict):
    data = cq.get("data") or ""
    msg = cq.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    message_id = msg.get("message_id")
    user_id = (cq.get("from") or {}).get("id")
    cb_id = cq.get("id")

    if not chat_id or not is_admin(user_id):
        await tg_answer(cb_id, "⛔ فقط ادمین")
        return

    await tg_answer(cb_id)

    if data == "home":
        TG_STATE.pop(user_id, None)
        await tg_edit(chat_id, message_id, "🚀 <b>VROOM Bot</b>\n\nاز دکمه‌های زیر استفاده کن:", reply_markup=kb_main())
        return

    if data == "stats":
        domain = get_domain()
        async with LINKS_LOCK:
            n_links = len(LINKS)
            active_links = sum(1 for L in LINKS.values() if L.get("active") and not is_expired(L))
        text = f"""📊 <b>آمار زنده VROOM</b>

👥 اتصالات فعال: <code>{len(connections)}</code>
📡 اینباندها: <code>{n_links}</code> (فعال: {active_links})
📥 دانلود: <code>{fmt_bytes(stats['download_bytes'])}</code>
📤 آپلود: <code>{fmt_bytes(stats['upload_bytes'])}</code>
📦 کل ترافیک: <code>{fmt_bytes(stats['total_bytes'])}</code>
📨 درخواست‌ها: <code>{stats['total_requests']}</code>
⏱️ آپتایم: <code>{uptime()}</code>
🌐 دامنه: <code>{domain}</code>
💻 CPU: <code>{psutil.cpu_percent()}%</code>
🧠 RAM: <code>{psutil.virtual_memory().percent}%</code>"""
        await tg_edit(chat_id, message_id, text, reply_markup=kb_back())
        return

    if data == "list":
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 هیچ اینباندی نیست.", reply_markup=kb_back())
            return
        lines = []
        buttons = []
        for i, (uid, L) in enumerate(items[:15]):
            used = fmt_bytes(L["used_bytes"])
            lim = fmt_bytes(L["limit_bytes"]) if L["limit_bytes"] else "∞"
            st = "✅" if L["active"] and not is_expired(L) else "❌"
            conn = count_connections_for_link(uid)
            lines.append(f"{st} <b>{L['label']}</b>\n   {used}/{lim} | 👥 {conn}")
            buttons.append([{"text": f"🔗 {L['label']}", "callback_data": f"sub_{uid}"}])
        buttons.append([{"text": "⬅️ منوی اصلی", "callback_data": "home"}])
        await tg_edit(chat_id, message_id, "📋 <b>لیست اینباندها:</b>\n\n" + "\n\n".join(lines), reply_markup={"inline_keyboard": buttons})
        return

    if data == "conns":
        if not connections:
            await tg_edit(chat_id, message_id, "👥 هیچ اتصال فعالی نیست.", reply_markup=kb_back())
            return
        lines = []
        for cid, info in list(connections.items())[:20]:
            lines.append(f"• <code>{info.get('uuid')}</code> | IP: <code>{info.get('ip')}</code> | {fmt_bytes(info.get('bytes', 0))}")
        await tg_edit(chat_id, message_id, f"👥 <b>اتصالات فعال ({len(connections)})</b>\n\n" + "\n".join(lines), reply_markup=kb_back())
        return

    if data == "create_menu":
        await tg_edit(chat_id, message_id, "➕ <b>ساخت کانفیگ</b>\n\nیکی از پکیج‌های آماده را انتخاب کن:", reply_markup=kb_create_presets())
        return

    if data.startswith("cpre_"):
        # cpre_5_30 → 5 GB, 30 days
        parts = data.split("_")
        try:
            gb = float(parts[1])
            days = float(parts[2])
        except Exception:
            gb, days = 5, 30
        label = f"u{secrets.token_hex(3)}"
        limit_bytes = parse_size_to_bytes(gb, "GB") if gb > 0 else 0
        expiry = compute_expiry(days)
        async with LINKS_LOCK:
            LINKS[label] = {
                "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
                "max_connections": 0, "created_at": datetime.now().isoformat(),
                "active": True, "expiry": expiry,
            }
        domain = get_domain()
        sub_url = f"https://{domain}/sub/{label}"
        vless = generate_vless_link(label, remark=f"VROOM-{label}")
        text = f"""✅ <b>کانفیگ ساخته شد!</b>

🏷 نام: <code>{label}</code>
📦 حجم: <code>{'∞' if gb <= 0 else f'{gb} GB'}</code>
📅 انقضا: <code>{int(days)} روز</code>

📥 <b>لینک ساب:</b>
<code>{sub_url}</code>

📋 <b>کانفیگ:</b>
<code>{vless}</code>"""
        await tg_edit(chat_id, message_id, text, reply_markup={
            "inline_keyboard": [
                [{"text": "📋 لیست", "callback_data": "list"}, {"text": "➕ یکی دیگه", "callback_data": "create_menu"}],
                [{"text": "⬅️ منوی اصلی", "callback_data": "home"}],
            ]
        })
        return

    if data == "sub_menu":
        async with LINKS_LOCK:
            items = list(LINKS.keys())[:20]
        if not items:
            await tg_edit(chat_id, message_id, "📭 اینباندی نیست.", reply_markup=kb_back())
            return
        buttons = [[{"text": f"🔗 {uid}", "callback_data": f"sub_{uid}"}] for uid in items]
        buttons.append([{"text": "⬅️ بازگشت", "callback_data": "home"}])
        await tg_edit(chat_id, message_id, "🔗 کدام اینباند؟", reply_markup={"inline_keyboard": buttons})
        return

    if data.startswith("sub_"):
        uid = data[4:]
        async with LINKS_LOCK:
            link = LINKS.get(uid)
        if not link:
            await tg_edit(chat_id, message_id, "❌ پیدا نشد.", reply_markup=kb_back())
            return
        domain = get_domain()
        sub_url = f"https://{domain}/sub/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
        text = f"""🔗 <b>{link['label']}</b>

📥 ساب:
<code>{sub_url}</code>

📋 کانفیگ:
<code>{vless}</code>

📊 مصرف: {fmt_bytes(link['used_bytes'])} / {fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}
👥 اتصالات: {count_connections_for_link(uid)}"""
        await tg_edit(chat_id, message_id, text, reply_markup={
            "inline_keyboard": [
                [{"text": "🗑 حذف این اینباند", "callback_data": f"del_{uid}"}],
                [{"text": "⬅️ منوی اصلی", "callback_data": "home"}],
            ]
        })
        return

    if data.startswith("del_"):
        uid = data[4:]
        async with LINKS_LOCK:
            LINKS.pop(uid, None)
        await close_connections_for_link(uid)
        await tg_edit(chat_id, message_id, f"✅ «{uid}» حذف شد.", reply_markup=kb_back())
        return


async def handle_tg_message(msg: dict):
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    from_user = msg.get("from", {})
    user_id = from_user.get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return

    if not is_admin(user_id):
        await tg_send(chat_id, "⛔ فقط ادمین می‌تونه از ربات استفاده کنه.")
        return

    # Any message → show main menu (no text commands needed)
    await tg_send(chat_id, "🚀 <b>VROOM Bot</b>\n\nاز دکمه‌های شیشه‌ای زیر استفاده کن — نیازی به تایپ دستور نیست:", reply_markup=kb_main())


async def telegram_poll_loop():
    logger.info("🤖 Telegram button-bot started")
    while True:
        try:
            async with TELEGRAM_LOCK:
                token = TELEGRAM.get("token")
                enabled = TELEGRAM.get("enabled")
                offset = TELEGRAM.get("offset", 0)
            if not token or not enabled:
                await asyncio.sleep(5)
                continue
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(
                    f"https://api.telegram.org/bot{token}/getUpdates",
                    params={"offset": offset, "timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])},
                )
                data = r.json()
            if not data.get("ok"):
                await asyncio.sleep(3)
                continue
            for upd in data.get("result", []):
                async with TELEGRAM_LOCK:
                    TELEGRAM["offset"] = upd["update_id"] + 1
                if "callback_query" in upd:
                    try:
                        await handle_callback(upd["callback_query"])
                    except Exception as e:
                        logger.error(f"cb error: {e}")
                elif "message" in upd:
                    try:
                        await handle_tg_message(upd["message"])
                    except Exception as e:
                        logger.error(f"msg error: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"poll: {e}")
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
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.get(f"https://{domain}/health")
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000),
        timeout=httpx.Timeout(180.0, connect=30.0),
        follow_redirects=True,
    )
    logger.info(f"🚀 VROOM v4 on port {CONFIG['port']}")
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
    return {
        "status": "ok",
        "connections": len(connections),
        "download": stats["download_bytes"],
        "upload": stats["upload_bytes"],
        "uptime": uptime(),
    }


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    if hash_password(str(body.get("password") or "")) != AUTH["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid password")
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
        raise HTTPException(status_code=400, detail="Current password incorrect")
    new = str(body.get("new_password") or "")
    if len(new) < 4:
        raise HTTPException(status_code=400, detail="Min 4 chars")
    AUTH["password_hash"] = hash_password(new)
    return {"ok": True}


@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    # per-link connection breakdown
    link_conns = {}
    for cid, info in connections.items():
        u = info.get("uuid", "?")
        link_conns[u] = link_conns.get(u, 0) + 1
    return {
        "active_connections": len(connections),
        "download_bytes": stats["download_bytes"],
        "upload_bytes": stats["upload_bytes"],
        "total_bytes": stats["total_bytes"],
        "download_fmt": fmt_bytes(stats["download_bytes"]),
        "upload_fmt": fmt_bytes(stats["upload_bytes"]),
        "total_fmt": fmt_bytes(stats["total_bytes"]),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.05),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "disk_used": round(psutil.disk_usage("/").used / 1024**3, 2),
        "disk_total": round(psutil.disk_usage("/").total / 1024**3, 2),
        "hourly_traffic": dict(hourly_traffic),
        "telegram_enabled": TELEGRAM.get("enabled", False),
        "link_connections": link_conns,
        "connection_list": [
            {"uuid": i.get("uuid"), "ip": i.get("ip"), "bytes": i.get("bytes", 0), "bytes_fmt": fmt_bytes(i.get("bytes", 0)), "since": i.get("connected_at")}
            for i in connections.values()
        ],
    }


@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New").strip()[:60]
    if not re.match(r"^[a-zA-Z0-9\-_. ]+$", label):
        raise HTTPException(400, "English letters/numbers only")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(400, "Already exists")
    limit_value = float(body.get("limit_value") or 0)
    limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = max(0, int(body.get("max_connections") or 0))
    expiry = compute_expiry(body.get("expiry_days"))
    async with LINKS_LOCK:
        LINKS[label] = {
            "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
            "max_connections": max_conn, "created_at": datetime.now().isoformat(),
            "active": True, "expiry": expiry,
        }
    return {
        "uuid": label, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0,
        "max_connections": max_conn, "active": True, "expiry": expiry,
        "vless_link": generate_vless_link(label, remark=f"VROOM-{label}"),
    }


@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            result.append({
                "uuid": uid, "label": data["label"],
                "limit_bytes": data["limit_bytes"], "used_bytes": data["used_bytes"],
                "max_connections": data.get("max_connections", 0),
                "active": data["active"], "expiry": data.get("expiry", ""),
                "expired": is_expired(data), "created_at": data["created_at"],
                "current_connections": count_connections_for_link(uid),
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
            lu = body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, lu)
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
    if domain and not re.match(r"^[a-z0-9\-_.]+$", domain):
        raise HTTPException(400, "Invalid domain")
    async with CUSTOM_DOMAIN_LOCK:
        global CUSTOM_DOMAIN
        CUSTOM_DOMAIN = domain
    return {"ok": True, "domain": CUSTOM_DOMAIN}


@app.get("/api/telegram")
async def get_tg(_=Depends(require_auth)):
    async with TELEGRAM_LOCK:
        return {
            "has_token": bool(TELEGRAM.get("token")),
            "admin_ids": TELEGRAM.get("admin_ids", []),
            "enabled": TELEGRAM.get("enabled", False),
        }


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


# ====== SUB PAGE (Gold) ======
@app.get("/sub/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if not link:
            raise HTTPException(404, "Not found")
    if not link["active"] or is_expired(link):
        raise HTTPException(403, "Disabled or expired")

    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / 1024**3, 2)
    limit_gb = round(link["limit_bytes"] / 1024**3, 2) if link["limit_bytes"] else 0
    percent = round((link["used_bytes"] / link["limit_bytes"]) * 100, 1) if link["limit_bytes"] else 0
    remaining = round(max(0, limit_gb - used_gb), 2) if limit_gb else "∞"

    if is_expired(link):
        status_text, status_color = "منقضی", "#f87171"
    elif link["limit_bytes"] and link["used_bytes"] >= link["limit_bytes"]:
        status_text, status_color = "محدود", "#fbbf24"
    else:
        status_text, status_color = "فعال", "#34d399"

    exp = link.get("expiry")
    if exp:
        try:
            ed = datetime.fromisoformat(exp)
            days_left = max(0, (ed - datetime.now()).days)
            days_txt, exp_disp = f"{days_left} روز", ed.strftime("%Y/%m/%d")
        except Exception:
            days_txt = exp_disp = "نامحدود"
    else:
        days_txt = exp_disp = "نامحدود"

    domain = get_domain()
    sub_url = f"https://{domain}/sub/{uid}"
    qr_data = quote(server_link, safe="")
    history = [0.7, 1.1, 0.5, 1.8, 1.3, 0.9, max(0.2, used_gb * 0.08)]
    live_conns = count_connections_for_link(uid)

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
.logo{{display:flex;align-items:center;gap:8px}}.logo b{{font-size:22px;font-weight:900;font-family:Inter;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.logo span{{font-size:11px;font-weight:700;color:#ff6b6b}}
.icon-btn{{width:36px;height:36px;display:flex;align-items:center;justify-content:center;background:rgba(255,215,0,.08);border:1px solid rgba(255,215,0,.15);color:var(--gold);border-radius:10px;cursor:pointer;font-size:16px}}
.main-title{{font-size:20px;font-weight:900;font-family:Inter;color:#fff}}
.sub-title{{color:var(--gold);font-size:12px;margin:4px 0 14px;letter-spacing:1px;opacity:.85}}
.live-bar{{display:flex;align-items:center;justify-content:space-between;background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.2);border-radius:14px;padding:10px 14px;margin-bottom:12px;flex-wrap:wrap;gap:8px}}
.live-left{{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:#34d399}}
.dot{{width:8px;height:8px;background:#34d399;border-radius:50%;animation:pd 1.6s infinite}}
@keyframes pd{{0%{{box-shadow:0 0 0 0 rgba(52,211,153,.5)}}70%{{box-shadow:0 0 0 8px transparent}}}}
.live-stats{{display:flex;gap:12px;font-size:11px;color:rgba(255,255,255,.55)}}
.live-stats b{{color:#fff;font-weight:800}}
.usage-card{{background:linear-gradient(145deg,rgba(15,23,42,.9),rgba(10,14,28,.95));border:1px solid rgba(59,130,246,.25);border-radius:18px;padding:14px;margin-bottom:12px}}
.usage-header{{display:flex;justify-content:space-between;margin-bottom:12px;font-size:13px;font-weight:700;color:#93c5fd}}
.usage-stats{{display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;align-items:center}}
@media(max-width:420px){{.usage-stats{{grid-template-columns:1fr 1fr}}.ucw{{grid-column:1/-1;justify-self:center;margin-top:6px}}}}
.us{{text-align:center}}.us .l{{font-size:10px;color:rgba(148,163,184,.8);margin-bottom:3px}}.us .v{{font-size:15px;font-weight:800}}
.ucw{{width:72px;height:72px;position:relative;display:flex;align-items:center;justify-content:center}}
.uco{{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(59,130,246,.25)}}
.uc{{width:62px;height:62px;border-radius:50%;background:conic-gradient(#3b82f6 0% 0%,rgba(30,41,59,.9) 0% 100%);display:flex;align-items:center;justify-content:center;position:relative;transition:background 1s}}
.uc::before{{content:'';position:absolute;inset:6px;border-radius:50%;background:#0b1222}}
.uct{{position:relative;z-index:1;text-align:center;font-size:13px;font-weight:800}}
.ub{{margin-top:12px;height:5px;background:rgba(30,41,59,.8);border-radius:10px;overflow:hidden}}
.ubf{{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#8b5cf6,#ef4444);border-radius:10px;transition:width 1s}}
.card{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.1);border-radius:16px;padding:14px;margin-bottom:12px}}
.card h3{{font-size:11px;letter-spacing:1px;font-weight:700;margin-bottom:10px;color:var(--gold);opacity:.85;text-transform:uppercase}}
.row{{background:rgba(0,0,0,.35);padding:10px 12px;border-radius:11px;display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:10px;font-size:11px;font-family:monospace;color:rgba(255,255,255,.4);border:1px solid rgba(255,215,0,.08);cursor:pointer}}
.row button{{background:linear-gradient(135deg,var(--gold),var(--gold2));border:none;color:#0a0a10;padding:5px 12px;border-radius:8px;font-size:11px;font-weight:800;cursor:pointer}}
.row .lt{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.ig{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,215,0,.08)}}
.ii{{background:rgba(0,0,0,.3);padding:8px 6px;border-radius:10px;text-align:center}}.ii .l{{font-size:9px;opacity:.4;display:block;margin-bottom:2px}}.ii .v{{font-size:13px;font-weight:800}}
.cb{{background:rgba(0,0,0,.4);padding:10px;border-radius:11px;font-size:10px;font-family:monospace;word-break:break-all;margin-bottom:10px;max-height:60px;overflow-y:auto;border:1px solid rgba(255,215,0,.08);direction:ltr;text-align:left;color:rgba(255,255,255,.4);cursor:pointer}}
.qr{{width:110px;height:110px;background:#fff;border-radius:12px;margin:0 auto 10px;overflow:hidden;border:2px solid rgba(255,215,0,.25);cursor:pointer}}
.qr img{{width:100%;height:100%}}
.br{{display:flex;gap:8px}}.br button{{flex:1;padding:11px;border:none;border-radius:12px;font-weight:800;font-size:13px;cursor:pointer;font-family:inherit}}
.add{{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#0a0a10}}.sh{{background:rgba(255,215,0,.1);color:var(--gold);border:1px solid rgba(255,215,0,.25)}}
.qg{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}}
@media(max-width:400px){{.qg{{grid-template-columns:repeat(2,1fr)}}}}
.qi{{background:rgba(255,255,255,.03);border:1px solid rgba(255,215,0,.09);border-radius:14px;padding:10px 4px;text-align:center;cursor:pointer}}
.qi .ic{{width:40px;height:40px;margin:0 auto 5px;border-radius:10px;overflow:hidden}}.qi .ic svg{{width:100%;height:100%}}.qi .nm{{font-size:10px;color:rgba(255,255,255,.55)}}
footer{{margin-top:12px;padding-top:10px;border-top:1px solid rgba(255,215,0,.08);text-align:center;font-size:11px;color:#4a5370}}
footer b{{background:linear-gradient(135deg,var(--gold),var(--gold2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:rgba(10,10,18,.95);padding:12px 22px;border-radius:12px;font-size:13px;color:var(--gold);opacity:0;transition:.35s;border:1px solid rgba(255,215,0,.25);z-index:9999;font-weight:700}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.menu{{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:10001;display:none;align-items:flex-end;justify-content:center;padding:0}}
.menu.show{{display:flex !important}}
.ms{{background:#12121f;border-radius:24px 24px 0 0;padding:20px 16px 28px;width:100%;max-width:500px;border:1px solid rgba(255,215,0,.15);box-shadow:0 -8px 40px rgba(0,0,0,.5)}}
.ms h4{{text-align:center;color:var(--gold);margin-bottom:14px;font-size:15px}}
.mi{{padding:14px 16px;background:rgba(255,255,255,.05);border-radius:14px;margin-bottom:8px;cursor:pointer;font-size:14px;font-weight:600;border:1px solid rgba(255,215,0,.1);transition:background .15s;user-select:none;-webkit-user-select:none}}
.mi:active{{background:rgba(255,215,0,.15)}}
.mi.close-btn{{color:#f87171;text-align:center;margin-top:4px}}
</style></head><body>
<div class="container">
<header>
  <div class="logo"><b>VROOM</b><span>PANEL</span></div>
  <div style="display:flex;gap:6px">
    <button type="button" class="icon-btn" id="btnRefresh" title="بروزرسانی">🔄</button>
    <button type="button" class="icon-btn" id="btnMenu" title="منو">☰</button>
  </div>
</header>
<h1 class="main-title">Subscription</h1>
<p class="sub-title">✦ {link['label']} ✦</p>

<div class="live-bar">
  <div class="live-left"><div class="dot"></div> سرور آنلاین</div>
  <div class="live-stats">
    <span>👥 <b id="liveConns">{live_conns}</b> وصل</span>
  </div>
</div>

<div class="usage-card">
  <div class="usage-header"><span>⚡ وضعیت مصرف</span><span style="font-size:10px;opacity:.6" id="lu">همین الان</span></div>
  <div class="usage-stats">
    <div class="us"><div class="l">مصرفی</div><div class="v">{used_gb} GB</div></div>
    <div class="us"><div class="l">وضعیت</div><div class="v" style="color:{status_color};font-size:13px">{status_text}</div></div>
    <div class="us"><div class="l">باقی</div><div class="v">{remaining}{' GB' if remaining!='∞' else ''}</div></div>
    <div class="ucw"><div class="uco"></div><div class="uc" id="uc"><div class="uct" id="pct">0%</div></div></div>
  </div>
  <div class="ub"><div class="ubf" id="ub"></div></div>
</div>

<div class="card">
  <h3>⬡ لینک ساب</h3>
  <div class="row" onclick="cp(SUB,'لینک ساب کپی شد')"><span class="lt">{sub_url}</span><button onclick="event.stopPropagation();cp(SUB,'لینک ساب کپی شد')">کپی</button></div>
  <div class="ig">
    <div class="ii"><span class="l">وضعیت</span><span class="v" style="color:{status_color}">{status_text}</span></div>
    <div class="ii"><span class="l">انقضا</span><span class="v" style="color:#fbbf24">{exp_disp}</span></div>
    <div class="ii"><span class="l">باقی</span><span class="v" style="color:#6bcbff">{days_txt}</span></div>
  </div>
</div>

<div class="card">
  <h3>⬡ کانفیگ و QR</h3>
  <div class="cb" onclick="cp(CFG,'کانفیگ کپی شد')">{server_link}</div>
  <div style="text-align:center">
    <div class="qr" onclick="document.getElementById('qrm').style.display='flex'"><img src="https://api.qrserver.com/v1/create-qr-code/?size=220x220&data={qr_data}"></div>
    <div class="br"><button class="add" onclick="cp(SUB,'کپی شد')">＋ اضافه</button><button class="sh" onclick="share()">اشتراک</button></div>
  </div>
</div>

<div class="card">
  <h3>⚡ ابزارهای سریع</h3>
  <div class="qg">
    <div class="qi" onclick="oa('hiddify')"><div class="ic"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#455FE9"/><path d="M14 34V14h8.8c4.1 0 6.7 2.2 6.7 5.6 0 2.2-1.1 3.9-3.1 4.7L32 34h-5.6l-4.6-7.8H19V34h-5zm5-12h2.8c1.8 0 2.8-.8 2.8-2.2s-1-2.2-2.8-2.2H19v4.4z" fill="#fff"/></svg></div><div class="nm">Hiddify</div></div>
    <div class="qi" onclick="oa('v2rayng')"><div class="ic"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#1E88E5"/><path d="M24 10L12 18v12l12 8 12-8V18L24 10zm0 5.5l7.5 5v7l-7.5 5-7.5-5v-7l7.5-5z" fill="#fff"/><circle cx="24" cy="24" r="3.5" fill="#fff"/></svg></div><div class="nm">v2rayNG</div></div>
    <div class="qi" onclick="oa('v2box')"><div class="ic"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#6C5CE7"/><text x="24" y="31" text-anchor="middle" fill="#fff" font-size="16" font-weight="800">V2</text></svg></div><div class="nm">V2Box</div></div>
    <div class="qi" onclick="oa('clash')"><div class="ic"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#D63031"/><circle cx="24" cy="24" r="11" fill="none" stroke="#fff" stroke-width="3.5"/><circle cx="24" cy="24" r="5" fill="#fff"/></svg></div><div class="nm">Clash</div></div>
  </div>
</div>
<footer>Powered by <b>VROOM</b></footer>
</div>
<div id="qrm" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);align-items:center;justify-content:center;z-index:10000" onclick="this.style.display='none'">
<img style="width:min(80vw,300px);border-radius:16px;border:2px solid rgba(255,215,0,.4)" src="https://api.qrserver.com/v1/create-qr-code/?size=360x360&data={qr_data}">
</div>
<div class="toast" id="toast"></div>
<div class="menu" id="menu">
  <div class="ms" id="menuSheet">
    <h4>منو</h4>
    <div class="mi" data-act="copy-sub">📥 کپی لینک ساب</div>
    <div class="mi" data-act="copy-cfg">📋 کپی کانفیگ</div>
    <div class="mi" data-act="share">↗ اشتراک‌گذاری</div>
    <div class="mi" data-act="reload">🔄 بروزرسانی وضعیت</div>
    <div class="mi close-btn" data-act="close">بستن</div>
  </div>
</div>
<script>
(function(){{
  var SUB = {json.dumps(sub_url)};
  var CFG = {json.dumps(server_link)};
  var P = {percent};

  var apps = {{
    hiddify: {{s: 'hiddify://import/' + encodeURIComponent(SUB), d: 'https://github.com/hiddify/hiddify-app/releases/latest'}},
    v2rayng: {{s: 'v2rayng://install-config?url=' + encodeURIComponent(SUB), d: 'https://github.com/2dust/v2rayNG/releases/latest'}},
    v2box: {{s: 'v2box://install-config?url=' + encodeURIComponent(SUB), d: 'https://apps.apple.com/app/v2box-v2ray-client/id6446814690'}},
    clash: {{s: 'clash://install-config?url=' + encodeURIComponent(SUB), d: 'https://github.com/MetaCubeX/ClashMetaForAndroid/releases/latest'}}
  }};

  function toast(m) {{
    var t = document.getElementById('toast');
    if (!t) return;
    t.textContent = m;
    t.classList.add('show');
    setTimeout(function(){{ t.classList.remove('show'); }}, 2500);
  }}

  function cp(text, msg) {{
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(text).then(function(){{ toast(msg); }}).catch(function(){{ fallbackCopy(text, msg); }});
    }} else {{
      fallbackCopy(text, msg);
    }}
  }}

  function fallbackCopy(text, msg) {{
    var i = document.createElement('textarea');
    i.value = text;
    i.style.position = 'fixed';
    i.style.left = '-9999px';
    document.body.appendChild(i);
    i.select();
    try {{ document.execCommand('copy'); toast(msg); }} catch(e) {{ toast('خطا در کپی'); }}
    document.body.removeChild(i);
  }}

  function share() {{
    if (navigator.share) {{
      navigator.share({{ title: 'VROOM', url: SUB }}).catch(function(){{ cp(SUB, 'لینک کپی شد'); }});
    }} else {{
      cp(SUB, 'لینک کپی شد');
    }}
  }}

  function openMenu() {{
    var m = document.getElementById('menu');
    if (m) m.classList.add('show');
  }}
  function closeMenu() {{
    var m = document.getElementById('menu');
    if (m) m.classList.remove('show');
  }}

  function oa(n) {{
    var a = apps[n];
    if (!a) return;
    if (a.s) {{
      var t = Date.now();
      window.location.href = a.s;
      setTimeout(function() {{
        if (Date.now() - t < 1600) {{
          toast('برنامه پیدا نشد → دانلود');
          setTimeout(function(){{ window.open(a.d, '_blank'); }}, 600);
        }}
      }}, 1400);
    }} else {{
      window.open(a.d, '_blank');
    }}
  }}

  // bind header buttons
  var btnMenu = document.getElementById('btnMenu');
  var btnRefresh = document.getElementById('btnRefresh');
  if (btnMenu) btnMenu.addEventListener('click', function(e){{ e.preventDefault(); openMenu(); }});
  if (btnRefresh) btnRefresh.addEventListener('click', function(e){{ e.preventDefault(); location.reload(); }});

  // menu backdrop close
  var menuEl = document.getElementById('menu');
  if (menuEl) {{
    menuEl.addEventListener('click', function(e) {{
      if (e.target === menuEl) closeMenu();
    }});
  }}

  // menu items
  document.querySelectorAll('.mi[data-act]').forEach(function(el) {{
    el.addEventListener('click', function(e) {{
      e.preventDefault();
      e.stopPropagation();
      var act = el.getAttribute('data-act');
      if (act === 'copy-sub') {{ cp(SUB, 'لینک ساب کپی شد'); closeMenu(); }}
      else if (act === 'copy-cfg') {{ cp(CFG, 'کانفیگ کپی شد'); closeMenu(); }}
      else if (act === 'share') {{ share(); closeMenu(); }}
      else if (act === 'reload') {{ location.reload(); }}
      else if (act === 'close') {{ closeMenu(); }}
    }});
  }});

  // row / config box / buttons that use inline onclick still need global helpers
  window.cp = cp;
  window.share = share;
  window.oa = oa;
  window.SUB = SUB;
  window.CFG = CFG;

  // animate usage
  setTimeout(function() {{
    var uc = document.getElementById('uc');
    var ub = document.getElementById('ub');
    var pct = document.getElementById('pct');
    if (uc) uc.style.background = 'conic-gradient(#3b82f6 0% ' + P + '%, rgba(30,41,59,.9) ' + P + '% 100%)';
    if (ub) ub.style.width = P + '%';
    if (pct) pct.textContent = P + '%';
  }}, 300);
}})();
</script></body></html>"""

    return HTMLResponse(content=html)


# ====== WS PROXY with separate up/down ======
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
        address = ":".join(f"{first_chunk[pos + i]:02x}{first_chunk[pos + i + 1]:02x}" for i in range(0, 16, 2))
        pos += 16
    else:
        raise ValueError("addr")
    return address, port, first_chunk[pos:]


async def add_usage(uid: str, n: int, direction: str = "total"):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n
    stats["total_bytes"] += n
    if direction == "up":
        stats["upload_bytes"] += n
    elif direction == "down":
        stats["download_bytes"] += n


async def ws_to_tcp(ws: WebSocket, writer, conn_id, uid):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            size = len(data)
            stats["total_requests"] += 1
            connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(uid, size, "up")
            writer.write(data)
            await writer.drain()
    except WebSocketDisconnect:
        pass
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass


async def tcp_to_ws(ws: WebSocket, reader, conn_id, uid):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data:
                break
            size = len(data)
            connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(uid, size, "down")
            await ws.send_bytes((b"\x00\x00" + data) if first else data)
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
        if max_conn > 0:
            if client_ip not in link_ip_map.get(uuid, set()) and count_connections_for_link(uuid) >= max_conn:
                await websocket.close(code=1008, reason="limit")
                return
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=10)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return
        address, port, payload = await parse_vless_header(first_chunk)
        conn_id = secrets.token_urlsafe(8)
        connections[conn_id] = {"uuid": uuid, "ip": client_ip, "connected_at": datetime.now().isoformat(), "bytes": 0}
        connection_sockets[conn_id] = websocket
        link_ip_map[uuid].add(client_ip)
        await add_usage(uuid, len(first_chunk), "up")
        connections[conn_id]["bytes"] += len(first_chunk)
        reader, writer = await asyncio.wait_for(asyncio.open_connection(address, port), timeout=5)
        if payload:
            await add_usage(uuid, len(payload), "up")
            writer.write(payload)
            await writer.drain()
        t1 = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        t2 = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    except Exception as e:
        stats["total_errors"] += 1
        error_logs.append({"error": str(e), "time": datetime.now().isoformat()})
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
LOGIN_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VROOM Login</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;700;800&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#05050c;color:#e8ecf4;direction:rtl}
.card{background:rgba(12,12,22,.95);border:1px solid rgba(255,215,0,.12);border-radius:24px;padding:40px 32px;width:100%;max-width:380px}
.brand{text-align:center;margin-bottom:28px}
.brand h1{font-size:28px;font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.brand p{font-size:12px;color:rgba(255,255,255,.35);margin-top:4px}
input{width:100%;padding:12px 14px;background:rgba(0,0,0,.35);border:1px solid rgba(255,215,0,.12);border-radius:12px;color:#fff;font-size:14px;font-family:inherit;outline:none;margin-bottom:12px}
input:focus{border-color:#ffd700}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#ffd700,#f7971e);border:none;border-radius:12px;color:#0a0a10;font-size:15px;font-weight:800;cursor:pointer;font-family:inherit}
.err{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:#f87171;padding:10px;border-radius:10px;font-size:13px;display:none;margin-bottom:12px;text-align:center}
.err.show{display:block}
</style></head><body>
<div class="card"><div class="brand"><h1>VROOM</h1><p>PANEL LOGIN</p></div>
<div class="err" id="err"></div>
<form id="f"><input type="password" id="pw" placeholder="رمز ادمین..." autofocus>
<button class="btn" type="submit">ورود</button></form></div>
<script>
document.getElementById('f').onsubmit=async e=>{
e.preventDefault();const err=document.getElementById('err');err.classList.remove('show');
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
if(!r.ok)throw new Error('رمز اشتباه');location.href='/dashboard'}catch(ex){err.textContent=ex.message;err.classList.add('show')}
}</script></body></html>"""


# ====== DASHBOARD FULL ======
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800;900&family=Inter:wght@700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#05050c;--s:#12121f;--g:#ffd700;--g2:#f7971e;--t:#e8ecf4;--t2:rgba(255,255,255,.5);--b:rgba(255,215,0,.1);--gn:#34d399;--rd:#f87171;--bl:#3b82f6}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;direction:rtl}
.sb{width:200px;background:#0a0a12;border-left:1px solid var(--b);position:fixed;right:0;top:0;bottom:0;padding:14px 10px;display:flex;flex-direction:column;z-index:50;transition:.3s}
.brand{font-size:18px;font-weight:900;font-family:Inter;background:linear-gradient(135deg,var(--g),var(--g2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;padding:6px;margin-bottom:14px}
.ni{padding:10px 12px;border-radius:10px;font-size:12px;font-weight:600;color:var(--t2);cursor:pointer;margin-bottom:3px;border:none;background:none;width:100%;text-align:right;font-family:inherit}
.ni:hover,.ni.on{background:rgba(255,215,0,.08);color:var(--g)}
.main{margin-right:200px;padding:18px 14px}
.pg{display:none}.pg.on{display:block}
.pt{font-size:20px;font-weight:900;margin-bottom:14px;background:linear-gradient(135deg,var(--g),var(--g2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.live-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.lc{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:14px;position:relative;overflow:hidden}
.lc::after{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--g),transparent);opacity:.4}
.lc .l{font-size:10px;color:var(--t2);margin-bottom:4px}.lc .v{font-size:20px;font-weight:800}.lc .s{font-size:10px;color:var(--t2);margin-top:2px}
.lc.live .v{color:var(--gn)}
.card{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:14px;margin-bottom:12px}
.card h3{font-size:13px;color:var(--g);margin-bottom:10px;display:flex;align-items:center;gap:6px}
.btn{padding:8px 14px;border-radius:10px;border:none;font-weight:700;font-size:12px;cursor:pointer;font-family:inherit}
.bg{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}
.bo{background:rgba(255,215,0,.08);color:var(--g);border:1px solid var(--b)}
.bd{background:rgba(248,113,113,.12);color:var(--rd)}
input,select{width:100%;padding:10px 12px;background:rgba(0,0,0,.3);border:1px solid var(--b);border-radius:10px;color:#fff;font-family:inherit;font-size:13px;outline:none;margin-bottom:8px}
input:focus{border-color:var(--g)}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:right;padding:8px;color:var(--t2);border-bottom:1px solid var(--b);font-size:10px}
td{padding:8px;border-bottom:1px solid rgba(255,255,255,.04)}
.tag{display:inline-block;padding:2px 8px;border-radius:8px;font-size:10px;font-weight:700}
.ton{background:rgba(52,211,153,.15);color:var(--gn)}.toff{background:rgba(248,113,113,.12);color:var(--rd)}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(60px);background:#12121f;border:1px solid var(--b);padding:10px 18px;border-radius:12px;font-size:13px;color:var(--g);opacity:0;transition:.3s;z-index:999}
.toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
.mb{display:none;position:fixed;top:0;left:0;right:0;height:48px;background:#0a0a12;border-bottom:1px solid var(--b);z-index:60;align-items:center;justify-content:space-between;padding:0 14px}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;display:none;align-items:center;justify-content:center}
.modal.on{display:flex}.modal .box{background:var(--s);border:1px solid var(--b);border-radius:16px;padding:20px;width:90%;max-width:400px}
.conn-row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.05);font-size:12px}
.pulse{display:inline-block;width:8px;height:8px;background:var(--gn);border-radius:50%;animation:p 1.5s infinite;margin-left:6px}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(52,211,153,.5)}70%{box-shadow:0 0 0 8px transparent}}
@media(max-width:768px){.sb{transform:translateX(100%)}.sb.open{transform:translateX(0)}.main{margin-right:0;padding-top:56px}.live-grid{grid-template-columns:1fr 1fr}.mb{display:flex}}
</style></head><body>
<div class="mb"><span style="font-weight:900;background:linear-gradient(135deg,#ffd700,#f7971e);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter">VROOM</span>
<button type="button" class="btn bo" id="btnSide">☰</button></div>
<aside class="sb" id="sidebar">
<div class="brand">VROOM</div>
<button type="button" class="ni on" data-p="dash">📊 داشبورد زنده</button>
<button type="button" class="ni" data-p="links">📡 اینباندها</button>
<button type="button" class="ni" data-p="conns">👥 اتصالات</button>
<button type="button" class="ni" data-p="tg">🤖 ربات تلگرام</button>
<button type="button" class="ni" data-p="domain">🌐 دامنه</button>
<button type="button" class="ni" data-p="sec">🔒 امنیت</button>
<div style="flex:1"></div>
<button type="button" class="ni" id="btnLogout" style="color:var(--rd)">خروج</button>
</aside>
<div id="sideOverlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:49"></div>
<main class="main">

<section class="pg on" id="p-dash">
<div class="pt">داشبورد زنده <span class="pulse"></span></div>
<div class="live-grid">
  <div class="lc live"><div class="l">👥 اتصالات فعال</div><div class="v" id="v-conn">0</div><div class="s">نفر آنلاین</div></div>
  <div class="lc"><div class="l">📥 دانلود</div><div class="v" id="v-dl" style="font-size:16px">0</div><div class="s">از سمت سرور</div></div>
  <div class="lc"><div class="l">📤 آپلود</div><div class="v" id="v-ul" style="font-size:16px">0</div><div class="s">از سمت کلاینت</div></div>
  <div class="lc"><div class="l">📦 کل ترافیک</div><div class="v" id="v-tot" style="font-size:16px">0</div><div class="s">مجموع</div></div>
</div>
<div class="live-grid">
  <div class="lc"><div class="l">📡 اینباندها</div><div class="v" id="v-links">0</div></div>
  <div class="lc"><div class="l">⏱️ آپتایم</div><div class="v" id="v-up" style="font-size:15px">--</div></div>
  <div class="lc"><div class="l">💻 CPU</div><div class="v" id="v-cpu">--%</div></div>
  <div class="lc"><div class="l">🧠 RAM</div><div class="v" id="v-ram">--%</div></div>
</div>
<div class="card">
  <h3>⚡ دسترسی سریع</h3>
  <div style="display:flex;gap:8px;flex-wrap:wrap">
    <button class="btn bg" onclick="qc(1)">+1 GB / 30d</button>
    <button class="btn bg" onclick="qc(5)">+5 GB / 30d</button>
    <button class="btn bg" onclick="qc(10)">+10 GB / 30d</button>
    <button class="btn bo" onclick="go('links')">اینباندها</button>
    <button class="btn bo" onclick="go('tg')">ربات تلگرام</button>
    <button class="btn bo" onclick="go('conns')">اتصالات زنده</button>
  </div>
</div>
<div class="card">
  <h3>📋 آخرین اتصالات</h3>
  <div id="dash-conns" style="font-size:12px;color:var(--t2)">در حال بارگذاری...</div>
</div>
</section>

<section class="pg" id="p-links">
<div class="pt">اینباندها <button class="btn bg" style="float:left;font-size:11px" onclick="document.getElementById('addM').classList.add('on')">+ افزودن</button></div>
<div class="card" style="overflow-x:auto">
<table><thead><tr><th>نام</th><th>مصرف</th><th>وصل</th><th>وضعیت</th><th>عملیات</th></tr></thead>
<tbody id="lb"></tbody></table>
</div>
</section>

<section class="pg" id="p-conns">
<div class="pt">اتصالات زنده <span class="pulse"></span></div>
<div class="card">
  <h3>👥 همه اتصالات فعال (<span id="conn-count">0</span>)</h3>
  <div id="conn-list">خالی</div>
</div>
</section>

<section class="pg" id="p-tg">
<div class="pt">🤖 ربات تلگرام</div>
<div class="card">
  <h3>تنظیمات (فقط دکمه — بدون تایپ دستور)</h3>
  <p style="font-size:12px;color:var(--t2);margin-bottom:10px">توکن از @BotFather | آیدی عددی از @userinfobot</p>
  <input id="tg-tok" placeholder="توکن ربات">
  <input id="tg-adm" placeholder="آیدی ادمین (چندتا با فاصله)">
  <div style="display:flex;gap:8px">
    <button class="btn bg" onclick="saveTg()">✅ ذخیره و روشن</button>
    <button class="btn bd" onclick="stopTg()">⏹ خاموش</button>
  </div>
  <div id="tg-st" style="margin-top:12px;font-size:12px;color:var(--t2)"></div>
</div>
<div class="card">
  <h3>قابلیت‌های ربات (همه با دکمه شیشه‌ای)</h3>
  <ul style="font-size:12px;color:var(--t2);line-height:2;padding-right:16px">
    <li>➕ ساخت کانفیگ با پکیج‌های آماده (دکمه)</li>
    <li>📋 لیست اینباندها + لینک ساب هر کدام</li>
    <li>📊 آمار زنده (دانلود/آپلود/اتصالات)</li>
    <li>👥 لیست اتصالات فعال</li>
    <li>🗑 حذف اینباند از ربات</li>
  </ul>
</div>
</section>

<section class="pg" id="p-domain">
<div class="pt">دامنه</div>
<div class="card">
  <input id="dom-in" placeholder="example.com">
  <button class="btn bg" onclick="saveDom()">ذخیره</button>
  <div id="dom-cur" style="margin-top:10px;font-size:12px;color:var(--t2)"></div>
</div>
</section>

<section class="pg" id="p-sec">
<div class="pt">امنیت</div>
<div class="card">
  <input type="password" id="cpw" placeholder="رمز فعلی">
  <input type="password" id="npw" placeholder="رمز جدید">
  <button class="btn bg" onclick="chPass()">تغییر رمز</button>
</div>
</section>
</main>

<div class="modal" id="addM" onclick="if(event.target===this)this.classList.remove('on')">
<div class="box">
  <h3 style="color:var(--g);margin-bottom:12px">افزودن اینباند</h3>
  <input id="nl" placeholder="نام انگلیسی">
  <div style="display:flex;gap:8px">
    <input id="nlim" type="number" placeholder="حجم" style="flex:2">
    <select id="nun"><option value="GB">GB</option><option value="MB">MB</option></select>
  </div>
  <input id="nexp" type="number" placeholder="انقضا (روز)">
  <input id="nmax" type="number" placeholder="حداکثر IP همزمان">
  <button class="btn bg" style="width:100%" onclick="createL()">ایجاد</button>
</div>
</div>
<div class="toast" id="toast"></div>

<script>
const $=s=>document.querySelector(s);
function go(id){
  document.querySelectorAll('.pg').forEach(p=>p.classList.remove('on'));
  const page=document.getElementById('p-'+id);
  if(page) page.classList.add('on');
  document.querySelectorAll('.ni').forEach(n=>n.classList.toggle('on',n.dataset.p===id));
  closeSide();
  if(id==='links') loadLinks();
  if(id==='conns') loadConns();
  if(id==='tg') loadTg();
  if(id==='domain') loadDom();
}
function openSide(){
  const sb=document.getElementById('sidebar');
  const ov=document.getElementById('sideOverlay');
  if(sb) sb.classList.add('open');
  if(ov) ov.style.display='block';
}
function closeSide(){
  const sb=document.getElementById('sidebar');
  const ov=document.getElementById('sideOverlay');
  if(sb) sb.classList.remove('open');
  if(ov) ov.style.display='none';
}
function toast(m){const t=$('#toast');if(!t)return;t.textContent=m;t.classList.add('on');setTimeout(()=>t.classList.remove('on'),2800)}

// bind nav + mobile menu
document.querySelectorAll('.ni[data-p]').forEach(el=>{
  el.addEventListener('click', function(e){ e.preventDefault(); go(el.dataset.p); });
});
const btnSide=document.getElementById('btnSide');
if(btnSide) btnSide.addEventListener('click', function(e){ e.preventDefault(); const sb=document.getElementById('sidebar'); if(sb&&sb.classList.contains('open')) closeSide(); else openSide(); });
const sideOverlay=document.getElementById('sideOverlay');
if(sideOverlay) sideOverlay.addEventListener('click', closeSide);
const btnLogout=document.getElementById('btnLogout');
if(btnLogout) btnLogout.addEventListener('click', function(){ fetch('/api/logout',{method:'POST'}).then(()=>location.href='/login'); });


async function loadStats(){
  try{
    const r=await fetch('/stats');if(!r.ok)return;const d=await r.json();
    $('#v-conn').textContent=d.active_connections;
    $('#v-dl').textContent=d.download_fmt;
    $('#v-ul').textContent=d.upload_fmt;
    $('#v-tot').textContent=d.total_fmt;
    $('#v-links').textContent=d.links_count;
    $('#v-up').textContent=d.uptime;
    $('#v-cpu').textContent=d.cpu_percent.toFixed(0)+'%';
    $('#v-ram').textContent=d.memory_percent.toFixed(0)+'%';
    $('#conn-count').textContent=d.active_connections;
    // dash conns preview
    const list=d.connection_list||[];
    $('#dash-conns').innerHTML=list.length?list.slice(0,5).map(c=>`<div class="conn-row"><span><b>${c.uuid}</b> · ${c.ip}</span><span>${c.bytes_fmt}</span></div>`).join('')+'':'هیچ اتصال فعالی نیست';
    if(document.getElementById('p-conns').classList.contains('on')){
      $('#conn-list').innerHTML=list.length?list.map(c=>`<div class="conn-row"><span><b>${c.uuid}</b><br><small style="opacity:.5">${c.ip} · از ${c.since||'?'}</small></span><span>${c.bytes_fmt}</span></div>`).join(''):'خالی';
    }
  }catch(e){}
}
async function loadLinks(){
  const r=await fetch('/api/links');const d=await r.json();
  const b=$('#lb');
  if(!d.links?.length){b.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t2)">خالی</td></tr>';return}
  b.innerHTML=d.links.map(l=>{
    const u=(l.used_bytes/1e9).toFixed(2),lim=l.limit_bytes?(l.li
The document content is too long to display in full
