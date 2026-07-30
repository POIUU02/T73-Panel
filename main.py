#!/usr/bin/env python3
"""
VROOM Panel v5 — Bilingual, Themes, Fixed Subscription for Apps, Day/Night
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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse, Response
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

connections: dict = {}
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {
    "total_bytes": 0, "download_bytes": 0, "upload_bytes": 0,
    "total_requests": 0, "total_errors": 0, "start_time": time.time(),
}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
http_client = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = ["www.speedtest.net"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()
CUSTOM_DOMAIN: str = ""
CUSTOM_DOMAIN_LOCK = asyncio.Lock()

TELEGRAM = {"token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), "admin_ids": [], "enabled": False, "offset": 0}
TELEGRAM_LOCK = asyncio.Lock()
TELEGRAM_TASK = None
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
    return (
        os.environ.get("RENDER_EXTERNAL_URL")
        or os.environ.get("RAILWAY_PUBLIC_DOMAIN")
        or "localhost"
    ).replace("https://", "").replace("http://", "").rstrip("/")


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
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"


def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def parse_size_to_bytes(value: float, unit: str) -> int:
    u = (unit or "GB").upper()
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


def fmt_bytes(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024**3:.2f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024**2:.1f} MB"
    return f"{b / 1024:.0f} KB"


async def build_sub_content(uid: str, link: dict) -> str:
    """Build plain-text subscription body (one vless per line) — what apps expect."""
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    lines = [generate_vless_link(uid, remark=f"VROOM-{link['label']}")]
    for i, addr in enumerate(addresses):
        lines.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))
    return "\n".join(lines)


# ====== TELEGRAM (button only) ======
def ikb(rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": c} for t, c in row] for row in rows]}


def main_menu_kb():
    return ikb([
        [("➕ Create Config", "create_start"), ("📋 List", "list")],
        [("📊 Stats", "stats"), ("🔗 Sub Links", "sub_menu")],
        [("ℹ️ Help", "help")],
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


async def tg_edit(chat_id, message_id, text, reply_markup=None):
    data = {
        "chat_id": chat_id, "message_id": message_id, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
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
    user_id = (cq.get("from") or {}).get("id")

    if user_id not in (TELEGRAM.get("admin_ids") or []):
        await tg_answer(cq_id, "Admin only", True)
        return
    await tg_answer(cq_id)

    if data == "menu":
        TG_STATE.pop(user_id, None)
        await tg_edit(chat_id, message_id, "🚀 <b>VROOM Bot</b>\n\nUse buttons only:", reply_markup=main_menu_kb())
        return

    if data == "help":
        await tg_edit(chat_id, message_id,
            "ℹ️ <b>Help</b>\n\nAll actions via buttons.\nCreate → name → volume → days\nList → details / delete / sub",
            reply_markup=ikb([[("🏠 Menu", "menu")]]))
        return

    if data == "stats":
        domain = get_domain()
        async with LINKS_LOCK:
            n = len(LINKS)
            active = sum(1 for x in LINKS.values() if x.get("active") and not is_expired(x))
        text = f"""📊 <b>Stats</b>

🔗 Inbounds: <code>{n}</code> (active: {active})
📡 Live connections: <code>{len(connections)}</code>
📥 Download: <code>{fmt_bytes(stats['download_bytes'])}</code>
📤 Upload: <code>{fmt_bytes(stats['upload_bytes'])}</code>
📦 Total: <code>{fmt_bytes(stats['total_bytes'])}</code>
⏱️ Uptime: <code>{uptime()}</code>
🌐 Domain: <code>{domain}</code>
💻 CPU: <code>{psutil.cpu_percent()}%</code>
🧠 RAM: <code>{psutil.virtual_memory().percent}%</code>"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([[("🔄 Refresh", "stats"), ("🏠 Menu", "menu")]]))
        return

    if data == "list":
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 Empty", reply_markup=ikb([[("➕ Create", "create_start"), ("🏠 Menu", "menu")]]))
            return
        rows = [[(f"{'✅' if d.get('active') and not is_expired(d) else '❌'} {d['label']}", f"link:{uid}")] for uid, d in items[:15]]
        rows.append([("🏠 Menu", "menu")])
        await tg_edit(chat_id, message_id, "📋 <b>Inbounds</b>", reply_markup=ikb(rows))
        return

    if data.startswith("link:"):
        uid = data[5:]
        async with LINKS_LOCK:
            link = LINKS.get(uid)
        if not link:
            await tg_edit(chat_id, message_id, "❌ Not found", reply_markup=ikb([[("📋 List", "list"), ("🏠 Menu", "menu")]]))
            return
        domain = get_domain()
        sub = f"https://{domain}/sub/{uid}"
        page = f"https://{domain}/page/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
        used = fmt_bytes(link["used_bytes"])
        lim = fmt_bytes(link["limit_bytes"]) if link["limit_bytes"] else "∞"
        text = f"""🏷 <b>{link['label']}</b>

📦 {used} / {lim}
🔌 Connections: <code>{count_connections_for_link(uid)}</code>

📥 Sub (for apps):
<code>{sub}</code>

🖥 Panel page:
<code>{page}</code>

📋 Config:
<code>{vless}</code>"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([
            [("🗑 Delete", f"delask:{uid}"), ("📋 List", "list")],
            [("🏠 Menu", "menu")],
        ]))
        return

    if data.startswith("delask:"):
        uid = data[7:]
        await tg_edit(chat_id, message_id, f"Delete «{uid}»?", reply_markup=ikb([
            [("✅ Yes", f"deldo:{uid}"), ("❌ No", f"link:{uid}")],
        ]))
        return

    if data.startswith("deldo:"):
        uid = data[6:]
        async with LINKS_LOCK:
            LINKS.pop(uid, None)
        await close_connections_for_link(uid)
        await tg_edit(chat_id, message_id, f"✅ Deleted «{uid}»", reply_markup=ikb([[("📋 List", "list"), ("🏠 Menu", "menu")]]))
        return

    if data == "sub_menu":
        async with LINKS_LOCK:
            items = list(LINKS.keys())[:12]
        if not items:
            await tg_edit(chat_id, message_id, "📭 Empty", reply_markup=ikb([[("🏠 Menu", "menu")]]))
            return
        rows = [[(uid, f"showsub:{uid}")] for uid in items]
        rows.append([("🏠 Menu", "menu")])
        await tg_edit(chat_id, message_id, "🔗 Pick inbound:", reply_markup=ikb(rows))
        return

    if data.startswith("showsub:"):
        uid = data[8:]
        domain = get_domain()
        await tg_send(chat_id, f"📥 Sub URL (import in apps):\n<code>https://{domain}/sub/{uid}</code>")
        return

    if data == "create_start":
        TG_STATE[user_id] = {"step": "label"}
        await tg_edit(chat_id, message_id, "➕ <b>Create</b>\nChoose name:", reply_markup=ikb([
            [("user1", "c_name:user1"), ("user2", "c_name:user2"), ("vip", "c_name:vip")],
            [("test", "c_name:test"), ("mobile", "c_name:mobile"), ("pc", "c_name:pc")],
            [("🎲 Random", "c_name:rand"), ("❌ Cancel", "menu")],
        ]))
        return

    if data.startswith("c_name:"):
        name = data[7:]
        if name == "rand":
            name = "u" + secrets.token_hex(3)
        if not re.match(r"^[a-zA-Z0-9\-_.]+$", name):
            await tg_edit(chat_id, message_id, "❌ Invalid name", reply_markup=ikb([[("🏠 Menu", "menu")]]))
            return
        async with LINKS_LOCK:
            if name in LINKS:
                await tg_edit(chat_id, message_id, f"❌ «{name}» exists", reply_markup=ikb([[("➕ Again", "create_start"), ("🏠 Menu", "menu")]]))
                return
        TG_STATE[user_id] = {"step": "limit", "label": name}
        await tg_edit(chat_id, message_id, f"📦 Volume for <b>{name}</b>?", reply_markup=ikb([
            [("1 GB", "c_lim:1"), ("5 GB", "c_lim:5"), ("10 GB", "c_lim:10")],
            [("20 GB", "c_lim:20"), ("50 GB", "c_lim:50"), ("100 GB", "c_lim:100")],
            [("∞ Unlimited", "c_lim:0"), ("❌ Cancel", "menu")],
        ]))
        return

    if data.startswith("c_lim:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "limit":
            await tg_edit(chat_id, message_id, "Start over", reply_markup=main_menu_kb())
            return
        lim = float(data[6:])
        st["limit"] = lim
        st["step"] = "days"
        TG_STATE[user_id] = st
        await tg_edit(chat_id, message_id, f"📅 Days for <b>{st['label']}</b>?", reply_markup=ikb([
            [("7", "c_day:7"), ("15", "c_day:15"), ("30", "c_day:30")],
            [("60", "c_day:60"), ("90", "c_day:90"), ("∞", "c_day:0")],
            [("❌ Cancel", "menu")],
        ]))
        return

    if data.startswith("c_day:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "days":
            await tg_edit(chat_id, message_id, "Start over", reply_markup=main_menu_kb())
            return
        days = float(data[6:])
        label = st["label"]
        lim = st.get("limit", 0)
        limit_bytes = parse_size_to_bytes(lim, "GB") if lim > 0 else 0
        expiry = compute_expiry(days)
        async with LINKS_LOCK:
            if label in LINKS:
                await tg_edit(chat_id, message_id, "❌ Name taken", reply_markup=ikb([[("🏠 Menu", "menu")]]))
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
        page = f"https://{domain}/page/{label}"
        vless = generate_vless_link(label, remark=f"VROOM-{label}")
        text = f"""✅ <b>Created!</b>

🏷 <code>{label}</code>
📦 <code>{lim if lim else '∞'} GB</code>
📅 <code>{int(days) if days else '∞'} days</code>

📥 Sub (apps):
<code>{sub}</code>

🖥 Page:
<code>{page}</code>

📋 Config:
<code>{vless}</code>"""
        await tg_edit(chat_id, message_id, text, reply_markup=ikb([
            [("➕ Again", "create_start"), ("📋 List", "list")],
            [("🏠 Menu", "menu")],
        ]))
        return


async def handle_tg_message(msg: dict):
    chat_id = (msg.get("chat") or {}).get("id")
    user_id = (msg.get("from") or {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return
    is_admin = user_id in (TELEGRAM.get("admin_ids") or [])
    if text in ("/start", "start", "منو", "menu"):
        if is_admin:
            await tg_send(chat_id, "🚀 <b>VROOM Bot</b>\nButtons only — no typing needed.", reply_markup=main_menu_kb())
        else:
            await tg_send(chat_id, "⛔ Admin only")
        return
    if not is_admin:
        await tg_send(chat_id, "⛔ No access")
        return
    await tg_send(chat_id, "Use buttons 👇", reply_markup=main_menu_kb())


async def telegram_poll_loop():
    logger.info("🤖 Telegram bot started")
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
    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000),
        timeout=httpx.Timeout(180.0, connect=30.0),
        follow_redirects=True,
    )
    logger.info(f"🚀 VROOM v5 on :{CONFIG['port']}")
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


# ====== BASIC API ======
@app.get("/")
async def root():
    return {"service": "VROOM", "version": "5.0", "domain": get_domain()}


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
        raise HTTPException(400, "Wrong current password")
    new = str(body.get("new_password") or "")
    if len(new) < 4:
        raise HTTPException(400, "Min 4 chars")
    AUTH["password_hash"] = hash_password(new)
    return {"ok": True}


@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    return {
        "active_connections": len(connections),
        "download_bytes": stats["download_bytes"],
        "upload_bytes": stats["upload_bytes"],
        "total_bytes": stats["total_bytes"],
        "download_fmt": fmt_bytes(stats["download_bytes"]),
        "upload_fmt": fmt_bytes(stats["upload_bytes"]),
        "total_fmt": fmt_bytes(stats["total_bytes"]),
        "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"],
        "total_errors": stats["total_errors"],
        "uptime": uptime(),
        "links_count": len(LINKS),
        "domain": get_domain(),
        "cpu_percent": psutil.cpu_percent(interval=0.05),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "hourly_traffic": dict(hourly_traffic),
        "telegram_enabled": TELEGRAM.get("enabled", False),
        "connections_detail": [
            {
                "uuid": i.get("uuid"), "ip": i.get("ip"),
                "bytes": i.get("bytes", 0), "since": i.get("connected_at"),
            }
            for i in connections.values()
        ],
    }


@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New").strip()[:60]
    if not re.match(r"^[a-zA-Z0-9\-_. ]+$", label):
        raise HTTPException(400, "English only")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(400, "Exists")
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
        "sub_url": f"https://{get_domain()}/sub/{label}",
        "page_url": f"https://{get_domain()}/page/{label}",
    }


@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    result = []
    domain = get_domain()
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
                "sub_url": f"https://{domain}/sub/{uid}",
                "page_url": f"https://{domain}/page/{uid}",
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


@app.post("/api/reset-all-usage")
async def reset_all_usage(_=Depends(require_auth)):
    async with LINKS_LOCK:
        for v in LINKS.values():
            v["used_bytes"] = 0
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


@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        return {"addresses": list(CUSTOM_ADDRESSES)}


@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address:
        raise HTTPException(400, "Required")
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
            raise HTTPException(404)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}


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


# ============================================================
# SUBSCRIPTION FOR APPS (plain text) — THIS FIXES CONFIG IMPORT
# ============================================================
@app.get("/sub/{uid}")
async def subscription_raw(uid: str, request: Request):
    """
    Returns plain-text vless links (one per line).
    This is what Hiddify / v2rayNG / Clash / etc. expect.
    Optional: ?b64=1 for base64 body
    """
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(404, "Not found")
    if not link["active"]:
        raise HTTPException(403, "Disabled")
    if is_expired(link):
        raise HTTPException(403, "Expired")

    content = await build_sub_content(uid, link)
    # subscription userinfo headers (some clients show traffic)
    used = link["used_bytes"]
    total = link["limit_bytes"] if link["limit_bytes"] > 0 else 0
    expire_ts = 0
    if link.get("expiry"):
        try:
            expire_ts = int(datetime.fromisoformat(link["expiry"]).timestamp())
        except Exception:
            pass

    headers = {
        "Content-Disposition": f'attachment; filename="{uid}.txt"',
        "Profile-Update-Interval": "6",
        "Subscription-Userinfo": f"upload=0; download={used}; total={total}; expire={expire_ts}",
    }

    if request.query_params.get("b64") in ("1", "true", "yes"):
        encoded = base64.b64encode(content.encode()).decode()
        return Response(content=encoded, media_type="text/plain; charset=utf-8", headers=headers)

    return Response(content=content + "\n", media_type="text/plain; charset=utf-8", headers=headers)


# ============================================================
# BEAUTIFUL USER PAGE (bilingual + day/night + app photos)
# ============================================================
@app.get("/page/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(404, "Not found")
    if not link["active"] or is_expired(link):
        raise HTTPException(403, "Disabled or expired")

    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / 1024 ** 3, 2)
    limit_gb = round(link["limit_bytes"] / 1024 ** 3, 2) if link["limit_bytes"] else 0
    percent = round((link["used_bytes"] / link["limit_bytes"]) * 100, 1) if link["limit_bytes"] else 0
    remaining = round(max(0, limit_gb - used_gb), 2) if limit_gb else "∞"

    if is_expired(link):
        status_fa, status_en, status_color = "منقضی", "Expired", "#f87171"
    elif link["limit_bytes"] and link["used_bytes"] >= link["limit_bytes"]:
        status_fa, status_en, status_color = "محدود", "Limited", "#fbbf24"
    else:
        status_fa, status_en, status_color = "فعال", "Active", "#34d399"

    exp = link.get("expiry")
    if exp:
        try:
            ed = datetime.fromisoformat(exp)
            days_left = max(0, (ed - datetime.now()).days)
            days_fa, days_en = f"{days_left} روز", f"{days_left} days"
            exp_disp = ed.strftime("%Y/%m/%d")
        except Exception:
            days_fa = days_en = exp_disp = "∞"
    else:
        days_fa = days_en = exp_disp = "∞"

    domain = get_domain()
    sub_url = f"https://{domain}/sub/{uid}"
    qr_data = quote(server_link, safe="")
    live_conns = count_connections_for_link(uid)

    # App icon images (brand-colored rounded — works offline as SVG, looks like app photos)
    # Using high-quality SVG "app icon" style blocks

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="dark">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>VROOM — {link['label']}</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@600;800;900&family=Vazirmatn:wght@400;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
:root {{
  --bg:#07071a; --card:rgba(18,18,36,.92); --gold:#ffd700; --gold2:#f7971e;
  --text:#f0f2f8; --muted:rgba(255,255,255,.45); --border:rgba(255,215,0,.14);
  --accent:#a78bfa; --blue:#3b82f6; --green:#34d399; --pink:#f472b6;
}}
html[data-theme="light"] {{
  --bg:#f4f6fb; --card:#ffffff; --text:#1a1a2e; --muted:rgba(0,0,0,.45);
  --border:rgba(124,92,252,.18); --gold:#c9a000; --gold2:#e67e22;
}}
*{{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}}
body{{
  font-family:'Vazirmatn','Inter',sans-serif; background:var(--bg); color:var(--text);
  min-height:100vh; display:flex; justify-content:center; padding:16px 12px;
  transition:background .4s,color .4s;
  background-image:
    radial-gradient(ellipse at 10% 0%, rgba(124,92,252,.12), transparent 50%),
    radial-gradient(ellipse at 90% 100%, rgba(255,215,0,.08), transparent 45%);
}}
html[data-theme="light"] body{{
  background-image:
    radial-gradient(ellipse at 10% 0%, rgba(124,92,252,.08), transparent 50%),
    radial-gradient(ellipse at 90% 100%, rgba(247,151,30,.06), transparent 45%);
}}
.wrap{{max-width:460px;width:100%}}
.topbar{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}}
.logo{{display:flex;align-items:center;gap:8px}}
.logo-mark{{
  width:36px;height:36px;border-radius:12px;
  background:linear-gradient(135deg,var(--gold),var(--gold2),var(--accent));
  display:flex;align-items:center;justify-content:center;
  box-shadow:0 4px 20px rgba(255,215,0,.25);
}}
.logo-mark svg{{width:20px;height:20px;stroke:#0a0a10;fill:none;stroke-width:2}}
.logo b{{font-family:Inter,sans-serif;font-size:20px;font-weight:900;
  background:linear-gradient(135deg,var(--gold),var(--accent));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.actions{{display:flex;gap:6px}}
.ibtn{{
  width:36px;height:36px;border-radius:11px;border:1px solid var(--border);
  background:var(--card); color:var(--gold); cursor:pointer;
  display:flex;align-items:center;justify-content:center;transition:.2s;
}}
.ibtn:active{{transform:scale(.92)}}
.ibtn svg{{width:18px;height:18px;stroke:currentColor;fill:none;stroke-width:1.7;stroke-linecap:round}}
.card{{
  background:var(--card); border:1px solid var(--border); border-radius:20px;
  padding:16px; margin-bottom:12px;
  box-shadow:0 8px 32px rgba(0,0,0,.15);
  backdrop-filter:blur(12px);
}}
.title{{font-size:22px;font-weight:900;font-family:Inter,sans-serif;margin-bottom:2px}}
.sub{{font-size:12px;color:var(--gold);letter-spacing:1px;margin-bottom:12px;opacity:.9}}
.live{{
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;
  padding:10px 12px; border-radius:14px; margin-bottom:12px;
  background:linear-gradient(135deg,rgba(52,211,153,.1),rgba(59,130,246,.08));
  border:1px solid rgba(52,211,153,.25);
}}
.live-l{{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:var(--green)}}
.dot{{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 0 rgba(52,211,153,.5);animation:pulse 1.6s infinite}}
@keyframes pulse{{0%{{box-shadow:0 0 0 0 rgba(52,211,153,.5)}}70%{{box-shadow:0 0 0 8px transparent}}}}
.live-r{{font-size:11px;color:var(--muted)}} .live-r b{{color:var(--text)}}
.usage{{
  display:grid; grid-template-columns:1fr 1fr 1fr auto; gap:8px; align-items:center;
}}
@media(max-width:400px){{.usage{{grid-template-columns:1fr 1fr}}.ring-w{{grid-column:1/-1;justify-self:center;margin-top:6px}}}}
.st{{text-align:center}}
.st .l{{font-size:10px;color:var(--muted);margin-bottom:3px}}
.st .v{{font-size:15px;font-weight:800}}
.ring-w{{width:74px;height:74px;position:relative;display:flex;align-items:center;justify-content:center}}
.ring-o{{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(59,130,246,.3);box-shadow:0 0 16px rgba(59,130,246,.15)}}
.ring{{
  width:64px;height:64px;border-radius:50%;
  background:conic-gradient(#3b82f6 0% 0%, rgba(30,41,59,.85) 0% 100%);
  display:flex;align-items:center;justify-content:center;position:relative;
  transition:background 1s cubic-bezier(.4,0,.2,1);
}}
.ring::before{{content:'';position:absolute;inset:6px;border-radius:50%;background:var(--bg)}}
.ring-t{{position:relative;z-index:1;text-align:center;font-size:13px;font-weight:800}}
.bar{{margin-top:12px;height:6px;background:rgba(30,41,59,.5);border-radius:10px;overflow:hidden}}
html[data-theme="light"] .bar{{background:rgba(0,0,0,.08)}}
.bar-f{{height:100%;width:0;border-radius:10px;background:linear-gradient(90deg,#3b82f6,#a78bfa,#f472b6);transition:width 1s}}
.h3{{
  font-size:11px; font-weight:700; letter-spacing:1px; text-transform:uppercase;
  color:var(--gold); opacity:.85; margin-bottom:10px;
  display:flex; align-items:center; gap:6px;
}}
.h3 svg{{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.8}}
.row{{
  background:rgba(0,0,0,.2); padding:10px 12px; border-radius:12px;
  display:flex; justify-content:space-between; align-items:center; gap:8px;
  margin-bottom:10px; font-size:11px; font-family:ui-monospace,monospace;
  color:var(--muted); border:1px solid var(--border); cursor:pointer;
}}
html[data-theme="light"] .row{{background:rgba(0,0,0,.04)}}
.row button{{
  background:linear-gradient(135deg,var(--gold),var(--gold2)); border:none;
  color:#0a0a10; padding:6px 12px; border-radius:8px; font-size:11px; font-weight:800; cursor:pointer;
  font-family:inherit;
}}
.row .lt{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.grid3{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:10px;padding-top:10px;border-top:1px solid var(--border)}}
.ii{{background:rgba(0,0,0,.15);padding:9px 6px;border-radius:12px;text-align:center;border:1px solid var(--border)}}
html[data-theme="light"] .ii{{background:rgba(0,0,0,.03)}}
.ii .l{{font-size:9px;opacity:.45;display:block;margin-bottom:2px}}
.ii .v{{font-size:13px;font-weight:800}}
.cfg{{
  background:rgba(0,0,0,.25); padding:10px; border-radius:12px; font-size:10px;
  font-family:ui-monospace,monospace; word-break:break-all; margin-bottom:10px;
  max-height:56px; overflow-y:auto; border:1px solid var(--border);
  direction:ltr; text-align:left; color:var(--muted); cursor:pointer;
}}
html[data-theme="light"] .cfg{{background:rgba(0,0,0,.04)}}
.qr-wrap{{text-align:center}}
.qr{{
  width:120px;height:120px;background:#fff;border-radius:14px;margin:0 auto 10px;
  overflow:hidden;border:2px solid rgba(255,215,0,.3);cursor:pointer;
  box-shadow:0 8px 24px rgba(0,0,0,.2);
}}
.qr img{{width:100%;height:100%}}
.btns{{display:flex;gap:8px}}
.btns button{{
  flex:1; padding:12px; border:none; border-radius:12px; font-weight:800; font-size:13px;
  cursor:pointer; font-family:inherit;
}}
.b1{{background:linear-gradient(135deg,var(--gold),var(--gold2),var(--pink)); color:#0a0a10;
  box-shadow:0 4px 20px rgba(255,215,0,.25)}}
.b2{{background:rgba(255,215,0,.08); color:var(--gold); border:1px solid var(--border)}}
.apps{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}
@media(max-width:380px){{.apps{{grid-template-columns:repeat(2,1fr)}}}}
.app{{
  background:rgba(255,255,255,.04); border:1px solid var(--border); border-radius:16px;
  padding:12px 6px 10px; text-align:center; cursor:pointer; transition:.2s; position:relative;
}}
html[data-theme="light"] .app{{background:rgba(0,0,0,.02)}}
.app:active{{transform:scale(.96)}}
.app-img{{
  width:48px;height:48px;margin:0 auto 6px;border-radius:14px;overflow:hidden;
  box-shadow:0 4px 14px rgba(0,0,0,.25); display:block;
}}
.app-img img,.app-img svg{{width:100%;height:100%;display:block}}
.app-name{{font-size:10px;font-weight:600;color:var(--muted)}}
.badge{{position:absolute;top:6px;right:6px;font-size:9px;background:rgba(255,215,0,.2);color:var(--gold);padding:1px 5px;border-radius:6px;font-weight:700}}
.chips{{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}}
.chip{{padding:4px 10px;border-radius:20px;font-size:10px;border:1px solid var(--border);color:var(--muted);font-weight:600}}
footer{{text-align:center;font-size:11px;color:var(--muted);margin-top:8px;padding-top:10px;border-top:1px solid var(--border)}}
footer b{{background:linear-gradient(135deg,var(--gold),var(--accent));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{
  position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);
  background:var(--card); padding:12px 22px; border-radius:12px; font-size:13px;
  color:var(--gold); opacity:0; transition:.35s; border:1px solid var(--border);
  z-index:9999; font-weight:700; box-shadow:0 8px 32px rgba(0,0,0,.3);
}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.theme-bar{{
  display:flex; gap:8px; justify-content:center; margin-bottom:12px; flex-wrap:wrap;
}}
.tbtn{{
  width:28px;height:28px;border-radius:50%; border:2px solid transparent; cursor:pointer;
  transition:.2s;
}}
.tbtn.on{{border-color:var(--gold); transform:scale(1.12); box-shadow:0 0 12px rgba(255,215,0,.4)}}
.t-space{{background:radial-gradient(#0d1b2a,#000)}}
.t-ocean{{background:linear-gradient(135deg,#1a2980,#26d0ce)}}
.t-sunset{{background:linear-gradient(135deg,#f12711,#f5af19)}}
.t-neon{{background:linear-gradient(135deg,#1d1d2e,#ff00cc)}}
.t-forest{{background:linear-gradient(135deg,#134e5e,#71b280)}}
.lang-pill{{
  display:inline-flex; gap:0; border-radius:20px; border:1px solid var(--border);
  overflow:hidden; font-size:11px; font-weight:700;
}}
.lang-pill button{{
  border:none; padding:6px 12px; cursor:pointer; background:transparent; color:var(--muted);
  font-family:inherit; font-weight:700;
}}
.lang-pill button.on{{background:linear-gradient(135deg,var(--gold),var(--gold2)); color:#0a0a10}}
</style>
</head>
<body>
<div class="wrap">

<div class="topbar">
  <div class="logo">
    <div class="logo-mark"><svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg></div>
    <b>VROOM</b>
  </div>
  <div class="actions">
    <div class="lang-pill">
      <button type="button" id="langFa" class="on" onclick="setLang('fa')">FA</button>
      <button type="button" id="langEn" onclick="setLang('en')">EN</button>
    </div>
    <button class="ibtn" id="themeToggle" onclick="toggleDayNight()" title="Day/Night">
      <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
    </button>
  </div>
</div>

<div class="theme-bar">
  <button class="tbtn t-space on" data-t="space" onclick="setTheme('space',this)"></button>
  <button class="tbtn t-ocean" data-t="ocean" onclick="setTheme('ocean',this)"></button>
  <button class="tbtn t-sunset" data-t="sunset" onclick="setTheme('sunset',this)"></button>
  <button class="tbtn t-neon" data-t="neon" onclick="setTheme('neon',this)"></button>
  <button class="tbtn t-forest" data-t="forest" onclick="setTheme('forest',this)"></button>
</div>

<div class="card">
  <div class="title" data-i18n="title">Subscription</div>
  <div class="sub">✦ {link['label']} ✦</div>

  <div class="live">
    <div class="live-l"><div class="dot"></div><span data-i18n="online">سرور آنلاین</span></div>
    <div class="live-r"><span data-i18n="conn">اتصالات</span>: <b>{live_conns}</b></div>
  </div>

  <div class="usage">
    <div class="st"><div class="l" data-i18n="used">مصرفی</div><div class="v">{used_gb} GB</div></div>
    <div class="st"><div class="l" data-i18n="status">وضعیت</div><div class="v" style="color:{status_color};font-size:13px" id="statusTxt">{status_fa}</div></div>
    <div class="st"><div class="l" data-i18n="left">باقی</div><div class="v">{remaining}{' GB' if remaining!='∞' else ''}</div></div>
    <div class="ring-w"><div class="ring-o"></div><div class="ring" id="ring"><div class="ring-t" id="pct">0%</div></div></div>
  </div>
  <div class="bar"><div class="bar-f" id="bar"></div></div>
</div>

<div class="card">
  <div class="h3">
    <svg viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg>
    <span data-i18n="subLink">لینک ساب (برای برنامه‌ها)</span>
  </div>
  <div class="row" onclick="cp(SUB,'OK')">
    <span class="lt">{sub_url}</span>
    <button type="button" onclick="event.stopPropagation();cp(SUB,'OK')" data-i18n="copy">کپی</button>
  </div>
  <div class="grid3">
    <div class="ii"><span class="l" data-i18n="status">وضعیت</span><span class="v" style="color:{status_color}" id="st2">{status_fa}</span></div>
    <div class="ii"><span class="l" data-i18n="expire">انقضا</span><span class="v" style="color:#fbbf24">{exp_disp}</span></div>
    <div class="ii"><span class="l" data-i18n="days">باقی</span><span class="v" style="color:#6bcbff" id="daysTxt">{days_fa}</span></div>
  </div>
</div>

<div class="card">
  <div class="h3">
    <svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M7 7h.01M17 7h.01M7 17h.01M17 17h.01M12 12h.01"/></svg>
    <span data-i18n="configQr">کانفیگ و QR</span>
  </div>
  <div class="cfg" onclick="cp(CFG,'OK')">{server_link}</div>
  <div class="qr-wrap">
    <div class="qr" onclick="document.getElementById('qrm').style.display='flex'">
      <img src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={qr_data}" alt="QR"/>
    </div>
    <div class="btns">
      <button type="button" class="b1" onclick="cp(SUB,'OK')" data-i18n="add">＋ اضافه کردن</button>
      <button type="button" class="b2" onclick="share()" data-i18n="share">اشتراک</button>
    </div>
  </div>
</div>

<div class="card">
  <div class="h3">
    <svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
    <span data-i18n="apps">برنامه‌ها</span>
  </div>
  <div class="apps">
    <div class="app" onclick="oa('hiddify')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><defs><linearGradient id="g1" x1="0" y1="0" x2="1" y2="1"><stop stop-color="#5B7CFF"/><stop offset="1" stop-color="#3D5AFE"/></linearGradient></defs><rect width="48" height="48" rx="12" fill="url(#g1)"/><path d="M14 34V14h9c4.2 0 6.8 2.3 6.8 5.8 0 2.3-1.2 4.1-3.3 4.9L33 34h-5.8l-4.8-8.1H19V34h-5zm5-12.2h3c1.9 0 3-.9 3-2.3s-1.1-2.3-3-2.3H19v4.6z" fill="#fff"/></svg></div>
      <div class="app-name">Hiddify</div>
    </div>
    <div class="app" onclick="oa('v2rayng')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#1E88E5"/><path d="M24 10L12 18v12l12 8 12-8V18L24 10zm0 5.2l7.2 4.8v7.2L24 32l-7.2-4.8v-7.2L24 15.2z" fill="#fff"/><circle cx="24" cy="24" r="3.2" fill="#fff"/></svg></div>
      <div class="app-name">v2rayNG</div>
    </div>
    <div class="app" onclick="oa('v2box')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#6C5CE7"/><text x="24" y="31" text-anchor="middle" fill="#fff" font-size="15" font-weight="800" font-family="Arial">V2</text></svg></div>
      <div class="app-name">V2Box</div>
    </div>
    <div class="app" onclick="oa('singbox')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#00B894"/><path d="M14 15h20v3.6H14V15zm0 7.5h20v3.6H14V22.5zm0 7.5h14v3.6H14V30z" fill="#fff"/></svg></div>
      <div class="app-name">Sing-box</div>
    </div>
    <div class="app" onclick="oa('shadowrocket')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#E84393"/><path d="M24 9l-2.4 9.2H13l6.6 4.8-2.5 9.2 7.3-5.3 7.3 5.3-2.5-9.2 6.6-4.8h-8.6L24 9z" fill="#fff"/></svg></div>
      <div class="app-name">Shadowrocket</div>
    </div>
    <div class="app" onclick="oa('clash')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#D63031"/><circle cx="24" cy="24" r="10" fill="none" stroke="#fff" stroke-width="3.2"/><circle cx="24" cy="24" r="4.5" fill="#fff"/></svg></div>
      <div class="app-name">Clash</div>
    </div>
    <div class="app" onclick="oa('streisand')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#FF6B6B"/><path d="M16 15h16v3.4H16V15zm0 7.2h16v3.4H16V22.2zm0 7.2h12v3.4H16V29.4z" fill="#fff"/></svg></div>
      <div class="app-name">Streisand</div>
    </div>
    <div class="app" onclick="oa('nekoray')"><span class="badge">+</span>
      <div class="app-img"><svg viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#F39C12"/><circle cx="24" cy="18" r="7" fill="#fff"/><path d="M12 36c0-5.5 5.4-10 12-10s12 4.5 12 10H12z" fill="#fff"/></svg></div>
      <div class="app-name">NekoRay</div>
    </div>
  </div>
  <div class="chips">
    <span class="chip">Android</span><span class="chip">iOS</span>
    <span class="chip">Windows</span><span class="chip">macOS</span><span class="chip">Linux</span>
  </div>
</div>

<footer>Powered by <b>VROOM</b></footer>
</div>

<div id="qrm" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.88);align-items:center;justify-content:center;z-index:10000" onclick="this.style.display='none'">
  <img style="width:min(80vw,300px);border-radius:16px;border:2px solid rgba(255,215,0,.4)" src="https://api.qrserver.com/v1/create-qr-code/?size=360x360&data={qr_data}" alt="QR"/>
</div>
<div class="toast" id="toast"></div>

<script>
const SUB = '{sub_url}';
const CFG = `{server_link}`;
const P = {percent};
const STATUS = {{fa:'{status_fa}', en:'{status_en}'}};
const DAYS = {{fa:'{days_fa}', en:'{days_en}'}};

const I18N = {{
  fa: {{
    title:'Subscription', online:'سرور آنلاین', conn:'اتصالات', used:'مصرفی', status:'وضعیت',
    left:'باقی', subLink:'لینک ساب (برای برنامه‌ها)', copy:'کپی', expire:'انقضا', days:'باقی',
    configQr:'کانفیگ و QR', add:'＋ اضافه کردن', share:'اشتراک', apps:'برنامه‌ها'
  }},
  en: {{
    title:'Subscription', online:'Server Online', conn:'Connections', used:'Used', status:'Status',
    left:'Left', subLink:'Subscription link (for apps)', copy:'Copy', expire:'Expiry', days:'Left',
    configQr:'Config & QR', add:'＋ Add', share:'Share', apps:'Apps'
  }}
}};

let lang = localStorage.getItem('vroom_lang') || 'fa';
let dayNight = localStorage.getItem('vroom_dn') || 'dark';

function setLang(l) {{
  lang = l;
  localStorage.setItem('vroom_lang', l);
  document.documentElement.lang = l;
  document.documentElement.dir = l === 'fa' ? 'rtl' : 'ltr';
  document.getElementById('langFa').classList.toggle('on', l==='fa');
  document.getElementById('langEn').classList.toggle('on', l==='en');
  const t = I18N[l];
  document.querySelectorAll('[data-i18n]').forEach(el => {{
    const k = el.getAttribute('data-i18n');
    if (t[k]) el.textContent = t[k];
  }});
  document.getElementById('statusTxt').textContent = STATUS[l];
  document.getElementById('st2').textContent = STATUS[l];
  document.getElementById('daysTxt').textContent = DAYS[l];
}}

function toggleDayNight() {{
  dayNight = dayNight === 'dark' ? 'light' : 'dark';
  localStorage.setItem('vroom_dn', dayNight);
  document.documentElement.setAttribute('data-theme', dayNight);
}}

function setTheme(name, btn) {{
  document.querySelectorAll('.tbtn').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  const map = {{
    space: 'radial-gradient(ellipse at bottom, #0d1b2a 0%, #05050c 100%)',
    ocean: 'linear-gradient(135deg, #0a1628 0%, #0d3b4c 100%)',
    sunset: 'linear-gradient(135deg, #1a0a0a 0%, #3d1508 100%)',
    neon: 'linear-gradient(135deg, #12081a 0%, #2a0a2e 100%)',
    forest: 'linear-gradient(135deg, #061410 0%, #0a2a1c 100%)',
  }};
  if (dayNight === 'dark') document.body.style.backgroundImage = 'none';
  document.body.style.background = map[name] || map.space;
  localStorage.setItem('vroom_theme_bg', name);
}}

const apps = {{
  hiddify: {{s:'hiddify://import/'+encodeURIComponent(SUB), d:'https://github.com/hiddify/hiddify-app/releases/latest'}},
  v2rayng: {{s:'v2rayng://install-config?url='+encodeURIComponent(SUB), d:'https://github.com/2dust/v2rayNG/releases/latest'}},
  v2box: {{s:'v2box://install-config?url='+encodeURIComponent(SUB), d:'https://apps.apple.com/app/v2box-v2ray-client/id6446814690'}},
  singbox: {{s:'sing-box://import-remote-profile?url='+encodeURIComponent(SUB), d:'https://github.com/SagerNet/sing-box/releases/latest'}},
  shadowrocket: {{s:'shadowrocket://add/sub://'+btoa(SUB).replace(/\\+/g,'-').replace(/\\//g,'_'), d:'https://apps.apple.com/app/shadowrocket/id932747118'}},
  clash: {{s:'clash://install-config?url='+encodeURIComponent(SUB), d:'https://github.com/MetaCubeX/ClashMetaForAndroid/releases/latest'}},
  streisand: {{s:'streisand://import/'+encodeURIComponent(SUB), d:'https://apps.apple.com/app/streisand/id6450534064'}},
  nekoray: {{s:'', d:'https://github.com/MatsuriDayo/nekoray/releases/latest'}}
}};

function oa(n) {{
  const a = apps[n]; if (!a) return;
  if (a.s) {{
    const t = Date.now(); location.href = a.s;
    setTimeout(() => {{ if (Date.now()-t < 1600) {{ toast('App not found → download'); setTimeout(()=>open(a.d,'_blank'),600); }} }}, 1400);
  }} else open(a.d, '_blank');
}}

function cp(t, m) {{
  if (navigator.clipboard) navigator.clipboard.writeText(t).then(()=>toast(lang==='fa'?'کپی شد':'Copied'));
  else {{ const i=document.createElement('input'); i.value=t; document.body.appendChild(i); i.select(); document.execCommand('copy'); document.body.removeChild(i); toast(lang==='fa'?'کپی شد':'Copied'); }}
}}
function share() {{
  if (navigator.share) navigator.share({{title:'VROOM', url:SUB}}).catch(()=>cp(SUB));
  else cp(SUB);
}}
function toast(m) {{
  const t=document.getElementById('toast'); t.textContent=m; t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'), 2500);
}}

document.documentElement.setAttribute('data-theme', dayNight);
setLang(lang);
const savedTheme = localStorage.getItem('vroom_theme_bg');
if (savedTheme) {{
  const btn = document.querySelector('.tbtn[data-t="'+savedTheme+'"]');
  if (btn) setTheme(savedTheme, btn);
}}
setTimeout(() => {{
  document.getElementById('ring').style.background = `conic-gradient(#3b82f6 0% ${{P}}%, rgba(30,41,59,.85) ${{P}}% 100%)`;
  document.getElementById('bar').style.width = P + '%';
  document.getElementById('pct').textContent = P + '%';
}}, 280);
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ====== WEBSOCKET PROXY ======
RELAY_BUF = 2 * 1024 * 1024


async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("small")
    pos = 1 + 16
    addon_len = first_chunk[pos]
    pos += 1 + addon_len
    pos += 1
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


async def add_usage(uid: str, n: int, direction: str = "total"):
    async with LINKS_LOCK:
        if uid in LINKS:
            LINKS[uid]["used_bytes"] += n
    stats["total_bytes"] += n
    if direction == "up":
        stats["upload_bytes"] += n
    elif direction == "down":
        stats["download_bytes"] += n


async def ws_to_tcp(ws, writer, conn_id, uid):
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


async def tcp_to_ws(ws, reader, conn_id, uid):
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
LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VROOM Login</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;700;900&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;
background:#07071a;color:#f0f2f8;direction:rtl;
background-image:radial-gradient(ellipse at 20% 20%,rgba(124,92,252,.15),transparent 50%),radial-gradient(ellipse at 80% 80%,rgba(255,215,0,.08),transparent 45%)}
.card{background:rgba(18,18,36,.95);border:1px solid rgba(255,215,0,.14);border-radius:24px;padding:40px 32px;width:100%;max-width:380px;box-shadow:0 20px 60px rgba(0,0,0,.4)}
.brand{text-align:center;margin-bottom:28px}
.brand h1{font-size:28px;font-weight:900;background:linear-gradient(135deg,#ffd700,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.brand p{font-size:11px;color:rgba(255,255,255,.3);margin-top:4px;letter-spacing:2px}
input{width:100%;padding:12px 14px;background:rgba(0,0,0,.35);border:1px solid rgba(255,215,0,.14);border-radius:12px;color:#fff;font-size:14px;font-family:inherit;outline:none;margin-bottom:14px}
input:focus{border-color:#ffd700}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#ffd700,#f7971e);border:none;border-radius:12px;color:#0a0a10;font-size:15px;font-weight:800;cursor:pointer;font-family:inherit}
.err{background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:#f87171;padding:10px;border-radius:10px;font-size:13px;display:none;margin-bottom:12px;text-align:center}
.err.show{display:block}
</style></head><body>
<div class="card"><div class="brand"><h1>VROOM</h1><p>PANEL LOGIN</p></div>
<div class="err" id="err"></div>
<form id="f"><input type="password" id="pw" placeholder="Admin password" autofocus>
<button class="btn" type="submit">Login / ورود</button></form></div>
<script>
document.getElementById('f').onsubmit=async e=>{
e.preventDefault();const err=document.getElementById('err');err.classList.remove('show');
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
if(!r.ok)throw new Error('Wrong password');location.href='/dashboard'}catch(ex){err.textContent=ex.message;err.classList.add('show')}
}</script></body></html>"""


# ====== DASHBOARD (bilingual) ======
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="dark"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800;900&family=Inter:wght@800&display=swap" rel="stylesheet">
<style>
:root{--bg:#07071a;--s:#121224;--g:#ffd700;--g2:#f7971e;--t:#f0f2f8;--t2:rgba(255,255,255,.5);--b:rgba(255,215,0,.12);--gn:#34d399;--rd:#f87171;--ac:#a78bfa}
html[data-theme=light]{--bg:#f4f6fb;--s:#fff;--t:#1a1a2e;--t2:rgba(0,0,0,.5);--b:rgba(124,92,252,.15);--g:#c9a000}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;direction:rtl;
background-image:radial-gradient(ellipse at 15% 0%,rgba(124,92,252,.1),transparent 50%),radial-gradient(ellipse at 85% 100%,rgba(255,215,0,.06),transparent 45%)}
.side{width:200px;background:#0a0a14;border-left:1px solid var(--b);position:fixed;right:0;top:0;bottom:0;padding:14px 8px;display:flex;flex-direction:column;z-index:40}
.brand{font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter,sans-serif;padding:8px;margin-bottom:12px}
.ni{padding:9px 11px;border-radius:10px;font-size:12px;font-weight:600;color:var(--t2);cursor:pointer;margin-bottom:3px;border:none;background:none;width:100%;text-align:right;font-family:inherit}
.ni:hover,.ni.on{background:rgba(255,215,0,.08);color:var(--g)}
.main{margin-right:200px;padding:18px 14px}
.page{display:none}.page.on{display:block}
.pt{font-size:18px;font-weight:900;margin-bottom:14px;background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}
.st{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:12px;box-shadow:0 4px 16px rgba(0,0,0,.1)}
.st .l{font-size:9px;color:var(--t2)}.st .v{font-size:17px;font-weight:800}
.card{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:14px;margin-bottom:10px}
.card h3{font-size:12px;color:var(--g);margin-bottom:10px}
.btn{padding:7px 12px;border-radius:9px;border:none;font-weight:700;font-size:11px;cursor:pointer;font-family:inherit}
.bg{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}
.bo{background:rgba(255,215,0,.08);color:var(--g);border:1px solid var(--b)}
.bd{background:rgba(248,113,113,.12);color:var(--rd)}
input,select{width:100%;padding:9px 11px;background:rgba(0,0,0,.25);border:1px solid var(--b);border-radius:9px;color:var(--t);font-family:inherit;font-size:12px;outline:none;margin-bottom:7px}
input:focus{border-color:var(--g)}
table{width:100%;border-collapse:collapse;font-size:11px}th{text-align:right;padding:7px;color:var(--t2);border-bottom:1px solid var(--b);font-size:9px}td{padding:7px;border-bottom:1px solid rgba(255,255,255,.04)}
.tag{display:inline-block;padding:2px 7px;border-radius:7px;font-size:9px;font-weight:700}.ton{background:rgba(52,211,153,.15);color:var(--gn)}.toff{background:rgba(248,113,113,.12);color:var(--rd)}
.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%) translateY(50px);background:var(--s);border:1px solid var(--b);padding:9px 18px;border-radius:11px;font-size:12px;color:var(--g);opacity:0;transition:.3s;z-index:999}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mob{display:none;position:fixed;top:0;left:0;right:0;height:46px;background:#0a0a14;border-bottom:1px solid var(--b);z-index:50;align-items:center;justify-content:space-between;padding:0 12px}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:80;display:none;align-items:center;justify-content:center}.modal-bg.show{display:flex}
.modal{background:var(--s);border:1px solid var(--b);border-radius:16px;padding:18px;width:92%;max-width:400px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.sys{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.sys .box{background:rgba(0,0,0,.2);border-radius:10px;padding:10px;text-align:center;border:1px solid var(--b)}.sys .box .v{font-size:15px;font-weight:800}.sys .box .l{font-size:9px;color:var(--t2)}
.pulse{display:inline-block;width:8px;height:8px;background:var(--gn);border-radius:50%;animation:p 1.5s infinite;margin:0 4px}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(52,211,153,.5)}70%{box-shadow:0 0 0 8px transparent}}
.lang{display:inline-flex;border:1px solid var(--b);border-radius:16px;overflow:hidden;font-size:10px;font-weight:700;margin-bottom:10px}
.lang button{border:none;padding:5px 10px;background:transparent;color:var(--t2);cursor:pointer;font-family:inherit;font-weight:700}
.lang button.on{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}
@media(max-width:768px){.side{transform:translateX(100%)}.side.open{transform:translateX(0)}.main{margin-right:0;padding-top:56px}.stats{grid-template-columns:1fr 1fr}.mob{display:flex}.sys{grid-template-columns:1fr 1fr}}
</style></head>
<body>
<div class="mob"><span style="font-weight:900;background:linear-gradient(135deg,#ffd700,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent">VROOM</span>
<button class="btn bo" onclick="document.querySelector('.side').classList.toggle('open')">☰</button></div>
<aside class="side">
  <div class="brand">VROOM</div>
  <div class="lang"><button type="button" id="lFa" class="on" onclick="setL('fa')">FA</button><button type="button" id="lEn" onclick="setL('en')">EN</button></div>
  <button class="ni on" data-p="dash" data-fa="داشبورد زنده" data-en="Live Dashboard">📊 داشبورد زنده</button>
  <button class="ni" data-p="links" data-fa="اینباندها" data-en="Inbounds">📡 اینباندها</button>
  <button class="ni" data-p="conn" data-fa="اتصالات زنده" data-en="Live Connections">🔗 اتصالات زنده</button>
  <button class="ni" data-p="addr" data-fa="آی‌پی تمیز" data-en="Clean IP">🌐 آی‌پی تمیز</button>
  <button class="ni" data-p="tg" data-fa="ربات تلگرام" data-en="Telegram Bot">🤖 ربات تلگرام</button>
  <button class="ni" data-p="domain" data-fa="دامنه" data-en="Domain">🌍 دامنه</button>
  <button class="ni" data-p="sec" data-fa="امنیت" data-en="Security">🔒 امنیت</button>
  <div style="flex:1"></div>
  <button class="ni" style="color:var(--rd)" onclick="fetch('/api/logout',{method:'POST'}).then(()=>location='/login')" data-fa="خروج" data-en="Logout">خروج</button>
</aside>
<main class="main">
<section class="page on" id="p-dash">
  <div class="pt">داشبورد <span class="pulse"></span></div>
  <div class="stats">
    <div class="st"><div class="l">👥 Connections</div><div class="v" id="s-cn">0</div></div>
    <div class="st"><div class="l">📥 Download</div><div class="v" id="s-dl" style="font-size:14px">0</div></div>
    <div class="st"><div class="l">📤 Upload</div><div class="v" id="s-ul" style="font-size:14px">0</div></div>
    <div class="st"><div class="l">📦 Total</div><div class="v" id="s-tr" style="font-size:14px">0</div></div>
  </div>
  <div class="stats">
    <div class="st"><div class="l">📡 Inbounds</div><div class="v" id="s-lk">0</div></div>
    <div class="st"><div class="l">⏱️ Uptime</div><div class="v" id="s-up" style="font-size:13px">--</div></div>
    <div class="st"><div class="l">💻 CPU</div><div class="v" id="s-cpu">--</div></div>
    <div class="st"><div class="l">🧠 RAM</div><div class="v" id="s-ram">--</div></div>
  </div>
  <div class="card"><h3>⚡ Quick create</h3>
    <div style="display:flex;gap:6px;flex-wrap:wrap">
      <button class="btn bg" onclick="qc(1)">+1GB / 30d</button>
      <button class="btn bg" onclick="qc(5)">+5GB / 30d</button>
      <button class="btn bg" onclick="qc(10)">+10GB / 30d</button>
      <button class="btn bo" onclick="resetAll()">Reset usage</button>
    </div>
  </div>
</section>

<section class="page" id="p-links">
  <div class="pt">Inbounds <button class="btn bg" style="float:left" onclick="$('#addM').classList.add('show')">+ Add</button></div>
  <div class="card" style="overflow-x:auto"><table><thead><tr><th>Name</th><th>Usage</th><th>IP</th><th>Status</th><th>Actions</th></tr></thead><tbody id="lb"></tbody></table></div>
  <p style="font-size:11px;color:var(--t2);margin-top:8px">📌 <b>Sub</b> = for apps (Hiddify…) · <b>Page</b> = beautiful user panel</p>
</section>

<section class="page" id="p-conn">
  <div class="pt">Live connections <span class="pulse"></span></div>
  <div class="card"><table><thead><tr><th>Inbound</th><th>IP</th><th>Traffic</th><th>Since</th></tr></thead><tbody id="cb"></tbody></table></div>
</section>

<section class="page" id="p-addr">
  <div class="pt">Clean IP</div>
  <div class="card">
    <div class="grid2"><input id="new-addr" placeholder="IP or domain"><button class="btn bg" onclick="addAddr()">Add</button></div>
    <div id="alist" style="margin-top:10px"></div>
  </div>
</section>

<section class="page" id="p-tg">
  <div class="pt">Telegram Bot</div>
  <div class="card">
    <p style="font-size:11px;color:var(--t2);margin-bottom:10px">Token from @BotFather · Numeric ID from @userinfobot<br>Fully button-based bot.</p>
    <input id="tg-tok" placeholder="Bot token">
    <input id="tg-adm" placeholder="Admin ID(s)">
    <div style="display:flex;gap:6px"><button class="btn bg" onclick="saveTg()">Enable</button><button class="btn bd" onclick="stopTg()">Stop</button></div>
    <div id="tg-st" style="margin-top:10px;font-size:12px;color:var(--t2)"></div>
  </div>
</section>

<section class="page" id="p-domain">
  <div class="pt">Domain</div>
  <div class="card"><input id="dom-in" placeholder="example.com"><button class="btn bg" onclick="saveDom()">Save</button><div id="dom-cur" style="margin-top:8px;font-size:12px;color:var(--t2)"></div></div>
</section>

<section class="page" id="p-sec">
  <div class="pt">Security</div>
  <div class="card"><input type="password" id="cpw" placeholder="Current password"><input type="password" id="npw" placeholder="New password"><button class="btn bg" onclick="chPass()">Change</button></div>
</section>
</main>

<div class="modal-bg" id="addM" onclick="if(event.target===this)this.classList.remove('show')">
  <div class="modal">
    <h3 style="color:var(--g);margin-bottom:10px">Add inbound</h3>
    <input id="nl" placeholder="English name">
    <div class="grid2"><input id="nlim" type="number" placeholder="Volume"><select id="nun"><option>GB</option><option>MB</option></select></div>
    <input id="nexp" type="number" placeholder="Expiry days">
    <input id="nmax" type="number" placeholder="Max IPs">
    <button class="btn bg" style="width:100%" onclick="createL()">Create</button>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
const $=s=>document.querySelector(s);
let LANG=localStorage.getItem('vroom_dash_lang')||'fa';
function setL(l){
  LANG=l; localStorage.setItem('vroom_dash_lang',l);
  document.documentElement.lang=l; document.documentElement.dir=l==='fa'?'rtl':'ltr';
  $('#lFa').classList.toggle('on',l==='fa'); $('#lEn').classList.toggle('on',l==='en');
  document.querySelectorAll('.ni[data-fa]').forEach(el=>{ el.textContent = (el.dataset.p? (el.textContent.match(/^[\u{1F300}-\u{1F9FF}]|[\u2600-\u26FF]/u)||[''])[0]+' ' : '') + (l==='fa'?el.dataset.fa:el.dataset.en); });
}
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
  $('#s-cn').textContent=d.active_connections;
  $('#s-dl').textContent=d.download_fmt||'0';
  $('#s-ul').textContent=d.upload_fmt||'0';
  $('#s-tr').textContent=d.total_fmt||(d.total_traffic_mb+' MB');
  $('#s-lk').textContent=d.links_count;
  $('#s-up').textContent=d.uptime;
  $('#s-cpu').textContent=(d.cpu_percent||0).toFixed(0)+'%';
  $('#s-ram').textContent=(d.memory_percent||0).toFixed(0)+'%';
  window._conns=d.connections_detail||[];
  }catch(e){}
}
async function loadL(){
  const r=await fetch('/api/links');const d=await r.json();const b=$('#lb');
  if(!d.links?.length){b.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t2)">Empty</td></tr>';return}
  b.innerHTML=d.links.map(l=>{
    const u=(l.used_bytes/1e9).toFixed(2),lim=l.limit_bytes?(l.limit_bytes/1e9).toFixed(1)+'G':'∞';
    const sub=l.sub_url||(location.origin+'/sub/'+l.uuid);
    const page=l.page_url||(location.origin+'/page/'+l.uuid);
    return `<tr><td><b>${l.label}</b></td><td>${u}/${lim}</td><td>${l.current_connections}/${l.max_connections||'∞'}</td>
    <td><span class="tag ${l.active&&!l.expired?'ton':'toff'}">${l.active&&!l.expired?'ON':'OFF'}</span></td>
    <td style="display:flex;gap:3px;flex-wrap:wrap">
      <button class="btn bo" style="padding:3px 7px;font-size:9px" onclick="navigator.clipboard.writeText('${sub}').then(()=>toast('Sub URL'))">Sub</button>
      <button class="btn bo" style="padding:3px 7px;font-size:9px" onclick="navigator.clipboard.writeText('${page}').then(()=>toast('Page URL'))">Page</button>
      <button class="btn bo" style="padding:3px 7px;font-size:9px" onclick="navigator.clipboard.writeText('${l.vless_link.replace(/'/g,"\\'")}').then(()=>toast('Config'))">Copy</button>
      <button class="btn bd" style="padding:3px 7px;font-size:9px" onclick="delL('${l.uuid}')">Del</button>
    </td></tr>`}).join('');
}
function loadC(){
  const list=window._conns||[];const b=$('#cb');
  if(!list.length){b.innerHTML='<tr><td colspan="4" style="text-align:center;color:var(--t2)">None</td></tr>';return}
  b.innerHTML=list.map(c=>`<tr><td>${c.uuid}</td><td>${c.ip}</td><td>${(c.bytes/1e6).toFixed(2)} MB</td><td style="font-size:10px">${(c.since||'').slice(11,19)}</td></tr>`).join('');
}
async function delL(u){if(!confirm('Delete?'))return;await fetch('/api/links/'+u,{method:'DELETE'});toast('Deleted');loadL();loadS()}
async function createL(){
  const label=$('#nl').value.trim(),limit=parseFloat($('#nlim').value)||0,unit=$('#nun').value,expiry=parseFloat($('#nexp').value)||0,max=parseInt($('#nmax').value)||0;
  if(!label){toast('Name required');return}
  const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,limit_value:limit,limit_unit:unit,expiry_days:expiry,max_connections:max})});
  if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'Error');return}
  toast('Created');$('#addM').classList.remove('show');loadL();loadS();
}
async function qc(gb){const n='u'+Math.floor(Math.random()*900+100);await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:n,limit_value:gb,limit_unit:'GB',expiry_days:30})});toast(n+' created');loadS()}
async function resetAll(){if(!confirm('Reset all usage?'))return;await fetch('/api/reset-all-usage',{method:'POST'});toast('Reset');loadL()}
async function loadA(){const r=await fetch('/api/addresses');const d=await r.json();$('#alist').innerHTML=(d.addresses||[]).map((a,i)=>`<div style="display:flex;justify-content:space-between;padding:8px;background:rgba(0,0,0,.2);border-radius:8px;margin-bottom:5px;font-size:12px"><span>${a}</span><button class="btn bd" style="padding:2px 8px;font-size:10px" onclick="delA(${i})">Del</button></div>`).join('')||'<div style="color:var(--t2);font-size:12px">Empty</div>'}
async function addAddr(){const a=$('#new-addr').value.trim();if(!a)return;await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});$('#new-addr').value='';loadA();toast('Added')}
async function delA(i){await fetch('/api/addresses/'+i,{method:'DELETE'});loadA()}
async function loadTg(){const r=await fetch('/api/telegram');const d=await r.json();$('#tg-st').innerHTML=d.enabled?'<span style="color:var(--gn)">● ON</span> — admins: '+(d.admin_ids||[]).join(', '):'<span style="color:var(--rd)">● OFF</span>';if(d.admin_ids?.length)$('#tg-adm').value=d.admin_ids.join(' ')}
async function saveTg(){const r=await fetch('/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:$('#tg-tok').value.trim(),admin_ids:$('#tg-adm').value.trim()})});const d=await r.json().catch(()=>({}));if(!r.ok){toast(d.detail||'Error');return}toast(d.enabled?'Bot ON @'+(d.bot_username||''):'Saved');loadTg()}
async function stopTg(){await fetch('/api/telegram/stop',{method:'POST'});toast('Stopped');loadTg()}
async function loadDom(){const r=await fetch('/api/domain');const d=await r.json();$('#dom-cur').textContent='Current: '+(d.domain||'server default')}
async function saveDom(){await fetch('/api/domain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domain:$('#dom-in').value.trim()})});toast('Saved');loadDom()}
async function chPass(){const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:$('#cpw').value,new_password:$('#npw').value})});if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'Error');return}toast('Password changed')}
setL(LANG); loadS(); setInterval(loadS,5000);
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
