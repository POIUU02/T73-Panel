#!/usr/bin/env python3
"""
VROOM Panel v5 - COMPLETE FIXED
- /sub/{uid} → plain-text vless lines (FOR APPS)
- /page/{uid} → beautiful bilingual panel + day/night (Crystal Soft Glass UI - exact match to design)
- Dashboard bilingual
- Telegram button bot
- REAL app photos from client/icons/
"""
import asyncio, json, os, hashlib, secrets, time, re, base64
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import deque, defaultdict
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn, httpx, logging, psutil
from pathlib import Path
from contextlib import asynccontextmanager

# ================================================================
# ========== CONFIG ==========
# ================================================================
try:
    SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_urlsafe(32)
    os.environ.setdefault("SECRET_KEY", SECRET_KEY)
except Exception:
    SECRET_KEY = "vroom-default-secret-key"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VROOM")

# ================================================================
# ========== LIFESPAN ==========
# ================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000), timeout=httpx.Timeout(180.0, connect=30.0), follow_redirects=True)
    logger.info(f"🚀 VROOM v5 :{CONFIG['port']}")
    asyncio.create_task(keep_alive())
    if TELEGRAM.get("token") and TELEGRAM.get("admin_ids"):
        TELEGRAM["enabled"] = True
        await start_telegram_bot()
    yield
    if http_client:
        await http_client.aclose()
    if TELEGRAM_TASK:
        TELEGRAM_TASK.cancel()

app = FastAPI(title="VROOM", docs_url=None, redoc_url=None, lifespan=lifespan)
CONFIG = {"port": int(os.environ.get("PORT", 8080)), "secret": SECRET_KEY}
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ================================================================
# ========== STATIC FILES ==========
# ================================================================
STATIC_DIR = Path("client/icons")
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/client", StaticFiles(directory="client"), name="client")

connections, connection_sockets = {}, {}
link_ip_map = defaultdict(set)
stats = {"total_bytes": 0, "download_bytes": 0, "upload_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs = deque(maxlen=50)
hourly_traffic = defaultdict(int)
http_client = None
LINKS, LINKS_LOCK = {}, asyncio.Lock()
CUSTOM_ADDRESSES, CUSTOM_ADDRESSES_LOCK = ["www.speedtest.net"], asyncio.Lock()
CUSTOM_DOMAIN, CUSTOM_DOMAIN_LOCK = "", asyncio.Lock()
TELEGRAM = {"token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), "admin_ids": [], "enabled": False, "offset": 0}
TELEGRAM_LOCK, TELEGRAM_TASK, TG_STATE = asyncio.Lock(), None, {}
SESSION_COOKIE, SESSION_TTL = "vroom_session", 60 * 60 * 24 * 7

# ================================================================
# ========== APP PHOTOS ==========
# ================================================================
APP_PHOTOS = {
    "Hiddify": {"file": "Hiddify.pnq", "name": "Hiddify", "fallback": "🛡️", "download": "https://github.com/hiddify/hiddify-app/releases/latest"},
    "v2rayng": {"file": "v2rayng.png", "name": "v2rayNG", "fallback": "📱", "download": "https://github.com/2dust/v2rayNG/releases/latest"},
    "NPV Tunnel": {"file": "NPV Tunnel.pnq", "name": "NPV Tunnel", "fallback": "🔒", "download": "https://play.google.com/store/apps/details?id=com.npv.tunnel"},
    "Happ": {"file": "Happ.pnq", "name": "Happ", "fallback": "🟢", "download": "https://apps.apple.com/app/happ/id123456789"},
    "v2box": {"file": "v2box.png", "name": "V2Box", "fallback": "📦", "download": "https://apps.apple.com/app/v2box/id6446814670"},
    "v2rayn": {"file": "v2rayn.png", "name": "v2rayN", "fallback": "💻", "download": "https://github.com/2dust/v2rayN/releases/latest"}
}

@app.get("/api/app-photo/{app_id}")
async def get_app_photo(app_id: str):
    app_data = APP_PHOTOS.get(app_id)
    if not app_data:
        return {"url": None, "fallback": "📱", "download": ""}
    filepath = STATIC_DIR / app_data["file"]
    if filepath.exists():
        return {"url": f"/client/icons/{app_data['file']}", "fallback": app_data["fallback"], "download": app_data["download"], "name": app_data["name"]}
    return {"url": None, "fallback": app_data["fallback"], "download": app_data["download"], "name": app_data["name"]}

# ================================================================
# ========== AUTH ==========
# ================================================================
def hash_password(pw):
    return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()

AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "admin"))}
SESSIONS, SESSIONS_LOCK = {}, asyncio.Lock()

async def create_session():
    t = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK:
        SESSIONS[t] = time.time() + SESSION_TTL
    return t

async def is_valid_session(token):
    if not token:
        return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None or exp < time.time():
            SESSIONS.pop(token, None)
            return False
        return True

async def destroy_session(token):
    if token:
        async with SESSIONS_LOCK:
            SESSIONS.pop(token, None)

async def require_auth(request: Request):
    if not await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        raise HTTPException(401, "unauthorized")
    return True

# ================================================================
# ========== HELPERS ==========
# ================================================================
def get_domain():
    return (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "localhost").replace("https://", "").replace("http://", "").rstrip("/")

def generate_vless_link(uuid, remark="VROOM", address=None):
    domain = CUSTOM_DOMAIN if CUSTOM_DOMAIN else get_domain()
    addr = address if address else domain
    params = {"encryption": "none", "security": "tls", "type": "ws", "host": domain, "path": f"/ws/{uuid}", "sni": domain, "fp": "chrome", "alpn": "http/1.1"}
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"

def uptime():
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value, unit):
    u = (unit or "GB").upper()
    if u == "GB":
        return int(value * 1024 ** 3)
    if u == "MB":
        return int(value * 1024 ** 2)
    return int(value)

def compute_expiry(expiry_days):
    try:
        days = float(expiry_days or 0)
    except Exception:
        days = 0
    return "" if days <= 0 else (datetime.now() + timedelta(days=days)).isoformat()

def is_expired(link):
    exp = link.get("expiry") if isinstance(link, dict) else None
    if not exp:
        return False
    try:
        return datetime.now() >= datetime.fromisoformat(exp)
    except Exception:
        return False

def count_connections_for_link(uid):
    return len(link_ip_map.get(uid, set()))

def get_client_ip(websocket):
    f = websocket.headers.get("x-forwarded-for")
    if f:
        return f.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"

def remove_ip_from_link(uid, ip):
    if uid in link_ip_map:
        link_ip_map[uid].discard(ip)
        if not link_ip_map[uid]:
            link_ip_map.pop(uid, None)

async def close_connections_for_link(uid):
    for cid in [c for c, i in connections.items() if i.get("uuid") == uid]:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000)
            except Exception:
                pass
        connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    link_ip_map.pop(uid, None)

def fmt_bytes(b):
    if b >= 1024 ** 3:
        return f"{b/1024**3:.2f} GB"
    if b >= 1024 ** 2:
        return f"{b/1024**2:.1f} MB"
    return f"{b/1024:.0f} KB"

async def build_sub_content(uid, link):
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    lines = [generate_vless_link(uid, remark=f"VROOM-{link['label']}")]
    for i, addr in enumerate(addresses):
        lines.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))
    return "\n".join(lines)

# ================================================================
# ========== TELEGRAM ==========
# ================================================================
def ikb(rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": c} for t, c in row] for row in rows]}

def main_menu_kb(lang="fa"):
    if lang == "en":
        return ikb([[("➕ Create", "create_start"), ("📋 List", "list")], [("📊 Stats", "stats"), ("🔗 Sub link", "sub_menu")], [("🇮🇷 فارسی", "lang_fa"), ("ℹ️ Help", "help")]])
    return ikb([[("➕ ساخت", "create_start"), ("📋 لیست", "list")], [("📊 آمار", "stats"), ("🔗 لینک ساب", "sub_menu")], [("🇬🇧 English", "lang_en"), ("ℹ️ راهنما", "help")]])

def tg_lang(user_id):
    return (TG_STATE.get(user_id) or {}).get("lang", "fa")

async def tg_api(method, **kwargs):
    token = TELEGRAM.get("token")
    if not token:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            return (await client.post(f"https://api.telegram.org/bot{token}/{method}", json=kwargs)).json()
    except Exception as e:
        logger.error(f"TG: {e}")
        return None

async def tg_send(chat_id, text, reply_markup=None):
    d = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        d["reply_markup"] = reply_markup
    return await tg_api("sendMessage", **d)

async def tg_edit(chat_id, message_id, text, reply_markup=None):
    d = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        d["reply_markup"] = reply_markup
    return await tg_api("editMessageText", **d)

async def tg_answer(cq_id, text=None, show_alert=False):
    d = {"callback_query_id": cq_id}
    if text:
        d.update({"text": text, "show_alert": show_alert})
    return await tg_api("answerCallbackQuery", **d)

async def handle_callback(cq):
    data, cq_id = cq.get("data") or "", cq.get("id")
    msg = cq.get("message") or {}
    chat_id, message_id = msg.get("chat", {}).get("id"), msg.get("message_id")
    user_id = (cq.get("from") or {}).get("id")
    if user_id not in (TELEGRAM.get("admin_ids") or []):
        await tg_answer(cq_id, "Admin only", True)
        return
    await tg_answer(cq_id)
    lang = tg_lang(user_id)
    home = "🏠 منو" if lang == "fa" else "🏠 Menu"
    
    if data in ("lang_fa", "lang_en"):
        TG_STATE.setdefault(user_id, {})["lang"] = "fa" if data == "lang_fa" else "en"
        lang = tg_lang(user_id)
        txt = "🚀 <b>VROOM Bot</b>\nفقط با دکمه‌ها کار کن." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only."
        await tg_edit(chat_id, message_id, txt, reply_markup=main_menu_kb(lang))
        return
    
    if data == "menu":
        for k in list((TG_STATE.get(user_id) or {}).keys()):
            if k != "lang":
                TG_STATE.get(user_id, {}).pop(k, None)
        txt = "🚀 <b>VROOM Bot</b>\nفقط با دکمه‌ها." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only."
        await tg_edit(chat_id, message_id, txt, reply_markup=main_menu_kb(lang))
        return
    
    if data == "help":
        txt = "ℹ️ همه کارها با دکمه.\nساب + صفحه + کانفیگ داده می‌شه." if lang == "fa" else "ℹ️ Everything via buttons.\nSub + page + config are shared."
        await tg_edit(chat_id, message_id, txt, reply_markup=ikb([[(home, "menu")]]))
        return
    
    if data == "stats":
        async with LINKS_LOCK:
            n, active = len(LINKS), sum(1 for x in LINKS.values() if x.get("active") and not is_expired(x))
        if lang == "fa":
            t = f"📊 <b>آمار زنده</b>\n\n🔗 اینباند: {n} (فعال: {active})\n📡 اتصال: {len(connections)}\n📥 دانلود: {fmt_bytes(stats['download_bytes'])}\n📤 آپلود: {fmt_bytes(stats['upload_bytes'])}\n📦 کل: {fmt_bytes(stats['total_bytes'])}\n⏱️ آپتایم: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        else:
            t = f"📊 <b>Live Stats</b>\n\n🔗 Links: {n} (active: {active})\n📡 Conns: {len(connections)}\n📥 DL: {fmt_bytes(stats['download_bytes'])}\n📤 UL: {fmt_bytes(stats['upload_bytes'])}\n📦 Total: {fmt_bytes(stats['total_bytes'])}\n⏱️ Uptime: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("🔄", "stats"), (home, "menu")]]))
        return
    
    if data == "list":
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 خالی" if lang == "fa" else "📭 Empty", reply_markup=ikb([[("➕", "create_start"), (home, "menu")]]))
            return
        rows = [[(f"{'✅' if d.get('active') and not is_expired(d) else '❌'} {d['label']}", f"link:{uid}")] for uid, d in items[:15]]
        rows.append([(home, "menu")])
        await tg_edit(chat_id, message_id, "📋 اینباندها" if lang == "fa" else "📋 Inbounds", reply_markup=ikb(rows))
        return
    
    if data.startswith("link:"):
        uid = data[5:]
        async with LINKS_LOCK:
            link = LINKS.get(uid)
        if not link:
            await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[("📋", "list"), (home, "menu")]]))
            return
        domain = get_domain()
        sub, page = f"https://{domain}/sub/{uid}", f"https://{domain}/page/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
        if lang == "fa":
            t = f"🏷 <b>{link['label']}</b>\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 اتصالات: {count_connections_for_link(uid)}\n\n📥 <b>لینک ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"🏷 <b>{link['label']}</b>\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 Conns: {count_connections_for_link(uid)}\n\n📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("🗑", f"delask:{uid}"), ("📋", "list")], [(home, "menu")]]))
        return
    
    if data.startswith("delask:"):
        uid = data[7:]
        q = f"حذف {uid}؟" if lang == "fa" else f"Delete {uid}?"
        await tg_edit(chat_id, message_id, q, reply_markup=ikb([[("✅", f"deldo:{uid}"), ("❌", f"link:{uid}")]]))
        return
    
    if data.startswith("deldo:"):
        uid = data[6:]
        async with LINKS_LOCK:
            LINKS.pop(uid, None)
        await close_connections_for_link(uid)
        await tg_edit(chat_id, message_id, "✅ حذف شد" if lang == "fa" else "✅ Deleted", reply_markup=ikb([[("📋", "list"), (home, "menu")]]))
        return
    
    if data == "sub_menu":
        async with LINKS_LOCK:
            items = list(LINKS.keys())[:12]
        if not items:
            await tg_edit(chat_id, message_id, "📭", reply_markup=ikb([[(home, "menu")]]))
            return
        rows = [[(u, f"showsub:{u}")] for u in items] + [[(home, "menu")]]
        await tg_edit(chat_id, message_id, "🔗 انتخاب کن:" if lang == "fa" else "🔗 Pick:", reply_markup=ikb(rows))
        return
    
    if data.startswith("showsub:"):
        uid = data[8:]
        sub = f"https://{get_domain()}/sub/{uid}"
        page = f"https://{get_domain()}/page/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{uid}")
        if lang == "fa":
            t = f"📥 <b>ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
        await tg_send(chat_id, t)
        return
    
    if data == "create_start":
        prev = TG_STATE.get(user_id) or {}
        TG_STATE[user_id] = {"step": "label", "lang": prev.get("lang", "fa")}
        await tg_edit(chat_id, message_id, "➕ نام؟" if lang == "fa" else "➕ Name?", reply_markup=ikb([[("user1", "c_name:user1"), ("vip", "c_name:vip"), ("test", "c_name:test")], [("🎲", "c_name:rand"), ("❌", "menu")]]))
        return
    
    if data.startswith("c_name:"):
        name = data[7:]
        if name == "rand":
            name = "u" + secrets.token_hex(3)
        if not re.match(r"^[a-zA-Z0-9\-_.]+$", name):
            await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[(home, "menu")]]))
            return
        async with LINKS_LOCK:
            if name in LINKS:
                await tg_edit(chat_id, message_id, "❌ تکراری" if lang == "fa" else "❌ exists", reply_markup=ikb([[("➕", "create_start"), (home, "menu")]]))
                return
        st = TG_STATE.get(user_id) or {}
        TG_STATE[user_id] = {"step": "limit", "label": name, "lang": st.get("lang", "fa")}
        await tg_edit(chat_id, message_id, f"📦 حجم <b>{name}</b>؟" if lang == "fa" else f"📦 Volume <b>{name}</b>?", reply_markup=ikb([[("1GB", "c_lim:1"), ("5GB", "c_lim:5"), ("10GB", "c_lim:10")], [("50GB", "c_lim:50"), ("∞", "c_lim:0"), ("❌", "menu")]]))
        return
    
    if data.startswith("c_lim:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "limit":
            await tg_edit(chat_id, message_id, "Restart", reply_markup=main_menu_kb(lang))
            return
        st["limit"] = float(data[6:])
        st["step"] = "days"
        TG_STATE[user_id] = st
        await tg_edit(chat_id, message_id, f"📅 روز <b>{st['label']}</b>؟" if lang == "fa" else f"📅 Days <b>{st['label']}</b>?", reply_markup=ikb([[("7", "c_day:7"), ("30", "c_day:30"), ("90", "c_day:90")], [("∞", "c_day:0"), ("❌", "menu")]]))
        return
    
    if data.startswith("c_day:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "days":
            await tg_edit(chat_id, message_id, "Restart", reply_markup=main_menu_kb(lang))
            return
        days, label, lim = float(data[6:]), st["label"], st.get("limit", 0)
        limit_bytes = parse_size_to_bytes(lim, "GB") if lim > 0 else 0
        async with LINKS_LOCK:
            if label in LINKS:
                await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[(home, "menu")]]))
                TG_STATE.pop(user_id, None)
                return
            LINKS[label] = {"label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": 0, "created_at": datetime.now().isoformat(), "active": True, "expiry": compute_expiry(days)}
        keep_lang = st.get("lang", "fa")
        TG_STATE[user_id] = {"lang": keep_lang}
        domain = get_domain()
        sub, page = f"https://{domain}/sub/{label}", f"https://{domain}/page/{label}"
        vless = generate_vless_link(label, remark=f"VROOM-{label}")
        if lang == "fa":
            t = f"✅ <b>ساخته شد</b>\n\n🏷 <code>{label}</code>\n📦 <code>{lim if lim else '∞'} GB</code>\n📅 <code>{int(days) if days else '∞'} روز</code>\n\n📥 <b>لینک ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"✅ <b>Created</b>\n\n🏷 <code>{label}</code>\n📦 <code>{lim if lim else '∞'} GB</code>\n📅 <code>{int(days) if days else '∞'}d</code>\n\n📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("➕", "create_start"), ("📋", "list")], [(home, "menu")]]))

async def handle_tg_message(msg):
    chat_id = (msg.get("chat") or {}).get("id")
    user_id = (msg.get("from") or {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return
    is_admin = user_id in (TELEGRAM.get("admin_ids") or [])
    lang = tg_lang(user_id)
    if text in ("/start", "start", "منو", "menu"):
        if is_admin:
            txt = "🚀 <b>VROOM Bot</b>\nفقط دکمه — لینک ساب برای برنامه‌ها." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only — sub link for apps."
            await tg_send(chat_id, txt, reply_markup=main_menu_kb(lang))
        else:
            await tg_send(chat_id, "⛔ فقط ادمین" if lang == "fa" else "⛔ Admin only")
        return
    if not is_admin:
        await tg_send(chat_id, "⛔")
        return
    await tg_send(chat_id, "👇" if lang == "fa" else "Buttons 👇", reply_markup=main_menu_kb(lang))

async def telegram_poll_loop():
    logger.info("🤖 ربات تلگرام شروع شد")
    while True:
        try:
            async with TELEGRAM_LOCK:
                token, enabled, offset = TELEGRAM.get("token"), TELEGRAM.get("enabled"), TELEGRAM.get("offset", 0)
            if not token or not enabled:
                await asyncio.sleep(4)
                continue
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"offset": offset, "timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])})
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
                    logger.error(f"TG: {e}")
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

# ================================================================
# ========== API ENDPOINTS ==========
# ================================================================
@app.get("/")
async def root():
    return {"service": "VROOM", "version": "5.0", "domain": get_domain()}

@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "download": stats["download_bytes"], "upload": stats["upload_bytes"], "uptime": uptime()}

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
        raise HTTPException(400, "Wrong password")
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
        "connections_detail": [{"uuid": i.get("uuid"), "ip": i.get("ip"), "bytes": i.get("bytes", 0), "since": i.get("connected_at")} for i in connections.values()],
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
        LINKS[label] = {"label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn, "created_at": datetime.now().isoformat(), "active": True, "expiry": expiry}
    domain = get_domain()
    return {"uuid": label, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn, "active": True, "expiry": expiry,
            "vless_link": generate_vless_link(label, remark=f"VROOM-{label}"), "sub_url": f"https://{domain}/sub/{label}", "page_url": f"https://{domain}/page/{label}"}

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    domain = get_domain()
    result = []
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            result.append({"uuid": uid, "label": data["label"], "limit_bytes": data["limit_bytes"], "used_bytes": data["used_bytes"],
                "max_connections": data.get("max_connections", 0), "active": data["active"], "expiry": data.get("expiry", ""), "expired": is_expired(data),
                "created_at": data["created_at"], "current_connections": count_connections_for_link(uid),
                "vless_link": generate_vless_link(uid, remark=f"VROOM-{data['label']}"), "sub_url": f"https://{domain}/sub/{uid}", "page_url": f"https://{domain}/page/{uid}"})
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
            lv, lu = float(body.get("limit_value") or 0), body.get("limit_unit") or "GB"
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
        raise HTTPException(400, "Invalid")
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
        return {"has_token": bool(TELEGRAM.get("token")), "admin_ids": TELEGRAM.get("admin_ids", []), "enabled": TELEGRAM.get("enabled", False)}

@app.post("/api/telegram")
async def set_tg(request: Request, _=Depends(require_auth)):
    body = await request.json()
    token = (body.get("token") or "").strip()
    admin_raw = body.get("admin_ids") or body.get("admin_id") or ""
    admin_ids = [int(x) for x in admin_raw] if isinstance(admin_raw, list) else [int(x) for x in re.findall(r"-?\d+", str(admin_raw))]
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

# ================================================================
# ========== SUBSCRIPTION RAW ==========
# ================================================================
@app.get("/sub/{uid}")
async def subscription_raw(uid: str, request: Request):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(404, "Not found")
    if not link["active"]:
        raise HTTPException(403, "Disabled")
    if is_expired(link):
        raise HTTPException(403, "Expired")
    
    content = await build_sub_content(uid, link)
    used, total, expire_ts = link["used_bytes"], (link["limit_bytes"] if link["limit_bytes"] > 0 else 0), 0
    if link.get("expiry"):
        try:
            expire_ts = int(datetime.fromisoformat(link["expiry"]).timestamp())
        except Exception:
            pass
    
    import base64 as _b64
    title_b64 = _b64.b64encode(f"VROOM-{link['label']}".encode()).decode()
    headers = {
        "Profile-Update-Interval": "12",
        "Profile-Title": title_b64,
        "Subscription-Userinfo": f"upload=0; download={used}; total={total}; expire={expire_ts}",
        "Cache-Control": "no-cache",
    }
    raw = content + "\n"
    if request.query_params.get("b64") in ("1", "true", "yes"):
        return Response(content=_b64.b64encode(raw.encode()).decode(), media_type="text/plain; charset=utf-8", headers=headers)
    return Response(content=raw, media_type="text/plain; charset=utf-8", headers=headers)

# ================================================================
# ========== PAGE HTML  (Exact match to design + Day/Night + Bilingual) ==========
# ================================================================
@app.get("/page/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(404)
    if not link["active"] or is_expired(link):
        raise HTTPException(403)
    
    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / 1024 ** 3, 2)
    limit_gb = round(link["limit_bytes"] / 1024 ** 3, 2) if link["limit_bytes"] else 0
    percent = round((link["used_bytes"] / link["limit_bytes"]) * 100, 1) if link["limit_bytes"] else 0
    remaining = round(max(0, limit_gb - used_gb), 2) if limit_gb else "∞"
    
    if is_expired(link):
        status_fa, status_en, sc = "منقضی", "Expired", "#ff6b9d"
    elif link["limit_bytes"] and link["used_bytes"] >= link["limit_bytes"]:
        status_fa, status_en, sc = "محدود", "Limited", "#fbbf24"
    else:
        status_fa, status_en, sc = "فعال", "Active", "#22c55e"
    
    exp = link.get("expiry")
    if exp:
        try:
            ed = datetime.fromisoformat(exp)
            days_left = max(0, (ed - datetime.now()).days)
            days_fa, days_en, exp_disp = f"{days_left}", f"{days_left}", ed.strftime("%Y/%m/%d")
        except Exception:
            days_fa = days_en = exp_disp = "∞"
    else:
        days_fa = days_en = exp_disp = "∞"
    
    domain = get_domain()
    sub_url = f"https://{domain}/sub/{uid}"
    qr_data = quote(server_link, safe="")
    live_conns = count_connections_for_link(uid)

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="light">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>VROOM — {link['label']}</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@500;600;700;800;900&family=Vazirmatn:wght@400;500;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
:root {{
  --bg: #eef2ff;
  --card: rgba(255,255,255,0.82);
  --card-border: rgba(255,255,255,0.9);
  --text: #1e293b;
  --muted: #64748b;
  --blue: #3b82f6;
  --blue2: #6366f1;
  --pink: #ec4899;
  --green: #22c55e;
  --radius: 24px;
  --shadow: 0 10px 40px -8px rgba(99,102,241,0.18), 0 4px 16px -4px rgba(0,0,0,0.06);
  --glow: 0 8px 32px rgba(59,130,246,0.35);
}}
html[data-theme="dark"] {{
  --bg: #0b0f1a;
  --card: rgba(20,25,45,0.78);
  --card-border: rgba(255,255,255,0.08);
  --text: #f1f5f9;
  --muted: #94a3b8;
  --shadow: 0 12px 40px -8px rgba(0,0,0,0.55), 0 4px 16px rgba(59,130,246,0.12);
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{
  font-family:Vazirmatn,Inter,system-ui,sans-serif;
  background:var(--bg);
  color:var(--text);
  min-height:100vh;
  padding:16px 14px 40px;
  transition:background .35s,color .35s;
  overflow-x:hidden;
}}
/* soft aurora background */
body::before{{
  content:'';
  position:fixed;inset:0;pointer-events:none;z-index:0;
  background:
    radial-gradient(ellipse 90% 60% at 10% -5%, rgba(99,102,241,.22), transparent 55%),
    radial-gradient(ellipse 70% 50% at 95% 5%, rgba(236,72,153,.18), transparent 50%),
    radial-gradient(ellipse 60% 40% at 50% 100%, rgba(59,130,246,.1), transparent 50%);
}}
html[data-theme="dark"] body::before{{
  background:
    radial-gradient(ellipse 90% 60% at 10% -5%, rgba(99,102,241,.18), transparent 55%),
    radial-gradient(ellipse 70% 50% at 95% 5%, rgba(236,72,153,.14), transparent 50%),
    radial-gradient(ellipse 60% 40% at 50% 100%, rgba(59,130,246,.08), transparent 50%);
}}
.wrap{{max-width:440px;margin:0 auto;position:relative;z-index:1}}

/* ===== HEADER ===== */
.header{{
  display:flex;align-items:center;justify-content:space-between;
  margin-bottom:18px;padding:0 2px;
}}
.header-left{{display:flex;align-items:center;gap:10px}}
.icon-btn{{
  width:40px;height:40px;border-radius:14px;
  background:var(--card);border:1px solid var(--card-border);
  display:flex;align-items:center;justify-content:center;
  cursor:pointer;font-size:18px;box-shadow:var(--shadow);
  backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  transition:.2s;color:var(--text);
}}
.icon-btn:active{{transform:scale(.94)}}
.lang-switch{{
  display:flex;background:var(--card);border:1px solid var(--card-border);
  border-radius:14px;overflow:hidden;box-shadow:var(--shadow);
  backdrop-filter:blur(16px);
}}
.lang-switch button{{
  border:none;padding:8px 12px;font-size:12px;font-weight:700;
  background:transparent;color:var(--muted);cursor:pointer;font-family:inherit;
  transition:.2s;
}}
.lang-switch button.on{{
  background:linear-gradient(135deg,var(--blue),var(--blue2));
  color:#fff;
}}
.logo{{
  display:flex;align-items:center;gap:6px;
  font-family:Inter;font-weight:900;font-size:20px;
  background:linear-gradient(135deg,#6366f1,#ec4899);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
}}
.logo span{{font-size:11px;font-weight:600;opacity:.7;display:block;letter-spacing:.5px;margin-top:-2px}}

/* ===== CARDS ===== */
.card{{
  background:var(--card);
  border:1px solid var(--card-border);
  border-radius:var(--radius);
  padding:18px;
  margin-bottom:14px;
  box-shadow:var(--shadow);
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  position:relative;
}}
.card-title{{
  font-size:15px;font-weight:800;margin-bottom:14px;
  display:flex;align-items:center;justify-content:space-between;
}}
.card-title .badge{{
  font-size:12px;font-weight:600;color:var(--blue);
  background:rgba(59,130,246,.1);padding:3px 10px;border-radius:20px;
}}

/* ===== TOP STATUS CARD ===== */
.status-row{{
  display:flex;justify-content:space-between;align-items:center;
  margin-bottom:16px;font-size:12px;font-weight:600;
}}
.status-online{{
  display:flex;align-items:center;gap:6px;color:var(--green);
}}
.dot{{
  width:8px;height:8px;border-radius:50%;background:var(--green);
  box-shadow:0 0 0 0 rgba(34,197,94,.5);animation:pulse 1.8s infinite;
}}
@keyframes pulse{{
  0%{{box-shadow:0 0 0 0 rgba(34,197,94,.5)}}
  70%{{box-shadow:0 0 0 8px transparent}}
  100%{{box-shadow:0 0 0 0 transparent}}
}}
.stats-grid{{
  display:grid;grid-template-columns:auto 1fr 1fr 1fr;gap:10px;align-items:center;
}}
@media(max-width:380px){{.stats-grid{{grid-template-columns:1fr 1fr;gap:12px}}
.ring-wrap{{grid-column:1/-1;justify-self:center;margin-bottom:4px}}}}
.stat{{text-align:center}}
.stat .label{{font-size:11px;color:var(--muted);font-weight:600;margin-bottom:2px}}
.stat .value{{font-size:15px;font-weight:800}}
.ring-wrap{{width:72px;height:72px;position:relative}}
.ring{{
  width:72px;height:72px;border-radius:50%;
  background:conic-gradient(var(--blue) 0% 0%, #e2e8f0 0% 100%);
  display:flex;align-items:center;justify-content:center;
  box-shadow:0 0 20px rgba(59,130,246,.25);
  transition:background 1s;
}}
html[data-theme="dark"] .ring{{background:conic-gradient(var(--blue) 0% 0%, #1e293b 0% 100%)}}
.ring-inner{{
  width:56px;height:56px;border-radius:50%;
  background:var(--card);display:flex;align-items:center;justify-content:center;
  font-size:14px;font-weight:800;
}}
.progress-bar{{
  margin-top:14px;height:6px;background:rgba(148,163,184,.25);
  border-radius:99px;overflow:hidden;
}}
.progress-fill{{
  height:100%;width:0;border-radius:99px;
  background:linear-gradient(90deg,var(--blue),var(--pink));
  transition:width 1s;box-shadow:0 0 10px rgba(59,130,246,.4);
}}

/* ===== SUB LINK ===== */
.sub-row{{
  display:flex;align-items:center;gap:10px;
  background:rgba(148,163,184,.08);border-radius:16px;
  padding:8px 10px;margin-bottom:14px;
}}
.sub-url{{
  flex:1;font-size:11px;font-family:monospace;color:var(--muted);
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;direction:ltr;text-align:left;
}}
.copy-btn{{
  background:linear-gradient(135deg,var(--blue),var(--blue2));
  color:#fff;border:none;padding:9px 16px;border-radius:12px;
  font-size:12px;font-weight:700;cursor:pointer;font-family:inherit;
  box-shadow:var(--glow);white-space:nowrap;transition:.2s;
}}
.copy-btn:active{{transform:scale(.96)}}
.info-pills{{
  display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;
}}
.pill{{
  background:rgba(148,163,184,.08);border-radius:14px;
  padding:10px 6px;text-align:center;
}}
.pill .p-label{{font-size:10px;color:var(--muted);font-weight:600;margin-bottom:3px;display:flex;align-items:center;justify-content:center;gap:3px}}
.pill .p-value{{font-size:13px;font-weight:800}}

/* ===== CONFIG + QR ===== */
.cfg-box{{
  background:rgba(148,163,184,.08);border-radius:14px;
  padding:12px;font-size:10px;font-family:monospace;
  word-break:break-all;max-height:52px;overflow-y:auto;
  direction:ltr;text-align:left;color:var(--muted);margin-bottom:14px;
  cursor:pointer;position:relative;
}}
.cfg-box .copy-icon{{
  position:absolute;top:8px;right:8px;font-size:14px;opacity:.5;
}}
.qr-wrap{{
  display:flex;justify-content:center;margin-bottom:14px;
}}
.qr{{
  width:130px;height:130px;background:#fff;border-radius:18px;
  padding:8px;box-shadow:0 8px 30px rgba(59,130,246,.2);
  border:2px solid rgba(99,102,241,.2);cursor:pointer;
}}
.qr img{{width:100%;height:100%;border-radius:10px}}
.action-btns{{display:flex;gap:10px}}
.action-btns button{{
  flex:1;padding:13px;border:none;border-radius:14px;
  font-weight:800;font-size:13px;cursor:pointer;font-family:inherit;transition:.2s;
}}
.btn-share{{
  background:rgba(148,163,184,.12);color:var(--text);
  border:1px solid rgba(148,163,184,.2);
}}
.btn-add{{
  background:linear-gradient(135deg,var(--blue),var(--pink));
  color:#fff;box-shadow:0 8px 24px rgba(236,72,153,.35);
}}
.action-btns button:active{{transform:scale(.97)}}

/* ===== QUICK TOOLS ===== */
.plat-row{{
  display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px;
}}
.plat-btn{{
  padding:8px 14px;border-radius:20px;border:1px solid rgba(148,163,184,.2);
  background:rgba(148,163,184,.08);color:var(--muted);
  font-size:12px;font-weight:700;cursor:pointer;font-family:inherit;transition:.2s;
}}
.plat-btn.on{{
  background:linear-gradient(135deg,var(--blue),var(--blue2));
  color:#fff;border-color:transparent;box-shadow:0 4px 16px rgba(59,130,246,.35);
}}
.apps-grid{{
  display:grid;grid-template-columns:repeat(4,1fr);gap:10px;
}}
@media(max-width:380px){{.apps-grid{{grid-template-columns:repeat(3,1fr)}}}}
.app-card{{
  background:rgba(148,163,184,.06);border:1px solid rgba(148,163,184,.12);
  border-radius:18px;padding:14px 6px 10px;text-align:center;
  cursor:pointer;transition:.2s;position:relative;
}}
.app-card:active{{transform:scale(.96)}}
.app-icon{{
  width:48px;height:48px;margin:0 auto 8px;border-radius:14px;
  background:linear-gradient(145deg,#e0e7ff,#fce7f3);
  display:flex;align-items:center;justify-content:center;
  font-size:22px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,.08);
}}
html[data-theme="dark"] .app-icon{{background:linear-gradient(145deg,#1e293b,#312e81)}}
.app-icon img{{width:100%;height:100%;object-fit:cover}}
.app-name{{font-size:11px;font-weight:700}}
.app-hint{{font-size:9px;color:var(--muted);margin-top:2px}}

/* ===== FOOTER ===== */
.footer{{
  text-align:center;font-size:11px;color:var(--muted);
  margin-top:8px;padding-top:12px;
}}
.footer b{{
  background:linear-gradient(135deg,var(--blue),var(--pink));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
}}

/* ===== TOAST ===== */
.toast{{
  position:fixed;bottom:28px;left:50%;transform:translateX(-50%) translateY(80px);
  background:var(--card);backdrop-filter:blur(20px);
  padding:12px 22px;border-radius:14px;font-size:13px;font-weight:700;
  color:var(--blue);opacity:0;transition:.35s;border:1px solid var(--card-border);
  z-index:9999;box-shadow:var(--shadow);
}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}

/* QR Modal */
#qrm{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);align-items:center;justify-content:center;z-index:10000;backdrop-filter:blur(8px)}}
#qrm img{{width:min(80vw,300px);border-radius:20px;box-shadow:0 0 60px rgba(59,130,246,.4)}}
</style>
</head>
<body>
<div class="wrap">

  <!-- HEADER -->
  <div class="header">
    <div class="header-left">
      <button class="icon-btn" id="themeBtn" onclick="toggleTheme()">☀️</button>
      <div class="lang-switch">
        <button id="btnEn" onclick="setLang('en')">EN</button>
        <button id="btnFa" class="on" onclick="setLang('fa')">FA</button>
      </div>
    </div>
    <div class="logo">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none"><path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="url(#g)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><defs><linearGradient id="g" x1="2" y1="2" x2="22" y2="22"><stop stop-color="#6366f1"/><stop offset="1" stop-color="#ec4899"/></linearGradient></defs></svg>
      VROOM
    </div>
    <button class="icon-btn">⚡</button>
  </div>

  <!-- STATUS CARD -->
  <div class="card">
    <div class="card-title">
      <span data-fa="Subscription /" data-en="Subscription /">Subscription /</span>
      <span class="badge">✨ {link['label']} ✨</span>
    </div>
    <div class="status-row">
      <span data-fa="اتصالات : {live_conns}" data-en="Connections : {live_conns}">اتصالات : {live_conns}</span>
      <span class="status-online"><span class="dot"></span> <span data-fa="سرور آنلاین" data-en="Server Online">سرور آنلاین</span></span>
    </div>
    <div class="stats-grid">
      <div class="ring-wrap">
        <div class="ring" id="ring"><div class="ring-inner" id="pct">0%</div></div>
      </div>
      <div class="stat">
        <div class="label" data-fa="باقی" data-en="Left">باقی</div>
        <div class="value">{remaining}{' GB' if remaining != '∞' else ''}</div>
      </div>
      <div class="stat">
        <div class="label" data-fa="وضعیت" data-en="Status">وضعیت</div>
        <div class="value" style="color:{sc};font-size:13px" data-fa="{status_fa}" data-en="{status_en}">{status_fa}</div>
      </div>
      <div class="stat">
        <div class="label" data-fa="مصرف" data-en="Used">مصرف</div>
        <div class="value">{used_gb} GB</div>
      </div>
    </div>
    <div class="progress-bar"><div class="progress-fill" id="bar"></div></div>
  </div>

  <!-- SUB LINK CARD -->
  <div class="card">
    <div class="card-title">
      <span data-fa="لینک ساب (دریافت‌ها)" data-en="Sub Link (for apps)">لینک ساب (دریافت‌ها)</span>
      <span style="font-size:16px">🔗</span>
    </div>
    <div class="sub-row">
      <button class="copy-btn" onclick="cp(SUB)" data-fa="کپی" data-en="Copy">کپی</button>
      <div class="sub-url">{sub_url}</div>
    </div>
    <div class="info-pills">
      <div class="pill">
        <div class="p-label">🛡️ <span data-fa="وضعیت" data-en="Status">وضعیت</span></div>
        <div class="p-value" style="color:{sc}" data-fa="{status_fa}" data-en="{status_en}">{status_fa}</div>
      </div>
      <div class="pill">
        <div class="p-label">📅 <span data-fa="انقضا" data-en="Expiry">انقضا</span></div>
        <div class="p-value" style="color:#f59e0b">{exp_disp}</div>
      </div>
      <div class="pill">
        <div class="p-label">⏳ <span data-fa="باقی" data-en="Left">باقی</span></div>
        <div class="p-value" style="color:#06b6d4" data-fa="{days_fa} روز" data-en="{days_en} days">{days_fa} روز</div>
      </div>
    </div>
  </div>

  <!-- CONFIG + QR -->
  <div class="card">
    <div class="card-title">
      <span data-fa="کانفیگ و QR" data-en="Config & QR">کانفیگ و QR</span>
      <span style="font-size:16px">📱</span>
    </div>
    <div class="cfg-box" onclick="cp(CFG)">
      {server_link}
      <span class="copy-icon">📋</span>
    </div>
    <div class="qr-wrap">
      <div class="qr" onclick="document.getElementById('qrm').style.display='flex'">
        <img src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={qr_data}" alt="QR"/>
      </div>
    </div>
    <div class="action-btns">
      <button class="btn-share" onclick="shareApp()" data-fa="اشتراک" data-en="Share">اشتراک</button>
      <button class="btn-add" onclick="cp(SUB)" data-fa="+ اضافه اشتراک +" data-en="+ Add Sub +">+ اضافه اشتراک +</button>
    </div>
  </div>

  <!-- QUICK TOOLS -->
  <div class="card">
    <div class="card-title">
      <span data-fa="ابزارهای سریع" data-en="Quick Tools">ابزارهای سریع</span>
      <span style="font-size:16px">⚡</span>
    </div>
    <div class="plat-row" id="platBar">
      <button class="plat-btn on" data-p="android" onclick="showPlat('android',this)">Android</button>
      <button class="plat-btn" data-p="ios" onclick="showPlat('ios',this)">iOS</button>
      <button class="plat-btn" data-p="windows" onclick="showPlat('windows',this)">Windows</button>
      <button class="plat-btn" data-p="macos" onclick="showPlat('macos',this)">macOS</button>
      <button class="plat-btn" data-p="linux" onclick="showPlat('linux',this)">Linux</button>
    </div>
    <div class="apps-grid" id="appsGrid"></div>
  </div>

  <div class="footer">Powered by <b>VROOM</b></div>
</div>

<div id="qrm" onclick="this.style.display='none'">
  <img src="https://api.qrserver.com/v1/create-qr-code/?size=360x360&data={qr_data}" alt="QR"/>
</div>
<div class="toast" id="toast"></div>

<script>
const SUB = '{sub_url}';
const CFG = `{server_link}`;
const P = {percent};
let LANG = localStorage.getItem('vroom_lang') || 'fa';

const CATALOG = {{
  android: [
    {{id:'Hiddify',name:'Hiddify',scheme:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayng',name:'v2rayNG',scheme:'v2rayng://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'v2box',name:'V2Box',scheme:'v2box://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'Happ',name:'Happ',scheme:'happ://import?url='+encodeURIComponent(SUB)}}
  ],
  ios: [
    {{id:'Hiddify',name:'Hiddify',scheme:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2box',name:'V2Box',scheme:'v2box://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'Happ',name:'Happ',scheme:'happ://import?url='+encodeURIComponent(SUB)}}
  ],
  windows: [
    {{id:'Hiddify',name:'Hiddify',scheme:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayn',name:'v2rayN',scheme:'v2rayN://import?url='+encodeURIComponent(SUB)}}
  ],
  macos: [
    {{id:'Hiddify',name:'Hiddify',scheme:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2box',name:'V2Box',scheme:'v2box://install-config?url='+encodeURIComponent(SUB)}}
  ],
  linux: [
    {{id:'Hiddify',name:'Hiddify',scheme:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayn',name:'v2rayN',scheme:'v2rayN://import?url='+encodeURIComponent(SUB)}}
  ]
}};

function setLang(l) {{
  LANG = l;
  localStorage.setItem('vroom_lang', l);
  document.documentElement.lang = l;
  document.documentElement.dir = l === 'fa' ? 'rtl' : 'ltr';
  document.getElementById('btnFa').classList.toggle('on', l === 'fa');
  document.getElementById('btnEn').classList.toggle('on', l === 'en');
  document.querySelectorAll('[data-fa]').forEach(el => {{
    el.textContent = l === 'fa' ? el.dataset.fa : el.dataset.en;
  }});
}}

function toggleTheme() {{
  const cur = document.documentElement.getAttribute('data-theme');
  const next = cur === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('vroom_theme', next);
  document.getElementById('themeBtn').textContent = next === 'light' ? '☀️' : '🌙';
}}

async function getAppPhoto(appId) {{
  try {{
    const r = await fetch(`/api/app-photo/${{appId}}`);
    if (r.ok) {{
      const d = await r.json();
      return {{url: d.url, name: d.name}};
    }}
  }} catch(e) {{}}
  return {{url: null, name: appId}};
}}

async function showPlat(p, btn) {{
  document.querySelectorAll('.plat-btn').forEach(b => b.classList.remove('on'));
  if (btn) btn.classList.add('on');
  const list = CATALOG[p] || [];
  const grid = document.getElementById('appsGrid');
  grid.innerHTML = list.map(a => `
    <div class="app-card" onclick="openApp('${{a.id}}','${{p}}')" id="app-${{a.id}}">
      <div class="app-icon"><span style="font-size:18px">⬇️</span></div>
      <div class="app-name">${{a.name}}</div>
      <div class="app-hint">Tap to open</div>
    </div>
  `).join('');
  for (const a of list) {{
    const data = await getAppPhoto(a.id);
    const icon = document.querySelector(`#app-${{a.id}} .app-icon`);
    if (icon && data.url) {{
      icon.innerHTML = `<img src="${{data.url}}" alt="${{a.name}}" onerror="this.parentElement.innerHTML='${{a.name[0]}}'"/>`;
    }} else if (icon) {{
      icon.innerHTML = `<span style="font-weight:800;font-size:18px">${{a.name[0]}}</span>`;
    }}
  }}
}}

function openApp(id, plat) {{
  const a = (CATALOG[plat] || []).find(x => x.id === id);
  if (!a) return;
  if (a.scheme) {{ try {{ location.href = a.scheme; }} catch(e) {{}} }}
  setTimeout(() => {{ cp(SUB); toast(LANG==='fa'?'لینک ساب کپی شد':'Sub link copied'); }}, 1400);
}}

function shareApp() {{
  if (navigator.share) {{
    navigator.share({{title:'VROOM', url:SUB}}).catch(() => cp(SUB));
  }} else {{
    cp(SUB);
  }}
}}

function cp(t) {{
  if (navigator.clipboard) {{
    navigator.clipboard.writeText(t).then(() => toast(LANG==='fa'?'کپی شد ✅':'Copied ✅'));
  }} else {{
    const i = document.createElement('input'); i.value = t;
    document.body.appendChild(i); i.select(); document.execCommand('copy');
    document.body.removeChild(i); toast(LANG==='fa'?'کپی شد ✅':'Copied ✅');
  }}
}}

function toast(m) {{
  const t = document.getElementById('toast');
  t.textContent = m; t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2300);
}}

// init
const savedTheme = localStorage.getItem('vroom_theme') || 'light';
document.documentElement.setAttribute('data-theme', savedTheme);
document.getElementById('themeBtn').textContent = savedTheme === 'light' ? '☀️' : '🌙';
setLang(LANG);
showPlat('android', document.querySelector('.plat-btn'));
setTimeout(() => {{
  document.getElementById('ring').style.background = `conic-gradient(#3b82f6 0% ${{P}}%, ${{savedTheme==='dark'?'#1e293b':'#e2e8f0'}} ${{P}}% 100%)`;
  document.getElementById('bar').style.width = P + '%';
  document.getElementById('pct').textContent = P + '%';
}}, 200);
</script>
</body>
</html>"""
    return HTMLResponse(content=html)

# ================================================================
# ========== WS PROXY ==========
# ================================================================
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

async def add_usage(uid, n, direction="total"):
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
        if max_conn > 0 and client_ip not in link_ip_map.get(uuid, set()) and count_connections_for_link(uuid) >= max_conn:
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
        done, pending = await asyncio.wait({{t1, t2}}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    except Exception as e:
        stats["total_errors"] += 1
        error_logs.append({{"error": str(e), "time": datetime.now().isoformat()}})
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

# ================================================================
# ========== LOGIN & DASHBOARD ==========
# ================================================================
LOGIN_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@700;900&family=Inter:wght@800&display=swap" rel="stylesheet">
<style>
:root{--bg:#eef2ff;--card:rgba(255,255,255,.85);--border:rgba(255,255,255,.9);--text:#1e293b;--blue:#3b82f6;--pink:#ec4899}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:var(--bg);color:var(--text);direction:rtl;
background-image:radial-gradient(ellipse 80% 50% at 15% 0%,rgba(99,102,241,.2),transparent 55%),radial-gradient(ellipse 70% 45% at 90% 20%,rgba(236,72,153,.15),transparent 50%)}
.card{background:var(--card);border:1px solid var(--border);border-radius:28px;padding:42px 34px;width:100%;max-width:380px;
backdrop-filter:blur(24px);box-shadow:0 20px 50px -12px rgba(99,102,241,.2)}
h1{font-size:30px;font-weight:900;text-align:center;font-family:Inter;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:28px}
input{width:100%;padding:14px 16px;background:rgba(148,163,184,.1);border:1px solid rgba(148,163,184,.2);border-radius:14px;color:var(--text);font-size:15px;font-family:inherit;outline:none;margin-bottom:14px}
input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.15)}
.btn{width:100%;padding:15px;background:linear-gradient(135deg,var(--blue),#6366f1);border:none;border-radius:14px;color:#fff;font-size:16px;font-weight:800;cursor:pointer;font-family:inherit;box-shadow:0 8px 24px rgba(59,130,246,.35)}
.err{color:#ec4899;font-size:13px;text-align:center;display:none;margin-bottom:12px}.err.show{display:block}
</style></head><body>
<div class="card"><h1>VROOM</h1><div class="err" id="err"></div>
<form id="f"><input type="password" id="pw" placeholder="رمز عبور / Password" autofocus><button class="btn" type="submit">ورود / Login</button></form></div>
<script>document.getElementById('f').onsubmit=async e=>{e.preventDefault();const err=document.getElementById('err');err.classList.remove('show');
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
if(!r.ok)throw new Error('رمز اشتباه است');location.href='/dashboard'}catch(ex){err.textContent=ex.message;err.classList.add('show')}}</script></body></html>"""

DASHBOARD_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800;900&family=Inter:wght@800&display=swap" rel="stylesheet">
<style>
:root{--bg:#eef2ff;--card:rgba(255,255,255,.82);--border:rgba(255,255,255,.9);--text:#1e293b;--muted:#64748b;--blue:#3b82f6;--pink:#ec4899;--green:#22c55e;--red:#ec4899;--radius:18px;--shadow:0 10px 30px -8px rgba(99,102,241,.15)}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Vazirmatn,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;direction:rtl;
background-image:radial-gradient(ellipse 70% 40% at 10% 0%,rgba(99,102,241,.15),transparent 50%),radial-gradient(ellipse 50% 30% at 90% 5%,rgba(236,72,153,.1),transparent 45%)}
.side{width:200px;background:rgba(255,255,255,.7);border-left:1px solid rgba(148,163,184,.15);position:fixed;right:0;top:0;bottom:0;padding:16px 10px;display:flex;flex-direction:column;z-index:40;backdrop-filter:blur(20px)}
.brand{font-size:18px;font-weight:900;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter;padding:8px;margin-bottom:14px}
.ni{padding:10px 12px;border-radius:12px;font-size:12px;font-weight:600;color:var(--muted);cursor:pointer;margin-bottom:3px;border:none;background:none;width:100%;text-align:right;font-family:inherit;transition:.2s}
.ni:hover,.ni.on{background:rgba(59,130,246,.1);color:var(--blue)}
.main{margin-right:200px;padding:20px 16px}
.page{display:none}.page.on{display:block}
.pt{font-size:18px;font-weight:900;margin-bottom:16px;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.st{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:14px;backdrop-filter:blur(16px);box-shadow:var(--shadow)}
.st .l{font-size:10px;color:var(--muted);font-weight:600}.st .v{font-size:17px;font-weight:800;margin-top:4px}
.card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-bottom:12px;backdrop-filter:blur(16px);box-shadow:var(--shadow)}
.card h3{font-size:12px;color:var(--blue);margin-bottom:10px;font-weight:700}
.btn{padding:8px 14px;border-radius:11px;border:none;font-weight:700;font-size:12px;cursor:pointer;font-family:inherit;transition:.2s}
.bg{background:linear-gradient(135deg,var(--blue),#6366f1);color:#fff;box-shadow:0 6px 18px rgba(59,130,246,.3)}
.bo{background:rgba(59,130,246,.1);color:var(--blue);border:1px solid rgba(59,130,246,.2)}
.bd{background:rgba(236,72,153,.1);color:var(--red)}
input,select{width:100%;padding:10px 12px;background:rgba(148,163,184,.08);border:1px solid rgba(148,163,184,.2);border-radius:11px;color:var(--text);font-family:inherit;font-size:13px;outline:none;margin-bottom:8px}
table{width:100%;border-collapse:collapse;font-size:12px}th{text-align:right;padding:8px;color:var(--muted);border-bottom:1px solid rgba(148,163,184,.15);font-size:10px}td{padding:8px;border-bottom:1px solid rgba(148,163,184,.08)}
.tag{display:inline-block;padding:3px 8px;border-radius:8px;font-size:10px;font-weight:700}.ton{background:rgba(34,197,94,.12);color:var(--green)}.toff{background:rgba(236,72,153,.1);color:var(--red)}
.toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%) translateY(50px);background:var(--card);border:1px solid var(--border);padding:10px 18px;border-radius:12px;font-size:13px;color:var(--blue);opacity:0;transition:.3s;z-index:999;backdrop-filter:blur(16px);font-weight:700}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mob{display:none;position:fixed;top:0;left:0;right:0;height:48px;background:rgba(255,255,255,.85);border-bottom:1px solid rgba(148,163,184,.15);z-index:50;align-items:center;justify-content:space-between;padding:0 14px;backdrop-filter:blur(16px)}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:80;display:none;align-items:center;justify-content:center;backdrop-filter:blur(6px)}.modal-bg.show{display:flex}
.modal{background:rgba(255,255,255,.95);border:1px solid var(--border);border-radius:20px;padding:20px;width:92%;max-width:380px;backdrop-filter:blur(20px)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.pulse{display:inline-block;width:8px;height:8px;background:var(--green);border-radius:50%;animation:p 1.6s infinite;margin:0 4px}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(34,197,94,.5)}70%{box-shadow:0 0 0 8px transparent}}
.lang{display:inline-flex;border:1px solid rgba(148,163,184,.2);border-radius:14px;overflow:hidden;font-size:11px;font-weight:700;margin-bottom:10px}
.lang button{border:none;padding:5px 11px;background:transparent;color:var(--muted);cursor:pointer;font-family:inherit;font-weight:700}
.lang button.on{background:linear-gradient(135deg,var(--blue),#6366f1);color:#fff}
@media(max-width:768px){.side{transform:translateX(100%)}.side.open{transform:translateX(0)}.main{margin-right:0;padding-top:60px}.stats{grid-template-columns:1fr 1fr}.mob{display:flex}}
</style></head><body>
<div class="mob"><span style="font-weight:900;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter">VROOM</span>
<button class="btn bo" onclick="document.querySelector('.side').classList.toggle('open')">☰</button></div>
<aside class="side">
<div class="brand">VROOM</div>
<div class="lang"><button type="button" id="lFa" class="on" onclick="setL('fa')">FA</button><button type="button" id="lEn" onclick="setL('en')">EN</button></div>
<button class="ni on" data-p="dash" data-fa="📊 داشبورد" data-en="📊 Dashboard">📊 داشبورد</button>
<button class="ni" data-p="links" data-fa="📡 اینباندها" data-en="📡 Inbounds">📡 اینباندها</button>
<button class="ni" data-p="conn" data-fa="🔗 اتصالات" data-en="🔗 Connections">🔗 اتصالات</button>
<button class="ni" data-p="addr" data-fa="🌐 آی‌پی تمیز" data-en="🌐 Clean IP">🌐 آی‌پی تمیز</button>
<button class="ni" data-p="tg" data-fa="🤖 ربات تلگرام" data-en="🤖 Telegram">🤖 ربات تلگرام</button>
<button class="ni" data-p="domain" data-fa="🌍 دامنه" data-en="🌍 Domain">🌍 دامنه</button>
<button class="ni" data-p="sec" data-fa="🔒 امنیت" data-en="🔒 Security">🔒 امنیت</button>
<div style="flex:1"></div>
<button class="ni" style="color:var(--red)" data-fa="خروج" data-en="Logout" onclick="fetch('/api/logout',{method:'POST'}).then(()=>location='/login')">خروج</button>
</aside>
<main class="main">
<section class="page on" id="p-dash">
<div class="pt">داشبورد <span class="pulse"></span></div>
<div class="stats">
<div class="st"><div class="l">👥 اتصال</div><div class="v" id="s-cn">0</div></div>
<div class="st"><div class="l">📥 دانلود</div><div class="v" id="s-dl" style="font-size:14px">0</div></div>
<div class="st"><div class="l">📤 آپلود</div><div class="v" id="s-ul" style="font-size:14px">0</div></div>
<div class="st"><div class="l">📦 کل</div><div class="v" id="s-tr" style="font-size:14px">0</div></div>
</div>
<div class="stats">
<div class="st"><div class="l">📡 لینک‌ها</div><div class="v" id="s-lk">0</div></div>
<div class="st"><div class="l">⏱️ آپتایم</div><div class="v" id="s-up" style="font-size:13px">--</div></div>
<div class="st"><div class="l">💻 CPU</div><div class="v" id="s-cpu">--</div></div>
<div class="st"><div class="l">🧠 RAM</div><div class="v" id="s-ram">--</div></div>
</div>
<div class="card"><h3>⚡ سریع</h3>
<div style="display:flex;gap:8px;flex-wrap:wrap">
<button class="btn bg" onclick="qc(1)">+1GB</button>
<button class="btn bg" onclick="qc(5)">+5GB</button>
<button class="btn bg" onclick="qc(10)">+10GB</button>
<button class="btn bo" onclick="resetAll()">ریست مصرف</button>
</div></div>
</section>
<section class="page" id="p-links">
<div class="pt">اینباندها <button class="btn bg" style="float:left" onclick="$('#addM').classList.add('show')">+ افزودن</button></div>
<div class="card" style="overflow-x:auto"><table><thead><tr><th>نام</th><th>مصرف</th><th>IP</th><th>وضعیت</th><th>عملیات</th></tr></thead><tbody id="lb"></tbody></table></div>
<p style="font-size:11px;color:var(--muted);margin-top:8px">📌 <b>ساب</b> = برای برنامه‌ها · <b>صفحه</b> = پنل کاربری</p>
</section>
<section class="page" id="p-conn">
<div class="pt">اتصالات <span class="pulse"></span></div>
<div class="card"><table><thead><tr><th>اینباند</th><th>IP</th><th>ترافیک</th><th>از زمان</th></tr></thead><tbody id="cb"></tbody></table></div>
</section>
<section class="page" id="p-addr">
<div class="pt">آی‌پی تمیز</div>
<div class="card"><div class="grid2"><input id="new-addr" placeholder="IP/دامنه"><button class="btn bg" onclick="addAddr()">افزودن</button></div><div id="alist" style="margin-top:10px"></div></div>
</section>
<section class="page" id="p-tg">
<div class="pt">تلگرام</div>
<div class="card">
<p style="font-size:12px;color:var(--muted);margin-bottom:10px">توکن از @BotFather · آی‌دی از @userinfobot · ربات دکمه‌ای</p>
<input id="tg-tok" placeholder="توکن بات"><input id="tg-adm" placeholder="آی‌دی ادمین(ها)">
<div style="display:flex;gap:8px"><button class="btn bg" onclick="saveTg()">فعال‌سازی</button><button class="btn bd" onclick="stopTg()">توقف</button></div>
<div id="tg-st" style="margin-top:10px;font-size:13px;color:var(--muted)"></div>
</div>
</section>
<section class="page" id="p-domain">
<div class="pt">دامنه</div>
<div class="card"><input id="dom-in" placeholder="example.com"><button class="btn bg" onclick="saveDom()">ذخیره</button><div id="dom-cur" style="margin-top:8px;font-size:13px;color:var(--muted)"></div></div>
</section>
<section class="page" id="p-sec">
<div class="pt">امنیت</div>
<div class="card"><input type="password" id="cpw" placeholder="رمز فعلی"><input type="password" id="npw" placeholder="رمز جدید"><button class="btn bg" onclick="chPass()">تغییر</button></div>
</section>
</main>
<div class="modal-bg" id="addM" onclick="if(event.target===this)this.classList.remove('show')">
<div class="modal"><h3 style="color:var(--blue);margin-bottom:12px">افزودن اینباند</h3>
<input id="nl" placeholder="نام انگلیسی">
<div class="grid2"><input id="nlim" type="number" placeholder="حجم"><select id="nun"><option>GB</option><option>MB</option></select></div>
<input id="nexp" type="number" placeholder="روز"><input id="nmax" type="number" placeholder="حداکثر IP">
<button class="btn bg" style="width:100%" onclick="createL()">ساخت</button></div></div>
<div class="toast" id="toast"></div>
<script>
const $=s=>document.querySelector(s);let LANG=localStorage.getItem('vroom_dl')||'fa';
function setL(l){LANG=l;localStorage.setItem('vroom_dl',l);document.documentElement.lang=l;document.documentElement.dir=l==='fa'?'rtl':'ltr';$('#lFa').classList.toggle('on',l==='fa');$('#lEn').classList.toggle('on',l==='en');document.querySelectorAll('[data-fa]').forEach(el=>{el.textContent=l==='fa'?el.dataset.fa:el.dataset.en})}
document.querySelectorAll('.ni[data-p]').forEach(el=>el.onclick=()=>go(el.dataset.p));
function go(id){document.querySelectorAll('.page').forEach(p=>p.classList.remove('on'));document.getElementById('p-'+id)?.classList.add('on');
document.querySelectorAll('.ni').forEach(n=>n.classList.toggle('on',n.dataset.p===id));document.querySelector('.side')?.classList.remove('open');
if(id==='links')loadL();if(id==='conn')loadC();if(id==='addr')loadA();if(id==='tg')loadTg();if(id==='domain')loadDom()}
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2400)}
async function loadS(){try{const r=await fetch('/stats');if(!r.ok)return;const d=await r.json();
$('#s-cn').textContent=d.active_connections;$('#s-dl').textContent=d.download_fmt||'0';$('#s-ul').textContent=d.upload_fmt||'0';
$('#s-tr').textContent=d.total_fmt||(d.total_traffic_mb+' MB');$('#s-lk').textContent=d.links_count;$('#s-up').textContent=d.uptime;
$('#s-cpu').textContent=(d.cpu_percent||0).toFixed(0)+'%';$('#s-ram').textContent=(d.memory_percent||0).toFixed(0)+'%';window._conns=d.connections_detail||[]}catch(e){}}
async function loadL(){const r=await fetch('/api/links');const d=await r.json();const b=$('#lb');
if(!d.links?.length){b.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--muted)">خالی</td></tr>';return}
b.innerHTML=d.links.map(l=>{const u=(l.used_bytes/1e9).toFixed(2),lim=l.limit_bytes?(l.limit_bytes/1e9).toFixed(1)+'G':'∞';
const sub=l.sub_url||(location.origin+'/sub/'+l.uuid),page=l.page_url||(location.origin+'/page/'+l.uuid);
return `<tr><td><b>${l.label}</b></td><td>${u}/${lim}</td><td>${l.current_connections}/${l.max_connections||'∞'}</td>
<td><span class="tag ${l.active&&!l.expired?'ton':'toff'}">${l.active&&!l.expired?'روشن':'خاموش'}</span></td>
<td style="display:flex;gap:4px;flex-wrap:wrap">
<button class="btn bo" style="padding:3px 8px;font-size:10px" onclick="navigator.clipboard.writeText('${sub}').then(()=>toast('ساب'))">ساب</button>
<button class="btn bo" style="padding:3px 8px;font-size:10px" onclick="navigator.clipboard.writeText('${page}').then(()=>toast('صفحه'))">صفحه</button>
<button class="btn bo" style="padding:3px 8px;font-size:10px" onclick="navigator.clipboard.writeText('${l.vless_link.replace(/'/g,"\\\\'")}').then(()=>toast('کانفیگ'))">کپی</button>
<button class="btn bd" style="padding:3px 8px;font-size:10px" onclick="delL('${l.uuid}')">حذف</button></td></tr>`}).join('')}
function loadC(){const list=window._conns||[];const b=$('#cb');
if(!list.length){b.innerHTML='<tr><td colspan="4" style="text-align:center;color:var(--muted)">هیچ</td></tr>';return}
b.innerHTML=list.map(c=>`<tr><td>${c.uuid}</td><td>${c.ip}</td><td>${(c.bytes/1e6).toFixed(2)} MB</td><td style="font-size:11px">${(c.since||'').slice(11,19)}</td></tr>`).join('')}
async function delL(u){if(!confirm('حذف؟'))return;await fetch('/api/links/'+u,{method:'DELETE'});toast('OK');loadL();loadS()}
async function createL(){const label=$('#nl').value.trim(),limit=parseFloat($('#nlim').value)||0,unit=$('#nun').value,expiry=parseFloat($('#nexp').value)||0,max=parseInt($('#nmax').value)||0;
if(!label){toast('نام');return}
const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,limit_value:limit,limit_unit:unit,expiry_days:expiry,max_connections:max})});
if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'خطا');return}toast('ساخته شد');$('#addM').classList.remove('show');loadL();loadS()}
async function qc(gb){const n='u'+Math.floor(Math.random()*900+100);await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:n,limit_value:gb,limit_unit:'GB',expiry_days:30})});toast(n);loadS()}
async function resetAll(){if(!confirm('ریست همه؟'))return;await fetch('/api/reset-all-usage',{method:'POST'});toast('ریست');loadL()}
async function loadA(){const r=await fetch('/api/addresses');const d=await r.json();$('#alist').innerHTML=(d.addresses||[]).map((a,i)=>`<div style="display:flex;justify-content:space-between;padding:9px;background:rgba(148,163,184,.08);border-radius:10px;margin-bottom:5px;font-size:13px"><span>${a}</span><button class="btn bd" style="padding:3px 9px;font-size:11px" onclick="delA(${i})">حذف</button></div>`).join('')||'<div style="color:var(--muted);font-size:13px">خالی</div>'}
async function addAddr(){const a=$('#new-addr').value.trim();if(!a)return;await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});$('#new-addr').value='';loadA();toast('افزوده شد')}
async function delA(i){await fetch('/api/addresses/'+i,{method:'DELETE'});loadA()}
async function loadTg(){const r=await fetch('/api/telegram');const d=await r.json();$('#tg-st').innerHTML=d.enabled?'<span style="color:var(--green)">● روشن</span> — '+(d.admin_ids||[]).join(', '):'<span style="color:var(--red)">● خاموش</span>';if(d.admin_ids?.length)$('#tg-adm').value=d.admin_ids.join(' ')}
async function saveTg(){const r=await fetch('/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:$('#tg-tok').value.trim(),admin_ids:$('#tg-adm').value.trim()})});const d=await r.json().catch(()=>({}));if(!r.ok){toast(d.detail||'خطا');return}toast(d.enabled?'روشن @'+(d.bot_username||''):'ذخیره');loadTg()}
async function stopTg(){await fetch('/api/telegram/stop',{method:'POST'});toast('متوقف');loadTg()}
async function loadDom(){const r=await fetch('/api/domain');const d=await r.json();$('#dom-cur').textContent='فعلی: '+(d.domain||'پیش‌فرض')}
async function saveDom(){await fetch('/api/domain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domain:$('#dom-in').value.trim()})});toast('ذخیره');loadDom()}
async function chPass(){const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:$('#cpw').value,new_password:$('#npw').value})});if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'خطا');return}toast('تغییر یافت')}
setL(LANG);loadS();setInterval(loadS,5000);
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

# ================================================================
# ========== RUN ==========
# ================================================================
async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            d = get_domain()
            if d and d != "localhost":
                async with httpx.AsyncClient(timeout=10) as c:
                    await c.get(f"https://{d}/health")
        except Exception:
            pass

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
