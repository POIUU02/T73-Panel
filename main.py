#!/usr/bin/env python3
"""
VROOM Panel v5
- /sub/{uid}  → plain-text vless lines (FOR APPS — fixes import)
- /page/{uid} → beautiful bilingual panel + day/night + themes
- Dashboard bilingual
- Telegram button bot
- Easy Import with REAL app photos ONLY
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
import aiofiles

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

# Create static directory for icons
STATIC_DIR = Path("static/icons")
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

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

# ========== APP REAL PHOTOS ==========
APP_PHOTOS = {
    "hiddify": {
        "url": "https://raw.githubusercontent.com/hiddify/hiddify-app/main/assets/images/app_icon.png",
        "name": "Hiddify"
    },
    "v2rayng": {
        "url": "https://raw.githubusercontent.com/2dust/v2rayNG/master/app/src/main/res/mipmap-xxxhdpi/ic_launcher_round.png",
        "name": "v2rayNG"
    },
    "clash": {
        "url": "https://raw.githubusercontent.com/MetaCubeX/ClashMetaForAndroid/main/app/src/main/res/mipmap-xxxhdpi/ic_launcher.png",
        "name": "Clash Meta"
    },
    "surfboard": {
        "url": "https://surfboard.tools/assets/logo.png",
        "name": "Surfboard"
    },
    "v2box": {
        "url": "https://is1-ssl.mzstatic.com/image/thumb/Purple116/v4/8c/3a/8d/8c3a8d56-2b1c-8c3a-8d56-2b1c8c3a8d56/AppIcon-0-0-1x_U007emarketing-0-0-0-7-0-0-sRGB-0-0-0-GLES2_U002c0-512MB-85-220-0-0.png/512x512bb.jpg",
        "name": "V2Box"
    },
    "shadowrocket": {
        "url": "https://raw.githubusercontent.com/Hackl0us/Shadowrocket-ADBlock-Rules/master/icon.png",
        "name": "Shadowrocket"
    },
    "streisand": {
        "url": "https://raw.githubusercontent.com/StreisandEffect/streisand/master/icon.png",
        "name": "Streisand"
    },
    "v2rayn": {
        "url": "https://raw.githubusercontent.com/2dust/v2rayN/master/v2rayN/Resources/logo.ico",
        "name": "v2rayN"
    },
    "nekoray": {
        "url": "https://raw.githubusercontent.com/MatsuriDayo/nekoray/main/nekoray/logo.png",
        "name": "NekoRay"
    },
    "singbox": {
        "url": "https://raw.githubusercontent.com/SagerNet/sing-box/main/logo.png",
        "name": "Sing-box"
    }
}

async def download_photo(app_id: str):
    """Download app photo and save locally"""
    filepath = STATIC_DIR / f"{app_id}.png"
    if filepath.exists():
        return f"/static/icons/{app_id}.png"
    
    photo_data = APP_PHOTOS.get(app_id)
    if not photo_data:
        return None
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(photo_data["url"])
            if response.status_code == 200:
                async with aiofiles.open(filepath, 'wb') as f:
                    await f.write(response.content)
                logger.info(f"✅ Downloaded photo for {app_id}")
                return f"/static/icons/{app_id}.png"
    except Exception as e:
        logger.error(f"Failed to download photo for {app_id}: {e}")
    return None

@app.get("/api/app-photo/{app_id}")
async def get_app_photo(app_id: str):
    """API endpoint to get app photo URL"""
    photo_path = await download_photo(app_id)
    if photo_path:
        return {"url": photo_path}
    return {"url": None}

def hash_password(pw): return hashlib.sha256(f"{pw}{CONFIG['secret']}".encode()).hexdigest()
AUTH = {"password_hash": hash_password(os.environ.get("ADMIN_PASSWORD", "admin"))}
SESSIONS, SESSIONS_LOCK = {}, asyncio.Lock()

async def create_session():
    t = secrets.token_urlsafe(32)
    async with SESSIONS_LOCK: SESSIONS[t] = time.time() + SESSION_TTL
    return t

async def is_valid_session(token):
    if not token: return False
    async with SESSIONS_LOCK:
        exp = SESSIONS.get(token)
        if exp is None or exp < time.time():
            SESSIONS.pop(token, None); return False
        return True

async def destroy_session(token):
    if token:
        async with SESSIONS_LOCK: SESSIONS.pop(token, None)

async def require_auth(request: Request):
    if not await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        raise HTTPException(401, "unauthorized")
    return True

def get_domain():
    return (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "localhost").replace("https://", "").replace("http://", "").rstrip("/")

def generate_vless_link(uuid, remark="VROOM", address=None):
    domain = CUSTOM_DOMAIN if CUSTOM_DOMAIN else get_domain()
    addr = address if address else domain
    params = {"encryption": "none", "security": "tls", "type": "ws", "host": domain, "path": f"/ws/{uuid}", "sni": domain, "fp": "chrome", "alpn": "http/1.1"}
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark)}"

def uptime():
    secs = int(time.time() - stats["start_time"]); h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value, unit):
    u = (unit or "GB").upper()
    if u == "GB": return int(value * 1024 ** 3)
    if u == "MB": return int(value * 1024 ** 2)
    return int(value)

def compute_expiry(expiry_days):
    try: days = float(expiry_days or 0)
    except Exception: days = 0
    return "" if days <= 0 else (datetime.now() + timedelta(days=days)).isoformat()

def is_expired(link):
    exp = link.get("expiry") if isinstance(link, dict) else None
    if not exp: return False
    try: return datetime.now() >= datetime.fromisoformat(exp)
    except Exception: return False

def count_connections_for_link(uid): return len(link_ip_map.get(uid, set()))

def get_client_ip(websocket):
    f = websocket.headers.get("x-forwarded-for")
    if f: return f.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"

def remove_ip_from_link(uid, ip):
    if uid in link_ip_map:
        link_ip_map[uid].discard(ip)
        if not link_ip_map[uid]: link_ip_map.pop(uid, None)

async def close_connections_for_link(uid):
    for cid in [c for c, i in connections.items() if i.get("uuid") == uid]:
        ws = connection_sockets.get(cid)
        if ws:
            try: await ws.close(code=1000)
            except Exception: pass
        connections.pop(cid, None); connection_sockets.pop(cid, None)
    link_ip_map.pop(uid, None)

def fmt_bytes(b):
    if b >= 1024 ** 3: return f"{b/1024**3:.2f} GB"
    if b >= 1024 ** 2: return f"{b/1024**2:.1f} MB"
    return f"{b/1024:.0f} KB"

async def build_sub_content(uid, link):
    async with CUSTOM_ADDRESSES_LOCK: addresses = list(CUSTOM_ADDRESSES)
    lines = [generate_vless_link(uid, remark=f"VROOM-{link['label']}")]
    for i, addr in enumerate(addresses):
        lines.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))
    return "\n".join(lines)

# ---- Telegram (buttons) ----
def ikb(rows): return {"inline_keyboard": [[{"text": t, "callback_data": c} for t, c in row] for row in rows]}
def main_menu_kb(lang="fa"):
    if lang == "en":
        return ikb([[("➕ Create", "create_start"), ("📋 List", "list")], [("📊 Stats", "stats"), ("🔗 Sub link", "sub_menu")], [("🇮🇷 فارسی", "lang_fa"), ("ℹ️ Help", "help")]])
    return ikb([[("➕ ساخت", "create_start"), ("📋 لیست", "list")], [("📊 آمار", "stats"), ("🔗 لینک ساب", "sub_menu")], [("🇬🇧 English", "lang_en"), ("ℹ️ راهنما", "help")]])

def tg_lang(user_id):
    return (TG_STATE.get(user_id) or {}).get("lang", "fa")

async def tg_api(method, **kwargs):
    token = TELEGRAM.get("token")
    if not token: return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            return (await client.post(f"https://api.telegram.org/bot{token}/{method}", json=kwargs)).json()
    except Exception as e:
        logger.error(f"TG: {e}"); return None

async def tg_send(chat_id, text, reply_markup=None):
    d = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup: d["reply_markup"] = reply_markup
    return await tg_api("sendMessage", **d)

async def tg_edit(chat_id, message_id, text, reply_markup=None):
    d = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup: d["reply_markup"] = reply_markup
    return await tg_api("editMessageText", **d)

async def tg_answer(cq_id, text=None, show_alert=False):
    d = {"callback_query_id": cq_id}
    if text: d.update({"text": text, "show_alert": show_alert})
    return await tg_api("answerCallbackQuery", **d)

async def handle_callback(cq):
    data, cq_id = cq.get("data") or "", cq.get("id")
    msg = cq.get("message") or {}
    chat_id, message_id = msg.get("chat", {}).get("id"), msg.get("message_id")
    user_id = (cq.get("from") or {}).get("id")
    if user_id not in (TELEGRAM.get("admin_ids") or []):
        await tg_answer(cq_id, "Admin only", True); return
    await tg_answer(cq_id)
    lang = tg_lang(user_id)
    home = "🏠 منو" if lang == "fa" else "🏠 Menu"
    if data in ("lang_fa", "lang_en"):
        TG_STATE.setdefault(user_id, {})["lang"] = "fa" if data == "lang_fa" else "en"
        lang = tg_lang(user_id)
        txt = "🚀 <b>VROOM Bot</b>\nفقط با دکمه‌ها کار کن." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only."
        await tg_edit(chat_id, message_id, txt, reply_markup=main_menu_kb(lang)); return
    if data == "menu":
        for k in list((TG_STATE.get(user_id) or {}).keys()):
            if k != "lang": TG_STATE.get(user_id, {}).pop(k, None)
        txt = "🚀 <b>VROOM Bot</b>\nفقط با دکمه‌ها." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only."
        await tg_edit(chat_id, message_id, txt, reply_markup=main_menu_kb(lang)); return
    if data == "help":
        txt = "ℹ️ همه کارها با دکمه.\nساب + صفحه + کانفیگ داده می‌شه." if lang == "fa" else "ℹ️ Everything via buttons.\nSub + page + config are shared."
        await tg_edit(chat_id, message_id, txt, reply_markup=ikb([[(home, "menu")]])); return
    if data == "stats":
        async with LINKS_LOCK:
            n, active = len(LINKS), sum(1 for x in LINKS.values() if x.get("active") and not is_expired(x))
        if lang == "fa":
            t = f"📊 <b>آمار زنده</b>\n\n🔗 اینباند: {n} (فعال: {active})\n📡 اتصال: {len(connections)}\n📥 دانلود: {fmt_bytes(stats['download_bytes'])}\n📤 آپلود: {fmt_bytes(stats['upload_bytes'])}\n📦 کل: {fmt_bytes(stats['total_bytes'])}\n⏱️ آپتایم: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        else:
            t = f"📊 <b>Live Stats</b>\n\n🔗 Links: {n} (active: {active})\n📡 Conns: {len(connections)}\n📥 DL: {fmt_bytes(stats['download_bytes'])}\n📤 UL: {fmt_bytes(stats['upload_bytes'])}\n📦 Total: {fmt_bytes(stats['total_bytes'])}\n⏱️ Uptime: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("🔄", "stats"), (home, "menu")]])); return
    if data == "list":
        async with LINKS_LOCK: items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 خالی" if lang == "fa" else "📭 Empty", reply_markup=ikb([[("➕", "create_start"), (home, "menu")]])); return
        rows = [[(f"{'✅' if d.get('active') and not is_expired(d) else '❌'} {d['label']}", f"link:{uid}")] for uid, d in items[:15]]
        rows.append([(home, "menu")])
        await tg_edit(chat_id, message_id, "📋 اینباندها" if lang == "fa" else "📋 Inbounds", reply_markup=ikb(rows)); return
    if data.startswith("link:"):
        uid = data[5:]
        async with LINKS_LOCK: link = LINKS.get(uid)
        if not link:
            await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[("📋", "list"), (home, "menu")]])); return
        domain = get_domain()
        sub, page = f"https://{domain}/sub/{uid}", f"https://{domain}/page/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
        if lang == "fa":
            t = f"🏷 <b>{link['label']}</b>\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 اتصالات: {count_connections_for_link(uid)}\n\n📥 <b>لینک ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"🏷 <b>{link['label']}</b>\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 Conns: {count_connections_for_link(uid)}\n\n📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("🗑", f"delask:{uid}"), ("📋", "list")], [(home, "menu")]])); return
    if data.startswith("delask:"):
        uid = data[7:]
        q = f"حذف {uid}؟" if lang == "fa" else f"Delete {uid}?"
        await tg_edit(chat_id, message_id, q, reply_markup=ikb([[("✅", f"deldo:{uid}"), ("❌", f"link:{uid}")]])); return
    if data.startswith("deldo:"):
        uid = data[6:]
        async with LINKS_LOCK: LINKS.pop(uid, None)
        await close_connections_for_link(uid)
        await tg_edit(chat_id, message_id, "✅ حذف شد" if lang == "fa" else "✅ Deleted", reply_markup=ikb([[("📋", "list"), (home, "menu")]])); return
    if data == "sub_menu":
        async with LINKS_LOCK: items = list(LINKS.keys())[:12]
        if not items:
            await tg_edit(chat_id, message_id, "📭", reply_markup=ikb([[(home, "menu")]])); return
        rows = [[(u, f"showsub:{u}")] for u in items] + [[(home, "menu")]]
        await tg_edit(chat_id, message_id, "🔗 انتخاب کن:" if lang == "fa" else "🔗 Pick:", reply_markup=ikb(rows)); return
    if data.startswith("showsub:"):
        uid = data[8:]
        sub = f"https://{get_domain()}/sub/{uid}"
        page = f"https://{get_domain()}/page/{uid}"
        vless = generate_vless_link(uid, remark=f"VROOM-{uid}")
        if lang == "fa":
            t = f"📥 <b>ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
        await tg_send(chat_id, t); return
    if data == "create_start":
        prev = TG_STATE.get(user_id) or {}
        TG_STATE[user_id] = {"step": "label", "lang": prev.get("lang", "fa")}
        await tg_edit(chat_id, message_id, "➕ نام؟" if lang == "fa" else "➕ Name?", reply_markup=ikb([[("user1", "c_name:user1"), ("vip", "c_name:vip"), ("test", "c_name:test")], [("🎲", "c_name:rand"), ("❌", "menu")]])); return
    if data.startswith("c_name:"):
        name = data[7:]
        if name == "rand": name = "u" + secrets.token_hex(3)
        if not re.match(r"^[a-zA-Z0-9\-_.]+$", name):
            await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[(home, "menu")]])); return
        async with LINKS_LOCK:
            if name in LINKS:
                await tg_edit(chat_id, message_id, "❌ تکراری" if lang == "fa" else "❌ exists", reply_markup=ikb([[("➕", "create_start"), (home, "menu")]])); return
        st = TG_STATE.get(user_id) or {}
        TG_STATE[user_id] = {"step": "limit", "label": name, "lang": st.get("lang", "fa")}
        await tg_edit(chat_id, message_id, f"📦 حجم <b>{name}</b>؟" if lang == "fa" else f"📦 Volume <b>{name}</b>?", reply_markup=ikb([[("1GB", "c_lim:1"), ("5GB", "c_lim:5"), ("10GB", "c_lim:10")], [("50GB", "c_lim:50"), ("∞", "c_lim:0"), ("❌", "menu")]])); return
    if data.startswith("c_lim:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "limit":
            await tg_edit(chat_id, message_id, "Restart", reply_markup=main_menu_kb(lang)); return
        st["limit"] = float(data[6:]); st["step"] = "days"; TG_STATE[user_id] = st
        await tg_edit(chat_id, message_id, f"📅 روز <b>{st['label']}</b>؟" if lang == "fa" else f"📅 Days <b>{st['label']}</b>?", reply_markup=ikb([[("7", "c_day:7"), ("30", "c_day:30"), ("90", "c_day:90")], [("∞", "c_day:0"), ("❌", "menu")]])); return
    if data.startswith("c_day:"):
        st = TG_STATE.get(user_id) or {}
        if st.get("step") != "days":
            await tg_edit(chat_id, message_id, "Restart", reply_markup=main_menu_kb(lang)); return
        days, label, lim = float(data[6:]), st["label"], st.get("limit", 0)
        limit_bytes = parse_size_to_bytes(lim, "GB") if lim > 0 else 0
        async with LINKS_LOCK:
            if label in LINKS:
                await tg_edit(chat_id, message_id, "❌", reply_markup=ikb([[(home, "menu")]])); TG_STATE.pop(user_id, None); return
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
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("➕", "create_start"), ("📋", "list")], [(home, "menu")]])); return

async def handle_tg_message(msg):
    chat_id = (msg.get("chat") or {}).get("id")
    user_id = (msg.get("from") or {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id: return
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
        await tg_send(chat_id, "⛔"); return
    await tg_send(chat_id, "👇" if lang == "fa" else "Buttons 👇", reply_markup=main_menu_kb(lang))

async def telegram_poll_loop():
    logger.info("🤖 TG started")
    while True:
        try:
            async with TELEGRAM_LOCK:
                token, enabled, offset = TELEGRAM.get("token"), TELEGRAM.get("enabled"), TELEGRAM.get("offset", 0)
            if not token or not enabled:
                await asyncio.sleep(4); continue
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"offset": offset, "timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])})
                data = r.json()
            if not data.get("ok"):
                await asyncio.sleep(3); continue
            for upd in data.get("result", []):
                async with TELEGRAM_LOCK: TELEGRAM["offset"] = upd["update_id"] + 1
                try:
                    if "callback_query" in upd: await handle_callback(upd["callback_query"])
                    elif "message" in upd: await handle_tg_message(upd["message"])
                except Exception as e: logger.error(f"TG: {e}")
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"poll: {e}"); await asyncio.sleep(5)

async def start_telegram_bot():
    global TELEGRAM_TASK
    if TELEGRAM_TASK and not TELEGRAM_TASK.done():
        TELEGRAM_TASK.cancel()
        try: await TELEGRAM_TASK
        except Exception: pass
    TELEGRAM_TASK = asyncio.create_task(telegram_poll_loop())

async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            d = get_domain()
            if d and d != "localhost":
                async with httpx.AsyncClient(timeout=10) as c: await c.get(f"https://{d}/health")
        except Exception: pass

@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000), timeout=httpx.Timeout(180.0, connect=30.0), follow_redirects=True)
    logger.info(f"🚀 VROOM v5 :{CONFIG['port']}")
    asyncio.create_task(keep_alive())
    if TELEGRAM.get("token") and TELEGRAM.get("admin_ids"):
        TELEGRAM["enabled"] = True
        await start_telegram_bot()
    
    # Download all app photos in background
    asyncio.create_task(download_all_photos())

async def download_all_photos():
    """Download all app photos in background"""
    logger.info("📥 Downloading app photos...")
    tasks = []
    for app_id in APP_PHOTOS.keys():
        tasks.append(download_photo(app_id))
    await asyncio.gather(*tasks)
    logger.info("✅ App photos downloaded")

@app.on_event("shutdown")
async def shutdown():
    if http_client: await http_client.aclose()
    if TELEGRAM_TASK: TELEGRAM_TASK.cancel()

@app.get("/")
async def root(): return {"service": "VROOM", "version": "5.0", "domain": get_domain()}

@app.get("/health")
async def health(): return {"status": "ok", "connections": len(connections), "download": stats["download_bytes"], "upload": stats["upload_bytes"], "uptime": uptime()}

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
    resp = JSONResponse({"ok": True}); resp.delete_cookie(SESSION_COOKIE, path="/"); return resp

@app.get("/api/me")
async def api_me(request: Request): return {"authenticated": await is_valid_session(request.cookies.get(SESSION_COOKIE))}

@app.post("/api/change-password")
async def api_change_password(request: Request, _=Depends(require_auth)):
    body = await request.json()
    if hash_password(str(body.get("current_password") or "")) != AUTH["password_hash"]:
        raise HTTPException(400, "Wrong password")
    new = str(body.get("new_password") or "")
    if len(new) < 4: raise HTTPException(400, "Min 4 chars")
    AUTH["password_hash"] = hash_password(new); return {"ok": True}

@app.get("/stats")
async def get_stats(_=Depends(require_auth)):
    return {
        "active_connections": len(connections), "download_bytes": stats["download_bytes"], "upload_bytes": stats["upload_bytes"],
        "total_bytes": stats["total_bytes"], "download_fmt": fmt_bytes(stats["download_bytes"]), "upload_fmt": fmt_bytes(stats["upload_bytes"]),
        "total_fmt": fmt_bytes(stats["total_bytes"]), "total_traffic_mb": round(stats["total_bytes"] / (1024 * 1024), 2),
        "total_requests": stats["total_requests"], "total_errors": stats["total_errors"], "uptime": uptime(), "links_count": len(LINKS),
        "domain": get_domain(), "cpu_percent": psutil.cpu_percent(interval=0.05), "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent, "hourly_traffic": dict(hourly_traffic), "telegram_enabled": TELEGRAM.get("enabled", False),
        "connections_detail": [{"uuid": i.get("uuid"), "ip": i.get("ip"), "bytes": i.get("bytes", 0), "since": i.get("connected_at")} for i in connections.values()],
    }

@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json(); label = (body.get("label") or "New").strip()[:60]
    if not re.match(r"^[a-zA-Z0-9\-_. ]+$", label): raise HTTPException(400, "English only")
    async with LINKS_LOCK:
        if label in LINKS: raise HTTPException(400, "Exists")
    limit_value = float(body.get("limit_value") or 0); limit_unit = body.get("limit_unit") or "GB"
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    max_conn = max(0, int(body.get("max_connections") or 0)); expiry = compute_expiry(body.get("expiry_days"))
    async with LINKS_LOCK:
        LINKS[label] = {"label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn, "created_at": datetime.now().isoformat(), "active": True, "expiry": expiry}
    domain = get_domain()
    return {"uuid": label, "label": label, "limit_bytes": limit_bytes, "used_bytes": 0, "max_connections": max_conn, "active": True, "expiry": expiry,
            "vless_link": generate_vless_link(label, remark=f"VROOM-{label}"), "sub_url": f"https://{domain}/sub/{label}", "page_url": f"https://{domain}/page/{label}"}

@app.get("/api/links")
async def list_links(_=Depends(require_auth)):
    domain = get_domain(); result = []
    async with LINKS_LOCK:
        for uid, data in LINKS.items():
            result.append({"uuid": uid, "label": data["label"], "limit_bytes": data["limit_bytes"], "used_bytes": data["used_bytes"],
                "max_connections": data.get("max_connections", 0), "active": data["active"], "expiry": data.get("expiry", ""), "expired": is_expired(data),
                "created_at": data["created_at"], "current_connections": count_connections_for_link(uid),
                "vless_link": generate_vless_link(uid, remark=f"VROOM-{data['label']}"), "sub_url": f"https://{domain}/sub/{uid}", "page_url": f"https://{domain}/page/{uid}"})
    result.sort(key=lambda x: x["created_at"], reverse=True); return {"links": result}

@app.patch("/api/links/{uid}")
async def patch_link(uid: str, request: Request, _=Depends(require_auth)):
    body = await request.json()
    async with LINKS_LOCK:
        if uid not in LINKS: raise HTTPException(404)
        if "active" in body: LINKS[uid]["active"] = bool(body["active"])
        if "limit_value" in body:
            lv, lu = float(body.get("limit_value") or 0), body.get("limit_unit") or "GB"
            LINKS[uid]["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, lu)
        if body.get("reset_usage"): LINKS[uid]["used_bytes"] = 0
        if "expiry_days" in body: LINKS[uid]["expiry"] = compute_expiry(body.get("expiry_days"))
        if "max_connections" in body: LINKS[uid]["max_connections"] = max(0, int(body["max_connections"] or 0))
    return {"ok": True}

@app.delete("/api/links/{uid}")
async def delete_link(uid: str, _=Depends(require_auth)):
    async with LINKS_LOCK: LINKS.pop(uid, None)
    await close_connections_for_link(uid); return {"ok": True}

@app.post("/api/reset-all-usage")
async def reset_all_usage(_=Depends(require_auth)):
    async with LINKS_LOCK:
        for v in LINKS.values(): v["used_bytes"] = 0
    return {"ok": True}

@app.get("/api/domain")
async def get_domain_api(_=Depends(require_auth)):
    async with CUSTOM_DOMAIN_LOCK: return {"domain": CUSTOM_DOMAIN}

@app.post("/api/domain")
async def set_domain_api(request: Request, _=Depends(require_auth)):
    body = await request.json()
    domain = (body.get("domain") or "").strip().lower().replace("https://", "").replace("http://", "").rstrip("/")
    if domain and not re.match(r"^[a-z0-9\-_.]+$", domain): raise HTTPException(400, "Invalid")
    async with CUSTOM_DOMAIN_LOCK:
        global CUSTOM_DOMAIN; CUSTOM_DOMAIN = domain
    return {"ok": True, "domain": CUSTOM_DOMAIN}

@app.get("/api/addresses")
async def list_addresses(_=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK: return {"addresses": list(CUSTOM_ADDRESSES)}

@app.post("/api/addresses")
async def add_address(request: Request, _=Depends(require_auth)):
    body = await request.json(); address = (body.get("address") or "").strip()
    if not address: raise HTTPException(400, "Required")
    async with CUSTOM_ADDRESSES_LOCK:
        if address not in CUSTOM_ADDRESSES: CUSTOM_ADDRESSES.append(address)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

@app.delete("/api/addresses/{index}")
async def delete_address(index: int, _=Depends(require_auth)):
    async with CUSTOM_ADDRESSES_LOCK:
        if 0 <= index < len(CUSTOM_ADDRESSES): CUSTOM_ADDRESSES.pop(index)
        else: raise HTTPException(404)
    return {"ok": True, "addresses": list(CUSTOM_ADDRESSES)}

@app.get("/api/telegram")
async def get_tg(_=Depends(require_auth)):
    async with TELEGRAM_LOCK: return {"has_token": bool(TELEGRAM.get("token")), "admin_ids": TELEGRAM.get("admin_ids", []), "enabled": TELEGRAM.get("enabled", False)}

@app.post("/api/telegram")
async def set_tg(request: Request, _=Depends(require_auth)):
    body = await request.json(); token = (body.get("token") or "").strip()
    admin_raw = body.get("admin_ids") or body.get("admin_id") or ""
    admin_ids = [int(x) for x in admin_raw] if isinstance(admin_raw, list) else [int(x) for x in re.findall(r"-?\d+", str(admin_raw))]
    async with TELEGRAM_LOCK:
        if token: TELEGRAM["token"] = token
        if admin_ids: TELEGRAM["admin_ids"] = admin_ids
        TELEGRAM["enabled"] = bool(TELEGRAM.get("token") and TELEGRAM.get("admin_ids"))
    if TELEGRAM["enabled"]:
        me = await tg_api("getMe")
        if not me or not me.get("ok"):
            async with TELEGRAM_LOCK: TELEGRAM["enabled"] = False
            raise HTTPException(400, "Invalid bot token")
        await start_telegram_bot()
        return {"ok": True, "enabled": True, "bot_username": me["result"].get("username"), "admin_ids": TELEGRAM["admin_ids"]}
    return {"ok": True, "enabled": False}

@app.post("/api/telegram/stop")
async def stop_tg(_=Depends(require_auth)):
    async with TELEGRAM_LOCK: TELEGRAM["enabled"] = False
    global TELEGRAM_TASK
    if TELEGRAM_TASK and not TELEGRAM_TASK.done(): TELEGRAM_TASK.cancel()
    return {"ok": True}

# ========== FIX: plain text sub for apps ==========
@app.get("/sub/{uid}")
async def subscription_raw(uid: str, request: Request):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None: raise HTTPException(404, "Not found")
    if not link["active"]: raise HTTPException(403, "Disabled")
    if is_expired(link): raise HTTPException(403, "Expired")
    content = await build_sub_content(uid, link)
    used, total, expire_ts = link["used_bytes"], (link["limit_bytes"] if link["limit_bytes"] > 0 else 0), 0
    if link.get("expiry"):
        try: expire_ts = int(datetime.fromisoformat(link["expiry"]).timestamp())
        except Exception: pass
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

# ========== Beautiful page (bilingual + day/night + themes) ==========
@app.get("/page/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None: raise HTTPException(404)
    if not link["active"] or is_expired(link): raise HTTPException(403)
    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / 1024 ** 3, 2)
    limit_gb = round(link["limit_bytes"] / 1024 ** 3, 2) if link["limit_bytes"] else 0
    percent = round((link["used_bytes"] / link["limit_bytes"]) * 100, 1) if link["limit_bytes"] else 0
    remaining = round(max(0, limit_gb - used_gb), 2) if limit_gb else "∞"
    if is_expired(link): status_fa, status_en, sc = "منقضی", "Expired", "#f87171"
    elif link["limit_bytes"] and link["used_bytes"] >= link["limit_bytes"]: status_fa, status_en, sc = "محدود", "Limited", "#fbbf24"
    else: status_fa, status_en, sc = "فعال", "Active", "#34d399"
    exp = link.get("expiry")
    if exp:
        try:
            ed = datetime.fromisoformat(exp); days_left = max(0, (ed - datetime.now()).days)
            days_fa, days_en, exp_disp = f"{days_left} روز", f"{days_left} days", ed.strftime("%Y/%m/%d")
        except Exception: days_fa = days_en = exp_disp = "∞"
    else: days_fa = days_en = exp_disp = "∞"
    domain = get_domain(); sub_url = f"https://{domain}/sub/{uid}"; qr_data = quote(server_link, safe="")
    live_conns = count_connections_for_link(uid)

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="dark"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"/>
<title>VROOM — {link['label']}</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@800;900&family=Vazirmatn:wght@400;700;800;900&display=swap" rel="stylesheet"/>
<style>
:root{{--bg:#0b0b14;--card:rgba(20,20,38,.96);--g:#e8c547;--g2:#e09a2a;--t:#e6e8f0;--m:rgba(230,232,240,.55);--b:rgba(232,197,71,.16);--ac:#9b8afb;--bl:#4f8cff;--gn:#3ecf8e}}
html[data-theme=light]{{--bg:#e8ebf2;--card:#f3f5fa;--t:#1e2433;--m:rgba(30,36,51,.55);--b:rgba(100,90,200,.16);--g:#b8941a;--g2:#c47a18;--ac:#6b5ce0}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:Vazirmatn,Inter,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;display:flex;justify-content:center;padding:16px 12px;transition:.4s;
background-image:radial-gradient(ellipse at 12% 0%,rgba(155,138,251,.14),transparent 48%),radial-gradient(ellipse at 88% 100%,rgba(232,197,71,.09),transparent 42%)}}
.w{{max-width:460px;width:100%}}
.top{{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}}
.logo{{display:flex;align-items:center;gap:8px}}.logo b{{font-family:Inter;font-size:20px;font-weight:900;background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.mark{{width:34px;height:34px;border-radius:11px;background:linear-gradient(135deg,var(--g),var(--g2),var(--ac));display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(255,215,0,.3)}}
.mark svg{{width:18px;height:18px;stroke:#0a0a10;fill:none;stroke-width:2}}
.ib{{width:34px;height:34px;border-radius:10px;border:1px solid var(--b);background:var(--card);color:var(--g);cursor:pointer;display:flex;align-items:center;justify-content:center}}
.ib svg{{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:1.7}}
.card{{background:var(--card);border:1px solid var(--b);border-radius:18px;padding:15px;margin-bottom:11px;box-shadow:0 8px 28px rgba(0,0,0,.12)}}
.title{{font-size:20px;font-weight:900;font-family:Inter}}.sub{{font-size:12px;color:var(--g);margin:3px 0 10px}}
.live{{display:flex;justify-content:space-between;align-items:center;padding:9px 12px;border-radius:12px;margin-bottom:11px;background:linear-gradient(135deg,rgba(52,211,153,.1),rgba(59,130,246,.08));border:1px solid rgba(52,211,153,.25);font-size:12px;font-weight:700;color:var(--gn);flex-wrap:wrap;gap:6px}}
.dot{{width:8px;height:8px;border-radius:50%;background:var(--gn);display:inline-block;margin-left:6px;animation:p 1.6s infinite}}
@keyframes p{{0%{{box-shadow:0 0 0 0 rgba(52,211,153,.5)}}70%{{box-shadow:0 0 0 8px transparent}}}}
.ug{{display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;align-items:center}}
@media(max-width:400px){{.ug{{grid-template-columns:1fr 1fr}}.rw{{grid-column:1/-1;justify-self:center}}}}
.st{{text-align:center}}.st .l{{font-size:10px;color:var(--m)}}.st .v{{font-size:14px;font-weight:800}}
.rw{{width:70px;height:70px;position:relative;display:flex;align-items:center;justify-content:center}}
.ro{{position:absolute;inset:0;border-radius:50%;border:1.5px solid rgba(59,130,246,.3)}}
.rg{{width:60px;height:60px;border-radius:50%;background:conic-gradient(#3b82f6 0% 0%,rgba(30,41,59,.85) 0% 100%);display:flex;align-items:center;justify-content:center;position:relative;transition:background 1s}}
.rg::before{{content:'';position:absolute;inset:6px;border-radius:50%;background:var(--bg)}}
.rt{{position:relative;z-index:1;font-size:12px;font-weight:800}}
.bar{{margin-top:11px;height:5px;background:rgba(30,41,59,.4);border-radius:8px;overflow:hidden}}
.bf{{height:100%;width:0;background:linear-gradient(90deg,#3b82f6,#a78bfa,#f472b6);border-radius:8px;transition:width 1s}}
.h3{{font-size:11px;font-weight:700;color:var(--g);opacity:.9;margin-bottom:9px;display:flex;align-items:center;gap:6px;letter-spacing:.5px}}
.h3 svg{{width:13px;height:13px;stroke:currentColor;fill:none;stroke-width:1.8}}
.row{{background:rgba(0,0,0,.18);padding:9px 11px;border-radius:11px;display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:9px;font-size:10px;font-family:monospace;color:var(--m);border:1px solid var(--b);cursor:pointer}}
.row button{{background:linear-gradient(135deg,var(--g),var(--g2));border:none;color:#0a0a10;padding:5px 11px;border-radius:8px;font-size:11px;font-weight:800;cursor:pointer;font-family:inherit}}
.row .lt{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.g3{{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:9px;padding-top:9px;border-top:1px solid var(--b)}}
.ii{{background:rgba(0,0,0,.12);padding:8px 5px;border-radius:11px;text-align:center;border:1px solid var(--b)}}.ii .l{{font-size:9px;opacity:.45;display:block}}.ii .v{{font-size:12px;font-weight:800}}
.cfg{{background:rgba(0,0,0,.2);padding:9px;border-radius:11px;font-size:10px;font-family:monospace;word-break:break-all;margin-bottom:9px;max-height:52px;overflow-y:auto;border:1px solid var(--b);direction:ltr;text-align:left;color:var(--m);cursor:pointer}}
.qr{{width:112px;height:112px;background:#fff;border-radius:13px;margin:0 auto 9px;overflow:hidden;border:2px solid rgba(255,215,0,.3);cursor:pointer}}.qr img{{width:100%;height:100%}}
.btns{{display:flex;gap:7px}}.btns button{{flex:1;padding:11px;border:none;border-radius:11px;font-weight:800;font-size:12px;cursor:pointer;font-family:inherit}}
.b1{{background:linear-gradient(135deg,var(--g),var(--g2),#f472b6);color:#0a0a10;box-shadow:0 4px 16px rgba(255,215,0,.25)}}
.b2{{background:rgba(255,215,0,.08);color:var(--g);border:1px solid var(--b)}}
.easy-title{{font-size:13px;font-weight:800;margin-bottom:10px;color:var(--t)}}
.plat{{display:flex;gap:6px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:8px;margin-bottom:10px;scrollbar-width:none}}
.plat::-webkit-scrollbar{{display:none}}
.plat-btn{{flex-shrink:0;padding:7px 14px;border-radius:20px;border:1px solid var(--b);background:rgba(255,255,255,.03);color:var(--m);font-size:11px;font-weight:700;cursor:pointer;font-family:inherit;transition:.2s}}
.plat-btn.on{{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10;border-color:transparent;box-shadow:0 4px 14px rgba(232,197,71,.25)}}
.apps{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}
@media(max-width:400px){{.apps{{grid-template-columns:repeat(3,1fr)}}}}
.app{{background:rgba(255,255,255,.04);border:1px solid var(--b);border-radius:16px;padding:12px 6px 10px;text-align:center;cursor:pointer;position:relative;transition:.2s}}
.app:active{{transform:scale(.96)}}
.app-photo{{width:54px;height:54px;margin:0 auto 7px;border-radius:16px;overflow:hidden;box-shadow:0 6px 18px rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;background:#1a1a2e;transition:transform .2s ease}}
.app:hover .app-photo{{transform:scale(1.05)}}
.app-photo img{{width:100%;height:100%;object-fit:cover;display:block;border-radius:14px}}
.app-name{{font-size:10px;font-weight:700;color:var(--t)}}
.badge{{position:absolute;top:6px;right:6px;font-size:9px;background:rgba(232,197,71,.22);color:var(--g);padding:2px 5px;border-radius:6px;font-weight:800}}
footer{{text-align:center;font-size:11px;color:var(--m);margin-top:6px;padding-top:9px;border-top:1px solid var(--b)}}
footer b{{background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{position:fixed;bottom:22px;left:50%;transform:translateX(-50%) translateY(70px);background:var(--card);padding:11px 20px;border-radius:11px;font-size:12px;color:var(--g);opacity:0;transition:.3s;border:1px solid var(--b);z-index:9999;font-weight:700}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
.lp{{display:inline-flex;border-radius:18px;border:1px solid var(--b);overflow:hidden;font-size:10px;font-weight:700}}
.lp button{{border:none;padding:5px 10px;cursor:pointer;background:transparent;color:var(--m);font-family:inherit;font-weight:700}}
.lp button.on{{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}}
</style></head><body>
<div class="w">
<div class="top">
  <div class="logo"><div class="mark"><svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg></div><b>VROOM</b></div>
  <div style="display:flex;gap:6px;align-items:center">
    <div class="lp"><button type="button" id="langFa" class="on" onclick="setLang('fa')">FA</button><button type="button" id="langEn" onclick="setLang('en')">EN</button></div>
    <button class="ib" onclick="toggleDN()" title="Day/Night"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg></button>
  </div>
</div>
<div class="card">
  <div class="title" data-i="title">Subscription</div>
  <div class="sub">✦ {link['label']} ✦</div>
  <div class="live"><span><span class="dot"></span><span data-i="online">سرور آنلاین</span></span><span style="color:var(--m);font-weight:600"><span data-i="conn">اتصالات</span>: <b style="color:var(--t)">{live_conns}</b></span></div>
  <div class="ug">
    <div class="st"><div class="l" data-i="used">مصرفی</div><div class="v">{used_gb} GB</div></div>
    <div class="st"><div class="l" data-i="status">وضعیت</div><div class="v" style="color:{sc};font-size:12px" id="stTxt">{status_fa}</div></div>
    <div class="st"><div class="l" data-i="left">باقی</div><div class="v">{remaining}{' GB' if remaining!='∞' else ''}</div></div>
    <div class="rw"><div class="ro"></div><div class="rg" id="rg"><div class="rt" id="pct">0%</div></div></div>
  </div>
  <div class="bar"><div class="bf" id="bf"></div></div>
</div>
<div class="card">
  <div class="h3"><svg viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg><span data-i="subLink">لینک ساب (برنامه‌ها)</span></div>
  <div class="row" onclick="cp(SUB)"><span class="lt">{sub_url}</span><button type="button" onclick="event.stopPropagation();cp(SUB)" data-i="copy">کپی</button></div>
  <div class="g3">
    <div class="ii"><span class="l" data-i="status">وضعیت</span><span class="v" style="color:{sc}" id="st2">{status_fa}</span></div>
    <div class="ii"><span class="l" data-i="expire">انقضا</span><span class="v" style="color:#fbbf24">{exp_disp}</span></div>
    <div class="ii"><span class="l" data-i="days">باقی</span><span class="v" style="color:#6bcbff" id="daysT">{days_fa}</span></div>
  </div>
</div>
<div class="card">
  <div class="h3"><svg viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M7 7h.01M17 7h.01M7 17h.01M17 17h.01M12 12h.01"/></svg><span data-i="cfg">کانفیگ و QR</span></div>
  <div class="cfg" onclick="cp(CFG)">{server_link}</div>
  <div style="text-align:center">
    <div class="qr" onclick="document.getElementById('qrm').style.display='flex'"><img src="https://api.qrserver.com/v1/create-qr-code/?size=220x220&data={qr_data}" alt="QR"/></div>
    <div class="btns"><button type="button" class="b1" onclick="cp(SUB)" data-i="add">＋ اضافه</button><button type="button" class="b2" onclick="share()" data-i="share">اشتراک</button></div>
  </div>
</div>
<div class="card">
  <div class="easy-title" data-i="easy">Easy Import</div>
  <div class="plat" id="platBar">
    <button type="button" class="plat-btn on" data-p="android" onclick="showPlat('android',this)">Android</button>
    <button type="button" class="plat-btn" data-p="ios" onclick="showPlat('ios',this)">iOS</button>
    <button type="button" class="plat-btn" data-p="windows" onclick="showPlat('windows',this)">Windows</button>
    <button type="button" class="plat-btn" data-p="macos" onclick="showPlat('macos',this)">macOS</button>
    <button type="button" class="plat-btn" data-p="linux" onclick="showPlat('linux',this)">Linux</button>
    <button type="button" class="plat-btn" data-p="tv" onclick="showPlat('tv',this)">Android TV</button>
    <button type="button" class="plat-btn" data-p="appletv" onclick="showPlat('appletv',this)">Apple TV</button>
  </div>
  <div class="apps" id="appsGrid"></div>
</div>
<footer>Powered by <b>VROOM</b></footer>
</div>
<div id="qrm" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.88);align-items:center;justify-content:center;z-index:10000" onclick="this.style.display='none'">
<img style="width:min(80vw,300px);border-radius:16px;border:2px solid rgba(255,215,0,.4)" src="https://api.qrserver.com/v1/create-qr-code/?size=360x360&data={qr_data}" alt="QR"/>
</div>
<div class="toast" id="toast"></div>
<script>
const SUB='{sub_url}',CFG=`{server_link}`,P={percent};
const ST={{fa:'{status_fa}',en:'{status_en}'}}, DY={{fa:'{days_fa}',en:'{days_en}'}};
const I18N={{fa:{{title:'Subscription',online:'سرور آنلاین',conn:'اتصالات',used:'مصرفی',status:'وضعیت',left:'باقی',subLink:'لینک ساب (برنامه‌ها)',copy:'کپی',expire:'انقضا',days:'باقی',cfg:'کانفیگ و QR',add:'＋ اضافه اشتراک',share:'اشتراک',easy:'Easy Import'}},
en:{{title:'Subscription',online:'Server Online',conn:'Connections',used:'Used',status:'Status',left:'Left',subLink:'Sub link (for apps)',copy:'Copy',expire:'Expiry',days:'Left',cfg:'Config & QR',add:'＋ Add Sub',share:'Share',easy:'Easy Import'}}}};
let lang=localStorage.getItem('vroom_lang')||'fa', dn=localStorage.getItem('vroom_dn')||'dark';
function setLang(l){{lang=l;localStorage.setItem('vroom_lang',l);document.documentElement.lang=l;document.documentElement.dir=l==='fa'?'rtl':'ltr';
document.getElementById('langFa').classList.toggle('on',l==='fa');document.getElementById('langEn').classList.toggle('on',l==='en');
const t=I18N[l];document.querySelectorAll('[data-i]').forEach(el=>{{const k=el.getAttribute('data-i');if(t[k])el.textContent=t[k]}});
document.getElementById('stTxt').textContent=ST[l];document.getElementById('st2').textContent=ST[l];document.getElementById('daysT').textContent=DY[l]}}
function toggleDN(){{dn=dn==='dark'?'light':'dark';localStorage.setItem('vroom_dn',dn);document.documentElement.setAttribute('data-theme',dn)}}

// ========== APP CATALOG WITH REAL PHOTOS ONLY ==========
const CATALOG = {{
  android: [
    {{id:'hiddify',name:'Hiddify',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayng',name:'v2rayNG',bg:'#1E88E5',s:'v2rayng://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'clash',name:'Clash Meta',bg:'#D63031',s:'clash://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'surfboard',name:'Surfboard',bg:'#00B894',s:'surfboard://import?url='+encodeURIComponent(SUB)}}
  ],
  ios: [
    {{id:'hiddify',name:'Hiddify',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2box',name:'V2Box',bg:'#6C5CE7',s:'v2box://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'shadowrocket',name:'Shadowrocket',bg:'#E84393',s:'shadowrocket://add/sub://'+btoa(SUB).replace(/\\+/g,'-').replace(/\\//g,'_')}},
    {{id:'streisand',name:'Streisand',bg:'#FF6B6B',s:'streisand://import/'+encodeURIComponent(SUB)}}
  ],
  windows: [
    {{id:'hiddify',name:'Hiddify',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayn',name:'v2rayN',bg:'#0984E3',s:'v2rayN://import?url='+encodeURIComponent(SUB)}},
    {{id:'nekoray',name:'NekoRay',bg:'#F39C12',s:'nekoray://import?url='+encodeURIComponent(SUB)}},
    {{id:'singbox',name:'Sing-box',bg:'#00B894',s:'sing-box://import-remote-profile?url='+encodeURIComponent(SUB)}}
  ],
  macos: [
    {{id:'hiddify',name:'Hiddify',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2box',name:'V2Box',bg:'#6C5CE7',s:'v2box://install-config?url='+encodeURIComponent(SUB)}},
    {{id:'singbox',name:'Sing-box',bg:'#00B894',s:'sing-box://import-remote-profile?url='+encodeURIComponent(SUB)}}
  ],
  linux: [
    {{id:'hiddify',name:'Hiddify',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'nekoray',name:'NekoRay',bg:'#F39C12',s:'nekoray://import?url='+encodeURIComponent(SUB)}},
    {{id:'singbox',name:'Sing-box',bg:'#00B894',s:'sing-box://import-remote-profile?url='+encodeURIComponent(SUB)}}
  ],
  tv: [
    {{id:'hiddify',name:'Hiddify TV',bg:'#455FE9',s:'hiddify://import/'+encodeURIComponent(SUB)}},
    {{id:'v2rayng',name:'v2rayNG',bg:'#1E88E5',s:'v2rayng://install-config?url='+encodeURIComponent(SUB)}}
  ],
  appletv: [
    {{id:'streisand',name:'Streisand',bg:'#FF6B6B',s:'streisand://import/'+encodeURIComponent(SUB)}},
    {{id:'shadowrocket',name:'Shadowrocket',bg:'#E84393',s:'shadowrocket://add/sub://'+btoa(SUB).replace(/\\+/g,'-').replace(/\\//g,'_')}}
  ]
}};

// Load app photo from server
async function getAppPhoto(appId) {{
    try {{
        const response = await fetch(`/api/app-photo/${{appId}}`);
        if (response.ok) {{
            const data = await response.json();
            return data.url;
        }}
    }} catch (e) {{
        console.log('Photo load failed:', e);
    }}
    return null;
}}

// Show platform with real photos
async function showPlat(p, btn) {{
    document.querySelectorAll('.plat-btn').forEach(b => b.classList.remove('on'));
    if (btn) btn.classList.add('on');
    
    const list = CATALOG[p] || [];
    const grid = document.getElementById('appsGrid');
    grid.innerHTML = list.map(a => `
        <div class="app" onclick="oaApp('${{a.id}}','${{p}}')" id="app-${{a.id}}">
            <span class="badge">＋</span>
            <div class="app-photo" style="background:${{a.bg}}">
                <div style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:700;text-align:center;padding:4px;">Loading...</div>
            </div>
            <div class="app-name">${{a.name}}</div>
        </div>
    `).join('');
    
    // Load photos
    for (const a of list) {{
        const photoUrl = await getAppPhoto(a.id);
        const photoDiv = document.querySelector(`#app-${{a.id}} .app-photo`);
        if (photoUrl) {{
            photoDiv.innerHTML = `<img src="${{photoUrl}}" alt="${{a.name}}" 
                onerror="this.parentElement.innerHTML='<div style=\\'width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:700;text-align:center;padding:4px;\\'>${{a.name}}</div>'" 
                style="width:100%;height:100%;object-fit:cover;display:block;border-radius:14px"/>`;
        }} else {{
            photoDiv.innerHTML = `<div style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;color:white;font-size:10px;font-weight:700;text-align:center;padding:4px;">${{a.name}}</div>`;
        }}
    }}
}}

function oaApp(id, plat) {{
    const a = (CATALOG[plat] || []).find(x => x.id === id);
    if (!a) return;
    if (!a.s) {{
        cp(SUB);
        toast(lang === 'fa' ? 'لینک ساب کپی شد — در برنامه Import کن' : 'Sub copied — import in app');
        return;
    }}
    try {{
        location.href = a.s;
    }} catch(e) {{}}
    setTimeout(() => {{
        cp(SUB);
        toast(lang === 'fa' ? 'اگر باز نشد، ساب کپی شد' : 'If app did not open, sub was copied');
    }}, 900);
}}

function cp(t){{navigator.clipboard?navigator.clipboard.writeText(t).then(()=>toast(lang==='fa'?'کپی شد':'Copied')):(()=>{{const i=document.createElement('input');i.value=t;document.body.appendChild(i);i.select();document.execCommand('copy');document.body.removeChild(i);toast('OK')}})()}}
function share(){{navigator.share?navigator.share({{title:'VROOM',url:SUB}}).catch(()=>cp(SUB)):cp(SUB)}}
function toast(m){{const t=document.getElementById('toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2400)}}
document.documentElement.setAttribute('data-theme',dn);setLang(lang);showPlat('android',document.querySelector('.plat-btn'));
setTimeout(()=>{{document.getElementById('rg').style.background=`conic-gradient(#4f8cff 0% ${{P}}%,rgba(30,41,59,.85) ${{P}}% 100%)`;document.getElementById('bf').style.width=P+'%';document.getElementById('pct').textContent=P+'%'}},250);
</script></body></html>"""
    return HTMLResponse(content=html)

# ---- WS proxy ----
RELAY_BUF = 2 * 1024 * 1024

async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24: raise ValueError("small")
    pos = 1 + 16; addon_len = first_chunk[pos]; pos += 1 + addon_len; pos += 1
    port = int.from_bytes(first_chunk[pos:pos + 2], "big"); pos += 2
    addr_type = first_chunk[pos]; pos += 1
    if addr_type == 1:
        address = ".".join(str(b) for b in first_chunk[pos:pos + 4]); pos += 4
    elif addr_type == 2:
        dl = first_chunk[pos]; pos += 1; address = first_chunk[pos:pos + dl].decode("utf-8", errors="ignore"); pos += dl
    elif addr_type == 3:
        address = ":".join(f"{first_chunk[pos+i]:02x}{first_chunk[pos+i+1]:02x}" for i in range(0, 16, 2)); pos += 16
    else: raise ValueError("addr")
    return address, port, first_chunk[pos:]

async def add_usage(uid, n, direction="total"):
    async with LINKS_LOCK:
        if uid in LINKS: LINKS[uid]["used_bytes"] += n
    stats["total_bytes"] += n
    if direction == "up": stats["upload_bytes"] += n
    elif direction == "down": stats["download_bytes"] += n

async def ws_to_tcp(ws, writer, conn_id, uid):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect": break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data: continue
            size = len(data); stats["total_requests"] += 1; connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(uid, size, "up"); writer.write(data); await writer.drain()
    except WebSocketDisconnect: pass
    finally:
        try: writer.write_eof()
        except Exception: pass

async def tcp_to_ws(ws, reader, conn_id, uid):
    first = True
    try:
        while True:
            data = await reader.read(RELAY_BUF)
            if not data: break
            size = len(data); connections[conn_id]["bytes"] += size
            hourly_traffic[datetime.now().strftime("%H:00")] += size
            await add_usage(uid, size, "down")
            await ws.send_bytes((b"\x00\x00" + data) if first else data); first = False
    except Exception: pass

@app.websocket("/ws/{uuid}")
async def websocket_tunnel(websocket: WebSocket, uuid: str):
    await websocket.accept(); writer = None; conn_id = None; client_ip = get_client_ip(websocket)
    try:
        async with LINKS_LOCK:
            link_data = LINKS.get(uuid)
            if not link_data or not link_data["active"] or is_expired(link_data):
                await websocket.close(code=1008); return
            max_conn = link_data.get("max_connections", 0)
        if max_conn > 0 and client_ip not in link_ip_map.get(uuid, set()) and count_connections_for_link(uuid) >= max_conn:
            await websocket.close(code=1008, reason="limit"); return
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=10)
        if first_msg["type"] == "websocket.disconnect": return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk: return
        address, port, payload = await parse_vless_header(first_chunk)
        conn_id = secrets.token_urlsafe(8)
        connections[conn_id] = {"uuid": uuid, "ip": client_ip, "connected_at": datetime.now().isoformat(), "bytes": 0}
        connection_sockets[conn_id] = websocket; link_ip_map[uuid].add(client_ip)
        await add_usage(uuid, len(first_chunk), "up"); connections[conn_id]["bytes"] += len(first_chunk)
        reader, writer = await asyncio.wait_for(asyncio.open_connection(address, port), timeout=5)
        if payload:
            await add_usage(uuid, len(payload), "up"); writer.write(payload); await writer.drain()
        t1 = asyncio.create_task(ws_to_tcp(websocket, writer, conn_id, uuid))
        t2 = asyncio.create_task(tcp_to_ws(websocket, reader, conn_id, uuid))
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending: t.cancel()
    except Exception as e:
        stats["total_errors"] += 1; error_logs.append({"error": str(e), "time": datetime.now().isoformat()})
    finally:
        if writer:
            try: writer.close()
            except Exception: pass
        if conn_id:
            info = connections.pop(conn_id, None); connection_sockets.pop(conn_id, None)
            if info:
                uid, ip = info.get("uuid"), info.get("ip")
                if uid and ip and not any(c.get("uuid") == uid and c.get("ip") == ip for c in connections.values()):
                    remove_ip_from_link(uid, ip)

LOGIN_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@700;900&display=swap" rel="stylesheet">
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:Vazirmatn,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#07071a;color:#f0f2f8;direction:rtl;background-image:radial-gradient(ellipse at 20% 20%,rgba(124,92,252,.15),transparent 50%)}
.card{background:rgba(18,18,36,.95);border:1px solid rgba(255,215,0,.14);border-radius:24px;padding:40px 32px;width:100%;max-width:380px}
h1{font-size:28px;font-weight:900;text-align:center;background:linear-gradient(135deg,#ffd700,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:20px}
input{width:100%;padding:12px;background:rgba(0,0,0,.35);border:1px solid rgba(255,215,0,.14);border-radius:12px;color:#fff;font-size:14px;font-family:inherit;outline:none;margin-bottom:12px}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#ffd700,#f7971e);border:none;border-radius:12px;color:#0a0a10;font-size:15px;font-weight:800;cursor:pointer;font-family:inherit}
.err{color:#f87171;font-size:13px;text-align:center;display:none;margin-bottom:10px}.err.show{display:block}</style></head><body>
<div class="card"><h1>VROOM</h1><div class="err" id="err"></div>
<form id="f"><input type="password" id="pw" placeholder="Password / رمز" autofocus><button class="btn" type="submit">Login / ورود</button></form></div>
<script>document.getElementById('f').onsubmit=async e=>{e.preventDefault();const err=document.getElementById('err');err.classList.remove('show');
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
if(!r.ok)throw new Error('Wrong password');location.href='/dashboard'}catch(ex){err.textContent=ex.message;err.classList.add('show')}}</script></body></html>"""

DASHBOARD_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1"><title>VROOM Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;700;800;900&family=Inter:wght@800&display=swap" rel="stylesheet">
<style>
:root{--bg:#07071a;--s:#121224;--g:#ffd700;--g2:#f7971e;--t:#f0f2f8;--t2:rgba(255,255,255,.5);--b:rgba(255,215,0,.12);--gn:#34d399;--rd:#f87171;--ac:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}body{font-family:Vazirmatn,sans-serif;background:var(--bg);color:var(--t);min-height:100vh;direction:rtl;background-image:radial-gradient(ellipse at 15% 0%,rgba(124,92,252,.1),transparent 50%)}
.side{width:190px;background:#0a0a14;border-left:1px solid var(--b);position:fixed;right:0;top:0;bottom:0;padding:12px 8px;display:flex;flex-direction:column;z-index:40}
.brand{font-size:17px;font-weight:900;background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:Inter;padding:6px;margin-bottom:10px}
.ni{padding:8px 10px;border-radius:9px;font-size:11px;font-weight:600;color:var(--t2);cursor:pointer;margin-bottom:2px;border:none;background:none;width:100%;text-align:right;font-family:inherit}
.ni:hover,.ni.on{background:rgba(255,215,0,.08);color:var(--g)}.main{margin-right:190px;padding:16px 12px}
.page{display:none}.page.on{display:block}.pt{font-size:17px;font-weight:900;margin-bottom:12px;background:linear-gradient(135deg,var(--g),var(--ac));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-bottom:10px}
.st{background:var(--s);border:1px solid var(--b);border-radius:12px;padding:11px}.st .l{font-size:9px;color:var(--t2)}.st .v{font-size:16px;font-weight:800}
.card{background:var(--s);border:1px solid var(--b);border-radius:12px;padding:12px;margin-bottom:9px}.card h3{font-size:11px;color:var(--g);margin-bottom:8px}
.btn{padding:6px 11px;border-radius:8px;border:none;font-weight:700;font-size:11px;cursor:pointer;font-family:inherit}
.bg{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}.bo{background:rgba(255,215,0,.08);color:var(--g);border:1px solid var(--b)}.bd{background:rgba(248,113,113,.12);color:var(--rd)}
input,select{width:100%;padding:8px 10px;background:rgba(0,0,0,.25);border:1px solid var(--b);border-radius:8px;color:var(--t);font-family:inherit;font-size:12px;outline:none;margin-bottom:6px}
table{width:100%;border-collapse:collapse;font-size:11px}th{text-align:right;padding:6px;color:var(--t2);border-bottom:1px solid var(--b);font-size:9px}td{padding:6px;border-bottom:1px solid rgba(255,255,255,.04)}
.tag{display:inline-block;padding:2px 6px;border-radius:6px;font-size:9px;font-weight:700}.ton{background:rgba(52,211,153,.15);color:var(--gn)}.toff{background:rgba(248,113,113,.12);color:var(--rd)}
.toast{position:fixed;bottom:14px;left:50%;transform:translateX(-50%) translateY(40px);background:var(--s);border:1px solid var(--b);padding:8px 16px;border-radius:10px;font-size:12px;color:var(--g);opacity:0;transition:.3s;z-index:999}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.mob{display:none;position:fixed;top:0;left:0;right:0;height:44px;background:#0a0a14;border-bottom:1px solid var(--b);z-index:50;align-items:center;justify-content:space-between;padding:0 12px}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:80;display:none;align-items:center;justify-content:center}.modal-bg.show{display:flex}
.modal{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:16px;width:92%;max-width:380px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:7px}.pulse{display:inline-block;width:7px;height:7px;background:var(--gn);border-radius:50%;animation:p 1.5s infinite;margin:0 3px}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(52,211,153,.5)}70%{box-shadow:0 0 0 7px transparent}}
.lang{display:inline-flex;border:1px solid var(--b);border-radius:14px;overflow:hidden;font-size:10px;font-weight:700;margin-bottom:8px}
.lang button{border:none;padding:4px 9px;background:transparent;color:var(--t2);cursor:pointer;font-family:inherit;font-weight:700}.lang button.on{background:linear-gradient(135deg,var(--g),var(--g2));color:#0a0a10}
@media(max-width:768px){.side{transform:translateX(100%)}.side.open{transform:translateX(0)}.main{margin-right:0;padding-top:52px}.stats{grid-template-columns:1fr 1fr}.mob{display:flex}}
</style></head><body>
<div class="mob"><span style="font-weight:900;background:linear-gradient(135deg,#ffd700,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent">VROOM</span>
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
<button class="ni" style="color:var(--rd)" data-fa="خروج" data-en="Logout" onclick="fetch('/api/logout',{method:'POST'}).then(()=>location='/login')">خروج</button>
</aside>
<main class="main">
<section class="page on" id="p-dash">
<div class="pt">Dashboard <span class="pulse"></span></div>
<div class="stats">
<div class="st"><div class="l">👥 Conn</div><div class="v" id="s-cn">0</div></div>
<div class="st"><div class="l">📥 DL</div><div class="v" id="s-dl" style="font-size:13px">0</div></div>
<div class="st"><div class="l">📤 UL</div><div class="v" id="s-ul" style="font-size:13px">0</div></div>
<div class="st"><div class="l">📦 Total</div><div class="v" id="s-tr" style="font-size:13px">0</div></div>
</div>
<div class="stats">
<div class="st"><div class="l">📡 Links</div><div class="v" id="s-lk">0</div></div>
<div class="st"><div class="l">⏱️ Up</div><div class="v" id="s-up" style="font-size:12px">--</div></div>
<div class="st"><div class="l">💻 CPU</div><div class="v" id="s-cpu">--</div></div>
<div class="st"><div class="l">🧠 RAM</div><div class="v" id="s-ram">--</div></div>
</div>
<div class="card"><h3>⚡ Quick</h3>
<div style="display:flex;gap:6px;flex-wrap:wrap">
<button class="btn bg" onclick="qc(1)">+1GB</button>
<button class="btn bg" onclick="qc(5)">+5GB</button>
<button class="btn bg" onclick="qc(10)">+10GB</button>
<button class="btn bo" onclick="resetAll()">Reset usage</button>
</div></div>
</section>
<section class="page" id="p-links">
<div class="pt">Inbounds <button class="btn bg" style="float:left" onclick="$('#addM').classList.add('show')">+ Add</button></div>
<div class="card" style="overflow-x:auto"><table><thead><tr><th>Name</th><th>Usage</th><th>IP</th><th>Status</th><th>Actions</th></tr></thead><tbody id="lb"></tbody></table></div>
<p style="font-size:10px;color:var(--t2);margin-top:6px">📌 <b>Sub</b> = for apps · <b>Page</b> = user panel</p>
</section>
<section class="page" id="p-conn">
<div class="pt">Connections <span class="pulse"></span></div>
<div class="card"><table><thead><tr><th>Inbound</th><th>IP</th><th>Traffic</th><th>Since</th></tr></thead><tbody id="cb"></tbody></table></div>
</section>
<section class="page" id="p-addr">
<div class="pt">Clean IP</div>
<div class="card"><div class="grid2"><input id="new-addr" placeholder="IP/domain"><button class="btn bg" onclick="addAddr()">Add</button></div><div id="alist" style="margin-top:8px"></div></div>
</section>
<section class="page" id="p-tg">
<div class="pt">Telegram</div>
<div class="card">
<p style="font-size:11px;color:var(--t2);margin-bottom:8px">Token @BotFather · ID @userinfobot · Button-only bot</p>
<input id="tg-tok" placeholder="Bot token"><input id="tg-adm" placeholder="Admin ID(s)">
<div style="display:flex;gap:6px"><button class="btn bg" onclick="saveTg()">Enable</button><button class="btn bd" onclick="stopTg()">Stop</button></div>
<div id="tg-st" style="margin-top:8px;font-size:12px;color:var(--t2)"></div>
</div>
</section>
<section class="page" id="p-domain">
<div class="pt">Domain</div>
<div class="card"><input id="dom-in" placeholder="example.com"><button class="btn bg" onclick="saveDom()">Save</button><div id="dom-cur" style="margin-top:6px;font-size:12px;color:var(--t2)"></div></div>
</section>
<section class="page" id="p-sec">
<div class="pt">Security</div>
<div class="card"><input type="password" id="cpw" placeholder="Current"><input type="password" id="npw" placeholder="New"><button class="btn bg" onclick="chPass()">Change</button></div>
</section>
</main>
<div class="modal-bg" id="addM" onclick="if(event.target===this)this.classList.remove('show')">
<div class="modal"><h3 style="color:var(--g);margin-bottom:8px">Add inbound</h3>
<input id="nl" placeholder="English name">
<div class="grid2"><input id="nlim" type="number" placeholder="Volume"><select id="nun"><option>GB</option><option>MB</option></select></div>
<input id="nexp" type="number" placeholder="Days"><input id="nmax" type="number" placeholder="Max IPs">
<button class="btn bg" style="width:100%" onclick="createL()">Create</button></div></div>
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
if(!d.links?.length){b.innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t2)">Empty</td></tr>';return}
b.innerHTML=d.links.map(l=>{const u=(l.used_bytes/1e9).toFixed(2),lim=l.limit_bytes?(l.limit_bytes/1e9).toFixed(1)+'G':'∞';
const sub=l.sub_url||(location.origin+'/sub/'+l.uuid),page=l.page_url||(location.origin+'/page/'+l.uuid);
return `<tr><td><b>${l.label}</b></td><td>${u}/${lim}</td><td>${l.current_connections}/${l.max_connections||'∞'}</td>
<td><span class="tag ${l.active&&!l.expired?'ton':'toff'}">${l.active&&!l.expired?'ON':'OFF'}</span></td>
<td style="display:flex;gap:3px;flex-wrap:wrap">
<button class="btn bo" style="padding:2px 6px;font-size:9px" onclick="navigator.clipboard.writeText('${sub}').then(()=>toast('Sub'))">Sub</button>
<button class="btn bo" style="padding:2px 6px;font-size:9px" onclick="navigator.clipboard.writeText('${page}').then(()=>toast('Page'))">Page</button>
<button class="btn bo" style="padding:2px 6px;font-size:9px" onclick="navigator.clipboard.writeText('${l.vless_link.replace(/'/g,"\\\\'")}').then(()=>toast('Cfg'))">Copy</button>
<button class="btn bd" style="padding:2px 6px;font-size:9px" onclick="delL('${l.uuid}')">Del</button></td></tr>`}).join('')}
function loadC(){const list=window._conns||[];const b=$('#cb');
if(!list.length){b.innerHTML='<tr><td colspan="4" style="text-align:center;color:var(--t2)">None</td></tr>';return}
b.innerHTML=list.map(c=>`<tr><td>${c.uuid}</td><td>${c.ip}</td><td>${(c.bytes/1e6).toFixed(2)} MB</td><td style="font-size:10px">${(c.since||'').slice(11,19)}</td></tr>`).join('')}
async function delL(u){if(!confirm('Delete?'))return;await fetch('/api/links/'+u,{method:'DELETE'});toast('OK');loadL();loadS()}
async function createL(){const label=$('#nl').value.trim(),limit=parseFloat($('#nlim').value)||0,unit=$('#nun').value,expiry=parseFloat($('#nexp').value)||0,max=parseInt($('#nmax').value)||0;
if(!label){toast('Name');return}
const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,limit_value:limit,limit_unit:unit,expiry_days:expiry,max_connections:max})});
if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'Err');return}toast('Created');$('#addM').classList.remove('show');loadL();loadS()}
async function qc(gb){const n='u'+Math.floor(Math.random()*900+100);await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label:n,limit_value:gb,limit_unit:'GB',expiry_days:30})});toast(n);loadS()}
async function resetAll(){if(!confirm('Reset all?'))return;await fetch('/api/reset-all-usage',{method:'POST'});toast('Reset');loadL()}
async function loadA(){const r=await fetch('/api/addresses');const d=await r.json();$('#alist').innerHTML=(d.addresses||[]).map((a,i)=>`<div style="display:flex;justify-content:space-between;padding:7px;background:rgba(0,0,0,.2);border-radius:7px;margin-bottom:4px;font-size:12px"><span>${a}</span><button class="btn bd" style="padding:2px 7px;font-size:10px" onclick="delA(${i})">Del</button></div>`).join('')||'<div style="color:var(--t2);font-size:12px">Empty</div>'}
async function addAddr(){const a=$('#new-addr').value.trim();if(!a)return;await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});$('#new-addr').value='';loadA();toast('Added')}
async function delA(i){await fetch('/api/addresses/'+i,{method:'DELETE'});loadA()}
async function loadTg(){const r=await fetch('/api/telegram');const d=await r.json();$('#tg-st').innerHTML=d.enabled?'<span style="color:var(--gn)">● ON</span> — '+(d.admin_ids||[]).join(', '):'<span style="color:var(--rd)">● OFF</span>';if(d.admin_ids?.length)$('#tg-adm').value=d.admin_ids.join(' ')}
async function saveTg(){const r=await fetch('/api/telegram',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:$('#tg-tok').value.trim(),admin_ids:$('#tg-adm').value.trim()})});const d=await r.json().catch(()=>({}));if(!r.ok){toast(d.detail||'Err');return}toast(d.enabled?'ON @'+(d.bot_username||''):'Saved');loadTg()}
async function stopTg(){await fetch('/api/telegram/stop',{method:'POST'});toast('Stopped');loadTg()}
async function loadDom(){const r=await fetch('/api/domain');const d=await r.json();$('#dom-cur').textContent='Current: '+(d.domain||'default')}
async function saveDom(){await fetch('/api/domain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({domain:$('#dom-in').value.trim()})});toast('Saved');loadDom()}
async function chPass(){const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:$('#cpw').value,new_password:$('#npw').value})});if(!r.ok){toast((await r.json().catch(()=>({}))).detail||'Err');return}toast('Changed')}
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
