#!/usr/bin/env python3
"""
VROOM Panel v5.8 - Quota Enforced + Custom Telegram Bot Runner
"""
import asyncio, json, os, hashlib, secrets, time, re, base64, subprocess, signal, sys, shutil
from datetime import datetime, timedelta
from urllib.parse import quote
from collections import deque, defaultdict
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn, httpx, logging, psutil
from pathlib import Path
from contextlib import asynccontextmanager

# ===================== CONFIG =====================
try:
    SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_urlsafe(32)
    os.environ.setdefault("SECRET_KEY", SECRET_KEY)
except Exception:
    SECRET_KEY = "vroom-default-secret-key"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VROOM")

# پوشه نگهداری ربات‌های سفارشی کاربران
BOTS_DIR = Path("user_bots")
BOTS_DIR.mkdir(parents=True, exist_ok=True)
BOTS_DATA_FILE = BOTS_DIR / "bots.json"

# وابستگی‌های رایج تلگرام که به صورت خودکار نصب می‌شوند
TELEGRAM_DEPS = [
    "python-telegram-bot==21.6",
    "pyTelegramBotAPI==4.23.0",
    "aiogram==3.13.1",
    "pyrogram==2.0.106",
    "telethon==1.36.0",
    "httpx",
    "aiohttp",
    "requests",
]

# وضعیت ربات‌های در حال اجرا
RUNNING_BOTS = {}          # bot_id -> {"process": Popen, "permanent": bool, "token": str, ...}
BOTS_LOCK = asyncio.Lock()

def load_bots_data():
    if BOTS_DATA_FILE.exists():
        try:
            return json.loads(BOTS_DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_bots_data(data):
    BOTS_DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

async def install_telegram_deps():
    """نصب خودکار وابستگی‌های تلگرام"""
    logger.info("📦 در حال نصب وابستگی‌های تلگرام...")
    for dep in TELEGRAM_DEPS:
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "--quiet", dep,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.wait()
        except Exception as e:
            logger.warning(f"نصب {dep} ناموفق: {e}")
    logger.info("✅ وابستگی‌های تلگرام آماده شد")

async def start_user_bot(bot_id: str, code: str, token: str, permanent: bool = False):
    """اجرای ربات کاربر در فرآیند جداگانه"""
    bot_dir = BOTS_DIR / bot_id
    bot_dir.mkdir(parents=True, exist_ok=True)
    code_file = bot_dir / "bot.py"
    
    # جایگزینی ساده توکن اگر کاربر از متغیر محیطی استفاده نکرده باشد
    final_code = code
    if "TOKEN" not in code and "token" not in code.lower()[:500]:
        final_code = f'TOKEN = "{token}"\n' + code
    
    code_file.write_text(final_code, encoding="utf-8")
    
    # فایل لاگ
    log_file = bot_dir / "bot.log"
    
    env = os.environ.copy()
    env["BOT_TOKEN"] = token
    env["TOKEN"] = token
    
    try:
        log_handle = open(log_file, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, str(code_file)],
            cwd=str(bot_dir),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True
        )
        
        async with BOTS_LOCK:
            RUNNING_BOTS[bot_id] = {
                "process": proc,
                "permanent": permanent,
                "token": token[:12] + "...",
                "started_at": datetime.now().isoformat(),
                "pid": proc.pid,
                "log_file": str(log_file)
            }
        
        # ذخیره وضعیت
        data = load_bots_data()
        data[bot_id] = {
            "permanent": permanent,
            "token": token,
            "code": code,
            "started_at": datetime.now().isoformat(),
            "active": True
        }
        save_bots_data(data)
        
        return True, f"ربات با PID {proc.pid} اجرا شد"
    except Exception as e:
        return False, str(e)

async def stop_user_bot(bot_id: str):
    """خاموش کردن ربات"""
    async with BOTS_LOCK:
        info = RUNNING_BOTS.get(bot_id)
        if info and info.get("process"):
            try:
                os.killpg(os.getpgid(info["process"].pid), signal.SIGTERM)
            except Exception:
                try:
                    info["process"].terminate()
                except Exception:
                    pass
            RUNNING_BOTS.pop(bot_id, None)
    
    data = load_bots_data()
    if bot_id in data:
        data[bot_id]["active"] = False
        data[bot_id]["permanent"] = False
        save_bots_data(data)
    return True

async def restore_permanent_bots():
    """بعد از ریستارت پنل، ربات‌های همیشه روشن را دوباره بالا بیاور"""
    data = load_bots_data()
    for bot_id, info in data.items():
        if info.get("permanent") and info.get("active"):
            logger.info(f"🔄 بازیابی ربات همیشه روشن: {bot_id}")
            await start_user_bot(bot_id, info.get("code", ""), info.get("token", ""), permanent=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=5000, max_keepalive_connections=1000),
        timeout=httpx.Timeout(180.0, connect=30.0),
        follow_redirects=True
    )
    logger.info(f"🚀 VROOM v5.8 :{CONFIG['port']}")
    
    # نصب وابستگی‌های تلگرام
    asyncio.create_task(install_telegram_deps())
    
    # بازیابی ربات‌های همیشه روشن
    asyncio.create_task(restore_permanent_bots())
    
    asyncio.create_task(keep_alive())
    if TELEGRAM.get("token") and TELEGRAM.get("admin_ids"):
        TELEGRAM["enabled"] = True
        await start_telegram_bot()
    yield
    if http_client:
        await http_client.aclose()
    if TELEGRAM_TASK:
        TELEGRAM_TASK.cancel()
    # خاموش کردن ربات‌های غیر-permanent
    for bot_id, info in list(RUNNING_BOTS.items()):
        if not info.get("permanent"):
            await stop_user_bot(bot_id)

app = FastAPI(title="VROOM", docs_url=None, redoc_url=None, lifespan=lifespan)
CONFIG = {"port": int(os.environ.get("PORT", 8080)), "secret": SECRET_KEY}
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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

def is_quota_exceeded(link):
    limit = link.get("limit_bytes", 0) if isinstance(link, dict) else 0
    if not limit or limit <= 0:
        return False
    return link.get("used_bytes", 0) >= limit

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

# ===================== TELEGRAM BOT (VROOM ADMIN) =====================
def ikb(rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": c} for t, c in row] for row in rows]}

def main_menu_kb(lang="fa"):
    if lang == "en":
        return ikb([
            [("➕ Create", "create_start"), ("📋 List", "list")],
            [("📊 Stats", "stats"), ("🔗 Sub link", "sub_menu")],
            [("🤖 Custom Bots", "bots_menu"), ("🇮🇷 فارسی", "lang_fa")],
            [("ℹ️ Help", "help")]
        ])
    return ikb([
        [("➕ ساخت", "create_start"), ("📋 لیست", "list")],
        [("📊 آمار", "stats"), ("🔗 لینک ساب", "sub_menu")],
        [("🤖 ربات‌های سفارشی", "bots_menu"), ("🇬🇧 English", "lang_en")],
        [("ℹ️ راهنما", "help")]
    ])

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
        txt = "ℹ️ همه کارها با دکمه.\nساب + صفحه + کانفیگ داده می‌شه.\n\n🤖 بخش ربات‌های سفارشی هم اضافه شده." if lang == "fa" else "ℹ️ Everything via buttons.\nSub + page + config are shared.\n\n🤖 Custom bots section added."
        await tg_edit(chat_id, message_id, txt, reply_markup=ikb([[(home, "menu")]]))
        return
    
    if data == "stats":
        async with LINKS_LOCK:
            n, active = len(LINKS), sum(1 for x in LINKS.values() if x.get("active") and not is_expired(x) and not is_quota_exceeded(x))
        running_count = len(RUNNING_BOTS)
        if lang == "fa":
            t = f"📊 <b>آمار زنده</b>\n\n🔗 اینباند: {n} (فعال: {active})\n📡 اتصال: {len(connections)}\n🤖 ربات‌های در حال اجرا: {running_count}\n📥 دانلود: {fmt_bytes(stats['download_bytes'])}\n📤 آپلود: {fmt_bytes(stats['upload_bytes'])}\n📦 کل: {fmt_bytes(stats['total_bytes'])}\n⏱️ آپتایم: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        else:
            t = f"📊 <b>Live Stats</b>\n\n🔗 Links: {n} (active: {active})\n📡 Conns: {len(connections)}\n🤖 Running bots: {running_count}\n📥 DL: {fmt_bytes(stats['download_bytes'])}\n📤 UL: {fmt_bytes(stats['upload_bytes'])}\n📦 Total: {fmt_bytes(stats['total_bytes'])}\n⏱️ Uptime: {uptime()}\n🌐 {get_domain()}\n💻 CPU {psutil.cpu_percent()}%\n🧠 RAM {psutil.virtual_memory().percent}%"
        await tg_edit(chat_id, message_id, t, reply_markup=ikb([[("🔄", "stats"), (home, "menu")]]))
        return
    
    # ===== بخش ربات‌های سفارشی =====
    if data == "bots_menu":
        data_bots = load_bots_data()
        rows = []
        for bid, info in list(data_bots.items())[:10]:
            status = "🟢" if bid in RUNNING_BOTS else "🔴"
            perm = "♾️" if info.get("permanent") else ""
            rows.append([(f"{status} {bid[:12]} {perm}", f"botinfo:{bid}")])
        rows.append([("🌐 باز کردن صفحه راه‌انداز", "open_runner")])
        rows.append([(home, "menu")])
        txt = "🤖 <b>ربات‌های سفارشی</b>\n\nبرای ساخت ربات جدید از صفحه وب استفاده کن." if lang == "fa" else "🤖 <b>Custom Bots</b>\n\nUse web page to create new bots."
        await tg_edit(chat_id, message_id, txt, reply_markup=ikb(rows))
        return
    
    if data == "open_runner":
        domain = get_domain()
        url = f"https://{domain}/bot-runner"
        await tg_edit(chat_id, message_id, f"🌐 صفحه راه‌انداز:\n<code>{url}</code>", reply_markup=ikb([[(home, "menu")]]))
        return
    
    if data.startswith("botinfo:"):
        bid = data[8:]
        info = load_bots_data().get(bid, {})
        running = bid in RUNNING_BOTS
        status = "🟢 در حال اجرا" if running else "🔴 خاموش"
        perm = "بله ♾️" if info.get("permanent") else "خیر"
        t = f"🤖 <b>{bid}</b>\n\nوضعیت: {status}\nهمیشه روشن: {perm}\nشروع: {info.get('started_at', '-')[:19]}"
        kb = []
        if running:
            kb.append([("⏹ خاموش کردن", f"botstop:{bid}")])
        else:
            kb.append([("▶️ روشن کردن", f"botstart:{bid}")])
        kb.append([("🗑 حذف", f"botdel:{bid}"), ("📋 بازگشت", "bots_menu")])
        await tg_edit(chat_id, message_id, t, reply_markup=ikb(kb))
        return
    
    if data.startswith("botstop:"):
        bid = data[8:]
        await stop_user_bot(bid)
        await tg_edit(chat_id, message_id, "✅ ربات خاموش شد", reply_markup=ikb([[("📋", "bots_menu"), (home, "menu")]]))
        return
    
    if data.startswith("botstart:"):
        bid = data[9:]
        info = load_bots_data().get(bid)
        if info:
            ok, msg = await start_user_bot(bid, info.get("code", ""), info.get("token", ""), permanent=info.get("permanent", False))
            await tg_edit(chat_id, message_id, f"{'✅' if ok else '❌'} {msg}", reply_markup=ikb([[("📋", "bots_menu"), (home, "menu")]]))
        else:
            await tg_edit(chat_id, message_id, "❌ پیدا نشد", reply_markup=ikb([[("📋", "bots_menu")]]))
        return
    
    if data.startswith("botdel:"):
        bid = data[7:]
        await stop_user_bot(bid)
        data_bots = load_bots_data()
        data_bots.pop(bid, None)
        save_bots_data(data_bots)
        shutil.rmtree(BOTS_DIR / bid, ignore_errors=True)
        await tg_edit(chat_id, message_id, "✅ حذف شد", reply_markup=ikb([[("📋", "bots_menu"), (home, "menu")]]))
        return
    
    # ===== بقیه منوهای قبلی =====
    if data == "list":
        async with LINKS_LOCK:
            items = list(LINKS.items())
        if not items:
            await tg_edit(chat_id, message_id, "📭 خالی" if lang == "fa" else "📭 Empty", reply_markup=ikb([[("➕", "create_start"), (home, "menu")]]))
            return
        rows = [[(f"{'✅' if d.get('active') and not is_expired(d) and not is_quota_exceeded(d) else '❌'} {d['label']}", f"link:{uid}")] for uid, d in items[:15]]
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
        quota_txt = " | حجم تموم" if is_quota_exceeded(link) else ""
        if lang == "fa":
            t = f"🏷 <b>{link['label']}</b>{quota_txt}\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 اتصالات: {count_connections_for_link(uid)}\n\n📥 <b>لینک ساب:</b>\n<code>{sub}</code>\n\n🖥 <b>صفحه:</b>\n<code>{page}</code>\n\n📋 <b>کانفیگ:</b>\n<code>{vless}</code>"
        else:
            t = f"🏷 <b>{link['label']}</b>{quota_txt}\n\n📦 {fmt_bytes(link['used_bytes'])}/{fmt_bytes(link['limit_bytes']) if link['limit_bytes'] else '∞'}\n🔌 Conns: {count_connections_for_link(uid)}\n\n📥 <b>Sub:</b>\n<code>{sub}</code>\n\n🖥 <b>Page:</b>\n<code>{page}</code>\n\n📋 <b>Config:</b>\n<code>{vless}</code>"
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
            txt = "🚀 <b>VROOM Bot</b>\nفقط دکمه — لینک ساب برای برنامه‌ها.\n\n🤖 بخش ربات‌های سفارشی هم فعال است." if lang == "fa" else "🚀 <b>VROOM Bot</b>\nButtons only — sub link for apps.\n\n🤖 Custom bots section is active."
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

# ===================== API اصلی =====================
@app.get("/")
async def root():
    return {"service": "VROOM", "version": "5.8", "domain": get_domain()}

@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "download": stats["download_bytes"], "upload": stats["upload_bytes"], "uptime": uptime(), "running_bots": len(RUNNING_BOTS)}

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
        "running_bots": len(RUNNING_BOTS),
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
                "quota_exceeded": is_quota_exceeded(data), "created_at": data["created_at"], "current_connections": count_connections_for_link(uid),
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

# ===================== BOT RUNNER API =====================
@app.post("/api/user/run-bot")
async def run_user_bot(
    token: str = Form(...),
    code_type: str = Form("text"),
    code_text: str = Form(""),
    is_permanent: str = Form("off"),
    code_file: UploadFile = File(None)
):
    """اجرای ربات سفارشی کاربر"""
    permanent = is_permanent.lower() in ("on", "true", "1", "yes")
    
    code = code_text.strip()
    if code_type == "file" and code_file:
        content = await code_file.read()
        if len(content) > 500 * 1024:
            return JSONResponse({"success": False, "message": "❌ حجم فایل بیشتر از ۵۰۰ کیلوبایت است", "logs": ""})
        code = content.decode("utf-8", errors="ignore")
    
    if not code:
        return JSONResponse({"success": False, "message": "❌ کد خالی است", "logs": ""})
    if not token or len(token) < 20:
        return JSONResponse({"success": False, "message": "❌ توکن نامعتبر است", "logs": ""})
    
    bot_id = "bot_" + secrets.token_hex(4)
    
    # نصب وابستگی‌ها (اگر لازم باشد)
    await install_telegram_deps()
    
    ok, msg = await start_user_bot(bot_id, code, token, permanent=permanent)
    
    logs = ""
    log_path = BOTS_DIR / bot_id / "bot.log"
    if log_path.exists():
        try:
            logs = log_path.read_text(encoding="utf-8")[-3000:]
        except Exception:
            pass
    
    if ok:
        return JSONResponse({
            "success": True,
            "message": f"✅ ربات با موفقیت اجرا شد (ID: {bot_id})" + (" | حالت همیشه روشن فعال است" if permanent else ""),
            "logs": logs or "ربات در حال اجرا است...",
            "bot_id": bot_id
        })
    else:
        return JSONResponse({"success": False, "message": f"❌ خطا: {msg}", "logs": logs})

@app.get("/api/user/bots")
async def list_user_bots(_=Depends(require_auth)):
    data = load_bots_data()
    result = []
    for bid, info in data.items():
        result.append({
            "id": bid,
            "permanent": info.get("permanent", False),
            "active": bid in RUNNING_BOTS,
            "started_at": info.get("started_at"),
            "token_preview": (info.get("token") or "")[:12] + "..."
        })
    return {"bots": result}

@app.post("/api/user/bots/{bot_id}/stop")
async def api_stop_bot(bot_id: str, _=Depends(require_auth)):
    await stop_user_bot(bot_id)
    return {"ok": True}

@app.delete("/api/user/bots/{bot_id}")
async def api_delete_bot(bot_id: str, _=Depends(require_auth)):
    await stop_user_bot(bot_id)
    data = load_bots_data()
    data.pop(bot_id, None)
    save_bots_data(data)
    shutil.rmtree(BOTS_DIR / bot_id, ignore_errors=True)
    return {"ok": True}

# ===================== SUB & PAGE =====================
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
    if is_quota_exceeded(link):
        raise HTTPException(403, "Quota exceeded")
    
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

@app.get("/page/{uid}")
async def subscription_page(uid: str):
    # ... (همان صفحه ساب قبلی بدون تغییر اساسی)
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            raise HTTPException(404)
    if not link["active"]:
        raise HTTPException(403, "Disabled")
    if is_expired(link):
        raise HTTPException(403, "Expired")
    
    server_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    used_gb = round(link["used_bytes"] / 1024 ** 3, 2)
    limit_gb = round(link["limit_bytes"] / 1024 ** 3, 2) if link["limit_bytes"] else 0
    percent = min(100, round((link["used_bytes"] / link["limit_bytes"]) * 100, 1)) if link["limit_bytes"] else 0
    remaining = round(max(0, limit_gb - used_gb), 2) if limit_gb else "∞"
    
    if is_expired(link):
        status_fa, status_en, sc = "منقضی", "Expired", "#f43f5e"
    elif is_quota_exceeded(link):
        status_fa, status_en, sc = "حجم تمام", "Quota Full", "#f59e0b"
    else:
        status_fa, status_en, sc = "فعال", "Active", "#10b981"
    
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

    ring_bg = f"conic-gradient(#3b82f6 0% {percent}%, #e2e8f0 {percent}% 100%)"
    bar_w = f"{percent}%"

    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl" data-theme="light">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>VROOM — {link['label']}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@600;700;800&family=Vazirmatn:wght@500;600;700;800&display=swap" rel="stylesheet"/>
<style>
:root{{
  --bg:#eef2ff;--card:#ffffff;--border:rgba(0,0,0,.06);
  --text:#0f172a;--muted:#64748b;--blue:#3b82f6;--indigo:#6366f1;--pink:#ec4899;--green:#10b981;
  --r:20px;--shadow:0 4px 20px rgba(0,0,0,.06);
}}
[data-theme=dark]{{
  --bg:#0b0f1a;--card:#111827;--border:rgba(255,255,255,.06);
  --text:#f1f5f9;--muted:#94a3b8;--shadow:0 4px 20px rgba(0,0,0,.3);
}}
*{{margin:0;padding:0;box-sizing:border-box}}
html{{-webkit-text-size-adjust:100%}}
body{{
  font-family:Vazirmatn,Inter,system-ui,sans-serif;background:var(--bg);color:var(--text);
  min-height:100vh;padding:12px 12px 32px;-webkit-font-smoothing:antialiased;
  transition:background .2s;
}}
.w{{max-width:420px;margin:0 auto}}
.hdr{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}}
.hdr-l{{display:flex;gap:8px;align-items:center}}
.ib{{
  width:36px;height:36px;border-radius:12px;background:var(--card);border:1px solid var(--border);
  display:flex;align-items:center;justify-content:center;font-size:16px;cursor:pointer;
  box-shadow:var(--shadow);
}}
.ls{{display:flex;background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;box-shadow:var(--shadow)}}
.ls button{{border:none;padding:6px 10px;font-size:11px;font-weight:700;background:transparent;color:var(--muted);cursor:pointer;font-family:inherit}}
.ls button.on{{background:linear-gradient(135deg,var(--blue),var(--indigo));color:#fff}}
.logo{{font-family:Inter;font-weight:800;font-size:18px;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.card{{
  background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:14px;
  margin-bottom:10px;box-shadow:var(--shadow);
}}
.ct{{font-size:13px;font-weight:800;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}}
.badge{{font-size:11px;color:var(--blue);background:rgba(59,130,246,.1);padding:2px 8px;border-radius:20px}}
.sr{{display:flex;justify-content:space-between;font-size:12px;font-weight:600;margin-bottom:12px}}
.so{{color:var(--green);display:flex;align-items:center;gap:5px}}
.dot{{width:6px;height:6px;border-radius:50%;background:var(--green)}}
.sg{{display:grid;grid-template-columns:auto 1fr 1fr 1fr;gap:8px;align-items:center}}
.st{{text-align:center}}.st .l{{font-size:10px;color:var(--muted)}}.st .v{{font-size:13px;font-weight:800}}
.rw{{width:64px;height:64px}}
.rg{{width:64px;height:64px;border-radius:50%;background:{ring_bg};display:flex;align-items:center;justify-content:center}}
.ri{{width:48px;height:48px;border-radius:50%;background:var(--card);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:800}}
.pb{{margin-top:10px;height:4px;background:rgba(148,163,184,.2);border-radius:99px;overflow:hidden}}
.pf{{height:100%;width:{bar_w};background:linear-gradient(90deg,var(--blue),var(--pink));border-radius:99px}}
.srow{{display:flex;gap:8px;align-items:center;background:rgba(148,163,184,.06);border-radius:12px;padding:6px 8px;margin-bottom:10px}}
.surl{{flex:1;font-size:10px;font-family:monospace;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;direction:ltr;text-align:left}}
.cb{{
  background:linear-gradient(135deg,var(--blue),var(--indigo));color:#fff;border:none;
  padding:7px 12px;border-radius:10px;font-size:11px;font-weight:700;cursor:pointer;font-family:inherit;
}}
.ip{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}}
.pi{{background:rgba(148,163,184,.06);border-radius:12px;padding:8px 4px;text-align:center}}
.pi .pl{{font-size:9px;color:var(--muted)}}.pi .pv{{font-size:12px;font-weight:800}}
.cfg{{
  background:rgba(148,163,184,.06);border-radius:12px;padding:9px;font-size:9.5px;font-family:monospace;
  word-break:break-all;max-height:44px;overflow-y:auto;direction:ltr;text-align:left;color:var(--muted);
  margin-bottom:10px;cursor:pointer;
}}
.qr-w{{display:flex;justify-content:center;margin-bottom:10px}}
.qr{{width:110px;height:110px;background:#fff;border-radius:14px;padding:6px;box-shadow:0 4px 16px rgba(59,130,246,.12)}}
.qr img{{width:100%;height:100%;border-radius:8px}}
.ab{{display:flex;gap:8px}}
.ab button{{flex:1;padding:11px;border:none;border-radius:12px;font-weight:800;font-size:12px;cursor:pointer;font-family:inherit}}
.bs{{background:rgba(148,163,184,.1);color:var(--text)}}
.ba{{background:linear-gradient(135deg,var(--blue),var(--pink));color:#fff}}
.pr{{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}}
.pb2{{padding:6px 12px;border-radius:16px;border:none;background:rgba(148,163,184,.08);color:var(--muted);font-size:11px;font-weight:700;cursor:pointer;font-family:inherit}}
.pb2.on{{background:linear-gradient(135deg,var(--blue),var(--indigo));color:#fff}}
.ag{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}
.ac{{background:rgba(148,163,184,.05);border-radius:14px;padding:10px 4px;text-align:center;cursor:pointer}}
.ai{{width:40px;height:40px;margin:0 auto 5px;border-radius:10px;background:linear-gradient(145deg,#e0e7ff,#fce7f3);display:flex;align-items:center;justify-content:center;font-size:16px;overflow:hidden}}
[data-theme=dark] .ai{{background:linear-gradient(145deg,#1e293b,#312e81)}}
.ai img{{width:100%;height:100%;object-fit:cover}}
.an{{font-size:10px;font-weight:700}}.ah{{font-size:8px;color:var(--muted)}}
.ft{{text-align:center;font-size:10px;color:var(--muted);margin-top:6px}}
.ft b{{background:linear-gradient(135deg,var(--blue),var(--pink));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.toast{{
  position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(50px);
  background:var(--card);padding:9px 16px;border-radius:10px;font-size:12px;font-weight:700;
  color:var(--blue);opacity:0;transition:opacity .2s,transform .2s;z-index:99;box-shadow:var(--shadow);
}}
.toast.show{{opacity:1;transform:translateX(-50%) translateY(0)}}
#qrm{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.8);align-items:center;justify-content:center;z-index:100}}
#qrm img{{width:min(75vw,280px);border-radius:16px}}
</style>
</head>
<body>
<div class="w">
  <div class="hdr">
    <div class="hdr-l">
      <button class="ib" id="th" onclick="togTheme()">☀️</button>
      <div class="ls">
        <button id="bEn" onclick="setL('en')">EN</button>
        <button id="bFa" class="on" onclick="setL('fa')">FA</button>
      </div>
    </div>
    <div class="logo">VROOM</div>
    <button class="ib">⚡</button>
  </div>

  <div class="card">
    <div class="ct"><span data-f="Subscription /" data-e="Subscription /">Subscription /</span><span class="badge">✨ {link['label']} ✨</span></div>
    <div class="sr">
      <span data-f="اتصالات : {live_conns}" data-e="Connections : {live_conns}">اتصالات : {live_conns}</span>
      <span class="so"><span class="dot"></span> <span data-f="سرور آنلاین" data-e="Server Online">سرور آنلاین</span></span>
    </div>
    <div class="sg">
      <div class="rw"><div class="rg"><div class="ri">{percent}%</div></div></div>
      <div class="st"><div class="l" data-f="باقی" data-e="Left">باقی</div><div class="v">{remaining}{' GB' if remaining!='∞' else ''}</div></div>
      <div class="st"><div class="l" data-f="وضعیت" data-e="Status">وضعیت</div><div class="v" style="color:{sc}" data-f="{status_fa}" data-e="{status_en}">{status_fa}</div></div>
      <div class="st"><div class="l" data-f="مصرف" data-e="Used">مصرف</div><div class="v">{used_gb} GB</div></div>
    </div>
    <div class="pb"><div class="pf"></div></div>
  </div>

  <div class="card">
    <div class="ct"><span data-f="لینک ساب (دریافت‌ها)" data-e="Sub Link (for apps)">لینک ساب (دریافت‌ها)</span><span>🔗</span></div>
    <div class="srow">
      <button class="cb" onclick="cp(SUB)" data-f="کپی" data-e="Copy">کپی</button>
      <div class="surl">{sub_url}</div>
    </div>
    <div class="ip">
      <div class="pi"><div class="pl">🛡️ <span data-f="وضعیت" data-e="Status">وضعیت</span></div><div class="pv" style="color:{sc}" data-f="{status_fa}" data-e="{status_en}">{status_fa}</div></div>
      <div class="pi"><div class="pl">📅 <span data-f="انقضا" data-e="Expiry">انقضا</span></div><div class="pv" style="color:#f59e0b">{exp_disp}</div></div>
      <div class="pi"><div class="pl">⏳ <span data-f="باقی" data-e="Left">باقی</span></div><div class="pv" style="color:#06b6d4" data-f="{days_fa} روز" data-e="{days_en} days">{days_fa} روز</div></div>
    </div>
  </div>

  <div class="card">
    <div class="ct"><span data-f="کانفیگ و QR" data-e="Config & QR">کانفیگ و QR</span><span>📱</span></div>
    <div class="cfg" onclick="cp(CFG)">{server_link}</div>
    <div class="qr-w"><div class="qr" onclick="document.getElementById('qrm').style.display='flex'"><img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={qr_data}" alt="QR" loading="lazy" width="98" height="98"/></div></div>
    <div class="ab">
      <button class="bs" onclick="share()" data-f="اشتراک" data-e="Share">اشتراک</button>
      <button class="ba" onclick="cp(SUB)" data-f="+ اضافه اشتراک +" data-e="+ Add Sub +">+ اضافه اشتراک +</button>
    </div>
  </div>

  <div class="card">
    <div class="ct"><span data-f="ابزارهای سریع" data-e="Quick Tools">ابزارهای سریع</span><span>⚡</span></div>
    <div class="pr">
      <button class="pb2 on" data-p="android" onclick="sp(this)">Android</button>
      <button class="pb2" data-p="ios" onclick="sp(this)">iOS</button>
      <button class="pb2" data-p="windows" onclick="sp(this)">Windows</button>
      <button class="pb2" data-p="macos" onclick="sp(this)">macOS</button>
      <button class="pb2" data-p="linux" onclick="sp(this)">Linux</button>
    </div>
    <div class="ag" id="ag"></div>
  </div>
  <div class="ft">Powered by <b>VROOM</b></div>
</div>
<div id="qrm" onclick="this.style.display='none'"><img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={qr_data}" alt="QR" loading="lazy"/></div>
<div class="toast" id="to"></div>
<script>
const SUB="{sub_url}",CFG=`{server_link}`;
let L=localStorage.getItem("vl")||"fa";
const C={{android:[["Hiddify","Hiddify","hiddify://import/"+encodeURIComponent(SUB)],["v2rayng","v2rayNG","v2rayng://install-config?url="+encodeURIComponent(SUB)],["v2box","V2Box","v2box://install-config?url="+encodeURIComponent(SUB)],["Happ","Happ","happ://import?url="+encodeURIComponent(SUB)]],ios:[["Hiddify","Hiddify","hiddify://import/"+encodeURIComponent(SUB)],["v2box","V2Box","v2box://install-config?url="+encodeURIComponent(SUB)],["Happ","Happ","happ://import?url="+encodeURIComponent(SUB)]],windows:[["Hiddify","Hiddify","hiddify://import/"+encodeURIComponent(SUB)],["v2rayn","v2rayN","v2rayN://import?url="+encodeURIComponent(SUB)]],macos:[["Hiddify","Hiddify","hiddify://import/"+encodeURIComponent(SUB)],["v2box","V2Box","v2box://install-config?url="+encodeURIComponent(SUB)]],linux:[["Hiddify","Hiddify","hiddify://import/"+encodeURIComponent(SUB)],["v2rayn","v2rayN","v2rayN://import?url="+encodeURIComponent(SUB)]]}};
function setL(l){{L=l;localStorage.setItem("vl",l);document.documentElement.lang=l;document.documentElement.dir=l==="fa"?"rtl":"ltr";document.getElementById("bFa").classList.toggle("on",l==="fa");document.getElementById("bEn").classList.toggle("on",l==="en");document.querySelectorAll("[data-f]").forEach(e=>e.textContent=l==="fa"?e.dataset.f:e.dataset.e)}}
function togTheme(){{const n=document.documentElement.getAttribute("data-theme")==="light"?"dark":"light";document.documentElement.setAttribute("data-theme",n);localStorage.setItem("vt",n);document.getElementById("th").textContent=n==="light"?"☀️":"🌙"}}
function sp(btn){{
  document.querySelectorAll(".pb2").forEach(b=>b.classList.remove("on"));
  btn.classList.add("on");
  const p=btn.dataset.p,list=C[p]||[],g=document.getElementById("ag");
  g.innerHTML=list.map(([id,n])=>`<div class="ac" onclick="oa('${{id}}','${{p}}')"><div class="ai" id="ai-${{id}}">${{n[0]}}</div><div class="an">${{n}}</div><div class="ah">Tap</div></div>`).join("");
  list.forEach(([id])=>{{
    fetch("/api/app-photo/"+id).then(r=>r.json()).then(d=>{{
      if(d.url){{const el=document.getElementById("ai-"+id);if(el)el.innerHTML=`<img src="${{d.url}}" alt="" loading="lazy" width="40" height="40">`}}
    }}).catch(()=>{{}});
  }});
}}
function oa(id,p){{const a=(C[p]||[]).find(x=>x[0]===id);if(!a)return;try{{location.href=a[2]}}catch(e){{}}setTimeout(()=>{{cp(SUB);toast(L==="fa"?"لینک ساب کپی شد":"Sub link copied")}},1000)}}
function share(){{if(navigator.share)navigator.share({{title:"VROOM",url:SUB}}).catch(()=>cp(SUB));else cp(SUB)}}
function cp(t){{navigator.clipboard?navigator.clipboard.writeText(t).then(()=>toast(L==="fa"?"کپی شد ✅":"Copied ✅")):(()=>{{const i=document.createElement("input");i.value=t;document.body.appendChild(i);i.select();document.execCommand("copy");document.body.removeChild(i);toast(L==="fa"?"کپی شد ✅":"Copied ✅")}})()}}
function toast(m){{const t=document.getElementById("to");t.textContent=m;t.classList.add("show");setTimeout(()=>t.classList.remove("show"),1800)}}
const st=localStorage.getItem("vt")||"light";document.documentElement.setAttribute("data-theme",st);document.getElementById("th").textContent=st==="light"?"☀️":"🌙";
setL(L);sp(document.querySelector(".pb2"));
</script>
</body></html>"""
    return HTMLResponse(content=html)

# ===================== BOT RUNNER PAGES =====================
BOT_RUNNER_HTML = r"""<!DOCTYPE html>
<html dir="rtl" lang="fa">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 راه‌انداز ربات تلگرام | VROOM</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }
        .container {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 24px;
            padding: 40px;
            max-width: 700px;
            width: 100%;
            box-shadow: 0 30px 80px rgba(0, 0, 0, 0.6);
        }
        .header { text-align: center; margin-bottom: 35px; }
        .header h1 {
            font-size: 32px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .header p { color: #a0aec0; font-size: 15px; margin-top: 8px; }
        .form-group { margin-bottom: 22px; }
        .form-group label { display: block; color: #e2e8f0; font-weight: 600; margin-bottom: 8px; font-size: 14px; }
        .form-control {
            width: 100%; padding: 12px 16px;
            border: 2px solid rgba(255, 255, 255, 0.1);
            border-radius: 12px; font-size: 14px;
            background: rgba(255, 255, 255, 0.05); color: #e2e8f0;
        }
        .form-control:focus { outline: none; border-color: #667eea; }
        textarea.form-control { min-height: 160px; font-family: monospace; direction: ltr; }
        .file-upload-wrapper {
            border: 2px dashed rgba(255, 255, 255, 0.15);
            border-radius: 12px; padding: 28px; text-align: center; cursor: pointer;
        }
        .file-upload-wrapper:hover { border-color: #667eea; }
        .file-upload-wrapper input { display: none; }
        .btn-submit {
            width: 100%; padding: 16px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; border: none; border-radius: 12px;
            font-size: 17px; font-weight: 700; cursor: pointer;
        }
        .btn-submit:disabled { opacity: 0.6; }
        .toggle-container {
            display: flex; align-items: center; gap: 15px;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 12px; padding: 14px 18px;
        }
        .toggle-switch { position: relative; width: 50px; height: 28px; }
        .toggle-switch input { opacity: 0; width: 0; height: 0; }
        .toggle-slider {
            position: absolute; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(255,255,255,0.15); border-radius: 34px;
        }
        .toggle-slider::before {
            content: ""; position: absolute; height: 20px; width: 20px;
            left: 4px; bottom: 4px; background: #fff; border-radius: 50%;
            transition: .3s;
        }
        .toggle-switch input:checked + .toggle-slider { background: linear-gradient(135deg, #667eea, #764ba2); }
        .toggle-switch input:checked + .toggle-slider::before { transform: translateX(22px); }
        .alert { padding: 14px; border-radius: 12px; margin-bottom: 16px; display: none; }
        .alert.show { display: block; }
        .alert.error { background: rgba(254,215,215,0.15); color: #fc8181; }
        .alert.success { background: rgba(198,246,213,0.15); color: #68d391; }
        .result-container { margin-top: 20px; display: none; }
        .result-container.show { display: block; }
        .logs-box pre {
            background: rgba(0,0,0,0.5); color: #e2e8f0; padding: 14px;
            border-radius: 8px; max-height: 300px; overflow-y: auto;
            direction: ltr; text-align: left; font-size: 13px;
        }
        .info-box {
            background: rgba(255,255,255,0.03); border-radius: 12px;
            padding: 14px; margin-top: 20px; border-right: 4px solid #667eea;
            color: #a0aec0; font-size: 13px; line-height: 1.8;
        }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🤖 راه‌انداز ربات تلگرام</h1>
        <p>کد ربات خود را آپلود کنید و اجرا کنید (VROOM)</p>
    </div>
    <div class="alert" id="alertMessage"></div>
    <form id="mainForm">
        <div class="form-group">
            <label>📄 نوع کد:</label>
            <select class="form-control" id="codeType">
                <option value="file">📁 آپلود فایل (پایتون)</option>
                <option value="text">✏️ چسباندن کد</option>
            </select>
        </div>
        <div class="form-group" id="fileInputGroup">
            <div class="file-upload-wrapper" id="dropZone">
                <input type="file" id="fileInput" accept=".py,.txt">
                <div style="font-size:40px">📤</div>
                <div style="color:#a0aec0">کلیک کنید یا فایل را بکشید</div>
                <div id="fileNameDisplay" style="color:#667eea;margin-top:8px;display:none"></div>
            </div>
        </div>
        <div class="form-group" id="textInputGroup" style="display:none">
            <label>✏️ کد ربات:</label>
            <textarea class="form-control" id="codeText" placeholder="# کد خود را اینجا بچسبانید..."></textarea>
        </div>
        <div class="form-group">
            <label>🔑 توکن ربات تلگرام:</label>
            <input class="form-control" id="tokenInput" type="text" placeholder="123456:ABC-DEF..." required>
        </div>
        <div class="form-group">
            <div class="toggle-container">
                <div style="flex:1">
                    <div style="color:#e2e8f0;font-weight:600">🔄 حالت همیشه روشن</div>
                    <div style="color:#718096;font-size:12px">اگر فعال باشد ربات ۲۴ ساعته روشن می‌ماند</div>
                </div>
                <label class="toggle-switch">
                    <input type="checkbox" id="permanentToggle">
                    <span class="toggle-slider"></span>
                </label>
                <span id="toggleStatus" style="color:#fc8181;font-size:12px;min-width:50px">خاموش</span>
            </div>
        </div>
        <button type="submit" class="btn-submit" id="submitBtn">🚀 بررسی و اجرا</button>
    </form>
    <div class="info-box">
        ⚡ کد شما در محیط امن اجرا می‌شود.<br>
        🔒 وابستگی‌های تلگرام به صورت خودکار نصب می‌شوند.<br>
        ⏱️ اگر تیک همیشه روشن فعال باشد، ربات بعد از ریستارت هم بالا می‌آید.
    </div>
    <div class="result-container" id="resultContainer">
        <div id="resultStatus" style="padding:14px;border-radius:12px;text-align:center;font-weight:600;margin-bottom:12px"></div>
        <div class="logs-box">
            <div style="color:#a0aec0;margin-bottom:8px">📋 لاگ‌ها:</div>
            <pre id="resultLogs"></pre>
        </div>
    </div>
</div>
<script>
const form = document.getElementById('mainForm');
const codeType = document.getElementById('codeType');
const fileInput = document.getElementById('fileInput');
const dropZone = document.getElementById('dropZone');
const permanentToggle = document.getElementById('permanentToggle');
const toggleStatus = document.getElementById('toggleStatus');

codeType.onchange = () => {
    document.getElementById('fileInputGroup').style.display = codeType.value === 'file' ? 'block' : 'none';
    document.getElementById('textInputGroup').style.display = codeType.value === 'text' ? 'block' : 'none';
};
permanentToggle.onchange = () => {
    toggleStatus.textContent = permanentToggle.checked ? 'فعال' : 'خاموش';
    toggleStatus.style.color = permanentToggle.checked ? '#68d391' : '#fc8181';
};
dropZone.onclick = () => fileInput.click();
fileInput.onchange = () => {
    if (fileInput.files[0]) {
        document.getElementById('fileNameDisplay').style.display = 'block';
        document.getElementById('fileNameDisplay').textContent = '📎 ' + fileInput.files[0].name;
    }
};
form.onsubmit = async (e) => {
    e.preventDefault();
    const btn = document.getElementById('submitBtn');
    btn.disabled = true;
    btn.textContent = '⏳ در حال اجرا...';
    const fd = new FormData();
    fd.append('token', document.getElementById('tokenInput').value.trim());
    fd.append('code_type', codeType.value);
    fd.append('is_permanent', permanentToggle.checked ? 'on' : 'off');
    if (codeType.value === 'file' && fileInput.files[0]) {
        fd.append('code_file', fileInput.files[0]);
        fd.append('code_text', '');
    } else {
        fd.append('code_text', document.getElementById('codeText').value);
    }
    try {
        const r = await fetch('/api/user/run-bot', { method: 'POST', body: fd });
        const d = await r.json();
        document.getElementById('resultContainer').classList.add('show');
        const st = document.getElementById('resultStatus');
        st.textContent = d.message;
        st.style.background = d.success ? 'rgba(198,246,213,0.15)' : 'rgba(254,215,215,0.15)';
        st.style.color = d.success ? '#68d391' : '#fc8181';
        document.getElementById('resultLogs').textContent = d.logs || 'خروجی خاصی وجود ندارد.';
    } catch (err) {
        alert('خطا در ارتباط با سرور');
    }
    btn.disabled = false;
    btn.textContent = '🚀 بررسی و اجرا';
};
</script>
</body>
</html>"""

@app.get("/bot-runner", response_class=HTMLResponse)
async def bot_runner_page():
    return HTMLResponse(BOT_RUNNER_HTML)

# ===================== LOGIN + DASHBOARD (خلاصه) =====================
LOGIN_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>VROOM</title>
<style>body{font-family:sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#eef2ff;margin:0}
.card{background:#fff;border-radius:24px;padding:36px;width:100%;max-width:360px;box-shadow:0 8px 30px rgba(0,0,0,.08)}
h1{text-align:center;background:linear-gradient(135deg,#6366f1,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
input{width:100%;padding:12px;border-radius:12px;border:1px solid #ddd;margin-bottom:12px}
.btn{width:100%;padding:13px;background:linear-gradient(135deg,#3b82f6,#6366f1);border:none;border-radius:12px;color:#fff;font-weight:700;cursor:pointer}
</style></head><body>
<div class="card"><h1>VROOM</h1>
<form id="f"><input type="password" id="pw" placeholder="رمز عبور"><button class="btn" type="submit">ورود</button></form></div>
<script>
document.getElementById('f').onsubmit=async e=>{e.preventDefault();
const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('pw').value})});
if(r.ok)location.href='/dashboard';else alert('رمز اشتباه');}
</script></body></html>"""

DASHBOARD_HTML = """<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>VROOM Dashboard</title>
<style>
body{font-family:sans-serif;background:#eef2ff;margin:0;padding:20px}
.card{background:#fff;border-radius:16px;padding:16px;margin-bottom:12px;box-shadow:0 4px 16px rgba(0,0,0,.05)}
.btn{padding:8px 14px;border-radius:10px;border:none;background:linear-gradient(135deg,#3b82f6,#6366f1);color:#fff;font-weight:700;cursor:pointer;margin:4px}
a{color:#3b82f6}
</style></head><body>
<h2>VROOM Dashboard v5.8</h2>
<div class="card">
<a class="btn" href="/bot-runner" target="_blank">🤖 راه‌انداز ربات تلگرام</a>
<a class="btn" href="/login">ورود</a>
</div>
<p>برای مدیریت کامل از ربات تلگرام یا API استفاده کنید.</p>
</body></html>"""

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

# ===================== WS =====================
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
            if is_quota_exceeded(LINKS[uid]):
                asyncio.create_task(close_connections_for_link(uid))
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
            if not link_data or not link_data["active"] or is_expired(link_data) or is_quota_exceeded(link_data):
                await websocket.close(code=1008, reason="quota/expired/disabled")
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
