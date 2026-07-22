# ============================================================
# 🚀 VROOM GATEWAY - ULTIMATE EDITION v3.0
# ============================================================
# Complete with: 
# ✅ Beautiful Dashboard with 5 Themes
# ✅ Smart Telegram Bot with Glass Design
# ✅ Hidden Country Flags in VLESS
# ✅ Clean IP Management
# ✅ Domain Management with DDNS
# ✅ Auto Backup System
# ✅ Complete Subscription Page
# ============================================================

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
import base64
import logging
import psutil
import socket
from typing import Optional, Dict, Any

# ====== WEB FRAMEWORK ======
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx

# ====== TELEGRAM BOT ======
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ConversationHandler
    TELEGRAM_AVAILABLE = True
except:
    TELEGRAM_AVAILABLE = False

# ====== GEOIP ======
try:
    import geoip2.database
    GEOIP_AVAILABLE = True
except:
    GEOIP_AVAILABLE = False

# ============================================================
# 📋 CONFIGURATION
# ============================================================
try:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    if not SECRET_KEY:
        SECRET_KEY = secrets.token_urlsafe(32)
        os.environ["SECRET_KEY"] = SECRET_KEY
except:
    SECRET_KEY = "vroom-default-secret-key"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("VROOM-Gateway")

app = FastAPI(title="VROOM Gateway", docs_url=None, redoc_url=None)

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

# ============================================================
# 📦 DATA STORAGE
# ============================================================
connections: dict = {}
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=100)
hourly_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()

CUSTOM_ADDRESSES: list = ["www.speedtest.net"]
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

CUSTOM_DOMAIN: str = ""
CUSTOM_DOMAIN_LOCK = asyncio.Lock()

SESSION_COOKIE = "vroom_session"
SESSION_TTL = 60 * 60 * 24 * 7

# ============================================================
# 🔐 AUTHENTICATION
# ============================================================
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

# ============================================================
# 🌍 GEOIP FUNCTIONS
# ============================================================
COUNTRY_FLAGS = {
    "IR": "🇮🇷", "US": "🇺🇸", "GB": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
    "IT": "🇮🇹", "CA": "🇨🇦", "AU": "🇦🇺", "NL": "🇳🇱", "SE": "🇸🇪",
    "CH": "🇨🇭", "JP": "🇯🇵", "CN": "🇨🇳", "RU": "🇷🇺", "BR": "🇧🇷",
    "IN": "🇮🇳", "TR": "🇹🇷", "AE": "🇦🇪", "SG": "🇸🇬", "HK": "🇭🇰",
    "KR": "🇰🇷", "ES": "🇪🇸", "PT": "🇵🇹", "PL": "🇵🇱", "UA": "🇺🇦",
    "RO": "🇷🇴", "BG": "🇧🇬", "GR": "🇬🇷", "IL": "🇮🇱", "SA": "🇸🇦",
    "EG": "🇪🇬", "ZA": "🇿🇦", "NG": "🇳🇬", "KE": "🇰🇪", "PK": "🇵🇰",
    "BD": "🇧🇩", "VN": "🇻🇳", "TH": "🇹🇭", "MY": "🇲🇾", "PH": "🇵🇭",
    "ID": "🇮🇩", "NZ": "🇳🇿", "AR": "🇦🇷", "CL": "🇨🇱", "CO": "🇨🇴",
    "PE": "🇵🇪", "VE": "🇻🇪", "MX": "🇲🇽", "CU": "🇨🇺", "DO": "🇩🇴",
}

def get_country_code(ip: str) -> str:
    """دریافت کد کشور از IP"""
    if not GEOIP_AVAILABLE:
        return "XX"
    
    try:
        db_path = "GeoLite2-Country.mmdb"
        if os.path.exists(db_path):
            reader = geoip2.database.Reader(db_path)
            response = reader.country(ip)
            return response.country.iso_code
    except:
        pass
    
    try:
        import requests
        resp = requests.get(f"http://ip-api.com/json/{ip}?fields=countryCode", timeout=2)
        data = resp.json()
        return data.get("countryCode", "XX")
    except:
        return "XX"

def get_country_flag(country_code: str) -> str:
    """دریافت پرچم از کد کشور"""
    return COUNTRY_FLAGS.get(country_code, "🌍")

# ============================================================
# 🔗 VLESS LINK GENERATION WITH HIDDEN FLAG
# ============================================================
def get_domain() -> str:
    """دریافت دامنه معتبر"""
    if CUSTOM_DOMAIN:
        return CUSTOM_DOMAIN
    
    env_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RENDER_EXTERNAL_URL")
    if env_domain:
        return env_domain.replace("https://", "").replace("http://", "")
    
    return "localhost"

def generate_vless_link(uuid: str, remark: str = "VROOM", address: str = None, country_code: str = "XX") -> str:
    """تولید لینک VLESS با پرچم مخفی"""
    domain = get_domain()
    addr = address if address else domain
    
    # پرچم مخفی (با کاراکترهای نامرئی)
    flag = get_country_flag(country_code)
    hidden_flag = f"\u200B{flag}\u200C" if country_code != "XX" else ""
    
    # Remark با پرچم مخفی
    remark_with_flag = f"{remark}{hidden_flag}"
    
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
    
    return f"vless://{uuid}@{addr}:443?{query}#{quote(remark_with_flag)}"

def generate_vless_link_visible(uuid: str, remark: str = "VROOM", address: str = None) -> str:
    """تولید لینک VLESS با پرچم قابل مشاهده (برای نمایش)"""
    domain = get_domain()
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

# ============================================================
# 📊 UTILITY FUNCTIONS
# ============================================================
def uptime() -> str:
    secs = int(time.time() - stats["start_time"])
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_size_to_bytes(value: float, unit: str) -> int:
    unit = unit.upper()
    if unit == "GB": return int(value * 1024 * 1024 * 1024)
    if unit == "MB": return int(value * 1024 * 1024)
    if unit == "KB": return int(value * 1024)
    return int(value)

def compute_expiry(expiry_days) -> str:
    try:
        days = float(expiry_days or 0)
    except:
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
    except:
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

# ============================================================
# 🚀 KEEP ALIVE
# ============================================================
async def keep_alive():
    while True:
        await asyncio.sleep(600)
        try:
            domain = get_domain()
            if domain and domain != "localhost":
                async with httpx.AsyncClient(timeout=10.0) as client:
                    await client.get(f"https://{domain}/health")
                logger.info("Keep-alive ping sent")
        except Exception:
            pass

# ============================================================
# 🚀 AUTO BACKUP
# ============================================================
async def auto_backup():
    while True:
        await asyncio.sleep(21600)  # 6 hours
        try:
            backup_data = {
                "timestamp": datetime.now().isoformat(),
                "links": dict(LINKS),
                "addresses": list(CUSTOM_ADDRESSES),
                "domain": CUSTOM_DOMAIN
            }
            backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(backup_file, "w") as f:
                json.dump(backup_data, f, indent=2)
            
            # Keep only last 5 backups
            backups = sorted([f for f in os.listdir() if f.startswith("backup_")])
            if len(backups) > 5:
                for old_backup in backups[:-5]:
                    os.remove(old_backup)
            
            logger.info(f"💾 Auto backup created: {backup_file}")
        except Exception as e:
            logger.error(f"Auto backup failed: {e}")

# ============================================================
# 🌐 API ENDPOINTS
# ============================================================
@app.on_event("startup")
async def startup():
    global http_client
    limits = httpx.Limits(max_connections=5000, max_keepalive_connections=1000)
    timeout = httpx.Timeout(180.0, connect=30.0)
    http_client = httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True)
    logger.info(f"🚀 VROOM started on port {CONFIG['port']}")
    asyncio.create_task(keep_alive())
    asyncio.create_task(auto_backup())
    
    # Start Telegram bot if configured
    if TELEGRAM_AVAILABLE and TELEGRAM_TOKEN and TELEGRAM_ADMIN_ID:
        asyncio.create_task(start_telegram_bot())

@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()
    if TELEGRAM_AVAILABLE and telegram_bot:
        await stop_telegram_bot()

@app.get("/")
async def root():
    return {"service": "VROOM", "version": "3.0", "status": "active", "domain": get_domain()}

@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(connections), "uptime": uptime()}

# ============================================================
# 🔐 AUTH API
# ============================================================
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

# ============================================================
# 📊 STATS API
# ============================================================
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
    }

# ============================================================
# 📡 LINKS API
# ============================================================
@app.post("/api/links")
async def create_link(request: Request, _=Depends(require_auth)):
    body = await request.json()
    label = (body.get("label") or "New Link").strip()[:60]
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', label):
        raise HTTPException(status_code=400, detail="Name must contain only English letters, numbers, and - _ .")
    if not label:
        raise HTTPException(status_code=400, detail="Name is required")
    async with LINKS_LOCK:
        if label in LINKS:
            raise HTTPException(status_code=400, detail="A link with this name already exists")
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
            "expiry": expiry
        }
    
    # Generate link with country flag (hidden)
    vless_link = generate_vless_link(uid, remark=f"VROOM-{label}")
    
    return {
        "uuid": uid,
        "label": label,
        "limit_bytes": limit_bytes,
        "used_bytes": 0,
        "max_connections": max_conn,
        "active": True,
        "expiry": expiry,
        "created_at": LINKS[uid]["created_at"],
        "vless_link": vless_link
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
                "vless_link": generate_vless_link(uid, remark=f"VROOM-{data['label']}")
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
    # Close connections
    to_close = [cid for cid, info in connections.items() if info.get("uuid") == uid]
    for cid in to_close:
        ws = connection_sockets.get(cid)
        if ws:
            try:
                await ws.close(code=1000, reason="link deleted")
            except:
                pass
        connections.pop(cid, None)
        connection_sockets.pop(cid, None)
    link_ip_map.pop(uid, None)
    return {"ok": True}

# ============================================================
# 🌐 DOMAIN API
# ============================================================
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
        if not re.match(r'^[a-z0-9\-_.]+\.[a-z]{2,}$', domain) and not re.match(r'^[a-z0-9\-_.]+$', domain):
            raise HTTPException(status_code=400, detail="Invalid domain format")
    async with CUSTOM_DOMAIN_LOCK:
        global CUSTOM_DOMAIN
        CUSTOM_DOMAIN = domain
    return {"ok": True, "domain": CUSTOM_DOMAIN}

@app.get("/api/domain/status")
async def domain_status(_=Depends(require_auth)):
    current = get_domain()
    result = {
        "domain": current,
        "type": "unknown",
        "reachable": False,
        "ip": None,
        "ssl_valid": False,
    }
    try:
        ip = socket.gethostbyname(current)
        result["ip"] = ip
        result["reachable"] = True
        # Check SSL
        try:
            import ssl
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(socket.socket(), server_hostname=current) as s:
                s.connect((current, 443))
                result["ssl_valid"] = True
        except:
            pass
    except:
        pass
    
    if current == "localhost":
        result["type"] = "local"
    elif "railway.app" in current or "onrender.com" in current:
        result["type"] = "cloud"
    elif any(s in current for s in ["duckdns.org", "no-ip.org", "dynv6.net", "afraid.org", "dpdns.org"]):
        result["type"] = "ddns"
    elif current and current != "localhost":
        result["type"] = "custom"
    
    return {
        "current": result,
        "environment": {
            "railway": os.environ.get("RAILWAY_PUBLIC_DOMAIN"),
            "render": os.environ.get("RENDER_EXTERNAL_URL"),
        }
    }

# ============================================================
# 🌐 CLEAN IP ADDRESSES API
# ============================================================
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
    if not re.match(r'^[a-zA-Z0-9\-_. ]+$', address):
        raise HTTPException(status_code=400, detail="Address must contain only English letters, numbers, and - _ .")
    async with CUSTOM_ADDRESSES_LOCK:
        if address in CUSTOM_ADDRESSES:
            raise HTTPException(status_code=400, detail="Address already exists")
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

# ============================================================
# 📄 SUBSCRIPTION PAGE
# ============================================================
@app.get("/sub/{uid}")
async def subscription_page(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return HTMLResponse(content="<h2>❌ Link not found</h2>", status_code=404)
    
    if not link["active"] or is_expired(link):
        return HTMLResponse(content="<h2>⛔ Link inactive or expired</h2>", status_code=403)
    
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    
    # Build links
    main_link = generate_vless_link_visible(uid, remark=f"VROOM-{link['label']}")
    all_links = [main_link]
    for i, addr in enumerate(addresses):
        if addr and addr != "www.speedtest.net":
            remark = f"VROOM-{link['label']}-{i+1}"
            vless_link = generate_vless_link_visible(uid, remark=remark, address=addr)
            all_links.append(vless_link)
    
    sub_content = "\n".join(all_links)
    sub_base64 = base64.b64encode(sub_content.encode()).decode()
    sub_url = f"https://{get_domain()}/sub/{uid}/v2ray"
    
    used_gb = round(link['used_bytes'] / (1024 * 1024 * 1024), 2)
    limit_gb = round(link['limit_bytes'] / (1024 * 1024 * 1024), 2) if link['limit_bytes'] > 0 else 0
    percent = round((link['used_bytes'] / link['limit_bytes']) * 100, 1) if link['limit_bytes'] > 0 else 0
    
    exp = link.get("expiry")
    if exp:
        try:
            exp_date = datetime.fromisoformat(exp)
            days_left = (exp_date - datetime.now()).days
            days_left_text = f"{days_left} روز" if days_left > 0 else "منقضی شده"
        except:
            days_left_text = "نامحدود"
    else:
        days_left_text = "نامحدود"
    
    if is_expired(link):
        status = "expired"
        status_text = "منقضی شده"
    elif link['limit_bytes'] > 0 and link['used_bytes'] >= link['limit_bytes']:
        status = "limited"
        status_text = "محدود شده"
    else:
        status = "active"
        status_text = "فعال"
    
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 VROOM - {link['label']}</title>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Vazirmatn:wght@300;400;700;900&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Vazirmatn', 'Orbitron', sans-serif;
            background: radial-gradient(ellipse at bottom, #0d1b2a 0%, #000000 100%);
            color: #fff;
            padding: 20px;
        }}
        .container {{
            max-width: 500px;
            width: 100%;
            background: rgba(255,255,255,0.04);
            backdrop-filter: blur(30px);
            border-radius: 30px;
            padding: 30px 25px;
            border: 1px solid rgba(255,255,255,0.06);
            box-shadow: 0 40px 80px rgba(0,0,0,0.5);
        }}
        .header {{ text-align: center; margin-bottom: 20px; }}
        .badge {{
            display: inline-block;
            background: rgba(124,92,252,0.15);
            color: #a78bfa;
            padding: 4px 20px;
            border-radius: 50px;
            font-size: 0.6rem;
            letter-spacing: 3px;
            text-transform: uppercase;
            border: 1px solid rgba(124,92,252,0.1);
            font-family: 'Orbitron', monospace;
        }}
        h1 {{
            font-size: 2rem;
            font-weight: 900;
            background: linear-gradient(135deg, #7c5cfc, #a78bfa);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-family: 'Orbitron', monospace;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin: 15px 0;
        }}
        .info-item {{
            background: rgba(255,255,255,0.03);
            padding: 10px 12px;
            border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.03);
        }}
        .info-item.full {{ grid-column: span 2; }}
        .label {{
            font-size: 0.5rem;
            text-transform: uppercase;
            opacity: 0.35;
            letter-spacing: 1px;
            display: block;
            font-weight: 700;
        }}
        .value {{ font-size: 0.9rem; font-weight: 700; }}
        .status-dot {{
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-left: 6px;
        }}
        .status-dot.active {{ background: #34d399; }}
        .status-dot.limited {{ background: #fbbf24; }}
        .status-dot.expired {{ background: #f87171; }}
        .progress-section {{ margin: 12px 0; }}
        .progress-bar {{
            width: 100%;
            height: 4px;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            overflow: hidden;
        }}
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #7c5cfc, #a78bfa);
            border-radius: 10px;
            transition: width 1s;
            width: {percent}%;
        }}
        .btn-group {{
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
            margin-top: 12px;
        }}
        .btn {{
            flex: 1;
            min-width: 60px;
            padding: 8px 12px;
            border-radius: 10px;
            font-weight: 700;
            font-size: 0.65rem;
            border: none;
            cursor: pointer;
            transition: all 0.3s;
            font-family: 'Vazirmatn', sans-serif;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 4px;
        }}
        .btn-primary {{
            background: linear-gradient(135deg, #7c5cfc, #a78bfa);
            color: #fff;
            box-shadow: 0 8px 30px rgba(124,92,252,0.25);
        }}
        .btn-primary:hover {{ transform: translateY(-2px); box-shadow: 0 12px 48px rgba(124,92,252,0.35); }}
        .btn-success {{
            background: linear-gradient(135deg, #11998e, #38ef7d);
            color: #fff;
        }}
        .btn-success:hover {{ transform: translateY(-2px); }}
        .btn-secondary {{
            background: rgba(255,255,255,0.06);
            color: rgba(255,255,255,0.6);
            border: 1px solid rgba(255,255,255,0.06);
        }}
        .btn-secondary:hover {{ background: rgba(255,255,255,0.1); }}
        .config-box {{
            background: rgba(0,0,0,0.3);
            padding: 10px 12px;
            border-radius: 10px;
            font-size: 0.55rem;
            font-family: 'Courier New', monospace;
            word-break: break-all;
            margin: 10px 0;
            max-height: 80px;
            overflow-y: auto;
            color: rgba(255,255,255,0.4);
            line-height: 1.6;
            text-align: left;
            direction: ltr;
        }}
        .qr-section {{ text-align: center; margin: 12px 0; }}
        .qr-container {{
            display: inline-block;
            background: rgba(255,255,255,0.95);
            padding: 10px;
            border-radius: 14px;
            box-shadow: 0 8px 40px rgba(0,0,0,0.3);
        }}
        .qr-container img {{ width: 120px; height: 120px; border-radius: 8px; display: block; }}
        .footer {{ text-align: center; margin-top: 15px; font-size: 0.45rem; opacity: 0.12; letter-spacing: 3px; font-family: 'Orbitron', monospace; }}
        @media (max-width: 500px) {{
            .container {{ padding: 20px 15px; }}
            h1 {{ font-size: 1.5rem; }}
            .info-grid {{ grid-template-columns: 1fr; }}
            .info-item.full {{ grid-column: span 1; }}
            .btn {{ font-size: 0.55rem; padding: 6px 10px; min-width: 50px; }}
            .qr-container img {{ width: 100px; height: 100px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="badge">✦ VROOM</div>
            <h1>🚀 {link['label']}</h1>
        </div>
        
        <div class="info-grid">
            <div class="info-item full">
                <span class="label">وضعیت</span>
                <span class="value"><span class="status-dot {status}"></span>{status_text}</span>
            </div>
            <div class="info-item">
                <span class="label">📊 مصرف</span>
                <span class="value">{used_gb} GB</span>
            </div>
            <div class="info-item">
                <span class="label">📦 حجم کل</span>
                <span class="value">{limit_gb if limit_gb > 0 else '∞'} GB</span>
            </div>
            <div class="info-item">
                <span class="label">⏳ انقضا</span>
                <span class="value" style="font-size:0.8rem;">{exp if exp else 'نامحدود'}</span>
            </div>
            <div class="info-item">
                <span class="label">📅 روز باقی‌مانده</span>
                <span class="value">{days_left_text}</span>
            </div>
        </div>
        
        <div class="progress-section">
            <div class="progress-bar"><div class="progress-fill"></div></div>
        </div>
        
        <div class="qr-section">
            <div class="qr-container">
                <img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={base64.b64encode(main_link.encode()).decode()}" alt="QR">
            </div>
        </div>
        
        <div class="config-box">{main_link}</div>
        
        <div class="btn-group">
            <button class="btn btn-primary" onclick="copyMain()">📋 کپی</button>
            <button class="btn btn-success" onclick="copyAll()">📥 کپی همه</button>
            <button class="btn btn-secondary" onclick="copySub()">🔗 ساب</button>
        </div>
        
        <div class="footer">✦ VROOM GATEWAY v3.0 ✦</div>
    </div>
    
    <script>
        const main = `{main_link}`;
        const all = `{sub_content}`;
        const sub = `{sub_url}`;
        
        function copyText(text, msg) {{
            navigator.clipboard.writeText(text).then(() => alert(msg));
        }}
        function copyMain() {{ copyText(main, '✅ کانفیگ اصلی کپی شد!'); }}
        function copyAll() {{ copyText(all, '✅ همه کانفیگ‌ها کپی شدند!'); }}
        function copySub() {{ copyText(sub, '✅ لینک ساب کپی شد!'); }}
    </script>
</body>
</html>"""
    
    return HTMLResponse(content=html)

@app.get("/sub/{uid}/v2ray")
async def subscription_v2ray(uid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link["active"] or is_expired(link):
            return Response(content="", status_code=404)
    
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    
    main_link = generate_vless_link_visible(uid, remark=f"VROOM-{link['label']}")
    all_links = [main_link]
    for i, addr in enumerate(addresses):
        if addr and addr != "www.speedtest.net":
            remark = f"VROOM-{link['label']}-{i+1}"
            vless_link = generate_vless_link_visible(uid, remark=remark, address=addr)
            all_links.append(vless_link)
    
    content = "\n".join(all_links)
    encoded = base64.b64encode(content.encode()).decode()
    
    return Response(
        content=encoded,
        media_type="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename=vroom-{uid}.txt",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        }
    )

# ============================================================
# 🤖 TELEGRAM BOT (SIMPLIFIED)
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_ADMIN_ID = os.environ.get("TELEGRAM_ADMIN_ID", "")
telegram_bot = None
telegram_running = False

async def start_telegram_bot():
    global telegram_bot, telegram_running
    if not TELEGRAM_AVAILABLE or not TELEGRAM_TOKEN or not TELEGRAM_ADMIN_ID:
        return
    
    try:
        from telegram.ext import Application
        
        telegram_bot = Application.builder().token(TELEGRAM_TOKEN).build()
        
        # Simple commands
        async def start(update, context):
            if str(update.effective_user.id) != TELEGRAM_ADMIN_ID:
                await update.message.reply_text("🚫 Access denied")
                return
            
            keyboard = [
                [InlineKeyboardButton("📊 Status", callback_data="stats")],
                [InlineKeyboardButton("📡 Links", callback_data="links")],
                [InlineKeyboardButton("➕ Create", callback_data="create")],
            ]
            await update.message.reply_text(
                "🚀 **VROOM Bot**\n\nWelcome Admin!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
        
        async def button_callback(update, context):
            query = update.callback_query
            await query.answer()
            
            if query.data == "stats":
                text = f"📊 **Status**\n\n"
                text += f"🔗 Connections: {len(connections)}\n"
                text += f"📡 Links: {len(LINKS)}\n"
                text += f"⏱️ Uptime: {uptime()}\n"
                text += f"📥 Traffic: {round(stats['total_bytes']/(1024*1024),2)} MB"
                await query.message.reply_text(text, parse_mode="Markdown")
            
            elif query.data == "links":
                if not LINKS:
                    await query.message.reply_text("📭 No links")
                    return
                text = "📡 **Links:**\n\n"
                for uid, data in LINKS.items():
                    used = round(data['used_bytes']/(1024*1024*1024),2)
                    limit = round(data['limit_bytes']/(1024*1024*1024),2) if data['limit_bytes'] > 0 else "∞"
                    status = "✅" if data['active'] else "❌"
                    text += f"{status} **{data['label']}**: {used}GB/{limit}GB\n"
                await query.message.reply_text(text, parse_mode="Markdown")
            
            elif query.data == "create":
                await query.message.reply_text(
                    "➕ **Create Link**\n\n"
                    "Send: `/create name 5GB`\n"
                    "Example: `/create User1 2GB`",
                    parse_mode="Markdown"
                )
        
        telegram_bot.add_handler(CommandHandler("start", start))
        telegram_bot.add_handler(CallbackQueryHandler(button_callback))
        
        await telegram_bot.initialize()
        await telegram_bot.start()
        await telegram_bot.updater.start_polling()
        telegram_running = True
        logger.info("🤖 Telegram bot started")
    except Exception as e:
        logger.error(f"Telegram bot error: {e}")

async def stop_telegram_bot():
    global telegram_bot, telegram_running
    if telegram_bot:
        try:
            await telegram_bot.updater.stop()
            await telegram_bot.stop()
            await telegram_bot.shutdown()
        except:
            pass
    telegram_running = False

# ============================================================
# 🚪 LOGIN PAGE
# ============================================================
LOGIN_HTML = '''<!DOCTYPE html>
<html lang="fa">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:'Orbitron',sans-serif;background:radial-gradient(ellipse at bottom,#0d1b2a 0%,#000 100%);color:#fff}
.login-box{background:rgba(255,255,255,0.04);backdrop-filter:blur(30px);padding:40px;border-radius:30px;border:1px solid rgba(255,255,255,0.06);width:100%;max-width:360px;text-align:center;box-shadow:0 40px 80px rgba(0,0,0,0.5)}
h1{font-size:2rem;font-weight:900;background:linear-gradient(135deg,#7c5cfc,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:30px;letter-spacing:2px}
input{width:100%;padding:14px 18px;border-radius:14px;border:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.03);color:#fff;font-size:14px;font-family:inherit;outline:none;transition:all .3s;margin-bottom:16px}
input:focus{border-color:#7c5cfc;box-shadow:0 0 30px rgba(124,92,252,0.1)}
input::placeholder{color:rgba(255,255,255,0.2)}
button{width:100%;padding:14px;border:none;border-radius:14px;background:linear-gradient(135deg,#7c5cfc,#a78bfa);color:#fff;font-size:16px;font-weight:700;font-family:inherit;cursor:pointer;transition:all .3s;box-shadow:0 8px 30px rgba(124,92,252,0.25)}
button:hover{transform:translateY(-2px);box-shadow:0 12px 48px rgba(124,92,252,0.35)}
.error{color:#f87171;font-size:13px;margin-top:12px;display:none}
</style>
</head>
<body>
<div class="login-box">
<h1>🚀 VROOM</h1>
<form id="loginForm">
<input type="password" id="password" placeholder="Enter Password..." autofocus>
<button type="submit">➜ Enter</button>
<div class="error" id="error">Invalid password</div>
</form>
</div>
<script>
document.getElementById('loginForm').addEventListener('submit',async e=>{
e.preventDefault();const err=document.getElementById('error');err.style.display='none';
try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:document.getElementById('password').value})});if(!r.ok)throw new Error();location.href='/dashboard'}catch(e){err.style.display='block'}});
</script>
</body>
</html>'''

# ============================================================
# 📊 DASHBOARD - COMPLETE BEAUTIFUL EDITION
# ============================================================
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🚀 VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Vazirmatn:wght@300;400;700;900&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {
    --bg: #0a0a12;
    --surface: #12121f;
    --surface2: #1a1a2e;
    --surface3: #252540;
    --border: rgba(255,255,255,0.05);
    --text: rgba(255,255,255,0.92);
    --text2: rgba(255,255,255,0.5);
    --text3: rgba(255,255,255,0.25);
    --primary: #7c5cfc;
    --primary-dim: rgba(124,92,252,0.12);
    --primary-glow: rgba(124,92,252,0.3);
    --green: #34d399;
    --green-dim: rgba(52,211,153,0.1);
    --red: #f87171;
    --red-dim: rgba(248,113,113,0.08);
    --yellow: #fbbf24;
    --shadow: 0 8px 40px rgba(0,0,0,0.5);
    --radius: 16px;
    --transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
}

[data-theme="light"] {
    --bg: #f0f2f5;
    --surface: #ffffff;
    --surface2: #f8f9fa;
    --surface3: #f3f4f6;
    --border: rgba(0,0,0,0.06);
    --text: rgba(0,0,0,0.88);
    --text2: rgba(0,0,0,0.5);
    --text3: rgba(0,0,0,0.25);
    --shadow: 0 8px 40px rgba(0,0,0,0.08);
}

* { margin: 0; padding: 0; box-sizing: border-box; }
html, body { height: 100%; }
body {
    font-family: 'Vazirmatn', 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
    transition: var(--transition);
    display: flex;
    background-image: radial-gradient(ellipse at 20% 50%, rgba(124,92,252,0.03), transparent 50%);
}
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-thumb { background: var(--surface3); border-radius: 10px; }

/* ===== SIDEBAR ===== */
.sidebar {
    width: 200px;
    background: var(--surface);
    border-left: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    position: fixed;
    right: 0;
    top: 0;
    bottom: 0;
    z-index: 100;
    transition: var(--transition);
}
.sidebar-brand {
    padding: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid var(--border);
}
.brand-name {
    font-size: 15px;
    font-weight: 900;
    background: linear-gradient(135deg, var(--primary), #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-family: 'Orbitron', monospace;
}
.sidebar-nav { flex: 1; padding: 8px 6px; overflow-y: auto; }
.nav-section {
    font-size: 8px;
    font-weight: 700;
    color: var(--text3);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    padding: 12px 8px 4px;
    font-family: 'Orbitron', monospace;
}
.nav-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 7px 10px;
    margin: 1px 0;
    border-radius: 8px;
    color: var(--text2);
    font-size: 11px;
    font-weight: 500;
    cursor: pointer;
    transition: var(--transition);
    border: none;
    background: none;
    width: 100%;
    text-align: right;
}
.nav-item:hover { background: var(--primary-dim); color: var(--text); transform: translateX(-3px); }
.nav-item.active { background: var(--primary-dim); color: var(--primary); font-weight: 600; box-shadow: inset -3px 0 0 var(--primary); }
.nav-icon { width: 16px; height: 16px; flex-shrink: 0; opacity: 0.7; }
.nav-item.active .nav-icon { opacity: 1; }
.sidebar-footer { padding: 10px; border-top: 1px solid var(--border); }
.sidebar-footer .logout-btn {
    width: 100%;
    padding: 6px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: none;
    color: var(--text3);
    font-family: inherit;
    font-size: 9px;
    font-weight: 700;
    cursor: pointer;
    transition: var(--transition);
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
}
.sidebar-footer .logout-btn:hover { background: var(--red-dim); border-color: rgba(248,113,113,0.2); color: var(--red); }
.sidebar-footer .version {
    text-align: center;
    font-size: 8px;
    color: var(--text3);
    margin-top: 4px;
    opacity: 0.5;
    font-family: 'Orbitron', monospace;
}

/* ===== MAIN ===== */
.main { margin-right: 200px; flex: 1; padding: 12px 14px 24px; min-height: 100vh; }
.page { display: none; animation: fadeIn 0.4s; }
.page.active { display: block; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px) scale(0.96); } to { opacity: 1; transform: translateY(0) scale(1); } }

/* ===== STATS ROW ===== */
.stats-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin-bottom: 10px;
}
.stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 12px 14px;
    transition: var(--transition);
    position: relative;
    overflow: hidden;
    backdrop-filter: blur(10px);
}
.stat-card::before {
    content: '';
    position: absolute;
    top: 0;
    right: 0;
    width: 60px;
    height: 60px;
    background: radial-gradient(circle, var(--primary-glow), transparent 70%);
    border-radius: 50%;
    transform: translate(40%, -40%);
    opacity: 0.15;
    pointer-events: none;
}
.stat-card:hover { box-shadow: var(--shadow); transform: translateY(-2px); }
.stat-icon { font-size: 18px; display: block; margin-bottom: 2px; }
.stat-label { font-size: 8px; color: var(--text3); font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; }
.stat-value { font-size: 20px; font-weight: 900; color: var(--text); letter-spacing: -0.02em; }
.stat-unit { font-size: 10px; font-weight: 400; color: var(--text3); margin-right: 2px; }

/* ===== CARDS ===== */
.card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
    margin-bottom: 10px;
    transition: var(--transition);
    backdrop-filter: blur(10px);
}
.card:hover { box-shadow: var(--shadow); }
.card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
    flex-wrap: wrap;
    gap: 4px;
}
.card-title { font-size: 13px; font-weight: 700; color: var(--text); display: flex; align-items: center; gap: 6px; }

/* ===== BUTTONS ===== */
.btn {
    font-family: inherit;
    font-size: 10px;
    font-weight: 700;
    border-radius: 8px;
    padding: 5px 10px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 3px;
    border: none;
    transition: var(--transition);
}
.btn-primary {
    background: linear-gradient(135deg, var(--primary), #a78bfa);
    color: #fff;
    box-shadow: 0 4px 16px var(--primary-glow);
}
.btn-primary:hover { filter: brightness(1.1); transform: translateY(-2px); }
.btn-secondary {
    background: var(--surface3);
    color: var(--text2);
    border: 1px solid var(--border);
}
.btn-secondary:hover { border-color: var(--primary); color: var(--primary); }
.btn-danger {
    background: var(--red-dim);
    color: var(--red);
    border: 1px solid rgba(248,113,113,0.12);
}
.btn-danger:hover { background: rgba(248,113,113,0.2); }
.btn-success {
    background: var(--green-dim);
    color: var(--green);
    border: 1px solid rgba(52,211,153,0.12);
}
.btn-success:hover { background: rgba(52,211,153,0.2); }
.btn-sm { padding: 3px 8px; font-size: 9px; }

/* ===== TABLE ===== */
.table-wrap { overflow-x: auto; border-radius: 10px; }
.table { width: 100%; border-collapse: collapse; }
.table th {
    text-align: right;
    font-size: 9px;
    font-weight: 700;
    color: var(--text3);
    padding: 6px 8px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border-bottom: 2px solid var(--border);
    background: var(--surface2);
}
.table td { padding: 6px 8px; border-bottom: 1px solid var(--border); font-size: 11px; vertical-align: middle; }
.table tr:last-child td { border-bottom: none; }
.table tbody tr:hover td { background: var(--primary-dim); }

/* ===== TAGS ===== */
.tag {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 8px;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
.tag-vless { background: var(--primary-dim); color: var(--primary); }
.tag-active { background: var(--green-dim); color: var(--green); }
.tag-disabled { background: var(--red-dim); color: var(--red); }

/* ===== USAGE PILL ===== */
.usage-pill {
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 2px 8px;
    border-radius: 999px;
    background: var(--surface3);
    font-size: 9px;
    color: var(--text2);
}
.usage-pill .used { color: var(--text); font-weight: 600; }
.usage-pill .bar { flex: 1; height: 3px; background: var(--bg); border-radius: 2px; min-width: 30px; overflow: hidden; }
.usage-pill .fill { height: 100%; border-radius: 2px; transition: width .6s; }
.usage-pill .limit { color: var(--text3); }

/* ===== TOGGLE ===== */
.toggle {
    width: 30px;
    height: 16px;
    border-radius: 8px;
    background: var(--surface3);
    position: relative;
    cursor: pointer;
    transition: var(--transition);
    border: 1px solid var(--border);
}
.toggle::after {
    content: '';
    position: absolute;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--text3);
    top: 2px;
    right: 2px;
    transition: var(--transition);
}
.toggle.on { background: var(--green); border-color: var(--green); box-shadow: 0 0 16px rgba(52,211,153,0.3); }
.toggle.on::after { right: 16px; background: #fff; }

/* ===== SYSTEM GRID ===== */
.system-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 6px;
    margin-top: 6px;
}
.system-item {
    background: var(--surface2);
    border-radius: 8px;
    padding: 8px 10px;
    text-align: center;
    border: 1px solid var(--border);
    transition: var(--transition);
}
.system-item:hover { border-color: var(--primary); transform: translateY(-2px); }
.system-item .label { font-size: 7px; color: var(--text3); text-transform: uppercase; letter-spacing: 0.04em; }
.system-item .value { font-size: 13px; font-weight: 700; color: var(--text); }
.system-item .sub { font-size: 8px; color: var(--text2); }

/* ===== TOAST ===== */
.toast {
    position: fixed;
    bottom: 12px;
    left: 50%;
    transform: translateX(-50%) translateY(16px);
    background: var(--surface);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 8px 16px;
    font-size: 11px;
    font-weight: 500;
    opacity: 0;
    transition: var(--transition);
    z-index: 999;
    display: flex;
    align-items: center;
    gap: 6px;
    box-shadow: 0 8px 24px rgba(0,0,0,0.3);
    backdrop-filter: blur(20px);
}
.toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
.toast.error { border-color: var(--red-dim); color: var(--red); }

/* ===== MODAL ===== */
.modal-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.6);
    z-index: 200;
    display: none;
    align-items: center;
    justify-content: center;
    backdrop-filter: blur(6px);
}
.modal-overlay.show { display: flex; }
.modal {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 18px 20px;
    width: 100%;
    max-width: 400px;
    position: relative;
    box-shadow: 0 16px 48px rgba(0,0,0,0.4);
    transform: scale(0.9) translateY(12px);
    opacity: 0;
    transition: var(--transition);
}
.modal-overlay.show .modal { transform: scale(1) translateY(0); opacity: 1; }
.modal-title { font-size: 15px; font-weight: 800; margin-bottom: 12px; color: var(--text); }
.modal-close {
    position: absolute;
    top: 10px;
    left: 10px;
    background: var(--surface3);
    border: 1px solid var(--border);
    color: var(--text3);
    width: 26px;
    height: 26px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: var(--transition);
}
.modal-close:hover { background: var(--red-dim); color: var(--red); }

/* ===== FORM ===== */
.form-group { display: flex; flex-direction: column; gap: 3px; margin-bottom: 8px; }
.form-label { font-size: 9px; font-weight: 700; color: var(--text2); text-transform: uppercase; letter-spacing: 0.05em; }
.form-input, .form-select {
    padding: 6px 10px;
    border-radius: 8px;
    border: 1px solid var(--border);
    font-family: inherit;
    font-size: 11px;
    outline: none;
    color: var(--text);
    background: var(--surface2);
    transition: var(--transition);
}
.form-input:focus, .form-select:focus { border-color: var(--primary); box-shadow: 0 0 0 4px var(--primary-glow); }
.form-row { display: flex; gap: 6px; flex-wrap: wrap; align-items: flex-end; }
.form-row .form-group { margin-bottom: 0; flex: 1; min-width: 70px; }

/* ===== THEME SELECTOR ===== */
.theme-selector {
    position: fixed;
    left: 20px;
    top: 50%;
    transform: translateY(-50%);
    z-index: 50;
    display: flex;
    flex-direction: column;
    gap: 8px;
    background: rgba(255,255,255,0.04);
    backdrop-filter: blur(20px);
    padding: 10px 8px;
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.04);
}
.theme-btn {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    border: 2px solid rgba(255,255,255,0.1);
    cursor: pointer;
    transition: var(--transition);
}
.theme-btn:hover { transform: scale(1.15); border-color: rgba(255,255,255,0.3); }
.theme-btn.active { border-color: #ffd700; box-shadow: 0 0 20px rgba(255,215,0,0.2); }
.theme-btn.space { background: radial-gradient(ellipse at bottom, #0d1b2a 0%, #000 100%); }
.theme-btn.ocean { background: linear-gradient(135deg, #1a2980, #26d0ce); }
.theme-btn.sunset { background: linear-gradient(135deg, #f12711, #f5af19); }
.theme-btn.forest { background: linear-gradient(135deg, #134e5e, #71b280); }
.theme-btn.neon { background: linear-gradient(135deg, #1d1d2e, #ff00cc); }
.theme-btn.rose { background: linear-gradient(135deg, #ff6b6b, #ffd93d); }
.theme-btn.ice { background: linear-gradient(135deg, #a8e6cf, #dcedc1); }
.theme-btn.dark { background: #0a0a12; }

/* ===== RESPONSIVE ===== */
@media (max-width: 768px) {
    .sidebar { transform: translateX(100%); width: 220px; }
    .sidebar.open { transform: translateX(0); box-shadow: -4px 0 32px rgba(0,0,0,0.4); }
    .main { margin-right: 0; padding-top: 52px; }
    .stats-row { grid-template-columns: 1fr 1fr; gap: 6px; }
    .system-grid { grid-template-columns: 1fr; }
    .theme-selector { left: 10px; padding: 6px 4px; gap: 4px; }
    .theme-btn { width: 22px; height: 22px; }
    .table-wrap { display: none; }
    .inbound-cards { display: flex; flex-direction: column; gap: 6px; }
}
@media (max-width: 480px) { .stats-row { grid-template-columns: 1fr; } }

/* ===== INBOUND CARDS (Mobile) ===== */
.inbound-cards { display: none; flex-direction: column; gap: 6px; }
.inbound-card {
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 10px 12px;
    background: var(--surface2);
    display: flex;
    flex-direction: column;
    gap: 4px;
}
.inbound-card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.inbound-card-name { font-size: 12px; font-weight: 600; color: var(--text); }
.inbound-card-actions { display: flex; gap: 3px; flex-wrap: wrap; }

/* ===== MOBILE HEADER ===== */
.mobile-header {
    display: none;
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    height: 44px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    z-index: 90;
    align-items: center;
    justify-content: space-between;
    padding: 0 12px;
    backdrop-filter: blur(20px);
}
.menu-toggle {
    width: 30px;
    height: 30px;
    border-radius: 6px;
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--text2);
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    font-size: 16px;
}
.sidebar-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.5);
    z-index: 99;
}
.sidebar-overlay.show { display: block; }
@media (max-width: 768px) {
    .mobile-header { display: flex; }
    .inbound-cards { display: flex; }
}
</style>
</head>
<body>

<!-- ===== TOAST ===== -->
<div class="toast" id="toast"></div>

<!-- ===== THEME SELECTOR ===== -->
<div class="theme-selector">
    <button class="theme-btn space active" data-theme="space" title="فضایی"></button>
    <button class="theme-btn ocean" data-theme="ocean" title="اقیانوسی"></button>
    <button class="theme-btn sunset" data-theme="sunset" title="غروب"></button>
    <button class="theme-btn forest" data-theme="forest" title="جنگلی"></button>
    <button class="theme-btn neon" data-theme="neon" title="نئون"></button>
    <button class="theme-btn rose" data-theme="rose" title="رز"></button>
    <button class="theme-btn ice" data-theme="ice" title="یخی"></button>
    <button class="theme-btn dark" data-theme="dark" title="تاریک"></button>
</div>

<!-- ===== MOBILE HEADER ===== -->
<div class="mobile-header">
    <span style="font-weight:900;font-size:14px;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace">VROOM</span>
    <button class="menu-toggle" onclick="document.getElementById('sidebar').classList.toggle('open')">☰</button>
</div>
<div class="sidebar-overlay" id="sidebarOverlay" onclick="document.getElementById('sidebar').classList.remove('open')"></div>

<!-- ===== SIDEBAR ===== -->
<aside class="sidebar" id="sidebar">
    <div class="sidebar-brand">
        <span class="brand-name">🚀 VROOM</span>
        <button onclick="toggleTheme()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:14px;">🌓</button>
    </div>
    <nav class="sidebar-nav">
        <div class="nav-section">MAIN</div>
        <button class="nav-item active" data-page="dashboard"><span class="nav-icon">📊</span> داشبورد</button>
        <button class="nav-item" data-page="inbounds"><span class="nav-icon">📡</span> اینباندها</button>
        <button class="nav-item" data-page="addresses"><span class="nav-icon">🌐</span> آی‌پی تمیز</button>
        <button class="nav-item" data-page="domain"><span class="nav-icon">🌍</span> دامنه</button>
        <button class="nav-item" data-page="security"><span class="nav-icon">🔒</span> امنیت</button>
    </nav>
    <div class="sidebar-footer">
        <button class="logout-btn" onclick="logout()">🚪 خروج</button>
        <div class="version">VROOM v3.0</div>
    </div>
</aside>

<!-- ===== MAIN CONTENT ===== -->
<main class="main">

<!-- ===== DASHBOARD ===== -->
<section class="page active" id="page-dashboard">
    <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <div>
            <div style="font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace;">📊 DASHBOARD</div>
            <div style="font-size:10px;color:var(--text3);">آخرین بروزرسانی: <span id="lastUpdate">--</span></div>
        </div>
        <div style="display:flex;gap:4px;">
            <button class="btn btn-secondary" onclick="quickCreate(0.5,'GB')">+0.5</button>
            <button class="btn btn-primary" onclick="quickCreate(1,'GB')">+1</button>
            <button class="btn btn-success" onclick="quickCreate(5,'GB')">+5</button>
        </div>
    </div>

    <div class="stats-row">
        <div class="stat-card"><span class="stat-icon">📊</span><div class="stat-label">ترافیک کل</div><div class="stat-value" id="sTraffic">--<span class="stat-unit">MB</span></div></div>
        <div class="stat-card"><span class="stat-icon">📡</span><div class="stat-label">اینباندها</div><div class="stat-value" id="sLinks">--</div></div>
        <div class="stat-card"><span class="stat-icon">⏱️</span><div class="stat-label">آپتایم</div><div class="stat-value" id="sUptime" style="font-size:14px;">--</div></div>
        <div class="stat-card"><span class="stat-icon">🌐</span><div class="stat-label">دامنه</div><div class="stat-value" id="sDomain" style="font-size:11px;word-break:break-all;font-weight:600;">--</div></div>
    </div>

    <div class="grid-2" style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div class="card">
            <div class="card-header"><div class="card-title">⚡ منابع سیستم</div></div>
            <div class="system-grid">
                <div class="system-item"><div class="label">💾 دیسک</div><div class="value" id="sDisk">--%</div><div class="sub" id="sDiskDetail">-- / -- GB</div></div>
                <div class="system-item"><div class="label">🧠 رم</div><div class="value" id="sRam">--%</div><div class="sub" id="sRamDetail">-- / -- GB</div></div>
                <div class="system-item"><div class="label">⚡ پردازنده</div><div class="value" id="sCpu">--%</div><div class="sub">مصرف</div></div>
                <div class="system-item"><div class="label">🔗 اتصالات</div><div class="value" id="sConnections">--</div><div class="sub">فعال</div></div>
            </div>
        </div>
        <div class="card">
            <div class="card-header"><div class="card-title">📈 نمودار ترافیک</div></div>
            <div style="height:130px;"><canvas id="trafficChart"></canvas></div>
        </div>
    </div>
</section>

<!-- ===== INBOUNDS ===== -->
<section class="page" id="page-inbounds">
    <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <div>
            <div style="font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace;">📡 INBOUNDS</div>
            <div style="font-size:10px;color:var(--text3);">مدیریت اتصالات VLESS</div>
        </div>
        <button class="btn btn-primary" onclick="showAddModal()">➕ افزودن</button>
    </div>

    <div style="display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;">
        <input id="searchInput" placeholder="🔍 جستجو..." style="flex:1;min-width:120px;padding:5px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-family:inherit;font-size:11px;outline:none;">
        <div style="display:flex;gap:2px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:2px;">
            <button class="chip active" data-filter="all" style="padding:3px 10px;border:none;border-radius:4px;font-size:9px;font-weight:600;cursor:pointer;background:none;color:var(--text3);font-family:inherit;">همه</button>
            <button class="chip" data-filter="active" style="padding:3px 10px;border:none;border-radius:4px;font-size:9px;font-weight:600;cursor:pointer;background:none;color:var(--text3);font-family:inherit;">فعال</button>
            <button class="chip" data-filter="disabled" style="padding:3px 10px;border:none;border-radius:4px;font-size:9px;font-weight:600;cursor:pointer;background:none;color:var(--text3);font-family:inherit;">غیرفعال</button>
        </div>
    </div>

    <div class="card" style="padding:0;overflow:hidden;border-radius:10px;">
        <div class="table-wrap">
            <table class="table">
                <thead><tr><th>#</th><th>نام</th><th>نوع</th><th>ترافیک</th><th>IP</th><th>وضعیت</th><th>عملیات</th></tr></thead>
                <tbody id="linksTbody"></tbody>
            </table>
        </div>
        <div class="inbound-cards" id="inboundCards"></div>
        <div class="empty" id="emptyState" style="display:none;text-align:center;padding:30px;color:var(--text3);">
            <div style="font-size:32px;opacity:0.2;">📭</div>
            <div>هیچ اینباندی یافت نشد</div>
        </div>
    </div>
</section>

<!-- ===== ADDRESSES ===== -->
<section class="page" id="page-addresses">
    <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <div>
            <div style="font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace;">🌐 CLEAN IP</div>
            <div style="font-size:10px;color:var(--text3);">مدیریت آی‌پی‌های تمیز</div>
        </div>
        <button class="btn btn-primary" onclick="showAddAddressModal()">➕ افزودن</button>
    </div>
    <div class="card">
        <div class="card-header"><div class="card-title">📋 لیست آی‌پی‌ها</div></div>
        <div id="addressList"></div>
    </div>
</section>

<!-- ===== DOMAIN ===== -->
<section class="page" id="page-domain">
    <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <div>
            <div style="font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace;">🌍 DOMAIN</div>
            <div style="font-size:10px;color:var(--text3);">مدیریت دامنه</div>
        </div>
        <button class="btn btn-secondary" onclick="checkDomain()">🔍 بررسی</button>
    </div>
    <div class="card" style="max-width:440px;">
        <div style="padding:10px;background:var(--surface2);border-radius:8px;border:1px solid var(--border);margin-bottom:10px;">
            <div style="font-size:8px;color:var(--text3);text-transform:uppercase;letter-spacing:0.05em;">دامنه فعلی</div>
            <div id="currentDomainDisplay" style="font-size:13px;font-weight:600;font-family:monospace;color:var(--text);">--</div>
            <div id="domainStatus" style="font-size:9px;color:var(--text2);margin-top:2px;"></div>
        </div>
        <div class="form-group">
            <label class="form-label">دامنه سفارشی</label>
            <div style="display:flex;gap:6px;">
                <input class="form-input" id="customDomainInput" placeholder="example.com" style="flex:1;font-family:monospace;">
                <button class="btn btn-primary" onclick="saveDomain()">💾 ذخیره</button>
            </div>
        </div>
        <button class="btn btn-danger btn-sm" onclick="clearDomain()" style="width:100%;margin-top:4px;">🗑️ حذف دامنه سفارشی</button>
    </div>
</section>

<!-- ===== SECURITY ===== -->
<section class="page" id="page-security">
    <div class="page-header" style="margin-bottom:10px;">
        <div>
            <div style="font-size:18px;font-weight:900;background:linear-gradient(135deg,var(--primary),#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-family:'Orbitron',monospace;">🔒 SECURITY</div>
            <div style="font-size:10px;color:var(--text3);">تغییر رمز عبور</div>
        </div>
    </div>
    <div class="card" style="max-width:360px;">
        <div class="card-header"><div class="card-title">🔑 تغییر رمز</div></div>
        <div class="form-group"><label class="form-label">رمز فعلی</label><input class="form-input" type="password" id="currentPassword" placeholder="رمز فعلی"></div>
        <div class="form-group"><label class="form-label">رمز جدید</label><input class="form-input" type="password" id="newPassword" placeholder="حداقل ۴ کاراکتر"></div>
        <button class="btn btn-primary" onclick="changePassword()" style="width:100%;justify-content:center;padding:8px;">🔄 تغییر رمز</button>
    </div>
</section>

</main>

<!-- ===== MODALS ===== -->
<div class="modal-overlay" id="addModal" onclick="if(event.target===this)this.classList.remove('show')">
    <div class="modal">
        <button class="modal-close" onclick="document.getElementById('addModal').classList.remove('show')">✕</button>
        <div class="modal-title">➕ افزودن اینباند</div>
        <div class="form-group"><label class="form-label">نام</label><input class="form-input" id="newName" placeholder="مثال: کاربر ۱"></div>
        <div class="form-row">
            <div class="form-group"><label class="form-label">حجم</label><input class="form-input" id="newLimit" type="number" min="0" step="0.1" placeholder="۰"></div>
            <div class="form-group" style="min-width:70px;"><label class="form-label">واحد</label><select class="form-select" id="newUnit"><option value="GB">GB</option><option value="MB">MB</option></select></div>
        </div>
        <div class="form-group"><label class="form-label">انقضا (روز)</label><input class="form-input" id="newExpiry" type="number" min="0" step="1" placeholder="۰"></div>
        <div class="form-group"><label class="form-label">حداکثر اتصال</label><input class="form-input" id="newMaxConn" type="number" min="0" step="1" placeholder="۰"></div>
        <button class="btn btn-primary" onclick="createLink()" style="width:100%;margin-top:6px;justify-content:center;padding:8px;">🚀 ایجاد</button>
    </div>
</div>

<div class="modal-overlay" id="addressModal" onclick="if(event.target===this)this.classList.remove('show')">
    <div class="modal">
        <button class="modal-close" onclick="document.getElementById('addressModal').classList.remove('show')">✕</button>
        <div class="modal-title">🌐 افزودن آی‌پی</div>
        <div class="form-group"><label class="form-label">آی‌پی یا دامنه (هر خط یکی)</label><textarea class="form-input" id="newAddressInput" rows="3" placeholder="8.8.8.8&#10;1.1.1.1" style="resize:vertical;font-family:monospace;font-size:11px;"></textarea></div>
        <button class="btn btn-primary" onclick="addAddresses()" style="width:100%;margin-top:4px;justify-content:center;padding:8px;">➕ افزودن</button>
    </div>
</div>

<!-- ===== JAVASCRIPT ===== -->
<script>
// ============================================================
// 🌈 THEME MANAGEMENT
// ============================================================
let currentTheme = localStorage.getItem('vroom_theme') || 'space';

const themes = {
    space: { background: 'radial-gradient(ellipse at bottom, #0d1b2a 0%, #000000 100%)' },
    ocean: { background: 'linear-gradient(135deg, #1a2980 0%, #26d0ce 100%)' },
    sunset: { background: 'linear-gradient(135deg, #f12711 0%, #f5af19 100%)' },
    forest: { background: 'linear-gradient(135deg, #134e5e 0%, #71b280 100%)' },
    neon: { background: 'linear-gradient(135deg, #1d1d2e 0%, #ff00cc 100%)' },
    rose: { background: 'linear-gradient(135deg, #ff6b6b 0%, #ffd93d 100%)' },
    ice: { background: 'linear-gradient(135deg, #a8e6cf 0%, #dcedc1 100%)' },
    dark: { background: '#0a0a12' },
};

function applyTheme(name) {
    currentTheme = name;
    localStorage.setItem('vroom_theme', name);
    document.body.style.background = themes[name].background;
    document.querySelectorAll('.theme-btn').forEach(b => b.classList.toggle('active', b.dataset.theme === name));
}

function toggleTheme() {
    const names = Object.keys(themes);
    const idx = names.indexOf(currentTheme);
    applyTheme(names[(idx + 1) % names.length]);
}

document.querySelectorAll('.theme-btn').forEach(btn => {
    btn.addEventListener('click', () => applyTheme(btn.dataset.theme));
});
applyTheme(currentTheme);

// ============================================================
// 📋 NAVIGATION
// ============================================================
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', () => {
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        item.classList.add('active');
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById('page-' + item.dataset.page).classList.add('active');
        document.getElementById('sidebar').classList.remove('open');
    });
});

// ============================================================
// 🔔 TOAST
// ============================================================
function toast(msg, err = false) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast' + (err ? ' error' : '') + ' show';
    setTimeout(() => t.classList.remove('show'), 3000);
}

// ============================================================
// 📊 LOAD STATS
// ============================================================
async function loadStats() {
    try {
        const resp = await fetch('/stats');
        const data = await resp.json();
        document.getElementById('sTraffic').innerHTML = data.total_traffic_mb + '<span class="stat-unit">MB</span>';
        document.getElementById('sLinks').textContent = data.links_count;
        document.getElementById('sUptime').textContent = data.uptime;
        document.getElementById('sDomain').textContent = data.domain;
        document.getElementById('sDisk').textContent = data.disk_percent + '%';
        document.getElementById('sDiskDetail').textContent = data.disk_used + ' / ' + data.disk_total + ' GB';
        document.getElementById('sRam').textContent = data.memory_percent + '%';
        document.getElementById('sRamDetail').textContent = (data.memory_percent * 0.08).toFixed(2) + ' / 8 GB';
        document.getElementById('sCpu').textContent = data.cpu_percent + '%';
        document.getElementById('sConnections').textContent = data.active_connections;
        document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString('fa-IR');
        updateChart(data);
    } catch(e) { console.error(e); }
}

// ============================================================
// 📈 CHART
// ============================================================
let chart = null;

function updateChart(data) {
    const ctx = document.getElementById('trafficChart');
    if (!ctx) return;
    if (chart) { chart.destroy(); }
    const hours = Object.keys(data.hourly_traffic || {}).slice(-12);
    const values = hours.map(h => Math.round(data.hourly_traffic[h] / 1048576));
    chart = new Chart(ctx, {
        type: 'bar',
        data: { labels: hours, datasets: [{ label: 'MB', data: values, backgroundColor: 'rgba(124,92,252,0.6)', borderColor: '#7c5cfc', borderWidth: 2, borderRadius: 6 }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false }, ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 7 } } }, y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 7 } }, beginAtZero: true } } }
    });
}

// ============================================================
// 📡 LINKS MANAGEMENT
// ============================================================
let allLinks = [];
let currentFilter = 'all';

async function loadLinks() {
    try {
        const resp = await fetch('/api/links');
        const data = await resp.json();
        allLinks = data.links || [];
        renderLinks();
    } catch(e) { console.error(e); }
}

function renderLinks() {
    const search = document.getElementById('searchInput').value.toLowerCase();
    let filtered = allLinks;
    if (currentFilter === 'active') filtered = filtered.filter(l => l.active);
    if (currentFilter === 'disabled') filtered = filtered.filter(l => !l.active);
    if (search) filtered = filtered.filter(l => l.label.toLowerCase().includes(search) || l.uuid.toLowerCase().includes(search));
    
    const tbody = document.getElementById('linksTbody');
    const cards = document.getElementById('inboundCards');
    const empty = document.getElementById('emptyState');
    
    if (!filtered.length) {
        tbody.innerHTML = '';
        cards.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';
    
    tbody.innerHTML = filtered.map((l, i) => {
        const pct = l.limit_bytes > 0 ? Math.min(100, (l.used_bytes / l.limit_bytes) * 100) : 0;
        const color = pct > 90 ? 'var(--red)' : pct > 70 ? 'var(--yellow)' : 'var(--primary)';
        const used = (l.used_bytes / 1073741824).toFixed(2);
        const limit = l.limit_bytes > 0 ? (l.limit_bytes / 1073741824).toFixed(2) : '∞';
        return `<tr>
            <td style="color:var(--text3);font-size:9px;">${i+1}</td>
            <td style="font-weight:600;font-size:11px;">${l.label}</td>
            <td><span class="tag tag-vless">VLESS</span></td>
            <td><div class="usage-pill"><span class="used">${used}GB</span><div class="bar"><div class="fill" style="width:${pct}%;background:${color};"></div></div><span class="limit">${limit}GB</span></div></td>
            <td style="font-size:10px;font-weight:600;color:var(--text2);">${l.current_connections}/${l.max_connections||'∞'}</td>
            <td><span class="tag ${l.active?'tag-active':'tag-disabled'}">${l.active?'فعال':'غیرفعال'}</span></td>
            <td>
                <button class="toggle ${l.active?'on':''}" onclick="toggleLink('${l.uuid}')"></button>
                <button class="btn btn-danger btn-sm" onclick="deleteLink('${l.uuid}')">🗑</button>
                <button class="btn btn-secondary btn-sm" onclick="copyLink('${l.vless_link}')">📋</button>
            </td>
        </tr>`;
    }).join('');
    
    cards.innerHTML = filtered.map((l, i) => {
        const pct = l.limit_bytes > 0 ? Math.min(100, (l.used_bytes / l.limit_bytes) * 100) : 0;
        const used = (l.used_bytes / 1073741824).toFixed(2);
        const limit = l.limit_bytes > 0 ? (l.limit_bytes / 1073741824).toFixed(2) : '∞';
        return `<div class="inbound-card">
            <div class="inbound-card-header">
                <span class="inbound-card-name">${l.label}</span>
                <button class="toggle ${l.active?'on':''}" onclick="toggleLink('${l.uuid}')"></button>
            </div>
            <div class="usage-pill"><span class="used">${used}GB</span><div class="bar"><div class="fill" style="width:${pct}%;background:var(--primary);"></div></div><span class="limit">${limit}GB</span></div>
            <div style="display:flex;gap:4px;">
                <button class="btn btn-danger btn-sm" onclick="deleteLink('${l.uuid}')">🗑</button>
                <button class="btn btn-secondary btn-sm" onclick="copyLink('${l.vless_link}')">📋</button>
            </div>
        </div>`;
    }).join('');
}

// Filter chips
document.querySelectorAll('.chip').forEach(chip => {
    chip.addEventListener('click', function() {
        document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
        this.classList.add('active');
        currentFilter = this.dataset.filter;
        renderLinks();
    });
});

// Search
document.getElementById('searchInput').addEventListener('input', renderLinks);

// ============================================================
// 🔄 LINK OPERATIONS
// ============================================================
async function toggleLink(uid) {
    const link = allLinks.find(l => l.uuid === uid);
    if (!link) return;
    try {
        await fetch('/api/links/' + uid, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ active: !link.active })
        });
        link.active = !link.active;
        renderLinks();
        loadStats();
        toast('✅ وضعیت تغییر کرد');
    } catch(e) { toast('❌ خطا', true); }
}

async function deleteLink(uid) {
    if (!confirm('❓ حذف اینباند؟')) return;
    try {
        await fetch('/api/links/' + uid, { method: 'DELETE' });
        toast('✅ حذف شد');
        loadLinks();
        loadStats();
    } catch(e) { toast('❌ خطا', true); }
}

function copyLink(text) {
    navigator.clipboard.writeText(text).then(() => toast('📋 کپی شد'));
}

// ============================================================
// ➕ CREATE LINK
// ============================================================
function showAddModal() {
    document.getElementById('addModal').classList.add('show');
}

async function createLink() {
    const name = document.getElementById('newName').value.trim() || 'New Link';
    const limit = parseFloat(document.getElementById('newLimit').value) || 0;
    const unit = document.getElementById('newUnit').value;
    const expiry = parseInt(document.getElementById('newExpiry').value) || 0;
    const maxConn = parseInt(document.getElementById('newMaxConn').value) || 0;
    
    if (!/^[a-zA-Z0-9\-_. ]+$/.test(name)) {
        toast('❌ نام نامعتبر است', true);
        return;
    }
    
    try {
        const resp = await fetch('/api/links', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ label: name, limit_value: limit, limit_unit: unit, expiry_days: expiry, max_connections: maxConn })
        });
        if (!resp.ok) throw new Error();
        toast('✅ اینباند ساخته شد');
        document.getElementById('addModal').classList.remove('show');
        document.getElementById('newName').value = '';
        document.getElementById('newLimit').value = '';
        document.getElementById('newExpiry').value = '';
        document.getElementById('newMaxConn').value = '';
        loadLinks();
        loadStats();
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// 🚀 QUICK CREATE
// ============================================================
async function quickCreate(limit, unit) {
    const names = ['User', 'Test', 'Link', 'VPN', 'Server', 'Node', 'Client', 'Main'];
    const name = names[Math.floor(Math.random() * names.length)] + '-' + Math.floor(Math.random() * 999);
    try {
        await fetch('/api/links', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ label: name, limit_value: limit, limit_unit: unit })
        });
        toast('✅ ' + name + ' ساخته شد');
        loadLinks();
        loadStats();
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// 🌐 ADDRESSES
// ============================================================
async function loadAddresses() {
    try {
        const resp = await fetch('/api/addresses');
        const data = await resp.json();
        const list = document.getElementById('addressList');
        if (!data.addresses || !data.addresses.length) {
            list.innerHTML = '<div style="color:var(--text3);font-size:11px;padding:8px;text-align:center;">هیچ آی‌پی اضافه نشده</div>';
            return;
        }
        list.innerHTML = data.addresses.map((a, i) => `
            <div style="display:flex;align-items:center;justify-content:space-between;padding:6px 10px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;margin-bottom:4px;">
                <span style="font-size:11px;font-weight:600;font-family:monospace;">${a}</span>
                <button class="btn btn-danger btn-sm" onclick="deleteAddress(${i})">🗑</button>
            </div>
        `).join('');
    } catch(e) { console.error(e); }
}

function showAddAddressModal() {
    document.getElementById('addressModal').classList.add('show');
}

async function addAddresses() {
    const text = document.getElementById('newAddressInput').value.trim();
    if (!text) { toast('❌ وارد کنید', true); return; }
    const lines = text.split('\n').map(l => l.trim()).filter(l => l);
    let added = 0;
    for (const addr of lines) {
        try {
            const resp = await fetch('/api/addresses', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ address: addr })
            });
            if (resp.ok) added++;
        } catch(e) {}
    }
    if (added) {
        toast('✅ ' + added + ' آدرس افزوده شد');
        document.getElementById('addressModal').classList.remove('show');
        document.getElementById('newAddressInput').value = '';
        loadAddresses();
    } else {
        toast('❌ خطا', true);
    }
}

async function deleteAddress(index) {
    if (!confirm('❓ حذف؟')) return;
    try {
        await fetch('/api/addresses/' + index, { method: 'DELETE' });
        toast('✅ حذف شد');
        loadAddresses();
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// 🌍 DOMAIN
// ============================================================
async function loadDomain() {
    try {
        const resp = await fetch('/api/domain');
        const data = await resp.json();
        const domain = data.domain || getDefaultDomain();
        document.getElementById('currentDomainDisplay').textContent = domain;
        checkDomain();
    } catch(e) { console.error(e); }
}

function getDefaultDomain() {
    return window.location.hostname || 'localhost';
}

async function checkDomain() {
    try {
        const resp = await fetch('/api/domain/status');
        const data = await resp.json();
        const status = data.current;
        const el = document.getElementById('domainStatus');
        if (status.reachable) {
            el.innerHTML = '✅ آنلاین | IP: ' + (status.ip || '--') + ' | SSL: ' + (status.ssl_valid ? '✅' : '⚠️');
            el.style.color = 'var(--green)';
        } else {
            el.textContent = '❌ آفلاین';
            el.style.color = 'var(--red)';
        }
    } catch(e) { console.error(e); }
}

async function saveDomain() {
    const domain = document.getElementById('customDomainInput').value.trim();
    if (!domain) { toast('❌ دامنه وارد کنید', true); return; }
    try {
        const resp = await fetch('/api/domain', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain })
        });
        if (!resp.ok) throw new Error();
        toast('✅ دامنه ذخیره شد');
        document.getElementById('customDomainInput').value = '';
        loadDomain();
    } catch(e) { toast('❌ خطا', true); }
}

async function clearDomain() {
    if (!confirm('❓ حذف دامنه سفارشی؟')) return;
    try {
        await fetch('/api/domain', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain: '' })
        });
        toast('✅ دامنه حذف شد');
        loadDomain();
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// 🔒 SECURITY
// ============================================================
async function changePassword() {
    const current = document.getElementById('currentPassword').value;
    const newPass = document.getElementById('newPassword').value;
    if (!current || !newPass) { toast('❌ همه فیلدها را پر کنید', true); return; }
    if (newPass.length < 4) { toast('❌ رمز جدید حداقل ۴ کاراکتر', true); return; }
    try {
        const resp = await fetch('/api/change-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ current_password: current, new_password: newPass })
        });
        if (!resp.ok) throw new Error();
        toast('✅ رمز تغییر کرد');
        document.getElementById('currentPassword').value = '';
        document.getElementById('newPassword').value = '';
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// 🚪 LOGOUT
// ============================================================
async function logout() {
    await fetch('/api/logout', { method: 'POST' });
    location.href = '/login';
}

// ============================================================
// 🔄 AUTO REFRESH
// ============================================================
loadStats();
loadLinks();
loadAddresses();
loadDomain();
setInterval(() => { loadStats(); }, 5000);
setInterval(() => { loadLinks(); }, 30000);

// ============================================================
// ⌨️ KEYBOARD SHORTCUTS
// ============================================================
document.addEventListener('keydown', (e) => {
    if (e.ctrlKey && e.key === 'r') { e.preventDefault(); loadStats(); loadLinks(); }
});
</script>
</body>
</html>'''

# ============================================================
# 📄 LOGIN & DASHBOARD ROUTES
# ============================================================
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

# ============================================================
# 🚀 WEBSOCKET PROXY
# ============================================================
RELAY_BUF = 2 * 1024 * 1024

async def parse_vless_header(first_chunk: bytes):
    if len(first_chunk) < 24:
        raise ValueError("chunk too small")
    pos = 0
    pos += 1  # version
    pos += 16  # uuid
    addon_len = first_chunk[pos]
    pos += 1
    pos += addon_len
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

@app.websocket("/ws/{uuid}")
async def websocket_tunnel(websocket: WebSocket, uuid: str):
    await websocket.accept()
    writer = None
    conn_id = None
    client_ip = get_client_ip(websocket)
    
    try:
        async with LINKS_LOCK:
            link_data = LINKS.get(uuid)
            if link_data is None or not link_data["active"] or is_expired(link_data):
                await websocket.close(code=1008, reason="link not found or inactive/expired")
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
        
        # Relay
        async def ws_to_tcp():
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
                    await add_usage(uuid, size)
                    writer.write(data)
                    await writer.drain()
            except:
                pass
        
        async def tcp_to_ws():
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
                    await add_usage(uuid, size)
                    await websocket.send_bytes((b"\x00\x00" + data) if first else data)
                    first = False
            except:
                pass
        
        task_up = asyncio.create_task(ws_to_tcp())
        task_down = asyncio.create_task(tcp_to_ws())
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
            except:
                pass
        if conn_id:
            info = connections.pop(conn_id, None)
            connection_sockets.pop(conn_id, None)
            if info:
                uid2 = info.get("uuid")
                ip = info.get("ip")
                if uid2 and ip:
                    has_other = any(c.get("uuid") == uid2 and c.get("ip") == ip for c in connections.values())
                    if not has_other:
                        link_ip_map.get(uid2, set()).discard(ip)

# ============================================================
# 🚀 MAIN
# ============================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["port"])
