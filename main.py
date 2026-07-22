# ============================================================
# 🚀 VROOM GATEWAY - COMPLETE FINAL EDITION
# ============================================================
# ✅ Fully Working Dashboard
# ✅ All Buttons Functional
# ✅ Clean IP Management
# ✅ Domain Management
# ✅ Subscription Page
# ✅ WebSocket Proxy
# ✅ Auto Backup
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

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx

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
# 🌐 VLESS LINK GENERATION
# ============================================================
def get_domain() -> str:
    if CUSTOM_DOMAIN:
        return CUSTOM_DOMAIN
    env_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RENDER_EXTERNAL_URL")
    if env_domain:
        return env_domain.replace("https://", "").replace("http://", "")
    return "localhost"

def generate_vless_link(uuid: str, remark: str = "VROOM", address: str = None) -> str:
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

# ============================================================
# 🚀 KEEP ALIVE & AUTO BACKUP
# ============================================================
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

async def auto_backup():
    while True:
        await asyncio.sleep(21600)
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

@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()

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
# 📡 LINKS API - FULL CRUD
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
async def update_link(uid: str, request: Request, _=Depends(require_auth)):
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
            return HTMLResponse(content="<h2 style='color:#fff;text-align:center;margin-top:50px;'>❌ Link not found</h2>", status_code=404)
    
    if not link["active"] or is_expired(link):
        return HTMLResponse(content="<h2 style='color:#fff;text-align:center;margin-top:50px;'>⛔ Link inactive or expired</h2>", status_code=403)
    
    async with CUSTOM_ADDRESSES_LOCK:
        addresses = list(CUSTOM_ADDRESSES)
    
    main_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    all_links = [main_link]
    for i, addr in enumerate(addresses):
        if addr and addr != "www.speedtest.net":
            all_links.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))
    
    sub_content = "\n".join(all_links)
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
    
    status_text = "فعال" if link['active'] and not is_expired(link) else "غیرفعال"
    
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🚀 VROOM - {link['label']}</title>
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;700;900&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Vazirmatn', sans-serif;
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
        h1 {{ font-size: 24px; font-weight: 900; background: linear-gradient(135deg, #7c5cfc, #a78bfa); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 15px 0; }}
        .info-item {{ background: rgba(255,255,255,0.03); padding: 10px 12px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.03); }}
        .info-item.full {{ grid-column: span 2; }}
        .label {{ font-size: 10px; opacity: 0.4; display: block; font-weight: 700; }}
        .value {{ font-size: 14px; font-weight: 700; }}
        .status-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-left: 6px; background: #34d399; }}
        .progress-section {{ margin: 12px 0; }}
        .progress-bar {{ width: 100%; height: 4px; background: rgba(255,255,255,0.05); border-radius: 10px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: linear-gradient(90deg, #7c5cfc, #a78bfa); border-radius: 10px; width: {percent}%; }}
        .btn-group {{ display: flex; gap: 6px; flex-wrap: wrap; margin-top: 12px; }}
        .btn {{ flex: 1; min-width: 60px; padding: 8px 12px; border-radius: 10px; font-weight: 700; font-size: 12px; border: none; cursor: pointer; transition: all 0.3s; font-family: 'Vazirmatn', sans-serif; }}
        .btn-primary {{ background: linear-gradient(135deg, #7c5cfc, #a78bfa); color: #fff; }}
        .btn-primary:hover {{ transform: translateY(-2px); }}
        .btn-success {{ background: linear-gradient(135deg, #11998e, #38ef7d); color: #fff; }}
        .btn-success:hover {{ transform: translateY(-2px); }}
        .btn-secondary {{ background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.6); border: 1px solid rgba(255,255,255,0.06); }}
        .btn-secondary:hover {{ background: rgba(255,255,255,0.1); }}
        .config-box {{ background: rgba(0,0,0,0.3); padding: 10px 12px; border-radius: 10px; font-size: 11px; font-family: monospace; word-break: break-all; margin: 10px 0; max-height: 80px; overflow-y: auto; color: rgba(255,255,255,0.5); line-height: 1.6; text-align: left; direction: ltr; }}
        .qr-section {{ text-align: center; margin: 12px 0; }}
        .qr-container {{ display: inline-block; background: rgba(255,255,255,0.95); padding: 10px; border-radius: 14px; }}
        .qr-container img {{ width: 120px; height: 120px; border-radius: 8px; display: block; }}
        .footer {{ text-align: center; margin-top: 15px; font-size: 11px; opacity: 0.2; }}
        @media (max-width: 500px) {{ .container {{ padding: 20px 15px; }} .info-grid {{ grid-template-columns: 1fr; }} .info-item.full {{ grid-column: span 1; }} }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header"><h1>🚀 {link['label']}</h1></div>
        <div class="info-grid">
            <div class="info-item full"><span class="label">وضعیت</span><span class="value"><span class="status-dot"></span> {status_text}</span></div>
            <div class="info-item"><span class="label">📊 مصرف</span><span class="value">{used_gb} GB</span></div>
            <div class="info-item"><span class="label">📦 حجم کل</span><span class="value">{limit_gb if limit_gb > 0 else '∞'} GB</span></div>
            <div class="info-item"><span class="label">⏳ انقضا</span><span class="value">{exp if exp else 'نامحدود'}</span></div>
            <div class="info-item"><span class="label">📅 روز باقی‌مانده</span><span class="value">{days_left_text}</span></div>
        </div>
        <div class="progress-section"><div class="progress-bar"><div class="progress-fill"></div></div></div>
        <div class="qr-section"><div class="qr-container"><img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={base64.b64encode(main_link.encode()).decode()}" alt="QR"></div></div>
        <div class="config-box">{main_link}</div>
        <div class="btn-group">
            <button class="btn btn-primary" onclick="copyText('{main_link}','✅ کانفیگ اصلی کپی شد!')">📋 کپی</button>
            <button class="btn btn-success" onclick="copyText('{sub_content}','✅ همه کانفیگ‌ها کپی شدند!')">📥 کپی همه</button>
            <button class="btn btn-secondary" onclick="copyText('{sub_url}','✅ لینک ساب کپی شد!')">🔗 ساب</button>
        </div>
        <div class="footer">✦ VROOM GATEWAY v3.0 ✦</div>
    </div>
    <script>
        function copyText(text, msg) {{
            navigator.clipboard.writeText(text).then(() => alert(msg)).catch(() => {{
                const ta = document.createElement('textarea');
                ta.value = text;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                document.body.removeChild(ta);
                alert(msg);
            }});
        }}
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
    
    main_link = generate_vless_link(uid, remark=f"VROOM-{link['label']}")
    all_links = [main_link]
    for i, addr in enumerate(addresses):
        if addr and addr != "www.speedtest.net":
            all_links.append(generate_vless_link(uid, remark=f"VROOM-{link['label']}-{i+1}", address=addr))
    
    content = "\n".join(all_links)
    encoded = base64.b64encode(content.encode()).decode()
    return Response(content=encoded, media_type="text/plain")

# ============================================================
# 🚪 LOGIN PAGE
# ============================================================
LOGIN_HTML = '''<!DOCTYPE html>
<html lang="fa">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🚀 VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;700;900&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:'Vazirmatn',sans-serif;background:radial-gradient(ellipse at bottom,#0d1b2a 0%,#000 100%);color:#fff;direction:rtl}
.login-box{background:rgba(255,255,255,0.04);backdrop-filter:blur(30px);padding:40px;border-radius:30px;border:1px solid rgba(255,255,255,0.06);width:100%;max-width:360px;text-align:center;box-shadow:0 40px 80px rgba(0,0,0,0.5)}
h1{font-size:28px;font-weight:900;background:linear-gradient(135deg,#7c5cfc,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:30px}
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
<input type="password" id="password" placeholder="رمز عبور..." autofocus>
<button type="submit">➜ ورود</button>
<div class="error" id="error">رمز عبور اشتباه است</div>
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
# 📊 DASHBOARD - FULLY WORKING
# ============================================================
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🚀 VROOM</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;700;900&display=swap" rel="stylesheet">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'Vazirmatn', sans-serif;
    background: #0a0a12;
    color: #fff;
    min-height: 100vh;
    padding: 15px;
}
.container { max-width: 900px; margin: 0 auto; }

/* ===== HEADER ===== */
.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 15px 20px;
    background: rgba(255,255,255,0.04);
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.05);
    margin-bottom: 15px;
}
.header h1 {
    font-size: 22px;
    background: linear-gradient(135deg, #7c5cfc, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 900;
}
.header-actions { display: flex; gap: 8px; }
.btn {
    padding: 8px 16px;
    border-radius: 10px;
    border: none;
    font-family: 'Vazirmatn', sans-serif;
    font-weight: 700;
    font-size: 12px;
    cursor: pointer;
    transition: all 0.3s;
}
.btn-primary { background: linear-gradient(135deg, #7c5cfc, #a78bfa); color: #fff; }
.btn-primary:hover { transform: translateY(-2px); box-shadow: 0 8px 30px rgba(124,92,252,0.3); }
.btn-success { background: linear-gradient(135deg, #11998e, #38ef7d); color: #fff; }
.btn-success:hover { transform: translateY(-2px); }
.btn-danger { background: rgba(248,113,113,0.15); color: #f87171; border: 1px solid rgba(248,113,113,0.15); }
.btn-danger:hover { background: rgba(248,113,113,0.25); }
.btn-secondary { background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.6); border: 1px solid rgba(255,255,255,0.06); }
.btn-secondary:hover { background: rgba(255,255,255,0.1); }
.btn-sm { padding: 4px 10px; font-size: 10px; }

/* ===== STATS ===== */
.stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin-bottom: 15px;
}
.stat-card {
    background: rgba(255,255,255,0.04);
    padding: 14px 16px;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.05);
    text-align: center;
}
.stat-card .icon { font-size: 20px; display: block; }
.stat-card .label { font-size: 10px; color: rgba(255,255,255,0.3); margin-top: 4px; }
.stat-card .value { font-size: 18px; font-weight: 900; margin-top: 2px; }

/* ===== CARD ===== */
.card {
    background: rgba(255,255,255,0.04);
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.05);
    padding: 16px 18px;
    margin-bottom: 12px;
}
.card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}
.card-title { font-size: 14px; font-weight: 700; }

/* ===== TABLE ===== */
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th {
    text-align: right;
    font-size: 10px;
    color: rgba(255,255,255,0.3);
    padding: 8px 6px;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    font-weight: 700;
    text-transform: uppercase;
}
td {
    padding: 8px 6px;
    border-bottom: 1px solid rgba(255,255,255,0.03);
    font-size: 12px;
    vertical-align: middle;
}
tr:hover td { background: rgba(124,92,252,0.05); }

/* ===== TAGS ===== */
.tag {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 9px;
    font-weight: 700;
}
.tag-active { background: rgba(52,211,153,0.15); color: #34d399; }
.tag-disabled { background: rgba(248,113,113,0.15); color: #f87171; }
.tag-vless { background: rgba(124,92,252,0.15); color: #a78bfa; }

/* ===== TOGGLE ===== */
.toggle {
    width: 32px;
    height: 18px;
    border-radius: 10px;
    background: rgba(255,255,255,0.1);
    position: relative;
    cursor: pointer;
    border: none;
    transition: 0.3s;
}
.toggle.on { background: #34d399; }
.toggle::after {
    content: '';
    position: absolute;
    width: 12px;
    height: 12px;
    border-radius: 50%;
    background: #fff;
    top: 3px;
    right: 3px;
    transition: 0.3s;
}
.toggle.on::after { right: 17px; }

/* ===== USAGE BAR ===== */
.usage-bar {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 10px;
}
.usage-bar .track {
    flex: 1;
    height: 4px;
    background: rgba(255,255,255,0.05);
    border-radius: 4px;
    overflow: hidden;
    min-width: 40px;
}
.usage-bar .fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.5s;
}
.usage-bar .used { font-weight: 700; color: #fff; }
.usage-bar .limit { color: rgba(255,255,255,0.3); }

/* ===== MODAL ===== */
.modal-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.7);
    backdrop-filter: blur(6px);
    z-index: 999;
    align-items: center;
    justify-content: center;
}
.modal-overlay.show { display: flex; }
.modal {
    background: #1a1a2e;
    padding: 24px 28px;
    border-radius: 20px;
    max-width: 400px;
    width: 90%;
    border: 1px solid rgba(255,255,255,0.06);
}
.modal-title { font-size: 18px; font-weight: 700; margin-bottom: 16px; }
.modal-close {
    float: left;
    background: none;
    border: none;
    color: rgba(255,255,255,0.3);
    font-size: 20px;
    cursor: pointer;
}
.form-group { margin-bottom: 12px; }
.form-group label {
    display: block;
    font-size: 10px;
    color: rgba(255,255,255,0.4);
    margin-bottom: 4px;
    font-weight: 700;
    text-transform: uppercase;
}
.form-group input, .form-group select {
    width: 100%;
    padding: 8px 12px;
    border-radius: 8px;
    border: 1px solid rgba(255,255,255,0.06);
    background: rgba(255,255,255,0.03);
    color: #fff;
    font-size: 13px;
    font-family: 'Vazirmatn', sans-serif;
    outline: none;
}
.form-group input:focus, .form-group select:focus {
    border-color: #7c5cfc;
    box-shadow: 0 0 20px rgba(124,92,252,0.1);
}
.form-row { display: flex; gap: 8px; }
.form-row .form-group { flex: 1; }

/* ===== ADDRESS LIST ===== */
.address-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 0;
    border-bottom: 1px solid rgba(255,255,255,0.03);
}
.address-item:last-child { border-bottom: none; }
.address-item .addr { font-size: 13px; font-family: monospace; }
.address-item .actions { display: flex; gap: 4px; }

/* ===== TOAST ===== */
.toast {
    position: fixed;
    bottom: 20px;
    left: 50%;
    transform: translateX(-50%) translateY(80px);
    padding: 10px 24px;
    border-radius: 12px;
    background: #1a1a2e;
    color: #fff;
    font-size: 13px;
    font-weight: 500;
    border: 1px solid rgba(255,255,255,0.05);
    opacity: 0;
    transition: all 0.4s;
    z-index: 9999;
}
.toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
.toast.error { border-color: rgba(248,113,113,0.3); color: #f87171; }

/* ===== DOMAIN ===== */
.domain-display {
    padding: 10px 14px;
    background: rgba(255,255,255,0.03);
    border-radius: 10px;
    border: 1px solid rgba(255,255,255,0.05);
    margin-bottom: 10px;
}
.domain-display .label { font-size: 10px; color: rgba(255,255,255,0.3); }
.domain-display .value { font-size: 14px; font-weight: 700; font-family: monospace; margin-top: 2px; }
.domain-status { font-size: 11px; margin-top: 4px; }
.domain-status.online { color: #34d399; }
.domain-status.offline { color: #f87171; }

/* ===== RESPONSIVE ===== */
@media (max-width: 600px) {
    .stats { grid-template-columns: 1fr 1fr; }
    .header { flex-direction: column; gap: 8px; }
    .header-actions { width: 100%; justify-content: center; flex-wrap: wrap; }
}
</style>
</head>
<body>

<div class="container">
    <!-- HEADER -->
    <div class="header">
        <h1>🚀 VROOM</h1>
        <div class="header-actions">
            <button class="btn btn-secondary" onclick="refreshData()">🔄 بروزرسانی</button>
            <button class="btn btn-danger" onclick="logout()">🚪 خروج</button>
        </div>
    </div>

    <!-- STATS -->
    <div class="stats" id="stats">
        <div class="stat-card"><span class="icon">📊</span><div class="label">ترافیک کل</div><div class="value" id="sTraffic">--</div></div>
        <div class="stat-card"><span class="icon">📡</span><div class="label">اینباندها</div><div class="value" id="sLinks">--</div></div>
        <div class="stat-card"><span class="icon">⏱️</span><div class="label">آپتایم</div><div class="value" id="sUptime" style="font-size:14px;">--</div></div>
        <div class="stat-card"><span class="icon">🌐</span><div class="label">دامنه</div><div class="value" id="sDomain" style="font-size:13px;font-weight:600;">--</div></div>
    </div>

    <!-- LINKS -->
    <div class="card">
        <div class="card-header">
            <span class="card-title">📡 اینباندها</span>
            <button class="btn btn-primary" onclick="showAddModal()">➕ افزودن</button>
        </div>
        <div id="linksContainer">
            <div style="text-align:center;padding:20px;color:rgba(255,255,255,0.2);">در حال بارگذاری...</div>
        </div>
    </div>

    <!-- ADDRESSES -->
    <div class="card">
        <div class="card-header">
            <span class="card-title">🌐 آی‌پی تمیز</span>
            <button class="btn btn-primary" onclick="showAddressModal()">➕ افزودن</button>
        </div>
        <div id="addressesContainer">
            <div style="text-align:center;padding:10px;color:rgba(255,255,255,0.2);">در حال بارگذاری...</div>
        </div>
    </div>

    <!-- DOMAIN -->
    <div class="card">
        <div class="card-header">
            <span class="card-title">🌍 دامنه</span>
            <button class="btn btn-secondary" onclick="checkDomain()">🔍 بررسی</button>
        </div>
        <div id="domainContainer">
            <div style="text-align:center;padding:10px;color:rgba(255,255,255,0.2);">در حال بارگذاری...</div>
        </div>
        <div style="margin-top:10px;display:flex;gap:6px;">
            <input id="domainInput" placeholder="example.com" style="flex:1;padding:8px 12px;border-radius:8px;border:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.03);color:#fff;font-family:inherit;outline:none;">
            <button class="btn btn-primary" onclick="saveDomain()">💾 ذخیره</button>
            <button class="btn btn-danger" onclick="clearDomain()">🗑️</button>
        </div>
    </div>
</div>

<!-- ===== MODAL: ADD LINK ===== -->
<div class="modal-overlay" id="addModal" onclick="if(event.target===this)closeModal('addModal')">
    <div class="modal">
        <button class="modal-close" onclick="closeModal('addModal')">✕</button>
        <div class="modal-title">➕ افزودن اینباند</div>
        <div class="form-group">
            <label>نام</label>
            <input id="linkName" placeholder="مثال: کاربر ۱">
        </div>
        <div class="form-row">
            <div class="form-group">
                <label>حجم</label>
                <input id="linkLimit" type="number" min="0" step="0.1" placeholder="۰">
            </div>
            <div class="form-group">
                <label>واحد</label>
                <select id="linkUnit"><option value="GB">GB</option><option value="MB">MB</option></select>
            </div>
        </div>
        <div class="form-group">
            <label>انقضا (روز)</label>
            <input id="linkExpiry" type="number" min="0" step="1" placeholder="۰ = نامحدود">
        </div>
        <div class="form-group">
            <label>حداکثر اتصال</label>
            <input id="linkMaxConn" type="number" min="0" step="1" placeholder="۰ = نامحدود">
        </div>
        <button class="btn btn-primary" onclick="createLink()" style="width:100%;margin-top:6px;padding:10px;">🚀 ایجاد</button>
    </div>
</div>

<!-- ===== MODAL: ADD ADDRESS ===== -->
<div class="modal-overlay" id="addressModal" onclick="if(event.target===this)closeModal('addressModal')">
    <div class="modal">
        <button class="modal-close" onclick="closeModal('addressModal')">✕</button>
        <div class="modal-title">🌐 افزودن آی‌پی</div>
        <div class="form-group">
            <label>آی‌پی یا دامنه (هر خط یکی)</label>
            <textarea id="addressInput" rows="3" placeholder="8.8.8.8&#10;1.1.1.1" style="width:100%;padding:8px 12px;border-radius:8px;border:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.03);color:#fff;font-family:monospace;font-size:12px;resize:vertical;outline:none;"></textarea>
        </div>
        <button class="btn btn-primary" onclick="addAddresses()" style="width:100%;margin-top:4px;padding:10px;">➕ افزودن</button>
    </div>
</div>

<!-- ===== TOAST ===== -->
<div class="toast" id="toast"></div>

<!-- ============================================================ -->
<!-- JAVASCRIPT -->
<!-- ============================================================ -->
<script>
// ============================================================
// TOAST
// ============================================================
function toast(msg, err = false) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.className = 'toast show' + (err ? ' error' : '');
    clearTimeout(t._timeout);
    t._timeout = setTimeout(() => t.classList.remove('show'), 3000);
}

// ============================================================
// MODAL
// ============================================================
function showAddModal() { document.getElementById('addModal').classList.add('show'); }
function showAddressModal() { document.getElementById('addressModal').classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }

// ============================================================
// LOGOUT
// ============================================================
async function logout() {
    await fetch('/api/logout', { method: 'POST' });
    location.href = '/login';
}

// ============================================================
// REFRESH
// ============================================================
function refreshData() {
    loadStats();
    loadLinks();
    loadAddresses();
    loadDomain();
    toast('🔄 بروزرسانی شد');
}

// ============================================================
// LOAD STATS
// ============================================================
async function loadStats() {
    try {
        const r = await fetch('/stats');
        const d = await r.json();
        document.getElementById('sTraffic').textContent = d.total_traffic_mb + ' MB';
        document.getElementById('sLinks').textContent = d.links_count;
        document.getElementById('sUptime').textContent = d.uptime;
        document.getElementById('sDomain').textContent = d.domain;
    } catch(e) { console.error(e); }
}

// ============================================================
// LOAD LINKS
// ============================================================
async function loadLinks() {
    try {
        const r = await fetch('/api/links');
        const d = await r.json();
        const links = d.links || [];
        const container = document.getElementById('linksContainer');
        
        if (!links.length) {
            container.innerHTML = '<div style="text-align:center;padding:20px;color:rgba(255,255,255,0.2);">📭 هیچ اینباندی وجود ندارد</div>';
            return;
        }
        
        let html = '<div class="table-wrap"><table><thead><tr><th>#</th><th>نام</th><th>نوع</th><th>ترافیک</th><th>IP</th><th>وضعیت</th><th>عملیات</th></tr></thead><tbody>';
        
        links.forEach((l, i) => {
            const pct = l.limit_bytes > 0 ? Math.min(100, (l.used_bytes / l.limit_bytes) * 100) : 0;
            const used = (l.used_bytes / 1073741824).toFixed(2);
            const limit = l.limit_bytes > 0 ? (l.limit_bytes / 1073741824).toFixed(2) : '∞';
            const color = pct > 90 ? '#f87171' : pct > 70 ? '#fbbf24' : '#7c5cfc';
            const statusClass = l.active ? 'tag-active' : 'tag-disabled';
            const statusText = l.active ? 'فعال' : 'غیرفعال';
            const toggleClass = l.active ? 'on' : '';
            
            html += `<tr>
                <td style="color:rgba(255,255,255,0.3);font-size:10px;">${i+1}</td>
                <td style="font-weight:600;">${l.label}</td>
                <td><span class="tag tag-vless">VLESS</span></td>
                <td>
                    <div class="usage-bar">
                        <span class="used">${used}GB</span>
                        <div class="track"><div class="fill" style="width:${pct}%;background:${color};"></div></div>
                        <span class="limit">${limit}GB</span>
                    </div>
                </td>
                <td style="font-size:11px;">${l.current_connections}/${l.max_connections||'∞'}</td>
                <td><span class="tag ${statusClass}">${statusText}</span></td>
                <td>
                    <button class="toggle ${toggleClass}" onclick="toggleLink('${l.uuid}')"></button>
                    <button class="btn btn-danger btn-sm" onclick="deleteLink('${l.uuid}')">🗑</button>
                    <button class="btn btn-secondary btn-sm" onclick="copyText('${l.vless_link}')">📋</button>
                </td>
            </tr>`;
        });
        
        html += '</tbody></table></div>';
        container.innerHTML = html;
    } catch(e) { console.error(e); }
}

// ============================================================
// LINK OPERATIONS
// ============================================================
async function toggleLink(uid) {
    try {
        const r = await fetch('/api/links/' + uid, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ active: true })
        });
        if (!r.ok) throw new Error();
        toast('✅ وضعیت تغییر کرد');
        loadLinks();
        loadStats();
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

async function createLink() {
    const name = document.getElementById('linkName').value.trim() || 'New Link';
    const limit = parseFloat(document.getElementById('linkLimit').value) || 0;
    const unit = document.getElementById('linkUnit').value;
    const expiry = parseInt(document.getElementById('linkExpiry').value) || 0;
    const maxConn = parseInt(document.getElementById('linkMaxConn').value) || 0;
    
    if (!/^[a-zA-Z0-9\-_. ]+$/.test(name)) {
        toast('❌ نام نامعتبر است', true);
        return;
    }
    
    try {
        const r = await fetch('/api/links', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ label: name, limit_value: limit, limit_unit: unit, expiry_days: expiry, max_connections: maxConn })
        });
        if (!r.ok) throw new Error();
        toast('✅ اینباند ساخته شد');
        closeModal('addModal');
        document.getElementById('linkName').value = '';
        document.getElementById('linkLimit').value = '';
        document.getElementById('linkExpiry').value = '';
        document.getElementById('linkMaxConn').value = '';
        loadLinks();
        loadStats();
    } catch(e) { toast('❌ خطا', true); }
}

// ============================================================
// ADDRESSES
// ============================================================
async function loadAddresses() {
    try {
        const r = await fetch('/api/addresses');
        const d = await r.json();
        const addresses = d.addresses || [];
        const container = document.getElementById('addressesContainer');
        
        if (!addresses.length) {
            container.innerHTML = '<div style="text-align:center;padding:10px;color:rgba(255,255,255,0.2);">هیچ آی‌پی اضافه نشده</div>';
            return;
        }
        
        let html = '';
        addresses.forEach((a, i) => {
            html += `<div class="address-item">
                <span class="addr">${a}</span>
                <div class="actions">
                    <button class="btn btn-danger btn-sm" onclick="deleteAddress(${i})">🗑</button>
                </div>
            </div>`;
        });
        container.innerHTML = html;
    } catch(e) { console.error(e); }
}

async function addAddresses() {
    const text = document.getElementById('addressInput').value.trim();
    if (!text) { toast('❌ وارد کنید', true); return; }
    const lines = text.split('\\n').map(l => l.trim()).filter(l => l);
    let added = 0;
    for (const addr of lines) {
        try {
            const r = await fetch('/api/addresses', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ address: addr })
            });
            if (r.ok) added++;
        } catch(e) {}
    }
    if (added) {
        toast('✅ ' + added + ' آدرس افزوده شد');
        closeModal('addressModal');
        document.getElementById('addressInput').value = '';
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
// DOMAIN
// ============================================================
async function loadDomain() {
    try {
        const r = await fetch('/api/domain');
        const d = await r.json();
        const domain = d.domain || window.location.hostname || 'localhost';
        const container = document.getElementById('domainContainer');
        
        container.innerHTML = `
            <div class="domain-display">
                <div class="label">دامنه فعلی</div>
                <div class="value">${domain}</div>
                <div class="domain-status" id="domainStatus">🔍 در حال بررسی...</div>
            </div>
        `;
        checkDomain();
    } catch(e) { console.error(e); }
}

async function checkDomain() {
    try {
        const r = await fetch('/api/domain/status');
        const d = await r.json();
        const status = d.current;
        const el = document.getElementById('domainStatus');
        if (status.reachable) {
            el.innerHTML = '✅ آنلاین | IP: ' + (status.ip || '--') + ' | SSL: ' + (status.ssl_valid ? '✅' : '⚠️');
            el.className = 'domain-status online';
        } else {
            el.textContent = '❌ آفلاین';
            el.className = 'domain-status offline';
        }
    } catch(e) { console.error(e); }
}

async function saveDomain() {
    const domain = document.getElementById('domainInput').value.trim();
    if (!domain) { toast('❌ دامنه وارد کنید', true); return; }
    try {
        const r = await fetch('/api/domain', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain })
        });
        if (!r.ok) throw new Error();
        toast('✅ دامنه ذخیره شد');
        document.getElementById('domainInput').value = '';
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
// COPY
// ============================================================
function copyText(text) {
    navigator.clipboard.writeText(text).then(() => toast('📋 کپی شد')).catch(() => {
        const ta = document.createElement('textarea');
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        toast('📋 کپی شد');
    });
}

// ============================================================
// AUTO REFRESH
// ============================================================
loadStats();
loadLinks();
loadAddresses();
loadDomain();
setInterval(loadStats, 5000);
setInterval(loadLinks, 30000);
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
    pos += 1
    pos += 16
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
