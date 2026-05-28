from fastapi import FastAPI, HTTPException, Request, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import uvicorn
import logging
import traceback
import urllib.request
import json
import time
import os
import smtplib
import sqlite3
import psycopg2
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import defaultdict
from datetime import datetime

# === CONFIGURE STANDARD SYSTEM LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler("D:/pod/backend/app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HHB_B2B")

from queue_manager import QueueManager

# === Hybrid DB: PostgreSQL preferred, SQLite fallback for dev ===
PG_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hhb_b2b")
SQLITE_PATH = os.getenv("SQLITE_PATH", "D:/pod/backend/catalog.db")

_use_pg = False

def _test_pg():
    global _use_pg
    try:
        conn = psycopg2.connect(PG_URL)
        conn.close()
        _use_pg = True
        logger.info("[Database] PostgreSQL доступен. Используем PG для каталога/КП.")
    except Exception:
        _use_pg = False
        logger.warning("[Database] PostgreSQL недоступен. Fallback на SQLite для каталога/КП.")

_test_pg()

def get_db():
    if _use_pg:
        return psycopg2.connect(PG_URL)
    else:
        return sqlite3.connect(SQLITE_PATH)

def q(sql):
    """Adapt SQL from PostgreSQL dialect to SQLite if needed."""
    if _use_pg:
        return sql
    return sql.replace('%s', '?').replace('ILIKE', 'LIKE').replace('RETURNING id', '')

def get_last_id(cursor):
    if _use_pg:
        return cursor.fetchone()[0]
    else:
        return cursor.lastrowid

def _ph(count):
    """Return placeholders for current DB driver."""
    if _use_pg:
        return ','.join(['%s'] * count)
    else:
        return ','.join(['?'] * count)

def init_catalog_tables():
    """Initialize SKU catalog, clients, proposals and proposal_items tables."""
    try:
        conn = get_db()
        cursor = conn.cursor()
        if _use_pg:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sku_catalog (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(200) NOT NULL UNIQUE,
                    category VARCHAR(100), gost VARCHAR(50),
                    d_inner NUMERIC(10,2), d_outer NUMERIC(10,2), b_width NUMERIC(10,2),
                    type VARCHAR(300), brand VARCHAR(50), stock VARCHAR(100),
                    price NUMERIC(12,2) NOT NULL DEFAULT 0,
                    img VARCHAR(300), created_at VARCHAR(100)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(300) NOT NULL, bitrix_id VARCHAR(100),
                    email VARCHAR(300), city VARCHAR(100),
                    discount INTEGER NOT NULL DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'active', created_at VARCHAR(100)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proposals (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER REFERENCES clients(id) ON DELETE SET NULL,
                    title VARCHAR(300), total_amount NUMERIC(14,2) DEFAULT 0,
                    discount_global INTEGER DEFAULT 0, status VARCHAR(50) DEFAULT 'draft',
                    email_sent BOOLEAN DEFAULT FALSE,
                    created_at VARCHAR(100), updated_at VARCHAR(100)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proposal_items (
                    id SERIAL PRIMARY KEY,
                    proposal_id INTEGER REFERENCES proposals(id) ON DELETE CASCADE,
                    sku_id INTEGER REFERENCES sku_catalog(id) ON DELETE CASCADE,
                    qty INTEGER NOT NULL DEFAULT 1,
                    price_base NUMERIC(12,2) NOT NULL DEFAULT 0,
                    discount_item INTEGER DEFAULT 0,
                    price_final NUMERIC(12,2) NOT NULL DEFAULT 0
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sku_catalog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT NOT NULL UNIQUE,
                    category TEXT, gost TEXT,
                    d_inner REAL, d_outer REAL, b_width REAL,
                    type TEXT, brand TEXT, stock TEXT,
                    price REAL NOT NULL DEFAULT 0,
                    img TEXT, created_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL, bitrix_id TEXT,
                    email TEXT, city TEXT,
                    discount INTEGER NOT NULL DEFAULT 0,
                    status TEXT DEFAULT 'active', created_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proposals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER,
                    title TEXT, total_amount REAL DEFAULT 0,
                    discount_global INTEGER DEFAULT 0, status TEXT DEFAULT 'draft',
                    email_sent INTEGER DEFAULT 0,
                    created_at TEXT, updated_at TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proposal_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id INTEGER,
                    sku_id INTEGER,
                    qty INTEGER NOT NULL DEFAULT 1,
                    price_base REAL NOT NULL DEFAULT 0,
                    discount_item INTEGER DEFAULT 0,
                    price_final REAL NOT NULL DEFAULT 0
                )
            """)
        conn.commit()
        logger.info("[Database] Таблицы КП, каталога и клиентов инициализированы.")
        conn.close()
    except Exception as e:
        logger.error(f"[!] [Database Error] Ошибка инициализации каталога/КП: {e}")

init_catalog_tables()

# === Seed Data (one-time load if tables empty) ===
def seed_data():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sku_catalog")
        if cursor.fetchone()[0] == 0:
            skus = [
                ('HHB UCP 206', 'housing', '480206', 30, 62, 38.1, 'Корпусной узел на лапах (Pillow Block)', 'HHB', 'Достаточно', 1180, 'images/ucp.jpg'),
                ('HHB UCF 208', 'housing', '480208', 40, 80, 49.2, 'Квадратный фланцевый узел (Flange Block)', 'HHB', 'Достаточно', 1420, 'images/ucf.jpg'),
                ('HHB UCFL 205', 'housing', '480205', 25, 52, 34.1, 'Ромбический фланцевый узел (2-bolt Flange)', 'HHB', 'Достаточно', 980, 'images/ucfl.jpg'),
                ('HHB UCT 207', 'housing', '480207', 35, 72, 42.9, 'Натяжной узел для нории (Take-up Unit)', 'HHB', 'В наличии', 1850, 'images/uct.jpg'),
                ('HHB STAINLESS UC 204', 'stainless', 'SS480204', 20, 47, 31, 'Нержавеющая сталь (Stainless Series)', 'HHB', '18 шт', 2950, 'images/stainless.jpg'),
                ('FKD UK 208 + H2308', 'housing', 'UK208', 35, 80, 49, 'С конической закрепительной втулкой', 'FKD', '95 шт', 1620, 'images/uk.jpg'),
                ('FKD NA 206', 'housing', 'NA206', 30, 62, 36.4, 'С эксцентриковым стопорным кольцом', 'FKD', 'Достаточно', 730, 'images/na.jpg'),
                ('HHB 22315-E1-T41A', 'roller', '3615', 75, 160, 55, 'Сферический роликовый для виброгрохотов', 'HHB', '12 шт', 7950, 'images/spherical.jpg'),
                ('HHB 6205-2RS C3', 'ball', '180205', 25, 52, 15, 'Радиальный шариковый с увеличенным зазором', 'HHB', '1 240 шт', 420, 'frames_eevee/mobile_webp/0060.webp'),
                ('HHB 6206-2RS C3', 'ball', '180206', 30, 62, 16, 'Радиальный шариковый с зазором C3', 'HHB', '850 шт', 540, 'frames_eevee/mobile_webp/0060.webp'),
                ('FKD UC 210', 'housing', '480210', 50, 90, 51.6, 'Шариковый радиальный под закрепительный винт', 'FKD', '320 шт', 690, 'images/ucp.jpg'),
                ('Сальник 30х52х10 (Манжета)', 'cuffs', '8752-79', 30, 52, 10, 'Армированная одновальная манжета ГОСТ', 'FKD', 'Достаточно', 180, 'frames_eevee/mobile_webp/0060.webp'),
                ('HHB NU 312 ECP', 'roller', '12312', 60, 130, 31, 'Цилиндрический роликовый', 'HHB', '45 шт', 4300, 'images/roller.jpg'),
                ('HHB 6308-2RS', 'ball', '180308', 40, 90, 23, 'Радиальный шариковый однорядный', 'HHB', '560 шт', 890, 'images/ball.jpg'),
                ('FKD UCP 209', 'housing', '480209', 45, 85, 49.2, 'Корпусной узел на лапах', 'FKD', '120 шт', 1050, 'images/ucp.jpg'),
            ]
            now = datetime.now().isoformat()
            skus = [sku + (now,) for sku in skus]
            cursor.executemany(f"""
                INSERT INTO sku_catalog (sku, category, gost, d_inner, d_outer, b_width, type, brand, stock, price, img, created_at)
                VALUES ({_ph(12)})
            """, skus)
            logger.info(f"[Seed] Загружено {len(skus)} SKU в каталог.")

        cursor.execute("SELECT COUNT(*) FROM clients")
        if cursor.fetchone()[0] == 0:
            clients = [
                ('ООО "АГРОЭКО"', 'BX_1245', 'snab@agroeco.ru', 'Воронеж', 15, 'active'),
                ('ООО "ЭКОНИВА-ЧЕРНОЗЕМЬЕ"', 'BX_3312', 'zakup@econiva.ru', 'Воронеж', 10, 'active'),
                ('АПХ "МИРАТОРГ"', 'BX_8821', 'supply@miratorg.ru', 'Орёл', 5, 'active'),
                ('ГК "РУСАГРО"', 'BX_9901', 'tender@rusagro.ru', 'Москва', 0, 'new'),
                ('ООО "Воронежский Элеватор"', 'BX_1122', 'main@vorelev.ru', 'Воронеж', 20, 'vip'),
            ]
            now = datetime.now().isoformat()
            clients = [client + (now,) for client in clients]
            cursor.executemany(f"""
                INSERT INTO clients (name, bitrix_id, email, city, discount, status, created_at)
                VALUES ({_ph(7)})
            """, clients)
            logger.info(f"[Seed] Загружено {len(clients)} клиентов.")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[!] [Seed Error] Ошибка загрузки seed-данных: {e}")

seed_data()

# === FastAPI Web Server with Integrated Task Queue ===

app = FastAPI(
    title="HHB / FKD B2B Integration Backend",
    description="Отказоустойчивый сервер обработки очередей задач (1С, Битрикс24, Генерация счетов)",
    version="2.0.0"
)

# Enable CORS for local testing on frontend (index.html, admin.html)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === IN-MEMORY TOKEN BUCKET RATE LIMITER ===
# Sliding window rate limiter to protect resources from brute-force/DDOS (No Redis needed!)
rate_limit_records = defaultdict(list)

def get_rate_limit(path: str) -> int:
    if "/api/ai/search" in path:
        return 10  # Max 10 search queries per minute
    if "/api/queue/add" in path:
        return 20  # Max 20 new tasks per minute
    if "/api/webhooks/" in path:
        return 30  # Max 30 incoming webhooks per minute
    return 60      # Default: 60 requests per minute for other endpoints

@app.middleware("http")
async def rate_limiting_middleware(request: Request, call_next):
    # Skip docs, redoc, openapi.json and root paths
    path = request.url.path
    if path in ["/", "/docs", "/redoc", "/openapi.json"]:
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    limit = get_rate_limit(path)
    now = time.time()
    
    # Unique key combining IP and path category
    key = f"{client_ip}:{path}"
    
    # Cleanup timestamps older than 60 seconds
    rate_limit_records[key] = [t for t in rate_limit_records[key] if now - t < 60]
    
    if len(rate_limit_records[key]) >= limit:
        logger.warning(f"[Rate Limit Blocked] IP {client_ip} превысил лимит на {path} ({limit} запр./мин).")
        return Response(
            content=json.dumps({"detail": "Too Many Requests. Вы превысили лимит запросов для этого эндпоинта. Попробуйте позже."}),
            status_code=429,
            media_type="application/json",
            headers={"Retry-After": "60"}
        )
        
    rate_limit_records[key].append(now)
    return await call_next(request)

# === SECURE BEARER TOKEN AUTHORIZATION DEPENDENCY ===
B2B_ADMIN_TOKEN = os.getenv("B2B_ADMIN_TOKEN", "hhb_b2b_secret_token_2026")

def verify_b2b_token(request: Request):
    # Allow local Swagger UI testing to bypass authorization easily if wanted
    # But strictly enforce on real requests
    auth_header = request.headers.get("Authorization") or request.headers.get("X-API-Key")
    
    token = None
    if auth_header:
        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
        else:
            token = auth_header

    if token != B2B_ADMIN_TOKEN:
        logger.warning(f"[Auth Failed] Неавторизованный запрос к {request.url.path} с IP {request.client.host if request.client else 'unknown'}")
        raise HTTPException(status_code=401, detail="Unauthorized. Неверный или отсутствующий API-токен авторизации B2B.")
    return token

# Instantiate queue manager and start worker thread on server boot
logger.info("[Server] Инициализация менеджера очередей задач...")
qm = None
if _use_pg:
    qm = QueueManager()
    qm.start_worker()
else:
    logger.warning("[Queue] PostgreSQL недоступен. Очередь задач отключена для локального SQLite-режима.")

def get_queue_manager():
    if qm is None:
        raise HTTPException(status_code=503, detail="Очередь задач недоступна: PostgreSQL не запущен. КП, каталог и клиенты работают в локальном SQLite-режиме.")
    return qm

class TaskInput(BaseModel):
    task_type: str
    payload: Dict[str, Any]
    max_retries: int = 3

@app.get("/")
def read_root():
    return {
        "status": "online",
        "service": "HHB B2B Integration Queue",
        "endpoints": {
            "swagger": "/docs",
            "add_task": "POST /api/queue/add",
            "list_tasks": "GET /api/queue/list",
            "stats": "GET /api/queue/stats"
        }
    }

@app.post("/api/queue/add")
def add_task(input_data: TaskInput):
    valid_types = ["1c_sync", "crm_lead", "email_invoice"]
    if input_data.task_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Невалидный тип задачи. Допустимые: {valid_types}")
        
    manager = get_queue_manager()
    task_id = manager.add_task(input_data.task_type, input_data.payload, input_data.max_retries)
    return {"status": "added", "task_id": task_id, "detail": "Задача успешно добавлена в очередь на обработку."}

@app.get("/api/queue/status/{task_id}")
def get_task_status(task_id: int):
    manager = get_queue_manager()
    status = manager.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача с таким ID не найдена в базе данных.")
    return status

@app.get("/api/queue/list", dependencies=[Depends(verify_b2b_token)])
def list_tasks():
    manager = get_queue_manager()
    return manager.list_tasks(limit=50)

@app.get("/api/queue/stats", dependencies=[Depends(verify_b2b_token)])
def get_stats():
    manager = get_queue_manager()
    return manager.get_queue_stats()

@app.post("/api/queue/retry/{task_id}", dependencies=[Depends(verify_b2b_token)])
def retry_task(task_id: int):
    manager = get_queue_manager()
    status = manager.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена.")
    if status["status"] != "failed":
        raise HTTPException(status_code=400, detail="Перезапустить можно только задачи со статусом 'failed'.")
        
    manager.retry_task(task_id)
    return {"status": "queued", "task_id": task_id, "detail": "Задача возвращена в статус 'pending' на повторную обработку."}

# === WEBHOOK ENDPOINTS (PUSH INTEGRATIONS) ===

class BitrixWebhookInput(BaseModel):
    event: str
    data: Dict[str, Any]

class OneCWebhookInput(BaseModel):
    sku: str
    new_stock: int
    new_price: float

class AiSearchRequest(BaseModel):
    query: str
    api_key: Optional[str] = None

@app.post("/api/webhooks/bitrix", dependencies=[Depends(verify_b2b_token)])
def bitrix_webhook(payload: BitrixWebhookInput):
    logger.info(f"[Webhook] Получено событие от Битрикс24: {payload.event}")
    
    # Process event payload (e.g. Lead update, Deal close)
    deal_id = payload.data.get("FIELDS", {}).get("ID") or payload.data.get("ID")
    task_payload = {
        "event_type": payload.event,
        "deal_id": deal_id,
        "raw_data": payload.data
    }
    
    # Queue task asynchronously so Bitrix gets immediate 200 OK reply
    manager = get_queue_manager()
    task_id = manager.add_task("crm_lead", task_payload, max_retries=3)
    return {
        "status": "received",
        "event_processed": payload.event,
        "task_id": task_id,
        "detail": "Событие Битрикс24 зарегистрировано и добавлено в асинхронную очередь воркера."
    }

@app.post("/api/webhooks/1c", dependencies=[Depends(verify_b2b_token)])
def one_c_webhook(payload: OneCWebhookInput):
    logger.info(f"[Webhook] Получено обновление остатков из 1С для артикула: {payload.sku}")
    
    task_payload = {
        "sku": payload.sku,
        "new_stock": payload.new_stock,
        "new_price": payload.new_price
    }
    
    # Queue heavy inventory update asynchronously
    manager = get_queue_manager()
    task_id = manager.add_task("1c_sync", task_payload, max_retries=3)
    return {
        "status": "received",
        "sku_updated": payload.sku,
        "task_id": task_id,
        "detail": "Запрос обновления номенклатуры из 1С принят и поставлен в очередь задач."
    }

# ============================================================================
# === CATALOG, CLIENTS & PROPOSAL (КП) API ===
# ============================================================================

class SkuInput(BaseModel):
    sku: str
    category: Optional[str] = ""
    gost: Optional[str] = ""
    d: Optional[float] = None
    D: Optional[float] = None
    B: Optional[float] = None
    type: Optional[str] = ""
    brand: Optional[str] = ""
    stock: Optional[str] = ""
    price: float = 0
    img: Optional[str] = ""

class ProposalInput(BaseModel):
    client_id: int
    title: Optional[str] = ""
    discount_global: int = 0

class ProposalItemInput(BaseModel):
    sku_id: int
    qty: int = 1
    discount_item: int = 0

class SendEmailInput(BaseModel):
    recipient_email: Optional[str] = None
    subject: Optional[str] = "Коммерческое предложение HHB / FKD"

class DiscountInput(BaseModel):
    discount_global: int = 0

def recalc_proposal_total(proposal_id: int):
    """Recalculate total amount for a proposal based on its items."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("SELECT discount_global FROM proposals WHERE id = %s"), (proposal_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return
    global_discount = row[0]
    cursor.execute(q("""
        SELECT qty, price_base, discount_item FROM proposal_items WHERE proposal_id = %s
    """), (proposal_id,))
    total = 0
    for qty, price_base, discount_item in cursor.fetchall():
        # Apply item discount first, then global discount
        price_after_item = float(price_base) * (1 - int(discount_item) / 100)
        price_after_global = price_after_item * (1 - int(global_discount) / 100)
        total += price_after_global * int(qty)
    cursor.execute(q("UPDATE proposals SET total_amount = %s, updated_at = %s WHERE id = %s"),
                   (total, datetime.now().isoformat(), proposal_id))
    conn.commit()
    conn.close()

# === SKU CATALOG ENDPOINTS ===

@app.get("/api/catalog/skus")
def list_skus(category: Optional[str] = None, search: Optional[str] = None, d_min: Optional[float] = None, d_max: Optional[float] = None):
    conn = get_db()
    cursor = conn.cursor()
    query = "SELECT id, sku, category, gost, d_inner, d_outer, b_width, type, brand, stock, price, img FROM sku_catalog WHERE 1=1"
    params = []
    if category and category != 'all':
        query += " AND category = %s"
        params.append(category)
    if search:
        query += " AND (sku ILIKE %s OR type ILIKE %s OR gost ILIKE %s)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    if d_min is not None:
        query += " AND d_inner >= %s"
        params.append(d_min)
    if d_max is not None:
        query += " AND d_inner <= %s"
        params.append(d_max)
    query += " ORDER BY id ASC"
    cursor.execute(q(query), params)
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "sku": r[1], "category": r[2], "gost": r[3], "d": float(r[4]) if r[4] else None,
             "D": float(r[5]) if r[5] else None, "B": float(r[6]) if r[6] else None,
             "type": r[7], "brand": r[8], "stock": r[9], "price": float(r[10]) if r[10] else 0, "img": r[11]} for r in rows]

@app.post("/api/catalog/skus", dependencies=[Depends(verify_b2b_token)])
def add_sku(data: SkuInput):
    now = datetime.now().isoformat()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(q("""
            INSERT INTO sku_catalog (sku, category, gost, d_inner, d_outer, b_width, type, brand, stock, price, img, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """), (data.sku, data.category, data.gost, data.d, data.D, data.B, data.type, data.brand, data.stock, data.price, data.img, now))
        sku_id = get_last_id(cursor)
        conn.commit()
        conn.close()
        logger.info(f"[Catalog] Добавлен SKU #{sku_id}: {data.sku}")
        return {"status": "created", "sku_id": sku_id}
    except psycopg2.IntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="SKU с таким артикулом уже существует.")

# === CLIENTS ENDPOINTS ===

@app.get("/api/clients")
def list_clients():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, bitrix_id, email, city, discount, status FROM clients ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "bitrix_id": r[2], "email": r[3], "city": r[4], "discount": r[5], "status": r[6]} for r in rows]

@app.get("/api/clients/{client_id}")
def get_client(client_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("SELECT id, name, bitrix_id, email, city, discount, status FROM clients WHERE id = %s"), (client_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден.")
    return {"id": row[0], "name": row[1], "bitrix_id": row[2], "email": row[3], "city": row[4], "discount": row[5], "status": row[6]}

# === PROPOSAL (КП) ENDPOINTS ===

@app.post("/api/proposals")
def create_proposal(data: ProposalInput):
    now = datetime.now().isoformat()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("""
        INSERT INTO proposals (client_id, title, total_amount, discount_global, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
    """), (data.client_id, data.title or f"КП от {now[:10]}", 0, data.discount_global, 'draft', now, now))
    proposal_id = get_last_id(cursor)
    conn.commit()
    conn.close()
    logger.info(f"[Proposal] Создано КП #{proposal_id} для клиента {data.client_id}")
    return {"status": "created", "proposal_id": proposal_id}

@app.get("/api/proposals")
def list_proposals():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.id, p.client_id, c.name as client_name, p.title, p.total_amount,
               p.discount_global, p.status, p.email_sent, p.created_at
        FROM proposals p LEFT JOIN clients c ON p.client_id = c.id
        ORDER BY p.id DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return [{"id": r[0], "client_id": r[1], "client_name": r[2], "title": r[3], "total_amount": float(r[4]) if r[4] else 0,
             "discount_global": r[5], "status": r[6], "email_sent": r[7], "created_at": r[8]} for r in rows]

@app.get("/api/proposals/{proposal_id}")
def get_proposal(proposal_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("""
        SELECT p.id, p.client_id, c.name, c.email, p.title, p.total_amount, p.discount_global, p.status, p.email_sent, p.created_at
        FROM proposals p LEFT JOIN clients c ON p.client_id = c.id WHERE p.id = %s
    """), (proposal_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="КП не найдено.")
    proposal = {"id": row[0], "client_id": row[1], "client_name": row[2], "client_email": row[3],
                "title": row[4], "total_amount": float(row[5]) if row[5] else 0, "discount_global": row[6],
                "status": row[7], "email_sent": row[8], "created_at": row[9]}
    cursor.execute(q("""
        SELECT pi.id, pi.sku_id, s.sku, s.type, s.brand, pi.qty, pi.price_base, pi.discount_item, pi.price_final
        FROM proposal_items pi JOIN sku_catalog s ON pi.sku_id = s.id WHERE pi.proposal_id = %s
    """), (proposal_id,))
    items = []
    for r in cursor.fetchall():
        items.append({"id": r[0], "sku_id": r[1], "sku": r[2], "type": r[3], "brand": r[4],
                      "qty": r[5], "price_base": float(r[6]) if r[6] else 0,
                      "discount_item": r[7], "price_final": float(r[8]) if r[8] else 0})
    proposal["items"] = items
    conn.close()
    return proposal

@app.post("/api/proposals/{proposal_id}/items")
def add_proposal_item(proposal_id: int, data: ProposalItemInput):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("SELECT price FROM sku_catalog WHERE id = %s"), (data.sku_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="SKU не найден.")
    price_base = float(row[0])
    price_final = price_base * (1 - data.discount_item / 100)
    cursor.execute(q("""
        INSERT INTO proposal_items (proposal_id, sku_id, qty, price_base, discount_item, price_final)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
    """), (proposal_id, data.sku_id, data.qty, price_base, data.discount_item, price_final))
    item_id = get_last_id(cursor)
    conn.commit()
    conn.close()
    recalc_proposal_total(proposal_id)
    logger.info(f"[Proposal] В КП #{proposal_id} добавлена позиция #{item_id} (SKU {data.sku_id})")
    return {"status": "added", "item_id": item_id}

@app.put("/api/proposals/{proposal_id}/items/{item_id}")
def update_proposal_item(proposal_id: int, item_id: int, data: ProposalItemInput):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("SELECT price_base FROM proposal_items WHERE id = %s AND proposal_id = %s"), (item_id, proposal_id))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Позиция не найдена.")
    price_base = float(row[0])
    price_final = price_base * (1 - data.discount_item / 100)
    cursor.execute(q("""
        UPDATE proposal_items SET qty = %s, discount_item = %s, price_final = %s WHERE id = %s
    """), (data.qty, data.discount_item, price_final, item_id))
    conn.commit()
    conn.close()
    recalc_proposal_total(proposal_id)
    return {"status": "updated", "item_id": item_id}

@app.delete("/api/proposals/{proposal_id}/items/{item_id}")
def delete_proposal_item(proposal_id: int, item_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("DELETE FROM proposal_items WHERE id = %s AND proposal_id = %s"), (item_id, proposal_id))
    conn.commit()
    conn.close()
    recalc_proposal_total(proposal_id)
    logger.info(f"[Proposal] Из КП #{proposal_id} удалена позиция #{item_id}")
    return {"status": "deleted", "item_id": item_id}

@app.post("/api/proposals/{proposal_id}/discount")
def set_proposal_discount(proposal_id: int, data: DiscountInput):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("UPDATE proposals SET discount_global = %s, updated_at = %s WHERE id = %s"),
                   (data.discount_global, datetime.now().isoformat(), proposal_id))
    conn.commit()
    conn.close()
    recalc_proposal_total(proposal_id)
    logger.info(f"[Proposal] Установлена глобальная скидка {data.discount_global}% для КП #{proposal_id}")
    return {"status": "updated", "discount_global": data.discount_global}

# === EMAIL SENDING ===

def send_proposal_email(proposal_id: int, to_email: str, subject: str):
    """Send proposal as HTML email via SMTP."""
    smtp_server = os.getenv("SMTP_SERVER", "smtp.yandex.ru")
    smtp_port = int(os.getenv("SMTP_PORT", 465))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        logger.warning("[Email] SMTP credentials not configured. Skipping real send.")
        return False

    proposal = get_proposal(proposal_id)
    items_html = ""
    for item in proposal["items"]:
        items_html += f"""
        <tr>
            <td style="border:1px solid #ddd;padding:8px">{item['sku']}</td>
            <td style="border:1px solid #ddd;padding:8px">{item['type']}</td>
            <td style="border:1px solid #ddd;padding:8px;text-align:center">{item['qty']}</td>
            <td style="border:1px solid #ddd;padding:8px;text-align:right">{item['price_base']:,.0f} ₽</td>
            <td style="border:1px solid #ddd;padding:8px;text-align:center">{item['discount_item']}%</td>
            <td style="border:1px solid #ddd;padding:8px;text-align:right">{item['price_final']:,.0f} ₽</td>
            <td style="border:1px solid #ddd;padding:8px;text-align:right">{item['price_final'] * item['qty']:,.0f} ₽</td>
        </tr>
        """

    html_body = f"""
    <html><body style="font-family:Arial,sans-serif">
    <div style="max-width:700px;margin:0 auto">
        <div style="border-bottom:3px solid #C8102E;padding-bottom:15px;margin-bottom:20px">
            <h2 style="color:#1A237E;margin:0">ООО «Компонент Сервис»</h2>
            <p style="color:#666;margin:5px 0 0;font-size:12px">Официальный Дистрибьютор HHB & FKD в России</p>
        </div>
        <h3 style="color:#C8102E">{proposal['title']}</h3>
        <p><strong>Клиент:</strong> {proposal['client_name']}</p>
        <p><strong>Дата:</strong> {proposal['created_at'][:10]}</p>
        <p><strong>Глобальная скидка:</strong> {proposal['discount_global']}%</p>
        <table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:15px">
            <thead style="background:#1A237E;color:#fff">
                <tr>
                    <th style="padding:8px;border:1px solid #ddd">Артикул</th>
                    <th style="padding:8px;border:1px solid #ddd">Описание</th>
                    <th style="padding:8px;border:1px solid #ddd">Кол-во</th>
                    <th style="padding:8px;border:1px solid #ddd">База</th>
                    <th style="padding:8px;border:1px solid #ddd">Скидка</th>
                    <th style="padding:8px;border:1px solid #ddd">Цена</th>
                    <th style="padding:8px;border:1px solid #ddd">Сумма</th>
                </tr>
            </thead>
            <tbody>{items_html}</tbody>
        </table>
        <p style="text-align:right;font-size:18px;font-weight:bold;margin-top:20px">
            ИТОГО: {proposal['total_amount']:,.0f} ₽
        </p>
        <div style="margin-top:30px;padding-top:15px;border-top:1px solid #ddd;font-size:11px;color:#999">
            По всем вопросам: +7 (473) 255-00-00 | csbrg.ru
        </div>
    </div>
    </body></html>
    """

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, [to_email], msg.as_string())
        logger.info(f"[Email] КП #{proposal_id} отправлено на {to_email}")
        return True
    except Exception as e:
        logger.error(f"[!] [Email Error] Ошибка отправки КП #{proposal_id}: {e}")
        return False

@app.post("/api/proposals/{proposal_id}/send")
def send_proposal(proposal_id: int, data: SendEmailInput):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(q("SELECT c.email, c.name FROM proposals p JOIN clients c ON p.client_id = c.id WHERE p.id = %s"), (proposal_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="КП или клиент не найден.")
    client_email = data.recipient_email or row[0]
    client_name = row[1]
    if not client_email:
        raise HTTPException(status_code=400, detail="У клиента не указан email. Введите вручную.")

    sent = send_proposal_email(proposal_id, client_email, data.subject)
    if sent:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(q("UPDATE proposals SET email_sent = TRUE, status = 'sent', updated_at = %s WHERE id = %s"),
                       (datetime.now().isoformat(), proposal_id))
        conn.commit()
        conn.close()
        # Also queue a task for CRM logging
        if qm is not None:
            qm.add_task("crm_lead", {"type": "proposal_sent", "proposal_id": proposal_id, "client_email": client_email, "client_name": client_name}, max_retries=3)
        else:
            logger.warning(f"[Queue] CRM-задача для КП #{proposal_id} не добавлена: очередь отключена.")
        return {"status": "sent", "proposal_id": proposal_id, "recipient": client_email}
    else:
        raise HTTPException(status_code=500, detail="Не удалось отправить email. Проверьте настройки SMTP.")

# === INTELLECTUAL AI DEEPSEEK ROUTE WITH ADVANCED LOGGING ===

@app.post("/api/ai/search")
def ai_search(payload: AiSearchRequest):
    query = payload.query.strip()
    logger.info(f"[AI Search] Получен новый поисковый запрос: '{query}'")
    
    # Measure response time
    start_time = time.time()
    
    # Determine which API Key to use
    api_key = payload.api_key or os.getenv("DEEPSEEK_API_KEY")
    
    if api_key:
        try:
            logger.info("[AI Search] Отправка запроса к официальному API DeepSeek...")
            
            req = urllib.request.Request(
                "https://api.deepseek.com/chat/completions",
                data=json.dumps({
                    "model": "deepseek-chat",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Ты профессиональный консультант ООО Компонент Сервис, эксперт по премиум-подшипникам HHB и FKD. Выдай строго JSON с полями title, desc, price, stock, cross."
                        },
                        {"role": "user", "content": query}
                    ],
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"}
                }).encode('utf-8'),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}'
                }
            )
            
            with urllib.request.urlopen(req, timeout=12) as response:
                resp_data = json.loads(response.read().decode('utf-8'))
                
            elapsed_time = time.time() - start_time
            logger.info(f"[AI Search] Успешный ответ от DeepSeek за {elapsed_time:.2f} сек.")
            
            ai_content = json.loads(resp_data["choices"][0]["message"]["content"])
            return ai_content
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"[!] [AI Search Error] Сбой при запросе к DeepSeek API через {elapsed_time:.2f} сек. Ошибка: {e}")
            logger.error(traceback.format_exc())
            # Graceful fallback to local scenario database
            logger.warning("[AI Search] Активирован локальный резервный офлайн-режим для бесперебойной работы фронтенда.")
    
    # Local fallback scenario database (Instant response)
    logger.info("[AI Search] Использование встроенного офлайн-генератора решений HHB/FKD.")
    time.sleep(1.2) # Realistic synthetic thinking latency
    return get_local_fallback_response(query)

def get_local_fallback_response(query):
    query_lower = query.lower()
    if any(k in query_lower for k in ["6205", "skf", "фаг", "fag"]):
        return {
            "title": "HHB 6205-2RS C3 Premium",
            "desc": "Премиальный шариковый радиальный подшипник HHB (аналог SKF 6205-2RS1). Снабжен двусторонним износостойким уплотнением из каучука для удержания смазки и радиальным зазором C3 для бесперебойной работы при температуре до +120°C.",
            "price": "420 ₽",
            "stock": "1 240 шт",
            "cross": "SKF 6205-2RS1/C3, FAG 6205-2RSR-C3"
        }
    elif any(k in query_lower for k in ["нори", "вал 30", "пыл", "uc"]):
        return {
            "title": "HHB UCP 206 (корпусной узел на лапах)",
            "desc": "Профессиональный подшипниковый узел (чугунный литой корпус UCP206 + радиальный подшипник UC206). Оснащен трехкромочным уплотнением LS3, исключающим попадание мелкодисперсной зерновой пыли нории внутрь узла. Заполнен высококачественной агропылевой смазкой.",
            "price": "1 180 ₽",
            "stock": "86 комплектов",
            "cross": "FKL UCP206, SKF SY 30 TF"
        }
    else:
        return {
            "title": "HHB UCF 208 (фланцевый квадратный узел)",
            "desc": "Высоконадежный фланцевый узел (четырехболтовый квадратный корпус F208 + подшипник UC208). Рассчитан на высокие статические и динамические радиальные нагрузки. Посадочный вал 40 мм. Подходит для приводов элеваторов и тяжелых сеялок.",
            "price": "1 420 ₽",
            "stock": "140 шт",
            "cross": "FKL UCF208, SKF FY 40 TF"
        }

if __name__ == "__main__":
    logger.info("[Server] Запуск веб-сервера FastAPI на http://127.0.0.1:8000 ...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
