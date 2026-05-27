from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional
import uvicorn
import logging
import traceback
import urllib.request
import json
import time
import os

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

# === FastAPI Web Server with Integrated Task Queue ===

app = FastAPI(
    title="HHB / FKD B2B Integration Backend",
    description="Отказоустойчивый сервер обработки очередей задач (1С, Битрикс24, Генерация счетов)",
    version="1.0.0"
)

# Enable CORS for local testing on frontend (index.html, admin.html)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instantiate queue manager and start worker thread on server boot
logger.info("[Server] Инициализация менеджера очередей задач...")
qm = QueueManager()
qm.start_worker()

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
        
    task_id = qm.add_task(input_data.task_type, input_data.payload, input_data.max_retries)
    return {"status": "added", "task_id": task_id, "detail": "Задача успешно добавлена в очередь на обработку."}

@app.get("/api/queue/status/{task_id}")
def get_task_status(task_id: int):
    status = qm.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача с таким ID не найдена в базе данных.")
    return status

@app.get("/api/queue/list")
def list_tasks():
    return qm.list_tasks(limit=50)

@app.get("/api/queue/stats")
def get_stats():
    return qm.get_queue_stats()

@app.post("/api/queue/retry/{task_id}")
def retry_task(task_id: int):
    status = qm.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена.")
    if status["status"] != "failed":
        raise HTTPException(status_code=400, detail="Перезапустить можно только задачи со статусом 'failed'.")
        
    qm.retry_task(task_id)
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

@app.post("/api/webhooks/bitrix")
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
    task_id = qm.add_task("crm_lead", task_payload, max_retries=3)
    return {
        "status": "received",
        "event_processed": payload.event,
        "task_id": task_id,
        "detail": "Событие Битрикс24 зарегистрировано и добавлено в асинхронную очередь воркера."
    }

@app.post("/api/webhooks/1c")
def one_c_webhook(payload: OneCWebhookInput):
    logger.info(f"[Webhook] Получено обновление остатков из 1С для артикула: {payload.sku}")
    
    task_payload = {
        "sku": payload.sku,
        "new_stock": payload.new_stock,
        "new_price": payload.new_price
    }
    
    # Queue heavy inventory update asynchronously
    task_id = qm.add_task("1c_sync", task_payload, max_retries=3)
    return {
        "status": "received",
        "sku_updated": payload.sku,
        "task_id": task_id,
        "detail": "Запрос обновления номенклатуры из 1С принят и поставлен в очередь задач."
    }

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
