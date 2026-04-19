"""
main.py — Pipeline Orkestratörü

AKIŞ:
  1. AsyncIO ile API'lardan veri çek (fetcher)
  2. Spark ile veriyi dönüştür (transformer)
  3. asyncpg ile PostgreSQL'e yaz (writer)

asyncio.run() → event loop'u başlatır ve bitirir
Tüm async kod bu loop içinde çalışır
"""

import asyncio
import logging
import os
import sys
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from starlette.responses import Response
import signal
import uvicorn

# Modüller
sys.path.insert(0, "/app")
from fetcher.fetcher     import fetch_all_data
from spark.transformer   import create_spark_session, transform_all
from db.writer           import write_all


worker_task: asyncio.Task
shutdown_event = asyncio.Event()
# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


# ── Ana Pipeline ───────────────────────────────────────────────────────────

async def run_pipeline():
    load_dotenv()   # .env dosyasını oku

    logger.info("═══ Pipeline başladı ═══════════════════════")

    # ── ADIM 1: Veri Çekme ─────────────────────────────────
    logger.info("ADIM 1/3 — API'lardan veri çekiliyor...")
    raw_data = await fetch_all_data()
    logger.info(
        f"  ürün={len(raw_data['products'])} "
        f"kullanıcı={len(raw_data['users'])} "
        f"sipariş={len(raw_data['orders'])}"
    )

    # ── ADIM 2: Spark Dönüşümü ─────────────────────────────
    logger.info("ADIM 2/3 — Spark ile veri dönüştürülüyor...")
    spark = create_spark_session(
        os.getenv("SPARK_MASTER", "local[*]")
        # local[*] → Docker dışında çalıştırırken (tüm CPU çekirdeklerini kullan)
        # spark://spark-master:7077 → Docker içinde (gerçek cluster)
    )

    try:
        dataframes = transform_all(raw_data, spark)
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    #finally:
    #   spark.stop()   # SparkSession'ı mutlaka kapat

    # ── ADIM 3: PostgreSQL'e Yaz ────────────────────────────
    logger.info("ADIM 3/3 — PostgreSQL'e yazılıyor...")
    await write_all(dataframes)

    logger.info("═══ Pipeline tamamlandı ════════════════════")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task,shutdown_event

    logger.info("Servis başladı")

    try:
        worker_task=asyncio.create_task(run_pipeline())
        yield
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        logger.info("Data Pipeline sonlandı")
    if worker_task:
        worker_task.cancel()
    if shutdown_event:
        shutdown_event.set()


app = FastAPI(
    title="Ecommerce Spark Pipeline",
    description="Asenkron veri çekme ve Spark ile transform operasyonlarını yönetir.",
    version="1.0.1",
    lifespan=lifespan
)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "ecommerce-pipeline"}

# ── Giriş Noktası ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Docker ortamı için host 0.0.0.0 olmalı
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
