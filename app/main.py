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

# Modüller
sys.path.insert(0, "/app")
from fetcher.fetcher     import fetch_all_data
from spark.transformer   import create_spark_session, transform_all
from db.writer           import write_all

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
    finally:
        spark.stop()   # SparkSession'ı mutlaka kapat

    # ── ADIM 3: PostgreSQL'e Yaz ────────────────────────────
    logger.info("ADIM 3/3 — PostgreSQL'e yazılıyor...")
    await write_all(dataframes)

    logger.info("═══ Pipeline tamamlandı ════════════════════")


# ── Giriş noktası ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    # asyncio.run() → event loop'u oluşturur, coroutine'i çalıştırır, kapatır
    asyncio.run(run_pipeline())
