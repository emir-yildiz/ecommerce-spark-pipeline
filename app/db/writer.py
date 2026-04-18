"""
writer.py — Spark DataFrame → PostgreSQL yazma

İKİ YÖNTEM gösteriliyor:
─────────────────────────
1. asyncpg  → Async, Python kontrolünde, küçük/orta veri için ideal
2. JDBC      → Spark'ın kendi motoru, büyük veri için ideal (paralel yazar)

asyncpg vs psycopg2:
- psycopg2  → senkron (blocking), geleneksel
- asyncpg   → async (non-blocking), asyncio ile uyumlu, ~3x daha hızlı
"""

import asyncio
import asyncpg
import logging
import os
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


# ── Bağlantı Havuzu (Connection Pool) ─────────────────────────────────────
# Her sorguda yeni bağlantı açmak pahalıdır.
# Pool: önceden açılmış bağlantıları yeniden kullanır.

async def create_pool() -> asyncpg.Pool:
    """
    min_size: başlangıçta açık bağlantı sayısı
    max_size: aynı anda en fazla açık bağlantı
    """
    return await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database=os.getenv("DB_NAME", "ecommerce"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        min_size=2,
        max_size=10,
    )


# ── asyncpg ile Yazma (YÖNTEM 1) ───────────────────────────────────────────

async def write_products_async(pool: asyncpg.Pool, products_df: DataFrame) -> None:
    """
    Spark DataFrame → Python list → asyncpg ile PostgreSQL

    executemany() → tek tek INSERT yerine toplu INSERT (çok daha hızlı)
    ON CONFLICT    → aynı id varsa güncelle (upsert pattern)
    """
    # Spark DataFrame'i Python list'e çevir
    # NOT: collect() tüm veriyi driver'a çeker → büyük veride kullanma!
    rows = products_df.collect()

    records = [
        (row["id"], row["title"], row["price"], row["category"], row["rating"])
        for row in rows
    ]

    async with pool.acquire() as conn:         # pool'dan bir bağlantı al
        await conn.executemany(
            """
            INSERT INTO products (id, title, price, category, rating)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                title    = EXCLUDED.title,
                price    = EXCLUDED.price,
                category = EXCLUDED.category,
                rating   = EXCLUDED.rating
            """,
            records
        )

    logger.info(f"✓ {len(records)} ürün PostgreSQL'e yazıldı")


async def write_users_async(pool: asyncpg.Pool, users_df: DataFrame) -> None:
    rows = users_df.collect()
    records = [(row["id"], row["name"], row["email"], row["city"]) for row in rows]

    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO users (id, name, email, city)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                name  = EXCLUDED.name,
                email = EXCLUDED.email,
                city  = EXCLUDED.city
            """,
            records
        )

    logger.info(f"✓ {len(records)} kullanıcı PostgreSQL'e yazıldı")


async def write_orders_async(pool: asyncpg.Pool, orders_df: DataFrame) -> None:
    rows = orders_df.collect()
    records = [
        (row["user_id"], row["product_id"], row["quantity"], float(row["total_price"]))
        for row in rows
    ]

    async with pool.acquire() as conn:
        # Siparişleri her çalışmada sıfırdan yaz (idempotent pipeline)
        await conn.execute("TRUNCATE TABLE orders RESTART IDENTITY CASCADE")
        await conn.executemany(
            """
            INSERT INTO orders (user_id, product_id, quantity, total_price)
            VALUES ($1, $2, $3, $4)
            """,
            records
        )

    logger.info(f"✓ {len(records)} sipariş PostgreSQL'e yazıldı")


# ── JDBC ile Yazma (YÖNTEM 2 — büyük veri için) ───────────────────────────

def write_with_jdbc(df: DataFrame, table_name: str, mode: str = "append") -> None:
    """
    Spark'ın kendi JDBC connector'ı ile doğrudan yazar.

    mode seçenekleri:
    - append    → mevcut veriye ekle
    - overwrite → tabloyu silip yeniden yaz (DİKKAT!)
    - ignore    → tablo doluysa hiçbir şey yapma
    - error     → tablo varsa hata fırlat

    Avantaj: Spark paralel olarak yazar (her partition ayrı connection)
    Dezavantaj: JDBC jar dosyası gerekir, asyncio ile uyumsuz
    """
    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:5432/"
        f"{os.getenv('DB_NAME', 'ecommerce')}"
    )

    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", os.getenv("DB_USER")) \
      .option("password", os.getenv("DB_PASS")) \
      .option("driver", "org.postgresql.Driver") \
      .mode(mode) \
      .save()

    logger.info(f"✓ JDBC ile {table_name} tablosuna yazıldı (mode={mode})")


# ── Pipeline'dan çağrılacak ana fonksiyon ──────────────────────────────────

async def write_all(dataframes: dict) -> None:
    """
    Tüm tabloları PostgreSQL'e yazar.
    asyncio.gather → ürün ve kullanıcı yazmayı AYNI ANDA başlatır.
    """
    pool = await create_pool()

    try:
        # Ürün ve kullanıcıları paralel yaz (birbirinden bağımsız)
        await asyncio.gather(
            write_products_async(pool, dataframes["products"]),
            write_users_async(pool, dataframes["users"]),
        )

        # Siparişler users ve products'a FK'ye sahip → önce onlar yazılmalı
        await write_orders_async(pool, dataframes["orders"])

        logger.info("✓ Tüm veriler başarıyla yazıldı")

    finally:
        await pool.close()   # her durumda pool'u kapat
