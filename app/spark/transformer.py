"""
transformer.py — Spark DataFrame API ile veri dönüşümü

TEMEL KAVRAMLAR:
─────────────────
• SparkSession   → Spark'a giriş kapısı, tüm işlemler buradan başlar
• DataFrame      → Tablo gibi düşün; satır ve sütunlardan oluşur
• Lazy Evaluation → Spark işlemleri hemen çalışmaz, .show()/.write ile tetiklenir
• Transformation → Yeni DataFrame döndürür (filter, select, join…)
• Action         → Gerçek hesaplamayı başlatır (show, count, write…)
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F    # col, lit, round, when vb.
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType, DoubleType
)

logger = logging.getLogger(__name__)


# ── SparkSession ───────────────────────────────────────────────────────────

def create_spark_session(master_url: str) -> SparkSession:
    """
    SparkSession → Spark'ın tek giriş noktası.
    getOrCreate() → Eğer zaten varsa yenisini oluşturmaz (önemli!).
    """
    return (
        SparkSession.builder
        .appName("EcommercePipeline")
        .master(master_url)                          # spark://spark-master:7077
        .config("spark.sql.shuffle.partitions", "4") # küçük veri → 4 partition yeterli
        .config("spark.ui.enabled", "false")         # container içinde UI kapatılır
        .getOrCreate()
    )


# ── Şema Tanımları ─────────────────────────────────────────────────────────
# Şema tanımlamak: JSON'dan otomatik şema çıkarmaktan 10x daha hızlı
# ve tip güvenli (type-safe) çalışmayı sağlar

PRODUCT_SCHEMA = StructType([
    StructField("id",       IntegerType(), nullable=False),
    StructField("title",    StringType(),  nullable=True),
    StructField("price",    DoubleType(),  nullable=True),
    StructField("category", StringType(),  nullable=True),
    # İç içe obje (rating.rate) için ayrı schema gerekir
    # Burada basit tutmak için flatten ederek alıyoruz
])

USER_SCHEMA = StructType([
    StructField("id",    IntegerType(), nullable=False),
    StructField("email", StringType(),  nullable=True),
    # name ve address iç içe objeler, ayrıca işleneceğiz
])

ORDER_SCHEMA = StructType([
    StructField("user_id",    IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity",   IntegerType(), nullable=True),
])


# ── Ham Veriyi DataFrame'e Çevirme ─────────────────────────────────────────

def json_to_dataframe(spark: SparkSession, data: list, schema: StructType) -> DataFrame:
    """
    Python listesini Spark DataFrame'e çevirir.

    NOT: Büyük veri için bunu yapma — doğrudan HDFS/S3'ten oku.
    Bu projede veri küçük olduğu için Python→Spark aktarımı kabul edilebilir.
    """
    # createDataFrame: şemaya uymayan sütunları otomatik atar
    return spark.createDataFrame(data, schema=schema)


# ── Ürün Dönüşümleri ───────────────────────────────────────────────────────

def transform_products(raw_products: list, spark: SparkSession) -> DataFrame:
    """
    Ürün verisini temizler ve zenginleştirir.

    Lazy evaluation örneği:
    - withColumn, filter, select → hesaplamayı BAŞLATMAZ (transformation)
    - show(), count(), write()   → hesaplamayı BAŞLATIR (action)
    """
    # Ham Python listesini düzleştir (rating nested object)
    flat = [
        {
            "id":       p.get("id"),
            "title":    p.get("title"),
            "price":    float(p.get("price", 0)),
            "category": p.get("category"),
            "rating":   float(p.get("rating", {}).get("rate", 0)),
        }
        for p in raw_products if p.get("id")
    ]

    df = spark.createDataFrame(flat)

    # ── Transformations (henüz hiçbir şey hesaplanmadı) ──

    df = (
        df
        # Fiyatı 2 ondalıkla yuvarla
        .withColumn("price", F.round(F.col("price"), 2))

        # Fiyat kategorisi ekle (yeni sütun)
        .withColumn("price_tier",
            F.when(F.col("price") < 20,  F.lit("budget"))
             .when(F.col("price") < 100, F.lit("mid"))
             .otherwise(F.lit("premium"))
        )

        # Boş title olanları çıkar
        .filter(F.col("title").isNotNull())

        # Sadece gerekli sütunları tut
        .select("id", "title", "price", "price_tier", "category", "rating")
    )

    logger.info(f"Ürünler dönüştürüldü: {df.count()} satır")  # count() → ACTION, burada Spark çalışır
    return df


# ── Kullanıcı Dönüşümleri ──────────────────────────────────────────────────

def transform_users(raw_users: list, spark: SparkSession) -> DataFrame:
    """
    İç içe (nested) objeleri düzleştirerek DataFrame oluşturur.
    """
    flat = [
        {
            "id":    u.get("id"),
            "email": u.get("email"),
            "name":  f"{u.get('name', {}).get('firstname', '')} {u.get('name', {}).get('lastname', '')}".strip(),
            "city":  u.get("address", {}).get("city", "unknown"),
        }
        for u in raw_users if u.get("id")
    ]

    df = spark.createDataFrame(flat).filter(F.col("id").isNotNull())
    logger.info(f"Kullanıcılar dönüştürüldü: {df.count()} satır")
    return df


# ── Sipariş Dönüşümleri + JOIN ─────────────────────────────────────────────

def transform_orders(raw_orders: list, products_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Siparişlere ürün fiyatı JOIN ederek toplam tutarı hesaplar.

    JOIN türleri:
    - inner  → her iki tabloda da eşleşen kayıtlar (varsayılan)
    - left   → sol tablonun tamamı + sağdan eşleşenler
    - outer  → her iki tablonun tamamı
    """
    orders_df = spark.createDataFrame(raw_orders)

    # orders ← products JOIN (product_id = id üzerinden)
    enriched = (
        orders_df
        .join(
            products_df.select("id", "price", "title"),  # sadece gerekli sütunlar
            orders_df["product_id"] == products_df["id"],
            how="inner"
        )
        # Toplam tutar = fiyat × adet
        .withColumn("total_price", F.round(F.col("price") * F.col("quantity"), 2))
        .select("user_id", "product_id", "quantity", "total_price")
    )

    logger.info(f"Siparişler dönüştürüldü: {enriched.count()} satır")
    return enriched


# ── Analitik Özet ──────────────────────────────────────────────────────────

def compute_summary(orders_df: DataFrame, products_df: DataFrame) -> None:
    """
    Spark SQL ile kategori bazlı özet.
    Bu bir action — burada Spark gerçekten çalışır.
    """
    summary = (
        orders_df
        .join(products_df, orders_df["product_id"] == products_df["id"])
        .groupBy("category")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    logger.info("─── Kategori Özeti ───────────────────────────")
    summary.show(truncate=False)


# ── Pipeline'dan çağrılacak ana fonksiyon ──────────────────────────────────

def transform_all(raw_data: dict, spark: SparkSession) -> dict:
    products_df = transform_products(raw_data["products"], spark)
    users_df    = transform_users(raw_data["users"], spark)
    orders_df   = transform_orders(raw_data["orders"], products_df, spark)

    compute_summary(orders_df, products_df)

    return {
        "products": products_df,
        "users":    users_df,
        "orders":   orders_df,
    }
