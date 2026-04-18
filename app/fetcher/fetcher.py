"""
fetcher.py — AsyncIO ile çoklu API'dan eş zamanlı veri çekme

TEMEL KAVRAMLAR:
─────────────────
• asyncio.gather()   → Görevleri AYNI ANDA başlatır, hepsi bitince döner
• asyncio.TaskGroup  → Python 3.11+, gather'ın daha güvenli hali
• aiohttp            → Non-blocking HTTP; thread bloke etmez
• Semaphore          → "Aynı anda max N istek" kısıtlaması
"""

import asyncio
import aiohttp
import logging
import random
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Ayarlar ────────────────────────────────────────────────────────────────

API_URLS = {
    "products": "https://fakestoreapi.com/products",
    "users":    "https://fakestoreapi.com/users",
}

TIMEOUT = aiohttp.ClientTimeout(total=10)  # 10 sn içinde cevap gelmezse hata fırlat

# Semaphore: aynı anda en fazla 5 eş zamanlı istek
# Bunu kaldırırsan API rate limit'e takılabilirsin
MAX_CONCURRENT = asyncio.Semaphore(5)


# ── Tek URL'den veri çeken temel fonksiyon ─────────────────────────────────

async def fetch_one(session: aiohttp.ClientSession, name: str, url: str) -> dict:
    """
    Tek bir API endpoint'inden veri çeker.

    async def → fonksiyon çalışırken event loop başka işleri yapabilir
    await      → "burada beklerken CPU'yu başkasına ver" demek
    """
    async with MAX_CONCURRENT:          # semaphore: slotu al
        try:
            logger.info(f"[{name}] istek gönderiliyor → {url}")

            async with session.get(url, timeout=TIMEOUT) as response:
                # response.json() da async çünkü body stream halinde gelir
                data = await response.json()

                logger.info(f"[{name}] ✓ {len(data)} kayıt alındı")
                return {"source": name, "data": data, "fetched_at": datetime.now().isoformat()}

        except asyncio.TimeoutError:
            # 10 sn içinde cevap gelmedi
            logger.error(f"[{name}] ✗ Timeout hatası")
            return {"source": name, "data": [], "error": "timeout"}

        except aiohttp.ClientError as e:
            # Bağlantı hatası, DNS çözümlenemedi vb.
            logger.error(f"[{name}] ✗ Bağlantı hatası: {e}")
            return {"source": name, "data": [], "error": str(e)}


# ── Sahte sipariş üretici (gerçek sipariş API'si olmadığı için) ────────────

def generate_fake_orders(user_ids: list, product_ids: list, count: int = 50) -> list:
    """
    Gerçek bir sipariş API'si olmadığında test verisi üretir.
    Üretim ortamında bunu gerçek API çağrısıyla değiştirirsin.
    """
    return [
        {
            "user_id":    random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "quantity":   random.randint(1, 5),
        }
        for _ in range(count)
    ]


# ── asyncio.gather() YOLU ──────────────────────────────────────────────────

async def fetch_all_with_gather() -> dict:
    """
    gather() → Tüm coroutine'leri AYNI ANDA başlatır.
    Biri hata verse diğerleri durur (return_exceptions=True ile önlenebilir).

    Sıralı çekseydin: 3 API × 1sn = 3sn
    gather ile:       hepsi aynı anda → ~1sn
    """
    async with aiohttp.ClientSession() as session:

        # Her coroutine bağımsız başlar, hepsi bitince sonuç döner
        results = await asyncio.gather(
            fetch_one(session, "products", API_URLS["products"]),
            fetch_one(session, "users",    API_URLS["users"]),
            return_exceptions=True   # bir tanesi patlasa bile diğerleri çalışır
        )

    # Hatalı sonuçları filtrele
    products_result = results[0]
    users_result    = results[1]

    products = products_result.get("data", []) if not isinstance(products_result, Exception) else []
    users    = users_result.get("data", [])    if not isinstance(users_result, Exception) else []

    # ID listelerini çıkar (sipariş üretimi için gerekli)
    user_ids    = [u["id"] for u in users]
    product_ids = [p["id"] for p in products]
    orders      = generate_fake_orders(user_ids, product_ids)

    logger.info(f"gather() tamamlandı: {len(products)} ürün, {len(users)} kullanıcı, {len(orders)} sipariş")

    return {"products": products, "users": users, "orders": orders}


# ── asyncio.TaskGroup() YOLU (Python 3.11+) ───────────────────────────────

async def fetch_all_with_taskgroup() -> dict:
    """
    TaskGroup → gather'ın daha modern ve güvenli hali.

    Fark: Bir task hata verirse diğer TÜM task'lar iptal edilir.
    Bu "ya hep ya hiç" semantiği bazı durumlar için daha güvenlidir.

    gather()    → hata izole edilebilir (return_exceptions=True)
    TaskGroup   → hata olunca grubun tamamı durur (daha strict)
    """
    results = {}

    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            # tg.create_task() → task'ı arka planda başlatır
            products_task = tg.create_task(fetch_one(session, "products", API_URLS["products"]))
            users_task    = tg.create_task(fetch_one(session, "users",    API_URLS["users"]))
        # Bu noktada tüm task'lar tamamlanmıştır (veya hepsi iptal edilmiştir)

    products = products_task.result().get("data", [])
    users    = users_task.result().get("data", [])

    user_ids    = [u["id"] for u in users]
    product_ids = [p["id"] for p in products]
    orders      = generate_fake_orders(user_ids, product_ids)

    logger.info(f"TaskGroup tamamlandı: {len(products)} ürün, {len(users)} kullanıcı")

    return {"products": products, "users": users, "orders": orders}


# ── Dışarıdan çağrılacak ana fonksiyon ────────────────────────────────────

async def fetch_all_data() -> dict:
    """
    Pipeline'ın geri kalanı bu fonksiyonu çağırır.
    gather veya taskgroup seçimini buradan değiştirebilirsin.
    """
    return await fetch_all_with_gather()
