# 🛒 E-Commerce Data Pipeline (Spark & Postgres)

Bu proje, bir e-ticaret platformunun sipariş verilerini PostgreSQL üzerinden Spark cluster'ına aktaran, işleyen ve analitik sonuçlar üreten uçtan uca (end-to-end) bir veri pipeline mimarisidir.

## 🏗️ Mimari Yapı

Sistem tamamen **Dockerized** bir yapıya sahiptir ve aşağıdaki bileşenleri içerir:

-   **PostgreSQL 15:** İşlem verilerinin (OLTP) tutulduğu ve başlangıçta otomatik şema kurulumu yapan ana veritabanı.
-   **Spark Cluster (Master & Worker):** Dağıtık veri işleme ve ETL operasyonlarını yöneten Bitnami tabanlı Spark 3.5 altyapısı.
-   **Python App:** Veritabanı bağlantılarını yöneten ve Spark işlerini (jobs) tetikleyen uygulama katmanı.

## 🚀 Hızlı Başlangıç

### 1. Hazırlık
Projenin çalışması için kök dizinde bir `.env` dosyası oluşturun ve veritabanı kimlik bilgilerini tanımlayın:

```env
DB_USER=admin
DB_PASS=securepassword123