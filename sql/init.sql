-- PostgreSQL ilk açılışta bu dosyayı çalıştırır
-- docker-entrypoint-initdb.d/ klasörüne mount edildi

CREATE TABLE IF NOT EXISTS products (
    id          INTEGER PRIMARY KEY,
    title       TEXT NOT NULL,
    price       NUMERIC(10,2),
    category    TEXT,
    rating      NUMERIC(3,2),
    fetched_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    city        TEXT,
    fetched_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id),
    product_id  INTEGER REFERENCES products(id),
    quantity    INTEGER,
    total_price NUMERIC(10,2),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Analiz için view: en çok satan ürünler
CREATE OR REPLACE VIEW top_products AS
SELECT
    p.title,
    p.category,
    SUM(o.quantity)    AS total_sold,
    SUM(o.total_price) AS total_revenue
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.title, p.category
ORDER BY total_revenue DESC;
