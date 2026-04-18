# ── Python 3.11 slim = küçük image boyutu ──
FROM python:3.11-slim

WORKDIR /app

# Java gerekli çünkü PySpark arka planda JVM kullanır
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Önce sadece requirements kopyala → Docker layer cache'i verimli kullanır
# (kod değişirse sadece son layer yeniden build edilir)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodunu kopyala
COPY app/ .

CMD ["python", "main.py"]
