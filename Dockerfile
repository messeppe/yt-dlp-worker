FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY media_mule.py .
COPY scout.py .
COPY subtitle_mule.py .
COPY logging_setup.py .
COPY proxy_pool.py .
COPY start_workers.sh .
RUN chmod +x start_workers.sh

# --- Deployment Command ---
# All-in-one: start_workers.sh runs Scout + media mule(s) + subtitle mule(s).
CMD ["./start_workers.sh"]
