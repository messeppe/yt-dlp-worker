FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .
COPY scout.py .
COPY subtitle_mule.py .
COPY start_workers.sh .
RUN chmod +x start_workers.sh

# --- Deployment Command ---
# Using start_workers.sh to run both Scout and Mule in the same container.
CMD ["./start_workers.sh"]
