FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .
COPY start_workers.sh .
RUN chmod +x start_workers.sh

# --- REVERSIBLE: Single worker vs Multi-worker ---
# Uncomment ONE of the two CMD lines below.

# Single worker (original):
CMD ["python", "-u", "worker.py"]

# Multi-worker (5 concurrent processes):
# CMD ["./start_workers.sh"]
