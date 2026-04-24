FROM python:3.11-slim

# Install ffmpeg (for merging) and Node.js (for YouTube n-challenge / sig)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .

CMD ["sh", "-c", "npx --yes bgutil-ytdlp-pot-provider serve & sleep 3 && exec python -u worker.py"]
