FROM python:3.11-slim

# Install ffmpeg, Node.js, and git
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/*

# Install bgutil PO token server (pre-built npm package)
RUN npm install -g bgutil-ytdlp-pot-provider

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .

# Start bgutil HTTP server on :4416, then start the worker
CMD ["sh", "-c", "bgutil-ytdlp-pot-provider serve & sleep 3 && exec python -u worker.py"]
