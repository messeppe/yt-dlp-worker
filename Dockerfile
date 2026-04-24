FROM python:3.11-slim

# Install ffmpeg and Node.js
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
        git \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/*

# Build bgutil POT server (server/ subdir has the lockfile)
RUN git clone --single-branch --branch 1.3.1 \
        https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /bgutil \
    && cd /bgutil/server \
    && npm ci \
    && npx tsc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .

# Start bgutil HTTP server on :4416, then the worker
CMD ["sh", "-c", "node /bgutil/server/build/index.js serve & sleep 3 && exec python -u worker.py"]
