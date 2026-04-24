FROM python:3.11-slim

# Install ffmpeg, Node.js, and git
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
        git \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get purge -y curl \
    && rm -rf /var/lib/apt/lists/*

# Build the bgutil JS side (required by the Python plugin)
RUN git clone https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /bgutil \
    && cd /bgutil \
    && npm install \
    && npx tsc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY worker.py .

# Start bgutil HTTP server on :4416, then start the worker
CMD ["sh", "-c", "node /bgutil/build/index.js serve & sleep 3 && exec python -u worker.py"]
