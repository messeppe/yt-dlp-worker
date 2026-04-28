#!/bin/bash
set -e

# Start bgutil POT server in background
cd /bgutil && node server/build/server.js &
BGUTIL_PID=$!

# Wait for it to be ready
echo "Waiting for bgutil server..."
for i in $(seq 1 10); do
    if curl -sf http://localhost:4416/ >/dev/null 2>&1; then
        echo "bgutil server ready"
        break
    fi
    sleep 1
done

# Start Python worker (foreground)
exec python -u /app/worker_ytdlp.py
