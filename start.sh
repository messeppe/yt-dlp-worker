#!/bin/bash
set -euo pipefail

if [ "${ENABLE_BGUTIL:-0}" = "1" ]; then
  echo "Starting bgutil POT server on 127.0.0.1:4416..."
  (cd /bgutil/server && node build/main.js) &
  for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:4416/" >/dev/null 2>&1 \
       || curl -sf "http://127.0.0.1:4416/ping" >/dev/null 2>&1; then
      echo "bgutil server ready"
      break
    fi
    sleep 1
  done
else
  echo "ENABLE_BGUTIL=0 — skipping PO provider (try android_vr / default clients first)"
fi

echo "JS runtime: $(deno --version 2>/dev/null | head -1 || echo missing)"
echo "yt-dlp: $(yt-dlp --version)"

exec python -u /app/worker_ytdlp.py
