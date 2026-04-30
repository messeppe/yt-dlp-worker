#!/bin/bash
# Start multiple concurrent worker processes.
# Each worker independently polls Postgres via FOR UPDATE SKIP LOCKED.
# Change WORKER_COUNT to scale up/down.

set -e

WORKER_COUNT=${WORKER_COUNT:-1}
SUBTITLE_WORKER_COUNT=${SUBTITLE_WORKER_COUNT:-1}

log() {
    echo "[start-workers] $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log "Starting $WORKER_COUNT media mule(s) and $SUBTITLE_WORKER_COUNT subtitle mule(s)..."

pids=()

log "Spawning 1 API Scout..."
WORKER_ID="scout" python -u /app/scout.py &
pids+=($!)

for i in $(seq 1 "$WORKER_COUNT"); do
    log "Spawning Media Mule (worker-$i)..."
    WORKER_ID="worker-$i" python -u /app/worker.py &
    pids+=($!)
done

for i in $(seq 1 "$SUBTITLE_WORKER_COUNT"); do
    log "Spawning Subtitle Mule (subtitle-mule-$i)..."
    WORKER_ID="subtitle-mule-$i" python -u /app/subtitle_mule.py &
    pids+=($!)
done

log "Scout, $WORKER_COUNT media mule(s), $SUBTITLE_WORKER_COUNT subtitle mule(s) spawned. PIDs: ${pids[*]}"

# Wait for any worker to exit, then kill the rest so Coolify restarts the container cleanly.
wait -n

log "A worker exited unexpectedly. Stopping remaining workers..."
for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
    fi
done
wait

log "All workers stopped. Exiting."
