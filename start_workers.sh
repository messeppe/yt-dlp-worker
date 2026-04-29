#!/bin/bash
# Start multiple concurrent worker processes.
# Each worker independently polls Postgres via FOR UPDATE SKIP LOCKED.
# Change WORKER_COUNT to scale up/down.

set -e

WORKER_COUNT=${WORKER_COUNT:-5}

log() {
    echo "[start-workers] $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log "Starting $WORKER_COUNT worker processes..."

pids=()
for i in $(seq 1 "$WORKER_COUNT"); do
    log "Spawning worker-$i"
    python -u /app/worker.py &
    pids+=($!)
done

log "All $WORKER_COUNT workers spawned. PIDs: ${pids[*]}"

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
