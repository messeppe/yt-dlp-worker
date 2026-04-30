import os
import signal
import threading
import time
import logging
from urllib.parse import urlparse, parse_qs

import requests
import psycopg2
import psycopg2.pool

DB_URL         = os.environ["SUPABASE_DB_URL"]
RAPIDAPI_KEY   = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_HOST  = os.environ["RAPIDAPI_HOST"]
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_VIDEO_QUALITY = int(os.environ.get("MAX_VIDEO_QUALITY", "720"))

H264_VIDEO_ITAGS = {160, 133, 134, 135, 136, 137, 264, 266}

_WORKER_ID = os.environ.get("WORKER_ID", "scout")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)-16s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(_WORKER_ID)

_shutdown = threading.Event()

def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current job then exiting")
    _shutdown.set()

signal.signal(signal.SIGTERM, handle_sigterm)

def poll_job(conn):
    """Claim one queued video atomically."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status = 'processing',
                media_locked_until = NOW() + INTERVAL '1 minute'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE media_status = 'queued'
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING v.id
            """
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0]
    return None

def get_streams(video_id: str):
    url = f"https://{RAPIDAPI_HOST}/download.php"
    resp = requests.get(
        url,
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST},
        params={"id": video_id},
        timeout=(10, 30),
    )
    resp.raise_for_status()
    data = resp.json()
    if "results" not in data:
        log.warning(f"[API-WARN] {video_id}: no 'results' key — status={data.get('status_code')}")
    return data.get("title", ""), data.get("results", [])

def pick_streams(results: list):
    def has_v(r): return r.get("has_video") or r.get("mime", "").startswith("video/")
    def has_a(r): return r.get("has_audio")
    def v_quality(r):
        try: return int("".join(c for c in r.get("quality", "") if c.isdigit()))
        except ValueError: return 0

    def get_itag(r):
        try: return int(parse_qs(urlparse(r.get("url", "")).query).get("itag", [0])[0])
        except Exception: return 0

    def best_video(streams):
        capped = [r for r in streams if v_quality(r) <= MAX_VIDEO_QUALITY]
        pool = capped if capped else sorted(streams, key=v_quality)[:1]
        h264 = [r for r in pool if get_itag(r) in H264_VIDEO_ITAGS]
        candidates = h264 if h264 else pool
        return sorted(candidates, key=v_quality, reverse=True)[0]

    combined = [r for r in results if has_v(r) and has_a(r)]
    if combined:
        return best_video(combined), None

    videos = [r for r in results if has_v(r) and not has_a(r)]
    audios = [r for r in results if has_a(r) and not has_v(r)]
    if not videos or not audios:
        return None, None

    return best_video(videos), sorted(audios, key=lambda r: r.get("quality", ""), reverse=True)[0]

def mark_ready(conn, video_id: str, video_url: str, audio_url: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'ready_for_download',
                   media_locked_until = NULL,
                   video_stream_url = %s,
                   audio_stream_url = %s,
                   stream_url_expires_at = NOW() + INTERVAL '4 hours'
               WHERE id = %s""",
            (video_url, audio_url, video_id),
        )
        conn.commit()

def mark_failed(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'failed',
                   media_locked_until = NULL,
                   media_last_error = %s,
                   media_retry_count = media_retry_count + 1
               WHERE id = %s""",
            (error[:500], video_id),
        )
        conn.commit()

def process(conn, video_id: str):
    last_error = "no attempts made"
    
    # Rate limit for RapidAPI (e.g. 2 req/sec)
    time.sleep(0.5)

    try:
        title, results = get_streams(video_id)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        last_error = f"RapidAPI error {status}"
        mark_failed(conn, video_id, last_error)
        log.warning(f"[FAIL] {video_id}: {last_error}")
        return
    except Exception as e:
        last_error = str(e)
        mark_failed(conn, video_id, last_error)
        log.warning(f"[FAIL] {video_id}: {last_error}")
        return

    if not results:
        mark_failed(conn, video_id, "RapidAPI returned empty results")
        return

    video_stream, audio_stream = pick_streams(results)
    if video_stream is None:
        mark_failed(conn, video_id, "no usable streams in RapidAPI")
        return

    v_url = video_stream["url"]
    a_url = audio_stream["url"] if audio_stream else None
    
    mark_ready(conn, video_id, v_url, a_url)
    log.info(f"[CACHED] {video_id} URLs extracted and ready for Mule")

def main():
    log.info("Scout started, polling Postgres for queued videos...")
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            break
        except Exception as e:
            log.error(f"DB connect failed: {e} — retrying in 10s")
            time.sleep(10)

    try:
        while not _shutdown.is_set():
            try:
                video_id = poll_job(conn)
            except Exception as e:
                log.error(f"Scout poll_job error: {e} — reconnecting")
                try: conn.close()
                except Exception: pass
                time.sleep(5)
                try: conn = psycopg2.connect(DB_URL)
                except Exception: pass
                continue

            if video_id:
                process(conn, video_id)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try: conn.close()
        except Exception: pass
        log.info("Scout shut down cleanly")

if __name__ == "__main__":
    main()
