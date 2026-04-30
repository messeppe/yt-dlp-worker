import os
import signal
import threading
import time
import logging
import random
from urllib.parse import urlparse

import requests
import boto3
import psycopg2
import psycopg2.pool

PROXY_URL      = os.environ["PROXY_URL"]
S3_ENDPOINT    = os.environ["S3_ENDPOINT"]
S3_BUCKET      = os.environ["S3_BUCKET"]
S3_ACCESS_KEY  = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY  = os.environ["S3_SECRET_KEY"]
DB_URL         = os.environ["SUPABASE_DB_URL"]
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "5"))
PROXY_POOL_SIZE = int(os.environ.get("PROXY_POOL_SIZE", "4"))
MAX_ATTEMPTS_PER_VIDEO = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "20"))
STREAM_MAX_RETRIES = int(os.environ.get("STREAM_MAX_RETRIES", "15"))
STREAM_READ_TIMEOUT = int(os.environ.get("STREAM_READ_TIMEOUT", "120"))

_WORKER_ID = os.environ.get("WORKER_ID", "mule")
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

def make_sticky_proxy(n: int) -> dict:
    url = PROXY_URL.replace("-rotate", f"-DE-{n}", 1) if "-rotate" in PROXY_URL else PROXY_URL
    return {"http": url, "https": url}

def get_s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

def poll_job(conn):
    """Claim one ready video atomically using media_locked_until."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status = 'processing',
                media_locked_until = NOW() + INTERVAL '5 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE media_status = 'ready_for_download'
                  AND stream_url_expires_at > NOW()
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                v.id,
                COALESCE(
                    (SELECT NULLIF(c.handle, '') FROM youtube.channels c WHERE c.id = v.channel_id),
                    (SELECT NULLIF(c.title, '') FROM youtube.channels c WHERE c.id = v.channel_id),
                    NULLIF(v.channel_id, ''),
                    'unknown'
                ) AS channel_handle,
                v.title,
                v.video_stream_url,
                v.audio_stream_url
            """
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0], row[1] or "unknown", row[2], row[3], row[4]
    return None, None, None, None, None

def renew_lock(conn, video_id: str, stop_event: threading.Event):
    """Background thread: extend media_locked_until every 60s."""
    while not stop_event.wait(60):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE youtube.videos SET media_locked_until = NOW() + INTERVAL '5 minutes' WHERE id = %s",
                    (video_id,),
                )
                conn.commit()
        except Exception as e:
            log.warning(f"[HEARTBEAT] {video_id}: {e}")

def sanitize_filename(s: str) -> str:
    import re
    s = re.sub(r'[\\/*?:"<>|]', '', s or '')
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:120]

def sanitize_path_segment(s: str) -> str:
    import re
    s = re.sub(r'[\\/*?:"<>|]', '', s or '')
    s = re.sub(r'\s+', '_', s).strip("._ ")
    return s[:80] or "unknown"

def infer_stream_ext(url: str, default_ext: str) -> str:
    """Infer extension directly from CDN url path."""
    try:
        path = urlparse(url).path
        tail = path.rsplit("/", 1)[-1]
        if "." in tail:
            ext = tail.rsplit(".", 1)[-1].lower()
            if ext and 1 <= len(ext) <= 8 and ext.isalnum(): return ext
    except Exception: pass
    return default_ext

def format_bytes(num: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(num)
    for u in units:
        if n < 1024 or u == units[-1]: return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} TB"

def render_progress_bar(pct: float, width: int = 20) -> str:
    p = max(0.0, min(100.0, pct))
    filled = int((p / 100.0) * width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"

def download_stream(url: str, dest: str, initial_proxy: dict = None):
    start = time.time()
    last_log = start
    downloaded = os.path.getsize(dest) if os.path.exists(dest) else 0
    total = 0
    stream_name = os.path.basename(dest)
    next_pct_log = 5.0
    proxy_idx = random.randint(1, max(PROXY_POOL_SIZE, 1))

    for stream_attempt in range(1, STREAM_MAX_RETRIES + 1):
        proxies = make_sticky_proxy(proxy_idx)
        headers = {}
        if downloaded > 0: headers["Range"] = f"bytes={downloaded}-"

        try:
            timeout = (15, STREAM_READ_TIMEOUT)
            with requests.get(url, proxies=proxies, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()

                if downloaded > 0 and r.status_code == 200:
                    log.warning(f"[DOWNLOAD-RESUME-RESET] {stream_name} server ignored Range; restarting")
                    downloaded = 0
                    if os.path.exists(dest): os.remove(dest)

                cl = int(r.headers.get("Content-Length", "0") or "0")
                if cl > 0: total = downloaded + cl if r.status_code == 206 else cl
                else: total = total or 0

                log.info(f"[DOWNLOAD] {stream_name} proxy={proxy_idx} attempt={stream_attempt}/{STREAM_MAX_RETRIES} from={format_bytes(downloaded)}")

                mode = "ab" if downloaded > 0 else "wb"
                with open(dest, mode) as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if not chunk: continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_log >= 15.0:
                            elapsed = max(now - start, 0.001)
                            speed = downloaded / elapsed
                            if total > 0:
                                pct = min((downloaded / total) * 100, 100.0)
                                if pct >= next_pct_log:
                                    log.info(f"[DOWNLOAD] {stream_name} {render_progress_bar(pct)} {pct:.1f}% {format_bytes(downloaded)}/{format_bytes(total)} speed={format_bytes(speed)}/s")
                                    while next_pct_log <= pct: next_pct_log += 5.0
                            last_log = now

                if total == 0 or downloaded >= total:
                    break
                log.warning(f"[DOWNLOAD-RETRY] {stream_name} ended cleanly early at {format_bytes(downloaded)}; retrying")
                proxy_idx = random.randint(1, max(PROXY_POOL_SIZE, 1))

        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            if stream_attempt == STREAM_MAX_RETRIES: raise
            sleep_s = min(2 ** stream_attempt, 15)
            log.warning(f"[DOWNLOAD-CRASH] {stream_name} proxy={proxy_idx} died at {format_bytes(downloaded)} bytes: {e} — swapping proxy and backing off {sleep_s}s")
            time.sleep(sleep_s)
            proxy_idx = random.randint(1, max(PROXY_POOL_SIZE, 1)) # Fresh proxy
            continue
            
    elapsed = max(time.time() - start, 0.001)
    if total > 0 and downloaded < total:
        raise RuntimeError(f"incomplete download after retries: got {downloaded} bytes, expected {total}")
    log.info(f"[DOWNLOAD-DONE] {stream_name} {format_bytes(downloaded)} elapsed={elapsed:.1f}s")
    return downloaded

def mark_complete(conn, video_id: str, files: list):
    """files: list of (s3_path, file_size, media_type, mime_type)"""
    with conn.cursor() as cur:
        for s3_path, file_size, media_type, mime_type in files:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag, s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, %s, %s, 'auto', %s, %s, %s, 'rapidapi-cache')
                   ON CONFLICT DO NOTHING""",
                (video_id, media_type, mime_type.split("/")[-1], s3_path, file_size, mime_type),
            )
        cur.execute("UPDATE youtube.videos SET media_status = 'completed', media_locked_until = NULL WHERE id = %s", (video_id,))
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

def process(conn, video_id: str, channel_handle: str, title: str, v_url: str, a_url: str):
    import tempfile
    
    s3 = get_s3()
    safe_channel = sanitize_path_segment(channel_handle)
    safe_title = sanitize_filename(title) if title else video_id

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(target=renew_lock, args=(conn, video_id, stop_heartbeat), daemon=True)
    heartbeat.start()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            uploaded = []
            if a_url is None: # Combined
                ext = infer_stream_ext(v_url, "mp4")
                local = os.path.join(tmpdir, f"video.{ext}")
                log.info(f"[DOWNLOAD] single cached stream {video_id}")
                download_stream(v_url, local)
                size = os.path.getsize(local)
                key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                s3.upload_file(local, S3_BUCKET, key)
                uploaded.append((key, size, "video", "video/mp4"))
            else: # Separate
                vext = infer_stream_ext(v_url, "mp4")
                aext = infer_stream_ext(a_url, "m4a")
                vlocal = os.path.join(tmpdir, f"video.{vext}")
                alocal = os.path.join(tmpdir, f"audio.{aext}")

                log.info(f"[DOWNLOAD] video stream {video_id}")
                download_stream(v_url, vlocal)
                log.info(f"[DOWNLOAD] audio stream {video_id}")
                download_stream(a_url, alocal)
                
                vsize = os.path.getsize(vlocal)
                asize = os.path.getsize(alocal)
                vkey = f"youtube/{safe_channel}/{safe_title}_{video_id}_v.{vext}"
                akey = f"youtube/{safe_channel}/{safe_title}_{video_id}_a.{aext}"
                s3.upload_file(vlocal, S3_BUCKET, vkey)
                s3.upload_file(alocal, S3_BUCKET, akey)
                uploaded.append((vkey, vsize, "video", "video/mp4"))
                uploaded.append((akey, asize, "audio", "audio/m4a"))
                
            mark_complete(conn, video_id, uploaded)
            log.info(f"[SUCCESS] {video_id} uploaded {[u[0] for u in uploaded]}")
            
    except Exception as e:
        log.error(f"[FAIL] {video_id}: caching or download error — {e}")
        mark_failed(conn, video_id, str(e))
    finally:
        stop_heartbeat.set()

def main():
    log.info("Mule started, pulling CDN URLs from DB cache...")
    while True:
        try: conn = psycopg2.connect(DB_URL); break
        except Exception as e:
            log.error(f"DB connect failed: {e} — retrying in 10s"); time.sleep(10)

    try:
        while not _shutdown.is_set():
            try:
                vid, chan, t, v_url, a_url = poll_job(conn)
            except Exception as e:
                log.error(f"Mule poll error: {e}"); time.sleep(5)
                try: conn.close()
                except Exception: pass
                try: conn = psycopg2.connect(DB_URL)
                except Exception: pass
                continue

            if vid:
                process(conn, vid, chan, t, v_url, a_url)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try: conn.close()
        except: pass
        log.info("Mule shut down cleanly")

if __name__ == "__main__":
    main()
