import os
import signal
import threading
import time
import logging
import random

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
RAPIDAPI_KEY   = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_HOST  = os.environ["RAPIDAPI_HOST"]
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "5"))
PROXY_POOL_SIZE = int(os.environ.get("PROXY_POOL_SIZE", "100"))
MAX_ATTEMPTS_PER_VIDEO = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "20"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_shutdown = threading.Event()


def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current job then exiting")
    _shutdown.set()


signal.signal(signal.SIGTERM, handle_sigterm)


def make_sticky_proxy(n: int) -> dict:
    url = PROXY_URL.replace("-rotate", f"-DE-{n}", 1)
    return {"http": url, "https": url}


def make_numbered_proxy(n: int) -> dict:
    # Supports Webshare numbered usernames (e.g. rcqplwgm-1 ... rcqplwgm-100).
    if "-rotate" in PROXY_URL:
        url = PROXY_URL.replace("-rotate", f"-{n}", 1)
    else:
        # Fallback for manually configured non-rotate usernames.
        url = PROXY_URL
    return {"http": url, "https": url}


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def poll_job(conn):
    """Claim one queued video atomically. Returns (video_id, channel_handle) or (None, None)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status = 'processing',
                locked_until = NOW() + INTERVAL '5 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE media_status = 'queued'
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                v.id,
                (SELECT COALESCE(c.handle, c.title)
                 FROM youtube.channels c WHERE c.id = v.channel_id) AS channel_handle
            """
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0], row[1] or "unknown"
    return None, None


def renew_lock(conn, video_id: str, stop_event: threading.Event):
    """Background thread: extend locked_until every 60s until stop_event is set."""
    while not stop_event.wait(60):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE youtube.videos SET locked_until = NOW() + INTERVAL '5 minutes' WHERE id = %s",
                    (video_id,),
                )
                conn.commit()
        except Exception as e:
            log.warning(f"[HEARTBEAT] {video_id}: {e}")


def sanitize_filename(s: str) -> str:
    """Strip unsafe chars and limit length for S3 keys."""
    import re
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:120]


def get_streams(video_id: str, proxies: dict):
    """Returns (title, results) from RapidAPI download endpoint."""
    url = f"https://{RAPIDAPI_HOST}/download.php"
    resp = requests.get(
        url,
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST},
        params={"id": video_id},
        proxies=proxies,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("title", ""), data.get("results", [])


def pick_streams(results: list):
    """Return (combined_stream, None) or (video_stream, audio_stream)."""
    def has_v(r):
        return r.get("has_video") or r.get("mime", "").startswith("video/")
    
    def has_a(r):
        return r.get("has_audio")

    def v_quality(r):
        q = r.get("quality", "")
        try:
            return int("".join(c for c in q if c.isdigit()))
        except ValueError:
            return 0

    combined = [r for r in results if has_v(r) and has_a(r)]
    if combined:
        best = sorted(combined, key=v_quality, reverse=True)[0]
        return best, None

    videos = [r for r in results if has_v(r) and not has_a(r)]
    audios = [r for r in results if has_a(r) and not has_v(r)]
    if not videos or not audios:
        return None, None

    best_v = sorted(videos, key=v_quality, reverse=True)[0]
    best_a = sorted(audios, key=lambda r: r.get("quality", ""), reverse=True)[0]
    return best_v, best_a


def download_stream(url: str, dest: str, proxies: dict = None):
    with requests.get(url, proxies=proxies, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)


def mark_complete(conn, video_id: str, files: list):
    """files: list of (s3_path, file_size, media_type, mime_type)"""
    with conn.cursor() as cur:
        for s3_path, file_size, media_type, mime_type in files:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag,
                      s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, %s, %s, 'auto', %s, %s, %s, 'rapidapi')
                   ON CONFLICT DO NOTHING""",
                (video_id, media_type, mime_type.split("/")[-1], s3_path, file_size, mime_type),
            )
        cur.execute(
            "UPDATE youtube.videos SET media_status = 'completed', locked_until = NULL WHERE id = %s",
            (video_id,),
        )
        conn.commit()


def mark_failed(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'failed',
                   locked_until = NULL,
                   media_last_error = %s,
                   media_retry_count = media_retry_count + 1
               WHERE id = %s""",
            (error[:500], video_id),
        )
        conn.commit()


def reset_to_queued(conn, video_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE youtube.videos SET media_status = 'queued', locked_until = NULL WHERE id = %s",
            (video_id,),
        )
        conn.commit()


def process(conn, video_id: str, channel_handle: str):
    import tempfile

    s3 = get_s3()
    used_proxies = set()
    last_error = "no attempts made"

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=renew_lock, args=(conn, video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        pool_size = max(PROXY_POOL_SIZE, 1)
        max_attempts = max(1, min(MAX_ATTEMPTS_PER_VIDEO, pool_size))

        for attempt in range(1, max_attempts + 1):
            # Pick a random proxy not used yet for this video.
            available = [n for n in range(1, pool_size + 1) if n not in used_proxies]
            if not available:
                break
            proxy_n = random.choice(available)
            used_proxies.add(proxy_n)
            proxies = make_numbered_proxy(proxy_n)

            log.info(f"[ATTEMPT {attempt}/{max_attempts}] {video_id} proxy={proxy_n}")

            # Rate-limit: ensure max 2 req/s to RapidAPI
            time.sleep(0.5)

            try:
                title, results = get_streams(video_id, proxies)
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if status in (400, 404):
                    last_error = f"RapidAPI permanent error {status}"
                    log.error(f"[FAIL-PERM] {video_id}: {last_error}")
                    mark_failed(conn, video_id, last_error)
                    return
                if status == 429:
                    log.warning(f"[RATE-LIMIT] {video_id}: backing off 60s")
                    time.sleep(60)
                    used_proxies.discard(proxy_n)  # don't count 429 as a proxy failure
                    continue
                last_error = f"RapidAPI HTTP {status}"
                log.warning(f"[RETRY] {video_id}: {last_error}")
                continue
            except Exception as e:
                last_error = str(e)
                log.warning(f"[RETRY] {video_id}: RapidAPI error: {e}")
                continue

            if not results:
                last_error = "RapidAPI returned empty results"
                log.warning(f"[RETRY] {video_id}: {last_error}")
                continue

            video_stream, audio_stream = pick_streams(results)
            if video_stream is None:
                last_error = "no usable streams in RapidAPI results"
                log.warning(f"[RETRY] {video_id}: {last_error}")
                continue

            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    uploaded = []

                    if audio_stream is None:
                        # Combined stream
                        ext = video_stream.get("mime", "video/mp4").split("/")[-1]
                        local = os.path.join(tmpdir, f"video.{ext}")
                        log.info(f"[DOWNLOAD] combined stream {video_id}")
                        download_stream(video_stream["url"], local, proxies)
                        size = os.path.getsize(local)
                        safe_title = sanitize_filename(title) if title else video_id
                        key = f"youtube/{channel_handle}/{safe_title}_{video_id}.{ext}"
                        s3.upload_file(local, S3_BUCKET, key)
                        uploaded.append((key, size, "video", video_stream.get("mime", "video/mp4")))
                    else:
                        # Separate video + audio
                        vext = video_stream.get("mime", "video/mp4").split("/")[-1]
                        aext = audio_stream.get("mime", "audio/m4a").split("/")[-1]
                        vlocal = os.path.join(tmpdir, f"video.{vext}")
                        alocal = os.path.join(tmpdir, f"audio.{aext}")

                        log.info(f"[DOWNLOAD] video stream {video_id}")
                        download_stream(video_stream["url"], vlocal, proxies)
                        log.info(f"[DOWNLOAD] audio stream {video_id}")
                        download_stream(audio_stream["url"], alocal, proxies)

                        vsize = os.path.getsize(vlocal)
                        asize = os.path.getsize(alocal)
                        safe_title = sanitize_filename(title) if title else video_id
                        vkey = f"youtube/{channel_handle}/{safe_title}_{video_id}.video.{vext}"
                        akey = f"youtube/{channel_handle}/{safe_title}_{video_id}.audio.{aext}"
                        s3.upload_file(vlocal, S3_BUCKET, vkey)
                        s3.upload_file(alocal, S3_BUCKET, akey)
                        uploaded.append((vkey, vsize, "video", video_stream.get("mime", "video/mp4")))
                        uploaded.append((akey, asize, "audio", audio_stream.get("mime", "audio/m4a")))

                    mark_complete(conn, video_id, uploaded)
                    log.info(f"[SUCCESS] {video_id} files={[u[0] for u in uploaded]}")
                    return

            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                last_error = f"CDN HTTP {status}"
                log.warning(f"[RETRY] {video_id}: {last_error}")
            except Exception as e:
                last_error = str(e)
                log.warning(f"[RETRY] {video_id}: download/upload error: {e}")

        log.error(f"[FAIL] {video_id}: all attempts exhausted — {last_error}")
        mark_failed(conn, video_id, last_error)

    finally:
        stop_heartbeat.set()


def main():
    log.info("Worker started, polling Postgres for queued videos...")

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
                video_id, channel_handle = poll_job(conn)
            except Exception as e:
                log.error(f"poll_job error: {e} — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(5)
                try:
                    conn = psycopg2.connect(DB_URL)
                except Exception:
                    pass
                continue

            if video_id:
                process(conn, video_id, channel_handle)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log.info("Worker shut down cleanly")


if __name__ == "__main__":
    main()
