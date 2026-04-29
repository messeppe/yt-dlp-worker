import os
import signal
import threading
import time
import logging
import random
from urllib.parse import urlparse, parse_qs

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
PROXY_POOL_SIZE = int(os.environ.get("PROXY_POOL_SIZE", "4"))
MAX_ATTEMPTS_PER_VIDEO = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "20"))
STREAM_MAX_RETRIES = int(os.environ.get("STREAM_MAX_RETRIES", "8"))
STREAM_READ_TIMEOUT = int(os.environ.get("STREAM_READ_TIMEOUT", "120"))
MAX_VIDEO_QUALITY = int(os.environ.get("MAX_VIDEO_QUALITY", "720"))

# itags for H.264 (AVC) DASH video streams: 144p→2160p
H264_VIDEO_ITAGS = {160, 133, 134, 135, 136, 137, 264, 266}

WORKER_ID = os.environ.get("WORKER_ID", "main")

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{WORKER_ID}] %(levelname)s %(message)s",
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
                COALESCE(
                    (SELECT NULLIF(c.handle, '') FROM youtube.channels c WHERE c.id = v.channel_id),
                    (SELECT NULLIF(c.title, '') FROM youtube.channels c WHERE c.id = v.channel_id),
                    NULLIF(v.channel_id, ''),
                    'unknown'
                ) AS channel_handle
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


def sanitize_path_segment(s: str) -> str:
    """Sanitize folder/file path segments for S3 keys."""
    import re
    s = re.sub(r'[\\/*?:"<>|]', '', s or '')
    s = re.sub(r'\s+', '_', s).strip("._ ")
    return s[:80] or "unknown"


def sanitize_tag(s: str, default: str) -> str:
    import re
    t = re.sub(r"[^a-zA-Z0-9]+", "", str(s or "")).lower()
    return t[:24] or default


def infer_stream_ext(stream: dict, default_ext: str) -> str:
    """Infer extension from CDN URL path first, then MIME type, then default."""
    try:
        url = stream.get("url", "")
        path = urlparse(url).path
        tail = path.rsplit("/", 1)[-1]
        if "." in tail:
            ext = tail.rsplit(".", 1)[-1].lower()
            if ext and 1 <= len(ext) <= 8 and ext.isalnum():
                return ext
    except Exception:
        pass

    mime = stream.get("mime", "")
    quality = str(stream.get("quality", "")).lower()
    has_audio = bool(stream.get("has_audio"))
    has_video = bool(stream.get("has_video"))

    # RapidAPI often labels audio-only as audio/mp4 while quality hints M4A.
    if mime.startswith("audio/") and quality in ("m4a", "audio"):
        return "m4a"
    if has_audio and not has_video and quality == "m4a":
        return "m4a"

    if "/" in mime:
        ext = mime.split("/")[-1].split(";")[0].strip().lower()
        if ext:
            return ext
    return default_ext


def format_bytes(num: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(num)
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} TB"


def render_progress_bar(pct: float, width: int = 20) -> str:
    p = max(0.0, min(100.0, pct))
    filled = int((p / 100.0) * width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


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
    if "results" not in data:
        log.warning(
            f"[API-WARN] {video_id}: no 'results' key — "
            f"status={data.get('status_code')} message={data.get('message')!r}"
        )
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

    def get_itag(r):
        try:
            qs = parse_qs(urlparse(r.get("url", "")).query)
            return int(qs.get("itag", [0])[0])
        except Exception:
            return 0

    def best_video(streams):
        """Highest quality ≤ MAX_VIDEO_QUALITY, prefer H.264. Falls back to lowest above cap."""
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

    best_v = best_video(videos)
    best_a = sorted(audios, key=lambda r: r.get("quality", ""), reverse=True)[0]
    return best_v, best_a


def download_stream(url: str, dest: str, proxies: dict = None):
    start = time.time()
    last_log = start
    downloaded = os.path.getsize(dest) if os.path.exists(dest) else 0
    total = 0
    stream_name = os.path.basename(dest)
    next_pct_log = 5.0

    for stream_attempt in range(1, STREAM_MAX_RETRIES + 1):
        headers = {}
        if downloaded > 0:
            headers["Range"] = f"bytes={downloaded}-"

        try:
            timeout = (15, STREAM_READ_TIMEOUT)
            with requests.get(url, proxies=proxies, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()

                # If server doesn't honor range and returns full content, restart cleanly.
                if downloaded > 0 and r.status_code == 200:
                    log.warning(
                        f"[DOWNLOAD-RESUME-RESET] {stream_name} server ignored Range; restarting from 0"
                    )
                    downloaded = 0
                    if os.path.exists(dest):
                        os.remove(dest)

                content_length = int(r.headers.get("Content-Length", "0") or "0")
                if content_length > 0:
                    total = downloaded + content_length if r.status_code == 206 else content_length
                else:
                    total = total or 0

                if stream_attempt == 1:
                    if total > 0:
                        log.info(f"[DOWNLOAD-START] {stream_name} total={format_bytes(total)}")
                    else:
                        log.info(f"[DOWNLOAD-START] {stream_name} total=unknown")
                else:
                    if total > 0:
                        log.info(
                            f"[DOWNLOAD-RESUME] {stream_name} attempt={stream_attempt}/{STREAM_MAX_RETRIES} "
                            f"from={format_bytes(downloaded)} total={format_bytes(total)}"
                        )
                    else:
                        log.info(
                            f"[DOWNLOAD-RESUME] {stream_name} attempt={stream_attempt}/{STREAM_MAX_RETRIES} "
                            f"from={format_bytes(downloaded)} total=unknown"
                        )

                mode = "ab" if downloaded > 0 else "wb"
                with open(dest, mode) as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.time()
                        if now - last_log >= 15.0:
                            elapsed = max(now - start, 0.001)
                            speed = downloaded / elapsed
                            if total > 0:
                                pct = min((downloaded / total) * 100, 100.0)
                                # Log only on progress milestones (5%, 10%, ...).
                                if pct >= next_pct_log:
                                    bar = render_progress_bar(pct)
                                    log.info(
                                        f"[DOWNLOAD] {stream_name} {bar} {pct:.1f}% "
                                        f"{format_bytes(downloaded)}/{format_bytes(total)} "
                                        f"speed={format_bytes(speed)}/s elapsed={elapsed:.0f}s"
                                    )
                                    while next_pct_log <= pct:
                                        next_pct_log += 5.0
                            else:
                                # Unknown total size: print sparse heartbeat.
                                log.info(
                                    f"[DOWNLOAD] {stream_name} "
                                    f"{format_bytes(downloaded)} speed={format_bytes(speed)}/s elapsed={elapsed:.1f}s"
                                )
                            last_log = now

                # Finished this request successfully.
                if total == 0 or downloaded >= total:
                    break
                log.warning(
                    f"[DOWNLOAD-RETRY] {stream_name} ended early "
                    f"{format_bytes(downloaded)}/{format_bytes(total)}; retrying"
                )

        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            if stream_attempt == STREAM_MAX_RETRIES:
                raise
            sleep_s = min(2 ** stream_attempt, 15)
            log.warning(
                f"[DOWNLOAD-RETRY] {stream_name} attempt={stream_attempt}/{STREAM_MAX_RETRIES} "
                f"at={format_bytes(downloaded)} error={e} backoff={sleep_s}s"
            )
            time.sleep(sleep_s)
            continue

    elapsed = max(time.time() - start, 0.001)
    speed = downloaded / elapsed
    if total > 0 and downloaded < total:
        raise RuntimeError(
            f"incomplete download after retries: got {downloaded} bytes, expected {total}"
        )

    if total > 0:
        log.info(
            f"[DOWNLOAD-DONE] {stream_name} "
            f"{format_bytes(downloaded)}/{format_bytes(total)} (100.0%) "
            f"avg_speed={format_bytes(speed)}/s elapsed={elapsed:.1f}s"
        )
    else:
        log.info(
            f"[DOWNLOAD-DONE] {stream_name} "
            f"{format_bytes(downloaded)} avg_speed={format_bytes(speed)}/s elapsed={elapsed:.1f}s"
        )


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
    safe_channel = sanitize_path_segment(channel_handle)
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
            proxies = make_sticky_proxy(proxy_n)

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
                log.warning(f"[RETRY] {video_id}: {last_error} title={title!r}")
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
                        ext = infer_stream_ext(video_stream, "mp4")
                        local = os.path.join(tmpdir, f"video.{ext}")
                        log.info(f"[DOWNLOAD] combined stream {video_id}")
                        download_stream(video_stream["url"], local, proxies)
                        size = os.path.getsize(local)
                        safe_title = sanitize_filename(title) if title else video_id
                        key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                        s3.upload_file(local, S3_BUCKET, key)
                        uploaded.append((key, size, "video", video_stream.get("mime", "video/mp4")))
                    else:
                        # Separate video + audio
                        vext = infer_stream_ext(video_stream, "mp4")
                        aext = infer_stream_ext(audio_stream, "m4a")
                        vlocal = os.path.join(tmpdir, f"video.{vext}")
                        alocal = os.path.join(tmpdir, f"audio.{aext}")

                        log.info(f"[DOWNLOAD] video stream {video_id}")
                        download_stream(video_stream["url"], vlocal, proxies)
                        log.info(f"[DOWNLOAD] audio stream {video_id}")
                        download_stream(audio_stream["url"], alocal, proxies)

                        vsize = os.path.getsize(vlocal)
                        asize = os.path.getsize(alocal)
                        safe_title = sanitize_filename(title) if title else video_id
                        vkey = f"youtube/{safe_channel}/{safe_title}_{video_id}.{vext}"
                        akey = f"youtube/{safe_channel}/{safe_title}_{video_id}.{aext}"
                        if vkey == akey:
                            # Rare fallback: avoid overwrite without using "audio"/"video" labels.
                            vtag = sanitize_tag(video_stream.get("quality") or video_stream.get("itag"), "v")
                            atag = sanitize_tag(audio_stream.get("quality") or audio_stream.get("itag"), "a")
                            vkey = f"youtube/{safe_channel}/{safe_title}_{video_id}_{vtag}.{vext}"
                            akey = f"youtube/{safe_channel}/{safe_title}_{video_id}_{atag}.{aext}"
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
