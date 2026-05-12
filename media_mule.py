import logging
import os
import signal
import threading
import time
from urllib.parse import urlparse

import boto3
import psycopg2
import psycopg2.pool
import requests
from logging_setup import setup_logging, log_event
from proxy_pool import build_pools, pick_pool

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
DB_URL = os.environ["SUPABASE_DB_URL"]
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_ATTEMPTS_PER_VIDEO = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "20"))
MAX_SCOUT_RETRIES = int(os.environ.get("MAX_SCOUT_RETRIES", "10"))
STREAM_MAX_RETRIES = int(os.environ.get("STREAM_MAX_RETRIES", "15"))
STREAM_READ_TIMEOUT = int(os.environ.get("STREAM_READ_TIMEOUT", "120"))

_proxy_pool, _proxy_pool_b = build_pools()

_WORKER_ID = os.environ.get("WORKER_ID", "media-mule")
log = setup_logging(_WORKER_ID)

_shutdown = threading.Event()


def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current job then exiting")
    _shutdown.set()


signal.signal(signal.SIGTERM, handle_sigterm)




def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def poll_job(conn):
    """Claim one job from media_queue atomically."""
    with conn.cursor() as cur:
        # Release stuck locks (crash recovery)
        cur.execute(
            """UPDATE youtube.media_queue
               SET locked_until = NULL
               WHERE locked_until IS NOT NULL AND locked_until < NOW()
                 AND url_expires_at > NOW()"""
        )
        # Delete expired URLs, reset videos to queued for re-scouting (cap at MAX_SCOUT_RETRIES)
        cur.execute(
            """DELETE FROM youtube.media_queue
               WHERE url_expires_at <= NOW()
               RETURNING video_id"""
        )
        expired = [r[0] for r in cur.fetchall()]
        if expired:
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status = CASE
                           WHEN scout_retry_count >= %s THEN 'failed'
                           ELSE 'queued'
                       END,
                       media_last_error = CASE
                           WHEN scout_retry_count >= %s
                           THEN 'URL expired too many times — mule too slow to download before TTL'
                           ELSE media_last_error
                       END,
                       scout_retry_count = CASE
                           WHEN scout_retry_count >= %s THEN scout_retry_count
                           ELSE scout_retry_count + 1
                       END
                   WHERE id = ANY(%s)""",
                (MAX_SCOUT_RETRIES, MAX_SCOUT_RETRIES, MAX_SCOUT_RETRIES, expired),
            )
        # Claim one job — soonest-to-expire first
        cur.execute(
            """UPDATE youtube.media_queue mq
               SET locked_until = NOW() + INTERVAL '5 minutes'
               WHERE mq.id = (
                   SELECT id FROM youtube.media_queue
                   WHERE locked_until IS NULL
                   ORDER BY url_expires_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING
                   mq.video_id,
                   mq.video_stream_url,
                   mq.audio_stream_url,
                   (SELECT COALESCE(NULLIF(c.handle,''), NULLIF(c.title,''), mq.video_id)
                    FROM youtube.videos v
                    JOIN youtube.channels c ON c.id = v.channel_id
                    WHERE v.id = mq.video_id) AS channel_handle,
                   (SELECT v.title FROM youtube.videos v WHERE v.id = mq.video_id)"""
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE youtube.videos SET media_status='processing' WHERE id=%s",
                (row[0],),
            )
            log_event(log, "info", "queue_claim", "Media queue job claimed", worker="media-mule", queue="youtube.media_queue", video_id=row[0])
        conn.commit()
    if row:
        return row[0], row[1], row[2], row[3] or "unknown", row[4]
    return None, None, None, None, None


def renew_lock(video_id: str, stop_event: threading.Event):
    """Background thread: extend media_queue locked_until every 60s using its own connection."""
    try:
        hb_conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
    except Exception as e:
        log.warning(f"[HEARTBEAT] {video_id}: connect failed: {e}")
        return
    try:
        while not stop_event.wait(60):
            try:
                with hb_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE youtube.media_queue SET locked_until = NOW() + INTERVAL '5 minutes' WHERE video_id = %s",
                        (video_id,),
                    )
                    hb_conn.commit()
            except Exception as e:
                log.warning(f"[HEARTBEAT] {video_id}: {e}")
    finally:
        try:
            hb_conn.close()
        except Exception:
            pass


def sanitize_filename(s: str) -> str:
    import re

    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120]


def sanitize_path_segment(s: str) -> str:
    import re

    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", "_", s).strip("._ ")
    return s[:80] or "unknown"


def infer_stream_ext(url: str, default_ext: str) -> str:
    """Infer extension directly from CDN url path."""
    try:
        path = urlparse(url).path
        tail = path.rsplit("/", 1)[-1]
        if "." in tail:
            ext = tail.rsplit(".", 1)[-1].lower()
            if ext and 1 <= len(ext) <= 8 and ext.isalnum():
                return ext
    except Exception:
        pass
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


def download_stream(url: str, dest: str, initial_proxy: dict = None,
                    video_id: str = "", worker_idx: str = "0", stream: str = "video"):
    start = time.time()
    last_log = start
    downloaded = os.path.getsize(dest) if os.path.exists(dest) else 0
    total = 0
    stream_name = os.path.basename(dest)
    pool = pick_pool(_proxy_pool, _proxy_pool_b)
    proxy_idx = pool.pick()
    proxy_rotations = 0

    for stream_attempt in range(1, STREAM_MAX_RETRIES + 1):
        proxies = pool.make_proxies(proxy_idx)
        headers = {"Accept-Encoding": "identity"}
        if downloaded > 0:
            headers["Range"] = f"bytes={downloaded}-"

        try:
            timeout = (15, STREAM_READ_TIMEOUT)
            with requests.get(
                url, proxies=proxies, headers=headers, stream=True, timeout=timeout
            ) as r:
                r.raise_for_status()

                if downloaded > 0 and r.status_code == 200:
                    log.warning(
                        f"[DOWNLOAD-RESUME-RESET] {stream_name} server ignored Range; restarting"
                    )
                    downloaded = 0
                    if os.path.exists(dest):
                        os.remove(dest)

                cl = int(r.headers.get("Content-Length", "0") or "0")
                if cl > 0:
                    total = downloaded + cl if r.status_code == 206 else cl
                else:
                    total = total or 0

                log.info(
                    f"[DOWNLOAD] {stream_name} proxy={proxy_idx} attempt={stream_attempt}/{STREAM_MAX_RETRIES} from={format_bytes(downloaded)}"
                )

                mode = "ab" if downloaded > 0 else "wb"
                with open(dest, mode) as f:
                    for chunk in r.iter_content(chunk_size=1048576):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_log >= 30.0:
                            elapsed = max(now - start, 0.001)
                            speed_mbps = round(downloaded / elapsed / 1_000_000, 3)
                            pct = round(min((downloaded / total) * 100, 100.0), 1) if total > 0 else 0.0
                            log_event(log, "info", "download_progress", "Downloading",
                                worker_idx=worker_idx, video_id=video_id, stream=stream,
                                downloaded_bytes=downloaded,
                                total_bytes=total if total > 0 else None,
                                pct=pct, speed_mbps=speed_mbps, proxy_idx=proxy_idx)
                            if total > 0:
                                log.info(
                                    f"[DOWNLOAD] {stream_name} {render_progress_bar(pct)} {pct:.1f}% "
                                    f"{format_bytes(downloaded)}/{format_bytes(total)} speed={format_bytes(speed_mbps * 1_000_000)}/s"
                                )
                            last_log = now

                if total == 0 or downloaded >= total:
                    break
                log.warning(
                    f"[DOWNLOAD-RETRY] {stream_name} ended cleanly early at {format_bytes(downloaded)}; retrying"
                )
                proxy_idx = pool.pick()
                proxy_rotations += 1

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 403 and stream_attempt < 3:
                sleep_s = min(2**stream_attempt, 15)
                log.warning(
                    f"[DOWNLOAD-403] {stream_name} proxy={proxy_idx} — may be IP block, swapping proxy, retrying in {sleep_s}s (attempt {stream_attempt}/{STREAM_MAX_RETRIES})"
                )
                pool.mark_failed(proxy_idx)
                time.sleep(sleep_s)
                pool = pick_pool(_proxy_pool, _proxy_pool_b)
                proxy_idx = pool.pick()
                proxy_rotations += 1
                continue
            raise
        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            if stream_attempt == STREAM_MAX_RETRIES:
                raise
            sleep_s = min(2**stream_attempt, 15)
            log.warning(
                f"[DOWNLOAD-CRASH] {stream_name} proxy={proxy_idx} died at {format_bytes(downloaded)} bytes: {e} — swapping proxy and backing off {sleep_s}s"
            )
            pool.mark_failed(proxy_idx, cooldown_secs=30)
            time.sleep(sleep_s)
            pool = pick_pool(_proxy_pool, _proxy_pool_b)
            proxy_idx = pool.pick()
            proxy_rotations += 1
            continue

    elapsed = max(time.time() - start, 0.001)
    if total > 0 and downloaded < total:
        raise RuntimeError(
            f"incomplete download after retries: got {downloaded} bytes, expected {total}"
        )
    log.info(
        f"[DOWNLOAD-DONE] {stream_name} {format_bytes(downloaded)} elapsed={elapsed:.1f}s"
    )
    return {
        "bytes": downloaded,
        "elapsed": elapsed,
        "speed_mbps": round(downloaded / elapsed / 1_000_000, 3),
        "proxy_idx": proxy_idx,
        "proxy_rotations": proxy_rotations,
    }


def mark_complete(conn, video_id: str, files: list):
    """files: list of (s3_path, file_size, media_type, mime_type)"""
    with conn.cursor() as cur:
        for s3_path, file_size, media_type, mime_type in files:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag, s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, %s, %s, 'auto', %s, %s, %s, 'rapidapi-cache')
                   ON CONFLICT DO NOTHING""",
                (
                    video_id,
                    media_type,
                    mime_type.split("/")[-1],
                    s3_path,
                    file_size,
                    mime_type,
                ),
            )
            log_event(log, "info", "db_write", "Inserted media_files row", worker="media-mule", table="youtube.media_files", video_id=video_id, media_type=media_type, s3_path=s3_path, file_size_bytes=file_size, mime_type=mime_type)
        cur.execute("DELETE FROM youtube.media_queue WHERE video_id = %s", (video_id,))
        log_event(log, "info", "queue_dequeue", "Media queue row removed", worker="media-mule", queue="youtube.media_queue", video_id=video_id, reason="completed")
        cur.execute(
            "UPDATE youtube.videos SET media_status='completed', scout_retry_count=0 WHERE id=%s",
            (video_id,),
        )
        log_event(log, "info", "db_write", "Updated media status completed", worker="media-mule", table="youtube.videos", video_id=video_id, media_status="completed")
        conn.commit()


def mark_failed(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM youtube.media_queue WHERE video_id = %s", (video_id,))
        log_event(log, "warning", "queue_dequeue", "Media queue row removed", worker="media-mule", queue="youtube.media_queue", video_id=video_id, reason="failed")
        cur.execute(
            """UPDATE youtube.videos
               SET media_status='failed',
                   media_last_error=%s,
                   media_retry_count=media_retry_count+1
               WHERE id=%s""",
            (error[:500], video_id),
        )
        log_event(log, "warning", "db_write", "Updated media status failed", worker="media-mule", table="youtube.videos", video_id=video_id, media_status="failed", error=error[:200])
        conn.commit()


def requeue_to_ready(conn, video_id: str) -> None:
    """Release lock in media_queue — row stays for next worker to claim."""
    with conn.cursor() as cur:
            cur.execute(
                "UPDATE youtube.media_queue SET locked_until=NULL WHERE video_id=%s",
                (video_id,),
            )
            cur.execute(
                "UPDATE youtube.videos SET media_status='ready_for_download' WHERE id=%s",
                (video_id,),
            )
            conn.commit()
    log_event(log, "info", "queue_requeue", "Media queue lock released", worker="media-mule", queue="youtube.media_queue", video_id=video_id, to_status="ready_for_download")


def process(
    conn, video_id: str, channel_handle: str, title: str, v_url: str, a_url: str
):
    import tempfile

    s3 = get_s3()
    safe_channel = sanitize_path_segment(channel_handle)
    safe_title = sanitize_filename(title) if title else video_id
    worker_idx = threading.current_thread().name.split('_')[-1] if '_' in threading.current_thread().name else '0'

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=renew_lock, args=(video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            uploaded = []
            if a_url is None:  # Combined
                ext = infer_stream_ext(v_url, "mp4")
                local = os.path.join(tmpdir, f"video.{ext}")
                log.info(f"[DOWNLOAD] single cached stream {video_id}")
                stats = download_stream(v_url, local, video_id=video_id, worker_idx=worker_idx, stream="video")
                log_event(log, "info", "download_complete", "Stream downloaded",
                    worker="media-mule", worker_idx=worker_idx, video_id=video_id, stream="video",
                    speed_mbps=stats["speed_mbps"], downloaded_bytes=stats["bytes"],
                    elapsed_seconds=round(stats["elapsed"], 2),
                    proxy_idx=stats["proxy_idx"], proxy_rotations=stats["proxy_rotations"])
                size = os.path.getsize(local)
                key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                s3.upload_file(local, S3_BUCKET, key)
                log_event(log, "info", "s3_upload", "Uploaded media to s3", worker="media-mule", video_id=video_id, s3_bucket=S3_BUCKET, s3_path=key, media_type="video")
                uploaded.append((key, size, "video", "video/mp4"))
            else:  # Separate
                vext = infer_stream_ext(v_url, "mp4")
                aext = infer_stream_ext(a_url, "m4a")
                vlocal = os.path.join(tmpdir, f"video.{vext}")
                alocal = os.path.join(tmpdir, f"audio.{aext}")

                log.info(f"[DOWNLOAD] video stream {video_id}")
                vstats = download_stream(v_url, vlocal, video_id=video_id, worker_idx=worker_idx, stream="video")
                log_event(log, "info", "download_complete", "Stream downloaded",
                    worker="media-mule", worker_idx=worker_idx, video_id=video_id, stream="video",
                    speed_mbps=vstats["speed_mbps"], downloaded_bytes=vstats["bytes"],
                    elapsed_seconds=round(vstats["elapsed"], 2),
                    proxy_idx=vstats["proxy_idx"], proxy_rotations=vstats["proxy_rotations"])
                log.info(f"[DOWNLOAD] audio stream {video_id}")
                astats = download_stream(a_url, alocal, video_id=video_id, worker_idx=worker_idx, stream="audio")
                log_event(log, "info", "download_complete", "Stream downloaded",
                    worker="media-mule", worker_idx=worker_idx, video_id=video_id, stream="audio",
                    speed_mbps=astats["speed_mbps"], downloaded_bytes=astats["bytes"],
                    elapsed_seconds=round(astats["elapsed"], 2),
                    proxy_idx=astats["proxy_idx"], proxy_rotations=astats["proxy_rotations"])

                vsize = os.path.getsize(vlocal)
                asize = os.path.getsize(alocal)
                vkey = f"youtube/{safe_channel}/{safe_title}_{video_id}_v.{vext}"
                akey = f"youtube/{safe_channel}/{safe_title}_{video_id}_a.{aext}"
                s3.upload_file(vlocal, S3_BUCKET, vkey)
                s3.upload_file(alocal, S3_BUCKET, akey)
                log_event(log, "info", "s3_upload", "Uploaded media to s3", worker="media-mule", video_id=video_id, s3_bucket=S3_BUCKET, s3_path=vkey, media_type="video")
                log_event(log, "info", "s3_upload", "Uploaded media to s3", worker="media-mule", video_id=video_id, s3_bucket=S3_BUCKET, s3_path=akey, media_type="audio")
                uploaded.append((vkey, vsize, "video", "video/mp4"))
                uploaded.append((akey, asize, "audio", "audio/m4a"))

            mark_complete(conn, video_id, uploaded)
            log.info(f"[SUCCESS] {video_id} uploaded {[u[0] for u in uploaded]}")

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status in (403, 404, 410):
            log.warning(f"[FAIL] {video_id}: HTTP {status} — URL expired or video gone")
            mark_failed(conn, video_id, f"HTTP {status} — URL expired or video gone")
        else:
            log.warning(f"[REQUEUE] {video_id}: HTTP {status} transient")
            requeue_to_ready(conn, video_id)
    except (ConnectionError, OSError, RuntimeError) as e:
        # ConnectionError = retry exhaustion from download_stream
        # RuntimeError = incomplete download after stream closed
        log.warning(f"[REQUEUE] {video_id}: {type(e).__name__} — {e}")
        requeue_to_ready(conn, video_id)
    except Exception as e:
        log.error(f"[FAIL] {video_id}: {e}", exc_info=True)
        mark_failed(conn, video_id, str(e))
    finally:
        stop_heartbeat.set()


def main():
    log.info("Mule started, pulling CDN URLs from DB cache...")
    while True:
        try:
            conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
            break
        except Exception as e:
            log.error(f"DB connect failed: {e} — retrying in 10s")
            time.sleep(10)

    try:
        while not _shutdown.is_set():
            try:
                vid, v_url, a_url, chan, t = poll_job(conn)
            except Exception as e:
                log.error(f"Mule poll error: {e}")
                time.sleep(5)
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
                except Exception:
                    pass
                continue

            if vid:
                try:
                    process(conn, vid, chan, t, v_url, a_url)
                except Exception as e:
                    log.error(f"Worker unhandled error for {vid}: {e}", exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    try:
                        conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
                    except Exception as ce:
                        log.error(f"Reconnect failed: {ce}")
                        time.sleep(5)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try:
            conn.close()
        except:
            pass
        log.info("Mule shut down cleanly")


if __name__ == "__main__":
    worker_count = int(os.environ.get("WORKER_COUNT", "1"))
    if worker_count <= 1:
        main()
    else:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(main) for _ in range(worker_count)]
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
