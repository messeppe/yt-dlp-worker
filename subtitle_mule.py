import logging
import os
import random
import re
import signal
import tempfile
import threading
import time

import boto3
import psycopg2
import requests
from logging_setup import setup_logging, log_event

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
DB_URL = os.environ["SUPABASE_DB_URL"]
PROXY_URL = os.environ["PROXY_URL"]
PROXY_POOL_SIZE = int(os.environ.get("PROXY_POOL_SIZE", "100"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "10"))
MAX_SCOUT_RETRIES = int(os.environ.get("MAX_SCOUT_RETRIES", "10"))

_WORKER_ID = os.environ.get("WORKER_ID", "subtitle-mule")
log = setup_logging(_WORKER_ID)

_shutdown = threading.Event()


def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current job then exiting")
    _shutdown.set()


signal.signal(signal.SIGTERM, handle_sigterm)


def make_sticky_proxy(n: int) -> dict:
    url = (
        PROXY_URL.replace("-rotate", f"-{n}", 1)
        if "-rotate" in PROXY_URL
        else PROXY_URL
    )
    return {"http": url, "https": url}


def sanitize_filename(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120]


def sanitize_path_segment(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", "_", s).strip("._ ")
    return s[:80] or "unknown"


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def poll_job(conn):
    row = None
    with conn.cursor() as cur:
        # Release stuck locks (crash recovery)
        cur.execute(
            """UPDATE youtube.subtitle_queue
               SET locked_until = NULL
               WHERE locked_until IS NOT NULL AND locked_until < NOW()
                 AND url_expires_at > NOW()"""
        )
        # Delete expired URLs, reset videos to pending for re-scouting (cap at MAX_SCOUT_RETRIES)
        cur.execute(
            """DELETE FROM youtube.subtitle_queue
               WHERE url_expires_at <= NOW()
               RETURNING video_id"""
        )
        expired = [r[0] for r in cur.fetchall()]
        if expired:
            cur.execute(
                """UPDATE youtube.videos
                   SET subtitle_status = CASE
                           WHEN subtitle_scout_retry_count >= %s THEN 'failed'
                           ELSE 'pending'
                       END,
                       subtitle_last_error = CASE
                           WHEN subtitle_scout_retry_count >= %s
                           THEN 'Subtitle URL expired too many times'
                           ELSE subtitle_last_error
                       END,
                       subtitle_scout_retry_count = CASE
                           WHEN subtitle_scout_retry_count >= %s THEN subtitle_scout_retry_count
                           ELSE subtitle_scout_retry_count + 1
                       END
                   WHERE id = ANY(%s)""",
                (MAX_SCOUT_RETRIES, MAX_SCOUT_RETRIES, MAX_SCOUT_RETRIES, expired),
            )
        # Claim one job — soonest-to-expire first
        cur.execute(
            """UPDATE youtube.subtitle_queue sq
               SET locked_until = NOW() + INTERVAL '5 minutes'
               WHERE sq.id = (
                   SELECT id FROM youtube.subtitle_queue
                   WHERE locked_until IS NULL
                   ORDER BY url_expires_at ASC
                   LIMIT 1
                   FOR UPDATE SKIP LOCKED
               )
               RETURNING
                   sq.video_id,
                   sq.payload,
                   (SELECT COALESCE(NULLIF(c.handle,''), NULLIF(c.title,''), sq.video_id)
                    FROM youtube.videos v
                    JOIN youtube.channels c ON c.id = v.channel_id
                    WHERE v.id = sq.video_id) AS channel_handle,
                   (SELECT v.title FROM youtube.videos v WHERE v.id = sq.video_id)"""
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE youtube.videos SET subtitle_status='processing' WHERE id=%s",
                (row[0],),
            )
            log_event(log, "info", "queue_claim", "Subtitle queue job claimed", worker="subtitle-mule", queue="youtube.subtitle_queue", video_id=row[0])
        conn.commit()
    if row:
        return row[0], row[1], row[2] or "unknown", row[3]
    return None, None, None, None


def renew_lock(video_id: str, stop_event: threading.Event):
    """Background thread: extend subtitle_queue locked_until every 60s using its own connection."""
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
                        "UPDATE youtube.subtitle_queue SET locked_until = NOW() + INTERVAL '5 minutes' WHERE video_id = %s",
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


def mark_complete(conn, video_id: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM youtube.subtitle_queue WHERE video_id = %s", (video_id,))
        log_event(log, "info", "queue_dequeue", "Subtitle queue row removed", worker="subtitle-mule", queue="youtube.subtitle_queue", video_id=video_id, reason="completed")
        cur.execute(
            "UPDATE youtube.videos SET subtitle_status='completed', subtitle_scout_retry_count=0 WHERE id=%s",
            (video_id,),
        )
        log_event(log, "info", "db_write", "Updated subtitle status completed", worker="subtitle-mule", table="youtube.videos", video_id=video_id, subtitle_status="completed")
        conn.commit()


def mark_failed(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM youtube.subtitle_queue WHERE video_id = %s", (video_id,))
        log_event(log, "warning", "queue_dequeue", "Subtitle queue row removed", worker="subtitle-mule", queue="youtube.subtitle_queue", video_id=video_id, reason="failed")
        cur.execute(
            """UPDATE youtube.videos
            SET subtitle_status='failed',
                subtitle_last_error=%s, subtitle_retry_count=subtitle_retry_count+1
            WHERE id=%s""",
            (error[:500], video_id),
        )
        log_event(log, "warning", "db_write", "Updated subtitle status failed", worker="subtitle-mule", table="youtube.videos", video_id=video_id, subtitle_status="failed", error=error[:200])
        conn.commit()


def requeue_to_queued(conn, video_id: str) -> None:
    """Release lock in subtitle_queue — row stays for next mule to claim."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE youtube.subtitle_queue SET locked_until=NULL WHERE video_id=%s",
            (video_id,),
        )
        cur.execute(
            "UPDATE youtube.videos SET subtitle_status='queued' WHERE id=%s",
            (video_id,),
        )
        conn.commit()
    log_event(log, "info", "queue_requeue", "Subtitle queue lock released", worker="subtitle-mule", queue="youtube.subtitle_queue", video_id=video_id, to_status="queued")


def upsert_subtitle(conn, video_id, language_code, is_automated, content, s3_path):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO youtube.subtitles (video_id, language_code, is_automated, content, s3_path)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_id, language_code) DO UPDATE SET
                is_automated = EXCLUDED.is_automated,
                content      = EXCLUDED.content,
                s3_path      = EXCLUDED.s3_path
        """,
            (video_id, language_code, is_automated, content, s3_path),
        )
        log_event(log, "info", "db_write", "Upserted subtitle row", worker="subtitle-mule", table="youtube.subtitles", video_id=video_id, language_code=language_code, is_automated=bool(is_automated), s3_path=s3_path, content_bytes=len(content.encode("utf-8")))
        conn.commit()


def _lang_from_url(url: str) -> str:
    m = re.search(r'[?&]lang=([^&]+)', url)
    return m.group(1) if m else ""


def extract_target_tracks(payload: dict) -> list:
    # Manual subtitles take priority (any language, assumed native)
    for track in payload.get("subtitle", []):
        url = track.get("url", "")
        if url:
            lang = track.get("language_code", "") or _lang_from_url(url)
            return [{"language_code": lang, "url": url, "is_automated": False}]
    # Native ASR — skip tlang= translation tracks
    for track in payload.get("automated_subtitle", []):
        url = track.get("url", "")
        if url and "tlang=" not in url:
            lang = track.get("language_code", "") or _lang_from_url(url)
            return [{"language_code": lang, "url": url, "is_automated": True}]
    return []


def vtt_url(url: str) -> str:
    """Replace fmt=json3 with fmt=vtt to get WebVTT directly."""
    return re.sub(r"\bfmt=json3\b", "fmt=vtt", url)


def download_vtt(url: str, video_id: str = "", lang: str = "") -> str:
    """Download VTT subtitle with retry, proxy rotation, and exponential backoff.

    Matches the media mule's download_stream retry strategy:
    - Retries on 429 (rate-limit) and transient connection errors
    - Swaps to a fresh random proxy on each attempt
    - Exponential backoff: min(2**attempt, 15) seconds
    - Gives up after MAX_RETRIES attempts
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        proxy_idx = random.randint(1, max(PROXY_POOL_SIZE, 1))
        proxies = make_sticky_proxy(proxy_idx)
        log.info(
            f"[DOWNLOAD] {video_id} lang={lang} proxy={proxy_idx} attempt={attempt}/{MAX_RETRIES}"
        )
        try:
            start = time.time()
            resp = requests.get(url, proxies=proxies, timeout=(10, 30))
            resp.raise_for_status()
            content = resp.text
            elapsed = time.time() - start
            size = len(content.encode("utf-8"))
            log.info(
                f"[DOWNLOAD-DONE] {video_id} lang={lang} {size} bytes elapsed={elapsed:.1f}s"
            )
            if not content or not content.strip():
                raise ValueError("VTT response was empty")
            if not content.lstrip().startswith("WEBVTT"):
                raise ValueError(
                    f"response is not WebVTT (starts with: {content[:80]!r})"
                )
            return content
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            last_error = e
            if status == 429:
                sleep_s = min(2**attempt, 15)
                log.warning(
                    f"[DOWNLOAD-RETRY] {video_id} lang={lang} proxy={proxy_idx} 429 rate-limited — backing off {sleep_s}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(sleep_s)
                continue
            elif status >= 500:
                sleep_s = min(2**attempt, 15)
                log.warning(
                    f"[DOWNLOAD-RETRY] {video_id} lang={lang} proxy={proxy_idx} server error {status} — backing off {sleep_s}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(sleep_s)
                continue
            else:
                log.warning(
                    f"[DOWNLOAD-FAIL] {video_id} lang={lang} proxy={proxy_idx} HTTP {status}: {e}"
                )
                raise
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            sleep_s = min(2**attempt, 15)
            log.warning(
                f"[DOWNLOAD-RETRY] {video_id} lang={lang} proxy={proxy_idx} connection error: {e} — backing off {sleep_s}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(sleep_s)
            continue
    raise ConnectionError(
        f"failed to download VTT after {MAX_RETRIES} attempts: {last_error}"
    )


def process(conn, video_id, payload, channel_handle, title):
    s3 = get_s3()
    safe_channel = sanitize_path_segment(channel_handle)
    safe_title = sanitize_filename(title) if title else video_id

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=renew_lock, args=(video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        if not payload:
            raise ValueError("subtitle_raw_payload is null")

        # Normalize: old payloads stored full API response {status, results{...}},
        # new ones store the results dict directly {subtitle, automated_subtitle}.
        if "subtitle" not in payload and "automated_subtitle" not in payload:
            payload = payload.get("results") or payload

        tracks = extract_target_tracks(payload)
        if not tracks:
            raise ValueError("no native ASR track found")

        log.info(
            f"[START] {video_id} — {len(tracks)} track(s): {[t['language_code'] for t in tracks]}"
        )
        success_count = 0
        transient_fail_count = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            for track in tracks:
                lang = track["language_code"] or "unknown"
                try:
                    content = download_vtt(
                        vtt_url(track["url"]), video_id=video_id, lang=lang
                    )

                    local = os.path.join(tmpdir, f"{video_id}_{lang}.vtt")
                    with open(local, "w", encoding="utf-8") as f:
                        f.write(content)

                    s3_key = (
                        f"youtube/{safe_channel}/{safe_title}_{video_id}_{lang}.vtt"
                    )
                    s3.upload_file(
                        local, S3_BUCKET, s3_key, ExtraArgs={"ContentType": "text/vtt"}
                    )
                    log_event(log, "info", "s3_upload", "Uploaded subtitle to s3", worker="subtitle-mule", video_id=video_id, lang=lang, s3_bucket=S3_BUCKET, s3_path=s3_key)

                    upsert_subtitle(
                        conn, video_id, lang, track["is_automated"], content, s3_key
                    )
                    log.info(f"[DB] {video_id} lang={lang} upserted")
                    success_count += 1
                except ConnectionError as e:
                    # Retry exhaustion — proxy/network down, not video's fault
                    transient_fail_count += 1
                    log.warning(f"[SKIP-TRANSIENT] {video_id} lang={lang}: {e}")
                    continue
                except Exception as e:
                    log.warning(f"[SKIP] {video_id} lang={lang}: {e}")
                    continue

        if success_count > 0:
            mark_complete(conn, video_id)
            log.info(
                f"[SUCCESS] {video_id} subtitle_status=completed — {success_count}/{len(tracks)} track(s) saved"
            )
        elif transient_fail_count == len(tracks):
            requeue_to_queued(conn, video_id)
            log.warning(
                f"[TRANSIENT-ALL] {video_id}: all {len(tracks)} track(s) failed transiently — requeued"
            )
        else:
            mark_failed(conn, video_id, f"all {len(tracks)} track(s) failed to process")
            log.error(
                f"[NONE] {video_id}: 0/{len(tracks)} track(s) saved — marking as failed"
            )

    except Exception as e:
        log.error(f"[FAIL] {video_id}: {e}")
        mark_failed(conn, video_id, str(e))
    finally:
        stop_heartbeat.set()


def main():
    log.info(
        "Subtitle Mule started — native ASR mode, polling Postgres..."
    )
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
                video_id, payload, channel_handle, title = poll_job(conn)
            except Exception as e:
                log.error(f"Subtitle Mule poll error: {e} — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(5)
                try:
                    conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
                except Exception:
                    pass
                continue

            if video_id:
                log.info(f"[CLAIMED] {video_id} channel={channel_handle}")
                try:
                    process(conn, video_id, payload, channel_handle, title)
                except Exception as e:
                    log.error(f"Worker unhandled error for {video_id}: {e}", exc_info=True)
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
        log.info("Subtitle Mule shut down cleanly")


if __name__ == "__main__":
    worker_count = int(os.environ.get("SUBTITLE_WORKER_COUNT", "1"))
    if worker_count <= 1:
        main()
    else:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(main) for _ in range(worker_count)]
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
