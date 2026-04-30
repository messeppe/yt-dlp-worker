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

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
DB_URL = os.environ["SUPABASE_DB_URL"]
PROXY_URL = os.environ["PROXY_URL"]
PROXY_POOL_SIZE = int(os.environ.get("PROXY_POOL_SIZE", "100"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "10"))
SUBTITLE_LANGS = [
    l.strip() for l in os.environ.get("SUBTITLE_LANGS", "id,ar").split(",") if l.strip()
]

_WORKER_ID = os.environ.get("WORKER_ID", "subtitle-mule")
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
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE youtube.videos v
            SET subtitle_status = 'processing',
                subtitle_locked_until = NOW() + INTERVAL '5 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE subtitle_status = 'queued'
                  AND subtitle_url_expires_at > NOW()
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                v.id,
                v.subtitle_raw_payload,
                COALESCE(
                    (SELECT NULLIF(c.handle,'') FROM youtube.channels c WHERE c.id = v.channel_id),
                    (SELECT NULLIF(c.title,'')  FROM youtube.channels c WHERE c.id = v.channel_id),
                    NULLIF(v.channel_id,''),
                    'unknown'
                ) AS channel_handle,
                v.title
        """)
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0], row[1], row[2] or "unknown", row[3]
    return None, None, None, None


def renew_lock(conn, video_id: str, stop_event: threading.Event):
    while not stop_event.wait(60):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE youtube.videos SET subtitle_locked_until = NOW() + INTERVAL '5 minutes' WHERE id = %s",
                    (video_id,),
                )
                conn.commit()
        except Exception as e:
            log.warning(f"[HEARTBEAT] {video_id}: {e}")


def mark_complete(conn, video_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE youtube.videos SET subtitle_status='completed', subtitle_locked_until=NULL WHERE id=%s",
            (video_id,),
        )
        conn.commit()


def mark_failed(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
            SET subtitle_status='failed', subtitle_locked_until=NULL,
                subtitle_last_error=%s, subtitle_retry_count=subtitle_retry_count+1
            WHERE id=%s""",
            (error[:500], video_id),
        )
        conn.commit()


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
        conn.commit()


def extract_target_tracks(payload: dict) -> list:
    target_set = set(SUBTITLE_LANGS)
    found = {}
    for track in payload.get("subtitle", []):
        lang = track.get("language_code", "")
        if lang in target_set and lang not in found:
            found[lang] = {
                "language_code": lang,
                "url": track["url"],
                "is_automated": False,
            }
    for track in payload.get("automated_subtitle", []):
        lang = track.get("language_code", "")
        if lang in target_set and lang not in found:
            found[lang] = {
                "language_code": lang,
                "url": track["url"],
                "is_automated": True,
            }
    return [found[lang] for lang in SUBTITLE_LANGS if lang in found]


def vtt_url(url: str) -> str:
    """Replace fmt=json3 with fmt=vtt3 to get WebVTT directly."""
    return re.sub(r"\bfmt=json3\b", "fmt=vtt3", url)


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
        target=renew_lock, args=(conn, video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        if not payload:
            raise ValueError("subtitle_raw_payload is null")

        tracks = extract_target_tracks(payload)
        if not tracks:
            raise ValueError(f"no tracks for langs {SUBTITLE_LANGS}")

        log.info(
            f"[START] {video_id} — {len(tracks)} track(s): {[t['language_code'] for t in tracks]}"
        )
        success_count = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            for track in tracks:
                lang = track["language_code"]
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
                    log.info(f"[S3] {video_id} lang={lang} → {s3_key}")

                    upsert_subtitle(
                        conn, video_id, lang, track["is_automated"], content, s3_key
                    )
                    log.info(f"[DB] {video_id} lang={lang} upserted")
                    success_count += 1
                except Exception as e:
                    log.warning(f"[SKIP] {video_id} lang={lang}: {e}")
                    continue

        if success_count > 0:
            mark_complete(conn, video_id)
            log.info(
                f"[SUCCESS] {video_id} subtitle_status=completed — {success_count}/{len(tracks)} track(s) saved"
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
        f"Subtitle Mule started — target langs: {SUBTITLE_LANGS}, polling Postgres..."
    )
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
                video_id, payload, channel_handle, title = poll_job(conn)
            except Exception as e:
                log.error(f"Subtitle Mule poll error: {e} — reconnecting")
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
                log.info(f"[CLAIMED] {video_id} channel={channel_handle}")
                process(conn, video_id, payload, channel_handle, title)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try:
            conn.close()
        except:
            pass
        log.info("Subtitle Mule shut down cleanly")


if __name__ == "__main__":
    main()
