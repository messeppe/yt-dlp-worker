import os
import re
import signal
import threading
import time
import logging
import tempfile

import yt_dlp
import boto3
import psycopg2

S3_ENDPOINT   = os.environ["S3_ENDPOINT"]
S3_BUCKET     = os.environ["S3_BUCKET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
DB_URL        = os.environ["SUPABASE_DB_URL"]
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_ATTEMPTS  = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "3"))
MAX_HEIGHT    = int(os.environ.get("MAX_VIDEO_QUALITY", "720"))
YT_PROXY      = os.environ.get("YT_PROXY", "")        # optional, leave empty for direct
YT_COOKIES    = os.environ.get("YT_COOKIES_FILE", "")  # optional path to Netscape cookies.txt

FORMAT_SPEC = (
    f"bestvideo[height<={MAX_HEIGHT}][vcodec^=avc1]+bestaudio[ext=m4a]"
    f"/bestvideo[height<={MAX_HEIGHT}]+bestaudio[ext=m4a]"
    f"/bestvideo[height<={MAX_HEIGHT}]+bestaudio"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

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
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status = 'processing',
                locked_until = NOW() + INTERVAL '10 minutes'
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
    while not stop_event.wait(60):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE youtube.videos SET locked_until = NOW() + INTERVAL '10 minutes' WHERE id = %s",
                    (video_id,),
                )
                conn.commit()
        except Exception as e:
            log.warning(f"[HEARTBEAT] {video_id}: {e}")


def sanitize_filename(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', '', s or '')
    s = re.sub(r'\s+', ' ', s).strip()
    return s[:120]


def sanitize_path_segment(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', '', s or '')
    s = re.sub(r'\s+', '_', s).strip("._ ")
    return s[:80] or "unknown"


def mark_complete(conn, video_id: str, files: list):
    with conn.cursor() as cur:
        for s3_path, file_size, media_type, mime_type in files:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag,
                      s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, %s, %s, 'auto', %s, %s, %s, 'ytdlp')
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


class YtDlpLogger:
    def debug(self, msg):
        if msg.startswith('[debug]'):
            return
        log.debug(msg)

    def info(self, msg):
        log.info(f"[YTDLP] {msg}")

    def warning(self, msg):
        log.warning(f"[YTDLP] {msg}")

    def error(self, msg):
        log.error(f"[YTDLP] {msg}")


def process(conn, video_id: str, channel_handle: str):
    s3 = get_s3()
    safe_channel = sanitize_path_segment(channel_handle)
    last_error = "no attempts made"

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=renew_lock, args=(conn, video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            log.info(f"[ATTEMPT {attempt}/{MAX_ATTEMPTS}] {video_id}")

            with tempfile.TemporaryDirectory() as tmpdir:
                outtmpl = os.path.join(tmpdir, "%(title)s_%(id)s.%(ext)s")

                ydl_opts = {
                    "format": FORMAT_SPEC,
                    "outtmpl": outtmpl,
                    "quiet": True,
                    "no_warnings": False,
                    "logger": YtDlpLogger(),
                    "extractor_args": {"youtube": {"player_client": ["android_vr"]}},
                    "retries": 3,
                    "fragment_retries": 5,
                    "concurrent_fragment_downloads": 4,
                    "keepvideo": True,  # keep video file when downloading separate streams
                }
                if YT_PROXY:
                    ydl_opts["proxy"] = YT_PROXY
                if YT_COOKIES and os.path.exists(YT_COOKIES):
                    ydl_opts["cookiefile"] = YT_COOKIES

                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(
                            f"https://www.youtube.com/watch?v={video_id}"
                        )

                    title = info.get("title", "") or video_id
                    safe_title = sanitize_filename(title)
                    uploaded = []

                    requested = info.get("requested_downloads") or []

                    if not requested:
                        last_error = "yt-dlp returned no downloaded files"
                        log.warning(f"[RETRY] {video_id}: {last_error}")
                        continue

                    for dl in requested:
                        local_path = dl.get("filepath") or dl.get("filename")
                        if not local_path or not os.path.exists(local_path):
                            continue

                        ext = dl.get("ext", os.path.splitext(local_path)[1].lstrip("."))
                        vcodec = dl.get("vcodec", "none")
                        acodec = dl.get("acodec", "none")
                        mime = dl.get("container") or (
                            "video/mp4" if vcodec != "none" else "audio/mp4"
                        )

                        if vcodec != "none" and acodec != "none":
                            # Combined stream
                            media_type = "video"
                            key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                        elif vcodec != "none":
                            media_type = "video"
                            key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                        else:
                            media_type = "audio"
                            key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"

                        size = os.path.getsize(local_path)
                        log.info(f"[UPLOAD] {video_id} → s3:{key} ({size} bytes)")
                        s3.upload_file(local_path, S3_BUCKET, key)
                        uploaded.append((key, size, media_type, mime))

                    if not uploaded:
                        last_error = "no files found after yt-dlp download"
                        log.warning(f"[RETRY] {video_id}: {last_error}")
                        continue

                    mark_complete(conn, video_id, uploaded)
                    log.info(f"[SUCCESS] {video_id} files={[u[0] for u in uploaded]}")
                    return

                except yt_dlp.utils.DownloadError as e:
                    last_error = str(e)[:500]
                    log.warning(f"[RETRY] {video_id} attempt={attempt}: {last_error}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(5 * attempt)
                    continue
                except Exception as e:
                    last_error = str(e)[:500]
                    log.warning(f"[RETRY] {video_id} attempt={attempt}: {last_error}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(5 * attempt)
                    continue

        log.error(f"[FAIL] {video_id}: all attempts exhausted — {last_error}")
        mark_failed(conn, video_id, last_error)

    finally:
        stop_heartbeat.set()


def main():
    log.info("yt-dlp worker started, polling Postgres for queued videos...")

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
