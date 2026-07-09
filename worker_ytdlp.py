"""yt-dlp media downloader — extract + download on the SAME sticky Decodo IP.

Replaces RapidAPI CDN URL handoff (broken by GVS PO / IP binding).
Stores best video + best audio as separate files (no ffmpeg merge).

Env:
  SUPABASE_DB_URL, S3_*, PROXY_URL, PROXY_POOL_SIZE, PROXY_BASE_PORT
  WORKER_COUNT, MAX_VIDEO_QUALITY, MAX_ATTEMPTS_PER_VIDEO
  YTDLP_SLEEP_MIN / YTDLP_SLEEP_MAX  (guest rate ~300/h → default 5–10s)
  ENABLE_BGUTIL=1  (optional PO provider at http://127.0.0.1:4416 — no Google cookies)
"""
from __future__ import annotations

import os
import re
import signal
import threading
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor

import yt_dlp
import boto3
import psycopg2
from logging_setup import setup_logging, log_event
from proxy_pool import build_pools, pick_pool

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
DB_URL = os.environ["SUPABASE_DB_URL"]

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS_PER_VIDEO", "3"))
MAX_HEIGHT = int(os.environ.get("MAX_VIDEO_QUALITY", "720"))
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "4"))
SLEEP_MIN = float(os.environ.get("YTDLP_SLEEP_MIN", "5"))
SLEEP_MAX = float(os.environ.get("YTDLP_SLEEP_MAX", "10"))
ENABLE_BGUTIL = os.environ.get("ENABLE_BGUTIL", "0") == "1"
JS_RUNTIME = os.environ.get("YTDLP_JS_RUNTIME", "deno")  # deno | node

# Comma = download each format as its own file (no ffmpeg merge).
FORMAT_SPEC = f"bestvideo[height<={MAX_HEIGHT}]/best[height<={MAX_HEIGHT}],bestaudio"

_WORKER_ID = os.environ.get("WORKER_ID", "ytdlp-mule")
log = setup_logging(_WORKER_ID)
_shutdown = threading.Event()

# Permanent / skip-list signals (no cookies → cannot unlock these).
_BLOCK_MARKERS = (
    "sign in to confirm your age",
    "age-restricted",
    "age restricted",
    "members-only",
    "members only",
    "private video",
    "this video is private",
    "video unavailable",
    "has been removed",
    "copyright",
    "account associated with this video has been terminated",
)

_BOT_MARKERS = (
    "sign in to confirm you're not a bot",
    "confirm you're not a bot",
    "http error 403",
    "403: forbidden",
)


def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current jobs then exiting")
    _shutdown.set()


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def sanitize_filename(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120]


def sanitize_path_segment(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "", s or "")
    s = re.sub(r"\s+", "_", s).strip("._ ")
    return s[:80] or "unknown"


def _classify_error(msg: str) -> str:
    low = (msg or "").lower()
    if any(m in low for m in _BLOCK_MARKERS):
        return "blocked"
    if any(m in low for m in _BOT_MARKERS):
        return "bot"
    return "transient"


class YtDlpLogger:
    def debug(self, msg):
        if isinstance(msg, str) and msg.startswith("[debug]"):
            return
        log.debug(msg)

    def info(self, msg):
        log.info(f"[YTDLP] {msg}")

    def warning(self, msg):
        log.warning(f"[YTDLP] {msg}")

    def error(self, msg):
        log.error(f"[YTDLP] {msg}")


def poll_job(conn):
    """Claim one queued video (bypasses RapidAPI / media_queue)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status = 'processing',
                media_locked_until = NOW() + INTERVAL '30 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE media_status = 'queued'
                  AND scout_retry_count < %s
                  AND NOT extractor_blocked
                  AND (media_locked_until IS NULL OR media_locked_until < NOW())
                ORDER BY created_at ASC NULLS LAST
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
                COALESCE(v.title, v.id) AS title
            """,
            (int(os.environ.get("MAX_SCOUT_RETRIES", "5")),),
        )
        row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    return row[0], row[1] or "unknown", row[2] or row[0]


def renew_lock(video_id: str, stop_event: threading.Event):
    """Own connection — pooler-safe heartbeat."""
    while not stop_event.wait(60):
        try:
            conn = psycopg2.connect(DB_URL)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE youtube.videos
                           SET media_locked_until = NOW() + INTERVAL '30 minutes'
                           WHERE id = %s AND media_status = 'processing'""",
                        (video_id,),
                    )
                    conn.commit()
            finally:
                conn.close()
        except Exception as e:
            log.warning(f"[HEARTBEAT] {video_id}: {e}")


def mark_complete(conn, video_id: str, files: list):
    with conn.cursor() as cur:
        for s3_path, file_size, media_type, mime_type in files:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag,
                      s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, %s, %s, 'auto', %s, %s, %s, 'ytdlp')
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
            log_event(
                log,
                "info",
                "db_write",
                "Inserted media_files row",
                worker=_WORKER_ID,
                table="youtube.media_files",
                video_id=video_id,
                media_type=media_type,
                s3_path=s3_path,
                file_size_bytes=file_size,
            )
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'completed',
                   media_locked_until = NULL,
                   scout_retry_count = 0,
                   media_last_error = NULL
               WHERE id = %s""",
            (video_id,),
        )
        conn.commit()
    log_event(
        log,
        "info",
        "db_write",
        "Updated media status completed",
        worker=_WORKER_ID,
        table="youtube.videos",
        video_id=video_id,
        media_status="completed",
    )


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
    log_event(
        log,
        "warning",
        "db_write",
        "Updated media status failed",
        worker=_WORKER_ID,
        table="youtube.videos",
        video_id=video_id,
        media_status="failed",
        error=error[:200],
    )


def mark_extractor_blocked(conn, video_id: str, reason: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET extractor_blocked = TRUE,
                   media_status = 'failed',
                   media_locked_until = NULL,
                   media_last_error = %s,
                   media_retry_count = media_retry_count + 1
               WHERE id = %s""",
            (reason[:500], video_id),
        )
        conn.commit()
    log_event(
        log,
        "warning",
        "EXTRACTOR-BLOCKED",
        "Video marked extractor_blocked",
        worker=_WORKER_ID,
        table="youtube.videos",
        video_id=video_id,
        reason=reason[:200],
    )


def requeue(conn, video_id: str, error: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'queued',
                   media_locked_until = NULL,
                   media_last_error = %s,
                   scout_retry_count = scout_retry_count + 1
               WHERE id = %s""",
            (error[:500], video_id),
        )
        conn.commit()
    log_event(
        log,
        "info",
        "queue_requeue",
        "Media job requeued after ytdlp failure",
        worker=_WORKER_ID,
        queue="youtube.videos",
        video_id=video_id,
        to_status="queued",
    )


def _ydl_opts(tmpdir: str, proxy_url: str | None) -> dict:
    outtmpl = os.path.join(tmpdir, "%(id)s_%(format_id)s.%(ext)s")
    opts: dict = {
        "format": FORMAT_SPEC,
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": False,
        "logger": YtDlpLogger(),
        "retries": 3,
        "fragment_retries": 5,
        "concurrent_fragment_downloads": 4,
        "noprogress": True,
        # Prefer clients that often work without PO / cookies. android_vr: no PO.
        "extractor_args": {
            "youtube": {
                "player_client": ["android_vr", "tv", "web_embedded", "mweb"],
            }
        },
        "js_runtimes": {JS_RUNTIME: {}},
        "remote_components": ["ejs:github"],
        "sleep_interval": SLEEP_MIN,
        "max_sleep_interval": SLEEP_MAX,
    }
    if proxy_url:
        opts["proxy"] = proxy_url
    return opts


def _collect_downloads(info: dict, tmpdir: str) -> list[dict]:
    """Return list of {path, vcodec, acodec, ext, mime} for files on disk."""
    found: list[dict] = []
    requested = info.get("requested_downloads") or []
    for dl in requested:
        path = dl.get("filepath") or dl.get("filename")
        if not path or not os.path.exists(path):
            continue
        ext = dl.get("ext") or os.path.splitext(path)[1].lstrip(".")
        vcodec = dl.get("vcodec") or "none"
        acodec = dl.get("acodec") or "none"
        if vcodec != "none" and acodec != "none":
            mime = "video/mp4"
        elif vcodec != "none":
            mime = f"video/{ext}" if ext else "video/mp4"
        else:
            mime = f"audio/{ext}" if ext else "audio/m4a"
        found.append(
            {
                "path": path,
                "vcodec": vcodec,
                "acodec": acodec,
                "ext": ext,
                "mime": mime,
            }
        )

    if found:
        return found

    # Fallback: scan tmpdir for anything yt-dlp wrote
    for name in os.listdir(tmpdir):
        path = os.path.join(tmpdir, name)
        if not os.path.isfile(path):
            continue
        ext = os.path.splitext(name)[1].lstrip(".")
        # Heuristic: audio-ish extensions
        if ext in ("m4a", "webm", "opus", "ogg", "mp3") and "_a" not in name:
            # still classify by size/name later via codecs unknown
            found.append(
                {
                    "path": path,
                    "vcodec": "none",
                    "acodec": "unknown",
                    "ext": ext,
                    "mime": f"audio/{ext}",
                }
            )
        else:
            found.append(
                {
                    "path": path,
                    "vcodec": "unknown",
                    "acodec": "none",
                    "ext": ext,
                    "mime": f"video/{ext}",
                }
            )
    return found


def process(conn, video_id: str, channel_handle: str, title: str, pools):
    primary, secondary = pools
    s3 = get_s3()
    safe_channel = sanitize_path_segment(channel_handle)
    safe_title = sanitize_filename(title) if title else video_id
    last_error = "no attempts made"
    last_kind = "transient"

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=renew_lock, args=(video_id, stop_heartbeat), daemon=True
    )
    heartbeat.start()

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if _shutdown.is_set():
                requeue(conn, video_id, "shutdown during download")
                return

            pool = pick_pool(primary, secondary)
            proxy_idx = pool.pick()
            proxy_url = pool.proxy_url(proxy_idx)
            log.info(
                f"[ATTEMPT {attempt}/{MAX_ATTEMPTS}] {video_id} "
                f"proxy={pool.name}:{proxy_idx}"
            )
            log_event(
                log,
                "info",
                "YTDLP-START",
                "Starting yt-dlp download",
                worker=_WORKER_ID,
                video_id=video_id,
                attempt=attempt,
                proxy=f"{pool.name}:{proxy_idx}",
            )

            with tempfile.TemporaryDirectory(prefix=f"ytdlp_{video_id}_") as tmpdir:
                opts = _ydl_opts(tmpdir, proxy_url)
                try:
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(
                            f"https://www.youtube.com/watch?v={video_id}",
                            download=True,
                        )

                    if not info:
                        last_error = "yt-dlp returned empty info"
                        last_kind = "transient"
                        log.warning(f"[RETRY] {video_id}: {last_error}")
                        continue

                    downloads = _collect_downloads(info, tmpdir)
                    if not downloads:
                        last_error = "yt-dlp produced no files"
                        last_kind = "transient"
                        log.warning(f"[RETRY] {video_id}: {last_error}")
                        continue

                    uploaded = []
                    video_parts = [
                        d
                        for d in downloads
                        if d["vcodec"] != "none"
                    ]
                    audio_parts = [
                        d
                        for d in downloads
                        if d["vcodec"] == "none" and d["acodec"] != "none"
                    ]
                    # Combined-only fallback
                    if not video_parts and not audio_parts:
                        video_parts = downloads

                    if len(video_parts) == 1 and not audio_parts:
                        d = video_parts[0]
                        ext = d["ext"] or "mp4"
                        key = f"youtube/{safe_channel}/{safe_title}_{video_id}.{ext}"
                        size = os.path.getsize(d["path"])
                        s3.upload_file(d["path"], S3_BUCKET, key)
                        log_event(
                            log,
                            "info",
                            "s3_upload",
                            "Uploaded media to s3",
                            worker=_WORKER_ID,
                            video_id=video_id,
                            s3_bucket=S3_BUCKET,
                            s3_path=key,
                            media_type="video",
                        )
                        uploaded.append((key, size, "video", d["mime"]))
                    else:
                        if video_parts:
                            d = video_parts[0]
                            ext = d["ext"] or "mp4"
                            key = f"youtube/{safe_channel}/{safe_title}_{video_id}_v.{ext}"
                            size = os.path.getsize(d["path"])
                            s3.upload_file(d["path"], S3_BUCKET, key)
                            log_event(
                                log,
                                "info",
                                "s3_upload",
                                "Uploaded media to s3",
                                worker=_WORKER_ID,
                                video_id=video_id,
                                s3_bucket=S3_BUCKET,
                                s3_path=key,
                                media_type="video",
                            )
                            uploaded.append((key, size, "video", d["mime"]))
                        if audio_parts:
                            d = audio_parts[0]
                            ext = d["ext"] or "m4a"
                            key = f"youtube/{safe_channel}/{safe_title}_{video_id}_a.{ext}"
                            size = os.path.getsize(d["path"])
                            s3.upload_file(d["path"], S3_BUCKET, key)
                            log_event(
                                log,
                                "info",
                                "s3_upload",
                                "Uploaded media to s3",
                                worker=_WORKER_ID,
                                video_id=video_id,
                                s3_bucket=S3_BUCKET,
                                s3_path=key,
                                media_type="audio",
                            )
                            uploaded.append((key, size, "audio", d["mime"]))

                    if not uploaded:
                        last_error = "no files uploaded after download"
                        last_kind = "transient"
                        continue

                    mark_complete(conn, video_id, uploaded)
                    log.info(f"[SUCCESS] {video_id} files={[u[0] for u in uploaded]}")
                    log_event(
                        log,
                        "info",
                        "YTDLP-OK",
                        "yt-dlp download completed",
                        worker=_WORKER_ID,
                        video_id=video_id,
                        file_count=len(uploaded),
                    )
                    return

                except yt_dlp.utils.DownloadError as e:
                    last_error = str(e)[:500]
                    last_kind = _classify_error(last_error)
                    log.warning(
                        f"[RETRY] {video_id} attempt={attempt} kind={last_kind}: {last_error}"
                    )
                    log_event(
                        log,
                        "warning",
                        "YTDLP-FAIL",
                        "yt-dlp download error",
                        worker=_WORKER_ID,
                        video_id=video_id,
                        attempt=attempt,
                        kind=last_kind,
                        error=last_error[:200],
                        proxy=f"{pool.name}:{proxy_idx}",
                    )
                    if last_kind == "bot":
                        pool.mark_failed(proxy_idx, cooldown_secs=120)
                    if last_kind == "blocked":
                        mark_extractor_blocked(conn, video_id, last_error)
                        return
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(min(5 * attempt, 20))
                    continue
                except Exception as e:
                    last_error = str(e)[:500]
                    last_kind = _classify_error(last_error)
                    log.warning(
                        f"[RETRY] {video_id} attempt={attempt}: {last_error}",
                        exc_info=True,
                    )
                    if last_kind == "blocked":
                        mark_extractor_blocked(conn, video_id, last_error)
                        return
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(min(5 * attempt, 20))
                    continue

        log.error(f"[FAIL] {video_id}: exhausted — {last_error}")
        if last_kind == "blocked":
            mark_extractor_blocked(conn, video_id, last_error)
        elif last_kind == "bot":
            requeue(conn, video_id, last_error)
        else:
            mark_failed(conn, video_id, last_error)
    finally:
        stop_heartbeat.set()


def worker_loop(pools):
    conn = None
    while not _shutdown.is_set():
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(DB_URL)
            job = poll_job(conn)
            if not job:
                _shutdown.wait(POLL_INTERVAL)
                continue
            video_id, channel_handle, title = job
            log_event(
                log,
                "info",
                "queue_claim",
                "Media job claimed by ytdlp mule",
                worker=_WORKER_ID,
                queue="youtube.videos",
                video_id=video_id,
                media_status="processing",
            )
            process(conn, video_id, channel_handle, title, pools)
        except Exception as e:
            log.error(f"worker_loop error: {e}", exc_info=True)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            _shutdown.wait(5)
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def main():
    log.info(
        f"yt-dlp mule started — workers={WORKER_COUNT} height<={MAX_HEIGHT} "
        f"js={JS_RUNTIME} bgutil={ENABLE_BGUTIL} format={FORMAT_SPEC!r}"
    )
    pools = build_pools()
    with ThreadPoolExecutor(
        max_workers=WORKER_COUNT, thread_name_prefix="ytdlp"
    ) as pool:
        futures = [pool.submit(worker_loop, pools) for _ in range(WORKER_COUNT)]
        for f in futures:
            try:
                f.result()
            except Exception as e:
                log.error(f"ytdlp worker crashed: {e}", exc_info=True)
    log.info("yt-dlp mule shut down cleanly")


if __name__ == "__main__":
    main()
