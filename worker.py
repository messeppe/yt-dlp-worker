import os
import time
import logging
import tempfile
import subprocess

import redis
import boto3
import psycopg2

# Config from environment (crash fast if missing)
REDIS_HOST     = os.environ["REDIS_HOST"]
REDIS_PORT     = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]
PROXY_URL      = os.environ["PROXY_URL"]
S3_ENDPOINT    = os.environ["S3_ENDPOINT"]
S3_BUCKET      = os.environ["S3_BUCKET"]
S3_ACCESS_KEY  = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY  = os.environ["S3_SECRET_KEY"]
DB_URL         = os.environ["SUPABASE_DB_URL"]

QUEUE_NAME = "video_queue"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


def get_redis():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        password=REDIS_PASSWORD, decode_responses=True
    )


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def claim_job(video_id: str) -> bool:
    """Atomically transition queued -> processing. Returns False if another worker got it."""
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status = 'processing',
                       locked_until = NOW() + INTERVAL '30 minutes'
                   WHERE id = %s AND media_status = 'queued'""",
                (video_id,)
            )
            return cur.rowcount == 1


def mark_complete(video_id: str, s3_path: str, file_size: int, file_ext: str):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO youtube.media_files
                     (video_id, media_type, format, quality_or_itag,
                      s3_path, file_size_bytes, mime_type, download_source)
                   VALUES (%s, 'video', %s, 'merged', %s, %s, 'video/mp4', 'ytdlp')
                   ON CONFLICT DO NOTHING""",
                (video_id, file_ext, s3_path, file_size)
            )
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status = 'completed', locked_until = NULL
                   WHERE id = %s""",
                (video_id,)
            )


def mark_failed(video_id: str, error: str):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status = 'failed',
                       locked_until = NULL,
                       media_last_error = %s
                   WHERE id = %s""",
                (error[:500], video_id)
            )


def download_and_upload(video_id: str) -> tuple[str, int, str]:
    """Download via yt-dlp through proxy, upload to S3. Returns (s3_key, file_size, ext)."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = [
            "yt-dlp",
            "--proxy", PROXY_URL,
            "-f", "bv*+ba/best/bestvideo+bestaudio",
            "--concurrent-fragments", "2",
            "--limit-rate", "2.5M",
            "--no-playlist",
            "--retries", "1",
            "--socket-timeout", "30",
            "--extractor-args", "youtube:player_client=web",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        ]
        # Support optional cookies file for YouTube auth
        cookies_path = os.environ.get("COOKIES_FILE", "")
        if cookies_path and os.path.isfile(cookies_path):
            cmd.extend(["--cookies", cookies_path])
            log.info(f"[COOKIES] Using cookies from {cookies_path}")

        cmd.extend(["-o", f"{tmpdir}/%(id)s.%(ext)s", url])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(result.stderr[-500:] or "yt-dlp exited non-zero")

        files = os.listdir(tmpdir)
        if not files:
            raise RuntimeError("yt-dlp produced no output file")

        filename = files[0]
        local_path = os.path.join(tmpdir, filename)
        ext = filename.rsplit(".", 1)[-1]
        s3_key = f"media/{video_id}/video.{ext}"
        file_size = os.path.getsize(local_path)

        get_s3().upload_file(local_path, S3_BUCKET, s3_key)
        return s3_key, file_size, ext


def process(video_id: str):
    log.info(f"[START] {video_id}")

    if not claim_job(video_id):
        log.warning(f"[SKIP]  {video_id} -- already claimed by another worker")
        return

    try:
        t0 = time.time()
        s3_key, file_size, ext = download_and_upload(video_id)
        elapsed = time.time() - t0
        mark_complete(video_id, s3_key, file_size, ext)
        log.info(f"[SUCCESS] {video_id} -> s3://{S3_BUCKET}/{s3_key} ({file_size} bytes, {elapsed:.0f}s)")
    except Exception as exc:
        log.error(f"[ERROR] {video_id}: {exc}")
        mark_failed(video_id, str(exc))


def main():
    # Verify Node.js is available (required for YouTube n-challenge solving)
    node_check = subprocess.run(["node", "--version"], capture_output=True, text=True)
    if node_check.returncode == 0:
        log.info(f"Node.js {node_check.stdout.strip()} detected -- n-challenge solver enabled")
    else:
        log.warning("Node.js NOT found -- YouTube downloads may fail with 'n challenge solving failed'")

    r = get_redis()
    log.info("Worker started, polling video_queue...")
    while True:
        video_id = r.rpop(QUEUE_NAME)
        if video_id:
            process(video_id)
        else:
            log.debug("[IDLE] queue empty, sleeping 2s")
            time.sleep(2)


if __name__ == "__main__":
    main()
