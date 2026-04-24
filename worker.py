import os
import time
import logging
import tempfile
import subprocess
import random

import requests
import redis
import boto3
import psycopg2

REDIS_HOST     = os.environ["REDIS_HOST"]
REDIS_PORT     = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ["REDIS_PASSWORD"]
PROXY_URL      = os.environ["PROXY_URL"]
S3_ENDPOINT    = os.environ["S3_ENDPOINT"]
S3_BUCKET      = os.environ["S3_BUCKET"]
S3_ACCESS_KEY  = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY  = os.environ["S3_SECRET_KEY"]
DB_URL         = os.environ["SUPABASE_DB_URL"]
RAPIDAPI_KEY   = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_HOST  = os.environ["RAPIDAPI_HOST"]
QUEUE_NAME    = "video_queue"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)


def get_s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT,
                        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)


def claim_job(video_id: str) -> bool:
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
                   VALUES (%s, 'video', %s, 'merged', %s, %s, 'video/mp4', 'rapidapi')
                   ON CONFLICT DO NOTHING""",
                (video_id, file_ext, s3_path, file_size)
            )
            cur.execute(
                "UPDATE youtube.videos SET media_status = 'completed', locked_until = NULL WHERE id = %s",
                (video_id,)
            )


def mark_failed(video_id: str, error: str):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status = 'failed', locked_until = NULL, media_last_error = %s
                   WHERE id = %s""",
                (error[:500], video_id)
            )


def make_sticky_proxies(exclude: set | None = None) -> dict:
    pool = set(range(1, 101)) - (exclude or set())
    n = random.choice(list(pool))
    if exclude is not None:
        exclude.add(n)
    sticky_url = PROXY_URL.replace("-rotate", f"-{n}", 1)
    log.info(f"[PROXY] using sticky proxy #{n}")
    return {"http": sticky_url, "https": sticky_url}


def get_streams(video_id: str, proxies: dict) -> tuple[str, str | None]:
    """Returns (video_url, audio_url). audio_url is None if video_url is a combined stream."""
    resp = requests.get(
        f"https://{RAPIDAPI_HOST}/download.php",
        params={"id": video_id},
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST},
        proxies=proxies,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    log.info(f"[RAPIDAPI] keys={list(data.keys())}")

    def res(f):
        return int("".join(filter(str.isdigit, str(f.get("quality", f.get("resolution", "0"))))) or "0")

    def mime(f):
        return f.get("mimeType", f.get("mime", f.get("type", "")))

    def has_video(f):
        return f.get("has_video") or f.get("hasVideo") or "video" in mime(f)

    def has_audio(f):
        return f.get("has_audio") or f.get("hasAudio") or "audio" in mime(f)

    all_streams = (
        data.get("results") or
        data.get("formats") or
        (data.get("video") or data.get("videos") or []) + (data.get("audio") or data.get("audios") or [])
    )

    if all_streams:
        log.info(f"[RAPIDAPI] streams={[{k: v for k, v in f.items() if k != 'url'} for f in all_streams]}")

    # Prefer combined stream (video+audio) — no ffmpeg needed, no IP binding on separate DASH streams
    combined = [f for f in all_streams if has_video(f) and has_audio(f)]
    if combined:
        best = sorted(combined, key=res, reverse=True)[0]
        log.info(f"[RAPIDAPI] using combined stream quality={best.get('quality')}")
        return best["url"], None

    # Fall back to separate video + audio
    v_fmts = [f for f in all_streams if has_video(f) and not has_audio(f)]
    a_fmts = [f for f in all_streams if has_audio(f) and not has_video(f)]

    if not v_fmts or not a_fmts:
        raise RuntimeError(f"Could not extract streams. keys={list(data.keys())}")

    video_url = sorted(v_fmts, key=res, reverse=True)[0]["url"]
    audio_url = sorted(a_fmts, key=lambda f: 1 if str(f.get("itag")) == "140" else 0, reverse=True)[0]["url"]
    return video_url, audio_url


def stream_download(url: str, dest: str, proxies: dict):
    with requests.get(url, proxies=proxies, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)


def _try_download(video_id: str, proxies: dict, tmpdir: str) -> tuple[str, int, str]:
    """One attempt: RapidAPI → download → (merge) → S3 upload. Raises on any failure."""
    video_url, audio_url = get_streams(video_id, proxies)

    if audio_url is None:
        combined_path = os.path.join(tmpdir, "video.mp4")
        log.info(f"[DOWNLOAD] Fetching combined stream for {video_id}")
        stream_download(video_url, combined_path, proxies)
        file_size = os.path.getsize(combined_path)
        s3_key = f"media/{video_id}/video.mp4"
        get_s3().upload_file(combined_path, S3_BUCKET, s3_key)
        return s3_key, file_size, "mp4"

    video_path  = os.path.join(tmpdir, "video.mp4")
    audio_path  = os.path.join(tmpdir, "audio.m4a")
    merged_path = os.path.join(tmpdir, "merged.mp4")

    log.info(f"[DOWNLOAD] Fetching video stream for {video_id}")
    stream_download(video_url, video_path, proxies)

    log.info(f"[DOWNLOAD] Fetching audio stream for {video_id}")
    stream_download(audio_url, audio_path, proxies)

    log.info(f"[MERGE] Merging streams for {video_id}")
    result = subprocess.run(
        ["ffmpeg", "-y", "-i", video_path, "-i", audio_path,
         "-c:v", "copy", "-c:a", "copy", merged_path],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg merge failed: {result.stderr[-300:]}")

    file_size = os.path.getsize(merged_path)
    s3_key = f"media/{video_id}/video.mp4"
    get_s3().upload_file(merged_path, S3_BUCKET, s3_key)
    return s3_key, file_size, "mp4"


def download_and_upload(video_id: str) -> tuple[str, int, str]:
    """Retry with a fresh sticky proxy on failure — some proxy IPs are CDN-blocked."""
    used = set()
    last_exc = None
    with tempfile.TemporaryDirectory() as tmpdir:
        for attempt in range(1, 6):
            proxies = make_sticky_proxies(exclude=used)
            try:
                return _try_download(video_id, proxies, tmpdir)
            except Exception as exc:
                last_exc = exc
                log.warning(f"[RETRY] attempt {attempt} failed: {exc}")
    raise RuntimeError(f"All 5 proxy attempts failed. Last error: {last_exc}")


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
    r = get_redis()
    log.info("Worker started, polling video_queue...")
    while True:
        video_id = r.rpop(QUEUE_NAME)
        if video_id:
            process(video_id)
        else:
            time.sleep(2)


if __name__ == "__main__":
    main()
