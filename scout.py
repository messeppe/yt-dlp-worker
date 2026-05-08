import json
import os
import signal
import threading
import time
import logging
from urllib.parse import urlparse, parse_qs

import requests
import psycopg2
import psycopg2.pool

DB_URL         = os.environ["SUPABASE_DB_URL"]
RAPIDAPI_KEY   = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_HOST  = os.environ["RAPIDAPI_HOST"]
POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "5"))
MAX_VIDEO_QUALITY = int(os.environ.get("MAX_VIDEO_QUALITY", "720"))
LOW_QUOTA_THRESHOLD  = int(os.environ.get("LOW_QUOTA_THRESHOLD", "100"))
HARD_QUOTA_THRESHOLD = int(os.environ.get("HARD_QUOTA_THRESHOLD", "20"))

H264_VIDEO_ITAGS = {160, 133, 134, 135, 136, 137, 264, 266}

_WORKER_ID = os.environ.get("WORKER_ID", "scout")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)-16s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(_WORKER_ID)

# Global quota state — updated from RapidAPI response headers after every call.
# None = state unknown (no successful call yet or headers absent).
_quota_remaining: int | None = None
_quota_reset_at: int = 0  # Unix timestamp when quota window resets

_shutdown = threading.Event()

def handle_sigterm(signum, frame):
    log.info("SIGTERM received — finishing current job then exiting")
    _shutdown.set()

signal.signal(signal.SIGTERM, handle_sigterm)


def _update_quota_state(resp) -> None:
    """Parse RapidAPI rate-limit headers and update module-level quota state."""
    global _quota_remaining, _quota_reset_at
    try:
        remaining = resp.headers.get("X-RateLimit-Requests-Remaining")
        reset = resp.headers.get("X-RateLimit-Requests-Reset")
        if remaining is not None:
            _quota_remaining = int(remaining)
        if reset is not None:
            _quota_reset_at = int(reset)
        reset_in = max(0, _quota_reset_at - int(time.time())) if _quota_reset_at else "?"
        log.info(f"[QUOTA] remaining={_quota_remaining} reset_in={reset_in}s")
    except Exception:
        pass


def _adaptive_delay() -> float:
    """Return inter-call sleep seconds scaled to remaining quota."""
    if _quota_remaining is None or _quota_remaining > 500:
        return 0.2
    if _quota_remaining > LOW_QUOTA_THRESHOLD:
        return 1.0
    if _quota_remaining > HARD_QUOTA_THRESHOLD:
        return 3.0
    return 0.0  # pre-call guard will handle hard exhaustion


def _quota_sleep_seconds() -> int:
    """Return seconds to sleep if quota is hard-exhausted and reset hasn't passed.
    Returns 0 if quota is healthy or if the reset window has already passed."""
    if _quota_remaining is None:
        return 0
    if _quota_remaining > HARD_QUOTA_THRESHOLD:
        return 0
    if _quota_reset_at and _quota_reset_at > int(time.time()):
        return max(_quota_reset_at - int(time.time()), 60)
    return 0  # reset already passed — quota likely refreshed


def persist_quota(conn) -> None:
    """Write current quota state to youtube.api_quota for cross-process visibility.
    No-op if quota state is unknown."""
    if _quota_remaining is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO youtube.api_quota (service, remaining, reset_at, updated_at)
                VALUES ('rapidapi', %s, to_timestamp(%s)::timestamptz, NOW())
                ON CONFLICT (service) DO UPDATE SET
                    remaining  = EXCLUDED.remaining,
                    reset_at   = EXCLUDED.reset_at,
                    updated_at = NOW()
                """,
                (_quota_remaining, _quota_reset_at if _quota_reset_at else 0),
            )
            conn.commit()
    except Exception as e:
        log.warning(f"[QUOTA-DB] failed to persist quota state: {e}")


def requeue(conn, video_id: str) -> None:
    """Return a video to queued status without incrementing media_retry_count.
    Used on 429/5xx — video was not processed, so retry count must not increase."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status       = 'queued',
                   media_locked_until = NULL
               WHERE id = %s""",
            (video_id,),
        )
        conn.commit()
    log.info(f"[REQUEUE] {video_id} returned to queued (quota event, no retry increment)")


def requeue_subtitle(conn, video_id: str) -> None:
    """Return subtitle job to pending without incrementing subtitle_retry_count."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET subtitle_status       = 'pending',
                   subtitle_locked_until = NULL
               WHERE id = %s""",
            (video_id,),
        )
        conn.commit()
    log.info(f"[SUBTITLE-REQUEUE] {video_id} returned to pending (quota event)")


def poll_job(conn):
    """Claim one queued video atomically. Skips videos with unexpired stream URLs."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE youtube.videos v
            SET media_status       = 'processing',
                media_locked_until = NOW() + INTERVAL '5 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE media_status = 'queued'
                  AND (stream_url_expires_at IS NULL
                       OR stream_url_expires_at < NOW() + INTERVAL '4 hours')
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING v.id
            """
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0]
    return None


def poll_subtitle_job(conn):
    """Claim one subtitle-pending video atomically.
    First recovers any stuck 'processing' rows with expired locks, then claims a pending job."""
    with conn.cursor() as cur:
        # Recovery: reset expired subtitle 'processing' locks.
        # If payload exists → mule crashed mid-download → reset to 'queued' (re-download, no re-scout).
        # If no payload → scout crashed mid-API-call → reset to 'pending' (re-scout).
        cur.execute(
            """
            UPDATE youtube.videos
            SET subtitle_status      = CASE
                WHEN subtitle_raw_payload IS NOT NULL THEN 'queued'
                ELSE 'pending'
            END,
            subtitle_locked_until = NULL
            WHERE subtitle_status = 'processing'
              AND subtitle_locked_until IS NOT NULL
              AND subtitle_locked_until < NOW()
            """
        )
        # Claim one pending job
        cur.execute(
            """
            UPDATE youtube.videos v
            SET subtitle_status       = 'processing',
                subtitle_locked_until = NOW() + INTERVAL '5 minutes'
            WHERE v.id = (
                SELECT id FROM youtube.videos
                WHERE subtitle_status = 'pending'
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING v.id
            """
        )
        row = cur.fetchone()
        conn.commit()
    if row:
        return row[0]
    return None


def get_streams(video_id: str):
    url = f"https://{RAPIDAPI_HOST}/download.php"
    resp = requests.get(
        url,
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST},
        params={"id": video_id},
        timeout=(10, 30),
    )
    _update_quota_state(resp)  # read headers from ALL responses including 429
    resp.raise_for_status()
    data = resp.json()
    if "results" not in data:
        log.warning(f"[API-WARN] {video_id}: no 'results' key — status={data.get('status_code')}")
    return data.get("title", ""), data.get("results", [])


def get_subtitle_payload(video_id: str) -> dict:
    """Call RapidAPI /subtitle.php and return parsed JSON payload.
    Updates global quota state from response headers.
    Raises HTTPError on non-2xx. Caller must handle 429/5xx."""
    url = f"https://{RAPIDAPI_HOST}/subtitle.php"
    resp = requests.get(
        url,
        headers={"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": RAPIDAPI_HOST},
        params={"id": video_id},
        timeout=(10, 30),
    )
    _update_quota_state(resp)  # read headers before raise_for_status (captures 429 headers too)
    resp.raise_for_status()
    return resp.json()


def pick_streams(results: list):
    def has_v(r): return r.get("has_video") or r.get("mime", "").startswith("video/")
    def has_a(r): return r.get("has_audio")
    def v_quality(r):
        try: return int("".join(c for c in r.get("quality", "") if c.isdigit()))
        except ValueError: return 0

    def get_itag(r):
        try: return int(parse_qs(urlparse(r.get("url", "")).query).get("itag", [0])[0])
        except Exception: return 0

    def best_video(streams):
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

    return best_video(videos), sorted(audios, key=lambda r: r.get("quality", ""), reverse=True)[0]


def mark_ready(conn, video_id: str, video_url: str, audio_url: str):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET media_status = 'ready_for_download',
                   media_locked_until = NULL,
                   video_stream_url = %s,
                   audio_stream_url = %s,
                   stream_url_expires_at = NOW() + INTERVAL '4 hours'
               WHERE id = %s""",
            (video_url, audio_url, video_id),
        )
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


def mark_subtitle_queued(conn, video_id: str, payload: dict) -> None:
    """Store subtitle payload and transition subtitle_status to queued.
    subtitle_mule picks this up and downloads the actual VTT files."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET subtitle_status         = 'queued',
                   subtitle_raw_payload    = %s::jsonb,
                   subtitle_url_expires_at = NOW() + INTERVAL '6 hours',
                   subtitle_locked_until   = NULL
               WHERE id = %s""",
            (json.dumps(payload), video_id),
        )
        conn.commit()
    log.info(f"[SUBTITLE-CACHED] {video_id} subtitle payload stored, ready for subtitle mule")


def mark_subtitle_no_captions(conn, video_id: str) -> None:
    """Mark subtitle_status completed when API confirms video has no captions.
    No VTT to download — treat as successfully processed."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET subtitle_status       = 'completed',
                   subtitle_locked_until = NULL
               WHERE id = %s""",
            (video_id,),
        )
        conn.commit()
    log.info(f"[SUBTITLE-NONE] {video_id} no captions available — marked completed")


def mark_subtitle_failed(conn, video_id: str, error: str) -> None:
    """Mark subtitle_status failed (API error, video not found, etc.)."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE youtube.videos
               SET subtitle_status       = 'failed',
                   subtitle_locked_until = NULL,
                   subtitle_last_error   = %s,
                   subtitle_retry_count  = subtitle_retry_count + 1
               WHERE id = %s""",
            (error[:500], video_id),
        )
        conn.commit()
    log.warning(f"[SUBTITLE-FAIL] {video_id}: {error}")


def process(conn, video_id: str):
    # Pre-call quota guard
    sleep_s = _quota_sleep_seconds()
    if sleep_s > 0:
        log.warning(
            f"[QUOTA-GUARD] remaining={_quota_remaining} ≤ {HARD_QUOTA_THRESHOLD} "
            f"— requeuing {video_id}, sleeping {sleep_s}s until reset"
        )
        requeue(conn, video_id)
        _shutdown.wait(sleep_s)
        return

    delay = _adaptive_delay()
    if delay > 0:
        time.sleep(delay)

    try:
        title, results = get_streams(video_id)
        persist_quota(conn)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        persist_quota(conn)
        if status == 429:
            sleep_s = max(_quota_reset_at - int(time.time()), 3600) if _quota_reset_at else 3600
            log.warning(f"[429] {video_id}: RapidAPI quota hit — requeuing, sleeping {sleep_s}s")
            requeue(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        if status in (500, 502, 503, 504):
            sleep_s = 60
            log.warning(f"[5XX] {video_id}: RapidAPI {status} transient — requeuing, backing off {sleep_s}s")
            requeue(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        last_error = f"RapidAPI HTTP {status}"
        mark_failed(conn, video_id, last_error)
        log.warning(f"[FAIL] {video_id}: {last_error}", exc_info=True)
        return
    except Exception as e:
        mark_failed(conn, video_id, str(e))
        log.error(f"[FAIL] {video_id}: {e}", exc_info=True)
        return

    if not results:
        mark_failed(conn, video_id, "RapidAPI returned empty results")
        return

    video_stream, audio_stream = pick_streams(results)
    if video_stream is None:
        mark_failed(conn, video_id, "no usable streams in RapidAPI response")
        return

    v_url = video_stream["url"]
    a_url = audio_stream["url"] if audio_stream else None

    mark_ready(conn, video_id, v_url, a_url)
    log.info(f"[CACHED] {video_id} URLs extracted and ready for Mule")


def process_subtitle(conn, video_id: str) -> None:
    """Scout subtitle URLs for a video via RapidAPI /subtitle.php.
    Stores payload in subtitle_raw_payload for subtitle_mule to download."""
    sleep_s = _quota_sleep_seconds()
    if sleep_s > 0:
        log.warning(
            f"[QUOTA-GUARD] remaining={_quota_remaining} ≤ {HARD_QUOTA_THRESHOLD} "
            f"— requeuing subtitle job {video_id}, sleeping {sleep_s}s"
        )
        requeue_subtitle(conn, video_id)
        _shutdown.wait(sleep_s)
        return

    delay = _adaptive_delay()
    if delay > 0:
        time.sleep(delay)

    try:
        payload = get_subtitle_payload(video_id)
        persist_quota(conn)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        persist_quota(conn)
        if status == 429:
            sleep_s = max(_quota_reset_at - int(time.time()), 3600) if _quota_reset_at else 3600
            log.warning(f"[429] {video_id}: subtitle quota hit — requeuing, sleeping {sleep_s}s")
            requeue_subtitle(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        if status in (500, 502, 503, 504):
            sleep_s = 60
            log.warning(f"[5XX] {video_id}: subtitle RapidAPI {status} — requeuing, backing off {sleep_s}s")
            requeue_subtitle(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        mark_subtitle_failed(conn, video_id, f"RapidAPI /subtitle.php HTTP {status}")
        return
    except Exception as e:
        mark_subtitle_failed(conn, video_id, str(e))
        log.error(f"[SUBTITLE-FAIL] {video_id}: {e}", exc_info=True)
        return

    # 3-way response classification:
    # Has captions  → "subtitle" key present, at least one array non-empty → store + queue for mule
    # No captions   → "subtitle" key present, both arrays empty → mark completed
    # API/video err → "subtitle" key absent → mark failed
    if "subtitle" not in payload:
        error_msg = payload.get("message") or payload.get("msg") or str(payload)[:200]
        mark_subtitle_failed(conn, video_id, f"API error: {error_msg}")
        return

    human_tracks = payload.get("subtitle", [])
    auto_tracks = payload.get("automated_subtitle", [])
    if not human_tracks and not auto_tracks:
        mark_subtitle_no_captions(conn, video_id)
        return

    mark_subtitle_queued(conn, video_id, payload)


def main():
    log.info("Scout started — polling for media (queued) and subtitle (pending) jobs...")
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            break
        except Exception as e:
            log.error(f"DB connect failed: {e} — retrying in 10s", exc_info=True)
            time.sleep(10)

    try:
        while not _shutdown.is_set():
            try:
                # Priority 1: media jobs (queued → ready_for_download)
                video_id = poll_job(conn)
                if video_id:
                    process(conn, video_id)
                    continue

                # Priority 2: subtitle jobs (pending → queued payload stored)
                video_id = poll_subtitle_job(conn)
                if video_id:
                    process_subtitle(conn, video_id)
                    continue

            except Exception as e:
                log.error(f"Scout poll error: {e} — reconnecting", exc_info=True)
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

            # Nothing to do — idle wait
            _shutdown.wait(POLL_INTERVAL)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log.info("Scout shut down cleanly")


if __name__ == "__main__":
    main()
