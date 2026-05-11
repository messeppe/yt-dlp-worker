import datetime
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
LOW_QUOTA_THRESHOLD    = int(os.environ.get("LOW_QUOTA_THRESHOLD", "100"))
HARD_QUOTA_THRESHOLD   = int(os.environ.get("HARD_QUOTA_THRESHOLD", "20"))
CIRCUIT_OPEN_THRESHOLD = int(os.environ.get("CIRCUIT_OPEN_THRESHOLD", "3"))
CIRCUIT_SLEEP_BASE     = int(os.environ.get("CIRCUIT_SLEEP_BASE", "300"))
MAX_MEDIA_QUEUE    = int(os.environ.get("MAX_MEDIA_QUEUE", "50"))
MIN_MEDIA_QUEUE    = int(os.environ.get("MIN_MEDIA_QUEUE", "10"))
MAX_SUBTITLE_QUEUE = int(os.environ.get("MAX_SUBTITLE_QUEUE", "10"))
MIN_SUBTITLE_QUEUE = int(os.environ.get("MIN_SUBTITLE_QUEUE", "3"))
MAX_SCOUT_RETRIES  = int(os.environ.get("MAX_SCOUT_RETRIES", "10"))
RAPIDAPI_TIMEOUT   = int(os.environ.get("RAPIDAPI_TIMEOUT", "60"))

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
_consecutive_api_failures: int = 0
_media_paused: bool = False
_subtitle_paused: bool = False

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


def _circuit_open() -> bool:
    return _consecutive_api_failures >= CIRCUIT_OPEN_THRESHOLD


def _circuit_sleep_seconds() -> int:
    excess = max(_consecutive_api_failures - CIRCUIT_OPEN_THRESHOLD, 0)
    return min(CIRCUIT_SLEEP_BASE * (2 ** excess), 3600)


def _on_api_success() -> None:
    global _consecutive_api_failures
    if _consecutive_api_failures > 0:
        log.info(f"[CIRCUIT-CLOSE] RapidAPI recovered after {_consecutive_api_failures} consecutive failures")
    _consecutive_api_failures = 0


def _on_api_failure() -> None:
    global _consecutive_api_failures
    _consecutive_api_failures += 1
    if _circuit_open():
        log.warning(
            f"[CIRCUIT-OPEN] {_consecutive_api_failures} consecutive RapidAPI failures "
            f"— halting scout for {_circuit_sleep_seconds()}s"
        )


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


def _media_queue_depth(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM youtube.media_queue")
        return cur.fetchone()[0]


def _subtitle_queue_depth(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM youtube.subtitle_queue")
        return cur.fetchone()[0]


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
        # Recovery: reset stuck processing videos (scout crashed before inserting into media_queue)
        cur.execute(
            """
            UPDATE youtube.videos
            SET media_status = 'queued', media_locked_until = NULL
            WHERE media_status IN ('processing', 'ready_for_download')
              AND media_locked_until < NOW()
              AND NOT EXISTS (
                  SELECT 1 FROM youtube.media_queue WHERE video_id = youtube.videos.id
              )
            """
        )
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
                  AND scout_retry_count < %s
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING v.id
            """,
            (MAX_SCOUT_RETRIES,),
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
                WHEN EXISTS (SELECT 1 FROM youtube.subtitle_queue sq WHERE sq.video_id = youtube.videos.id) THEN 'queued'
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
                  AND media_status = 'completed'
                  AND subtitle_scout_retry_count < %s
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING v.id
            """,
            (MAX_SCOUT_RETRIES,),
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
        timeout=(10, RAPIDAPI_TIMEOUT),
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
        params={"id": video_id, "type": "vtt"},
        timeout=(10, RAPIDAPI_TIMEOUT),
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


def _cdn_expiry(url: str) -> "datetime.datetime | None":
    """Parse expire= from YouTube CDN URL. Returns UTC-aware datetime minus 5min buffer, or None."""
    try:
        qs = parse_qs(urlparse(url).query)
        exp = int(qs.get("expire", [0])[0])
        if exp > time.time() + 300:
            return datetime.datetime.fromtimestamp(exp - 300, tz=datetime.timezone.utc)
    except Exception:
        pass
    return None


def _queue_expiry(video_url: str, audio_url: "str | None") -> "datetime.datetime | None":
    """Return MIN of video and audio CDN expiry. None if neither URL has expire= param."""
    candidates = [_cdn_expiry(video_url)]
    if audio_url:
        candidates.append(_cdn_expiry(audio_url))
    candidates = [c for c in candidates if c is not None]
    return min(candidates) if candidates else None


def _subtitle_expiry(results: dict) -> "datetime.datetime | None":
    """Return CDN expiry from first available subtitle track URL."""
    for key in ("subtitle", "automated_subtitle"):
        for track in results.get(key, []):
            url = track.get("url", "")
            if url:
                return _cdn_expiry(url)
    return None


def mark_ready(conn, video_id: str, video_url: str, audio_url: str) -> bool:
    """Insert into media_queue. Returns True if inserted/updated, False if queue full."""
    exp_dt = _queue_expiry(video_url, audio_url)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO youtube.media_queue
                   (video_id, video_stream_url, audio_stream_url, url_expires_at)
               SELECT %s, %s, %s, COALESCE(%s::timestamptz, NOW() + INTERVAL '6 hours')
               WHERE (SELECT COUNT(*) FROM youtube.media_queue) < %s
               ON CONFLICT (video_id) DO UPDATE SET
                   video_stream_url = EXCLUDED.video_stream_url,
                   audio_stream_url = EXCLUDED.audio_stream_url,
                   url_expires_at   = EXCLUDED.url_expires_at
               RETURNING id""",
            (video_id, video_url, audio_url, exp_dt, MAX_MEDIA_QUEUE),
        )
        inserted = cur.fetchone() is not None
        if inserted:
            cur.execute(
                """UPDATE youtube.videos
                   SET media_status='ready_for_download',
                       media_locked_until=NULL,
                       scout_retry_count=0
                   WHERE id=%s""",
                (video_id,),
            )
        conn.commit()
    return inserted


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


def mark_subtitle_queued(conn, video_id: str, payload: dict) -> bool:
    """Insert into subtitle_queue. Returns True if inserted/updated, False if queue full."""
    exp_dt = _subtitle_expiry(payload)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO youtube.subtitle_queue (video_id, payload, url_expires_at)
               SELECT %s, %s::jsonb, COALESCE(%s::timestamptz, NOW() + INTERVAL '6 hours')
               WHERE (SELECT COUNT(*) FROM youtube.subtitle_queue) < %s
               ON CONFLICT (video_id) DO UPDATE SET
                   payload        = EXCLUDED.payload,
                   url_expires_at = EXCLUDED.url_expires_at
               RETURNING id""",
            (video_id, json.dumps(payload), exp_dt, MAX_SUBTITLE_QUEUE),
        )
        inserted = cur.fetchone() is not None
        if inserted:
            cur.execute(
                """UPDATE youtube.videos
                   SET subtitle_status='queued',
                       subtitle_locked_until=NULL,
                       subtitle_scout_retry_count=0
                   WHERE id=%s""",
                (video_id,),
            )
        conn.commit()
    if inserted:
        log.info(f"[SUBTITLE-CACHED] {video_id} subtitle payload stored, ready for subtitle mule")
    return inserted


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

    # Backpressure: check queue depth before burning API quota
    global _media_paused
    depth = _media_queue_depth(conn)
    if depth >= MAX_MEDIA_QUEUE:
        if not _media_paused:
            log.info(f"[BACKPRESSURE] media_queue={depth} >= {MAX_MEDIA_QUEUE} — pausing media scout")
        _media_paused = True
        requeue(conn, video_id)
        return

    try:
        title, results = get_streams(video_id)
        persist_quota(conn)
        _on_api_success()
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        persist_quota(conn)
        if status == 429:
            sleep_s = max(_quota_reset_at - int(time.time()), 3600) if _quota_reset_at else 3600
            log.warning(f"[429] {video_id}: quota hit — requeuing, sleeping {sleep_s}s")
            requeue(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        if status == 407:
            log.warning(f"[407] {video_id}: proxy auth failure — requeuing, backing off 60s")
            requeue(conn, video_id)
            _shutdown.wait(60)
            return
        if status in (500, 502, 503, 504):
            _on_api_failure()
            sleep_s = _circuit_sleep_seconds() if _circuit_open() else 60
            log.warning(f"[5XX] {video_id}: RapidAPI {status} — requeuing, sleeping {sleep_s}s")
            requeue(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        mark_failed(conn, video_id, f"RapidAPI HTTP {status}")
        log.warning(f"[FAIL] {video_id}: RapidAPI HTTP {status}", exc_info=True)
        return
    except (requests.ConnectionError, requests.Timeout) as e:
        _on_api_failure()
        sleep_s = _circuit_sleep_seconds() if _circuit_open() else 60
        log.warning(f"[CONN-ERR] {video_id}: {e} — requeuing, sleeping {sleep_s}s")
        requeue(conn, video_id)
        _shutdown.wait(sleep_s)
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

    if not mark_ready(conn, video_id, v_url, a_url):
        log.info(f"[BACKPRESSURE] media_queue full ({MAX_MEDIA_QUEUE}) — requeuing {video_id}")
        requeue(conn, video_id)
        return
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

    # Backpressure: check queue depth before burning API quota
    global _subtitle_paused
    depth = _subtitle_queue_depth(conn)
    if depth >= MAX_SUBTITLE_QUEUE:
        if not _subtitle_paused:
            log.info(f"[BACKPRESSURE] subtitle_queue={depth} >= {MAX_SUBTITLE_QUEUE} — pausing subtitle scout")
        _subtitle_paused = True
        requeue_subtitle(conn, video_id)
        return

    try:
        payload = get_subtitle_payload(video_id)
        persist_quota(conn)
        _on_api_success()
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        persist_quota(conn)
        if status == 429:
            sleep_s = max(_quota_reset_at - int(time.time()), 3600) if _quota_reset_at else 3600
            log.warning(f"[429] {video_id}: subtitle quota hit — requeuing, sleeping {sleep_s}s")
            requeue_subtitle(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        if status == 407:
            log.warning(f"[407] {video_id}: subtitle proxy auth — requeuing, backing off 60s")
            requeue_subtitle(conn, video_id)
            _shutdown.wait(60)
            return
        if status in (500, 502, 503, 504):
            _on_api_failure()
            sleep_s = _circuit_sleep_seconds() if _circuit_open() else 60
            log.warning(f"[5XX] {video_id}: subtitle RapidAPI {status} — requeuing, sleeping {sleep_s}s")
            requeue_subtitle(conn, video_id)
            _shutdown.wait(sleep_s)
            return
        mark_subtitle_failed(conn, video_id, f"RapidAPI /subtitle.php HTTP {status}")
        return
    except (requests.ConnectionError, requests.Timeout) as e:
        _on_api_failure()
        sleep_s = _circuit_sleep_seconds() if _circuit_open() else 60
        log.warning(f"[CONN-ERR] {video_id}: subtitle {e} — requeuing, sleeping {sleep_s}s")
        requeue_subtitle(conn, video_id)
        _shutdown.wait(sleep_s)
        return
    except Exception as e:
        mark_subtitle_failed(conn, video_id, str(e))
        log.error(f"[SUBTITLE-FAIL] {video_id}: {e}", exc_info=True)
        return

    # 3-way response classification — data lives under payload["results"] with type=vtt
    results = payload.get("results") or {}
    # Backward compat: old payloads stored before this fix may have subtitle at top level
    if "subtitle" not in results and "subtitle" in payload:
        results = payload

    if "subtitle" not in results:
        error_msg = payload.get("message") or payload.get("msg") or str(payload)[:200]
        mark_subtitle_failed(conn, video_id, f"API error: {error_msg}")
        return

    human_tracks = results.get("subtitle", [])
    auto_tracks = results.get("automated_subtitle", [])
    if not human_tracks and not auto_tracks:
        mark_subtitle_no_captions(conn, video_id)
        return

    if not mark_subtitle_queued(conn, video_id, results):
        log.info(f"[BACKPRESSURE] subtitle_queue full ({MAX_SUBTITLE_QUEUE}) — requeuing {video_id}")
        requeue_subtitle(conn, video_id)
        return


def main():
    global _media_paused, _subtitle_paused
    log.info("Scout started — polling for media (queued) and subtitle (pending) jobs...")
    while True:
        try:
            conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
            break
        except Exception as e:
            log.error(f"DB connect failed: {e} — retrying in 10s", exc_info=True)
            time.sleep(10)

    try:
        while not _shutdown.is_set():
            # Backpressure: check depths, clear pause flags if drained
            if _media_paused:
                if _media_queue_depth(conn) <= MIN_MEDIA_QUEUE:
                    log.info("Scout [BACKPRESSURE-RESUME] media_queue drained — resuming media scout")
                    _media_paused = False
            if _subtitle_paused:
                if _subtitle_queue_depth(conn) <= MIN_SUBTITLE_QUEUE:
                    log.info("Scout [BACKPRESSURE-RESUME] subtitle_queue drained — resuming subtitle scout")
                    _subtitle_paused = False

            try:
                # Priority 1: media jobs (queued → ready_for_download) — skip if paused
                if not _media_paused:
                    video_id = poll_job(conn)
                    if video_id:
                        process(conn, video_id)
                        continue

                # Priority 2: subtitle jobs (pending → queued payload stored) — skip if paused
                if not _subtitle_paused:
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
                    conn = psycopg2.connect(DB_URL, options="-c timezone=Asia/Jakarta")
                except Exception:
                    pass
                continue

            # Both paused → longer sleep; otherwise normal idle wait
            if _media_paused and _subtitle_paused:
                _shutdown.wait(30)
            else:
                _shutdown.wait(POLL_INTERVAL)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log.info("Scout shut down cleanly")


if __name__ == "__main__":
    main()
