# Self-host yt-dlp media path (2026-07-09)

## Why
RapidAPI CDN URL handoff 403s under GVS PO / IP binding. Fix = extract + download on the **same sticky Decodo DC IP** via yt-dlp. No ffmpeg (separate `bv`/`ba`). No Google cookies. Optional bgutil PO (not a burner account).

## What shipped
| File | Role |
|------|------|
| `worker_ytdlp.py` | Claims `media_status=queued`, yt-dlp on sticky proxy, uploads `_v`/`_a` to S3, marks complete / extractor_blocked |
| `Dockerfile.ytdlp` | Python 3.11 + Deno + yt-dlp + optional bgutil server |
| `start.sh` | Starts bgutil if `ENABLE_BGUTIL=1`, then worker |
| `docker-compose.yaml` | Adds `yt-ytdlp-mule`; scout `SCOUT_MEDIA_ENABLED` default **0**; legacy media mule scaled to 1 |
| `scout.py` | `SCOUT_MEDIA_ENABLED` kill switch (subtitles still run) |
| `test_worker_ytdlp.py` | Unit tests (classify / format / opts) |

## Coolify env (set before/after deploy)
```
SCOUT_MEDIA_ENABLED=0
MEDIA_MULE_WORKER_COUNT=1
YTDLP_WORKER_COUNT=4
PROXY_BASE_PORT=10000
PROXY_POOL_SIZE=1000
PROXY_NAME=decodo
ENABLE_BGUTIL=0
YTDLP_SLEEP_MIN=5
YTDLP_SLEEP_MAX=10
MAX_VIDEO_QUALITY=720
```
Existing `PROXY_URL`, `SUPABASE_DB_URL`, `S3_*` unchanged.

## Rollback
1. Coolify: `SCOUT_MEDIA_ENABLED=1`, `MEDIA_MULE_WORKER_COUNT=50` (or prior), stop/remove ytdlp service if needed.
2. Or set `YTDLP_WORKER_COUNT=0` and re-enable scout media.

## Verify after deploy
1. Coolify logs `yt-ytdlp-mule`: `yt-dlp mule started`, Deno version, `YTDLP-START` / `YTDLP-OK`.
2. Scout banner: `media_enabled=False`.
3. DB: `media_files` rows with `download_source='ytdlp'`; `media_status=completed` growing.
4. If mass bot/403: set `ENABLE_BGUTIL=1` and **rebuild** (still no Google cookies).
   - Server git tag and pip plugin must be the **same** release (pinned `1.3.1` in `Dockerfile.ytdlp` + `requirements_ytdlp.txt`).
   - Boot must show `bgutil server ready`, banner `bgutil=True`, and **no** `ImportError: BgUtilPTPBase` / `Error while importing module ... getpot_bgutil`.

## Not done by this change
- Live smoke download on Coolify (needs deploy + Decodo).
- Subtitle path still RapidAPI/timedtext.
- SABR PR #13515 not merged upstream — watch only.
