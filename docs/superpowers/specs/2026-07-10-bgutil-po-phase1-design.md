# Design: bgutil PO Token Phase 1 (enable + verify)

**Date:** 2026-07-10  
**Status:** Approved (user explicit yes)  
**Scope:** Reliability — guest-path bot/403 reduction under hard constraints  
**Dashboard:** https://grafana.app7.kelana5.com/d/videomasjid-ytdlp/videomasjid-e28094-video-downloader  
**UID:** `videomasjid-ytdlp`

---

## Context

Self-host yt-dlp mule is live (`download_source=ytdlp`, RapidAPI media off). Live proof 2026-07-09 ~07:30 UTC: **YTDLP-OK ≈177 / YTDLP-FAIL ≈313** (~36% success). Failures mostly “Sign in to confirm you’re not a bot” on Decodo DC. `ENABLE_BGUTIL` was still **0**.

bgutil generates **Proof-of-Origin (PO) tokens** via BotGuard attestation. That is **not** a Google login. Cookies / burner accounts remain forbidden.

## Hard constraints (non-negotiable)

- No Google cookies / burner accounts  
- No residential proxies for PO (sticky Decodo DC remains the design)  
- No separate bgutil container (in-process via `start.sh` → `127.0.0.1:4416`)  
- No extra public ports (localhost only inside the mule container)

## Goal

Prove whether enabling the already-baked bgutil PO provider meaningfully reduces guest bot/403 failures, **before** changing retry/queue policy.

## Approach chosen

**Phase 1 = flip + verify only** (Approach 1).

| Rejected | Why |
|----------|-----|
| Harden first | Masks whether PO worked |
| Flip + harden together | Two variables; cannot attribute |

Best practice: change the root-cause lever → measure cleanly → harden only what still fails.

## Architecture (unchanged)

```
yt-ytdlp-mule container
├── start.sh
│   └── if ENABLE_BGUTIL=1 → node /bgutil/server → 127.0.0.1:4416
├── worker_ytdlp.py (yt-dlp + sticky Decodo)
└── pip plugin bgutil-ytdlp-pot-provider
        └── HTTP GETPOT → localhost:4416 (default; no extractor-args required)
```

Already in image (`Dockerfile.ytdlp` + `requirements_ytdlp.txt`):

- bgutil server clone/build at `/bgutil`  
- Python plugin `bgutil-ytdlp-pot-provider>=1.2.0`  
- Deno for EJS/nsig (separate from PO)

## Phase 1 steps

1. Coolify app `pzxrcvx9pf9ebw5hapfhxmss`, service `yt-ytdlp-mule`: set `ENABLE_BGUTIL=1`.  
2. Redeploy. Prefer env-only redeploy; rebuild only if boot shows missing `/bgutil` or plugin.  
3. Boot checks (Coolify / Loki):  
   - `bgutil server ready`  
   - mule banner includes `bgutil=True`  
4. Observe **60–90 minutes** on dashboard `videomasjid-ytdlp`:  
   - `YTDLP-OK` vs `YTDLP-FAIL`  
   - YouTube bot-check panel (`kind=bot`)  
5. Decide pass/fail (below).

## Success / failure criteria

**Baseline:** ~36% OK (177/490) with `ENABLE_BGUTIL=0`.

| Outcome | Definition | Action |
|---------|------------|--------|
| **Pass** | Success rate clearly above baseline (target: sustained **≥55–60%** OK over the window, or bot `kind=bot` rate drops by roughly half+) | Keep `ENABLE_BGUTIL=1`. Optionally schedule Phase 2 later. |
| **Flat** | Rate within noise of baseline | Keep or revert based on CPU/noise; proceed to Phase 2 diagnosis (rate/IP hygiene), still no cookies/residential. |
| **Regress** | OK rate worse, or bgutil fails to start / crashes mule | Set `ENABLE_BGUTIL=0`, redeploy; investigate logs before retry. |

Exact thresholds are directional; operator judgment on Grafana trends beats a single point sample.

## Out of scope (Phase 1)

- Code changes to `worker_ytdlp.py` classify/retry/requeue  
- Subtitle RapidAPI kill switch  
- Discovery / channel expansion  
- Cookies, residential, separate bgutil service, public ports  
- SABR upstream PR watching (informational only)

## Phase 2 (only after Phase 1 measured)

If bot fails remain high with PO on:

- Tighten queue policy for repeated `kind=bot` (avoid infinite requeue burn)  
- Review `YTDLP_SLEEP_*` / worker count vs IP reputation  
- Still under the same hard constraints  

Do **not** start Phase 2 until Phase 1 has a clear pass/flat/regress call.

## Rollback

Coolify: `ENABLE_BGUTIL=0` → redeploy. No code rollback required.

## Verification checklist

- [ ] Env shows `ENABLE_BGUTIL=1` on `yt-ytdlp-mule`  
- [ ] Log: `bgutil server ready`  
- [ ] Log: `bgutil=True` in start banner  
- [ ] Grafana 60–90m: OK/FAIL + bot panel compared to baseline  
- [ ] Pass / flat / regress recorded in memory bank  

## References

- `worker/docs/ytdlp-mule-deploy.md`  
- `memory-bank/ytdlp-selfhost-2026-07-09.md`  
- [yt-dlp PO Token Guide](https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide)  
- [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider)  
