# bgutil PO Phase 1 Implementation Plan

> **For agentic workers:** Ops-only plan (no app code). Execute tasks in order. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable in-container bgutil PO tokens (`ENABLE_BGUTIL=1`) and verify whether guest bot/403 fails drop vs the ~36% OK baseline.

**Architecture:** Coolify env flip on `yt-ytdlp-mule` starts `/bgutil` HTTP server on `127.0.0.1:4416` via `start.sh`; pip plugin `bgutil-ytdlp-pot-provider` feeds yt-dlp. No cookies, no residential, no separate container, no public ports.

**Tech Stack:** Coolify API, Loki via Grafana, dashboard `videomasjid-ytdlp`, existing `Dockerfile.ytdlp` image.

**Spec:** `docs/superpowers/specs/2026-07-10-bgutil-po-phase1-design.md`

---

## File map

| Path | Role |
|------|------|
| Coolify app `pzxrcvx9pf9ebw5hapfhxmss` env | Set `ENABLE_BGUTIL=1` |
| `start.sh` (already shipped) | Starts bgutil when env=1 |
| `memory-bank/ytdlp-selfhost-2026-07-09.md` | Record outcome |
| `memory-bank/index.md` | Session handoff |

No worker Python changes in Phase 1.

---

### Task 1: Capture pre-flip baseline (optional if 2026-07-09 numbers still trusted)

**Files:**
- Update later: `memory-bank/ytdlp-selfhost-2026-07-09.md`

- [ ] **Step 1: Query Loki last 1h OK/FAIL counts**

Via Grafana MCP `query_loki_stats` / Explore, or HTTP API:

```
{app="videomasjid", service="yt-ytdlp-mule"} | json | event="YTDLP-OK"
{app="videomasjid", service="yt-ytdlp-mule"} | json | event="YTDLP-FAIL"
{app="videomasjid", service="yt-ytdlp-mule"} | json | event="YTDLP-FAIL" | kind="bot"
```

Expected: numbers exist (pipeline alive). Record OK / FAIL / bot.

- [ ] **Step 2: Note baseline**

Trusted fallback if Loki quiet: **OK≈177 FAIL≈313 (~36%)** from 2026-07-09 ~07:30 UTC.

---

### Task 2: Set ENABLE_BGUTIL=1 on Coolify

**Files:**
- Coolify application UUID `pzxrcvx9pf9ebw5hapfhxmss`

- [ ] **Step 1: Read current env**

```bash
curl -sS -H "Authorization: Bearer $COOLIFY_TOKEN" \
  -H "Accept: application/json" \
  "https://coolify.app7.kelana5.com/api/v1/applications/pzxrcvx9pf9ebw5hapfhxmss" \
  | jq '{status, git_commit_sha, env: [.environment_variables[]? | select(.key|test("BGUTIL|YTDLP|SCOUT_MEDIA|WORKER"))] | map({key, value})}'
```

Expected: `ENABLE_BGUTIL` present with value `0` (or missing → treat as 0).

- [ ] **Step 2: Upsert ENABLE_BGUTIL=1**

Use Coolify env update API (PATCH/POST env endpoint as available on this Coolify version). Prefer updating the existing key in place; avoid duplicating keys.

If API only supports full env replace, merge carefully — do **not** wipe unrelated secrets.

Expected: env readback shows `ENABLE_BGUTIL=1`.

- [ ] **Step 3: Redeploy application**

```bash
curl -sS -X POST -H "Authorization: Bearer $COOLIFY_TOKEN" \
  -H "Accept: application/json" \
  "https://coolify.app7.kelana5.com/api/v1/applications/pzxrcvx9pf9ebw5hapfhxmss/restart"
```

Or force deploy / webhook if restart alone does not reload compose env on this Coolify build.

Expected: deployment `finished` / containers recreated; `yt-ytdlp-mule` running.

---

### Task 3: Boot verification

- [ ] **Step 1: Confirm bgutil started**

Coolify logs or Loki for `yt-ytdlp-mule`:

```
bgutil server ready
```

Also accept: `Starting bgutil POT server on 127.0.0.1:4416` followed by ready within ~30s.

- [ ] **Step 2: Confirm worker banner**

Log line containing:

```
bgutil=True
```

(or `bgutil=1` / equivalent from `worker_ytdlp.py` start banner).

- [ ] **Step 3: Regress gate**

If container crash-loops or never prints ready → set `ENABLE_BGUTIL=0`, redeploy, stop Phase 1 as **regress**.

---

### Task 4: Observe 60–90 minutes

- [ ] **Step 1: Open dashboard**

https://grafana.app7.kelana5.com/d/videomasjid-ytdlp/videomasjid-e28094-video-downloader

Watch: OK vs FAIL, YouTube bot-check (`kind=bot`).

- [ ] **Step 2: After window, recount Loki 1h**

Same queries as Task 1. Compute success rate = OK / (OK+FAIL).

- [ ] **Step 3: Call outcome**

| Call | Rule |
|------|------|
| **Pass** | Sustained ≥55–60% OK, or bot rate ~halved vs baseline |
| **Flat** | Within noise of ~36% |
| **Regress** | Worse OK rate, or bgutil broke boot |

---

### Task 5: Record + handoff

**Files:**
- Modify: `memory-bank/ytdlp-selfhost-2026-07-09.md`
- Modify: `memory-bank/index.md`

- [ ] **Step 1: Write outcome** into ytdlp handoff (timestamp, OK/FAIL/bot, pass/flat/regress, leave `ENABLE_BGUTIL` as decided).

- [ ] **Step 2: Update index Next Steps** — Phase 2 only if measured and still needed.

- [ ] **Step 3: Commit plan/spec only if not already committed** (worker repo). Do not push unless operator asks.

---

## Rollback

```text
Coolify: ENABLE_BGUTIL=0 → redeploy/restart
```

No git revert required.

## Self-review vs spec

| Spec requirement | Task |
|------------------|------|
| Set ENABLE_BGUTIL=1 | Task 2 |
| Redeploy | Task 2 Step 3 |
| Boot: ready + bgutil=True | Task 3 |
| Observe 60–90m | Task 4 |
| Pass/flat/regress | Task 4 Step 3 |
| No cookies/residential/extra ports | Constraints honored throughout |
| No code harden in Phase 1 | No worker.py tasks |
| Memory bank record | Task 5 |
