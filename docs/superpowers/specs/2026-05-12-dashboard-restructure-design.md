# Dashboard Restructure Design
**Date:** 2026-05-12  
**Dashboard:** VideoMasjid Pipeline (JSON Native) — UID `videomasjid-json-v1`  
**Grafana:** `https://grafana.app7.kelana5.com`

---

## Context

Dashboard grew organically over multiple sessions. Current version 12 has three structural problems:

1. **Logs are too prominent** — 4 full-width log panels occupy rows 23–55 (32 rows), appearing before operational metrics. Users see raw JSON walls before seeing pipeline health.
2. **Redundant panels** — download speed shown 3 ways; scout logs split across 2 separate sections; media live-progress logs duplicate the task manager view.
3. **Broken layout** — "API Quota" row header at y=94 but its bargauge at y=160 (separated by the entire worker status + task manager block).

Goal: restructure to industry-standard Grafana layout (metrics first, logs last), remove redundant panels, fix broken positioning.

---

## Current Panel Inventory

| ID | Title | Type | Section | Action |
|----|-------|------|---------|--------|
| 1 | Live KPIs | row | — | Keep |
| 2–7 | KPI stats (calls, claims, uploads, writes, failures) | stat | Live KPIs | Keep |
| 10 | Rates | row | — | Keep |
| 11–14 | Rate timeseries (scout, queue, media persist, subtitle persist) | timeseries | Rates | Keep |
| 20 | Live Logs | row | — | **Rename → "Logs", move to bottom** |
| 21 | Scout API Calls | logs | Live Logs | **Merge into new Scout Log panel** |
| 22 | Queue Operations | logs | Live Logs | Move to bottom section |
| 23 | Persistence Writes (DB + S3) | logs | Live Logs | Move to bottom section |
| 24 | Warnings & Errors | logs | Live Logs | Move to bottom section |
| 30 | Operational Health | row | — | **Move up (above Download Performance)** |
| 31–35 | Liveness stats + backpressure timeseries | stat/timeseries | Operational Health | Keep |
| 36–37 | Requeue rate + Circuit Breaker events | timeseries/logs | Operational Health | Keep |
| 40 | Download Performance | row | — | Keep |
| 41–46 | Error breakdown, speeds, proxy, completions | timeseries | Download Performance | Keep |
| 50 | API Quota | row | — | **Remove** (misplaced, redundant with new placement) |
| 51 | RapidAPI Quota Remaining | bargauge | API Quota | **Move to section ⑥** |
| 70 | Live Worker Status | row | — | Keep |
| 71 | Activity Timeline | state-timeline | Live Worker Status | Keep |
| 72 | Download Progress (%) | bargauge | Live Worker Status | Keep |
| 73 | Download Speed (MB/s) | bargauge | Live Worker Status | **Remove** (dup of panel 42 + task mgr) |
| 74 | Subtitle Completions | bargauge | Live Worker Status | Keep |
| 75 | Scout — Live Fetch Activity | logs | Live Worker Status | **Merge into new Scout Log panel** |
| 76 | Media Workers Live Download Progress | logs | Live Worker Status | **Remove** (dup of task manager) |
| 80 | Task Manager row | row | — | **Move into Live Worker Status section** |
| 81 | Worker $worker_idx (repeat timeseries) | timeseries | Task Manager | Keep, move up |
| 82 | W$worker_idx — Current Video (repeat stat) | stat | Task Manager | Keep, move up |

---

## Proposed Structure (7 Sections)

```
① Live KPIs          — unchanged
② Rates              — unchanged  
③ Operational Health — moved up (was ④)
④ Download Perf      — unchanged content, moved up (was ⑤)
⑤ Live Worker Status — task manager merged in; 3 redundant panels removed
⑥ API Quota          — fixed position (immediately after workers)
⑦ Logs               — new bottom section, 2-column grid
```

---

## Changes Detail

### Remove (3 panels)

| Panel | Reason |
|-------|--------|
| 73 — Media Download Speed bargauge | Duplicate of panel 42 (timeseries, same data) AND task manager (panel 81) |
| 76 — Media Workers Live Download Progress logs | Duplicate of task manager view (panel 81 shows per-worker speed) |
| 50 — "API Quota" row header | Misplaced (y=94 but content at y=160); new row header added at correct position |

### Merge (2 → 1 panel)

**New "Scout Log" panel** combines:
- Panel 21 "Scout API Calls" — filter: `event=~"api_call_start|api_call_done|queue_enqueue"`
- Panel 75 "Scout — Live Fetch Activity" — filter: `service=~"yt-scout.*"`

Unified filter: `{app="videomasjid", service=~"yt-scout.*"} | json` — covers all scout events in one panel.

### Move

| What | From | To |
|------|------|----|
| Operational Health section | After Live Logs | After Rates |
| Download Performance section | After Operational Health | After Operational Health (same relative order) |
| Task Manager (row 80, panels 81, 82) | After Live Worker Status | Inside Live Worker Status section |
| API Quota bargauge (panel 51) | y=160 (stranded at bottom) | After Live Worker Status |
| All log panels (21→merged, 22, 23, 24) | y=23–55 | Bottom "Logs" section |

### Bottom Logs Section Layout (2-column grid)

```
┌─────────────────────────┬─────────────────────────┐
│  Scout Log              │  Queue Operations        │
│  (merged 21+75)         │  (panel 22)              │
│  w=12                   │  w=12                    │
├─────────────────────────┴─────────────────────────┤
│  Persistence Writes (DB + S3)  (panel 23)  w=24   │
├───────────────────────────────────────────────────┤
│  Warnings & Errors  (panel 24)  w=24              │
└───────────────────────────────────────────────────┘
```

### Rename

| Panel | Old Title | New Title |
|-------|-----------|-----------|
| 21 (merged) | Scout API Calls | Scout Log |
| 20 row | Live Logs | Logs |

---

## Implementation Notes

- Deploy via GitHub push only — Coolify webhook fires automatically, no MCP/API deploy
- All changes are Grafana dashboard JSON only — no worker code changes
- Use Grafana HTTP API: GET dashboard → patch panels/gridPos → POST with `overwrite: true`
- Panel IDs preserved where possible; merged scout panel reuses ID 21
- Template variable `worker_idx` (0–19) unchanged — used by repeat panels 81, 82

---

## Verification

1. Open `https://grafana.app7.kelana5.com` after push
2. Confirm section order: KPIs → Rates → Operational Health → Download Perf → Worker Status → API Quota → Logs
3. Confirm panels 73 and 76 are gone
4. Confirm Scout Log panel shows both API call events AND queue_enqueue events
5. Confirm API Quota bargauge appears directly under its row header
6. Confirm task manager panels appear inside Live Worker Status section
7. Confirm Warnings & Errors log at very bottom
8. Confirm dashboard still refreshes at 10s interval
