#!/usr/bin/env python3
"""
Overview dashboard full revamp — best-practice layout.

Story order (top = most important):
  1. North-star stats: health, GB/24h vs 10TB/month target, videos/24h,
     median speed, API quota, blocked count
  2. Pipeline State (Supabase) — what the DB says is true
  3. Throughput & Downloads (Loki) — aggregate MB/s vs target pace,
     completions, speeds, errors
  4. Pipeline Activity (Loki) — scout/queue/persist event rates
  5. Operational Health — heartbeats, circuit, requeues, quota history

Fixes a lying metric: "Completed (24h)" counted videos.updated_at, which
subtitle writes also bump (showed ~11.9k; real completions ~1.5k).
Now sourced from youtube.media_files (one audio+video pair per video).

Removed: Live KPIs row (range-relative counters duplicating rate panels),
duplicate circuit stat. Proxy/subtitle detail panels move to deep-dive.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/revamp_overview_v2.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
OVERVIEW_UID = "videomasjid-json-v1"
DEEPDIVE_UID = "videomasjid-deepdive"
PG_DS = {"type": "grafana-postgresql-datasource", "uid": "afoi7fmhpv3eoe"}
LOKI_DS = {"type": "loki", "uid": "eflpdpm4rjim8b"}

# 10 TB / 30 days
TARGET_BYTES_DAY = 333e9
TARGET_BYTES_SEC = TARGET_BYTES_DAY / 86400  # ~3.86 MB/s

_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}


def fetch(uid):
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}",
                     headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()["dashboard"]


def push(dash, message):
    dash.pop("version", None)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS,
                      json={"dashboard": dash, "overwrite": True,
                            "message": message}, timeout=30)
    r.raise_for_status()
    return r.json()


def grid(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def row(rid, title, y):
    return {"id": rid, "type": "row", "title": title, "collapsed": False,
            "gridPos": grid(0, y, 24, 1), "panels": []}


def pg_stat(pid, title, sql, gp, unit="none", steps=None, decimals=None):
    return {
        "id": pid, "type": "stat", "title": title, "datasource": PG_DS,
        "gridPos": gp,
        "targets": [{"refId": "A", "datasource": PG_DS, "format": "table",
                     "rawQuery": True, "rawSql": sql}],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                    "colorMode": "value", "graphMode": "none"},
        "fieldConfig": {"defaults": {
            "unit": unit, "decimals": decimals,
            "thresholds": {"mode": "absolute", "steps": steps or
                           [{"color": "blue", "value": None}]}},
            "overrides": []},
    }


def main():
    ov = fetch(OVERVIEW_UID)
    by_id = {p["id"]: p for p in ov["panels"]}
    P = lambda i: by_id[i]

    # ---- 1. North-star stats -------------------------------------------
    y = 0
    top = []
    p = P(100); p["gridPos"] = grid(0, y, 4, 4); top.append(p)

    top.append(pg_stat(
        210, "Downloaded (24h)",
        "SELECT COALESCE(sum(file_size_bytes),0) FROM youtube.media_files "
        "WHERE created_at > now() - interval '24 hours'",
        grid(4, y, 4, 4), unit="decbytes",
        steps=[{"color": "red", "value": None},
               {"color": "yellow", "value": 280e9},
               {"color": "green", "value": TARGET_BYTES_DAY}]))

    cm = P(202)
    cm["title"] = "Videos Completed (24h)"
    cm["targets"][0]["rawSql"] = (
        "SELECT count(DISTINCT video_id) FROM youtube.media_files "
        "WHERE created_at > now() - interval '24 hours'")
    cm["fieldConfig"]["defaults"]["thresholds"]["steps"] = [
        {"color": "blue", "value": None}]
    cm["gridPos"] = grid(8, y, 4, 4); top.append(cm)

    p = P(101); p["gridPos"] = grid(12, y, 4, 4); top.append(p)
    p = P(103); p["gridPos"] = grid(16, y, 4, 4); top.append(p)
    p = P(104); p["gridPos"] = grid(20, y, 4, 4); top.append(p)
    y += 4

    # ---- 2. Pipeline State (Supabase) ----------------------------------
    sec_db = [row(200, "Pipeline State (Supabase)", y)]; y += 1
    p = P(201); p["gridPos"] = grid(0, y, 8, 6); sec_db.append(p)

    ch = P(206)
    ch["title"] = "Videos Completed per Hour (24h)"
    ch["targets"][0]["rawSql"] = (
        "SELECT date_trunc('hour', created_at) AS time, "
        "count(DISTINCT video_id) AS completed FROM youtube.media_files "
        "WHERE created_at > now() - interval '24 hours' "
        "GROUP BY 1 ORDER BY 1")
    ch["gridPos"] = grid(8, y, 8, 6); sec_db.append(ch)

    sec_db.append({
        "id": 211, "type": "timeseries", "title": "GB per Hour (24h)",
        "datasource": PG_DS, "timeFrom": "24h",
        "gridPos": grid(16, y, 8, 6),
        "targets": [{"refId": "A", "datasource": PG_DS,
                     "format": "time_series", "rawQuery": True,
                     "rawSql": "SELECT date_trunc('hour', created_at) AS time, "
                               "sum(file_size_bytes) AS bytes "
                               "FROM youtube.media_files "
                               "WHERE created_at > now() - interval '24 hours' "
                               "GROUP BY 1 ORDER BY 1"}],
        "options": {"legend": {"showLegend": False}},
        "fieldConfig": {"defaults": {"unit": "decbytes", "custom": {
            "drawStyle": "bars", "fillOpacity": 60, "lineWidth": 1}},
            "overrides": []},
    })
    y += 6
    p = P(203); p["gridPos"] = grid(0, y, 6, 3); sec_db.append(p)
    p = P(204); p["gridPos"] = grid(6, y, 6, 3); sec_db.append(p)
    p = P(205); p["gridPos"] = grid(12, y, 6, 3); sec_db.append(p)
    sec_db.append(pg_stat(
        212, "Failed (DB)",
        "SELECT count(*) FROM youtube.videos WHERE media_status='failed'",
        grid(18, y, 6, 3),
        steps=[{"color": "green", "value": None},
               {"color": "yellow", "value": 20},
               {"color": "red", "value": 100}]))
    y += 3

    # ---- 3. Throughput & Downloads (Loki) ------------------------------
    sec_tp = [row(220, "Throughput & Downloads (Loki)", y)]; y += 1
    sec_tp.append({
        "id": 213, "type": "timeseries",
        "title": "Aggregate Download Throughput (target 3.86 MB/s = 10 TB/mo)",
        "datasource": LOKI_DS,
        "gridPos": grid(0, y, 12, 8),
        "targets": [{"refId": "A", "datasource": LOKI_DS,
                     "expr": 'sum(sum_over_time({app="videomasjid",'
                             'service=~"yt-media-mule.*"} | json '
                             '| event="download_complete" '
                             '| unwrap downloaded_bytes [5m])) / 300',
                     "legendFormat": "throughput"}],
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["mean", "max", "lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {
            "unit": "Bps",
            "custom": {"lineWidth": 2, "fillOpacity": 12,
                       "thresholdsStyle": {"mode": "dashed"}},
            "thresholds": {"mode": "absolute", "steps": [
                {"color": "red", "value": None},
                {"color": "green", "value": TARGET_BYTES_SEC}]}},
            "overrides": []},
    })
    p = P(46); p["gridPos"] = grid(12, y, 12, 8); sec_tp.append(p)
    y += 8
    p = P(42); p["gridPos"] = grid(0, y, 12, 8); sec_tp.append(p)
    p = P(41); p["gridPos"] = grid(12, y, 12, 8); sec_tp.append(p)
    y += 8

    # ---- 4. Pipeline Activity (Loki) -----------------------------------
    sec_act = [row(230, "Pipeline Activity (Loki)", y)]; y += 1
    p = P(11); p["gridPos"] = grid(0, y, 12, 7); sec_act.append(p)
    p = P(12); p["gridPos"] = grid(12, y, 12, 7); sec_act.append(p)
    y += 7
    p = P(13); p["gridPos"] = grid(0, y, 12, 7); sec_act.append(p)
    p = P(14); p["gridPos"] = grid(12, y, 12, 7); sec_act.append(p)
    y += 7

    # ---- 5. Operational Health -----------------------------------------
    sec_oh = [row(30, "Operational Health", y)]
    P(30)["gridPos"] = grid(0, y, 24, 1)
    sec_oh[0] = P(30); y += 1
    p = P(31); p["gridPos"] = grid(0, y, 4, 4); sec_oh.append(p)
    p = P(32); p["gridPos"] = grid(4, y, 4, 4); sec_oh.append(p)
    p = P(33); p["gridPos"] = grid(8, y, 4, 4); sec_oh.append(p)
    p = P(34); p["gridPos"] = grid(12, y, 12, 4); sec_oh.append(p)
    y += 4
    p = P(35); p["gridPos"] = grid(0, y, 8, 7); sec_oh.append(p)
    p = P(38); p["gridPos"] = grid(8, y, 8, 7); sec_oh.append(p)
    p = P(36); p["gridPos"] = grid(16, y, 8, 7); sec_oh.append(p)
    y += 7
    p = P(37); p["gridPos"] = grid(0, y, 12, 8); sec_oh.append(p)
    p = P(51); p["gridPos"] = grid(12, y, 12, 4); sec_oh.append(p)

    ov["panels"] = top + sec_db + sec_tp + sec_act + sec_oh
    r = push(ov, "Full revamp: north-star stats, DB truth, throughput "
                 "target, activity, health; Live KPIs removed")
    print("overview:", r.get("status"), "v", r.get("version"))

    # ---- Deep-dive: take proxy/subtitle detail panels ------------------
    dd = fetch(DEEPDIVE_UID)
    dd_ids = {p["id"] for p in dd["panels"]}
    if 43 not in dd_ids:
        moved = [by_id[43], by_id[44], by_id[45]]
        # find logs row y; insert section before it
        logs_row = next(p for p in dd["panels"] if p["id"] == 20)
        ly = logs_row["gridPos"]["y"]
        sec = [row(86, "Proxy & Subtitle Detail", ly)]
        for i, p in enumerate(moved):
            p["gridPos"] = grid(i * 8, ly + 1, 8, 7)
            sec.append(p)
        shift = 8
        for p in dd["panels"]:
            if p["gridPos"]["y"] >= ly:
                p["gridPos"]["y"] += shift
        dd["panels"] = dd["panels"] + sec
        dd["panels"].sort(key=lambda p: p["gridPos"]["y"])
        r = push(dd, "Add Proxy & Subtitle Detail section (moved from overview)")
        print("deepdive:", r.get("status"), "v", r.get("version"))


if __name__ == "__main__":
    main()
