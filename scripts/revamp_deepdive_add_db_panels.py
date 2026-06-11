#!/usr/bin/env python3
"""
Dashboard revamp round 2.

Overview (videomasjid-json-v1):
  + new "Pipeline State (Supabase)" section right after the top stat row,
    backed by the Supabase Postgres datasource — status breakdown,
    completions/hour, queue depths, backlog. Loki tells what workers DO;
    these panels tell what the DATABASE says is true.

Deep Dive (videomasjid-deepdive):
  - Worker Fleet section: timeline + progress/subtitle gauges
  - Per-worker task manager (50 repeat panels) moved into a COLLAPSED row
    so the page opens clean
  - Logs rearranged into a 2x2 grid

Usage:
  set GRAFANA_AUTH=user:password
  python scripts/revamp_deepdive_add_db_panels.py [--dry-run]
"""

import base64
import os
import sys

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
OVERVIEW_UID = "videomasjid-json-v1"
DEEPDIVE_UID = "videomasjid-deepdive"
PG_DS = {"type": "grafana-postgresql-datasource", "uid": "afoi7fmhpv3eoe"}

_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}


def fetch(uid):
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()["dashboard"]


def push(dash, message):
    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        headers=HEADERS,
        json={"dashboard": dash, "overwrite": True, "message": message},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def sql_target(sql, fmt="table", ref="A"):
    return {
        "refId": ref,
        "datasource": PG_DS,
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql,
    }


def stat(pid, title, sql, x, y, w=4, h=3, unit="none", thresholds=None):
    p = {
        "id": pid, "type": "stat", "title": title,
        "datasource": PG_DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [sql_target(sql)],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                    "colorMode": "value", "graphMode": "none"},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {
            "mode": "absolute",
            "steps": thresholds or [{"color": "green", "value": None}],
        }}, "overrides": []},
    }
    return p


def db_section_panels(y):
    """Pipeline State (Supabase) section. Returns (panels, next_y)."""
    row = {"id": 200, "type": "row", "title": "Pipeline State (Supabase)",
           "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
           "panels": []}
    y += 1
    panels = [row]

    panels.append({
        "id": 201, "type": "bargauge", "title": "Videos by Media Status",
        "datasource": PG_DS,
        "gridPos": {"x": 0, "y": y, "w": 8, "h": 6},
        "targets": [sql_target(
            "SELECT media_status AS metric, count(*)::float AS value "
            "FROM youtube.videos GROUP BY media_status ORDER BY value DESC")],
        "options": {"displayMode": "gradient", "orientation": "horizontal",
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                    "valueMode": "color", "showUnfilled": True},
        "fieldConfig": {"defaults": {"unit": "none", "thresholds": {
            "mode": "absolute", "steps": [{"color": "blue", "value": None}]}},
            "overrides": []},
    })

    panels.append({
        "id": 206, "type": "timeseries", "title": "Completions per Hour (DB, 24h)",
        "datasource": PG_DS, "timeFrom": "24h",
        "gridPos": {"x": 8, "y": y, "w": 8, "h": 6},
        "targets": [sql_target(
            "SELECT date_trunc('hour', updated_at) AS time, "
            "count(*) AS completions FROM youtube.videos "
            "WHERE media_status='completed' "
            "AND updated_at > now() - interval '24 hours' "
            "GROUP BY 1 ORDER BY 1", fmt="time_series")],
        "options": {"legend": {"showLegend": False}},
        "fieldConfig": {"defaults": {"unit": "none", "custom": {
            "drawStyle": "bars", "fillOpacity": 60, "lineWidth": 1}},
            "overrides": []},
    })

    panels.append(stat(
        202, "Completed (24h)",
        "SELECT count(*) FROM youtube.videos WHERE media_status='completed' "
        "AND updated_at > now() - interval '24 hours'",
        x=16, y=y,
        thresholds=[{"color": "red", "value": None},
                    {"color": "yellow", "value": 10000},
                    {"color": "green", "value": 13900}]))  # 14k/day ~= 10TB/mo

    panels.append(stat(
        203, "Backlog (pending+queued+ready)",
        "SELECT count(*) FROM youtube.videos "
        "WHERE media_status IN ('pending','queued','ready_for_download')",
        x=20, y=y))

    panels.append(stat(
        204, "Media Queue Depth",
        "SELECT count(*) FROM youtube.media_queue",
        x=16, y=y + 3,
        thresholds=[{"color": "red", "value": None},
                    {"color": "green", "value": 50},
                    {"color": "yellow", "value": 350}]))  # cap 360: red=starved, yellow=at cap

    panels.append(stat(
        205, "Subtitle Queue Depth",
        "SELECT count(*) FROM youtube.subtitle_queue",
        x=20, y=y + 3))

    return panels, y + 6


def patch_overview(dry):
    ov = fetch(OVERVIEW_UID)
    # Drop any previous run of this section, keep everything else.
    keep = [p for p in ov["panels"] if not (200 <= p["id"] <= 219)]
    top = [p for p in keep if p["type"] != "row" and p["gridPos"]["y"] < 4]
    rest = [p for p in keep if p not in top]
    db_panels, next_y = db_section_panels(4)
    shift = next_y - 4
    for p in rest:
        p["gridPos"]["y"] += shift
    ov["panels"] = top + db_panels + rest
    if dry:
        dump("OVERVIEW", ov)
        return
    ov.pop("version", None)
    r = push(ov, "Add Pipeline State (Supabase) DB section")
    print("overview:", r.get("status"), r.get("version"))


def patch_deepdive(dry):
    dd = fetch(DEEPDIVE_UID)
    by_id = {p["id"]: p for p in dd["panels"]}
    fleet_row = by_id[70]
    fleet_row["title"] = "Worker Fleet"
    timeline, progress, subs = by_id[71], by_id[72], by_id[74]
    # Worker repeat panel lives either top-level (id 81) or inside an
    # existing collapsed row (e.g. id 85 "Per-Worker Detail").
    if 81 in by_id:
        worker_repeat = by_id[81]
    else:
        nested = [c for p in dd["panels"] if p["type"] == "row"
                  for c in p.get("panels", []) if c["id"] == 81]
        if not nested:
            raise SystemExit("worker repeat panel (id 81) not found")
        worker_repeat = nested[0]
    logs_row = by_id[20]
    scout, queue_ops, persist, warns = by_id[21], by_id[22], by_id[23], by_id[24]

    y = 0
    fleet_row["gridPos"] = {"x": 0, "y": y, "w": 24, "h": 1}; y += 1
    timeline["gridPos"] = {"x": 0, "y": y, "w": 24, "h": 10}; y += 10
    progress["gridPos"] = {"x": 0, "y": y, "w": 12, "h": 8}
    subs["gridPos"] = {"x": 12, "y": y, "w": 12, "h": 8}; y += 8

    # Per-worker task manager: 50 repeat panels inside a collapsed row.
    worker_repeat["gridPos"] = {"x": 0, "y": y + 1, "w": 6, "h": 6}
    tm_row = by_id.get(85) or {"id": 300, "type": "row", "panels": []}
    tm_row.update({"type": "row",
                   "title": "Per-Worker Task Manager (expand — 50 panels)",
                   "collapsed": True,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
                   "panels": [worker_repeat]})
    y += 1

    logs_row["gridPos"] = {"x": 0, "y": y, "w": 24, "h": 1}; y += 1
    scout["gridPos"] = {"x": 0, "y": y, "w": 12, "h": 10}
    queue_ops["gridPos"] = {"x": 12, "y": y, "w": 12, "h": 10}; y += 10
    persist["gridPos"] = {"x": 0, "y": y, "w": 12, "h": 10}
    warns["gridPos"] = {"x": 12, "y": y, "w": 12, "h": 10}; y += 10

    dd["panels"] = [fleet_row, timeline, progress, subs, tm_row,
                    logs_row, scout, queue_ops, persist, warns]
    if dry:
        dump("DEEPDIVE", dd)
        return
    dd.pop("version", None)
    r = push(dd, "Revamp: fleet section, collapsed task manager, 2x2 logs")
    print("deepdive:", r.get("status"), r.get("version"))


def dump(name, d):
    print(f"== {name} ==")
    for p in d["panels"]:
        g = p["gridPos"]
        flag = " [collapsed row]" if p.get("collapsed") else ""
        print(f"  {p['id']:>4} y={g['y']:<4} h={g['h']:<3} w={g['w']:<3} "
              f"{p['type']:15} {p.get('title','')}{flag}")
        for c in p.get("panels", []):
            print(f"       -> {c['id']} {c.get('title','')}".encode("ascii", "replace").decode())


def main():
    dry = "--dry-run" in sys.argv
    patch_overview(dry)
    patch_deepdive(dry)


if __name__ == "__main__":
    main()
