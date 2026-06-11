#!/usr/bin/env python3
"""
Dashboard revamp, round 2.

Overview (videomasjid-json-v1):
  + new "Pipeline State (Supabase)" section right under the top stat row,
    querying Postgres directly (queue depths, in-flight, backlog,
    completions, failures, status distribution, completions/hour).

Deep Dive (videomasjid-deepdive):
  * Worker Fleet section (timeline + progress + subtitle completions)
  * Per-Worker Detail moved into a collapsed row (50 repeat tiles, w3 h4,
    8 per row) so the page no longer opens as a wall of tiny charts
  * Logs section unchanged at the bottom

Usage:
  set GRAFANA_AUTH=user:password
  python scripts/revamp_deepdive_and_db_overview.py [--dry-run]
"""

import base64
import os
import sys

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
OVERVIEW_UID = "videomasjid-json-v1"
DEEPDIVE_UID = "videomasjid-deepdive"
PG = {"type": "grafana-postgresql-datasource", "uid": "afoi7fmhpv3eoe"}
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}


def fetch(uid):
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()["dashboard"]


def push(dash, message):
    dash.pop("version", None)
    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        headers=HEADERS,
        json={"dashboard": dash, "overwrite": True, "message": message},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def sql_target(sql, fmt="table"):
    return [{
        "datasource": PG, "editorMode": "code", "format": fmt,
        "rawQuery": True, "rawSql": sql, "refId": "A",
    }]


def stat(pid, title, sql, x, w, desc="", thresholds=None, unit="none"):
    steps = [{"color": "green", "value": None}]
    for color, val in (thresholds or []):
        steps.append({"color": color, "value": val})
    return {
        "id": pid, "type": "stat", "title": title, "description": desc,
        "datasource": PG,
        "gridPos": {"h": 4, "w": w, "x": x, "y": 0},
        "fieldConfig": {"defaults": {
            "color": {"mode": "thresholds"},
            "thresholds": {"mode": "absolute", "steps": steps},
            "unit": unit, "noValue": "0",
        }, "overrides": []},
        "options": {
            "colorMode": "background", "graphMode": "none",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
        "targets": sql_target(sql),
    }


# ---------------- Overview: Pipeline State (Supabase) ----------------

DB_ROW_ID = 200

def db_section():
    row = {"id": DB_ROW_ID, "type": "row", "title": "Pipeline State (Supabase)",
           "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0}, "panels": []}
    stats = [
        stat(201, "Media Queue Depth",
             "SELECT count(*) FROM youtube.media_queue;", 0, 4,
             desc="Rows in youtube.media_queue. Scout backpressure cap MAX_MEDIA_QUEUE=360.",
             thresholds=[("yellow", 300), ("red", 350)]),
        stat(202, "In-Flight Downloads",
             "SELECT count(*) FROM youtube.media_queue WHERE locked_until > now();", 4, 4,
             desc="Queue rows currently locked by a media worker (50 workers)."),
        stat(203, "Backlog (queued+pending)",
             "SELECT count(*) FROM youtube.videos WHERE media_status IN ('queued','pending');", 8, 4,
             desc="Videos waiting for media download."),
        stat(204, "Completed (24h)",
             "SELECT count(*) FROM youtube.videos WHERE media_status='completed' "
             "AND updated_at > now() - interval '24 hours';", 12, 4,
             desc="Media downloads finished in the last 24h. ~14.5k expected at 50 workers."),
        stat(205, "Failed (media)",
             "SELECT count(*) FROM youtube.videos WHERE media_status='failed';", 16, 4,
             desc="Videos in media_status=failed (sweeper retries <3).",
             thresholds=[("yellow", 1), ("red", 50)]),
        stat(206, "Subtitle Queue Depth",
             "SELECT count(*) FROM youtube.subtitle_queue;", 20, 4,
             desc="Rows in youtube.subtitle_queue. Empty during media processing is expected "
                  "(subtitles serialized after media)."),
    ]
    dist = {
        "id": 207, "type": "bargauge", "title": "Videos by media_status",
        "datasource": PG,
        "gridPos": {"h": 8, "w": 8, "x": 0, "y": 4},
        "fieldConfig": {"defaults": {
            "color": {"mode": "thresholds"},
            "thresholds": {"mode": "absolute",
                           "steps": [{"color": "blue", "value": None}]},
        }, "overrides": []},
        "options": {
            "displayMode": "gradient", "orientation": "horizontal",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
            "showUnfilled": True,
        },
        "targets": sql_target(
            "SELECT media_status, count(*) AS videos FROM youtube.videos "
            "GROUP BY media_status ORDER BY videos DESC;"),
        "transformations": [{"id": "rowsToFields", "options": {}}],
    }
    per_hour = {
        "id": 208, "type": "timeseries", "title": "Completions per Hour (DB, 48h)",
        "description": "media_status flips to completed, bucketed hourly from updated_at.",
        "datasource": PG,
        "gridPos": {"h": 8, "w": 16, "x": 8, "y": 4},
        "fieldConfig": {"defaults": {
            "color": {"mode": "palette-classic"},
            "custom": {"drawStyle": "bars", "fillOpacity": 70, "lineWidth": 1,
                       "barAlignment": 0, "showPoints": "never"},
            "min": 0, "unit": "none",
        }, "overrides": []},
        "options": {"legend": {"displayMode": "hidden"},
                    "tooltip": {"mode": "single", "sort": "none"}},
        "targets": sql_target(
            "SELECT date_trunc('hour', updated_at) AS \"time\", count(*) AS completed "
            "FROM youtube.videos WHERE media_status='completed' "
            "AND updated_at > now() - interval '48 hours' "
            "GROUP BY 1 ORDER BY 1;", fmt="time_series"),
    }
    return [row] + stats + [dist, per_hour]


def patch_overview(ov):
    # drop a previous run of this section, then re-insert after top stats
    ov["panels"] = [p for p in ov["panels"] if not (200 <= p.get("id", 0) <= 219)]
    top = [p for p in ov["panels"] if p["gridPos"]["y"] == 0 and p["type"] == "stat"]
    rest = [p for p in ov["panels"] if p not in top]
    section = db_section()
    y = 4  # below the h=4 top stat row
    section[0]["gridPos"]["y"] = y
    for p in section[1:7]:
        p["gridPos"]["y"] = y + 1
    for p in section[7:]:
        p["gridPos"]["y"] = y + 5
    shift = 1 + 4 + 8  # row + stats + charts
    for p in rest:
        p["gridPos"]["y"] += shift
    ov["panels"] = top + section + rest
    return ov


# ---------------- Deep Dive restructure ----------------

def patch_deepdive(dd):
    by_id = {p["id"]: p for p in dd["panels"] if p["type"] != "row"}
    rows = {p["id"]: p for p in dd["panels"] if p["type"] == "row"}

    fleet_row = rows.get(70) or {"id": 70, "type": "row", "title": "Worker Fleet"}
    fleet_row.update({"title": "Worker Fleet", "collapsed": False, "panels": [],
                      "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0}})
    t71 = by_id[71]; t71["gridPos"] = {"h": 10, "w": 24, "x": 0, "y": 1}
    t72 = by_id[72]; t72["gridPos"] = {"h": 8, "w": 12, "x": 0, "y": 11}
    t74 = by_id[74]; t74["gridPos"] = {"h": 8, "w": 12, "x": 12, "y": 11}

    tile = by_id[81]
    tile["gridPos"] = {"h": 4, "w": 3, "x": 0, "y": 20}
    tile["maxPerRow"] = 8
    detail_row = {
        "id": 85, "type": "row", "title": "Per-Worker Detail (expand)",
        "collapsed": True,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 19},
        "panels": [tile],
    }

    logs_row = rows.get(20) or {"id": 20, "type": "row", "title": "Logs"}
    logs_row.update({"collapsed": False, "panels": [],
                     "gridPos": {"h": 1, "w": 24, "x": 0, "y": 20}})
    l21 = by_id[21]; l21["gridPos"] = {"h": 8, "w": 12, "x": 0, "y": 21}
    l22 = by_id[22]; l22["gridPos"] = {"h": 8, "w": 12, "x": 12, "y": 21}
    l23 = by_id[23]; l23["gridPos"] = {"h": 8, "w": 24, "x": 0, "y": 29}
    l24 = by_id[24]; l24["gridPos"] = {"h": 8, "w": 24, "x": 0, "y": 37}

    dd["panels"] = [fleet_row, t71, t72, t74, detail_row, logs_row, l21, l22, l23, l24]
    return dd


def main():
    dry = "--dry-run" in sys.argv
    ov = patch_overview(fetch(OVERVIEW_UID))
    dd = patch_deepdive(fetch(DEEPDIVE_UID))
    if dry:
        for name, d in (("OVERVIEW", ov), ("DEEPDIVE", dd)):
            print(f"== {name} ==")
            for p in d["panels"]:
                g = p["gridPos"]
                flag = " [collapsed]" if p.get("collapsed") else ""
                print(f"  {p['id']:>4} y={g['y']:<4} h={g['h']:<3} w={g['w']:<3} "
                      f"{p['type']:15} {p.get('title','')}{flag}")
        return
    r1 = push(ov, "Add Pipeline State (Supabase) section")
    r2 = push(dd, "Revamp: fleet section + collapsed per-worker tiles")
    print("overview:", r1.get("status"), r1.get("version"))
    print("deepdive:", r2.get("status"), r2.get("version"))


if __name__ == "__main__":
    main()
