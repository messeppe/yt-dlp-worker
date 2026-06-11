#!/usr/bin/env python3
"""
Add a "Failures & Retries (Supabase)" section to the overview.

Failures were only visible as Loki event rates — nothing answered
"what failed, why, and how stuck is it". Found while building:
4,295 subtitle failures invisible on the old dashboard.

New panels (all DB-backed):
  241 table    Recent Media Failures (id, title, retries, error, when)
  242 bargauge Media Failure Reasons
  243 bargauge Subtitle Failure Reasons
  212 stat     Media Failed (moved from Pipeline State)
  244 stat     Subtitle Failed
  245 stat     Stuck (retry >= 3, not completed)
  246 stat     Extractor Blocked (total, DB)
  247 bargauge Subtitles by Status
Plus: 214 stat Backlog Drain (days) fills the Pipeline State slot 212
vacated — backlog divided by the last-24h completion rate.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/add_failures_section.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
UID = "videomasjid-json-v1"
PG_DS = {"type": "grafana-postgresql-datasource", "uid": "afoi7fmhpv3eoe"}
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

SECTION_H = 18  # 1 row header + 9 table + 4 stats + 4 subtitle bargauge


def t(sql, fmt="table"):
    return {"refId": "A", "datasource": PG_DS, "format": fmt,
            "rawQuery": True, "rawSql": sql}


def grid(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def stat(pid, title, sql, gp, steps=None, unit="none"):
    return {"id": pid, "type": "stat", "title": title, "datasource": PG_DS,
            "gridPos": gp, "targets": [t(sql)],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                        "colorMode": "value", "graphMode": "none"},
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": {
                "mode": "absolute",
                "steps": steps or [{"color": "blue", "value": None}]}},
                "overrides": []}}


def bargauge(pid, title, sql, gp, color="orange"):
    return {"id": pid, "type": "bargauge", "title": title, "datasource": PG_DS,
            "gridPos": gp, "targets": [t(sql)],
            "options": {"displayMode": "gradient",
                        "orientation": "horizontal",
                        "reduceOptions": {"calcs": ["lastNotNull"]},
                        "valueMode": "color", "showUnfilled": True},
            "fieldConfig": {"defaults": {"unit": "none", "thresholds": {
                "mode": "absolute",
                "steps": [{"color": color, "value": None}]}},
                "overrides": []}}


def build_section(y0):
    y = y0
    panels = [{"id": 240, "type": "row",
               "title": "Failures & Retries (Supabase)", "collapsed": False,
               "gridPos": grid(0, y, 24, 1), "panels": []}]
    y += 1
    panels.append({
        "id": 241, "type": "table", "title": "Recent Media Failures",
        "datasource": PG_DS, "gridPos": grid(0, y, 14, 9),
        "targets": [t(
            "SELECT id, left(title,60) AS title, media_retry_count AS retries, "
            "left(media_last_error,90) AS error, updated_at AS last_attempt "
            "FROM youtube.videos "
            "WHERE media_status='failed' "
            "   OR (media_status='pending' AND media_retry_count >= 3) "
            "ORDER BY updated_at DESC LIMIT 25")],
        "options": {"showHeader": True, "sortBy": []},
        "fieldConfig": {"defaults": {"custom": {"filterable": True}},
                        "overrides": []},
    })
    panels.append(bargauge(
        242, "Media Failure Reasons",
        "SELECT left(coalesce(media_last_error,'(none)'),60) AS metric, "
        "count(*)::float AS value FROM youtube.videos "
        "WHERE media_status='failed' GROUP BY 1 ORDER BY 2 DESC LIMIT 8",
        grid(14, y, 10, 5), color="red"))
    panels.append(bargauge(
        243, "Subtitle Failure Reasons",
        "SELECT left(coalesce(subtitle_last_error,'(none)'),60) AS metric, "
        "count(*)::float AS value FROM youtube.videos "
        "WHERE subtitle_status='failed' GROUP BY 1 ORDER BY 2 DESC LIMIT 8",
        grid(14, y + 5, 10, 4), color="orange"))
    y += 9
    panels.append(stat(
        212, "Media Failed",
        "SELECT count(*) FROM youtube.videos WHERE media_status='failed'",
        grid(0, y, 6, 4),
        steps=[{"color": "green", "value": None},
               {"color": "yellow", "value": 20},
               {"color": "red", "value": 100}]))
    panels.append(stat(
        244, "Subtitle Failed",
        "SELECT count(*) FROM youtube.videos WHERE subtitle_status='failed'",
        grid(6, y, 6, 4)))
    panels.append(stat(
        245, "Stuck (retry ≥ 3)",
        "SELECT count(*) FROM youtube.videos "
        "WHERE media_retry_count >= 3 AND media_status <> 'completed'",
        grid(12, y, 6, 4),
        steps=[{"color": "green", "value": None},
               {"color": "yellow", "value": 25},
               {"color": "red", "value": 100}]))
    panels.append(stat(
        246, "Extractor Blocked (total)",
        "SELECT count(*) FROM youtube.videos WHERE extractor_blocked",
        grid(18, y, 6, 4)))
    y += 4
    panels.append(bargauge(
        247, "Subtitles by Status",
        "SELECT subtitle_status AS metric, count(*)::float AS value "
        "FROM youtube.videos WHERE subtitle_status IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC",
        grid(0, y, 24, 4), color="blue"))
    return panels


def main():
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{UID}",
                     headers=HEADERS, timeout=30)
    r.raise_for_status()
    dash = r.json()["dashboard"]
    panels = [p for p in dash["panels"]
              if p["id"] not in (240, 241, 242, 243, 244, 245, 246, 247)]

    # Replace stat 212 (moves into the new section) with Backlog Drain ETA.
    old212 = next((p for p in panels if p["id"] == 212), None)
    if old212:
        gp = old212["gridPos"]
        panels.remove(old212)
        panels.append(stat(
            214, "Backlog Drain (days)",
            "SELECT round((SELECT count(*) FROM youtube.videos WHERE "
            "media_status IN ('pending','queued','ready_for_download'))"
            "::numeric / GREATEST((SELECT count(DISTINCT video_id) FROM "
            "youtube.media_files WHERE created_at > now() - interval "
            "'24 hours'),1), 1)",
            dict(gp),
            steps=[{"color": "green", "value": None},
                   {"color": "yellow", "value": 7},
                   {"color": "red", "value": 21}]))

    # Insert section where Pipeline Activity starts; shift below panels.
    act_row = next(p for p in panels if p.get("id") == 230)
    y0 = act_row["gridPos"]["y"]
    for p in panels:
        if p["gridPos"]["y"] >= y0:
            p["gridPos"]["y"] += SECTION_H
    panels.extend(build_section(y0))
    panels.sort(key=lambda p: (p["gridPos"]["y"], p["gridPos"]["x"]))

    dash["panels"] = panels
    dash.pop("version", None)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS,
                      json={"dashboard": dash, "overwrite": True,
                            "message": "Add Failures & Retries section; "
                                       "backlog drain ETA"}, timeout=30)
    r.raise_for_status()
    print("overview:", r.json().get("status"), "v", r.json().get("version"))


if __name__ == "__main__":
    main()
