#!/usr/bin/env python3
"""
Add an "API Health (RapidAPI)" section to the overview dashboard.

Motivation: 2026-06-12 outage — RapidAPI returned HTTP 400 "Unknown error
occurred" on every /download.php call for ~2h. Nothing on the dashboard
answered "is the API down, and which endpoint?" even though scout already
emits `api_call_done` events with `endpoint` + `status` fields per call.

New panels (all Loki-backed, ids 260-265):
  260 row        API Health (RapidAPI)
  261 stat       /download.php errors (15m)   — status >= 400 count
  262 stat       /subtitle.php errors (15m)   — status >= 400 count
  263 stat       API calls OK (15m)           — status < 400 count (traffic proof)
  264 stat       Canary API-down events (1h)  — event="CANARY"
  265 timeseries API calls by endpoint & HTTP status

Caveat encoded in panel descriptions: HTTP 200 with status:"error" body is
NOT visible here (shows as OK) — the canary stat + circuit panels cover that
failure mode.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/add_api_health_section.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
UID = "videomasjid-json-v1"
LOKI_DS = {"type": "loki", "uid": "eflpdpm4rjim8b"}
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

SECTION_H = 5  # 1 row header + 4 panels
SCOUT = '{app="videomasjid",service="yt-scout"}'


def lt(expr, legend=""):
    tgt = {"refId": "A", "datasource": LOKI_DS, "expr": expr}
    if legend:
        tgt["legendFormat"] = legend
    return tgt


def grid(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def stat(pid, title, expr, gp, steps, desc=""):
    return {"id": pid, "type": "stat", "title": title, "datasource": LOKI_DS,
            "description": desc, "gridPos": gp, "targets": [lt(expr)],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                        "colorMode": "value", "graphMode": "none"},
            "fieldConfig": {"defaults": {"unit": "none", "thresholds": {
                "mode": "absolute", "steps": steps}}, "overrides": []}}


def err_steps():
    return [{"color": "green", "value": None},
            {"color": "yellow", "value": 1},
            {"color": "red", "value": 5}]


def build_section(y0):
    y = y0
    panels = [{"id": 260, "type": "row", "title": "API Health (RapidAPI)",
               "collapsed": False, "gridPos": grid(0, y, 24, 1), "panels": []}]
    y += 1
    desc_err = ("HTTP >= 400 responses from scout api_call_done events. "
                "NOTE: HTTP 200 with status:\"error\" body is NOT counted here — "
                "watch Canary + circuit panels for that mode.")
    panels.append(stat(
        261, "/download.php errors (15m)",
        f'sum(count_over_time({SCOUT} | json | event="api_call_done" '
        f'| endpoint="/download.php" | status >= 400 [15m])) or vector(0)',
        grid(0, y, 4, 4), err_steps(), desc_err))
    panels.append(stat(
        262, "/subtitle.php errors (15m)",
        f'sum(count_over_time({SCOUT} | json | event="api_call_done" '
        f'| endpoint="/subtitle.php" | status >= 400 [15m])) or vector(0)',
        grid(4, y, 4, 4), err_steps(), desc_err))
    panels.append(stat(
        263, "API calls OK (15m)",
        f'sum(count_over_time({SCOUT} | json | event="api_call_done" '
        f'| status < 400 [15m])) or vector(0)',
        grid(8, y, 4, 4),
        [{"color": "red", "value": None}, {"color": "green", "value": 1}],
        "Successful HTTP calls — proves scout has traffic. 0 + 0 errors = "
        "scout idle (backpressure or circuit open), not necessarily healthy."))
    panels.append(stat(
        264, "Canary API-down (1h)",
        f'sum(count_over_time({SCOUT} | json | event="CANARY" [1h])) '
        f'or vector(0)',
        grid(12, y, 4, 4),
        [{"color": "green", "value": None}, {"color": "red", "value": 1}],
        "Canary probe declared the API down/degraded (catches HTTP-200 "
        "error-body mode that the error stats miss)."))
    panels.append({
        "id": 265, "type": "timeseries",
        "title": "API calls by endpoint & HTTP status",
        "datasource": LOKI_DS, "gridPos": grid(16, y, 8, 4),
        "description": "Which endpoint is failing and with what HTTP code.",
        "targets": [lt(
            f'sum by (endpoint, status) (count_over_time({SCOUT} | json '
            f'| event="api_call_done" [$__interval]))',
            legend="{{endpoint}} HTTP {{status}}")],
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi"}},
        "fieldConfig": {"defaults": {"unit": "short", "custom": {
            "drawStyle": "bars", "fillOpacity": 60, "stacking": {
                "mode": "normal"}}}, "overrides": []},
    })
    return panels


def verify(exprs):
    ok = True
    for expr in exprs:
        r = requests.post(f"{GRAFANA_URL}/api/ds/query", headers=HEADERS, json={
            "queries": [{"refId": "A", "datasource": LOKI_DS, "expr": expr,
                         "queryType": "instant", "intervalMs": 60000,
                         "maxDataPoints": 100}],
            "from": "now-1h", "to": "now"}, timeout=60)
        body = r.json()
        frames = body.get("results", {}).get("A", {}).get("frames", [])
        status = "OK" if r.status_code == 200 and frames else "EMPTY/FAIL"
        if status != "OK":
            ok = False
        print(f"  {status}: {expr[:100]}")
    return ok


def main():
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{UID}",
                     headers=HEADERS, timeout=30)
    r.raise_for_status()
    dash = r.json()["dashboard"]
    panels = [p for p in dash["panels"]
              if p["id"] not in (260, 261, 262, 263, 264, 265)]

    # Insert above the Failures & Retries row (id 240); shift rest down.
    anchor = next(p for p in panels if p.get("id") == 240)
    y0 = anchor["gridPos"]["y"]
    for p in panels:
        if p["gridPos"]["y"] >= y0:
            p["gridPos"]["y"] += SECTION_H
    panels.extend(build_section(y0))
    panels.sort(key=lambda p: (p["gridPos"]["y"], p["gridPos"]["x"]))

    dash["panels"] = panels
    dash.pop("version", None)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS,
                      json={"dashboard": dash, "overwrite": True,
                            "message": "Add API Health (RapidAPI) section"},
                      timeout=30)
    r.raise_for_status()
    print("overview:", r.json().get("status"), "v", r.json().get("version"))

    print("verifying queries end-to-end:")
    verify([
        f'sum(count_over_time({SCOUT} | json | event="api_call_done" '
        f'| endpoint="/download.php" | status >= 400 [15m])) or vector(0)',
        f'sum(count_over_time({SCOUT} | json | event="api_call_done" '
        f'| status < 400 [15m])) or vector(0)',
        f'sum by (endpoint, status) (count_over_time({SCOUT} | json '
        f'| event="api_call_done" [5m]))',
    ])


if __name__ == "__main__":
    main()
