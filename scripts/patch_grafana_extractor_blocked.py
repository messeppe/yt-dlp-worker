#!/usr/bin/env python3
"""Add an EXTRACTOR-BLOCKED stat panel (id=104) to dashboard videomasjid-json-v1.

Counts scout EXTRACTOR-BLOCKED events in last 24h.
Idempotent: if panel id=104 exists, updates in place.

Run: GRAFANA_PASSWORD=... python worker/scripts/patch_grafana_extractor_blocked.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from base64 import b64encode

GRAFANA = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com").rstrip("/")
DASH_UID = os.environ.get("GRAFANA_DASH_UID", "videomasjid-json-v1")
USER = os.environ.get("GRAFANA_USER", "admin")
PASSWORD = os.environ.get("GRAFANA_PASSWORD", "")
PANEL_ID = 104

LOKI_DATASOURCE = {"type": "loki", "uid": "eflpdpm4rjim8b"}


def auth_header() -> str:
    return "Basic " + b64encode(f"{USER}:{PASSWORD}".encode()).decode()


def http_json(method: str, url: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", auth_header())
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read().decode())


def build_panel() -> dict:
    return {
        "id": PANEL_ID,
        "type": "stat",
        "title": "Extractor Blocked (24h)",
        "description": (
            "Count of EXTRACTOR-BLOCKED events in the last 24h. "
            "Fires when scout permanently flags a video as un-extractable "
            "by RapidAPI (age/graphic-gated, geo-blocked, etc.). "
            "Operator un-flag: UPDATE youtube.videos SET extractor_blocked=false, "
            "scout_retry_count=0, media_status='queued' WHERE id=<vid>"
        ),
        "datasource": LOKI_DATASOURCE,
        "gridPos": {"h": 4, "w": 4, "x": 20, "y": 0},
        "targets": [
            {
                "refId": "A",
                "datasource": LOKI_DATASOURCE,
                "expr": (
                    'sum(count_over_time({app="videomasjid",service=~"yt-scout.*"} '
                    '| json | event="EXTRACTOR-BLOCKED" [24h]))'
                ),
                "queryType": "instant",
                "instant": True,
                "range": False,
                "legendFormat": "blocked",
            }
        ],
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 1},
                        {"color": "red", "value": 5},
                    ],
                },
                "noValue": "0",
                "unit": "short",
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False,
            },
            "textMode": "value",
            "colorMode": "background",
            "graphMode": "none",
            "justifyMode": "auto",
        },
    }


def main() -> int:
    if not PASSWORD:
        print("Set GRAFANA_PASSWORD", file=sys.stderr)
        return 2

    payload = http_json("GET", f"{GRAFANA}/api/dashboards/uid/{DASH_UID}")
    dash = payload["dashboard"]
    meta = payload.get("meta", {})

    panels = dash.setdefault("panels", [])
    new_panel = build_panel()
    for i, p in enumerate(panels):
        if p.get("id") == PANEL_ID:
            panels[i] = new_panel
            print(f"Updated existing panel id={PANEL_ID}")
            break
    else:
        panels.append(new_panel)
        print(f"Added new panel id={PANEL_ID}")

    result = http_json(
        "POST",
        f"{GRAFANA}/api/dashboards/db",
        {
            "dashboard": dash,
            "overwrite": True,
            "folderId": meta.get("folderId", 0),
            "message": "Add EXTRACTOR-BLOCKED stat panel (id=104)",
        },
    )
    print("Saved:", result.get("status"), "v=", result.get("version"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
