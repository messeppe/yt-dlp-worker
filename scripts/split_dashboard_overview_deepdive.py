#!/usr/bin/env python3
"""
Split VideoMasjid dashboard into Overview + Deep Dive.

Overview (uid videomasjid-json-v1, existing): top stats, Live KPIs, Rates,
Operational Health, Download Performance, API Quota. No per-worker repeat
panels, no log walls.

Deep Dive (uid videomasjid-deepdive, new): Live Worker Status section
(timeline, progress, per-worker task manager) + all log panels.

Also:
  - fixes top stat row overflow (5 panels totalling w=28 on a 24 grid)
  - repairs mojibake titles (UTF-8 em dash read as cp1252)
  - extends worker_idx template variable 0-49 (WORKER_COUNT=50)
  - cross-links the two dashboards via the 'videomasjid' tag

Usage:
  python scripts/split_dashboard_overview_deepdive.py            # apply
  python scripts/split_dashboard_overview_deepdive.py --dry-run  # print layout
"""

import base64
import copy
import json
import os
import sys

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
OVERVIEW_UID = "videomasjid-json-v1"
DEEPDIVE_UID = "videomasjid-deepdive"
# Set GRAFANA_AUTH as "user:password" before running.
_auth = os.environ["GRAFANA_AUTH"]
_tok = base64.b64encode(_auth.encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

# Panel ids per dashboard. Rows keep their child panels with them.
DEEPDIVE_IDS = {70, 71, 72, 74, 81, 20, 21, 22, 23, 24}
WORKER_COUNT = 50


def fix_mojibake(s):
    if "â" not in s:
        return s
    try:
        return s.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def fetch_dashboard(uid):
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()["dashboard"]


def push_dashboard(dash, message):
    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        headers=HEADERS,
        json={"dashboard": dash, "overwrite": True, "message": message},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def relayout(panels, section_rows):
    """Stack sections top-down: each row header followed by its panels,
    preserving each panel's x and relative y order within the section."""
    out = []
    y = 0
    # Panels before the first row (top stats) handled by caller.
    for row_id, children in section_rows:
        if row_id is not None:
            row = next(p for p in panels if p["id"] == row_id)
            row["gridPos"] = {"h": 1, "w": 24, "x": 0, "y": y}
            out.append(row)
            y += 1
        block = [p for p in panels if p["id"] in children]
        block.sort(key=lambda p: (p["gridPos"]["y"], p["gridPos"]["x"]))
        base_y = None
        row_h = 0
        cur_y = None
        for p in block:
            if base_y is None:
                base_y = p["gridPos"]["y"]
            p["gridPos"]["y"] = y + (p["gridPos"]["y"] - base_y)
            out.append(p)
        if block:
            y = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in block)
    return out


def main():
    dry = "--dry-run" in sys.argv
    src = fetch_dashboard(OVERVIEW_UID)

    for p in src["panels"]:
        if "title" in p:
            p["title"] = fix_mojibake(p["title"])

    by_id = {p["id"]: p for p in src["panels"]}

    # ---------- Overview ----------
    ov = copy.deepcopy(src)
    ov["title"] = "VideoMasjid — Overview"
    ov["tags"] = sorted(set(ov.get("tags", []) + ["videomasjid"]))
    ov["links"] = [{
        "type": "dashboards", "tags": ["videomasjid"], "asDropdown": False,
        "includeVars": False, "keepTime": True, "icon": "external link",
        "targetBlank": False, "title": "", "tooltip": "", "url": ""
    }]
    keep = [copy.deepcopy(by_id[i]) for i in
            [100, 101, 102, 103, 104] if i in by_id]
    # Top stat row: 5 panels -> w 5,5,5,5,4 = 24
    widths = [5, 5, 5, 5, 4]
    x = 0
    for p, w in zip(keep, widths):
        p["gridPos"] = {"h": 4, "w": w, "x": x, "y": 0}
        x += w
    body_ids = {
        1: [2, 3, 4, 5, 6, 7],
        10: [11, 12, 13, 14],
        30: [31, 32, 33, 34, 35, 38, 36, 37],
        40: [41, 42, 43, 44, 45, 46],
        90: [51],
    }
    body = [copy.deepcopy(by_id[i]) for ids in
            ([k] + v for k, v in body_ids.items()) for i in ids if i in by_id]
    stacked = relayout(body, [(k, set(v)) for k, v in body_ids.items()])
    for p in stacked:
        p["gridPos"]["y"] += 4
    ov["panels"] = keep + stacked
    # Overview has no repeat panels; drop worker_idx variable if present.
    ov.setdefault("templating", {})["list"] = [
        v for v in ov.get("templating", {}).get("list", [])
        if v.get("name") != "worker_idx"
    ]

    # ---------- Deep Dive ----------
    dd_panels = [copy.deepcopy(by_id[i]) for i in
                 [70, 71, 72, 74, 81, 20, 21, 22, 23, 24] if i in by_id]
    dd_ids = {
        70: [71, 72, 74, 81],
        20: [21, 22, 23, 24],
    }
    dd_stacked = relayout(dd_panels, [(k, set(v)) for k, v in dd_ids.items()])
    dd = {
        "uid": DEEPDIVE_UID,
        "title": "VideoMasjid — Deep Dive (Workers & Logs)",
        "tags": sorted(set(src.get("tags", []) + ["videomasjid"])),
        "timezone": src.get("timezone", "browser"),
        "refresh": src.get("refresh", "10s"),
        "time": src.get("time", {"from": "now-1h", "to": "now"}),
        "templating": {"list": [
            {
                "name": "worker_idx",
                "type": "custom",
                "label": "Worker",
                "query": ",".join(str(i) for i in range(WORKER_COUNT)),
                "includeAll": True,
                "multi": True,
                "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
                "options": [],
            }
        ]},
        "links": [{
            "type": "dashboards", "tags": ["videomasjid"], "asDropdown": False,
            "includeVars": False, "keepTime": True, "icon": "external link",
            "targetBlank": False, "title": "", "tooltip": "", "url": ""
        }],
        "panels": dd_stacked,
        "schemaVersion": src.get("schemaVersion", 39),
        "editable": True,
    }

    if dry:
        for name, d in (("OVERVIEW", ov), ("DEEPDIVE", dd)):
            print(f"== {name} ==")
            for p in d["panels"]:
                g = p["gridPos"]
                print(f"  {p['id']:>4} y={g['y']:<4} h={g['h']:<3} w={g['w']:<3} "
                      f"{p.get('type',''):15} {p.get('title','')}")
        return

    ov.pop("version", None)
    r1 = push_dashboard(ov, "Split: overview only (workers/logs moved to deep-dive)")
    r2 = push_dashboard(dd, "Split: new deep-dive dashboard (workers + logs)")
    print("overview:", r1.get("status"), r1.get("version"), r1.get("url"))
    print("deepdive:", r2.get("status"), r2.get("version"), r2.get("url"))


if __name__ == "__main__":
    main()
