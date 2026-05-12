#!/usr/bin/env python3
"""
Grafana dashboard restructure — metrics-first, logs-at-bottom.

Changes applied:
  Remove : panels 50 (misplaced row), 73 (speed dup), 76 (progress log dup)
  Merge  : panels 21+75 -> unified Scout Log
  Reorder: Operational Health before Download Performance
  Merge  : Task Manager (row 80 panels) into Live Worker Status (row 70)
  Move   : All log panels to new bottom "Logs" section
  Fix    : API Quota row at correct position (new row id=90)
  Layout : Logs section in 2-column grid

Usage:
  python scripts/patch_dashboard_restructure.py            # apply to Grafana
  python scripts/patch_dashboard_restructure.py --dry-run  # print positions only
"""

import base64
import sys

import requests

GRAFANA_URL = "https://grafana.app7.kelana5.com"
DASHBOARD_UID = "videomasjid-json-v1"
_tok = base64.b64encode(b"admin:q0HLu1KsOVG8qJXTZl4Ezo089BeCmCmR").decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

# Absolute y-start for each section (row panel ID -> y).
# Worker section height accounts for Grafana repeat expansion (20 workers):
#   Activity Timeline h=10, Progress/Subtitle row h=8, Task timeseries 5x6=30, Task stat 5x2=10
SECTION_START_Y = {
    1:  0,    # Live KPIs
    10: 5,    # Rates
    30: 22,   # Operational Health
    40: 35,   # Download Performance
    70: 60,   # Live Worker Status (incl. merged task manager)
    90: 119,  # API Quota  (new row)
    20: 124,  # Logs
}


# -- HTTP helpers --------------------------------------------------------------

def fetch_dashboard():
    r = requests.get(
        f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}",
        headers=HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def push_dashboard(data, dash):
    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        headers=HEADERS,
        json={
            "dashboard": dash,
            "overwrite": True,
            "folderId": data.get("meta", {}).get("folderId", 0),
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# -- Section helpers -----------------------------------------------------------

def by_id(panels, pid):
    return next(p for p in panels if p["id"] == pid)


def group_sections(panels):
    """Return list of sections. Each section = [row_panel, *content_panels]."""
    sections, current = [], []
    for p in sorted(panels, key=lambda p: (p["gridPos"]["y"], p["gridPos"]["x"])):
        if p["type"] == "row":
            if current:
                sections.append(current)
            current = [p]
        else:
            current.append(p)
    if current:
        sections.append(current)
    return sections


def normalize(section):
    """Shift section so row panel is at y=0 (relative positions within section)."""
    base = section[0]["gridPos"]["y"]
    for p in section:
        p["gridPos"]["y"] -= base


def apply_y_offsets(sec_by_id, section_start_y):
    """Add absolute y offset to each normalized section."""
    for row_id, start_y in section_start_y.items():
        if row_id not in sec_by_id:
            continue
        for p in sec_by_id[row_id]:
            p["gridPos"]["y"] += start_y  # normalized row at 0 + start_y = start_y


# -- Main transformation -------------------------------------------------------

def restructure(panels):
    # 1. Remove redundant panels
    panels = [p for p in panels if p["id"] not in {50, 73, 76}]

    # 2. Merge panel 75 into panel 21 -> unified Scout Log
    p21 = by_id(panels, 21)
    p21["title"] = "Scout Log"
    p21["gridPos"]["x"] = 0
    p21["gridPos"]["w"] = 12
    # Replace all existing targets with single all-scout Loki query
    for t in p21.get("targets", []):
        t["expr"] = (
            '{app="videomasjid", service=~"yt-scout.*"} | json'
            ' | line_format "{{.ts}} [{{.event}}] {{.message}}'
            '{{if .video_id}} video={{.video_id}}{{end}}"'
        )
    panels = [p for p in panels if p["id"] != 75]

    # 3. Queue Operations: right column next to Scout Log
    p22 = by_id(panels, 22)
    p22["gridPos"]["x"] = 12
    p22["gridPos"]["w"] = 12

    # 4. Persistence Writes + Warnings & Errors: full width
    for pid in (23, 24):
        by_id(panels, pid)["gridPos"].update({"x": 0, "w": 24})

    # 5. Rename row 20
    by_id(panels, 20)["title"] = "Logs"

    # 6. Add new API Quota row (id=90); relocate panel 51 under it
    panels.append({
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": 9999},
        "id": 90,
        "title": "API Quota",
        "type": "row",
    })
    # Move panel 51 to just after new row 90 so group_sections places it there
    by_id(panels, 51)["gridPos"]["y"] = 10000

    # 7. Group sections and normalize
    sections = group_sections(panels)
    for sec in sections:
        normalize(sec)
        sec[0]["collapsed"] = False  # ensure all rows are expanded
    sec_by_id = {sec[0]["id"]: sec for sec in sections}

    # 8. Merge task manager panels (row 80) into worker status (row 70)
    if 80 in sec_by_id:
        # Panels 81 and 82 absorbed into row 70 section; row 80 header discarded.
        sec_by_id[70].extend(sec_by_id[80][1:])

    # 9. Explicit relative layout for Worker Status section
    wp = {p["id"]: p for p in sec_by_id[70]}
    # Row header at relative y=0 (already normalized)
    wp[71]["gridPos"].update({"y": 1,  "x": 0,  "w": 24, "h": 10})  # Activity Timeline
    wp[72]["gridPos"].update({"y": 11, "x": 0,  "w": 12, "h": 8})   # Progress %
    wp[74]["gridPos"].update({"y": 11, "x": 12, "w": 12, "h": 8})   # Subtitle Completions
    # Task manager timeseries: 20 workers, maxPerRow=4 -> 5 rows x h=6 = 30 rendered rows
    wp[81]["gridPos"].update({"y": 19, "x": 0,  "w": 6,  "h": 6})
    # Task manager stat: y=19+30=49; 5 rows x h=2 = 10 rendered rows -> section ends at 59
    wp[82]["gridPos"].update({"y": 49, "x": 0,  "w": 6,  "h": 2})

    # 10. Explicit relative layout for Logs section
    lp = {p["id"]: p for p in sec_by_id[20]}
    # Row header at y=0
    lp[21]["gridPos"].update({"y": 1,  "x": 0,  "w": 12, "h": 8})  # Scout Log (left)
    lp[22]["gridPos"].update({"y": 1,  "x": 12, "w": 12, "h": 8})  # Queue Ops (right)
    lp[23]["gridPos"].update({"y": 9,  "x": 0,  "w": 24, "h": 8})  # Persistence (full)
    lp[24]["gridPos"].update({"y": 17, "x": 0,  "w": 24, "h": 8})  # Warnings   (full)

    # 11. Apply absolute y offsets and build ordered panel list
    apply_y_offsets(sec_by_id, SECTION_START_Y)
    ORDER = [1, 10, 30, 40, 70, 90, 20]
    return [p for row_id in ORDER if row_id in sec_by_id for p in sec_by_id[row_id]]


# -- Entry point ---------------------------------------------------------------

def main():
    dry_run = "--dry-run" in sys.argv

    print("Fetching dashboard...")
    data = fetch_dashboard()
    dash = data["dashboard"]
    print(f"  {len(dash['panels'])} panels, version {dash['version']}")

    dash["panels"] = restructure(dash["panels"])
    print(f"  {len(dash['panels'])} panels after restructure")

    if dry_run:
        print("\nDRY RUN -- panel positions (no POST):")
        print(f"  {'id':>4}  {'y':>4}  {'h':>3}  {'x':>3}  {'w':>3}  {'type':<15}  title")
        for p in dash["panels"]:
            gp = p["gridPos"]
            print(f"  {p['id']:>4}  {gp['y']:>4}  {gp['h']:>3}  {gp['x']:>3}  {gp['w']:>3}  {p['type']:<15}  {p.get('title', '')}")
        return

    print("Posting to Grafana...")
    result = push_dashboard(data, dash)
    print(f"  OK -- new version {result['version']}")
    print(f"  {GRAFANA_URL}/d/{dash['uid']}")


if __name__ == "__main__":
    main()
