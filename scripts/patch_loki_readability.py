#!/usr/bin/env python3
"""
Loki panel readability pass — display-only, no log event names touched
(see memory: renaming events silently blanks panels).

Timeseries: human legend names via legendFormat, proper units,
  proxy-usage panel limited to topk(15).
Logs: tighter line_format (no empty-field junk), wrapped lines.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/patch_loki_readability.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

# panel id -> (unit, [legend names per target in order])
TS = {
    11: ("reqps", ["{{endpoint}}"]),
    12: ("ops", ["claim", "enqueue", "dequeue", "requeue"]),
    13: ("ops", ["S3 upload", "DB write (media_files)", "dequeue (completed)"]),
    14: ("ops", ["S3 upload", "DB write (subtitles)", "dequeue (completed)"]),
    35: ("ops", ["backpressure", "backpressure resume",
                 "circuit OPEN", "circuit CLOSE"]),
    38: ("ops", ["video bad", "subtitle bad", "url expired (cap)"]),
    36: ("ops", ["scout", "media mule", "subtitle mule"]),
    41: ("ops", ["403", "crash", "retry", "url blocked", "url expired"]),
    42: ("MBs", ["avg", "peak"]),
    43: ("short", ["proxy {{proxy_idx}}"]),
    44: ("KBs", ["avg", "peak"]),
    45: ("short", ["rotations/min"]),
    46: ("short", ["media", "subtitles"]),
}

LOG_FORMATS = {
    21: '{{if .event}}[{{.event}}] {{end}}{{.message}}'
        '{{if .video_id}}  video={{.video_id}}{{end}}',
    22: '[{{.event}}] {{.worker}}  {{.video_id}}'
        '{{if .queue}}  {{.queue}}{{end}}'
        '{{if .to_status}} -> {{.to_status}}{{end}}'
        '{{if .reason}}  ({{.reason}}){{end}}',
    23: '[{{.event}}] {{.worker}}  {{.video_id}}'
        '{{if .table}}  {{.table}}{{end}}'
        '{{if .s3_path}}  {{.s3_path}}{{end}}'
        '{{if .language_code}}  lang={{.language_code}}{{end}}',
    24: '[{{.level}}] {{.service}}  {{.event}}: {{.message}}'
        '{{if .video_id}}  video={{.video_id}}{{end}}',
    37: '[{{.event}}] {{.message}}',
}


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


def iter_panels(dash):
    for p in dash["panels"]:
        yield p
        for c in p.get("panels", []):
            yield c


def patch_timeseries(p):
    unit, legends = TS[p["id"]]
    p.setdefault("fieldConfig", {}).setdefault("defaults", {})["unit"] = unit
    custom = p["fieldConfig"]["defaults"].setdefault("custom", {})
    custom.setdefault("lineWidth", 2)
    custom.setdefault("fillOpacity", 8)
    opts = p.setdefault("options", {})
    leg = opts.setdefault("legend", {})
    leg.update({"displayMode": "table", "placement": "bottom",
                "calcs": ["mean", "max", "lastNotNull"], "showLegend": True})
    opts.setdefault("tooltip", {})["mode"] = "multi"
    opts["tooltip"]["sort"] = "desc"
    for t, name in zip(p.get("targets", []), legends * len(p.get("targets", []))):
        t["legendFormat"] = name
    if p["id"] == 43:
        t = p["targets"][0]
        if not t["expr"].lstrip().startswith("topk"):
            t["expr"] = f"topk(15, {t['expr']})"
        if "top 15" not in p["title"]:
            p["title"] = "Proxy Usage by Index (downloads/min, top 15)"
        # 15 series is still a lot; legend list, not table
        p["options"]["legend"]["displayMode"] = "list"
        p["options"]["legend"]["calcs"] = []


def patch_logs(p):
    fmt = LOG_FORMATS[p["id"]]
    t = p["targets"][0]
    expr = t["expr"]
    head, sep, _old = expr.partition("| line_format")
    t["expr"] = f'{head.rstrip()} | line_format "{fmt}"' if sep else \
                f'{expr.rstrip()} | line_format "{fmt}"'
    p.setdefault("options", {}).update({
        "wrapLogMessage": True, "prettifyLogMessage": False,
        "showTime": True, "showLabels": False, "enableLogDetails": True,
        "dedupStrategy": "none", "sortOrder": "Descending",
    })


def main():
    for uid in ("videomasjid-json-v1", "videomasjid-deepdive"):
        d = fetch(uid)
        for p in iter_panels(d):
            pid = p.get("id")
            if p.get("type") == "timeseries" and pid in TS:
                patch_timeseries(p)
            elif p.get("type") == "logs" and pid in LOG_FORMATS:
                patch_logs(p)
        r = push(d, "Loki readability: legends, units, topk proxies, log line_format")
        print(uid, r.get("status"), "v", r.get("version"))


if __name__ == "__main__":
    main()
