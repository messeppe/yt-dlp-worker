#!/usr/bin/env python3
"""
Make quiet panels show healthy zeros instead of "No data".

Panels counting rare events (circuit trips, backpressure, failures,
requeues) return empty results when nothing fired — Grafana renders
"No data", which reads as broken. Append `or vector(0)` so they render
flat zero lines / a green 0 instead.

Also gives System Health an explicit OK / TRIPPED value mapping.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/patch_quiet_panels.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
UID = "videomasjid-json-v1"
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

# Panels whose every Loki metric target gets the vector(0) fallback.
ZERO_FALLBACK = {100, 101, 35, 38, 36, 41}


def main():
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{UID}",
                     headers=HEADERS, timeout=30)
    r.raise_for_status()
    dash = r.json()["dashboard"]

    for p in dash["panels"]:
        if p.get("id") in ZERO_FALLBACK:
            for t in p.get("targets", []):
                expr = t.get("expr", "")
                if expr and "vector(0)" not in expr:
                    t["expr"] = f"{expr.rstrip()} or vector(0)"

        if p.get("id") == 100:
            fd = p.setdefault("fieldConfig", {}).setdefault("defaults", {})
            fd["mappings"] = [
                {"type": "value",
                 "options": {"0": {"text": "OK", "color": "green",
                                   "index": 0}}},
                {"type": "range",
                 "options": {"from": 1, "to": 1e9,
                             "result": {"text": "CIRCUIT TRIPPED",
                                        "color": "red", "index": 1}}},
            ]
            fd["thresholds"] = {"mode": "absolute", "steps": [
                {"color": "green", "value": None},
                {"color": "red", "value": 1}]}
            fd["noValue"] = "OK"
            p["title"] = "Circuit Health"

        if p.get("id") == 101:
            p.setdefault("fieldConfig", {}).setdefault(
                "defaults", {})["noValue"] = "0"

        if p.get("id") == 37:
            p["description"] = ("Empty when healthy — only circuit breaker, "
                                "quota guard, and bad-video events appear here.")

    dash.pop("version", None)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS,
                      json={"dashboard": dash, "overwrite": True,
                            "message": "Quiet panels: vector(0) fallback, "
                                       "OK/TRIPPED mapping"}, timeout=30)
    r.raise_for_status()
    print("overview:", r.json().get("status"), "v", r.json().get("version"))


if __name__ == "__main__":
    main()
