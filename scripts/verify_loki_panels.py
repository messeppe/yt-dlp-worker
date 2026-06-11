#!/usr/bin/env python3
"""Run every Loki panel query (timeseries + logs) from both dashboards
through /api/ds/query and fail on parse errors or empty results.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/verify_loki_panels.py
"""

import base64
import os
import time

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}


def main():
    now = int(time.time() * 1000)
    failures = 0
    for uid in ("videomasjid-json-v1", "videomasjid-deepdive"):
        d = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}",
                         headers=HEADERS, timeout=30).json()["dashboard"]
        panels = [p for top in d["panels"]
                  for p in [top] + top.get("panels", [])
                  if p.get("type") in ("timeseries", "logs", "stat",
                                       "bargauge", "state-timeline")]
        for p in panels:
            for t in p.get("targets", []):
                if not t.get("expr"):
                    continue
                q = {"queries": [{**t, "refId": "A",
                                  "datasource": t.get("datasource",
                                                      {"type": "loki",
                                                       "uid": "eflpdpm4rjim8b"}),
                                  "maxDataPoints": 200,
                                  "intervalMs": 30000}],
                     "from": str(now - 1800_000), "to": str(now)}
                r = requests.post(f"{GRAFANA_URL}/api/ds/query",
                                  headers=HEADERS, json=q, timeout=60)
                res = r.json().get("results", {}).get("A", {})
                err = res.get("error")
                frames = res.get("frames", [])
                rows = sum(len(f["data"]["values"][0]) if f["data"]["values"]
                           else 0 for f in frames)
                status = "FAIL" if err else ("EMPTY" if rows == 0 else "ok")
                if err:
                    failures += 1
                print(f"[{status:5}] {uid[-12:]} panel {p['id']:>3} "
                      f"{p['title'][:34]:34} rows={rows} err={err}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
