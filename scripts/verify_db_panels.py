#!/usr/bin/env python3
"""Run every Pipeline State (Supabase) panel query through Grafana's
/api/ds/query — the same path the dashboard uses — and report per-panel
errors and row counts. A green health check only proves connection+auth;
this proves grants and query results.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/verify_db_panels.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}


def main():
    d = requests.get(
        f"{GRAFANA_URL}/api/dashboards/uid/videomasjid-json-v1",
        headers=HEADERS, timeout=30,
    ).json()["dashboard"]
    pg = [p for p in d["panels"]
          if p["type"] != "row"
          and any(t.get("rawSql") for t in p.get("targets", []))]
    failures = 0
    for p in pg:
        t = p["targets"][0]
        q = {"queries": [{
            "refId": "A", "datasource": t["datasource"], "format": t["format"],
            "rawQuery": True, "rawSql": t["rawSql"],
            "maxDataPoints": 100, "intervalMs": 60000,
        }], "from": "now-24h", "to": "now"}
        r = requests.post(f"{GRAFANA_URL}/api/ds/query",
                          headers=HEADERS, json=q, timeout=30)
        res = r.json().get("results", {}).get("A", {})
        err = res.get("error")
        frames = res.get("frames", [])
        vals = frames[0]["data"]["values"] if frames else []
        n = len(vals[0]) if vals else 0
        sample = [v[:3] for v in vals[:3]] if vals else []
        status = "FAIL" if (err or n == 0) else "ok"
        if status == "FAIL":
            failures += 1
        print(f"[{status}] panel {p['id']:>3} {p['title'][:38]:38} "
              f"rows={n} err={err} sample={sample}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
