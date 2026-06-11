#!/usr/bin/env python3
"""Check Loki ingestion through Grafana: label values + recent log volume."""
import base64
import json
import os
import time

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
H = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}
LOKI = {"type": "loki", "uid": "eflpdpm4rjim8b"}

now = int(time.time() * 1000)

# 1. What label values exist right now?
for label in ("app", "service"):
    r = requests.get(
        f"{GRAFANA_URL}/api/datasources/uid/{LOKI['uid']}/resources/label/{label}/values",
        params={"start": (now - 3600_000) * 1_000_000, "end": now * 1_000_000},
        headers=H, timeout=30)
    print(f"label {label!r}:", r.status_code, r.json().get("data"))

# 2. Any log lines in the last 15 minutes?
q = {"queries": [{"refId": "A", "datasource": LOKI,
                  "expr": '{app="videomasjid"}', "queryType": "range",
                  "maxLines": 10}],
     "from": str(now - 900_000), "to": str(now)}
r = requests.post(f"{GRAFANA_URL}/api/ds/query", headers=H, json=q, timeout=30)
res = r.json().get("results", {}).get("A", {})
frames = res.get("frames", [])
total = sum(len(f["data"]["values"][0]) if f["data"]["values"] else 0
            for f in frames)
print("error:", res.get("error"))
print("frames:", len(frames), "| log rows last 15m:", total)
if frames and total:
    vals = frames[0]["data"]["values"]
    print("sample line:", str(vals[-1][0])[:160])
