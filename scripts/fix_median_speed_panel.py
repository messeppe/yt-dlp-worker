#!/usr/bin/env python3
"""Fix the broken Median Download Speed stat.

Its expr used `quantile(...)`, which is a PromQL aggregator that does
not exist in LogQL — the panel has errored since creation. This Loki's
query frontend also can't evaluate quantile_over_time (SelectSamples
unimplemented), so the panel becomes a mean instead.

Usage: set GRAFANA_AUTH=user:password, then
  python scripts/fix_median_speed_panel.py
"""

import base64
import os

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "https://grafana.app7.kelana5.com")
UID = "videomasjid-json-v1"
_tok = base64.b64encode(os.environ["GRAFANA_AUTH"].encode()).decode()
HEADERS = {"Authorization": f"Basic {_tok}", "Content-Type": "application/json"}

NEW_EXPR = ('avg(avg_over_time({app="videomasjid",'
            'service=~"yt-media-mule.*"} | json '
            '| event="download_complete" | unwrap speed_mbps [$__range])) '
            'or vector(0)')


def main():
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{UID}",
                     headers=HEADERS, timeout=30)
    r.raise_for_status()
    dash = r.json()["dashboard"]
    p = next(p for p in dash["panels"] if p.get("id") == 101)
    p["title"] = "Avg Stream Speed"
    p["description"] = ("Mean per-stream download speed over the dashboard "
                        "range. LogQL has no quantile aggregator; the old "
                        "median query never worked.")
    p["targets"][0]["expr"] = NEW_EXPR
    p.setdefault("fieldConfig", {}).setdefault("defaults", {})["unit"] = "MBs"
    p["fieldConfig"]["defaults"]["decimals"] = 2
    dash.pop("version", None)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS,
                      json={"dashboard": dash, "overwrite": True,
                            "message": "Fix broken median speed stat "
                                       "(quantile() is not LogQL)"},
                      timeout=30)
    r.raise_for_status()
    print("overview:", r.json().get("status"), "v", r.json().get("version"))


if __name__ == "__main__":
    main()
