"""Tests for _raise_classified_4xx — 4xx + status:error body classification.

Run: python test_classified_4xx.py
"""
import os

os.environ.setdefault("SUPABASE_DB_URL", "postgresql://x")
os.environ.setdefault("RAPIDAPI_KEY", "x")
os.environ.setdefault("RAPIDAPI_HOST", "example.test")
os.environ.setdefault("PROXY_URL", "http://u:p@proxy.example:80")

import scout


class FakeResp:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body


def expect(resp, exc_type, label):
    try:
        scout._raise_classified_4xx(resp)
    except Exception as e:
        assert isinstance(e, exc_type), f"{label}: got {type(e).__name__}"
        print(f"PASS {label} -> {type(e).__name__}")
        return
    assert exc_type is None, f"{label}: expected {exc_type.__name__}, got no raise"
    print(f"PASS {label} -> no raise")


# 2026-06-12 outage shape: 400 + "Unknown error occurred" -> transient (circuit)
expect(FakeResp(400, {"status": "error", "message": "Unknown error occurred"}),
       scout.TransientAPIError, "400 unknown error")
# 400 + video not found -> per-video permanent
expect(FakeResp(400, {"status": "error", "message": "Video not found"}),
       scout.PermanentVideoError, "400 video not found")
# 429/407 keep dedicated HTTPError handling -> no raise here
expect(FakeResp(429, {"status": "error", "message": "rate limit"}), None, "429 passthrough")
expect(FakeResp(407, {"status": "error", "message": "auth"}), None, "407 passthrough")
# 5xx untouched -> raise_for_status handles
expect(FakeResp(502, {"status": "error", "message": "bad gateway"}), None, "502 passthrough")
# 4xx with non-JSON body -> falls through to raise_for_status
expect(FakeResp(400, None), None, "400 non-json passthrough")
# 4xx JSON without status:error -> falls through
expect(FakeResp(404, {"detail": "nope"}), None, "404 plain json passthrough")
# 2xx -> never touched
expect(FakeResp(200, {"status": "error", "message": "x"}), None, "200 passthrough")

print("ALL PASS")
