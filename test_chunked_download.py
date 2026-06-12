"""Offline tests for chunked-range download_stream using httpx.MockTransport.

Run: python test_chunked_download.py
No network, no proxy, no DB — httpx.Client is monkeypatched to a mock transport.
"""
import os
import tempfile

os.environ.setdefault("S3_ENDPOINT", "http://localhost")
os.environ.setdefault("S3_BUCKET", "test")
os.environ.setdefault("S3_ACCESS_KEY", "x")
os.environ.setdefault("S3_SECRET_KEY", "x")
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://x")
os.environ.setdefault("PROXY_URL", "http://u:p@proxy.example:80")
os.environ["STREAM_CHUNK_MB"] = "1"   # 1MB chunks so a 3MB file needs 3 requests
os.environ["STREAM_MAX_RETRIES"] = "4"

import httpx

import media_mule

FILE = bytes(range(256)) * 4096 * 3  # 3 MB, deterministic content
_real_client = httpx.Client


def patch_transport(handler):
    transport = httpx.MockTransport(handler)

    def factory(**kw):
        kw.pop("proxy", None)
        kw.pop("http2", None)
        return _real_client(transport=transport, **kw)

    httpx.Client = factory


def parse_range(request):
    rng = request.headers.get("Range", "")
    if not rng:
        return 0, len(FILE) - 1, False
    spec = rng.split("=", 1)[1]
    s, e = spec.split("-")
    start = int(s)
    bounded = e != ""
    end = min(int(e), len(FILE) - 1) if bounded else len(FILE) - 1
    return start, end, bounded


def run_download():
    with tempfile.TemporaryDirectory() as tmp:
        dest = os.path.join(tmp, "video.mp4")
        stats = media_mule.download_stream("http://cdn.example/file", dest)
        with open(dest, "rb") as f:
            data = f.read()
        return stats, data


def test_chunked_happy_path():
    calls = []

    def handler(request):
        start, end, bounded = parse_range(request)
        calls.append((start, end, bounded))
        body = FILE[start:end + 1]
        return httpx.Response(206, content=body, headers={
            "Content-Range": f"bytes {start}-{end}/{len(FILE)}",
        })

    patch_transport(handler)
    stats, data = run_download()
    assert data == FILE, f"content mismatch: {len(data)} vs {len(FILE)}"
    assert stats["bytes"] == len(FILE)
    assert len(calls) == 3, f"expected 3 chunk requests, got {len(calls)}"
    assert all(b for _, _, b in calls), "all requests should be bounded"
    print(f"PASS chunked_happy_path ({len(calls)} requests)")


def test_server_ignores_range():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(200, content=FILE, headers={
            "Content-Length": str(len(FILE)),
        })

    patch_transport(handler)
    stats, data = run_download()
    assert data == FILE
    assert len(calls) == 1, f"200 full-body should finish in 1 request, got {len(calls)}"
    print("PASS server_ignores_range")


def test_resume_after_connection_error():
    state = {"calls": 0, "failed": False}

    def handler(request):
        state["calls"] += 1
        start, end, bounded = parse_range(request)
        if start > 0 and not state["failed"]:
            state["failed"] = True
            raise httpx.ConnectError("proxy died", request=request)
        body = FILE[start:end + 1]
        return httpx.Response(206, content=body, headers={
            "Content-Range": f"bytes {start}-{end}/{len(FILE)}",
        })

    patch_transport(handler)
    stats, data = run_download()
    assert data == FILE, "resume after mid-download error must yield intact file"
    assert state["failed"], "error path never exercised"
    assert stats["proxy_rotations"] >= 1
    print(f"PASS resume_after_connection_error ({state['calls']} requests)")


def test_no_total_falls_back_to_open_ended():
    calls = []

    def handler(request):
        start, end, bounded = parse_range(request)
        calls.append(bounded)
        if bounded:
            # CDN refuses to reveal total size
            body = FILE[start:end + 1]
            return httpx.Response(206, content=body, headers={
                "Content-Range": f"bytes {start}-{end}/*",
            })
        body = FILE[start:]
        return httpx.Response(206, content=body, headers={
            "Content-Range": f"bytes {start}-{len(FILE) - 1}/{len(FILE)}",
            "Content-Length": str(len(body)),
        })

    patch_transport(handler)
    stats, data = run_download()
    assert data == FILE, "fallback to open-ended must yield intact file"
    assert calls[0] is True and calls[-1] is False, "should switch bounded -> open-ended"
    print("PASS no_total_falls_back_to_open_ended")


def test_zero_progress_exhausts_budget():
    def handler(request):
        start, end, bounded = parse_range(request)
        return httpx.Response(206, content=b"", headers={
            "Content-Range": f"bytes {start}-{end}/{len(FILE)}",
        })

    patch_transport(handler)
    try:
        run_download()
    except RuntimeError as e:
        assert "incomplete download" in str(e)
        print("PASS zero_progress_exhausts_budget")
        return
    raise AssertionError("empty responses must raise RuntimeError, not loop forever")


if __name__ == "__main__":
    test_chunked_happy_path()
    test_server_ignores_range()
    test_resume_after_connection_error()
    test_no_total_falls_back_to_open_ended()
    test_zero_progress_exhausts_budget()
    print("ALL PASS")
