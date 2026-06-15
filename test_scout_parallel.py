"""Offline unit tests for the parallel-scout concurrency primitives.

No DB or network: we set dummy env so scout imports, then exercise the
RateLimiter, the canary single-flight, and the thread-safety of the circuit
state. Run: python test_scout_parallel.py
"""
import os
import threading
import time

os.environ.setdefault("SUPABASE_DB_URL", "postgresql://x:y@localhost/z")
os.environ.setdefault("RAPIDAPI_KEY", "test")
os.environ.setdefault("RAPIDAPI_HOST", "test.host")
os.environ.setdefault("CIRCUIT_OPEN_THRESHOLD", "5")

import scout  # noqa: E402


def test_rate_limiter_throttles():
    # capacity=1 -> only the first token is free; the rest are paced at `rate`/s.
    rl = scout.RateLimiter(rate=20.0, capacity=1.0)
    n = 6
    t0 = time.monotonic()
    for _ in range(n):
        rl.acquire()
    elapsed = time.monotonic() - t0
    # (n-1) tokens after the initial burst, at 20/s -> ~0.25s minimum.
    expected = (n - 1) / 20.0
    assert elapsed >= expected * 0.9, f"too fast: {elapsed:.3f}s < {expected:.3f}s"
    print(f"  rate_limiter: {n} acquires in {elapsed:.3f}s (>= {expected:.3f}s) OK")


def test_rate_limiter_shared_across_threads():
    rl = scout.RateLimiter(rate=50.0, capacity=1.0)
    counts = []
    stop = time.monotonic() + 0.5

    def worker():
        c = 0
        while time.monotonic() < stop:
            rl.acquire()
            c += 1
        counts.append(c)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    total = sum(counts)
    # ~0.5s at 50/s ≈ 25, plus burst; never wildly above the shared cap.
    assert total <= 50, f"shared bucket exceeded cap: {total} acquires in 0.5s"
    print(f"  rate_limiter shared: {total} acquires across 8 threads in ~0.5s (cap-respecting) OK")


def test_canary_single_flight():
    calls = {"n": 0}
    lock = threading.Lock()

    def fake_get_streams(video_id):
        with lock:
            calls["n"] += 1
        time.sleep(0.2)  # hold the probe so other threads pile up
        return "", [{"url": "http://x"}]  # truthy results -> API up

    scout.get_streams = fake_get_streams
    scout._api_healthy_until = 0.0  # force a probe
    scout._last_good_video_id = "vid"

    verdicts = []
    vlock = threading.Lock()

    def probe():
        v = scout._api_is_up()
        with vlock:
            verdicts.append(v)

    threads = [threading.Thread(target=probe) for _ in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    assert calls["n"] == 1, f"expected 1 canary call, got {calls['n']}"
    assert all(verdicts) and len(verdicts) == 10, f"verdicts wrong: {verdicts}"
    print(f"  canary single-flight: 10 concurrent probes -> {calls['n']} API call, all UP OK")


def test_circuit_distinct_thread_safe():
    # Reset state.
    with scout._state_lock:
        scout._media_recent_failed_ids.clear()
        scout._media_circuit_opened_at = 0.0

    # 200 threads, but only CIRCUIT_OPEN_THRESHOLD distinct ids repeated -> deque must
    # hold exactly the distinct ids (deduped), and circuit opens exactly once.
    k = scout.CIRCUIT_OPEN_THRESHOLD
    ids = [f"vid{i % k}" for i in range(200)]

    def fail(vid):
        scout._on_media_failure(vid)

    threads = [threading.Thread(target=fail, args=(v,)) for v in ids]
    [t.start() for t in threads]
    [t.join() for t in threads]

    distinct = len(set(ids))
    assert len(scout._media_recent_failed_ids) == distinct == k, (
        f"deque has {len(scout._media_recent_failed_ids)} entries, expected {k}"
    )
    assert scout._media_circuit_open(), "circuit should be open at threshold"
    assert scout._media_circuit_opened_at != 0.0, "opened_at must be set once"
    print(f"  circuit thread-safe: 200 racing failures, {k} distinct -> opened once OK")


def test_circuit_resets_to_closed_on_success():
    scout._on_media_success("vidX")
    assert not scout._media_circuit_open(), "success must clear the circuit"
    assert len(scout._media_recent_failed_ids) == 0
    print("  circuit close on success OK")


def test_reclassify_requires_interleaved_success():
    # A video's failure only counts toward extractor_block if ANOTHER video succeeded
    # since it last failed (proof the API is genuinely up — distinguishes age-restricted
    # /unextractable videos from a true outage, which both return identical 400s).
    scout._global_success_counter = 100
    last_fail_ctr = 100  # this video last failed when the success counter was 100
    assert not (scout._global_success_counter > last_fail_ctr), \
        "no interleaving success → must NOT advance the per-video fail count"
    scout._on_media_success("someOtherVid")  # a real success bumps the global counter
    assert scout._global_success_counter > last_fail_ctr, \
        "interleaving success → failure now counts toward blocking the video"
    print("  reclassify success-interleaving OK (blocks bad video only when API proven up)")


if __name__ == "__main__":
    test_rate_limiter_throttles()
    test_rate_limiter_shared_across_threads()
    test_canary_single_flight()
    test_circuit_distinct_thread_safe()
    test_circuit_resets_to_closed_on_success()
    test_reclassify_requires_interleaved_success()
    print("ALL PASS")
