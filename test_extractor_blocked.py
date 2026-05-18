"""Unit-ish test for distinct-id circuit + per-video reclassify guard.

Stubs DB env so scout.py imports cleanly. Calls scout's internal helpers with
synthetic sequences and asserts circuit/block outcomes per plan section A/B/B-bis.

Run: python -m pytest worker/test_extractor_blocked.py -v
   (or: cd worker && python test_extractor_blocked.py)
"""
import os
import sys
import unittest
from unittest.mock import MagicMock

os.environ.setdefault("SUPABASE_DB_URL", "postgresql://stub")
os.environ.setdefault("RAPIDAPI_KEY", "stub")
os.environ.setdefault("RAPIDAPI_HOST", "stub")
os.environ.setdefault("CIRCUIT_OPEN_THRESHOLD", "5")
os.environ.setdefault("RECLASSIFY_K", "3")

sys.path.insert(0, os.path.dirname(__file__))

import scout  # noqa: E402


def _reset_state():
    scout._media_recent_failed_ids.clear()
    scout._media_circuit_opened_at = 0.0
    scout._global_success_counter = 0
    scout._video_fail_state.clear()


class TestDistinctCircuit(unittest.TestCase):
    def setUp(self):
        _reset_state()

    def test_same_video_repeats_does_not_trip_circuit(self):
        for _ in range(10):
            scout._on_media_failure("vidA")
        self.assertFalse(scout._media_circuit_open())
        self.assertEqual(len(scout._media_recent_failed_ids), 1)

    def test_distinct_videos_trip_circuit_at_threshold(self):
        for vid in ("v1", "v2", "v3", "v4"):
            scout._on_media_failure(vid)
            self.assertFalse(scout._media_circuit_open())
        scout._on_media_failure("v5")
        self.assertTrue(scout._media_circuit_open())

    def test_success_clears_circuit(self):
        for vid in ("v1", "v2", "v3", "v4", "v5"):
            scout._on_media_failure(vid)
        self.assertTrue(scout._media_circuit_open())
        scout._on_media_success("v6")
        self.assertFalse(scout._media_circuit_open())
        self.assertEqual(len(scout._media_recent_failed_ids), 0)


class TestPerVideoReclassifyGuard(unittest.TestCase):
    """Simulates the guard logic from the TransientAPIError handler directly."""

    def setUp(self):
        _reset_state()

    def _simulate_transient(self, video_id):
        """Mimics the guard block in scout.process() transient handler."""
        prev_count, prev_success = scout._video_fail_state.get(video_id, (0, -1))
        if scout._global_success_counter > prev_success:
            new_count = prev_count + 1
        else:
            new_count = prev_count
        scout._video_fail_state[video_id] = (new_count, scout._global_success_counter)
        return new_count

    def test_interleaved_successes_bump_per_video_count(self):
        # vidA fails, vidB succeeds, vidA fails, vidC succeeds, vidA fails
        self.assertEqual(self._simulate_transient("vidA"), 1)
        scout._on_media_success("vidB")
        self.assertEqual(self._simulate_transient("vidA"), 2)
        scout._on_media_success("vidC")
        self.assertEqual(self._simulate_transient("vidA"), 3)  # would trigger block at K=3

    def test_no_interleaving_does_not_bump_after_first(self):
        # First failure always counts (counter 0 > -1).
        self.assertEqual(self._simulate_transient("vidA"), 1)
        # No successes between → subsequent failures should NOT bump.
        for _ in range(10):
            self.assertEqual(self._simulate_transient("vidA"), 1)
        self.assertLess(scout._video_fail_state["vidA"][0], scout.RECLASSIFY_K)

    def test_success_on_same_video_resets_state(self):
        self._simulate_transient("vidA")
        self.assertIn("vidA", scout._video_fail_state)
        scout._on_media_success("vidA")
        self.assertNotIn("vidA", scout._video_fail_state)


class TestRequeueAtCapSetsBlocked(unittest.TestCase):
    """Verify the SQL update in requeue_media_transient includes extractor_blocked at cap."""

    def test_sql_contains_extractor_blocked_at_cap(self):
        import inspect
        src = inspect.getsource(scout.requeue_media_transient)
        self.assertIn("extractor_blocked", src)
        self.assertIn("scout_retry_count + 1 >= %s THEN TRUE", src)

    def test_mark_extractor_blocked_logs_event(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        scout.mark_extractor_blocked(conn, "vidX", "API body status=error: try again!")
        cur.execute.assert_called_once()
        sql, params = cur.execute.call_args[0]
        self.assertIn("extractor_blocked = TRUE", sql)
        self.assertIn("media_status      = 'failed'", sql)
        self.assertEqual(params[1], "vidX")


if __name__ == "__main__":
    unittest.main(verbosity=2)
