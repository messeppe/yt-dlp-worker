"""Unit tests for worker_ytdlp helpers (no network)."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

# Ensure worker/ is on path when run from repo root or worker/
sys.path.insert(0, os.path.dirname(__file__))

import worker_ytdlp as w


class ClassifyErrorTests(unittest.TestCase):
    def test_age_gate_is_blocked(self):
        self.assertEqual(w._classify_error("Sign in to confirm your age"), "blocked")

    def test_bot_check_is_bot(self):
        self.assertEqual(
            w._classify_error("Sign in to confirm you're not a bot"), "bot"
        )

    def test_bot_check_curly_apostrophe(self):
        # YouTube uses U+2019 in live errors
        self.assertEqual(
            w._classify_error("Sign in to confirm you\u2019re not a bot"), "bot"
        )

    def test_403_is_bot(self):
        self.assertEqual(w._classify_error("HTTP Error 403: Forbidden"), "bot")

    def test_other_is_transient(self):
        self.assertEqual(w._classify_error("Connection reset by peer"), "transient")


class FormatSpecTests(unittest.TestCase):
    def test_format_uses_comma_not_plus(self):
        # Plus would require ffmpeg merge; comma keeps streams separate.
        self.assertIn(",", w.FORMAT_SPEC)
        self.assertNotIn("+", w.FORMAT_SPEC)


class CollectDownloadsTests(unittest.TestCase):
    def test_from_requested_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            vpath = os.path.join(tmp, "v.mp4")
            apath = os.path.join(tmp, "a.m4a")
            open(vpath, "wb").write(b"v")
            open(apath, "wb").write(b"a")
            info = {
                "requested_downloads": [
                    {
                        "filepath": vpath,
                        "ext": "mp4",
                        "vcodec": "avc1",
                        "acodec": "none",
                    },
                    {
                        "filepath": apath,
                        "ext": "m4a",
                        "vcodec": "none",
                        "acodec": "mp4a",
                    },
                ]
            }
            found = w._collect_downloads(info, tmp)
            self.assertEqual(len(found), 2)
            self.assertEqual(found[0]["vcodec"], "avc1")
            self.assertEqual(found[1]["acodec"], "mp4a")


class SanitizeTests(unittest.TestCase):
    def test_path_segment(self):
        self.assertEqual(w.sanitize_path_segment("Foo Bar"), "Foo_Bar")


class YdlOptsTests(unittest.TestCase):
    def test_proxy_and_js_runtime(self):
        opts = w._ydl_opts("/tmp/x", "http://user:pass@dc.decodo.com:10001")
        self.assertEqual(opts["proxy"], "http://user:pass@dc.decodo.com:10001")
        self.assertIn("deno", opts["js_runtimes"])
        self.assertEqual(opts["remote_components"], ["ejs:github"])
        clients = opts["extractor_args"]["youtube"]["player_client"]
        self.assertEqual(clients[0], "mweb")
        self.assertIn("android_vr", clients)


class RequeueCapTests(unittest.TestCase):
    """requeue() must terminalize at MAX_SCOUT_RETRIES, else rows bump past the
    poll_job filter and sit 'queued' forever (the backlog-panel zombie floor)."""

    def _conn(self, returning):
        cur = MagicMock()
        cur.fetchone.return_value = returning
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        return conn, cur

    def test_cap_is_applied_in_sql(self):
        conn, cur = self._conn(("queued", 3))
        with patch.object(w, "MAX_SCOUT_RETRIES", 10), patch.object(w, "log_event"):
            w.requeue(conn, "vid1", "bot check")
        sql, params = cur.execute.call_args[0]
        self.assertIn("scout_retry_count + 1 >= %s", sql)
        self.assertIn("extractor_blocked", sql)
        self.assertEqual(params[0], 10)
        self.assertEqual(params[-1], "vid1")

    def test_below_cap_requeues(self):
        conn, _ = self._conn(("queued", 3))
        with patch.object(w, "MAX_SCOUT_RETRIES", 10), \
                patch.object(w, "log_event") as ev:
            w.requeue(conn, "vid1", "bot check")
        events = [c[0][2] for c in ev.call_args_list]
        self.assertEqual(events, ["queue_requeue"])

    def test_at_cap_emits_extractor_blocked(self):
        conn, _ = self._conn(("failed", 10))
        with patch.object(w, "MAX_SCOUT_RETRIES", 10), \
                patch.object(w, "log_event") as ev:
            w.requeue(conn, "vid1", "bot check")
        events = [c[0][2] for c in ev.call_args_list]
        self.assertEqual(events, ["EXTRACTOR-BLOCKED"])

    def test_missing_row_is_noop(self):
        conn, _ = self._conn(None)
        with patch.object(w, "MAX_SCOUT_RETRIES", 10), \
                patch.object(w, "log_event") as ev:
            w.requeue(conn, "gone", "bot check")
        ev.assert_not_called()

    def test_poll_job_and_requeue_share_the_cap(self):
        conn, cur = self._conn(None)
        with patch.object(w, "MAX_SCOUT_RETRIES", 10):
            w.poll_job(conn)
        self.assertEqual(cur.execute.call_args[0][1], (10,))


if __name__ == "__main__":
    unittest.main()
