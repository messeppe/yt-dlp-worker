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
        self.assertIn("android_vr", opts["extractor_args"]["youtube"]["player_client"])


if __name__ == "__main__":
    unittest.main()
