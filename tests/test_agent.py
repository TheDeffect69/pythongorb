import hashlib

import tempfile
import threading
import unittest
from pathlib import Path


from agent import Cancelled, Fixture, SourceError, Worker, delay_for, download, retry_seconds
from mock_service import MockServer, fixture_chunks


class TestAgent(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = MockServer(("127.0.0.1", 0), {"items": {"1234": {"size": 98304}}})
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.thread.join, 2)
        self.addCleanup(self.server.shutdown)
        self.source = Fixture(f"http://127.0.0.1:{self.server.server_port}")
        self.item = 1234
        self.data = b"".join(fixture_chunks(self.item, 98304))
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.charges = []

    def fetch(self, **kw):
        return download(self.source, "/files/1234", self.item, len(self.data),
                        kw.get("sha", self.sha), self.root, 5, kw.get("reserve", 0),
                        self.charges.append, kw.get("cancel", lambda: None))

    def test_resume_after_interruption_and_idempotent_replay(self):
        self.server.config["items"]["1234"]["interrupt_downloads"] = 1
        with self.assertRaisesRegex(SourceError, "short_download|interrupted_download"):
            self.fetch()
        part = self.root / "123" / "1234" / "video.mp4.part"
        self.assertGreater(part.stat().st_size, 0)
        self.assertLess(part.stat().st_size, len(self.data))
        self.assertFalse(part.with_suffix("").exists())
        path = self.fetch()
        self.assertEqual(path.read_bytes(), self.data)
        self.assertFalse(part.exists())
        self.assertEqual(self.server.counts["206"], 1)
        self.assertEqual(self.fetch(), path)
        self.assertEqual(self.charges, ["download", "download"])

    def test_server_ignores_range(self):
        self.server.config["items"]["1234"]["allow_range"] = False
        folder = self.root / "123" / "1234"
        folder.mkdir(parents=True)
        (folder / "video.mp4.part").write_bytes(self.data[:17])
        self.assertEqual(self.fetch().read_bytes(), self.data)

    def test_checksum_failure_never_commits(self):
        with self.assertRaisesRegex(SourceError, "checksum_mismatch"):
            self.fetch(sha="f" * 64)
        self.assertFalse(list(self.root.rglob("video.mp4*")))

    def test_refuses_external_source_and_download(self):
        for url in ("https://example.org", "file:///videos", "http://127.0.0.1/x", "http://user@localhost"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Fixture(url)
        for url in ("//example.org/file", "http://127.0.0.1:8/files", "https://localhost/anything"):
            with self.subTest(url=url), self.assertRaisesRegex(SourceError, "invalid_download_origin"):
                self.source.url(url)

    def test_space_reserve_and_cancellation_before_network(self):
        with self.assertRaisesRegex(SourceError, "insufficient_disk"):
            self.fetch(reserve=2**63)
        def stop():
            raise Cancelled()
        with self.assertRaises(Cancelled):
            self.fetch(cancel=stop)
        self.assertEqual(self.charges, [])

    def test_retry_after_and_backoff(self):
        self.assertEqual(retry_seconds("15"), 15)
        self.assertEqual(retry_seconds("invalid"), 0)
        self.assertEqual(retry_seconds("Wed, 21 Oct 2015 07:28:00 GMT"), 0)
        self.assertGreaterEqual(delay_for(1, 20), 20)
        self.assertGreaterEqual(delay_for(2), 1)

    def test_status_classification_and_retry_feedback(self):
        worker = Worker(None, self.source, "worker-1", 2, self.root, 2)
        config = self.server.config["items"]["1234"]
        reports = []
        def call(action, body):
            reports.append((action, body))
        def item():
            return worker.process_item(1234, {"max_retries": 1}, lambda _: None, lambda: None,
                                       lambda _: None, call)
        config["media_statuses"] = [429, 200]
        result = item()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(reports[0][1]["result_status"], 429)
        self.assertGreaterEqual(reports[0][1]["retry_after"], 1)
        self.assertEqual(result["file"]["sha256"], self.sha)
        for code, expected in [(401,"forbidden"), (403,"forbidden"), (404,"not_found"), (500,"failed")]:
            config["media_statuses"] = [code]
            self.assertEqual(item()["status"], expected)
        del config["media_statuses"]
        config["download_allowed"] = False
        checks = self.server.attempts[("files", 1234)]
        self.assertEqual(item()["status"], "skipped")
        self.assertEqual(self.server.attempts[("files", 1234)], checks)


if __name__ == "__main__":
    unittest.main()

