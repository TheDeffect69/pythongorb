import hashlib
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

import mock_service
import media_catalog
import ssh_control
import storage
from coordinator import auth_tokens
from deploy import worker_release


class InfrastructureTests(unittest.TestCase):
    def test_named_users_and_token_rotation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.json"
            env = {"LAB_USERS_FILE": str(path)}
            profiles = {"lead":{"role":"administrator","token":"first-local"},
                        "qa":{"role":"viewer","token":"second-local"}}
            path.write_text(json.dumps(profiles))
            self.assertEqual(auth_tokens(env)["second-local"], ("viewer","qa"))
            profiles["qa"] = {"role":"operator","token":"new-local"}
            path.write_text(json.dumps(profiles))
            identities = auth_tokens(env)
            self.assertNotIn("second-local", identities)
            self.assertEqual(identities["new-local"], ("operator","qa"))

    def test_unapproved_release_cannot_replace_current(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "agent.py"
            source.write_text("print('test')", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "checksum"):
                worker_release.stage(root, source, "v1", "0"*64)
            self.assertFalse((root / "releases" / "v1").exists())
            self.assertFalse((root / "current").exists())
            with self.assertRaisesRegex(ValueError, "release name"):
                worker_release.stage(root, source, "../v1", "0"*64)

    def test_catalog_metadata_permission_and_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "example.mp4").write_bytes(b"example-media")
            manifest = root / "catalog.json"
            manifest.write_text(json.dumps({
                "12": {"title": "Lecture", "file": "example.mp4", "download_allowed": True},
                "15": {"title": "Private", "download_allowed": False}
            }))
            catalog = media_catalog.MediaCatalog(manifest)
            with mock_service.MockServer(("127.0.0.1", 0), catalog=catalog) as server:
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                base = f"http://127.0.0.1:{server.server_port}"
                try:
                    with urllib.request.urlopen(base + "/media/12") as response:
                        data = json.load(response)
                        self.assertEqual(data["source_url"], "https://kinescope.io/12")
                        self.assertEqual(data["sha256"], hashlib.sha256(b"example-media").hexdigest())
                    req = urllib.request.Request(base + "/files/12", headers={"Range": "bytes=3-"})
                    with urllib.request.urlopen(req) as response:
                        self.assertEqual(response.status, 206)
                        self.assertEqual(response.read(), b"mple-media")
                    for path, code in [("/files/15", 403), ("/media/13", 404)]:
                        with self.assertRaises(urllib.error.HTTPError) as failure:
                            urllib.request.urlopen(base + path)
                        self.assertEqual(failure.exception.code, code)
                        failure.exception.close()
                finally:
                    server.shutdown()
                    thread.join()

    def test_range_payload_matches_full_digest_at_arbitrary_offsets(self):
        size = 150173
        full = b"".join(mock_service.fixture_chunks(11, size))
        self.assertEqual(hashlib.sha256(full).hexdigest(), mock_service.fixture_digest(11, size))
        for offset in (0, 31, 65535, size - 1, size):
            with self.subTest(offset=offset):
                self.assertEqual(b"".join(mock_service.fixture_chunks(11, size, offset)), full[offset:])

    def test_migrations_reentrant_and_enforce_foreign_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            storage.initialize(path)
            storage.initialize(path)
            conn = storage.connect(path)
            try:
                self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT count(*) FROM roles").fetchone()[0], 3)
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                conn.close()

    def test_ssh_only_inventory_host_and_fixed_action(self):
        with tempfile.TemporaryDirectory() as directory:
            inventory = Path(directory) / "inventory.json"
            inventory.write_text(json.dumps({"w1":{"host":"worker.lab","user":"limited"}}))
            with patch.dict(os.environ, LAB_SSH_INVENTORY=str(inventory)), patch("subprocess.run") as run:
                for worker, action in [("unknown","start"),("w1","unlisted")]:
                    with self.assertRaises(ValueError):
                        ssh_control.run_service_action(worker, action)
                run.assert_not_called()
                run.return_value.returncode = 0
                ssh_control.run_service_action("w1","restart")
                argv = run.call_args.args[0]
                self.assertIn("StrictHostKeyChecking=yes", argv)
                self.assertEqual(argv[-4:], ["limited@worker.lab","systemctl","restart","lab-worker.service"])
