import hashlib
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock

from agent import Kinescope, SourceError, Worker
from kinescope_index import build_index


class Response(io.BytesIO):
    status = 200
    headers = {}


class KinescopeContractTests(unittest.TestCase):
    def setUp(self):
        self.payload = b"authorized workspace video"
        self.asset = {"quality":"original","filetype":"mp4","file_size":len(self.payload),
                      "md5":hashlib.md5(self.payload, usedforsecurity=False).hexdigest(),
                      "download_link":"https://s3.kinescope.io/assets/video.mp4?sign=private-test-value"}
        self.video = {"id":"video-uuid","title":"Test","play_link":"https://kinescope.io/27",
                      "status":"done","assets":[self.asset]}
        self.source = Kinescope({"27":"video-uuid"}, "private-test-token")
        self.requests = []
        self.source.http = Mock()
        self.source.http.open.side_effect = self.open
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.worker = Worker(None, self.source, "worker", 1, Path(root.name), 1)

    def open(self, request, timeout):
        self.requests.append((request.full_url, request.get_header("Authorization")))
        if request.full_url.startswith(Kinescope.api):
            return Response(json.dumps({"data":self.video}).encode())
        if request.full_url == self.video["play_link"]:
            return Response(b"player")
        if request.full_url == self.asset["download_link"]:
            return Response(self.payload)
        self.fail("Unexpected HTTP request")

    def process(self, n=27):
        return self.worker.process_item(n, {"max_retries":0}, lambda _:None, lambda:None,
                                        lambda _:None, lambda *args:None)

    def test_real_source_contract_and_no_secret_in_results(self):
        result = self.process()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["file"]["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(Path(result["file"]["storage_path"]).read_bytes(), self.payload)
        self.assertEqual(self.requests, [(Kinescope.api+"/video-uuid","Bearer private-test-token"),
                         (self.video["play_link"],None),(self.asset["download_link"],None)])
        self.assertNotIn("private-test", json.dumps(result))
        self.assertNotIn("private-test", Path(result["file"]["storage_path"]).with_name("metadata.json").read_text())

    def test_missing_remote_checksum_verifies_full_transfer(self):
        del self.asset["md5"]
        self.assertEqual(self.process()["file"]["sha256"], hashlib.sha256(self.payload).hexdigest())

    def test_missing_permission_and_unlisted_id(self):
        self.assertEqual(self.process(54)["status"], "skipped")
        self.assertFalse(self.requests)
        self.asset["url"] = self.asset.pop("download_link")
        self.assertEqual(self.process()["status"], "skipped")
        self.assertEqual(len(self.requests), 2)

    def test_download_rejects_foreign_origin_and_corrupt_bytes(self):
        self.asset["download_link"] = "https://kinescope.io.other.example/x"
        self.assertEqual(self.process()["error"], "invalid_download_origin")
        self.assertEqual(len(self.requests), 2)
        self.asset["download_link"] = "https://s3.kinescope.io/x"
        self.asset["md5"] = "0" * 32
        self.assertEqual(self.process()["error"], "checksum_mismatch")

    def test_denied_page_prevents_download(self):
        def denied(req, timeout):
            if req.full_url == self.video["play_link"]:
                raise urllib.error.HTTPError(req.full_url,403,"denied",{},io.BytesIO(b""))
            return self.open(req, timeout)
        self.source.http.open.side_effect = denied
        self.assertEqual(self.process()["status"], "forbidden")
        self.assertEqual(self.source.http.open.call_count, 2)

    def test_workspace_pagination_uses_numeric_player_links(self):
        source = Mock()
        source.api_json.side_effect = [
            {"meta":{"pagination":{"page":1,"per_page":1,"total":2}},
             "data":[dict(self.video, project_id="project")]},
            {"meta":{"pagination":{"page":2,"per_page":1,"total":2}},
             "data":[dict(self.video, project_id="project", play_link="https://kinescope.io/xyz")]}]
        self.assertEqual(build_index(source, "project"), ({"27":"video-uuid"}, 1))
