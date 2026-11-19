import json
import signal
import socket
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

import demo


class LauncherTest(unittest.TestCase):
    def test_launcher_processes_catalog_end_to_end(self):
        # Reserve different ports for this test without assuming the demo defaults are free.
        with socket.socket() as first, socket.socket() as second:
            first.bind(("127.0.0.1", 0))
            second.bind(("127.0.0.1", 0))
            api_port, source_port = first.getsockname()[1], second.getsockname()[1]

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            manifest = root / "catalog.json"
            manifest.write_text(json.dumps({"2":{"title":"Seminar","download_allowed":True,
                                                "file":"seminar.webm","mime_type":"video/webm"}}))
            (root / "seminar.webm").write_bytes(b"operator-owned-file")
            state = root / "state"
            token = demo.credentials(state)["operator"]
            start_args = ["demo.py","--state",str(state),"--catalog",str(manifest),
                          "--port",str(api_port),"--mock-port",str(source_port)]
            errors = []
            handlers = {}

            def run():
                try:
                    demo.main()
                except BaseException as error:
                    errors.append(error)

            def api(path, body=None):
                req = urllib.request.Request(f"http://127.0.0.1:{api_port}/api/" + path,
                    None if body is None else json.dumps(body).encode(),
                    {"Authorization":"Bearer "+token,"Content-Type":"application/json"})
                with urllib.request.urlopen(req, timeout=2) as response:
                    return json.load(response)

            def wait_for(condition):
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline:
                    self.assertFalse(errors, str(errors))
                    try:
                        if condition():
                            return
                    except OSError:
                        pass
                    time.sleep(0.2)
                self.fail("demo readiness or completion timeout")

            with patch("sys.argv",start_args), patch("demo.signal.signal",handlers.__setitem__):
                thread = threading.Thread(target=run)
                thread.start()
                try:
                    wait_for(lambda:len(api("workers")) == 3)
                    job = api("jobs",{"range":"[1,3]","chunk_size":1})
                    path = f"jobs/{job['id']}"
                    api(path + "/start", {})
                    wait_for(lambda:api(path)["status"] == "completed")
                    items = api(path+"/items")
                    self.assertEqual([r["status"] for r in items], ["not_found","completed","not_found"])
                    file = Path(items[1]["file"]["storage_path"])
                    self.assertEqual(file.name, "video.webm")
                    self.assertEqual(file.read_bytes(), b"operator-owned-file")
                    self.assertEqual(items[1]["metadata"]["source_url"], "https://kinescope.io/2")
                finally:
                    if signal.SIGINT in handlers:
                        handlers[signal.SIGINT](signal.SIGINT, None)
                    thread.join(20)
                self.assertFalse(thread.is_alive())
                self.assertFalse(errors)
