"""Controllable, deterministic HTTP fixture for the distributed lab."""

import argparse
import hashlib
import json
import re
import socket
import threading
import time
from collections import Counter
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from media_catalog import MediaCatalog


def fixture_chunks(item_id, size, offset=0):
    # Generate arbitrarily large, seekable test files using bounded memory.
    seed = hashlib.sha256(f"lab-fixture-{item_id}".encode()).digest()
    shift = offset % len(seed)
    block = (seed[shift:] + seed[:shift]) * 2048
    remaining = size - offset
    while remaining > 0:
        yield block[:remaining]
        remaining -= len(block)


@lru_cache(maxsize=256)
def fixture_digest(item_id, size):
    digest = hashlib.sha256()
    for chunk in fixture_chunks(item_id, size):
        digest.update(chunk)
    return digest.hexdigest()


class MockServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256

    def __init__(self, address, config=None, catalog=None):
        super().__init__(address, MockHandler)
        self.config = config or {}
        self.catalog = catalog
        self.lock = threading.Lock()
        self.attempts = Counter()
        self.active = 0
        self.peak = 0
        self.counts = Counter()

    def scenario(self, item_id):
        return self.config.get("items", {}).get(str(item_id), {})


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send_json(self, status, payload, extra=None):
        body = json.dumps(payload).encode()
        self.send_response(status)
        for key, value in (extra or {}).items():
            self.send_header(key, str(value))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        server = self.server
        if self.path == "/stats":
            with server.lock:
                result = dict(active=server.active, peak=server.peak, responses=dict(server.counts))
            self.send_json(200, result)
            return
        match = re.fullmatch(r"/(media|files)/(\d{1,19})", self.path)
        if not match:
            self.send_json(404, {})
            return
        route, raw_id = match.groups()
        item_id = int(raw_id)
        with server.lock:
            server.attempts[(route, item_id)] += 1
            attempt = server.attempts[(route, item_id)]
            server.active += 1
            server.peak = max(server.peak, server.active)
        try:
            if server.catalog is not None:
                server.catalog.serve(self, route, item_id)
            else:
                self.serve_fixture(route, item_id, attempt)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            with server.lock:
                server.active -= 1

    def send_response(self, code, message=None):
        with self.server.lock:
            self.server.counts[str(code)] += 1
        super().send_response(code, message)

    def serve_fixture(self, route, item_id, attempt):
        config = self.server.config
        case = self.server.scenario(item_id)
        time.sleep(case.get("delay_ms", config.get("delay_ms", 0)) / 1000)
        sequence = case.get(route + "_statuses", [])
        status = sequence[min(attempt, len(sequence)) - 1] if sequence else 200
        if not case and config.get("missing_every", 0) and item_id % config["missing_every"] == 0:
            status = 404
        if status != 200:
            self.send_json(status, {"status": status}, {"Retry-After": case.get("retry_after", 1)} if status == 429 else {})
            return
        size = case.get("size", config.get("size", 4096) + (item_id % 7) * 128)
        allowed = case.get("download_allowed", True)
        if route == "media":
            self.send_json(200, dict(id=item_id, title=f"Стенд {item_id}",
                download_url=f"/files/{item_id}" if allowed else None,
                size=size, sha256=fixture_digest(item_id, size),
                mime_type="video/mp4", download_allowed=allowed))
            return
        if not allowed:
            self.send_json(403, {})
            return
        offset = self.start_file(size, "video/mp4", case.get("allow_range", True))
        if offset is None:
            return
        interrupted = attempt <= case.get("interrupt_downloads", 0)
        end = offset + (size - offset) // 2 if interrupted else size
        for chunk in fixture_chunks(item_id, end, offset):
            self.wfile.write(chunk)
        if interrupted:
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)

    def start_file(self, size, mime, allow_range):
        offset = 0
        header = self.headers.get("Range") if allow_range else None
        if header:
            match = re.fullmatch(r"bytes=(\d+)-", header)
            if not match or int(match.group(1)) >= size:
                self.send_json(416, {}, {"Content-Range": f"bytes */{size}"})
                return
            offset = int(match.group(1))
        self.send_response(206 if header else 200)
        if header:
            self.send_header("Content-Range", f"bytes {offset}-{size-1}/{size}")
        if allow_range:
            self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(size - offset))
        self.send_header("Content-Type", mime)
        self.end_headers()
        return offset


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    profile = parser.add_mutually_exclusive_group()
    profile.add_argument("--config", type=Path)
    profile.add_argument("--catalog", type=Path)
    args = parser.parse_args()
    settings = json.loads(args.config.read_text(encoding="utf-8")) if args.config else {}
    catalog = MediaCatalog(args.catalog) if args.catalog else None
    with MockServer((args.host, args.port), settings, catalog) as server:
        server.serve_forever()
