"""Worker for the controlled video laboratory fixture. Python standard library only."""
import argparse
import concurrent.futures
import contextlib
import datetime
import email.utils
import hashlib
import http.client
import ipaddress
import json
import os
import platform
import random
import re
import shutil
import signal
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class Cancelled(Exception):
    pass


class SourceError(Exception):
    def __init__(self, status, code, retry_after=0):
        super().__init__(code)
        self.status = status
        self.code = code
        self.retry_after = retry_after


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def retry_seconds(header):
    if not header:
        return 0
    try:
        return max(0, int(header))
    except ValueError:
        try:
            return max(0, email.utils.parsedate_to_datetime(header).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0


def delay_for(attempt, retry_after=0):
    return max(retry_after, min(30, 0.25 * 2 ** min(attempt, 16)) + random.uniform(0, 0.2))


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(path, cancel):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            cancel()
            sha.update(block)
    return sha.hexdigest()


def confined(root, path):
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise SourceError("failed", "unsafe_storage_path")
    return path


def check_disk(root, reserve_bytes, remaining=0):
    if shutil.disk_usage(root).free < reserve_bytes + remaining:
        raise SourceError("retry_wait", "insufficient_disk")


def save_json(path, record):
    temp = path.with_name(path.name + ".tmp")
    confined(path.parent.resolve(), temp)
    with temp.open("w", encoding="utf-8") as stream:
        json.dump(record, stream, ensure_ascii=False)
    os.replace(temp, path)


class Fixture:
    """Use a single operator-configured origin and never follow redirects."""
    def __init__(self, url):
        parsed = urllib.parse.urlsplit(url)
        host = parsed.hostname or ""
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = host in {"localhost", "mock"}
        if (not local or parsed.scheme not in {"http", "https"} or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment or parsed.path not in {"", "/"}):
            raise ValueError("source must be the local lab mock origin")
        self.origin = url.rstrip("/")
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def url(self, path):
        target = urllib.parse.urlsplit(urllib.parse.urljoin(self.origin + "/", path))
        base = urllib.parse.urlsplit(self.origin)
        if ((target.scheme, target.netloc) != (base.scheme, base.netloc) or target.fragment
                or not target.path.startswith("/") or "\\" in path):
            raise SourceError("failed", "invalid_download_origin")
        return target.geturl()

    def open(self, path, timeout, headers=None):
        req = urllib.request.Request(self.url(path), headers=headers or {})
        try:
            return self.http.open(req, timeout=timeout)
        except urllib.error.HTTPError as err:
            with err:
                code = err.code
                wait = retry_seconds(err.headers.get("Retry-After"))
            if code in {401, 403}:
                raise SourceError("forbidden", f"http_{code}") from None
            if code == 404:
                raise SourceError("not_found", "http_404") from None
            if code == 429:
                raise SourceError("rate_limited", "http_429", wait) from None
            if code >= 500 or code == 408:
                raise SourceError("retry_wait", f"http_{code}", wait) from None
            raise SourceError("failed", f"http_{code}") from None
        except (urllib.error.URLError, ConnectionError, TimeoutError, http.client.HTTPException):
            raise SourceError("retry_wait", "source_connection") from None


def download(source, route, item_id, expected_size, expected_hash, root, timeout,
             reserve, permit, cancel, on_bytes=lambda n: None, on_start=lambda status, offset: None,
             mime_type="video/mp4"):
    """Return a verified final file; an interrupted stream leaves a recoverable .part."""
    if type(item_id) is not int or item_id < 0 or type(expected_size) is not int or expected_size < 0:
        raise SourceError("failed", "invalid_metadata")
    if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_hash):
        raise SourceError("failed", "invalid_checksum")
    expected_hash = expected_hash.lower()
    root = Path(root).resolve()
    folder = confined(root, root / str(item_id)[:3] / str(item_id))
    folder.mkdir(parents=True, exist_ok=True)
    suffix = {"video/mp4":"mp4", "video/webm":"webm", "video/quicktime":"mov"}.get(mime_type, "bin")
    target = confined(root, folder / ("video." + suffix))
    part = confined(root, folder / ("video." + suffix + ".part"))

    def valid(path):
        return path.stat().st_size == expected_size and digest(path, cancel) == expected_hash

    if target.is_file() and valid(target):
        return target
    cancel()
    # A full, valid .part may remain after a process stops just before rename.
    if part.exists() and part.stat().st_size >= expected_size:
        if valid(part):
            part.replace(target)
            return target
        part.unlink()
    offset = part.stat().st_size if part.exists() else 0
    check_disk(root, reserve, expected_size - offset)
    permit("download")
    cancel()
    try:
        try:
            response = source.open(route, timeout, {"Range": f"bytes={offset}-"} if offset else {})
        except SourceError as err:
            if offset and err.code == "http_416":
                part.unlink(missing_ok=True)
                raise SourceError("retry_wait", "range_rejected") from None
            raise
        with response:
            if response.status == 206:
                actual = response.headers.get("Content-Range", "")
                if actual != f"bytes {offset}-{expected_size - 1}/{expected_size}":
                    part.unlink(missing_ok=True)
                    raise SourceError("retry_wait", "invalid_content_range")
            elif response.status == 200:
                offset = 0
            else:
                raise SourceError("failed", "unexpected_download_status")
            on_start(response.status, offset)
            with part.open("ab" if offset else "wb") as out:
                received = offset
                while True:
                    cancel()
                    data = response.read(65536)
                    if not data:
                        break
                    received += len(data)
                    if received > expected_size:
                        raise SourceError("failed", "oversize_response")
                    check_disk(root, reserve, len(data))
                    out.write(data)
                    on_bytes(len(data))
                out.flush()
                os.fsync(out.fileno())
    except (ConnectionError, TimeoutError, http.client.HTTPException):
        raise SourceError("retry_wait", "interrupted_download") from None
    if part.stat().st_size < expected_size:
        raise SourceError("retry_wait", "short_download")
    if not valid(part):
        part.unlink(missing_ok=True)
        raise SourceError("retry_wait", "checksum_mismatch")
    cancel()
    part.replace(target)
    return target

class APIError(Exception):
    def __init__(self, code):
        super().__init__(f"coordinator_http_{code}")
        self.code = code


class Coordinator:
    def __init__(self, origin, token):
        url = urllib.parse.urlsplit(origin)
        if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password:
            raise ValueError("invalid coordinator origin")
        if url.scheme == "http" and url.hostname not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("remote coordinator requires HTTPS")
        self.origin = origin.rstrip("/")
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        self.token = token
        self.coordinator_timeouts = 0
        self.counter_lock = threading.Lock()

    def post(self, path, body):
        req = urllib.request.Request(self.origin + "/api/" + path,
                                     json.dumps(body).encode(),
                                     {"Content-Type": "application/json",
                                      "Authorization": "Bearer " + self.token})
        try:
            with self.http.open(req, timeout=12) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as err:
            with err:
                raise APIError(err.code) from None
        except (OSError, http.client.HTTPException) as error:
            if isinstance(error, TimeoutError) or isinstance(getattr(error, "reason", None), TimeoutError):
                with self.counter_lock:
                    self.coordinator_timeouts += 1
            raise APIError(503) from None


class Monitor:
    def __init__(self, root):
        self.root = root
        self.start = self.last = time.monotonic()
        self.last_bytes = self.last_count = 0
        self.lock = threading.Lock()
        self.items = {}
        self.stats = dict(bytes_downloaded=0, processed=0, found=0, errors=0, retries=0, http_requests=0)
        self.codes = {}
        self.cpu = None
        self.durations = {"check": [0, 0.0], "download": [0, 0.0]}

    def update(self, item, **fields):
        with self.lock:
            self.items.setdefault(item, {"external_id": item}).update(fields)

    def finish(self, item, status):
        with self.lock:
            self.items.pop(item, None)
            if status:
                self.stats["processed"] += 1
                self.stats["errors"] += int(status == "failed")

    def add(self, key, value=1):
        with self.lock:
            self.stats[key] += value

    def timing(self, kind, elapsed):
        with self.lock:
            self.durations[kind][0] += 1
            self.durations[kind][1] += elapsed

    def http_status(self, code):
        with self.lock:
            self.codes[str(code)] = self.codes.get(str(code), 0) + 1

    def bytes(self, item, n):
        with self.lock:
            self.stats["bytes_downloaded"] += n
            self.items[item]["bytes"] = self.items[item].get("bytes", 0) + n

    def snapshot(self):
        now = time.monotonic()
        disk = shutil.disk_usage(self.root)
        with self.lock:
            rows = [x.copy() for x in self.items.values()]
            values = dict(self.stats, http_statuses=dict(self.codes), active_items=rows,
                          active_checks=sum(x.get("status") == "checking" for x in rows),
                          active_downloads=sum(x.get("status") == "downloading" for x in rows),
                          total_space_gb=disk.total/2**30, free_space_gb=disk.free/2**30,
                          uptime_seconds=now-self.start)
            elapsed = max(now-self.last, 0.001)
            values["download_bytes_per_second"] = (values["bytes_downloaded"] - self.last_bytes) / elapsed
            values["processed_per_second"] = (values["processed"] - self.last_count) / elapsed
            for kind, (count, total) in self.durations.items():
                values["avg_" + kind + "_seconds"] = total/count if count else 0
            self.last = now
            self.last_bytes = values["bytes_downloaded"]
            self.last_count = values["processed"]
        if hasattr(os, "getloadavg"):
            values["load_average"] = os.getloadavg()[0]
        if platform.system() == "Linux":
            self.linux_stats(values)
        elif platform.system() == "Windows":
            self.windows_stats(values)
        return values

    def windows_stats(self, values):
        import ctypes
        from ctypes import wintypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", wintypes.DWORD), ("load", wintypes.DWORD)] + [
                (field, ctypes.c_ulonglong) for field in ("ram", "free_ram", "commit", "free_commit",
                                                         "virtual", "free_virtual", "extended")]

        kernel = ctypes.windll.kernel32
        mem = MemoryStatus()
        mem.length = ctypes.sizeof(mem)
        if kernel.GlobalMemoryStatusEx(ctypes.byref(mem)):
            values.update(ram_total_bytes=mem.ram, ram_used_bytes=mem.ram-mem.free_ram,
                          commit_used_bytes=mem.commit-mem.free_commit)
        idle, system, user = (wintypes.FILETIME() for _ in range(3))
        if kernel.GetSystemTimes(ctypes.byref(idle), ctypes.byref(system), ctypes.byref(user)):
            def ticks(ft):
                return (ft.dwHighDateTime << 32) + ft.dwLowDateTime
            total, idle_ticks = ticks(system)+ticks(user), ticks(idle)
            if self.cpu and total > self.cpu[0]:
                values["cpu_percent"] = round(100*(1-(idle_ticks-self.cpu[1])/(total-self.cpu[0])), 1)
            self.cpu = (total, idle_ticks)

    def linux_stats(self, values):
        with contextlib.suppress(OSError, IndexError, ValueError):
            ticks = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
            total, idle = sum(ticks), sum(ticks[3:5])
            if self.cpu and total > self.cpu[0]:
                values["cpu_percent"] = round(100*(1-(idle-self.cpu[1])/(total-self.cpu[0])), 1)
            self.cpu = (total, idle)
        with contextlib.suppress(OSError, KeyError, ValueError):
            mem = dict((k.rstrip(":"), int(v)*1024) for k, v, *_ in
                       (row.split() for row in Path("/proc/meminfo").read_text().splitlines()))
            values.update(ram_total_bytes=mem["MemTotal"], ram_used_bytes=mem["MemTotal"]-mem["MemAvailable"],
                          swap_used_bytes=mem["SwapTotal"]-mem["SwapFree"])
        with contextlib.suppress(OSError, IndexError, ValueError):
            devices = [line.split(":")[1].split() for line in Path("/proc/net/dev").read_text().splitlines()[2:]]
            values.update(network_rx_bytes=sum(int(d[0]) for d in devices),
                          network_tx_bytes=sum(int(d[8]) for d in devices))
        with contextlib.suppress(OSError, IndexError, ValueError):
            disks = [row.split() for row in Path("/proc/diskstats").read_text().splitlines()]
            disks = [d for d in disks if (Path("/sys/block") / d[2]).is_dir() and not d[2].startswith("loop")]
            values.update(disk_read_bytes=512*sum(int(d[5]) for d in disks),
                          disk_write_bytes=512*sum(int(d[9]) for d in disks))
        with contextlib.suppress(OSError):
            values["open_files"] = len(list(Path("/proc/self/fd").iterdir()))
            values["active_connections"] = sum(row.split()[3] == "01" for f in ["tcp", "tcp6"] for row in
                                               (Path("/proc/net")/f).read_text().splitlines()[1:])


class Worker:
    def __init__(self, api, source, worker_id, threads, root, heartbeat, max_connections=512,
                 worker_rps=0, check_rps=0, download_rps=0):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", worker_id) or not 1 <= threads <= 512 or heartbeat <= 0:
            raise ValueError("invalid worker settings")
        if min(worker_rps, check_rps, download_rps) < 0 or not 1 <= max_connections <= 512:
            raise ValueError("invalid worker limits")
        self.max_connections = max_connections
        self.rates = {"all": worker_rps, "check": check_rps, "download": download_rps}
        self.rate_lock = threading.Lock()
        self.next_allowed = dict.fromkeys(self.rates, 0.0)
        self.api, self.source = api, source
        self.id, self.threads, self.interval = worker_id, threads, heartbeat
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.metrics = Monitor(self.root)
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.active = None

    def post(self, action, data):
        return self.api.post("workers/" + self.id + "/" + action, data)

    def heartbeat(self):
        with self.lock:
            active = self.active
        payload = dict(metrics=self.metrics.snapshot())
        payload["metrics"]["coordinator_timeouts"] = getattr(self.api, "coordinator_timeouts", 0)
        if active:
            payload.update(active[0])
        res = self.post("heartbeat", payload)
        with self.lock:
            if res.get("threads") is not None:
                self.threads = max(1, min(512, int(res["threads"])))
            if active and self.active is active and res.get("action") == "stop":
                active[1].set()

    def heartbeat_loop(self):
        while not self.stop.wait(self.interval):
            try:
                self.heartbeat()
            except APIError as err:
                if err.code in {401, 403, 404, 409, 410}:
                    with self.lock:
                        if self.active:
                            self.active[1].set()

    def run(self):
        beat = None
        try:
            registered = False
            while not self.stop.is_set():
                try:
                    if not registered:
                        self.api.post("workers/register", dict(id=self.id, name=self.id, threads=self.threads,
                                          storage_path=str(self.root), version="1.0"))
                        self.heartbeat()
                        registered = True
                        if beat is None:
                            beat = threading.Thread(target=self.heartbeat_loop, daemon=True)
                            beat.start()
                    lease = self.post("lease", {}).get("chunk")
                    if lease:
                        self.run_chunk(lease)
                    else:
                        self.stop.wait(min(1, self.interval))
                except APIError as err:
                    if err.code in {401, 403}:
                        raise
                    if err.code == 404:
                        registered = False
                    self.stop.wait(min(2, self.interval))
        finally:
            self.stop.set()
            if beat:
                beat.join(timeout=13)

    def run_chunk(self, chunk):
        envelope = {"chunk_id": chunk["id"], "attempt": chunk["attempt"]}
        cancel_event = threading.Event()
        self.threads = max(1, min(512, int(chunk.get("threads", self.threads))))
        with self.lock:
            self.active = (envelope, cancel_event)

        def cancel():
            if self.stop.is_set() or cancel_event.is_set():
                raise Cancelled()

        def pause(seconds):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                cancel()
                cancel_event.wait(min(0.2, max(0, deadline-time.monotonic())))
            cancel()

        def call(action, payload):
            while True:
                cancel()
                try:
                    return self.post(action, {**envelope, **payload})
                except APIError as err:
                    if err.code in {401, 403, 404, 409, 410}:
                        cancel_event.set()
                        raise Cancelled() from None
                    if err.code < 500 and err.code != 429:
                        raise
                    pause(0.5)

        def permit(kind):
            while True:
                # Reserve both limits together. A rejected global request consumes no local slot.
                keys = [key for key in ("all", kind) if self.rates[key]]
                with self.rate_lock if keys else contextlib.nullcontext():
                    local_wait = max((self.next_allowed[k]-time.monotonic() for k in keys), default=0)
                    if local_wait <= 0:
                        reply = call("permit", {"kind": kind})
                        if reply.get("granted"):
                            for key in keys:
                                self.next_allowed[key] = time.monotonic() + 1/self.rates[key]
                            self.metrics.add("http_requests")
                            return
                        local_wait = max(0.01, float(reply.get("wait", 0.1)))
                pause(local_wait)

        def process(item):
            status = None
            try:
                report = self.process_item(item, chunk, permit, cancel, pause, call)
                call("progress", {"external_id": item, **report})
                status = report["status"]
            except BaseException:
                cancel_event.set()
                raise
            finally:
                self.metrics.finish(item, status)

        try:
            skip = set(chunk.get("skip_ids", []))
            pending = iter(i for i in range(chunk.get("next_id", chunk["range_start"]), chunk["range_end"]+1)
                           if i not in skip)
            with concurrent.futures.ThreadPoolExecutor(max_workers=512) as pool:
                futures = set()
                exhausted = False
                while not exhausted or futures:
                    cancel()
                    while not exhausted and len(futures) < min(self.threads, self.max_connections):
                        item = next(pending, None)
                        if item is None:
                            exhausted = True
                        else:
                            futures.add(pool.submit(process, item))
                    if futures:
                        done, _ = concurrent.futures.wait(futures, timeout=0.25,
                                return_when=concurrent.futures.FIRST_COMPLETED)
                        for task in done:
                            task.result()
                        futures.difference_update(done)
            call("complete", {})
        except (Cancelled, APIError, OSError, SourceError):
            cancel_event.set()
            # The coordinator verifies the fencing token even on a failure report.
            with contextlib.suppress(APIError):
                self.post("fail", dict(envelope, error="lease_interrupted"))
        finally:
            with self.lock:
                self.active = None

    def process_item(self, item, chunk, permit, cancel, pause, call):
        meta = {}
        route = "/media/" + str(item)
        phase = "check"
        timeout = float(chunk.get("request_timeout_seconds", 20))
        retries = int(chunk.get("max_retries", 4))
        reserve = int(float(chunk.get("min_free_space_gb", 0)) * 2**30)
        for attempt in range(retries + 1):
            cancel()
            try:
                if phase == "check":
                    self.metrics.update(item, status="checking")
                    permit("check")
                    started = time.monotonic()
                    with self.source.open(route, timeout) as response:
                        self.metrics.http_status(response.status)
                        raw = response.read(65537)
                    self.metrics.timing("check", time.monotonic()-started)
                    if len(raw) > 65536:
                        raise SourceError("failed", "oversize_metadata")
                    data = json.loads(raw)
                    if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] != item:
                        raise SourceError("failed", "invalid_metadata")
                    page_url = data.get("source_url")
                    if page_url != f"https://kinescope.io/{item}":
                        page_url = self.source.url(route)
                    meta = dict(id=item, title=str(data.get("title", ""))[:1024],
                                source_url=page_url, discovered_at=stamp())
                    self.metrics.add("found")
                    if data.get("download_allowed") is not True:
                        return {"status": "skipped", "metadata": meta}
                    download_url = self.source.url(data["download_url"])
                    meta.update(download_url=download_url, size=data["size"], sha256=data["sha256"],
                                mime_type=str(data["mime_type"])[:200])
                    phase = "download"
                self.metrics.update(item, status="downloading", title=meta["title"], bytes=0, size=meta["size"])
                started = time.monotonic()
                def on_start(status, offset):
                    self.metrics.http_status(status)
                    self.metrics.update(item, bytes=offset)
                path = download(self.source, meta["download_url"], item, meta["size"], meta["sha256"],
                                self.root, timeout, reserve, permit, cancel,
                                lambda n: self.metrics.bytes(item, n), on_start, mime_type=meta["mime_type"])
                self.metrics.timing("download", time.monotonic()-started)
                meta.update(downloaded_at=stamp(), worker_id=self.id, storage_path=str(path), status="completed")
                save_json(path.with_name("metadata.json"), meta)
                return {"status": "completed", "metadata": meta,
                        "file": {"storage_path": str(path), "size": meta["size"], "sha256": meta["sha256"]}}
            except (KeyError, TypeError, ValueError, UnicodeError):
                return {"status":"failed", "metadata":meta, "error":"invalid_metadata"}
            except (OSError, http.client.HTTPException) as err:
                if isinstance(err, OSError) and err.errno == 28:
                    raise SourceError("retry_wait", "insufficient_disk") from None
                failure = SourceError("retry_wait", "io_interrupted")
            except SourceError as err:
                failure = err
            if failure.code == "insufficient_disk":
                raise failure
            if failure.code.startswith("http_"):
                status = int(failure.code[5:])
                self.metrics.http_status(status)
                if status == 429 or status >= 500:
                    call("permit", {"kind":phase, "result_status":status, "retry_after":failure.retry_after})
            if failure.status not in {"retry_wait","rate_limited"} or attempt == retries:
                status = failure.status if failure.status not in {"retry_wait","rate_limited"} else "failed"
                return {"status":status, "metadata":meta, "error":failure.code}
            self.metrics.add("retries")
            self.metrics.update(item, status=failure.status)
            pause(delay_for(attempt, failure.retry_after))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coordinator", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--storage", required=True)
    parser.add_argument("--id", required=True)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--heartbeat", type=float, default=2)
    parser.add_argument("--worker-rps", type=int, default=0, help="local total requests/second; 0 disables this cap")
    parser.add_argument("--check-rps", type=int, default=0)
    parser.add_argument("--download-rps", type=int, default=0)
    parser.add_argument("--max-connections", type=int, default=512)
    args = parser.parse_args()
    token = os.environ.get("LAB_WORKER_TOKEN")
    if not token:
        parser.error("LAB_WORKER_TOKEN is required")
    worker = Worker(Coordinator(args.coordinator, token), Fixture(args.source), args.id,
                    args.threads, args.storage, args.heartbeat, args.max_connections,
                    args.worker_rps, args.check_rps, args.download_rps)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: worker.stop.set())
    worker.run()


if __name__ == "__main__":
    main()
