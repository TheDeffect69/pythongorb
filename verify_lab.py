"""Run reproducible failover and concurrency checks on a disposable loopback lab."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parent
WORKERS = ["worker-1", "worker-2", "worker-3"]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def ensure(test, detail):
    if not test:
        raise AssertionError(detail)


def wait(description, probe, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = probe()
        except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
            last = str(error)
        else:
            if last:
                return last
        time.sleep(0.12)
    raise TimeoutError(f"{description}: {last}")


def port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Lab:
    def __init__(self, folder, workers=WORKERS):
        self.folder = folder
        self.workers = workers
        self.api_port = port()
        self.source_port = port()
        while self.api_port == self.source_port:
            self.source_port = port()
        self.api = f"http://127.0.0.1:{self.api_port}/api"
        self.source = f"http://127.0.0.1:{self.source_port}"
        self.processes = {}
        self.http = HTTP
        self.coordinator_options = []
        self.worker_env = {}
        self.capture_logs = False
        self.keys = {k: secrets.token_urlsafe(32) for k in ["admin","operator","viewer",*self.workers]}

    def start(self, name, script, args, extra_env=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith("LAB_")}
        env.update(extra_env or {})
        log = (self.folder / (name + ".log")).open("ab") if self.capture_logs else None
        try:
            self.processes[name] = subprocess.Popen([sys.executable, str(ROOT / script), *map(str, args)],
                env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        finally:
            if log:
                log.close()

    def kill(self, name):
        proc = self.processes.pop(name)
        proc.kill()
        proc.wait(timeout=10)

    def close(self):
        for name in list(self.processes)[::-1]:
            self.kill(name)

    def coordinator(self, lease):
        self.start("api", "coordinator.py", ["--port", self.api_port, "--db", self.folder/"state.db",
            "--lease-seconds", lease, "--heartbeat-seconds", 1, *self.coordinator_options],
            {**{"LAB_"+role.upper()+"_TOKEN":self.keys[role] for role in ("admin","operator","viewer")},
             "LAB_WORKER_TOKENS":json.dumps({w:self.keys[w] for w in self.workers})})
        wait("coordinator ready", lambda: self.request("/me"), 15)

    def worker(self, name, threads):
        self.start(name, "agent.py", ["--coordinator", self.api[:-4], "--source", self.source,
            "--id",name,"--threads",threads,"--storage",self.folder/name,"--heartbeat",0.5],
            {"LAB_WORKER_TOKEN":self.keys[name], **self.worker_env})

    def setup(self, config, threads, lease):
        profile = self.folder/"profile.json"
        profile.write_text(json.dumps(config), encoding="utf-8")
        self.start("mock","mock_service.py",["--port",self.source_port,"--config",profile])
        wait("mock ready", self.stats, 15)
        self.coordinator(lease)
        for name in self.workers:
            self.worker(name,threads)
        wait("all workers online", lambda: len(self.request("/workers")) == len(self.workers))

    def stats(self):
        with HTTP.open(self.source+"/stats", timeout=5) as resp:
            return json.load(resp)

    def request(self, path, method="GET", data=None, role="operator"):
        headers = {"Content-Type":"application/json"}
        if role:
            headers["Authorization"]="Bearer "+ self.keys[role]
        request = urllib.request.Request(self.api+path, method=method, headers=headers,
            data=json.dumps(data).encode() if data is not None else None)
        with self.http.open(request, timeout=8) as response:
            return json.load(response)

    def denied(self, code, *args, **kwargs):
        try:
            self.request(*args, **kwargs)
        except urllib.error.HTTPError as exc:
            ensure(exc.code == code, f"expected {code}, got {exc.code}")
            exc.close()
        else:
            raise AssertionError("Unauthorized request succeeded")

    def job(self, **settings):
        job = self.request("/jobs","POST",settings)
        path = f"/jobs/{job['id']}"
        self.request(path+"/start","POST",{})
        return path

    def completed(self, path):
        result = self.request(path)
        return result if result["status"] == "completed" else None


def recovery(lab):
    profile = json.loads((ROOT/"fixtures/demo.json").read_text(encoding="utf-8"))
    profile["delay_ms"] = 160
    # The first owner can die after the mock consumes its first response.
    # Repeat the throttling fixture in a block processed by a surviving worker.
    profile["items"]["26"] = {"media_statuses":[429,200], "retry_after":1}
    lease = 4
    lab.setup(profile,4,lease)
    lab.denied(401,"/jobs",role=None)
    lab.denied(403,"/jobs","POST",{"range":"0"},role="viewer")
    lab.denied(403,"/workers/worker-1/service","POST",{"action":"status"})
    ensure(lab.request("/audit", role="admin")[0]["result"] == "denied", "SSH denial not audited")

    path = lab.job(range="[0,59]",chunk_size=12,threads=4,rate_limit=120,worker_ids=[WORKERS[0]])
    def active_chunk():
        return lab.request("/workers/"+WORKERS[0])["current_chunk"]
    chunk = wait("first lease",active_chunk)["id"]
    lab.kill(WORKERS[0])
    death = time.time()
    lab.request(path,"PATCH",{"worker_ids":WORKERS})
    def takeover():
        return next((event for event in lab.request(path+"/events?limit=1000")
            if event["chunk_id"] == chunk and event["result"] == "leased" and event["worker_id"] != WORKERS[0]), None)
    reassigned = wait("reassignment",takeover)
    takeover_seconds = reassigned["ts"] - death
    ensure(takeover_seconds <= lease + 2, f"reassignment exceeded deadline: {takeover_seconds:.2f}s")
    # Keep results to verify that a completed acknowledgement survives a hard stop.
    confirmed = wait("persisted results",lambda: lab.request(path+"/items"))
    lab.kill("api")
    lab.coordinator(lease)
    restored = {r["external_id"]:r for r in lab.request(path+"/items")}
    ensure(all(restored.get(r["external_id"]) == r for r in confirmed), "lost committed rows")
    lab.worker(WORKERS[0],4)
    wait("recovery job completion",lambda:lab.completed(path),100)
    rows = lab.request(path+"/items?limit=1000")
    ensure([r["external_id"] for r in rows] == list(range(60)), "range coverage")
    expect = {1:"forbidden",4:"skipped",5:"not_found"}
    for item in rows:
        i = item["external_id"]
        status = expect.get(i, "not_found" if i % 5 == 0 else "completed")
        ensure(item["status"] == status, f"ID {i}: {item['status']} instead of {status}")
        if status == "completed":
            file = Path(item["file"]["storage_path"])
            ensure(file.is_relative_to(lab.folder/item["worker_id"]), "file outside worker directory")
            ensure(hashlib.sha256(file.read_bytes()).hexdigest() == item["file"]["sha256"], "digest mismatch")
    events = lab.request(path+"/events?limit=1000")
    ensure(any(e["code"] == "429" for e in events), "missing rate limit event")
    counts = Counter(i["status"] for i in rows)
    ensure(sum(1 for f in lab.folder.glob("worker-*/*/*/video.mp4")) == counts["completed"],"duplicate files")
    # Hold the first chunk using a reserve larger than every measured local disk.
    disk = lab.job(range="[100,111]", chunk_size=2, threads=1, rate_limit=5,
                   min_free_space_gb=1_000_000_000)
    time.sleep(1.25)
    ensure(not any(e["result"] == "leased" for e in lab.request(disk+"/events")), "disk gate bypassed")
    lab.request(disk,"PATCH",{"min_free_space_gb":0})
    wait("disk gate reopens",lambda:any(e["result"] == "leased" for e in lab.request(disk+"/events")))
    lab.request(disk+"/pause","POST",{})
    wait("soft pause",lambda:lab.request(disk)["status"]=="paused")
    lab.request(disk,"PATCH",{"threads":3,"rate_limit":20})
    lab.request(disk+"/resume","POST",{})
    wait("running after resume", lambda:lab.request(disk)["status"]=="running")
    lab.request(disk+"/stop","POST",{})
    ensure(lab.request(disk)["status"]=="stopped", "stop")
    return {"confirmed_at_restart":len(confirmed),"results":counts,
        "takeover_seconds":round(takeover_seconds,2),"previous_owner":WORKERS[0],
        "new_owner":reassigned["worker_id"],"responses":lab.stats()["responses"]}


def load(lab):
    # Slow missing pages keep real HTTP requests active while more workers enter.
    large_ids = range(2000, 2003)
    large_size = 16 * 1024**2
    lab.setup({"delay_ms":4500,"missing_every":1,"items":{
        str(i):{"size":large_size,"delay_ms":500} for i in large_ids}},48,30)
    started = time.monotonic()
    path = lab.job(range="[1000,1143]",chunk_size=48,threads=48,rate_limit=5000)
    result = wait("load completion",lambda:lab.completed(path),120)
    elapsed = time.monotonic()-started
    stats = lab.stats()
    rows = lab.request(path+"/items?limit=200")
    ensure(result["processed"] == 144 and len(rows) == 144, "load job incomplete")
    ensure(all(i["status"]=="not_found" for i in rows), "unexpected load classification")
    ensure(len({i["worker_id"] for i in rows})==3, "load not spread across three agents")
    ensure(stats["peak"]>=100, f"observed concurrency {stats['peak']}, required 100")
    io_started = time.monotonic()
    io_path = lab.job(range="[2000,2002]",chunk_size=1,threads=1,rate_limit=100)
    wait("large downloads", lambda:lab.completed(io_path))
    downloads = lab.request(io_path+"/items")
    io_time = time.monotonic() - io_started
    ensure(len(downloads) == 3, "three completed large IDs")
    for r in downloads:
        ensure(r["status"]=="completed" and r["file"]["size"]==large_size, "large file incomplete")
        with Path(r["file"]["storage_path"]).open("rb") as f:
            ensure(hashlib.file_digest(f,"sha256").hexdigest()==r["file"]["sha256"],"large file checksum")
    return {"workers":3,"threads_per_worker":48,"peak_parallel_http":stats["peak"],
            "large_files":len(downloads),"large_file_bytes":large_size,"large_job_seconds":round(io_time,2),
            "items":len(rows),"seconds":round(elapsed,2),"items_per_second":round(len(rows)/elapsed,2)}


def fleet(lab):
    lab.setup({"delay_ms":200,"missing_every":1},5,30)
    def timeout_count():
        cutoff = time.time()
        def sample():
            nodes = lab.request("/workers")
            if all(w["last_heartbeat"] >= cutoff and "coordinator_timeouts" in w["metrics"] for w in nodes):
                return nodes
        return sum(w["metrics"]["coordinator_timeouts"] for w in wait("fresh fleet heartbeats", sample))
    initial_timeouts = timeout_count()
    start = time.monotonic()
    path = lab.job(range="[0,1999]",chunk_size=25,threads=5,rate_limit=3000)
    job = wait("fleet job",lambda:lab.completed(path),300)
    results = lab.request(path+"/items?limit=2500")
    counts = Counter(row["worker_id"] for row in results)
    events = lab.request(path+"/events?limit=5000")
    expired = sum(e["operation"]=="lease" and e["result"]=="expired" for e in events)
    seconds = time.monotonic()-start
    timeouts = timeout_count() - initial_timeouts
    ensure(job["processed"] == 2000 and len(counts)==len(lab.workers), "fleet distribution")
    ensure([row["external_id"] for row in results] == list(range(2000)), "fleet ID coverage")
    ensure(all(row["status"] == "not_found" for row in results), "fleet item status")
    return {"workers":len(counts),"items":len(results),"seconds":round(seconds,2),"expired_leases":expired,
            "coordinator_timeouts":timeouts,
            "minimum_items_per_worker":min(counts.values()),"maximum_items_per_worker":max(counts.values())}


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=["all","recovery","load","fleet"], default="all")
    args=parser.parse_args()
    for name, run in [("recovery",recovery),("load",load),("fleet",fleet)]:
        if args.case not in ("all",name):
            continue
        print(f"Running {name}",flush=True)
        with tempfile.TemporaryDirectory(prefix="video-lab-qa-") as folder:
            nodes = [f"worker-{n}" for n in range(1,21)] if name=="fleet" else WORKERS
            lab=Lab(Path(folder),nodes)
            try:
                print(json.dumps(run(lab),ensure_ascii=True),flush=True)
            finally:
                lab.close()
