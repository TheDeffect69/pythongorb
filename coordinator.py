"""HTTP coordinator for the distributed video processing lab."""

import argparse
import csv
import hmac
import io
import json
import mimetypes
import math
import ntpath
import os
import posixpath
import re
import sqlite3
import ssl
import threading
import time
from collections import deque
from contextlib import closing, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import lifecycle
from storage import connect, initialize


MAX_ID = 2**63 - 2  # Leave room for the first ID beyond the inclusive bound.
WEB_ROOT = Path(__file__).resolve().parent / "ui"


class Problem(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def integer(value, label, minimum=0, maximum=MAX_ID):
    if type(value) is not int or not minimum <= value <= maximum:
        raise Problem(400, f"{label}: требуется целое число от {minimum} до {maximum}")
    return value


def parse_range(value):
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return 0, None
        if re.fullmatch(r"[0-9]{1,19}", value):
            value = int(value)
        else:
            match = re.fullmatch(r"\[\s*([0-9]{1,19})\s*,\s*([0-9]{0,19})\s*\]", value)
            if not match:
                raise Problem(400, "Диапазон должен иметь вид A, [A,B], [A,] или пустой строки")
            value = {"from": int(match[1]), "to": int(match[2]) if match[2] else None}
    if type(value) is int:
        return 0, integer(value, "A")
    if not isinstance(value, dict) or value.keys() - {"from", "to"}:
        raise Problem(400, "Некорректный диапазон")
    start = integer(value.get("from", 0), "from")
    end = value.get("to")
    if end is not None:
        integer(end, "to", start)
    return start, end


def row(db, statement, *params):
    result = db.execute(statement, params).fetchone()
    return dict(result) if result is not None else None


def rows(db, statement, *params):
    return [dict(r) for r in db.execute(statement, params)]


def bounded_string(value, label, limit=256):
    if not isinstance(value, str) or not value or len(value) > limit or any(ord(c) < 32 for c in value):
        raise Problem(400, f"Некорректное поле {label}")
    return value


def check_fields(data, allowed):
    unknown = set(data) - set(allowed)
    if unknown:
        raise Problem(400, "Неизвестные поля: " + ", ".join(sorted(unknown)))


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise Problem(400, "Некорректный идентификатор узла")
    return value


JOB_FIELDS = {"name", "range", "threads", "rate_limit", "check_rps", "download_rps", "worker_settings",
              "chunk_size", "worker_ids", "max_retries",
              "request_timeout_seconds", "min_free_space_gb", "max_items", "max_seconds",
              "stop_on_consecutive_missing"}
FINAL_STATES = {"completed", "forbidden", "failed", "skipped", "not_found"}
ROLES = {"viewer": 1, "operator": 2, "administrator": 3}
WORKER_RATES = ("worker_rps", "check_rps", "download_rps")
WORKER_SETTINGS = {*WORKER_RATES, "threads", "min_free_space_gb"}


def real_number(value, label, low, high):
    if type(value) not in (int, float) or not low <= value <= high:
        raise Problem(400, f"Некорректное поле {label}")
    return float(value)


def worker_settings(data):
    if not isinstance(data, dict):
        raise Problem(400, "Параметры узла должны быть объектом")
    check_fields(data, WORKER_SETTINGS)
    settings = {}
    for name, value in data.items():
        if value is None:
            settings[name] = None
        elif name == "min_free_space_gb":
            settings[name] = real_number(value, name, 0, 10**9)
        else:
            settings[name] = integer(value, name, 1, 512 if name == "threads" else 10000)
    return settings


def event(db, op, result="ok", message="", job=None, worker=None, chunk=None, item=None, code=None):
    db.execute("""INSERT INTO events(ts,level,component,job_id,worker_id,chunk_id,external_id,
                  operation,result,code,message) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
               (time.time(), "warning" if result in {"failed","expired","limited"} else "info",
                "agent" if worker else "coordinator", job, worker, chunk, item, op, result, code, message))


def audit(db, actor, op, job=None, worker=None):
    db.execute("INSERT INTO audit_log(ts,actor,operation,job_id,worker_id,result) VALUES(?,?,?,?,?,'ok')",
               (time.time(), actor, op, job, worker))


def get_job(db, jid):
    job = row(db, "SELECT * FROM jobs WHERE id=?", jid)
    if not job:
        raise Problem(404, "Задание не найдено")
    return job


def get_worker(db, wid):
    worker = row(db, "SELECT * FROM workers WHERE id=?", wid)
    if not worker:
        raise Problem(404, "Узел не зарегистрирован")
    worker["metrics"] = json.loads(worker["metrics"])
    return worker


def capacity_chunk(maximum, threads, lease_seconds, samples):
    """Estimate work for one pull from recent measurements on that same worker."""
    def counter(metrics, key):
        value = metrics.get(key)
        return value if type(value) in (int,float) and 0 <= value <= 2**63 else None

    target = maximum
    speeds = [counter(m, "processed_per_second") for _, m in samples]
    active_speeds = [speed for speed in speeds if speed]
    if active_speeds:
        # Idle intervals between pulls are not measurements of processing capacity.
        speed = sum(active_speeds) / len(active_speeds)
        target = min(target, max(threads, speed * lease_seconds / 2))

    io_intervals = {"network": [], "disk": []}
    failures = processed = 0
    for (start, prev), (end, curr) in zip(samples, samples[1:]):
        elapsed = end - start
        if elapsed <= 0:
            continue
        def change(key):
            before, after = counter(prev,key), counter(curr,key)
            if before is None or after is None or after < before:
                return None
            return after-before
        errors, finished = change("errors"), change("processed")
        if errors is not None and finished is not None:
            failures += errors
            processed += finished
        for kind, keys in (("network", ("network_rx_bytes","network_tx_bytes")),
                           ("disk", ("disk_read_bytes","disk_write_bytes"))):
            deltas = [change(k) for k in keys]
            if all(n is not None for n in deltas):
                io_intervals[kind].append(sum(deltas)/elapsed)

    # When throughput drops while the device or network is busier than usual,
    # reduce this pull, without assuming a fixed bandwidth for heterogeneous hosts.
    if len(active_speeds) >= 2 and speeds[-1] is not None:
        typical = sum(active_speeds[:-1]) / len(active_speeds[:-1])
        if speeds[-1] < 0.75 * typical:
            for rates in io_intervals.values():
                if len(rates)>=2 and rates[-1] > 2 * max(1, sum(rates[:-1])/len(rates[:-1])):
                    target *= 0.75
    if failures:
        target /= 1 + 3 * min(1, failures/max(1,processed))
    return max(1, min(maximum, math.ceil(target)))


def job_config(data, defaults=None):
    defaults = defaults or {}
    limits = {"threads": (4, 1, 512), "rate_limit": (20, 1, 10000),
              "chunk_size": (1000, 1, 10000), "max_retries": (4, 0, 20),
              "request_timeout_seconds": (20, 1, 3600)}
    config = {key: integer(data.get(key, defaults.get(key, default)), key, low, high)
              for key, (default, low, high) in limits.items()}
    for key in ("check_rps", "download_rps"):
        value = data.get(key, defaults.get(key))
        config[key] = None if value is None else integer(value, key, 1, 10000)
    config["min_free_space_gb"] = real_number(data.get("min_free_space_gb", defaults.get("min_free_space_gb", 0)),
                                               "min_free_space_gb", 0, 10**9)
    for field in ("max_items", "max_seconds", "stop_on_consecutive_missing"):
        val = data.get(field, defaults.get(field))
        config[field] = None if val is None else integer(val, field, 1)
    return config


class WriterQueue:
    """Admit the oldest waiting request when the SQLite writer becomes free."""

    def __init__(self):
        self.lock = threading.Lock()
        self.waiters = deque()

    @contextmanager
    def turn(self, timeout):
        ready = threading.Event()
        with self.lock:
            if not self.waiters:
                ready.set()
            self.waiters.append(ready)
        try:
            if not ready.wait(timeout):
                raise sqlite3.OperationalError("Coordinator write queue timeout")
            yield
        finally:
            with self.lock:
                if self.waiters[0] is ready:
                    self.waiters.popleft()
                    if self.waiters:
                        self.waiters[0].set()
                else:
                    self.waiters.remove(ready)


class Coordinator:
    def __init__(self, path, lease_seconds=30, heartbeat_seconds=10):
        self.path = path
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.writer_queue = WriterQueue()
        self.writer = None
        self.closed = False
        initialize(path)

    @contextmanager
    def transaction(self, write=True):
        if not write:
            # WAL readers can take independent snapshots while a writer is active.
            with closing(connect(self.path)) as reader, reader:
                reader.execute("BEGIN")
                yield reader
            return

        # The queue and external-lock timeouts are both below the HTTP client deadline.
        # Only the owner may enter, use or replace the shared connection.
        with self.writer_queue.turn(timeout=4):
            if self.closed:
                raise sqlite3.OperationalError("Coordinator is closing")
            if self.writer is None:
                self.writer = connect(self.path, timeout=2, check_same_thread=False)
            try:
                with self.writer as db:
                    db.execute("BEGIN IMMEDIATE")
                    yield db
            except sqlite3.OperationalError:
                self.writer.close()
                self.writer = None
                raise

    def close(self):
        with self.writer_queue.turn(timeout=None):
            self.closed = True
            if self.writer is not None:
                self.writer.close()
                self.writer = None

    def reap(self, db):
        now = time.time()
        for chunk in rows(db, "SELECT * FROM range_chunks WHERE status='leased' AND lease_expires_at<=?", now):
            db.execute("UPDATE range_chunks SET status='queued',leased_by=NULL,lease_expires_at=NULL,updated_at=? WHERE id=?",
                       (now,chunk["id"]))
            event(db,"lease","expired","Истекла аренда блока",chunk["job_id"],chunk["leased_by"],chunk["id"])
        for job in rows(db, "SELECT id FROM jobs WHERE status='pausing' AND NOT EXISTS (SELECT 1 FROM range_chunks WHERE job_id=jobs.id AND status='leased')"):
            db.execute("UPDATE jobs SET status='paused',updated_at=? WHERE id=?",(now,job["id"]))
            event(db,"pause","paused",job=job["id"])
        for job in rows(db, "SELECT id FROM jobs WHERE status='running' AND max_seconds IS NOT NULL AND started_at+max_seconds<=?", now):
            self.finish(db,job["id"],"completed","time_limit")
        # Give freshly started nodes time to register. Finite jobs wait for their fleet.
        cutoff = now - self.lease_seconds - 2 * self.heartbeat_seconds
        offline_jobs = rows(db, """SELECT id FROM jobs j WHERE status='running' AND range_end IS NULL
            AND started_at<=? AND NOT EXISTS (SELECT 1 FROM workers w
            WHERE w.enabled=1 AND w.last_heartbeat>=? AND (j.restricted_workers=0 OR EXISTS (
                SELECT 1 FROM job_workers jw WHERE jw.job_id=j.id AND jw.worker_id=w.id)))""",cutoff,cutoff)
        for job in offline_jobs:
            self.finish(db,job["id"],"stopped","no_online_workers")

    def finish(self, db, jid, status, reason):
        db.execute("UPDATE jobs SET status=?,updated_at=? WHERE id=?", (status,time.time(),jid))
        db.execute("""UPDATE range_chunks SET status='cancelled',leased_by=NULL,lease_expires_at=NULL,updated_at=?
                      WHERE job_id=? AND status IN ('queued','leased')""",(time.time(),jid))
        event(db,reason,status,job=jid)

    def create_job(self, db, data, actor):
        check_fields(data, JOB_FIELDS)
        start,end = parse_range(data.get("range",""))
        config = job_config(data)
        name = bounded_string(data.get("name","Задание"), "name",120)
        chosen = data.get("worker_ids",[])
        if not isinstance(chosen,list) or len(chosen)>1000:
            raise Problem(400,"Некорректный список узлов")
        chosen = {identifier(w) for w in chosen}
        for wid in chosen:
            get_worker(db,wid)
        record = dict(name=name,status="created",range_start=start,range_end=end,next_start=start,
                      restricted_workers=int(bool(chosen)),created_at=time.time(),updated_at=time.time(),**config)
        cur = db.execute(f"INSERT INTO jobs({','.join(record)}) VALUES({','.join('?' for _ in record)})",
                         list(record.values()))
        jid = cur.lastrowid
        db.executemany("INSERT INTO job_workers(job_id,worker_id) VALUES(?,?)", [(jid,wid) for wid in chosen])
        per_worker = data.get("worker_settings", {})
        if not isinstance(per_worker,dict) or per_worker.keys() - chosen:
            raise Problem(400,"worker_settings должен содержать только узлы из worker_ids")
        for wid, values in per_worker.items():
            settings = worker_settings(values)
            if settings:
                fields = ", ".join(key+"=?" for key in settings)
                db.execute(f"UPDATE job_workers SET {fields} WHERE job_id=? AND worker_id=?",
                           (*settings.values(), jid, wid))
        audit(db,actor,"create",job=jid)
        event(db,"create","created",job=jid)
        return self.job_summary(db,jid)

    def job_summary(self,db,jid):
        job=get_job(db,jid)
        stats=row(db,"""SELECT count(*) AS processed, count(CASE WHEN m.item_id IS NOT NULL THEN 1 END) AS found,
                     count(CASE WHEN i.status='completed' THEN 1 END) AS downloaded,
                     count(CASE WHEN i.status='failed' THEN 1 END) AS errors,
                     coalesce(sum(f.size),0) AS bytes_downloaded FROM items i
                     LEFT JOIN media_metadata m ON m.item_id=i.id LEFT JOIN downloads d ON d.item_id=i.id
                     LEFT JOIN files f ON f.download_id=d.id WHERE i.job_id=?""",jid)
        job.update(stats)
        observed = job["updated_at"] if job["status"] in ("paused","completed","stopped","failed") else time.time()
        elapsed = observed - job["started_at"] if job["started_at"] is not None else 0
        job["speed"] = job["processed"] / elapsed if elapsed > 0 else 0
        job["download_speed"] = job["bytes_downloaded"] / elapsed if elapsed > 0 else 0
        total = job["max_items"]
        if job["range_end"] is not None:
            count = job["range_end"] - job["range_start"] + 1
            total = min(count, total) if total is not None else count
        job["total_items"] = total
        job["progress_percent"] = min(100, 100 * job["processed"] / total) if total else None
        job["eta_seconds"] = None
        if job["range_end"] is not None:
            if job["status"] == "completed":
                job["eta_seconds"] = 0
            elif job["status"] == "running" and job["speed"] > 0:
                job["eta_seconds"] = max(0, total - job["processed"]) / job["speed"]
        job["worker_ids"]=[r[0] for r in db.execute("SELECT worker_id FROM job_workers WHERE job_id=?",(jid,))]
        job["worker_settings"]={}
        for member in rows(db, "SELECT * FROM job_workers WHERE job_id=?",jid):
            overrides={key:member[key] for key in WORKER_SETTINGS if member[key] is not None}
            if overrides:
                job["worker_settings"][member["worker_id"]]=overrides
        queue=row(db,"SELECT count(*) AS queued,min(updated_at) AS oldest FROM range_chunks WHERE job_id=? AND status='queued'",jid)
        job["queued_chunks"]=queue["queued"]
        job["queue_size"]=queue["queued"]
        job["oldest_queued_seconds"]=max(0,time.time()-queue["oldest"]) if queue["oldest"] else 0
        last=db.execute("SELECT max(ts) FROM items WHERE job_id=?",(jid,)).fetchone()[0]
        job["last_progress_at"]=last or job["started_at"]
        return job

    def control(self,db,jid,op,data,actor):
        self.reap(db)
        job=get_job(db,jid)
        if op=="edit":
            check_fields(data,{"threads","rate_limit","check_rps","download_rps","min_free_space_gb","worker_ids"})
            config=job_config(data,job)
            db.execute("""UPDATE jobs SET threads=?,rate_limit=?,check_rps=?,download_rps=?,
                       min_free_space_gb=?,updated_at=? WHERE id=?""",
                       (config["threads"],config["rate_limit"],config["check_rps"],config["download_rps"],
                        config["min_free_space_gb"],time.time(),jid))
            if "worker_ids" in data:
                chosen=data["worker_ids"]
                if not isinstance(chosen,list):
                    raise Problem(400,"Некорректный список узлов")
                members={identifier(w) for w in chosen}
                for existing in rows(db,"SELECT worker_id FROM job_workers WHERE job_id=?",jid):
                    if existing["worker_id"] not in members:
                        db.execute("DELETE FROM job_workers WHERE job_id=? AND worker_id=?",(jid,existing["worker_id"]))
                for wid in members:
                    get_worker(db,wid)
                    db.execute("INSERT OR IGNORE INTO job_workers(job_id,worker_id) VALUES(?,?)",(jid,wid))
                db.execute("UPDATE jobs SET restricted_workers=? WHERE id=?",(bool(chosen),jid))
        elif op=="retry":
            if job["status"] not in ("completed","stopped","paused","failed"):
                raise Problem(409,"Повтор ошибок доступен для завершённого или приостановленного задания")
            failed=rows(db,"SELECT chunk_id,min(external_id) AS first_id FROM items WHERE job_id=? AND status='failed' GROUP BY chunk_id",jid)
            if not failed:
                raise Problem(409,"Нет ошибочных элементов")
            db.execute("DELETE FROM items WHERE job_id=? AND status='failed'",(jid,))
            for group in failed:
                db.execute("UPDATE range_chunks SET status='queued',next_id=min(next_id,?),leased_by=NULL WHERE id=?",
                           (group["first_id"],group["chunk_id"]))
            db.execute("UPDATE range_chunks SET status='queued' WHERE job_id=? AND status='cancelled'",(jid,))
            db.execute("UPDATE jobs SET status='running',started_at=?,updated_at=? WHERE id=?",(time.time(),time.time(),jid))
        else:
            allowed={"start":({"created"},"running"),"resume":({"paused"},"running"),
                     "pause":({"running"},"pausing"),"stop":({"running","paused","pausing","created"},"stopped")}
            if op not in allowed:
                raise Problem(404,"Операция не найдена")
            states,status=allowed[op]
            if job["status"] not in states:
                raise Problem(409,"Недопустимый переход из состояния "+job["status"])
            if op == "start":
                db.execute("UPDATE jobs SET status='validating',updated_at=? WHERE id=?", (time.time(), jid))
                event(db, "start", "validating", job=jid)
                # A saved job can outlive a coordinator release or an inventory change.
                try:
                    parse_range({"from":job["range_start"], "to":job["range_end"]})
                    job_config({}, job)
                    assigned = rows(db, "SELECT * FROM job_workers WHERE job_id=?", jid)
                    if job["restricted_workers"] and not assigned:
                        raise Problem(400, "У задания отсутствуют назначенные узлы")
                    for member in assigned:
                        get_worker(db, member["worker_id"])
                        worker_settings({k:member[k] for k in WORKER_SETTINGS if member[k] is not None})
                except Problem:
                    self.finish(db, jid, "failed", "invalid_saved_configuration")
                    audit(db, actor, op, job=jid)
                    return self.job_summary(db, jid)
            if op=="stop":
                db.execute("UPDATE jobs SET status='stopping',updated_at=? WHERE id=?", (time.time(), jid))
                event(db, "stop", "stopping", job=jid)
                self.finish(db,jid,status,op)
            else:
                db.execute("UPDATE jobs SET status=?,started_at=coalesce(started_at,?),updated_at=? WHERE id=?",
                           (status,time.time(),time.time(),jid))
                event(db,op,status,job=jid)
                self.reap(db)
        audit(db,actor,op,job=jid)
        return self.job_summary(db,jid)

    def register(self,db,data,actor):
        wid=identifier(data.get("id"))
        if wid!=actor:
            raise Problem(403,"Удостоверение другого узла")
        name=bounded_string(data.get("name",wid),"name")
        version=bounded_string(data.get("version","1"),"version",64)
        path=bounded_string(data.get("storage_path"),"storage_path",1024)
        if not (posixpath.isabs(path) or ntpath.splitdrive(path)[0] and ntpath.isabs(path)):
            raise Problem(400,"Требуется абсолютный путь хранилища")
        old=row(db,"SELECT storage_path FROM workers WHERE id=?",wid)
        inventory=json.loads(os.environ.get("LAB_STORAGE_ROOTS","{}"))
        configured=inventory.get(wid) if isinstance(inventory,dict) else None
        if old and old["storage_path"]!=path or configured and configured!=path:
            raise Problem(409,"Путь хранилища отличается от зарегистрированного")
        threads=integer(data.get("threads",4),"threads",1,512)
        db.execute("""INSERT INTO workers(id,name,version,storage_path,threads,last_heartbeat,registered_at)
                 VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,version=excluded.version,
                 storage_path=excluded.storage_path,threads=excluded.threads,last_heartbeat=excluded.last_heartbeat""",
                   (wid,name,version,path,threads,time.time(),time.time()))
        # A fresh process cannot resume the old process's transition sequence. Fence it
        # and issue a new generation; confirmed results are excluded from the new lease.
        db.execute("""UPDATE range_chunks SET status='queued',leased_by=NULL,lease_expires_at=NULL,
                   updated_at=? WHERE status='leased' AND leased_by=?""", (time.time(), wid))
        event(db,"register",worker=wid)
        audit(db,"agent:"+wid,"register",worker=wid)
        return {"id":wid}

    def worker_summary(self,db,wid):
        w=get_worker(db,wid)
        w["online"]=time.time()-w["last_heartbeat"]<=2*self.heartbeat_seconds
        w["enabled"]=bool(w["enabled"])
        w["heartbeat_interval_seconds"]=self.heartbeat_seconds
        w["expected_agent_version"]="1.0"
        w["host"]=w["address"]
        w["ssh_configured"]=False
        filename=os.getenv("LAB_SSH_INVENTORY")
        if filename:
            try:
                inventory=json.loads(Path(filename).read_text(encoding="utf-8"))
                w["ssh_configured"]=isinstance(inventory,dict) and isinstance(inventory.get(wid),dict)
            except (OSError,ValueError):
                pass
        w["current_chunk"]=row(db,"SELECT id,job_id,range_start,range_end,next_id FROM range_chunks WHERE leased_by=? AND status='leased'",wid)
        return w

    def edit_worker(self, db, wid, data, actor):
        worker = get_worker(db, wid)
        check_fields(data, WORKER_SETTINGS | {"enabled"})
        enabled = data.get("enabled", bool(worker["enabled"]))
        if type(enabled) is not bool:
            raise Problem(400,"enabled должен быть логическим значением")
        settings = worker_settings({k:v for k,v in data.items() if k != "enabled"})
        if "threads" in settings:
            settings["thread_override"] = settings.pop("threads")
        settings["enabled"] = enabled
        fields = ", ".join(key+"=?" for key in settings)
        db.execute(f"UPDATE workers SET {fields} WHERE id=?", (*settings.values(), wid))
        if not enabled:
            db.execute("""UPDATE range_chunks SET status='queued',leased_by=NULL,lease_expires_at=NULL
                          WHERE status='leased' AND leased_by=?""",(wid,))
        audit(db,actor,"edit_worker",worker=wid)
        return self.worker_summary(db,wid)

    def lease(self,db,wid):
        self.reap(db)
        w=get_worker(db,wid)
        if not w["enabled"]:
            return {"chunk":None}
        # Re-send an existing lease if the original response was lost.
        previous=row(db,"SELECT * FROM range_chunks WHERE leased_by=? AND status='leased'",wid)
        if previous:
            return self.lease_payload(db,previous,get_job(db,previous["job_id"]),w)
        for job in rows(db,"""SELECT * FROM jobs WHERE status='running' AND (restricted_workers=0 OR
                    EXISTS(SELECT 1 FROM job_workers WHERE job_id=jobs.id AND worker_id=?))
                    ORDER BY last_dispatched_at,id""",wid):
            local=self.assignment_settings(db, job, w)
            free=w["metrics"].get("free_space_gb",0)
            if type(free) not in (float,int) or free<local["min_free_space_gb"] or self.overloaded(w):
                continue
            c=row(db,"SELECT * FROM range_chunks WHERE job_id=? AND status='queued' ORDER BY id LIMIT 1",job["id"])
            if c is None:
                end=self.end_bound(job)
                if job["next_start"]>end:
                    self.finish_if_done(db,job)
                    continue
                lo=job["next_start"]
                hi=min(lo+self.scheduled_size(db, job, w, local["threads"])-1,end)
                cur=db.execute("""INSERT INTO range_chunks(job_id,range_start,range_end,next_id,status,created_at,updated_at)
                                VALUES(?,?,?,?,'queued',?,?)""",(job["id"],lo,hi,lo,time.time(),time.time()))
                db.execute("UPDATE jobs SET next_start=? WHERE id=?",(hi+1,job["id"]))
                cid=cur.lastrowid
            else:
                cid=c["id"]
            now=time.time()
            db.execute("UPDATE jobs SET last_dispatched_at=? WHERE id=?",(now,job["id"]))
            db.execute("""UPDATE range_chunks SET status='leased',leased_by=?,leased_at=?,lease_expires_at=?,
                       attempt=attempt+1,updated_at=? WHERE id=?""",(wid,now,now+self.lease_seconds,now,cid))
            c=row(db,"SELECT * FROM range_chunks WHERE id=?",cid)
            event(db,"lease","leased",job=job["id"],worker=wid,chunk=cid)
            return self.lease_payload(db,c,job,w)
        return {"chunk":None}

    @staticmethod
    def assignment_settings(db, job, worker):
        overrides = row(db, "SELECT * FROM job_workers WHERE job_id=? AND worker_id=?", job["id"], worker["id"]) or {}
        threads = overrides.get("threads") or worker["thread_override"] or min(job["threads"], worker["threads"])
        reserve = overrides.get("min_free_space_gb")
        if reserve is None:
            reserve = job["min_free_space_gb"]
        return {"threads":threads, "min_free_space_gb":max(reserve,worker["min_free_space_gb"] or 0)}

    def scheduled_size(self, db, job, worker, threads):
        # Target half a lease of observed work. Preserve existing chunk boundaries on takeover.
        fresh_after = time.time() - max(60, self.heartbeat_seconds * 12)
        snapshots = rows(db, """SELECT ts,data FROM worker_metrics WHERE worker_id=? AND ts>=?
                             ORDER BY id DESC LIMIT 8""", worker["id"], fresh_after)
        samples = [(record["ts"], json.loads(record["data"])) for record in reversed(snapshots)]
        return capacity_chunk(job["chunk_size"], threads, self.lease_seconds, samples)

    @staticmethod
    def overloaded(w):
        metrics=w["metrics"]
        def metric(key):
            val=metrics.get(key)
            return val if type(val) in (int,float) else 0
        return (bool(metrics.get("overloaded")) or bool(metrics.get("disk_blocked")) or metric("cpu_percent")>=98 or
                metric("ram_total_bytes")>0 and metric("ram_used_bytes")/metric("ram_total_bytes")>=0.98 or
                metric("active_checks")+metric("active_downloads")>= (w["thread_override"] or w["threads"]))

    @staticmethod
    def end_bound(job):
        end=job["range_end"] if job["range_end"] is not None else MAX_ID
        if job["max_items"] is not None:
            end=min(end,job["range_start"]+job["max_items"]-1)
        return end

    def lease_payload(self,db,c,job,w):
        c=dict(c)
        c.update({k:job[k] for k in ("rate_limit","max_retries","request_timeout_seconds","min_free_space_gb")})
        c.update(self.assignment_settings(db,job,w))
        c["skip_ids"]=[r[0] for r in db.execute("SELECT external_id FROM items WHERE chunk_id=? AND external_id>=?",
                                                (c["id"],c["next_id"]))]
        return {"chunk":c}

    def current(self,db,wid,data):
        cid=integer(data.get("chunk_id"),"chunk_id",1)
        attempt=integer(data.get("attempt"),"attempt",1)
        c=row(db,"SELECT * FROM range_chunks WHERE id=?",cid)
        if (not c or c["status"]!="leased" or c["leased_by"]!=wid or c["attempt"]!=attempt
                or c["lease_expires_at"]<=time.time()):
            raise Problem(409,"Аренда больше не действует")
        return c

    def heartbeat(self,db,wid,data):
        self.reap(db)
        w=get_worker(db,wid)
        metrics=data.get("metrics",{})
        if not isinstance(metrics,dict) or len(json.dumps(metrics))>100000:
            raise Problem(400,"Некорректные метрики")
        now=time.time()
        serialized=json.dumps(metrics,allow_nan=False)
        db.execute("UPDATE workers SET last_heartbeat=?,metrics=? WHERE id=?",(now,serialized,wid))
        db.execute("INSERT INTO worker_metrics(worker_id,ts,data) VALUES(?,?,?)",(wid,now,serialized))
        action="idle"
        threads=w["thread_override"] or w["threads"]
        if data.get("chunk_id") is not None:
            try:
                c=self.current(db,wid,data)
            except Problem as exc:
                if exc.code!=409:
                    raise
                action="stop"
            else:
                job=get_job(db,c["job_id"])
                if w["enabled"] and job["status"] in ("running","pausing"):
                    db.execute("UPDATE range_chunks SET lease_expires_at=?,updated_at=? WHERE id=?",
                               (now+self.lease_seconds,now,c["id"]))
                    action="continue"
                    threads=self.assignment_settings(db,job,w)["threads"]
                else:
                    action="stop"
        return {"action":action,"threads":threads,"lease_seconds":self.lease_seconds}

    def permit(self,db,wid,data,c):
        kind=data.get("kind")
        if kind not in ("check","download"):
            raise Problem(400,"Неизвестный вид операции")
        job=get_job(db,c["job_id"])
        key=f"job/{job['id']}"
        now=time.time()
        kind_field = kind + "_rps"
        node = row(db, f"SELECT worker_rps,{kind_field} FROM workers WHERE id=?", wid)
        local = row(db, f"SELECT worker_rps,{kind_field} FROM job_workers WHERE job_id=? AND worker_id=?",
                    job["id"], wid) or {}
        # A permit consumes one token in every configured scope, across all applicable jobs and nodes.
        limits = {key:job["rate_limit"], key+"/kind/"+kind:job[kind_field],
                  "worker/"+wid:node["worker_rps"], "worker/"+wid+"/kind/"+kind:node[kind_field],
                  key+"/worker/"+wid:local.get("worker_rps"),
                  key+"/worker/"+wid+"/kind/"+kind:local.get(kind_field)}
        limits = {scope:rate for scope,rate in limits.items() if rate is not None}
        budgets=[]
        for k in limits:
            b=row(db,"SELECT * FROM rate_buckets WHERE key=?",k)
            if b is None:
                b=dict(tokens=1.,updated=now,blocked_until=0.,scale=1.)
                db.execute("INSERT INTO rate_buckets VALUES(?,?,?,?,?)",(k,1.,now,0.,1.))
            budgets.append(b)
        result=data.get("result_status")
        if result is not None:
            integer(result,"result_status",100,599)
            if result==429 or result>=500:
                value = data.get("retry_after")
                wait=real_number(1 if value is None else value,"retry_after",0,86400)
                db.execute("UPDATE rate_buckets SET blocked_until=max(blocked_until,?),scale=max(0.05,scale/2),tokens=0,updated=? WHERE key=?",
                           (now+wait,now,key))
                event(db,"rate_limit","limited","Снижение частоты запросов",job["id"],wid,c["id"],code=str(result))
                return {"granted":False,"wait":max(wait,budgets[0]["blocked_until"]-now)}
            return {"granted":False,"wait":max(0,budgets[0]["blocked_until"]-now)}
        wait=0.
        for rate,b in zip(limits.values(),budgets):
            scale=min(1.,b["scale"]+max(0,now-b["updated"])/60)
            b["effective"]=rate*scale
            b["scale"]=scale
            # Short burst permits concurrent slow HTTP calls without accumulating idle credit.
            b["available"]=min(max(1.,b["effective"]*0.1),b["tokens"]+max(0,now-b["updated"])*b["effective"])
            wait=max(wait,b["blocked_until"]-now,(1-b["available"])/b["effective"])
        if wait>0:
            return {"granted":False,"wait":round(wait+0.001,4)}
        for k,b in zip(limits,budgets):
            db.execute("UPDATE rate_buckets SET tokens=?,updated=?,scale=? WHERE key=?",(b["available"]-1,now,b["scale"],k))
        return {"granted":True,"wait":0}

    @staticmethod
    def finish_if_done(db,job):
        if job["next_start"]<=Coordinator.end_bound(job):
            return
        if not db.execute("SELECT 1 FROM range_chunks WHERE job_id=? AND status IN ('queued','leased') LIMIT 1",(job["id"],)).fetchone():
            db.execute("UPDATE jobs SET status='completed',updated_at=? WHERE id=?",(time.time(),job["id"]))
            event(db,"finish","completed",job=job["id"])

    def progress(self,db,wid,c,data):
        eid=integer(data.get("external_id"),"external_id",c["range_start"],c["range_end"])
        status=data.get("status")
        if status not in FINAL_STATES:
            raise Problem(400,"Нужен итоговый статус ID")
        # The fencing token is checked before idempotency; expired owners cannot alter results.
        if row(db,"SELECT id FROM items WHERE job_id=? AND external_id=?",c["job_id"],eid):
            return {"ok":True,"duplicate":True}
        meta=data.get("metadata") or {}
        info=data.get("file") or {}
        if not isinstance(info,dict) or not isinstance(meta,dict):
            raise Problem(400,"Некорректные метаданные")
        file_path=None
        if info or status=="completed":
            file_path=bounded_string(info.get("path") or info.get("storage_path") or info.get("local_path"),"path",2048)
            root=get_worker(db,wid)["storage_path"]
            mod=ntpath if ntpath.splitdrive(root)[0] else posixpath
            try:
                safe=mod.isabs(file_path) and mod.normcase(mod.commonpath([mod.normpath(root),mod.normpath(file_path)]))==mod.normcase(mod.normpath(root))
            except ValueError:
                safe=False
            if not safe or mod.normpath(file_path)==mod.normpath(root):
                raise Problem(400,"Путь файла вне хранилища")
        # Only persist diagnostic codes, never arbitrary request bodies or credentials in logs.
        err=data.get("error")
        if err:
            err=err if isinstance(err,str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}",err) else "processing_error"
        now=time.time()
        iid=db.execute("INSERT INTO items(job_id,external_id,chunk_id,worker_id,status,error,ts) VALUES(?,?,?,?,?,?,?)",
                       (c["job_id"],eid,c["id"],wid,status,err,now)).lastrowid
        if meta:
            db.execute("INSERT INTO media_metadata VALUES(?,?)",(iid,json.dumps(meta,allow_nan=False)))
        if info or status=="failed":
            did=db.execute("INSERT INTO downloads(item_id,worker_id,attempt,status,ts) VALUES(?,?,?,?,?)",
                           (iid,wid,c["attempt"],status,now)).lastrowid
            if info:
                size=integer(info.get("size",info.get("size_bytes",0)),"size")
                db.execute("INSERT INTO files(download_id,storage_path,data,size) VALUES(?,?,?,?)",
                           (did,file_path,json.dumps(info,allow_nan=False),size))
        if eid==c["next_id"]:
            nxt=eid+1
            for (present,) in db.execute("SELECT external_id FROM items WHERE chunk_id=? AND external_id>=? ORDER BY external_id",(c["id"],nxt)):
                if present!=nxt:
                    break
                nxt+=1
            db.execute("UPDATE range_chunks SET next_id=?,updated_at=? WHERE id=?",(nxt,now,c["id"]))
        if status in ("failed","forbidden"):
            event(db,"check",status,"Обработка ID завершена",c["job_id"],wid,c["id"],eid,err or status)
        return {"ok":True}

    def worker_op(self,db,wid,op,data):
        if op=="heartbeat":
            return self.heartbeat(db,wid,data)
        if op=="lease":
            return self.lease(db,wid)
        c=self.current(db,wid,data)
        if op in ("permit","progress","lifecycle","fail") and "transitions" in data:
            try:
                lifecycle.record(db,c,wid,data)
            except ValueError as exc:
                raise Problem(400,str(exc)) from None
        if op=="lifecycle":
            return {"ok":True}
        if op=="permit":
            reply=self.permit(db,wid,data,c)
            if "download_seq" in data and reply.get("granted") and data.get("kind")=="download":
                transition={"seq":data["download_seq"],"status":"downloading"}
                try:
                    lifecycle.record(db,c,wid,{"external_id":data.get("external_id"),"transitions":[transition]})
                except ValueError as exc:
                    raise Problem(400,str(exc)) from None
            return reply
        if op=="progress":
            return self.progress(db,wid,c,data)
        if op not in ("complete","fail"):
            raise Problem(404,"Неизвестная операция узла")
        if op=="complete" and c["next_id"]<=c["range_end"]:
            raise Problem(409,"В блоке остались необработанные ID")
        new_status="done" if op=="complete" else "queued"
        if op=="fail" and data.get("error")=="insufficient_disk":
            w=get_worker(db,wid)
            w["metrics"]["free_space_gb"]=0
            w["metrics"]["disk_blocked"]=True
            db.execute("UPDATE workers SET metrics=? WHERE id=?",(json.dumps(w["metrics"]),wid))
        db.execute("UPDATE range_chunks SET status=?,leased_by=NULL,lease_expires_at=NULL,updated_at=? WHERE id=?",
                   (new_status,time.time(),c["id"]))
        event(db,op,new_status,job=c["job_id"],worker=wid,chunk=c["id"])
        job=get_job(db,c["job_id"])
        stop=job["stop_on_consecutive_missing"]
        if op=="complete" and stop:
            # Exclude the unconfirmed suffix; evaluate numeric adjacency, not arrival order.
            boundary=db.execute("SELECT coalesce(min(next_id),?) FROM range_chunks WHERE job_id=? AND status!='done'",
                                (job["next_start"],job["id"])).fetchone()[0]
            missing=db.execute("""SELECT 1 FROM (SELECT external_id-row_number() OVER(ORDER BY external_id) AS grp
                        FROM items WHERE job_id=? AND status='not_found' AND external_id<?)
                        GROUP BY grp HAVING count(*)>=? LIMIT 1""",(job["id"],boundary,stop)).fetchone()
            if missing:
                self.finish(db,job["id"],"completed","missing_limit")
                return {"ok":True}
        self.finish_if_done(db,job)
        self.reap(db)
        return {"ok":True}


def auth_tokens(env):
    tokens={}
    def add(value,role,actor):
        if (not isinstance(value,str) or not value or not value.isascii()
                or not value.isprintable() or any(c.isspace() for c in value)):
            raise ValueError("Токен должен содержать печатные ASCII-символы без пробелов")
        if value in tokens:
            raise ValueError("Каждому пользователю и узлу требуется отдельный токен")
        tokens[value]=(role,actor)
    for role,key in (("administrator","ADMIN"),("viewer","VIEWER"),("operator","OPERATOR")):
        value=env.get(f"LAB_{key}_TOKEN")
        if value:
            add(value,role,role)
    if env.get("LAB_USERS_FILE"):
        profiles = json.loads(Path(env["LAB_USERS_FILE"]).read_text(encoding="utf-8-sig"))
        if not isinstance(profiles, dict):
            raise ValueError("Ожидается словарь пользователей")
        for login, profile in profiles.items():
            if identifier(login) in ROLES or not isinstance(profile, dict):
                raise ValueError("Некорректная учётная запись")
            role = profile.get("role")
            if not isinstance(role, str) or role not in ROLES:
                raise ValueError("Неизвестная роль")
            add(profile.get("token"), role, login)
    workers=json.loads(env.get("LAB_WORKER_TOKENS","{}"))
    if not isinstance(workers,dict):
        raise ValueError("LAB_WORKER_TOKENS должен быть объектом JSON")
    for wid,value in workers.items():
        add(value,"agent",identifier(wid))
    if not any(role == "administrator" for role, _ in tokens.values()):
        raise ValueError("Требуется учётная запись администратора")
    return tokens


def page_param(query,key,default,maximum):
    if key not in query:
        return default
    value=query[key][0]
    if not re.fullmatch("[0-9]{1,19}",value):
        raise Problem(400,"Некорректный числовой фильтр")
    return integer(int(value),key,0,maximum)


def select_log(db,query,jid=None,table="events"):
    cols=("job_id","worker_id","external_id","result","operation") if table=="events" else ("job_id","worker_id","result","operation","actor")
    cond=[]
    args=[]
    if jid is not None:
        cond.append("job_id=?")
        args.append(jid)
    for k in cols:
        if k in query:
            cond.append(k+"=?")
            args.append(query[k][0])
    for k,op in (("from",">="),("to","<=")):
        if k in query:
            try:
                val=float(query[k][0])
            except ValueError:
                raise Problem(400,"Некорректное время") from None
            cond.append("ts"+op+"?")
            args.append(val)
    where=" WHERE "+" AND ".join(cond) if cond else ""
    return rows(db,f"SELECT * FROM {table}{where} ORDER BY id DESC LIMIT ? OFFSET ?",*args,
                page_param(query,"limit",100,5000),page_param(query,"offset",0,MAX_ID))


def select_items(db,jid,query,export=False):
    filters=["i.job_id=?"]
    params=[jid]
    for key in ("status","external_id","worker_id"):
        if key in query:
            filters.append(f"i.{key}=?")
            params.append(query[key][0])
    sql="""SELECT i.*,m.data AS metadata,f.data AS file FROM items i
           LEFT JOIN media_metadata m ON m.item_id=i.id LEFT JOIN downloads d ON d.item_id=i.id
           LEFT JOIN files f ON f.download_id=d.id WHERE """+" AND ".join(filters)+" ORDER BY i.external_id"
    if not export:
        sql+=" LIMIT ? OFFSET ?"
        params.extend([page_param(query,"limit",100,5000),page_param(query,"offset",0,MAX_ID)])
    for record in db.execute(sql,params):
        rec=dict(record)
        rec["metadata"]=json.loads(rec["metadata"] or "{}")
        rec["file"]=json.loads(rec["file"] or "{}")
        yield rec


class Handler(BaseHTTPRequestHandler):
    server_version="LabCoordinator/1"

    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def handle(self):
        # The peer can disappear before a request line arrives (e.g. worker termination).
        try:
            super().handle()
        except (ConnectionError, TimeoutError):
            self.close_connection = True

    def log_message(self,*_):
        # Avoid logging URLs, query strings, credential headers or HTTP bodies.
        pass

    def send_content(self,status,body,content_type):
        try:
            self.write_content(status, body, content_type)
        except (ConnectionError, TimeoutError):
            # The request's transaction has already committed or rolled back.
            # A disconnected client can retry using the same lease and item ID.
            self.close_connection = True

    def write_content(self,status,body,content_type):
        self.send_response(status)
        self.send_header("Content-Type",content_type)
        self.send_header("Content-Length",str(len(body)))
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("Referrer-Policy","no-referrer")
        self.send_header("Content-Security-Policy","default-src 'self'; frame-ancestors 'none'; object-src 'none'; style-src 'self' 'unsafe-inline'")
        self.end_headers()
        self.wfile.write(body)

    def response(self,status,value):
        self.send_content(status,json.dumps(value,ensure_ascii=False,allow_nan=False).encode(),"application/json; charset=utf-8")

    def authenticate(self):
        header=self.headers.get("Authorization","")
        if header.startswith("Bearer ") and header.isascii():
            for token,identity in self.server.tokens.items():
                if hmac.compare_digest(header[7:],token):
                    return identity
        raise Problem(401,"Требуется действующий токен")

    def body(self):
        length=self.headers.get("Content-Length","0")
        if not re.fullmatch(r"[0-9]{1,9}",length) or int(length)>256*1024:
            raise Problem(413,"Слишком большой запрос")
        if not int(length):
            return {}
        if self.headers.get_content_type()!="application/json":
            raise Problem(415,"Требуется application/json")
        try:
            data=json.loads(self.rfile.read(int(length)).decode("utf-8"))
            json.dumps(data,allow_nan=False)
        except (UnicodeError,ValueError,RecursionError):
            raise Problem(400,"Некорректный JSON") from None
        if not isinstance(data,dict):
            raise Problem(400,"Ожидается объект JSON")
        return data

    def dispatch(self):
        url=urlsplit(self.path)
        if self.command=="GET" and (url.path=="/" or url.path in ("/ui","/ui/") or url.path.startswith("/ui/")):
            name="index.html" if url.path in ("/","/ui","/ui/") else url.path[4:]
            if name not in {"index.html","style.css","app.js","monitor.js"} or not (WEB_ROOT/name).is_file():
                raise Problem(404,"Файл не найден")
            self.send_content(200,(WEB_ROOT/name).read_bytes(),mimetypes.guess_type(name)[0]+"; charset=utf-8")
            return
        parts=url.path.strip("/").split("/")
        if not parts or parts.pop(0)!="api":
            raise Problem(404,"Маршрут не найден")
        role,actor=self.authenticate()
        query=parse_qs(url.query,max_num_fields=20)
        data={} if self.command=="GET" else self.body()
        svc=self.server.svc

        def require(required):
            if ROLES.get(role,0)<ROLES[required]:
                raise Problem(403,"Недостаточно прав")

        method=self.command
        if method=="POST" and len(parts)==3 and parts[0]=="workers" and parts[2]=="service":
            from ssh_control import run_service_action
            wid=identifier(parts[1])
            try:
                require("administrator")
                action=data.get("action")
                if action not in ("status","start","restart","update"):
                    raise Problem(400,"Недопустимая операция службы")
                run_service_action(wid,action)
            except (ValueError,RuntimeError,OSError,Problem) as exc:
                with svc.transaction() as db:
                    db.execute("INSERT INTO audit_log(ts,actor,operation,worker_id,result) VALUES(?,?,?,?,?)",
                               (time.time(),actor,"service",wid,"denied" if isinstance(exc,Problem) else "failed"))
                if isinstance(exc,Problem):
                    raise
                raise Problem(409,"Операция службы не выполнена") from None
            with svc.transaction() as db:
                audit(db,actor,"service."+action,worker=wid)
            self.response(200,{"ok":True})
            return

        with svc.transaction(method!="GET") as db:
            result=self.route(db,svc,method,parts,query,data,actor,role,require)
        if isinstance(result,bytes):
            self.send_content(200,result,"text/csv; charset=utf-8")
        else:
            self.response(200,result)

    def route(self,db,svc,method,p,q,data,actor,role,require):
        if method=="GET":
            require("viewer")
            if p==["me"]:
                return {"role":role}
            if p==["jobs"]:
                return [svc.job_summary(db,k[0]) for k in db.execute("SELECT id FROM jobs ORDER BY id DESC")]
            if p==["workers"]:
                return [svc.worker_summary(db,k[0]) for k in db.execute("SELECT id FROM workers ORDER BY id")]
            if p==["events"]:
                return select_log(db,q)
            if p==["audit"]:
                require("administrator")
                return select_log(db,q,table="audit_log")
            if len(p)>=2 and p[0]=="jobs":
                jid=self.job_id(p[1])
                get_job(db,jid)
                if len(p)==2:
                    return svc.job_summary(db,jid)
                if p[2:]==["events"]:
                    return select_log(db,q,jid)
                if p[2:]==["states"]:
                    return lifecycle.states(db,jid,q,page_param(q,"limit",100,5000),page_param(q,"offset",0,MAX_ID))
                if len(p)==5 and p[2]=="items" and p[4]=="history":
                    if not re.fullmatch(r"[0-9]{1,19}",p[3]):
                        raise Problem(400,"Некорректный ID материала")
                    eid=integer(int(p[3]),"external_id")
                    return lifecycle.history(db,jid,eid,page_param(q,"limit",100,5000),page_param(q,"offset",0,MAX_ID))
                if p[2:]==["items"]:
                    if q.get("format")==["csv"]:
                        output=io.StringIO(newline="")
                        writer=csv.writer(output)
                        cols=["external_id","status","worker_id","ts","error","metadata","file"]
                        writer.writerow(cols)
                        for rec in select_items(db,jid,q,True):
                            # JSON-encode non-numeric values that could be spreadsheet formulas.
                            vals=[]
                            for key in cols:
                                v=rec[key]
                                if isinstance(v,dict):
                                    v=json.dumps(v,ensure_ascii=False)
                                if isinstance(v,str) and v.startswith(("=","+","-","@")):
                                    v="'"+v
                                vals.append(v)
                            writer.writerow(vals)
                        return output.getvalue().encode("utf-8-sig")
                    return list(select_items(db,jid,q))
            if len(p)>=2 and p[0]=="workers":
                wid=identifier(p[1])
                worker=svc.worker_summary(db,wid)
                if len(p)==2:
                    return worker
                if p[2:]==["metrics"]:
                    return [dict(ts=r["ts"],**{"metrics":json.loads(r["data"])}) for r in
                            rows(db,"SELECT * FROM worker_metrics WHERE worker_id=? ORDER BY id DESC LIMIT ?",
                                 wid,page_param(q,"limit",60,5000))]
        elif p==["workers","register"] and method=="POST":
            if role!="agent":
                raise Problem(403,"Требуется удостоверение узла")
            result=svc.register(db,data,actor)
            db.execute("UPDATE workers SET address=? WHERE id=?",(self.client_address[0],actor))
            return result
        elif len(p)==3 and p[0]=="workers" and method=="POST":
            if role!="agent" or actor!=p[1]:
                raise Problem(403,"Требуется удостоверение этого узла")
            return svc.worker_op(db,actor,p[2],data)
        elif len(p)==2 and p[0]=="workers" and method=="PATCH":
            require("operator")
            return svc.edit_worker(db,identifier(p[1]),data,actor)
        elif p and p[0]=="jobs":
            require("operator")
            if len(p)==1 and method=="POST":
                return svc.create_job(db,data,actor)
            if len(p)==2 and method=="PATCH":
                return svc.control(db,self.job_id(p[1]),"edit",data,actor)
            if len(p)==3 and method=="POST":
                return svc.control(db,self.job_id(p[1]),p[2],data,actor)
        raise Problem(404,"Маршрут не найден")

    @staticmethod
    def job_id(value):
        if not re.fullmatch("[0-9]{1,18}",value):
            raise Problem(400,"Некорректный ID задания")
        return int(value)

    def handle_api(self):
        try:
            self.dispatch()
        except Problem as exc:
            self.response(exc.code,{"error":str(exc)})
        except sqlite3.OperationalError:
            self.response(503,{"error":"Хранилище временно недоступно"})
        except (ValueError,OverflowError,TypeError,RecursionError):
            self.response(400,{"error":"Некорректные параметры запроса"})
        except (ConnectionError,TimeoutError):
            return

    do_GET=do_POST=do_PATCH=handle_api


class Server(ThreadingHTTPServer):
    daemon_threads=True
    request_queue_size=256


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=8090)
    parser.add_argument("--db",default=".lab/lab.sqlite")
    parser.add_argument("--lease-seconds",type=float,default=30)
    parser.add_argument("--heartbeat-seconds",type=float,default=10)
    parser.add_argument("--cert")
    parser.add_argument("--key")
    args=parser.parse_args()
    if args.lease_seconds<=0 or args.heartbeat_seconds<=0:
        parser.error("Интервалы аренды и heartbeat должны быть положительными")
    if bool(args.cert)!=bool(args.key) or (not args.cert and args.host not in ("127.0.0.1","localhost")):
        parser.error("Для внешнего интерфейса задайте --cert и --key")
    try:
        tokens=auth_tokens(os.environ)
    except (OSError,Problem,ValueError):
        parser.error("Проверьте токены ролей и LAB_WORKER_TOKENS в окружении")
    with closing(Coordinator(args.db,args.lease_seconds,args.heartbeat_seconds)) as svc:
        serve(args, svc, tokens)


def serve(args, svc, tokens):
    with svc.transaction() as db:
        # Keep user IDs for historical audits. Grants reflect the active configuration.
        db.execute("DELETE FROM user_roles")
        for role,actor in tokens.values():
            if role in ROLES:
                db.execute("INSERT OR IGNORE INTO users VALUES(?)",(actor,))
                db.execute("INSERT OR IGNORE INTO user_roles VALUES(?,?)",(actor,role))
    with Server((args.host,args.port),Handler) as server:
        server.svc=svc
        server.tokens=tokens
        if args.cert:
            tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.minimum_version=ssl.TLSVersion.TLSv1_2
            tls.load_cert_chain(args.cert,args.key)
            server.socket=tls.wrap_socket(server.socket,server_side=True)
        running=threading.Event()
        def maintain():
            while not running.wait(min(1,svc.heartbeat_seconds)):
                try:
                    with svc.transaction() as db:
                        svc.reap(db)
                except sqlite3.OperationalError:
                    continue
        maintenance=threading.Thread(target=maintain,daemon=True)
        maintenance.start()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            running.set()
            maintenance.join(timeout=2)


if __name__=="__main__":
    main()
