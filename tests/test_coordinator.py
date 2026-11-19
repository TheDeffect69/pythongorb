import concurrent.futures
import json
import tempfile
import threading
import time
import sqlite3
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from coordinator import Coordinator, Handler, Problem, Server, auth_tokens, parse_range


class RangeTests(unittest.TestCase):
    def test_all_formats_and_boundaries(self):
        for text, expected in [("0", (0, 0)), ("7", (0, 7)), ("[7,7]", (7, 7)),
                               ("[0, 3]", (0, 3)), ("[9,]", (9, None)),
                               ("", (0, None)), ({"to": 0}, (0, 0))]:
            with self.subTest(text=text):
                self.assertEqual(parse_range(text), expected)
        for text in ("-1", "[3,2]", "[,5]", "1.5", "[]", "[0,-1]", {"to": True}, 2**63-1):
            with self.subTest(text=text), self.assertRaises(Problem):
                parse_range(text)

    def test_identities_need_distinct_secrets(self):
        with self.assertRaises(ValueError):
            auth_tokens({"LAB_ADMIN_TOKEN":"duplicate","LAB_WORKER_TOKENS":'{"alpha":"duplicate"}'})


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name)/"test.sqlite")
        self.svc = Coordinator(self.path, lease_seconds=60)
        self.addCleanup(lambda: self.svc.close())
        for name in ("a","b","c"):
            self.call("register", {"id":name, "threads":16, "storage_path":self.temp.name},name)

    def call(self, method, *args):
        with self.svc.transaction() as db:
            return getattr(self.svc,method)(db,*args)

    def job(self, **kwargs):
        data = {"range":"[0,9]", "chunk_size":4, **kwargs}
        jid = self.call("create_job", data,"operator")["id"]
        self.call("control", jid, "start", {}, "operator")
        return jid

    def lease(self, worker):
        return self.call("lease",worker)["chunk"]

    def op(self, worker, chunk, operation, **data):
        return self.call("worker_op",worker,operation,
                         dict(chunk_id=chunk["id"],attempt=chunk["attempt"],**data))

    def test_non_overlapping_atomic_leases_and_bounds(self):
        jid=self.job()
        with concurrent.futures.ThreadPoolExecutor(3) as executor:
            chunks=list(executor.map(self.lease,("a","b","c")))
        self.assertEqual(sorted((c["range_start"],c["range_end"]) for c in chunks),
                         [(0,3),(4,7),(8,9)])
        for w,c in zip(("a","b","c"),chunks):
            self.assertEqual(c,self.lease(w))
            for n in range(c["range_start"],c["range_end"]+1):
                self.op(w,c,"progress",external_id=n,status="not_found")
            self.op(w,c,"complete")
        self.assertEqual(self.call("job_summary",jid)["status"],"completed")

    def test_fifo_writer_rollback_and_concurrent_wal_reader(self):
        order = []
        def writer(index):
            with self.svc.transaction() as db:
                order.append(index)
                db.execute("INSERT INTO users VALUES(?)", (str(index),))
                if index == 1:
                    raise Problem(409, "Rollback test")

        with concurrent.futures.ThreadPoolExecutor(4) as executor:
            with self.svc.transaction() as db:
                db.execute("INSERT INTO users VALUES('pending')")
                # A reader sees the committed snapshot while the writer is busy.
                with self.svc.transaction(write=False) as reader:
                    self.assertFalse(reader.execute("SELECT 1 FROM users WHERE id='pending'").fetchone())
                tasks = []
                for i in range(3):
                    tasks.append(executor.submit(writer,i))
                    deadline = time.monotonic() + 2
                    while True:
                        with self.svc.writer_queue.lock:
                            if len(self.svc.writer_queue.waiters) == i + 2:
                                break
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.001)
                # Aborting a wait must not leave a dead ticket ahead of future requests.
                with self.assertRaises(sqlite3.OperationalError):
                    with self.svc.writer_queue.turn(timeout=0):
                        self.fail("writer was entered twice")
            tasks[0].result(timeout=3)
            with self.assertRaises(Problem):
                tasks[1].result(timeout=3)
            tasks[2].result(timeout=3)
        self.assertEqual(order,[0,1,2])
        with self.svc.transaction(write=False) as db:
            self.assertEqual({r[0] for r in db.execute("SELECT id FROM users")},{"0","2","pending"})

    def test_recovery_preserves_out_of_order_id_and_rejects_old_owner(self):
        jid=self.job(range="[0,2]")
        c=self.lease("a")
        self.op("a",c,"progress",external_id=1,status="not_found")
        self.assertTrue(self.op("a",c,"progress",external_id=1,status="not_found")["duplicate"])
        with self.svc.transaction() as db:
            db.execute("UPDATE range_chunks SET lease_expires_at=0 WHERE id=?",(c["id"],))
        self.svc.close()
        self.svc=Coordinator(self.path)
        replacement=self.lease("b")
        self.assertEqual(replacement["skip_ids"],[1])
        self.assertEqual(replacement["next_id"],0)
        self.assertGreater(replacement["attempt"],c["attempt"])
        with self.assertRaises(Problem):
            self.op("a",c,"progress",external_id=0,status="failed")
        with self.assertRaises(Problem):
            self.op("b",replacement,"complete")
        for n in (0,2):
            self.op("b",replacement,"progress",external_id=n,status="not_found")
        self.op("b",replacement,"complete")
        self.assertEqual(self.call("job_summary",jid)["processed"],3)

    def test_pause_drains_then_resume_and_stop_invalidates_lease(self):
        jid=self.job(range="[100,]",chunk_size=1)
        c=self.lease("a")
        self.call("control",jid,"pause",{},"operator")
        self.assertIsNone(self.lease("b"))
        self.op("a",c,"progress",external_id=100,status="forbidden")
        self.op("a",c,"complete")
        self.assertEqual(self.call("job_summary",jid)["status"],"paused")
        self.call("control",jid,"resume",{},"operator")
        nxt=self.lease("b")
        self.assertEqual(nxt["range_start"],101)
        self.call("control",jid,"stop",{},"operator")
        with self.assertRaises(Problem):
            self.op("b",nxt,"permit",kind="check")
        with self.assertRaises(Problem):
            self.call("control",jid,"start",{},"operator")

    def test_disk_threshold_and_backoff_shared_between_agents(self):
        self.job(chunk_size=1,min_free_space_gb=10)
        self.assertIsNone(self.lease("c"))
        for w in ("a","b"):
            self.call("heartbeat",w,{"metrics":{"free_space_gb":20}})
        a,b=self.lease("a"),self.lease("b")
        self.assertTrue(self.op("a",a,"permit",kind="check")["granted"])
        self.op("a",a,"permit",kind="check",result_status=429,retry_after=3)
        decision=self.op("b",b,"permit",kind="download")
        self.assertFalse(decision["granted"])
        self.assertGreater(decision["wait"],2)

    def test_open_job_stops_when_all_eligible_workers_are_offline(self):
        open_job=self.job(range="[40,]",worker_ids=["a"])
        finite_job=self.job(range="[0,9]",worker_ids=["a"])
        with self.svc.transaction() as db:
            db.execute("UPDATE jobs SET started_at=1")
            db.execute("UPDATE workers SET last_heartbeat=1 WHERE id='a'")
            self.svc.reap(db)
        self.assertEqual(self.call("job_summary",open_job)["status"],"stopped")
        self.assertEqual(self.call("job_summary",finite_job)["status"],"running")

    def test_open_range_limits_and_ordered_missing_run(self):
        jid=self.job(range="[8,]",chunk_size=4,stop_on_consecutive_missing=2,max_items=4)
        c=self.lease("a")
        self.assertEqual(c["range_end"],11)
        for n,status in [(11,"not_found"),(9,"forbidden"),(8,"failed"),(10,"not_found")]:
            self.op("a",c,"progress",external_id=n,status=status)
        self.op("a",c,"complete")
        self.assertEqual(self.call("job_summary",jid)["status"],"completed")
        with self.svc.transaction() as db:
            self.assertEqual(db.execute("SELECT operation FROM events ORDER BY id DESC LIMIT 1").fetchone()[0],"missing_limit")

    def test_migrations_repeat_and_preserve_existing_workers(self):
        from storage import initialize
        with self.svc.transaction() as db:
            db.execute("UPDATE workers SET address='127.0.0.2' WHERE id='b'")
        initialize(self.path)
        self.assertEqual(self.call("worker_summary","b")["host"],"127.0.0.2")

    def test_resource_cap_and_thread_override(self):
        self.job(threads=2)
        self.call("heartbeat","a",{"metrics":{"ram_total_bytes":1000,"ram_used_bytes":990}})
        self.assertIsNone(self.lease("a"))
        with self.svc.transaction() as db:
            db.execute("UPDATE workers SET thread_override=12 WHERE id='b'")
        self.assertEqual(self.lease("b")["threads"],12)

    def test_file_path_cannot_escape_and_completed_file_is_idempotent(self):
        self.job(range="0")
        c=self.lease("a")
        def save(path):
            return self.op("a",c,"progress",external_id=0,status="completed",
                           file={"storage_path":str(path),"size":5},metadata={"title":"test"})
        with self.assertRaises(Problem):
            save(Path(self.temp.name).parent/"outside.mp4")
        save(Path(self.temp.name)/"0.mp4")
        self.assertTrue(save(Path(self.temp.name)/"0.mp4")["duplicate"])
        self.assertEqual(self.call("job_summary",c["job_id"])["bytes_downloaded"],5)

    def test_authenticated_http_api_and_ssh_audit(self):
        with Server(("127.0.0.1",0),Handler) as server:
            server.svc=self.svc
            server.tokens={"aa":("administrator","administrator"),"vv":("viewer","viewer"),
                           "ww":("agent","a")}
            thread=threading.Thread(target=server.serve_forever)
            thread.start()
            def request(path,token,method="GET",body=None):
                req=urllib.request.Request(f"http://127.0.0.1:{server.server_port}/api/"+path,
                    data=None if body is None else json.dumps(body).encode(),method=method,
                    headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
                try:
                    with urllib.request.urlopen(req,timeout=5) as response:
                        return response.status,json.load(response)
                except urllib.error.HTTPError as exc:
                    with exc:
                        return exc.code,json.load(exc)
            try:
                self.assertEqual(request("jobs","bad")[0],401)
                self.assertEqual(request("jobs","vv","POST",{"range":"0"})[0],403)
                self.assertEqual(request("workers/b/lease","ww","POST",{})[0],403)
                self.assertEqual(request("workers/a/service","vv","POST",{"action":"start"})[0],403)
                with patch("ssh_control.run_service_action") as ssh:
                    self.assertEqual(request("workers/a/service","aa","POST",{"action":"status"})[0],200)
                    ssh.assert_called_once_with("a","status")
                audit=request("audit","aa")[1]
                self.assertEqual([x["result"] for x in audit[:2]],["ok","denied"])
                job=request("jobs","aa","POST",{"range":"0"})[1]
                self.assertEqual(request("jobs/"+str(job["id"]),"vv")[1]["range_end"],0)
            finally:
                server.shutdown()
                thread.join()
