import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import lifecycle
import storage
from agent import Fixture, Worker
from coordinator import Coordinator, Problem
from mock_service import MockServer


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "state.sqlite"
        self.svc = Coordinator(str(self.path))
        self.addCleanup(lambda: self.svc.close())
        self.identity = {"id":"a", "threads":4, "storage_path":self.tmp.name}
        self.call("register", self.identity, "a")
        self.job = self.call("create_job", {"range":"[0,5]", "max_retries":1, "rate_limit":500}, "admin")["id"]
        self.call("control", self.job, "start", {}, "admin")
        self.chunk = self.call("lease", "a")["chunk"]

    def call(self, method, *args):
        with self.svc.transaction() as db:
            return getattr(self.svc, method)(db, *args)

    def post(self, operation, **fields):
        data = dict(chunk_id=self.chunk["id"], attempt=self.chunk["attempt"], **fields)
        return self.call("worker_op","a",operation,data)

    def history(self, eid):
        with self.svc.transaction(False) as db:
            return [entry["status"] for entry in lifecycle.history(db,self.job,eid,100,0)]

    def test_duplicate_batch_rollback_and_cancel_only_unconfirmed(self):
        checking = [{"seq":1,"status":"checking"}]
        self.post("lifecycle",external_id=0,transitions=checking)
        self.post("lifecycle",external_id=0,transitions=checking)
        self.assertEqual(self.history(0),["pending","checking"])
        with self.assertRaises(Problem):
            self.post("lifecycle",external_id=0,transitions=[{"seq":2,"status":"found"},{"seq":3,"status":"checking"}])
        self.assertEqual(self.history(0),["pending","checking"])
        self.post("progress",external_id=0,status="not_found",transitions=checking)
        self.post("progress",external_id=0,status="not_found",transitions=checking)
        self.call("control",self.job,"stop",{},"admin")
        self.assertEqual(self.history(0),["pending","checking","not_found"])
        self.assertEqual(self.history(1),["pending","cancelled"])
        with self.assertRaises(Problem) as err:
            self.post("lifecycle",external_id=1,transitions=checking)
        self.assertEqual(err.exception.code,409)
        with self.svc.transaction(False) as db:
            self.assertEqual(len(lifecycle.states(db,self.job,{"status":["cancelled"]},100,0)),5)
            self.assertEqual(self.svc.job_summary(db,self.job)["processed"],1)

    def test_restart_preserves_history_and_fences_old_attempt(self):
        self.post("progress",external_id=3,status="skipped")
        self.post("lifecycle",external_id=1,transitions=[{"seq":1,"status":"checking"}])
        self.svc.close()
        self.svc=Coordinator(str(self.path))
        self.call("register",self.identity,"a")
        self.assertEqual(self.history(1),["pending","checking","cancelled"])
        replacement=self.call("lease","a")["chunk"]
        self.assertGreater(replacement["attempt"],self.chunk["attempt"])
        self.assertIn(3,replacement["skip_ids"])
        with self.assertRaises(Problem):
            self.post("progress",external_id=1,status="failed")
        self.chunk=replacement
        self.post("progress",external_id=1,status="forbidden")
        self.assertEqual(self.history(1),["pending","checking","cancelled","pending","forbidden"])
        self.assertEqual(self.history(3),["pending","skipped"])

    def test_denied_download_stays_queued_until_permit_is_granted(self):
        transitions=[{"seq":1,"status":"checking"},{"seq":2,"status":"found"},{"seq":3,"status":"download_queued"}]
        self.post("permit",kind="download",result_status=429,retry_after=60)
        reply=self.post("permit",kind="download",external_id=0,transitions=transitions,download_seq=4)
        self.assertFalse(reply["granted"])
        self.assertEqual(self.history(0)[-1],"download_queued")
        with self.svc.transaction() as db:
            db.execute("UPDATE rate_buckets SET tokens=1,updated=?,blocked_until=0",(time.time(),))
        self.assertTrue(self.post("permit",kind="download",external_id=0,download_seq=4)["granted"])
        self.assertEqual(self.history(0)[-1],"downloading")

    def test_upgrade_with_a_partially_confirmed_live_lease(self):
        # Seed a database with the deployed, pre-history schema, including a hole
        # before a confirmed ID. Migration may run while that lease is still valid.
        old_path = Path(self.tmp.name) / "legacy.sqlite"
        old_migrations = Path(self.tmp.name) / "legacy-schema"
        old_migrations.mkdir()
        for file in storage.MIGRATIONS.glob("*.sql"):
            if file.name < "004.sql":
                (old_migrations / file.name).write_bytes(file.read_bytes())
        with patch.object(storage, "MIGRATIONS", old_migrations):
            old = Coordinator(str(old_path))
        try:
            with old.transaction() as db:
                old.register(db, self.identity, "a")
                jid = old.create_job(db,{"range":"[0,1]"},"admin")["id"]
                old.control(db,jid,"start",{},"admin")
                chunk=old.lease(db,"a")["chunk"]
                old.worker_op(db,"a","progress",dict(chunk_id=chunk["id"],attempt=chunk["attempt"],
                                                 external_id=1,status="not_found"))
        finally:
            old.close()
        upgraded = Coordinator(str(old_path))
        self.addCleanup(upgraded.close)
        with upgraded.transaction(False) as db:
            self.assertEqual([(r["external_id"],r["status"]) for r in lifecycle.states(db,jid,{},10,0)],
                             [(0,"pending"),(1,"not_found")])
            self.assertEqual(lifecycle.history(db,jid,1,10,0)[0]["attempt"],0)
            self.assertEqual(upgraded.job_summary(db,jid)["processed"],1)

    def test_worker_transmits_each_state_with_actual_retries_and_terminal_reports(self):
        cfg = {"items":{"0":{"media_statuses":[429,200], "retry_after":0},
                        "1":{"media_statuses":[404]}, "2":{"media_statuses":[403]},
                        "3":{"download_allowed":False}, "4":{"media_statuses":[500]}}}
        server=MockServer(("127.0.0.1",0),cfg)
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join,3)
        self.addCleanup(server.shutdown)
        worker=Worker(None,Fixture(f"http://127.0.0.1:{server.server_port}"),"a",2,self.tmp.name,1)
        worker.post=lambda op, data: self.call("worker_op","a",op,data)
        worker.run_chunk(self.chunk)
        self.assertEqual(self.history(0),["pending","checking","rate_limited","retry_wait","checking",
                                          "found","download_queued","downloading","completed"])
        self.assertEqual(self.history(1),["pending","checking","not_found"])
        self.assertEqual(self.history(2),["pending","checking","forbidden"])
        self.assertEqual(self.history(3),["pending","checking","found","skipped"])
        self.assertEqual(self.history(4),["pending","checking","retry_wait","checking","failed"])
        self.assertEqual(self.call("job_summary",self.job)["processed"],6)
