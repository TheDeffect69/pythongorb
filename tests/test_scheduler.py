import tempfile
import time
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from coordinator import Coordinator, Problem, capacity_chunk


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.coordinator = Coordinator(Path(folder.name) / "lab.db")
        self.addCleanup(self.coordinator.close)
        for wid in ("a", "b"):
            self.call("register", {"id":wid,"storage_path":folder.name,"threads":8}, wid)

    def call(self, method, *args):
        with self.coordinator.transaction() as db:
            return getattr(self.coordinator, method)(db, *args)

    def job(self, **settings):
        settings = {"range":"[1,100]", "chunk_size":1, "rate_limit":1000, **settings}
        job = self.call("create_job", settings, "operator")
        self.call("control",job["id"],"start",{},"operator")
        return job["id"]

    def permit(self, wid, lease, kind):
        return self.call("worker_op",wid,"permit",
                         {"chunk_id":lease["id"],"attempt":lease["attempt"],"kind":kind})

    def test_job_kind_budget_is_shared_and_independent_of_other_kind(self):
        jid = self.job(check_rps=1, download_rps=1000)
        a = self.call("lease","a")["chunk"]
        b = self.call("lease","b")["chunk"]
        now = time.time()
        with patch("coordinator.time.time", return_value=now):
            self.assertTrue(self.permit("a",a,"check")["granted"])
        with patch("coordinator.time.time", return_value=now+0.1):
            wait=self.permit("b",b,"check")
            self.assertFalse(wait["granted"])
            self.assertGreaterEqual(wait["wait"],0.89)
            self.assertTrue(self.permit("b",b,"download")["granted"])
            self.call("control",jid,"edit",{"check_rps":None,"rate_limit":800},"operator")
            self.assertTrue(self.permit("b",b,"check")["granted"])

    def test_live_worker_caps_and_backpressure_are_independent(self):
        jid = self.job()
        a=self.call("lease","a")["chunk"]
        b=self.call("lease","b")["chunk"]
        self.call("edit_worker","a",{"worker_rps":2,"download_rps":1},"operator")
        now=time.time()
        with patch("coordinator.time.time",return_value=now):
            self.assertTrue(self.permit("a",a,"download")["granted"])
        with patch("coordinator.time.time",return_value=now+0.1):
            self.assertFalse(self.permit("a",a,"check")["granted"])
            self.assertTrue(self.permit("b",b,"download")["granted"])
            self.call("edit_worker","a",{"worker_rps":None},"operator")
            self.assertTrue(self.permit("a",a,"check")["granted"])
            self.assertFalse(self.permit("a",a,"download")["granted"])
            self.call("worker_op","a","permit",{"chunk_id":a["id"],"attempt":a["attempt"],
                      "kind":"check","result_status":429,"retry_after":5})
            self.call("control",jid,"edit",{"check_rps":1000},"operator")
            self.assertGreaterEqual(self.permit("b",b,"download")["wait"],5)

    def test_jobs_take_turns_and_node_overrides_are_scoped(self):
        first=self.job(worker_ids=["a"],worker_settings={"a":{"threads":3,"check_rps":1}})
        second=self.job(worker_ids=["a"],threads=6)
        lease=self.call("lease","a")["chunk"]
        self.assertEqual((lease["job_id"],lease["threads"]),(first,3))
        self.call("worker_op","a","fail",{"chunk_id":lease["id"],"attempt":lease["attempt"]})
        nxt=self.call("lease","a")["chunk"]
        self.assertEqual((nxt["job_id"],nxt["threads"]),(second,6))
        self.assertIsNone(self.call("worker_summary","a")["check_rps"])
        with self.assertRaises(Problem):
            self.job(worker_ids=["a"],worker_settings={"b":{"check_rps":0}})
        with self.coordinator.transaction(write=False) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0],2)

    def test_migration_003_preserves_prior_jobs_workers_and_ownership(self):
        # Build the previous schema and a populated membership before applying 003.
        from storage import MIGRATIONS, initialize
        path=Path(self.coordinator.path).with_name("prior.db")
        db=sqlite3.connect(path)
        try:
            db.execute("CREATE TABLE schema_migrations(version TEXT PRIMARY KEY,applied_at TEXT)")
            for name in ("001.sql","002.sql"):
                db.executescript((MIGRATIONS/name).read_text(encoding="utf-8"))
                db.execute("INSERT INTO schema_migrations(version) VALUES(?)",(name,))
            db.execute("""INSERT INTO workers(id,name,version,storage_path,threads,last_heartbeat,registered_at)
                        VALUES('a','A','1.0','/var/data',3,0,0)""")
            db.execute("""INSERT INTO jobs(name,status,range_start,next_start,chunk_size,threads,rate_limit,
                max_retries,request_timeout_seconds,min_free_space_gb,created_at,updated_at)
                VALUES('old','paused',0,0,5,2,30,4,10,2,0,0)""")
            db.execute("INSERT INTO job_workers(job_id,worker_id) VALUES(1,'a')")
            db.commit()
        finally:
            db.close()
        initialize(path)
        migrated=Coordinator(path)
        self.addCleanup(migrated.close)
        with migrated.transaction(write=False) as db:
            job=migrated.job_summary(db,1)
            self.assertEqual((job["worker_ids"],job["status"],job["rate_limit"]),(["a"],"paused",30))
            self.assertIsNone(job["check_rps"])
            self.assertIsNone(migrated.worker_summary(db,"a")["worker_rps"])

    def test_resource_measurements_scale_new_pulls_with_cold_start_fallback(self):
        self.assertEqual(capacity_chunk(500,8,30,[]),500)
        def samples(last_speed,network,disk,errors):
            snapshots=[]
            for t, speed, net, io, cnt in [(1,10,0,0,0),(2,10,100,100,0),
                                          (3,last_speed,network,disk,errors)]:
                snapshots.append((t,{"processed_per_second":speed,"network_rx_bytes":net,
                    "network_tx_bytes":0,"disk_read_bytes":0,"disk_write_bytes":io,
                    "errors":cnt,"processed":t*10}))
            return snapshots
        def size(*args):
            return capacity_chunk(500,8,20,samples(*args))
        base=size(10,200,200,0)
        slow=size(2,200,200,0)
        self.assertGreater(base,slow)
        self.assertLess(size(2,1000,200,0),slow)
        self.assertLess(size(2,200,1000,0),slow)
        self.assertLess(size(2,200,200,4),slow)
        self.assertGreaterEqual(size(0,1000,1000,1000),1)
