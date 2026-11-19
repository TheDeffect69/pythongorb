import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import Coordinator


class ProgressTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.coord = Coordinator(Path(tmp.name)/"job.db")
        self.addCleanup(self.coord.close)
        self.root = tmp.name

    def test_inclusive_count_speed_and_eta_with_item_cap(self):
        c = self.coord
        with c.transaction() as db:
            c.register(db, {"id":"w", "storage_path":self.root}, "w")
            job = c.create_job(db, {"range":"[10,109]", "max_items":4}, "operator")
            self.assertEqual((job["total_items"],job["progress_percent"],job["eta_seconds"]), (4,0,None))
            jid = job["id"]
            with patch("coordinator.time.time", return_value=100):
                c.control(db,jid,"start",{},"operator")
                chunk = c.lease(db, "w")["chunk"]
            with patch("coordinator.time.time", return_value=110):
                envelope = {"chunk_id":chunk["id"],"attempt":chunk["attempt"]}
                for eid in (11,10):
                    c.worker_op(db,"w","progress",dict(envelope,external_id=eid,status="not_found"))
                stats = c.job_summary(db,jid)
                self.assertEqual((stats["progress_percent"],stats["speed"],stats["eta_seconds"]), (50,.2,10))
                for eid in (12,13):
                    c.worker_op(db,"w","progress",dict(envelope,external_id=eid,status="not_found"))
                c.worker_op(db,"w","complete",envelope)
                stats = c.job_summary(db,jid)
                self.assertEqual((stats["status"],stats["progress_percent"],stats["eta_seconds"]),("completed",100,0))

    def test_open_range_has_no_eta_and_single_id_is_one(self):
        with self.coord.transaction() as db:
            for inputs, total in [({"range":"[7,7]"},1),({"range":"[7,]"},None),
                                  ({"range":"","max_items":12},12)]:
                job = self.coord.create_job(db,inputs,"operator")
                self.assertEqual(job["total_items"],total)
                self.assertIsNone(job["eta_seconds"])

    def test_start_checks_saved_configuration_and_stop_records_transitions(self):
        with self.coord.transaction() as db:
            create = lambda: self.coord.create_job(db, {"range":"[0,2]"}, "operator")["id"]
            good = create()
            self.coord.control(db,good,"start",{},"operator")
            self.coord.control(db,good,"stop",{},"operator")
            states = [r[0] for r in db.execute("SELECT result FROM events WHERE job_id=? ORDER BY id", (good,))]
            self.assertEqual(states, ["created","validating","running","stopping","stopped"])
            broken = create()
            db.execute("UPDATE jobs SET restricted_workers=1 WHERE id=?", (broken,))
            result = self.coord.control(db,broken,"start",{},"operator")
            self.assertEqual(result["status"], "failed")
            self.assertEqual(db.execute("SELECT operation FROM events WHERE job_id=? ORDER BY id DESC", (broken,)).fetchone()[0],
                             "invalid_saved_configuration")
