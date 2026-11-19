"""Bounded disk-full and database-full faults with recovery of real application state."""
import errno
from contextlib import closing
import hashlib
import io
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from agent import Worker
from coordinator import Coordinator


DATA = bytes(range(256)) * 512
CHECKSUM = hashlib.sha256(DATA).hexdigest()


class Fixture:
    def __init__(self):
        self.offsets = []

    def metadata(self, item, timeout, permit, report):
        permit("check")
        report(200)
        return dict(id=item, source_url="/media/7", download_url="/files/7", title="Fixture",
                    size=len(DATA), download_allowed=True, mime_type="video/mp4", sha256=CHECKSUM)

    def url(self, link):
        return link

    def open(self, link, timeout, headers):
        start = int(headers["Range"][6:-1]) if "Range" in headers else 0
        self.offsets.append(start)
        stream = io.BytesIO(DATA[start:])
        stream.status = 206 if start else 200
        stream.headers = {"Content-Range":f"bytes {start}-{len(DATA)-1}/{len(DATA)}"}
        return stream


def limited_tmpfs(directory):
    """Only a separately mounted, tiny tmpfs may be filled by this scenario."""
    root = Path(directory).resolve()
    if not sys.platform.startswith("linux") or not root.is_mount():
        raise ValueError("Expected a dedicated Linux tmpfs mount")
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields, details = line.split(" - ", 1)
        if fields.split()[4].replace(r"\040", " ") == str(root) and details.split()[0] == "tmpfs":
            break
    else:
        raise ValueError("Target must have its own tmpfs mountinfo entry")
    stats = os.statvfs(root)
    if not 0 < stats.f_blocks*stats.f_frsize <= 2*1024**2 or any(root.iterdir()):
        raise ValueError("Expected an empty tmpfs limited to 2 MiB")
    return root


class FaultTests(unittest.TestCase):
    def scenario(self, root, kernel):
        with tempfile.TemporaryDirectory() as state, closing(Coordinator(Path(state)/"lab.sqlite")) as svc:
            source = Fixture()
            worker = Worker(None, source, "one", 1, root, 1)
            reasons = []

            def post(op, data):
                if op == "fail":
                    reasons.append(data["error"])
                with svc.transaction() as db:
                    return svc.worker_op(db, "one", op, data)

            worker.post = post
            with svc.transaction() as db:
                svc.register(db, {"id":"one", "storage_path":str(root), "threads":1}, "one")
                jid = svc.create_job(db, {"range":"[7,7]"}, "operator")["id"]
                svc.control(db,jid,"start",{},"operator")
            post("heartbeat", {"metrics":{"free_space_gb":1}})
            chunk = post("lease", {})["chunk"]
            file_open = Path.open
            filler = root / "competing-writer.bin"
            write_errors = []

            class FaultingOutput:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

                def write(self, data):
                    if not self.stream.tell():
                        return self.stream.write(data)
                    if kernel:
                        # A competing writer takes the remaining capacity between
                        # the worker's free-space check and its second OS write.
                        stats = os.statvfs(root)
                        size = stats.f_bavail * stats.f_frsize
                        if not 0 < size <= 2*1024**2:
                            raise AssertionError("tmpfs capacity changed unexpectedly")
                        with file_open(filler, "xb", buffering=0) as out:
                            os.posix_fallocate(out.fileno(), 0, size)
                        try:
                            return self.stream.write(data)
                        except OSError as exc:
                            write_errors.append(exc.errno)
                            raise
                    write_errors.append(errno.ENOSPC)
                    raise OSError(errno.ENOSPC,"injected write fault")

                def __getattr__(self, key):
                    return getattr(self.stream, key)

            def intercept(path, *args, **kwargs):
                if path.name.endswith(".part") and args and args[0]=="wb":
                    return FaultingOutput(file_open(path, "wb", buffering=0))
                return file_open(path, *args, **kwargs)

            with patch.object(Path, "open", intercept):
                worker.run_chunk(chunk)
            self.assertEqual(write_errors, [errno.ENOSPC])
            self.assertEqual(reasons, ["insufficient_disk"])
            self.assertIsNone(post("lease", {})["chunk"])
            with patch.object(worker.metrics,"snapshot",return_value={"free_space_gb":0}):
                worker.heartbeat()
            self.assertIsNone(post("lease", {})["chunk"])
            part = root/"7"/"7"/"video.mp4.part"
            self.assertEqual(part.read_bytes(), DATA[:65536])
            target = part.with_suffix("")
            self.assertFalse(target.exists())
            if kernel:
                filler.unlink()
            available = shutil.disk_usage(root).free if kernel else worker.required_free_bytes + 4096
            with patch.object(worker.metrics,"snapshot",return_value={"free_space_gb":available / 2**30}):
                worker.heartbeat()
            resumed = post("lease", {})["chunk"]
            self.assertEqual(resumed["id"],chunk["id"])
            self.assertGreater(resumed["attempt"],chunk["attempt"])
            worker.run_chunk(resumed)
            self.assertEqual(source.offsets, [0,65536])
            self.assertEqual(target.read_bytes(), DATA)
            self.assertFalse(part.exists())
            with svc.transaction(False) as db:
                job = svc.job_summary(db,jid)
                self.assertEqual((job["status"], job["downloaded"]), ("completed", 1))
                self.assertEqual(db.execute("SELECT count(*) FROM files").fetchone()[0],1)

    def test_enospc_during_second_write_and_retry(self):
        with tempfile.TemporaryDirectory() as storage:
            self.scenario(Path(storage), False)

    def test_real_tmpfs_enospc(self):
        directory = os.getenv("LAB_DISPOSABLE_TMPFS")
        if not directory:
            self.skipTest("opt-in bounded Linux tmpfs; see docs/faults.md")
        mount = limited_tmpfs(directory)
        with tempfile.TemporaryDirectory(dir=mount) as root:
            self.scenario(Path(root),True)

    def test_sqlite_full_rolls_back_and_reopens_writer(self):
        with tempfile.TemporaryDirectory() as state:
            svc = Coordinator(Path(state)/"full.sqlite")
            try:
                with svc.transaction() as db:
                    svc.register(db, {"id":"one","threads":1,"storage_path":state}, "one")
                    jid = svc.create_job(db,{"range":"[0,1]","chunk_size":2},"operator")["id"]
                    svc.control(db,jid,"start",{},"operator")
                    c = svc.lease(db,"one")["chunk"]
                    lease = {"chunk_id":c["id"], "attempt":c["attempt"]}
                    svc.worker_op(db,"one","progress",{**lease,"external_id":0,"status":"not_found"})
                # The next metadata value cannot fit within the current page count.
                # FULL is returned by SQLite's pager while other jobs remain untouched.
                metadata = {"title":"x"*(128*1024)}
                result = {**lease,"external_id":1,"status":"skipped","metadata":metadata}
                with self.assertRaises(sqlite3.OperationalError) as failure:
                    with svc.transaction() as old:
                        pages = old.execute("PRAGMA page_count").fetchone()[0]
                        self.assertEqual(old.execute(f"PRAGMA max_page_count={pages}").fetchone()[0],pages)
                        svc.worker_op(old,"one","progress",result)
                self.assertEqual(failure.exception.sqlite_errorcode, sqlite3.SQLITE_FULL)
                with self.assertRaises(sqlite3.ProgrammingError):
                    old.execute("SELECT 1")
                with svc.transaction(False) as db:
                    self.assertEqual([r[0] for r in db.execute("SELECT external_id FROM items")],[0])
                    self.assertEqual(db.execute("SELECT next_id FROM range_chunks").fetchone()[0],1)
                with svc.transaction() as reopened:
                    self.assertIsNot(reopened,old)
                    reopened.execute("PRAGMA max_page_count=65536")
                    svc.worker_op(reopened,"one","progress",result)
                    svc.worker_op(reopened,"one","progress",result)
                    svc.worker_op(reopened,"one","complete",lease)
                    self.assertEqual(svc.job_summary(reopened,jid)["status"],"completed")
                    self.assertEqual(reopened.execute("SELECT count(*) FROM items").fetchone()[0],2)
                    self.assertEqual(reopened.execute("PRAGMA integrity_check").fetchone()[0],"ok")
            finally:
                svc.close()
