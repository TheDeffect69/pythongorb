-- One durable ledger entry per ID, lease generation and sequence number.
-- The current-state index is independent of items (confirmed results only).
CREATE TABLE item_state_history(
 id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
 external_id INTEGER NOT NULL, chunk_id INTEGER NOT NULL REFERENCES range_chunks(id),
 attempt INTEGER NOT NULL, worker_id TEXT NOT NULL REFERENCES workers(id),
 seq INTEGER NOT NULL, ts REAL NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','checking','not_found','found','download_queued',
 'downloading','completed','skipped','forbidden','rate_limited','retry_wait','failed','cancelled')),
 UNIQUE(chunk_id,attempt,external_id,seq)
);
CREATE INDEX item_history_lookup ON item_state_history(job_id,external_id,id);
CREATE TABLE item_states(
 job_id INTEGER NOT NULL REFERENCES jobs(id),external_id INTEGER NOT NULL,
 history_id INTEGER NOT NULL REFERENCES item_state_history(id),PRIMARY KEY(job_id,external_id)
);
CREATE TRIGGER item_state_latest AFTER INSERT ON item_state_history BEGIN
 INSERT INTO item_states(job_id,external_id,history_id) VALUES(NEW.job_id,NEW.external_id,NEW.id)
 ON CONFLICT(job_id,external_id) DO UPDATE SET history_id=excluded.history_id;
END;

-- Legacy confirmed results have no historical lease-generation information.
INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,ts,status)
 SELECT job_id,external_id,chunk_id,0,worker_id,2147483647,ts,status FROM items;
-- A database upgrade can occur with unexpired leases in progress.
INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,ts,status)
 WITH RECURSIVE unfinished(job_id,eid,hi,cid,attempt,worker,ts) AS (
 SELECT job_id,next_id,range_end,id,attempt,leased_by,leased_at FROM range_chunks
 WHERE status='leased' AND next_id<=range_end
 UNION ALL SELECT job_id,eid+1,hi,cid,attempt,worker,ts FROM unfinished WHERE eid<hi
 ) SELECT job_id,eid,cid,attempt,worker,0,ts,'pending' FROM unfinished u
 WHERE NOT EXISTS(SELECT 1 FROM items i WHERE i.job_id=u.job_id AND i.external_id=u.eid);

CREATE TRIGGER item_state_lease AFTER UPDATE OF status ON range_chunks
WHEN NEW.status='leased' AND OLD.status!='leased'
BEGIN
 INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,ts,status)
 WITH RECURSIVE ids(eid) AS (
 SELECT NEW.next_id WHERE NEW.next_id<=NEW.range_end
 UNION ALL SELECT eid+1 FROM ids WHERE eid<NEW.range_end
 ) SELECT NEW.job_id,eid,NEW.id,NEW.attempt,NEW.leased_by,0,NEW.leased_at,'pending' FROM ids
 WHERE NOT EXISTS(SELECT 1 FROM items WHERE job_id=NEW.job_id AND external_id=eid);
END;

CREATE TRIGGER item_state_revoke AFTER UPDATE OF status ON range_chunks
WHEN OLD.status='leased' AND NEW.status IN ('queued','cancelled')
BEGIN
 INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,ts,status)
 SELECT h.job_id,h.external_id,h.chunk_id,h.attempt,h.worker_id,2147483647,
 (julianday('now')-2440587.5)*86400.0,'cancelled'
 FROM item_states s JOIN item_state_history h ON h.id=s.history_id
 WHERE s.job_id=OLD.job_id AND s.external_id BETWEEN OLD.next_id AND OLD.range_end
 AND h.chunk_id=OLD.id AND h.attempt=OLD.attempt AND h.seq<2147483647;
END;

CREATE TRIGGER item_state_confirm AFTER INSERT ON items BEGIN
 INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,ts,status)
 SELECT NEW.job_id,NEW.external_id,NEW.chunk_id,c.attempt,NEW.worker_id,2147483647,NEW.ts,NEW.status
 FROM range_chunks c WHERE c.id=NEW.chunk_id;
END;
