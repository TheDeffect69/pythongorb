CREATE TABLE roles(name TEXT PRIMARY KEY);
INSERT INTO roles VALUES ('viewer'),('operator'),('administrator');
CREATE TABLE users(id TEXT PRIMARY KEY);
CREATE TABLE user_roles(user_id TEXT REFERENCES users(id),role TEXT REFERENCES roles(name),PRIMARY KEY(user_id,role));
CREATE TABLE workers(
 id TEXT PRIMARY KEY,name TEXT NOT NULL,version TEXT NOT NULL,storage_path TEXT NOT NULL,
 threads INTEGER NOT NULL,thread_override INTEGER,enabled INTEGER NOT NULL DEFAULT 1,
 last_heartbeat REAL NOT NULL,registered_at REAL NOT NULL,metrics TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE worker_metrics(id INTEGER PRIMARY KEY,worker_id TEXT NOT NULL REFERENCES workers(id),ts REAL NOT NULL,data TEXT NOT NULL);
CREATE INDEX worker_metrics_time ON worker_metrics(worker_id,ts);
CREATE TABLE jobs(
 id INTEGER PRIMARY KEY,name TEXT NOT NULL,status TEXT NOT NULL,
 range_start INTEGER NOT NULL,range_end INTEGER,next_start INTEGER NOT NULL,
 chunk_size INTEGER NOT NULL,threads INTEGER NOT NULL,rate_limit INTEGER NOT NULL,
 max_retries INTEGER NOT NULL,request_timeout_seconds INTEGER NOT NULL,min_free_space_gb REAL NOT NULL,
 max_items INTEGER,max_seconds INTEGER,stop_on_consecutive_missing INTEGER,
 restricted_workers INTEGER NOT NULL DEFAULT 0,
 created_at REAL NOT NULL,started_at REAL,updated_at REAL NOT NULL
);
CREATE TABLE job_workers(job_id INTEGER REFERENCES jobs(id),worker_id TEXT REFERENCES workers(id),PRIMARY KEY(job_id,worker_id));
CREATE TABLE range_chunks(
 id INTEGER PRIMARY KEY,job_id INTEGER NOT NULL REFERENCES jobs(id),
 range_start INTEGER NOT NULL,range_end INTEGER NOT NULL,next_id INTEGER NOT NULL,
 status TEXT NOT NULL,leased_by TEXT REFERENCES workers(id),lease_expires_at REAL,leased_at REAL,
 attempt INTEGER NOT NULL DEFAULT 0,created_at REAL NOT NULL,updated_at REAL NOT NULL,
 UNIQUE(job_id,range_start,range_end)
);
CREATE INDEX range_chunks_claim ON range_chunks(status,job_id,id);
CREATE UNIQUE INDEX one_lease_per_worker ON range_chunks(leased_by) WHERE status='leased';
CREATE TABLE items(id INTEGER PRIMARY KEY,job_id INTEGER NOT NULL REFERENCES jobs(id),external_id INTEGER NOT NULL,
 chunk_id INTEGER NOT NULL REFERENCES range_chunks(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 status TEXT NOT NULL,error TEXT,ts REAL NOT NULL,UNIQUE(job_id,external_id));
CREATE INDEX items_chunk ON items(chunk_id,external_id);
CREATE TABLE media_metadata(item_id INTEGER PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,data TEXT NOT NULL);
CREATE TABLE downloads(id INTEGER PRIMARY KEY,item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
 worker_id TEXT NOT NULL REFERENCES workers(id),attempt INTEGER NOT NULL,status TEXT NOT NULL,ts REAL NOT NULL);
CREATE TABLE files(id INTEGER PRIMARY KEY,download_id INTEGER NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
 storage_path TEXT NOT NULL,data TEXT NOT NULL,size INTEGER NOT NULL,UNIQUE(download_id,storage_path));
CREATE TABLE events(id INTEGER PRIMARY KEY,ts REAL NOT NULL,level TEXT NOT NULL,component TEXT NOT NULL,
 job_id INTEGER,worker_id TEXT,chunk_id INTEGER,external_id INTEGER,operation TEXT NOT NULL,result TEXT NOT NULL,
 code TEXT,message TEXT NOT NULL);
CREATE INDEX events_job ON events(job_id,id);
CREATE TABLE audit_log(id INTEGER PRIMARY KEY,ts REAL NOT NULL,actor TEXT NOT NULL,operation TEXT NOT NULL,
 job_id INTEGER,worker_id TEXT,result TEXT NOT NULL);
CREATE TABLE rate_buckets(key TEXT PRIMARY KEY,tokens REAL NOT NULL,updated REAL NOT NULL,
 blocked_until REAL NOT NULL DEFAULT 0,scale REAL NOT NULL DEFAULT 1);
