ALTER TABLE workers ADD COLUMN address TEXT;
CREATE INDEX events_worker_time ON events(worker_id,ts);
CREATE INDEX items_job_status ON items(job_id,status);
