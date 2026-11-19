"""Persist agent-reported transitions under the coordinator's fenced transaction."""
import time


NEXT_STATES = {
    "pending": {"checking"},
    "checking": {"found", "retry_wait", "rate_limited"},
    "found": {"download_queued"},
    "download_queued": {"downloading", "retry_wait"},
    "downloading": {"retry_wait", "rate_limited"},
    "rate_limited": {"retry_wait"},
    "retry_wait": {"checking", "download_queued"},
}
LAST_SEQ = 2**31 - 1


def record(db, chunk, worker, payload):
    """Replay-safe batched intermediate states. Terminal results are written by an SQL trigger."""
    if "transitions" not in payload:
        return
    batch = payload["transitions"]
    eid = payload.get("external_id")
    if (type(eid) is not int or not chunk["range_start"] <= eid <= chunk["range_end"]
            or not isinstance(batch, list) or not 1 <= len(batch) <= 128):
        raise ValueError("Некорректный пакет переходов ID")
    latest = db.execute("""SELECT h.status,h.seq,h.attempt FROM item_states s
        JOIN item_state_history h ON h.id=s.history_id WHERE s.job_id=? AND s.external_id=?""",
        (chunk["job_id"], eid)).fetchone()
    if latest is None:
        raise ValueError("ID ещё не назначен")
    if latest["seq"] == LAST_SEQ:
        # A previously committed terminal report may legitimately be replayed.
        return
    current, seq = latest["status"], latest["seq"]
    key = (chunk["id"], chunk["attempt"], eid)
    if latest["attempt"] != chunk["attempt"]:
        raise ValueError("Неверная попытка обработки")
    for entry in batch:
        if not isinstance(entry, dict):
            raise ValueError("Некорректный переход состояния")
        n, state = entry.get("seq"), entry.get("status")
        if type(n) is not int or not 0 < n < LAST_SEQ or not isinstance(state, str):
            raise ValueError("Некорректное состояние или номер перехода")
        if n <= seq:
            saved = db.execute("""SELECT status FROM item_state_history
                WHERE chunk_id=? AND attempt=? AND external_id=? AND seq=?""", (*key, n)).fetchone()
            if not saved or saved[0] != state:
                raise ValueError("Конфликт повторной отправки")
            continue
        if n != seq + 1 or state not in NEXT_STATES.get(current, ()):
            raise ValueError("Нарушен порядок переходов состояния")
        db.execute("""INSERT INTO item_state_history(job_id,external_id,chunk_id,attempt,worker_id,seq,status,ts)
                   VALUES(?,?,?,?,?,?,?,?)""",
                   (chunk["job_id"],eid,chunk["id"],chunk["attempt"],worker,n,state,time.time()))
        current, seq = state, n


def states(db, jid, query, limit, offset):
    clauses, params = ["s.job_id=?"], [jid]
    for field in ("external_id", "status", "worker_id"):
        if field in query:
            clauses.append(f"h.{field}=?")
            params.append(query[field][0])
    sql = """SELECT h.* FROM item_states s JOIN item_state_history h ON h.id=s.history_id
        WHERE """ + " AND ".join(clauses) + " ORDER BY s.external_id LIMIT ? OFFSET ?"
    return [dict(row) for row in db.execute(sql, (*params, limit, offset))]


def history(db, jid, eid, limit, offset):
    return [dict(row) for row in db.execute("""SELECT * FROM item_state_history
        WHERE job_id=? AND external_id=? ORDER BY id LIMIT ? OFFSET ?""", (jid, eid, limit, offset))]
