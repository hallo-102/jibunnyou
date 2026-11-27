"""workerだけが所有するSQLite台帳。実行と報告の再送を分離する。"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from .schema import TERMINAL, canonical, utcnow


class State:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise ValueError("UNSAFE_STATE_PATH")
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY, issue_number INTEGER NOT NULL,
          request_hash TEXT NOT NULL, job TEXT NOT NULL, status TEXT NOT NULL,
          edited INTEGER NOT NULL DEFAULT 0, started_at TEXT, finished_at TEXT,
          heartbeat_at TEXT, error_code TEXT, result TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS issues (number INTEGER PRIMARY KEY, request_hash TEXT NOT NULL, edited INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (job_id TEXT, request_hash TEXT, status TEXT, at TEXT, error_code TEXT);
        """)
        self.db.commit()

    def get(self, job_id):
        row = self.db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["history"] = [dict(r) for r in self.db.execute("SELECT request_hash,status,at,error_code FROM events WHERE job_id=? ORDER BY rowid DESC LIMIT 32", (job_id,))][::-1]
        return result

    def has_pending_finalization(self, kind):
        rows = self.db.execute("SELECT job FROM jobs WHERE status='PENDING_FINALIZATION'")
        return any(json.loads(row["job"])["job_type"] == kind for row in rows)

    def pending_wake_times(self, now):
        rows = self.db.execute("""
            SELECT meta.value FROM jobs JOIN meta ON meta.key = 'finalization:' || jobs.job_id
            WHERE jobs.status='PENDING_FINALIZATION'
        """)
        saved = [json.loads(row["value"]) for row in rows]
        times = [at for item in saved for at in (item["grace_until"], item.get("retry_at", 0)) if at > now]
        return bool(saved), times

    def for_issue(self, number):
        row = self.db.execute("SELECT job_id FROM jobs WHERE issue_number=? AND started_at IS NOT NULL ORDER BY rowid DESC LIMIT 1", (number,)).fetchone()
        return self.get(row["job_id"]) if row else None

    def observe(self, number, request_hash, job, *, initially_edited=False):
        seen = self.db.execute("SELECT * FROM issues WHERE number=?", (number,)).fetchone()
        edited = bool((seen and (seen["edited"] or seen["request_hash"] != request_hash)) or (not seen and initially_edited))
        self.db.execute("INSERT OR REPLACE INTO issues VALUES(?,?,?)", (number, request_hash, int(edited)))
        old = self.get(job["job_id"])
        if old and old["issue_number"] != number:
            self.db.commit()
            return None  # 未実行でも、別Issueへのjob_idの移し替えは認めない。
        if old and (old["issue_number"] != number or old["request_hash"] != request_hash):
            if old["status"] in TERMINAL or old["status"] in {"RUNNING", "PENDING_FINALIZATION"}:
                self.db.commit()
                return None  # 一度実行したjob_idは別本文/別Issueで再使用できない。
            self.db.execute("UPDATE jobs SET request_hash=?,job=?,status='WAIT_APPROVAL',edited=1 WHERE job_id=?", (request_hash, canonical(job), job["job_id"]))
            self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job["job_id"], request_hash, "WAIT_APPROVAL", utcnow().isoformat(), "REQUEST_CHANGED"))
        elif not old:
            self.db.execute("INSERT INTO jobs(job_id,issue_number,request_hash,job,status,edited) VALUES(?,?,?,?,?,?)", (job["job_id"], number, request_hash, canonical(job), "QUEUED", int(edited)))
            self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job["job_id"], request_hash, "QUEUED", utcnow().isoformat(), None))
        self.db.commit()
        return self.get(job["job_id"])

    def set(self, job_id, status, *, error=None, result=None, finalization=None):
        now = utcnow().isoformat()
        before = self.get(job_id)
        # 終端・履歴・復旧バリアは、電源断時にも全部保存または全部rollbackとなる。
        with self.db:
            if before["status"] != status or before["error_code"] != error:
                self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job_id, before["request_hash"], status, now, error))
            self.db.execute("UPDATE jobs SET status=?,error_code=?,heartbeat_at=?,started_at=CASE WHEN ?='RUNNING' THEN COALESCE(started_at,?) ELSE started_at END,finished_at=CASE WHEN ? IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED') THEN ? ELSE finished_at END,result=? WHERE job_id=?",
                            (status, error, now, status, now, status, now, canonical(result or {}), job_id))
            if finalization is not None:
                self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)",
                                ("finalization:" + job_id, canonical(finalization)))
            if status in TERMINAL:
                self.db.execute("DELETE FROM meta WHERE key=?", ("finalization:" + job_id,))
            if before["started_at"] and status in TERMINAL - {"SUCCEEDED"} and json.loads(before["job"])["job_type"] == "issue16_acceptance":
                self.db.execute("INSERT OR REPLACE INTO meta VALUES('recovery_required','1')")
        return self.get(job_id)

    def meta(self, key, value=None):
        if value is not None:
            self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(value)))
            self.db.commit()
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def recover(self):
        rows = self.db.execute("SELECT * FROM jobs WHERE status='RUNNING'").fetchall()
        for row in rows:
            self.set(row["job_id"], "INTERRUPTED", error="WORKER_RESTARTED")
        return [self.get(row["job_id"]) for row in rows]

    def all(self):
        return [self.get(row["job_id"]) for row in self.db.execute("SELECT job_id FROM jobs ORDER BY rowid")]
