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

    def observe(self, number, request_hash, job, *, initially_edited=False):
        seen = self.db.execute("SELECT * FROM issues WHERE number=?", (number,)).fetchone()
        edited = bool((seen and (seen["edited"] or seen["request_hash"] != request_hash)) or (not seen and initially_edited))
        self.db.execute("INSERT OR REPLACE INTO issues VALUES(?,?,?)", (number, request_hash, int(edited)))
        old = self.get(job["job_id"])
        if old and old["issue_number"] != number:
            self.db.commit()
            return None  # 未実行でも、別Issueへのjob_idの移し替えは認めない。
        if old and (old["issue_number"] != number or old["request_hash"] != request_hash):
            if old["status"] in TERMINAL or old["status"] == "RUNNING":
                self.db.commit()
                return None  # 一度実行したjob_idは別本文/別Issueで再使用できない。
            self.db.execute("UPDATE jobs SET request_hash=?,job=?,status='WAIT_APPROVAL',edited=1 WHERE job_id=?", (request_hash, canonical(job), job["job_id"]))
            self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job["job_id"], request_hash, "WAIT_APPROVAL", utcnow().isoformat(), "REQUEST_CHANGED"))
        elif not old:
            self.db.execute("INSERT INTO jobs(job_id,issue_number,request_hash,job,status,edited) VALUES(?,?,?,?,?,?)", (job["job_id"], number, request_hash, canonical(job), "QUEUED", int(edited)))
            self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job["job_id"], request_hash, "QUEUED", utcnow().isoformat(), None))
        self.db.commit()
        return self.get(job["job_id"])

    def set(self, job_id, status, *, error=None, result=None):
        now = utcnow().isoformat()
        before = self.get(job_id)
        if before["status"] != status or before["error_code"] != error:
            self.db.execute("INSERT INTO events VALUES(?,?,?,?,?)", (job_id, before["request_hash"], status, now, error))
        self.db.execute("UPDATE jobs SET status=?,error_code=?,heartbeat_at=?,started_at=CASE WHEN ?='RUNNING' THEN COALESCE(started_at,?) ELSE started_at END,finished_at=CASE WHEN ? IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED') THEN ? ELSE finished_at END,result=? WHERE job_id=?",
                        (status, error, now, status, now, status, now, canonical(result or {}), job_id))
        self.db.commit()
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
            job = json.loads(row["job"])
            self.set(row["job_id"], "INTERRUPTED", error="WORKER_RESTARTED")
            if job["job_type"] == "issue16_acceptance":
                self.meta("recovery_required", "1")
        return [self.get(row["job_id"]) for row in rows]

    def all(self):
        return [self.get(row["job_id"]) for row in self.db.execute("SELECT job_id FROM jobs ORDER BY rowid")]
