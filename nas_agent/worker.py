"""非特権spoolを信用せず、GitHubから再取得してローカル承認を再検証する。"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import time

from .config import load_policy
from .files import atomic_json, beneath, single_worker, spool_json, spool_page
from .github import Unavailable
from .report import record
from .schema import TERMINAL, Rejected, authorized, request, timestamp, utcnow
from .state import State


class Abort(RuntimeError):
    def __init__(self, state, code):
        self.state, self.code = state, code
        super().__init__(code)


class Worker:
    def __init__(self, config, github, backend):
        self.config, self.github, self.backend = config, github, backend
        self.state = State(Path(config["state_dir"]) / "worker.sqlite")
        self.spool = Path(config["spool_dir"])
        self.stop_requested = lambda: False

    def publish(self, row):
        number = row["issue_number"]
        atomic_json(beneath(self.spool / "outbox", self.spool / "outbox" / f"{number}.json"), record(self.config, row))

    def cycle(self):
        with single_worker(Path(self.config["state_dir"]) / "worker.lock"):
            for row in self.state.recover():
                self.publish(row)
            for row in self.state.all():
                if row["status"] in TERMINAL:
                    self.publish(row)  # 送信/ディスク一時失敗後も実行せず、保存結果だけを再発行する。
            policy = load_policy(self.config["policy_file"])
            if self.state.meta("recovery_required") == "1":
                return  # 副作用ジョブの電源断後は管理者確認まで新規実行しない。
            inbox = self.spool / "inbox"
            if not inbox.exists():
                return
            for path in spool_page(inbox, self.state, "inbox_cursor"):
                policy = load_policy(self.config["policy_file"])
                if not path.stem.isdigit() or path.is_symlink():
                    continue
                if path.stat().st_size > 2048:
                    continue
                try:
                    hint = spool_json(inbox, path, 2048)
                except (ValueError, OSError):
                    continue
                number = int(path.stem)
                if hint.get("issue_number") != number:
                    continue
                issue = self.github.issue(number)
                try:
                    job, request_hash = request(issue)
                except Rejected:
                    continue  # 非特権pollerが不正Issueを渡しても実行しない。
                if not self.github.identity_allowed(issue, policy["requester_ids"]):
                    continue
                row = self.state.observe(number, request_hash, job, initially_edited=issue.get("created_at") != issue.get("updated_at"))
                if row is None:
                    self.state.meta("last_rejection", json.dumps({"issue_number": number, "error_code": "JOB_ID_ALREADY_USED"}))
                    continue
                if row["status"] in TERMINAL:
                    continue
                self.publish(row)
                if issue["state"] == "closed":
                    self.publish(self.state.set(job["job_id"], "CANCELLED", error="ISSUE_CLOSED"))
                    continue
                if timestamp(job["expires_at"]) <= utcnow():
                    self.publish(self.state.set(job["job_id"], "CANCELLED", error="EXPIRED"))
                    continue
                if not authorized(policy, job, request_hash, issue["user"]["id"], edited=bool(row["edited"])):
                    self.publish(self.state.set(job["job_id"], "WAIT_APPROVAL", error="LOCAL_APPROVAL_REQUIRED"))
                    continue
                if job["job_type"] == "issue16_acceptance":
                    if not policy.get("allow_build", False):
                        self.publish(self.state.set(job["job_id"], "WAIT_APPROVAL", error="BUILD_REVIEW_REQUIRED"))
                        continue
                    window = policy.get("execution_window_jst")
                    now = utcnow().astimezone(timezone(timedelta(hours=9))).strftime("%H:%M")
                    if not window or not window["start"] <= now < window["end"]:
                        self.publish(self.state.set(job["job_id"], "QUEUED", error="OUTSIDE_APPROVED_WINDOW"))
                        continue
                    hour, minute = map(int, window["end"].split(":"))
                    local = utcnow().astimezone(timezone(timedelta(hours=9)))
                    if (local.replace(hour=hour, minute=minute, second=0, microsecond=0) - local).total_seconds() < policy["limits"]["timeout_seconds"]:
                        self.publish(self.state.set(job["job_id"], "QUEUED", error="INSUFFICIENT_WINDOW_REMAINING"))
                        continue
                # 最後のGETと承認照合を実行直前にも行う。以後jobはこのローカル値で固定する。
                live = self.github.issue(number)
                if live["state"] == "closed" or request(live)[1] != request_hash:
                    continue
                self.publish(self.state.set(job["job_id"], "RUNNING"))
                last_check = [0.0]
                deadline = time.monotonic() + policy["limits"]["timeout_seconds"]

                def control():
                    if self.stop_requested():
                        raise Abort("INTERRUPTED", "WORKER_STOP_REQUESTED")
                    if timestamp(job["expires_at"]) <= utcnow():
                        raise Abort("CANCELLED", "EXPIRED")
                    if job["job_type"] == "issue16_acceptance" and not policy["execution_window_jst"]["start"] <= utcnow().astimezone(timezone(timedelta(hours=9))).strftime("%H:%M") < policy["execution_window_jst"]["end"]:
                        raise Abort("INTERRUPTED", "APPROVED_WINDOW_ENDED")
                    if time.monotonic() >= deadline:
                        raise Abort("FAILED", "TIMEOUT")
                    if time.monotonic() - last_check[0] >= 15:
                        current = self.github.issue(number)
                        if current["state"] == "closed":
                            raise Abort("CANCELLED", "ISSUE_CLOSED")
                        # 編集で実行パラメータを変えない。新しい本文は次の依頼として再承認が必要。
                        last_check[0] = time.monotonic()
                        self.publish(self.state.set(job["job_id"], "RUNNING"))

                try:
                    result = self.backend.run(job, policy, control)
                    control()
                except Abort as exc:
                    row = self.state.set(job["job_id"], exc.state, error=exc.code)
                except (Exception, KeyboardInterrupt) as exc:
                    code = "GITHUB_UNAVAILABLE" if isinstance(exc, Unavailable) else "HANDLER_FAILED"
                    status = "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED"
                    row = self.state.set(job["job_id"], status, error=code)
                else:
                    error = result.get("error_code") if job["job_type"] == "full_feature_audit" else None
                    status = "FAILED" if error else "SUCCEEDED"
                    row = self.state.set(job["job_id"], status, error=error, result=result)
                self.publish(row)
                if self.state.meta("recovery_required") == "1":
                    return
