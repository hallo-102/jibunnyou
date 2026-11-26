"""非特権spoolを信用せず、GitHubから再取得してローカル承認を再検証する。"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import time

from .config import load_policy
from .checkpoint import SmokeInterrupted, run_smoke
from .files import atomic_json, beneath, single_worker, spool_json, spool_page
from .github import REQUEST_TIMEOUT, Unavailable
from .report import record
from .schema import TERMINAL, Rejected, authorized, request, timestamp, utcnow
from .state import State

OUTAGE_LIMIT = 300
AUTHENTICATED_INTERVAL = 60
ANONYMOUS_INTERVAL = 120


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
        pending = "outbox_pending:" + str(number)
        try:
            atomic_json(beneath(self.spool / "outbox", self.spool / "outbox" / f"{number}.json"), record(self.config, row))
        except OSError:
            # 結果は既にSQLiteに保存済み。端末状態の配信を計算の停止条件にしない。
            self.state.meta(pending, "1")
        else:
            if self.state.meta(pending) == "1":
                self.state.meta(pending, "0")

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
                last_good = time.monotonic()
                row = self.state.set(job["job_id"], "RUNNING")
                self.publish(row)
                deadline = last_good + policy["limits"]["timeout_seconds"]
                interval = AUTHENTICATED_INTERVAL if getattr(self.github, "token", None) else ANONYMOUS_INTERVAL
                next_check = last_good + interval

                def check_local():
                    if self.stop_requested():
                        raise Abort("INTERRUPTED", "WORKER_STOP_REQUESTED")
                    if timestamp(job["expires_at"]) <= utcnow():
                        raise Abort("CANCELLED", "EXPIRED")
                    if job["job_type"] == "issue16_acceptance" and not policy["execution_window_jst"]["start"] <= utcnow().astimezone(timezone(timedelta(hours=9))).strftime("%H:%M") < policy["execution_window_jst"]["end"]:
                        raise Abort("INTERRUPTED", "APPROVED_WINDOW_ENDED")
                    if time.monotonic() >= deadline:
                        raise Abort("FAILED", "TIMEOUT")

                def record_failure(exc, phase):
                    details = dict(exc.diagnostic(phase), issue_number=number, job_id=job["job_id"])
                    self.state.meta("github_failure:" + job["job_id"], json.dumps(details))
                    self.state.meta("retry_at", exc.retry_at)
                    print(json.dumps(details), file=sys.stderr)

                def recheck(*, final=False):
                    nonlocal last_good, next_check
                    check_local()
                    now = time.monotonic()
                    if not final and now < next_check:
                        return
                    # The regular schedule can wait for a known rate reset, up to the
                    # same monotonic deadline used for failures returned by the API.
                    phase = "final_issue_recheck" if final else "running_issue_recheck"
                    grace_deadline = last_good + OUTAGE_LIMIT
                    rate_reset = getattr(self.github, "rate_reset_at", 0)
                    wall_now = time.time()
                    if not final and rate_reset > wall_now:
                        exc = Unavailable("GITHUB_RATE_LIMIT", rate_reset, rate_limited=True)
                        record_failure(exc, phase)
                        next_check = min(grace_deadline, now + rate_reset - wall_now)
                    elif now < grace_deadline:
                        try:
                            remaining = min(grace_deadline, deadline) - now
                            timeout = min(REQUEST_TIMEOUT, remaining)
                            current = self.github.issue(number, timeout=timeout)
                        except Unavailable as exc:
                            record_failure(exc, phase)
                            check_local()
                            if exc.code == "GITHUB_AUTH_OR_PERMISSION":
                                raise Abort("FAILED", exc.code) from None
                            temporary = exc.code in {"GITHUB_UNAVAILABLE", "GITHUB_BACKOFF", "GITHUB_RATE_LIMIT"} or (
                                exc.code == "GITHUB_HTTP_ERROR" and
                                (exc.http_status == 408 or 500 <= (exc.http_status or 0) <= 599)
                            )
                            if final or not temporary:
                                raise Abort("FAILED", "GITHUB_UNAVAILABLE") from None
                            now = time.monotonic()
                            next_check = min(grace_deadline, now + max(1, exc.retry_at - time.time()))
                        else:
                            if current.get("state") == "closed":
                                raise Abort("CANCELLED", "ISSUE_CLOSED")
                            check_local()
                            try:
                                same_request = current.get("state") == "open" and request(current)[1] == request_hash
                            except Rejected:
                                same_request = False
                            if not same_request:
                                raise Abort("CANCELLED", "ISSUE_REQUEST_CHANGED")
                            now = time.monotonic()
                            if now < grace_deadline:
                                last_good = now
                                next_check = now + interval
                                if not final:
                                    self.publish(self.state.set(job["job_id"], "RUNNING"))
                                return
                    if now >= grace_deadline:
                        raise Abort("FAILED", "GITHUB_RECHECK_TIMEOUT")

                def control():
                    recheck()

                try:
                    if job["job_type"] == "checkpoint_smoke":
                        result = run_smoke(self.config, self.state, row, job, control)
                    else:
                        result = self.backend.run(job, policy, control)
                    recheck(final=True)
                except Abort as exc:
                    row = self.state.set(job["job_id"], exc.state, error=exc.code)
                except SmokeInterrupted:
                    row = self.state.set(job["job_id"], "INTERRUPTED", error="SMOKE_CHECKPOINT_INTERRUPTED")
                except (Exception, KeyboardInterrupt) as exc:
                    code = "GITHUB_UNAVAILABLE" if isinstance(exc, Unavailable) else "HANDLER_FAILED"
                    if job["job_type"] in {"ab_comparison", "checkpoint_smoke"} and isinstance(exc, Rejected):
                        code = str(exc)
                    status = "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED"
                    row = self.state.set(job["job_id"], status, error=code)
                else:
                    error = result.get("error_code") if job["job_type"] in {"full_feature_audit", "ab_comparison"} else None
                    status = "FAILED" if error else "SUCCEEDED"
                    row = self.state.set(job["job_id"], status, error=error, result=result)
                self.publish(row)
                if self.state.meta("recovery_required") == "1":
                    return
