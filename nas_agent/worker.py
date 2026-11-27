"""非特権spoolを信用せず、GitHubから再取得してローカル承認を再検証する。"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import time

from .artifacts import METADATA, bundle, checksums, checksum_text, handoff
from .comparison import sha256
from .config import load_policy
from .checkpoint import SmokeInterrupted, run_smoke
from .files import atomic_json, beneath, single_worker, spool_json, spool_page
from .github import REQUEST_TIMEOUT, Unavailable
from .report import record
from .schema import INPUT_JOBS, TERMINAL, Rejected, authorized, digest, request, timestamp, utcnow
from .state import State

OUTAGE_LIMIT = 300
AUTHENTICATED_INTERVAL = 60
ANONYMOUS_INTERVAL = 120


class Abort(RuntimeError):
    def __init__(self, state, code):
        self.state, self.code = state, code
        super().__init__(code)


def temporary(exc):
    return exc.code in {"GITHUB_UNAVAILABLE", "GITHUB_BACKOFF", "GITHUB_RATE_LIMIT"} or (
        exc.code == "GITHUB_HTTP_ERROR" and (exc.http_status == 408 or 500 <= (exc.http_status or 0) <= 599)
    )


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

    def check_current_approval(self, job, row, live, input_pin, *, running):
        status = "INTERRUPTED" if running else "WAIT_APPROVAL"
        try:
            current_policy = load_policy(self.config["policy_file"])
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise Abort(status, "LOCAL_APPROVAL_UNVERIFIABLE") from exc
        if job["job_type"] in INPUT_JOBS:
            input_id = job["input_id"]
            if current_policy["inputs"].get(input_id) != input_pin:
                raise Abort("INTERRUPTED", "APPROVED_INPUT_CHANGED")
        if not authorized(current_policy, job, row["request_hash"], live["user"]["id"], edited=bool(row["edited"])):
            code = "LOCAL_APPROVAL_REVOKED" if running else "LOCAL_APPROVAL_REQUIRED"
            raise Abort(status, code)
        if job["job_type"] == "issue16_acceptance":
            if not current_policy.get("allow_build", False):
                raise Abort(status, "BUILD_REVIEW_REQUIRED")
            window = current_policy.get("execution_window_jst")
            now = utcnow().astimezone(timezone(timedelta(hours=9))).strftime("%H:%M")
            if not window or not window["start"] <= now < window["end"]:
                raise Abort(status, "APPROVED_WINDOW_ENDED")

        return current_policy

    def record_failure(self, row, exc, phase):
        job_id = row["job_id"]
        details = dict(exc.diagnostic(phase), issue_number=row["issue_number"], job_id=job_id)
        self.state.meta("github_failure:" + job_id, json.dumps(details))
        self.state.meta("retry_at", exc.retry_at)
        try:
            root = Path(self.config["state_dir"]) / "recovery" / job_id
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            atomic_json(beneath(root, root / "latest-github-diagnostic.json"), details, mode=0o600)
        except (OSError, Rejected):
            pass  # The ledger already holds the diagnostic for bundle creation.
        print(json.dumps(details), file=sys.stderr)

    def deliver(self, row, policy, *, exchange=True, pending_handoff=False):
        job_id = row["job_id"]
        key = "artifact_delivery:" + job_id
        try:
            diagnostic = json.loads(self.state.meta("github_failure:" + job_id) or "null")
            source = bundle(self.config, row, diagnostic, getattr(self.github, "token", None),
                            pending_handoff=pending_handoff)
            if exchange and policy.get("artifact_exchange"):
                revision = digest([row["status"], policy["artifact_exchange"]])
                receipt = f"artifact_delivery_receipt:{job_id}:{row['status']}"
                if self.state.meta(receipt) != revision:
                    handoff(self.config, policy, row, source)
                    self.state.meta(receipt, revision)
        except (OSError, Rejected):
            self.state.meta(key, "ARTIFACT_DELIVERY_FAILED")
            print(json.dumps({"job_id": job_id, "code": "ARTIFACT_DELIVERY_FAILED"}), file=sys.stderr)
            return None
        if self.state.meta(key):
            self.state.meta(key, "")
        return source

    @staticmethod
    def verify_saved_artifacts(source, sums, saved, result):
        hashes = checksums(source)
        if (sha256(sums) != saved["bundle_sha256"]
                or sums.read_bytes() != checksum_text(source, hashes=hashes).encode("ascii")
                or any(hashes.get(name) != value for name, value in result.get("artifacts_sha256", {}).items())):
            raise Abort("FAILED", "SAVED_ARTIFACT_SHA_MISMATCH")
        return {name: value for name, value in hashes.items() if name not in METADATA}

    def deliver_pending(self, row, policy, source, sums, saved, result):
        exchange = policy.get("artifact_exchange")
        job = json.loads(row["job"])
        if not exchange or not exchange["jobs"].get(job["job_type"]):
            return
        policy = self.check_current_approval(job, row, {"user": {"id": saved["requester_id"]}},
                                             saved["input"], running=True)
        if policy.get("artifact_exchange") != exchange:
            return
        receipt = f"artifact_delivery_receipt:{row['job_id']}:{row['status']}"
        if self.state.meta(receipt) != digest([row["status"], exchange]):
            self.verify_saved_artifacts(source, sums, saved, result)
            self.deliver(row, policy, pending_handoff=True)

    def finalize(self, row, policy):
        job = json.loads(row["job"])
        job_id = row["job_id"]
        key = "finalization:" + job_id
        saved = json.loads(self.state.meta(key))
        result = json.loads(row["result"])
        source = self.deliver(row, policy, exchange=False)
        if source is None:
            return
        sums = source / "SHA256SUMS"
        now = time.time()
        try:
            if sums.is_symlink() or not sums.is_file():
                raise Abort("FAILED", "SAVED_ARTIFACT_SHA_MISMATCH")
            if "bundle_sha256" not in saved:
                saved["bundle_sha256"] = sha256(sums)
                self.state.meta(key, json.dumps(saved))
            if timestamp(job["expires_at"]) <= utcnow():
                raise Abort("CANCELLED", "EXPIRED")
            if self.stop_requested():
                self.publish(row)
                return
            if now >= saved["grace_until"] and row["error_code"] != "GITHUB_RECHECK_TIMEOUT":
                row = self.state.set(job_id, "PENDING_FINALIZATION", error="GITHUB_RECHECK_TIMEOUT", result=result)
                self.deliver_pending(row, policy, source, sums, saved, result)
                self.publish(row)
                return
            if row["error_code"] == "GITHUB_RECHECK_TIMEOUT":
                self.deliver_pending(row, policy, source, sums, saved, result)
            if now < saved.get("retry_at", 0):
                self.publish(row)
                return
            remaining = saved["grace_until"] - now
            timeout = min(REQUEST_TIMEOUT, remaining) if remaining > 0 else REQUEST_TIMEOUT
            live = self.github.issue(row["issue_number"], timeout=timeout)
            if live.get("state") == "closed":
                raise Abort("CANCELLED", "ISSUE_CLOSED")
            try:
                same_request = live.get("state") == "open" and request(live)[1] == row["request_hash"]
            except Rejected:
                same_request = False
            if not same_request:
                raise Abort("CANCELLED", "ISSUE_REQUEST_CHANGED")
            self.check_current_approval(job, row, live, saved["input"], running=True)
            if job["job_type"] in INPUT_JOBS and hasattr(self.backend, "verify_input"):
                try:
                    self.backend.verify_input(job, saved["input"], result)
                except (OSError, ValueError, KeyError, TypeError):
                    raise Abort("INTERRUPTED", "APPROVED_INPUT_CHANGED") from None
            hashes = self.verify_saved_artifacts(source, sums, saved, result)
            if self.stop_requested():
                self.publish(row)
                return
            if timestamp(job["expires_at"]) <= utcnow():
                raise Abort("CANCELLED", "EXPIRED")
            policy = self.check_current_approval(job, row, live, saved["input"], running=True)
        except Unavailable as exc:
            self.record_failure(row, exc, "final_issue_recheck")
            if temporary(exc):
                now = time.time()
                retry = max(now + 1, exc.retry_at)
                saved["retry_at"] = retry
                error = "GITHUB_RECHECK_TIMEOUT" if now >= saved["grace_until"] else exc.code
                row = self.state.set(job_id, "PENDING_FINALIZATION", error=error, result=result, finalization=saved)
                self.publish(row)
                return
            row = self.state.set(job_id, "FAILED", error=exc.code)
        except Abort as exc:
            row = self.state.set(job_id, exc.state, error=exc.code)
        except OSError:
            self.publish(row)
            return  # A temporary read failure leaves the saved calculation pending.
        else:
            error = result.get("error_code")
            result.pop("input_sha256", None)
            result["artifacts_sha256"] = hashes
            row = self.state.set(job_id, "FAILED" if error else "SUCCEEDED", error=error, result=result)
        self.deliver(row, policy)
        self.publish(row)

    def cycle(self):
        with single_worker(Path(self.config["state_dir"]) / "worker.lock"):
            self.state.recover()
            try:
                policy = load_policy(self.config["policy_file"])
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                policy = None
            for row in self.state.all():
                if row["status"] in TERMINAL:
                    if row["started_at"] or row["status"] in {"FAILED", "INTERRUPTED"}:
                        self.deliver(row, policy or {}, exchange=policy is not None)
                    self.publish(row)
                elif row["status"] == "PENDING_FINALIZATION":
                    self.finalize(row, policy or {})
            if policy is None:
                return
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
                prior = self.state.for_issue(number)
                if prior and prior["status"] == "PENDING_FINALIZATION":
                    continue
                try:
                    issue = self.github.issue(number)
                except Unavailable as exc:
                    if prior:
                        self.record_failure(prior, exc, "intake_issue_recheck")
                    raise
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
                if row["status"] in TERMINAL | {"PENDING_FINALIZATION"}:
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
                    if self.state.has_pending_finalization(kind="issue16_acceptance"):
                        continue
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
                try:
                    live = self.github.issue(number)
                except Unavailable as exc:
                    self.record_failure(row, exc, "preflight_issue_recheck")
                    raise
                if live["state"] == "closed" or request(live)[1] != request_hash:
                    continue

                last_good = time.monotonic()
                try:
                    self.check_current_approval(job, row, live, policy["inputs"].get(job["input_id"]), running=False)
                    if timestamp(job["expires_at"]) <= utcnow():
                        raise Abort("CANCELLED", "EXPIRED")
                except Abort as exc:
                    self.publish(self.state.set(job["job_id"], exc.state, error=exc.code))
                    continue
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

                def recheck():
                    nonlocal last_good, next_check
                    check_local()
                    now = time.monotonic()
                    if now < next_check:
                        return
                    # The regular schedule can wait for a known rate reset, up to the
                    # same monotonic deadline used for failures returned by the API.
                    phase = "running_issue_recheck"
                    grace_deadline = last_good + OUTAGE_LIMIT
                    rate_reset = getattr(self.github, "rate_reset_at", 0)
                    wall_now = time.time()
                    if rate_reset > wall_now:
                        exc = Unavailable("GITHUB_RATE_LIMIT", rate_reset, rate_limited=True)
                        self.record_failure(row, exc, phase)
                        next_check = min(grace_deadline, now + rate_reset - wall_now)
                    elif now < grace_deadline:
                        try:
                            remaining = min(grace_deadline, deadline) - now
                            timeout = min(REQUEST_TIMEOUT, remaining)
                            current = self.github.issue(number, timeout=timeout)
                        except Unavailable as exc:
                            self.record_failure(row, exc, phase)
                            check_local()
                            if exc.code == "GITHUB_AUTH_OR_PERMISSION":
                                raise Abort("FAILED", exc.code) from None
                            if not temporary(exc):
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
                    row = self.state.set(job["job_id"], "PENDING_FINALIZATION", result=result,
                                         finalization={"input": policy["inputs"].get(job["input_id"]),
                                                       "requester_id": live["user"]["id"],
                                                       "grace_until": time.time() + last_good + OUTAGE_LIMIT - time.monotonic()})
                    self.finalize(row, policy)
                    if self.state.meta("recovery_required") == "1":
                        return
                    continue
                self.deliver(row, policy)
                self.publish(row)
                if self.state.meta("recovery_required") == "1":
                    return
