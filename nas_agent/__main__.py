"""NAS管理者用CLI。自分自身のコードや本番設定を更新しない。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .backend import AGENT_CODE, Backend, clean_environment
from .checkpoint import export_smoke
from .config import load_config, load_policy
from .files import atomic_json, single_worker, validate_admin_file
from .github import GitHub, Unavailable
from .poller import Poller
from .schema import REPOSITORY, Rejected, request, timestamp, utcnow
from .state import State
from .worker import Worker


def verify_install(config):
    """起動時はローカルの導入済み版を検証するだけ。fetch/pullしない。"""
    revision = config["agent_commit"]
    git = ["git", "-c", f"safe.directory={AGENT_CODE}", "-C", str(AGENT_CODE)]
    env = clean_environment(Path(config["state_dir"]) / "git-home")
    head = subprocess.check_output([*git, "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True, env=env).strip()
    dirty = subprocess.check_output([*git, "status", "--porcelain"], stderr=subprocess.DEVNULL, text=True, env=env).strip()
    if head != revision or dirty:
        raise Rejected("AGENT_INSTALL_NOT_REVIEWED_AND_CLEAN")


def admin_approve(config, github, number, expected=None):
    policy = load_policy(config["policy_file"])
    issue = github.issue(number)
    job, request_hash = request(issue)
    if not github.identity_allowed(issue, policy["requester_ids"]):
        raise Rejected("REQUESTER_NOT_AUTHORIZED")
    if expected is None:
        return {"issue_number": number, "requester_id": issue["user"]["id"], "request_hash": request_hash, "job": job}
    if expected != request_hash or issue["state"] != "open" or timestamp(job["expires_at"]) <= utcnow():
        raise Rejected("APPROVAL_REQUEST_CHANGED_OR_EXPIRED")
    # 上位のcommit/type/input範囲外は個別承認を追加しても実行されない。
    validate_admin_file(config["policy_file"], for_write=True)
    with single_worker(Path(config["policy_file"]).parent / ".approval.lock"):
        policy = load_policy(config["policy_file"])
        if request_hash not in policy["approved_requests"]:
            policy["approved_requests"].append(request_hash)
            group = Path(config["policy_file"]).stat().st_gid if os.name == "posix" else None
            atomic_json(config["policy_file"], policy, mode=0o640, gid=group)
    return {"approved_request_hash": request_hash, "job_id": job["job_id"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["poll", "worker", "inspect", "approve", "status", "verify-github", "ack-interrupted", "export-smoke"])
    parser.add_argument("--config", type=Path)
    parser.add_argument("--policy-file", type=Path)
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--agent-commit")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--issue", type=int)
    parser.add_argument("--expected-hash")
    parser.add_argument("--confirm-build-idle", action="store_true")
    parser.add_argument("--job-id")
    parser.add_argument("--export-dir", type=Path)
    args = parser.parse_args()
    if args.command in {"inspect", "approve"}:
        # 管理者用経路。worker設定/私有state/tokenを読まず、管理者自身のread tokenを使う。
        if not args.policy_file or not args.policy_file.is_absolute() or not args.agent_commit or not args.issue or (args.command == "approve" and not args.expected_hash):
            raise Rejected("ADMIN_POLICY_COMMIT_ISSUE_REQUIRED")
        config = {"policy_file": args.policy_file, "agent_commit": args.agent_commit, "state_dir": args.policy_file.parent}
        load_policy(args.policy_file)
        if args.command == "approve":
            validate_admin_file(args.policy_file, for_write=True)
        verify_install(config)
        github = GitHub(args.token_file)
        print(json.dumps(admin_approve(config, github, args.issue, args.expected_hash if args.command == "approve" else None), ensure_ascii=False))
        return
    if not args.config:
        raise Rejected("CONFIG_REQUIRED")
    role = "poller" if args.command == "poll" else "worker"
    if args.command in {"status", "verify-github"}:
        from .files import private_json
        role = private_json(args.config)["role"]
    config = load_config(args.config, role)
    verify_install(config)
    if args.command == "export-smoke":
        if not args.job_id or not args.export_dir:
            raise Rejected("EXPORT_REQUIRES_JOB_ID_AND_DIRECTORY")
        state = State(Path(config["state_dir"]) / "worker.sqlite")
        print(export_smoke(config, state, args.job_id, args.export_dir))
        return
    if args.command == "status":
        state = State(Path(config["state_dir"]) / ("worker.sqlite" if role == "worker" else "poller.sqlite"))
        rows = state.all()
        for row in rows:
            failure = state.meta("github_failure:" + row["job_id"])
            if failure:
                row["github_failure"] = json.loads(failure)
        report_failure = state.meta("last_report_failure")
        print(json.dumps({"jobs": rows, "last_poll_at": state.meta("last_poll_at"), "last_error": state.meta("last_error"),
                          "last_rejection": state.meta("last_rejection"), "recovery_required": state.meta("recovery_required"),
                          "last_report_failure": json.loads(report_failure) if report_failure else None}, ensure_ascii=False))
        return
    if args.command == "ack-interrupted":
        if not args.confirm_build_idle:
            raise Rejected("ADMIN_MUST_CONFIRM_BUILD_IDLE")
        with single_worker(Path(config["state_dir"]) / "worker.lock"):
            if not Backend(config).no_orphans():
                raise Rejected("ORPHANS_REMAIN")
            State(Path(config["state_dir"]) / "worker.sqlite").meta("recovery_required", "0")
        print("Recovery barrier cleared; interrupted jobs will not be rerun.")
        return
    github = GitHub(config.get("token_file"))
    local_state = State(Path(config["state_dir"]) / ("poller.sqlite" if role == "poller" else "worker.sqlite"))
    github.retry_at = float(local_state.meta("retry_at") or 0)
    if role == "poller" and not github.token:
        raise Rejected("POLLER_TOKEN_REQUIRED")
    if args.command == "verify-github":
        repo = github.api(f"/repos/{REPOSITORY}")
        github.api(f"/repos/{REPOSITORY}/issues?per_page=1")
        github.api(f"/repos/{REPOSITORY}/commits/3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92")
        print(json.dumps({"repository": repo["full_name"], "private": repo["private"], "contents_read": True,
                          "issues_read": True, "issues_write": "UNVERIFIED_UNTIL_FIRST_REPORT"}))
        return
    backend = Backend(config, github.token) if role == "worker" else None
    stopped = [False]
    signal.signal(signal.SIGTERM, lambda *_: stopped.__setitem__(0, True))
    signal.signal(signal.SIGINT, lambda *_: stopped.__setitem__(0, True))
    component = Poller(config, github) if role == "poller" else Worker(config, github, backend)
    if role == "worker":
        component.stop_requested = lambda: stopped[0]
    while not stopped[0]:
        try:
            component.cycle()
            component.state.meta("last_error", "")
        except Unavailable as exc:
            component.state.meta("last_error", exc.code)
            component.state.meta("retry_at", exc.retry_at)
            if not args.serve:
                raise
            print(json.dumps(exc.diagnostic(role + "_cycle")), file=sys.stderr)
        if not args.serve:
            return
        now = time.time()
        deadline = now + config.get("poll_seconds", 60)
        pending, wake_times = component.state.pending_wake_times(now) if role == "worker" else (False, [])
        deadline = min(deadline, *wake_times) if wake_times else deadline
        if not pending:
            deadline = max(deadline, github.retry_at)
        while time.time() < deadline and not stopped[0]:
            time.sleep(min(1, max(0, deadline - time.time())))


if __name__ == "__main__":
    try:
        main()
    except (Rejected, Unavailable) as exc:
        print(json.dumps({"error_code": str(exc)}), file=sys.stderr)
        sys.exit(1)
    except Exception:
        print('{"error_code":"LOCAL_AGENT_ERROR"}', file=sys.stderr)
        sys.exit(1)
