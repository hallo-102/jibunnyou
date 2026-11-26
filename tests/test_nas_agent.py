"""模擬GitHub→承認→固定handler→永続状態/報告の最小受入。NASへは接続しない。"""
from copy import deepcopy
from datetime import timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error

import pytest

from nas_agent.__main__ import admin_approve
from nas_agent.backend import Backend, Commands, clean_environment
from nas_agent.config import load_config
from nas_agent.files import atomic_json, private_json, single_worker
from nas_agent.github import GitHub, Unavailable
from nas_agent.poller import Poller
from nas_agent.schema import PR17_SHA, REPOSITORY, Rejected, authorized, parse_job, request, utcnow
from nas_agent.worker import Abort, Worker


class MockGitHub(GitHub):
    """既存APIメソッドのパス/状態/コメント冪等性も通すメモリ上のAPI。"""
    def __init__(self):
        super().__init__()
        self.rows = {}
        self.comments = []
        self.lose_post_response = False
        self.offline = False

    def api(self, path, *, method="GET", payload=None, conditional=False, timeout=None):
        if self.offline:
            raise Unavailable()
        if path == "/user":
            return {"id": 900}
        if path == f"/repos/{REPOSITORY}":
            return {"full_name": REPOSITORY, "private": False, "owner": {"id": 42}}
        if "/collaborators/" in path:
            return {"permission": "read", "user": {"id": 43}}
        prefix = f"/repos/{REPOSITORY}/issues"
        if path.startswith(prefix + "?"):
            from urllib.parse import parse_qs
            page = int(parse_qs(path.split("?", 1)[1]).get("page", ["1"])[0])
            return deepcopy(list(self.rows.values())[(page - 1) * 100:page * 100])
        if "/comments?" in path:
            return deepcopy(self.comments)
        if method == "POST" and path.endswith("/comments"):
            comment = {"id": len(self.comments) + 1, "user": {"id": 900}, "body": payload["body"]}
            self.comments.append(comment)
            if self.lose_post_response:
                self.lose_post_response = False
                raise Unavailable()  # GitHub保存成功、応答だけ喪失した場合。
            return deepcopy(comment)
        if method == "PATCH" and "/issues/comments/" in path:
            comment = self.comments[int(path.rsplit("/", 1)[1]) - 1]
            comment["body"] = payload["body"]
            return deepcopy(comment)
        if path.startswith(prefix + "/"):
            return deepcopy(self.rows[int(path.rsplit("/", 1)[1])])
        raise AssertionError("unexpected fixed endpoint")

    def add(self, job, number=31):
        created = utcnow().isoformat()
        self.rows[number] = {"number": number, "title": "[NAS-JOB] " + job["job_id"], "body": json.dumps(job), "state": "open",
                             "user": {"id": 42, "login": "hallo-102"}, "created_at": created, "updated_at": created}


class CountingBackend:
    def __init__(self, config, action=None):
        self.config, self.action, self.calls = config, action, []

    def run(self, job, policy, control):
        self.calls.append(deepcopy(job))
        if self.action:
            return self.action(job, policy, control)
        return Backend(self.config).run(job, policy, control)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    # 模擬API試験では管理者操作を模擬する。実POSIX境界は専用テストで別途検査する。
    monkeypatch.setattr('nas_agent.files.validate_admin_file', lambda *a, **k: None)
    monkeypatch.setattr('nas_agent.__main__.validate_admin_file', lambda *a, **k: None)
    production = tmp_path / "production"
    production.mkdir()
    examples = Path(__file__).resolve().parents[1] / "nas_agent/examples"
    policy = json.loads((examples / "policy.json").read_text())
    policy.update(production_root=str(production), requester_ids=[42], inputs={})
    policy_path = tmp_path / "policy.json"
    atomic_json(policy_path, policy, mode=0o600)
    spool = tmp_path / "spool"
    (spool / "inbox").mkdir(parents=True)
    (spool / "outbox").mkdir()
    config = json.loads((examples / "worker.json").read_text())
    config.update(state_dir=str(tmp_path / "worker"), spool_dir=str(spool), policy_file=str(policy_path),
                  checkout_root=str(tmp_path / "checkouts"), validation_root=str(tmp_path / "validation"), validation_python=sys.executable)
    config_path = tmp_path / "worker.json"
    atomic_json(config_path, config, mode=0o600)
    config = load_config(config_path, "worker")
    poll_config = {**config, "role": "poller", "state_dir": str(tmp_path / "poller")}
    api = MockGitHub()
    job = {"schema_version": 1, "job_id": "health_01", "job_type": "healthcheck", "commit_sha": PR17_SHA,
           "raceday": "20260809", "input_id": "none", "expires_at": (utcnow() + timedelta(hours=8)).isoformat(), "scope": "offline"}
    api.add(job)
    return config, poll_config, api, job, policy_path


def approve(config, api, number=31):
    inspected = admin_approve(config, api, number)
    admin_approve(config, api, number, inspected["request_hash"])


def test_mock_api_approval_real_health_handler_and_duplicate(setup):
    config, poll_config, api, job, _ = setup
    poller = Poller(poll_config, api)
    backend = CountingBackend(config)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "WAIT_APPROVAL"
    assert not backend.calls
    poller.cycle()
    approve(config, api)
    worker.cycle()
    poller.cycle()
    worker.cycle()
    poller.cycle()
    assert len(backend.calls) == 1 and len(api.comments) == 1
    row = worker.state.get(job["job_id"])
    assert row["status"] == "SUCCEEDED"
    assert {r["status"] for r in row["history"]} >= {"QUEUED", "WAIT_APPROVAL", "RUNNING", "SUCCEEDED"}
    assert '"status": "SUCCEEDED"' in api.comments[0]["body"]
    assert "architecture" in api.comments[0]["body"]


def test_duplicate_job_id_cannot_move_to_another_issue_before_execution(setup):
    config, poll_config, api, job, _ = setup
    poller, backend = Poller(poll_config, api), CountingBackend(config)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    api.add(job, number=32)
    approve(config, api, number=32)  # 別Issueのhash承認で先のjobを乗っ取れない。
    poller.cycle()
    worker.cycle()
    assert not backend.calls
    row = worker.state.get(job["job_id"])
    assert row["issue_number"] == 31 and row["status"] == "WAIT_APPROVAL"
    assert "JOB_ID_ALREADY_USED" in worker.state.meta("last_rejection")


@pytest.mark.parametrize("change", [
    {"command": "rm -rf /"}, {"input_id": "../../production"}, {"input_id": "/volume1/prod"},
    {"commit_sha": "main"}, {"job_type": "shell"}, {"job_type": []}, {"scope": "production"},
    {"raceday": "20260230"}, {"expires_at": "2026-11-26"}, {"url": "https://example.com"},
])
def test_strict_schema_rejects_commands_paths_and_unknown_fields(setup, change):
    job = {**setup[3], **change}
    with pytest.raises(Rejected):
        parse_job(json.dumps(job))


def test_duplicate_json_keys_are_not_accepted(setup):
    body = json.dumps(setup[3])[:-1] + ', "scope":"offline"}'
    with pytest.raises(Rejected, match="DUPLICATE"):
        parse_job(body)


def test_unapproved_sha_and_untrusted_requester_never_execute(setup):
    config, poll_config, api, job, _ = setup
    job["commit_sha"] = "b" * 40
    api.add(job)
    approve(config, api)  # 個別ハッシュを承認しても上位の固定commit範囲は緩まない。
    poller, backend = Poller(poll_config, api), CountingBackend(config)
    poller.cycle()
    worker = Worker(config, api, backend)
    worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "WAIT_APPROVAL"
    api.rows[31]["user"] = {"id": 43, "login": "other-user"}
    worker.cycle()
    assert not backend.calls


def test_edited_request_needs_new_hash_even_with_preapproval_grant(setup):
    config, poll_config, api, job, policy_path = setup
    poller, backend = Poller(poll_config, api), CountingBackend(config)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    approve(config, api)
    job["input_id"] = "changed"
    api.rows[31]["body"] = json.dumps(job)
    api.rows[31]["updated_at"] = utcnow().isoformat()
    policy = private_json(policy_path)
    policy["grants"] = [{"job_type": "healthcheck", "commit_sha": PR17_SHA, "input_id": "changed", "from_date": "20260101", "through_date": "20261231", "expires_at": job["expires_at"]}]
    atomic_json(policy_path, policy, mode=0o600)
    poller.cycle()
    worker.cycle()
    assert not backend.calls and worker.state.get(job["job_id"])["edited"]
    with pytest.raises(Rejected, match="CHANGED"):
        admin_approve(config, api, 31, "a" * 64)
    approve(config, api)
    worker.cycle()
    assert len(backend.calls) == 1


def test_new_preapproved_job_and_cross_issue_duplicate_id(setup):
    config, poll_config, api, job, path = setup
    policy = private_json(path)
    policy["grants"] = [{"job_type": "healthcheck", "commit_sha": PR17_SHA, "input_id": "none", "from_date": "20260801", "through_date": "20260831", "expires_at": job["expires_at"]}]
    atomic_json(path, policy, mode=0o600)
    poller, backend = Poller(poll_config, api), CountingBackend(config)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    assert len(backend.calls) == 1
    api.add(job, number=32)
    poller.cycle()
    worker.cycle()
    assert len(backend.calls) == 1


@pytest.mark.parametrize("action,state,error", [("timeout", "FAILED", "TIMEOUT"), ("fail", "FAILED", "HANDLER_FAILED"), ("cancel", "CANCELLED", "ISSUE_CLOSED")])
def test_failure_timeout_and_running_cancel_are_reported(setup, action, state, error):
    config, poll_config, api, job, _ = setup
    approve(config, api)

    def perform(job, policy, control):
        if action == "timeout":
            raise Abort("FAILED", "TIMEOUT")
        if action == "fail":
            raise RuntimeError("secret should never reach GitHub")
        api.rows[31]["state"] = "closed"
        control()

    poller, backend = Poller(poll_config, api), CountingBackend(config, perform)
    poller.cycle()
    worker = Worker(config, api, backend)
    worker.cycle()
    poller.cycle()
    assert worker.state.get(job["job_id"])["status"] == state
    assert error in api.comments[0]["body"]
    assert "secret should" not in api.comments[0]["body"]


@pytest.mark.parametrize("reason", ["expired", "closed"])
def test_expired_and_closed_jobs_do_not_start(setup, reason):
    config, poll_config, api, job, _ = setup
    if reason == "expired":
        job["expires_at"] = (utcnow() - timedelta(seconds=1)).isoformat()
    api.add(job)
    if reason == "closed":
        api.rows[31]["state"] = "closed"
    backend = CountingBackend(config)
    Poller(poll_config, api).cycle()
    worker = Worker(config, api, backend)
    worker.cycle()
    assert not backend.calls and worker.state.get(job["job_id"])["error_code"] == ("EXPIRED" if reason == "expired" else "ISSUE_CLOSED")


def test_api_uses_json_and_never_exposes_http_error_body(tmp_path):
    token = tmp_path / "token"
    token.write_text("fixture-secret-do-not-publish", encoding="utf-8")
    token.chmod(0o600)
    api = GitHub(token)
    class Unauthorized:
        def open(self, request, **kwargs):
            assert request.get_header("Content-type") == "application/json"
            assert request.get_header("Authorization") == "Bearer fixture-secret-do-not-publish"
            raise urllib.error.HTTPError("https://api.github.com", 401, "fixture-secret-do-not-publish", {}, io.BytesIO(b"fixture-secret-do-not-publish"))
    api.opener = Unauthorized()
    with pytest.raises(Unavailable) as result:
        api.api(f"/repos/{REPOSITORY}/issues/31/comments", method="POST", payload={"body": "status"})
    assert "fixture-secret" not in str(result.value)
    assert result.value.diagnostic("report_comment")["http_status"] == 401


def test_power_loss_is_interrupted_not_reexecuted(setup):
    config, poll_config, api, job, path = setup
    job.update(job_type="issue16_acceptance", input_id="fixture")
    api.add(job)
    policy = private_json(path)
    policy["inputs"] = {"fixture": {"root": str(Path(config["state_dir"]).parent / "frozen"), "manifest_sha256": "d" * 64, "raceday": "20260809"}}
    atomic_json(path, policy, mode=0o600)
    worker = Worker(config, api, CountingBackend(config))
    job, request_hash = request(api.rows[31])
    worker.state.observe(31, request_hash, job)
    worker.state.set(job["job_id"], "RUNNING")
    backend = CountingBackend(config)
    restarted = Worker(config, api, backend)
    restarted.cycle()
    assert restarted.state.get(job["job_id"])["status"] == "INTERRUPTED"
    assert restarted.state.meta("recovery_required") == "1" and not backend.calls
    Poller(poll_config, api).cycle()
    assert "INTERRUPTED" in api.comments[0]["body"]


def test_lost_report_response_retries_without_reexecution_or_duplicate_comment(setup):
    config, poll_config, api, job, _ = setup
    approve(config, api)
    poller, backend = Poller(poll_config, api), CountingBackend(config)
    poller.cycle()
    worker = Worker(config, api, backend)
    worker.cycle()
    api.lose_post_response = True
    with pytest.raises(Unavailable):
        poller.cycle()
    assert json.loads(poller.state.meta("last_report_failure"))["phase"] == "report_comment"
    Worker(config, api, backend).cycle()
    poller.cycle()
    assert len(backend.calls) == 1 and len(api.comments) == 1
    assert worker.state.get(job["job_id"])["status"] == "SUCCEEDED"


def test_checkpoint_job_interrupt_resume_and_export(setup, tmp_path):
    from nas_agent.checkpoint import export_smoke

    config, poll_config, api, job, policy_path = setup
    job.update(job_id="codex-smoke-interrupt", job_type="checkpoint_smoke",
               commit_sha=config["agent_commit"], input_id="synthetic-v1", interrupt_after_checkpoint=True)
    policy = private_json(policy_path)
    policy["job_types"].append("checkpoint_smoke")
    policy["approved_commits"] = sorted(set(policy["approved_commits"]) | {config["agent_commit"]})
    policy["grants"] = [{"job_type": "checkpoint_smoke", "commit_sha": job["commit_sha"], "input_id": job["input_id"],
                         "from_date": job["raceday"], "through_date": job["raceday"], "expires_at": job["expires_at"]}]
    atomic_json(policy_path, policy, mode=0o600)
    api.add(job)
    poller, worker = Poller(poll_config, api), Worker(config, api, Backend(config))
    poller.cycle()
    worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "WAIT_APPROVAL"
    approve(config, api)
    worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "INTERRUPTED"
    workspace = Path(config["state_dir"]) / "jobs"
    checkpoint = (workspace / job["job_id"] / "checkpoint.json").read_bytes()
    partial = json.loads(checkpoint)
    assert partial["completed_count"] == 384 and partial["issue_number"] == 31 and partial["started_at"]

    resumed = {key: value for key, value in job.items() if key != "interrupt_after_checkpoint"}
    resumed.update(job_id="codex-smoke-resume", resume_from=job["job_id"],
                   checkpoint_sha256=hashlib.sha256(checkpoint).hexdigest())
    invalid = dict(resumed, job_id="codex-smoke-rejected", checkpoint_sha256="0" * 64)
    changed = dict(resumed, job_id="codex-smoke-changed", raceday="20260810")
    baseline = {key: value for key, value in resumed.items() if key not in {"resume_from", "checkpoint_sha256"}}
    baseline["job_id"] = "codex-smoke-baseline"
    api.add(resumed, number=32)
    api.add(invalid, number=33)
    api.add(baseline, number=34)
    api.add(changed, number=35)
    poller.cycle()
    worker.cycle()
    assert worker.state.get(resumed["job_id"])["status"] == "WAIT_APPROVAL"
    for number in (32, 33, 34, 35):
        approve(config, api, number)
    worker.cycle()
    assert worker.state.get(resumed["job_id"])["status"] == "SUCCEEDED"
    assert worker.state.get(invalid["job_id"])["error_code"] == "RESUME_CHECKPOINT_HASH_MISMATCH"
    assert worker.state.get(changed["job_id"])["error_code"] == "RESUME_CONDITIONS_CHANGED"
    result = (workspace / resumed["job_id"] / "result.json").read_bytes()
    assert result == (workspace / baseline["job_id"] / "result.json").read_bytes()
    assert json.loads(result) == {"count": 1024, "sum_squares": sum(i * i for i in range(1, 1025))}
    assert (workspace / resumed["job_id"] / "execution.log").read_text().splitlines()[0] == "start completed=384 total=1024"

    exported = export_smoke(config, worker.state, resumed["job_id"], tmp_path)
    for line in (exported / "SHA256SUMS").read_text().splitlines():
        checksum, name = line.split("  ")
        assert hashlib.sha256((exported / name).read_bytes()).hexdigest() == checksum
    assert {path.name for path in exported.iterdir()} == {
        "SHA256SUMS", "result.json", "checkpoint.json", "execution.log", "nas-agent-status.json"}


def test_outbox_io_failure_retains_result_and_republishes(setup, monkeypatch):
    config, poll_config, api, job, _ = setup
    approve(config, api)
    poller = Poller(poll_config, api)
    poller.cycle()
    backend = CountingBackend(config)
    worker = Worker(config, api, backend)

    def unavailable(*args, **kwargs):
        raise OSError("simulated outbox failure")

    with monkeypatch.context() as patch:
        patch.setattr("nas_agent.worker.atomic_json", unavailable)
        worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "SUCCEEDED"
    assert worker.state.meta("outbox_pending:31") == "1"
    worker.cycle()
    poller.cycle()
    assert len(backend.calls) == 1 and len(api.comments) == 1
    assert worker.state.meta("outbox_pending:31") == "0"


def test_control_api_failure_after_grace_stops_running_subprocess(setup, tmp_path, monkeypatch):
    config, poll_config, api, job, _ = setup
    approve(config, api)
    Poller(poll_config, api).cycle()
    marker = tmp_path / "child-completed"
    original_issue = api.issue
    failing = False

    def issue(number, *, timeout=None):
        if failing:
            raise Unavailable("GITHUB_RATE_LIMIT", http_status=429, rate_limited=True)
        return original_issue(number)

    monotonic = time.monotonic
    elapsed = 0

    def action(job, policy, control):
        nonlocal failing, elapsed
        failing = True
        elapsed = 120
        control()
        elapsed = 300
        return Commands().run([sys.executable, "-c", "import sys,time;time.sleep(2);open(sys.argv[1],'w').close()", str(marker)],
                              cwd=tmp_path, env=clean_environment(tmp_path), control=control)

    monkeypatch.setattr(api, "issue", issue)
    monkeypatch.setattr("nas_agent.worker.time.monotonic", lambda: monotonic() + elapsed)
    worker = Worker(config, api, CountingBackend(config, action))
    worker.cycle()
    assert worker.state.get(job["job_id"])["error_code"] == "GITHUB_RECHECK_TIMEOUT"
    details = json.loads(worker.state.meta("github_failure:" + job["job_id"]))
    assert details["phase"] == "running_issue_recheck" and details["http_status"] == 429 and details["rate_limited"]
    assert not marker.exists()


def test_comment_failure_during_calculation_keeps_running_and_retries(setup, monkeypatch):
    config, poll_config, api, job, _ = setup
    approve(config, api)
    poller = Poller(poll_config, api)
    poller.cycle()

    def action(job, policy, control):
        with monkeypatch.context() as patch:
            def report_unavailable(*args):
                raise Unavailable(transport="timeout")

            patch.setattr(api, "report", report_unavailable)
            with pytest.raises(Unavailable):
                poller.cycle()
            assert json.loads(poller.state.meta("last_report_failure"))["transport"] == "timeout"
        return Backend(config).run(job, policy, control)

    backend = CountingBackend(config, action)
    worker = Worker(config, api, backend)
    worker.cycle()
    poller.cycle()
    assert worker.state.get(job["job_id"])["status"] == "SUCCEEDED"
    assert len(backend.calls) == 1 and len(api.comments) == 1
    assert '"status": "SUCCEEDED"' in api.comments[0]["body"]


def test_command_timeout_stops_subprocess(tmp_path):
    started = time.monotonic()
    def control():
        if time.monotonic() - started > 0.15:
            raise Abort("FAILED", "TIMEOUT")
    with pytest.raises(Abort):
        Commands().run([sys.executable, "-c", "import time;time.sleep(30)"], cwd=tmp_path, env=clean_environment(tmp_path), control=control)
    assert time.monotonic() - started < 5


def test_poller_never_gets_docker_or_github_secret_in_child_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "secret")
    monkeypatch.setenv("DOCKER_HOST", "tcp://unsafe:2375")
    env = clean_environment(tmp_path)
    assert "GITHUB_TOKEN" not in env and "DOCKER_HOST" not in env
    from nas_agent.report import public_result
    result = public_result({"summary": {"raw_log": "secret", "horses": 376, "architecture": "secret"}, "artifacts_sha256": {"../../bad": "a" * 64}})
    assert result["summary"] == {"horses": 376} and result["artifacts_sha256"] == {}


def test_rate_limit_and_no_credential_redirect(monkeypatch):
    api = GitHub()
    class Limited:
        def open(self, *args, **kwargs):
            raise urllib.error.HTTPError("https://api.github.com", 429, "hidden", {"Retry-After": "120"}, io.BytesIO())
    api.opener = Limited()
    with pytest.raises(Unavailable) as result:
        api.issue(31)
    assert result.value.retry_at >= time.time() + 119
    from nas_agent.github import NoRedirect
    with pytest.raises(Unavailable):
        NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.example")


def test_single_worker_lock_and_dangerous_config(setup):
    config, _, _, _, _ = setup
    lock = Path(config["state_dir"]) / "test.lock"
    with single_worker(lock):
        with pytest.raises(Rejected):
            with single_worker(lock):
                pass
    with single_worker(lock):
        pass
    cfg = {**config, "validation_root": str(Path(config["state_dir"]).parent / "production/data/output")}
    path = Path(config["state_dir"]).parent / "bad.json"
    atomic_json(path, cfg, mode=0o600)
    with pytest.raises(Rejected, match="UNSAFE_AGENT_ROOT"):
        load_config(path, "worker")


def test_mock_acceptance_handler_and_frozen_running_request(setup, monkeypatch):
    config, poll_config, api, job, path = setup
    job.update(job_type="issue16_acceptance", input_id="fixture")
    api.add(job)
    policy = private_json(path)
    policy.update(allow_build=True, execution_window_jst={"start": "00:00", "end": "23:59"}, limits={"timeout_seconds": 30, "cpus": 1.0, "memory_mb": 2048, "pids": 256},
                  inputs={"fixture": {"root": str(Path(config["state_dir"]).parent / "frozen"), "manifest_sha256": "d" * 64, "raceday": "20260809"}})
    atomic_json(path, policy, mode=0o600)
    from datetime import datetime, timezone
    import nas_agent.worker as worker_module
    monkeypatch.setattr(worker_module, "utcnow", lambda: datetime(2026, 11, 25, 3, 0, tzinfo=timezone.utc))
    approve(config, api)

    def simulated_acceptance(frozen, policy, control):
        api.rows[31]["body"] = json.dumps({**job, "commit_sha": "e" * 40})
        control()
        assert frozen["commit_sha"] == PR17_SHA  # 実行途中の編集は固定パラメータを変えない。
        return {"summary": {"races": 27, "horses": 376, "acceptance": "PARTIAL", "windows_comparison": "NOT_AVAILABLE"}, "artifacts_sha256": {"manifest.json": "c" * 64}}

    backend = CountingBackend(config, simulated_acceptance)
    poller = Poller(poll_config, api)
    poller.cycle()
    worker = Worker(config, api, backend)
    worker.cycle()
    poller.cycle()
    assert worker.state.get(job["job_id"])["status"] == "CANCELLED"
    assert '"error_code": "ISSUE_REQUEST_CHANGED"' in api.comments[0]["body"]
    assert '"horses": 376' not in api.comments[0]["body"]
    assert len(backend.calls) == 1


@pytest.mark.parametrize("mutation", ["production_rw", "docker_socket", "root", "network", "resource", "secret_environment"])
def test_fixed_compose_refuses_escape_and_missing_limits(tmp_path, mutation):
    from nas_agent.acceptance_host import validate_compose
    source = tmp_path / "source"
    checked = {"CANDIDATE_SNAPSHOT": str(tmp_path / "snapshot"), "CANDIDATE_OUTPUT": str(tmp_path / "runs"),
               "CANDIDATE_WORKSPACE_MARKER": str(tmp_path / "marker"), "CANDIDATE_COMMIT": PR17_SHA}
    volumes = [{"type": "bind", "target": target, "source": checked[key], "read_only": ro} for target, key, ro in [
        ("/snapshot", "CANDIDATE_SNAPSHOT", True), ("/candidate/runs", "CANDIDATE_OUTPUT", False),
        ("/candidate/.nas-candidate-workspace.json", "CANDIDATE_WORKSPACE_MARKER", True)]]
    service = {"network_mode": "none", "read_only": True, "user": "1000:1000", "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
               "environment": {"TZ": "Asia/Tokyo"}, "command": ["--workspace", "/candidate", "--snapshot", "/snapshot", "--output", "/candidate/runs"],
               "volumes": volumes, "build": {"context": str(source), "dockerfile": "nas/Dockerfile", "args": {"SOURCE_COMMIT": PR17_SHA}},
               "cpus": 1.0, "mem_limit": 2147483648, "pids_limit": 256}
    limits = {"cpus": 1.0, "memory_mb": 2048, "pids": 256}
    validate_compose({"services": {"candidate": service}}, checked, source.resolve(), limits)
    if mutation == "production_rw":
        service["volumes"][0]["source"] = str(tmp_path / "production")
        service["volumes"][0]["read_only"] = False
    elif mutation == "docker_socket":
        service["volumes"].append({"type": "bind", "source": "/var/run/docker.sock", "target": "/var/run/docker.sock"})
    elif mutation == "root":
        service["user"] = "0:0"
    elif mutation == "network":
        service["network_mode"] = "host"
    elif mutation == "resource":
        service["mem_limit"] = 0
    else:
        service["environment"]["GITHUB_TOKEN"] = "secret"
    with pytest.raises(Rejected):
        validate_compose({"services": {"candidate": service}}, checked, source.resolve(), limits)


def comparison_fixture():
    # PR17 nas.compareが返す最小の非空レース・馬・分類を表す。
    row = {'レースID': 'race1', '予想状態': 'PREDICTED', '予想可否': '数値予想済み', '対象外理由コード': None, '対象外理由': None}
    side = {'horses': 1, 'top5': [1], 'bet_row': [row]}
    report = {'attribution': {'data': 'SAME', 'model': 'SAME', 'implementation': 'SAME'},
              'common_horses': 1, 'unavailable_columns': {'left': [], 'right': []},
              'horse_differences': [], 'left_only': [], 'right_only': [],
              'races': [{'race_id': 'race1', 'left': deepcopy(side), 'right': deepcopy(side),
                         'top5_changed': False, 'tickets_changed': False, 'fixed_tickets_changed': False}]}
    excluded = {'horses': 0, 'top5': [], 'bet_row': [{'レースID': 'race2', '予想状態': 'NO_PREDICTION', '予想可否': '予想対象外',
                                                   '対象外理由コード': 'FIXTURE_REASON', '対象外理由': 'fixture exclusion'}]}
    report['races'].append({'race_id': 'race2', 'left': deepcopy(excluded), 'right': deepcopy(excluded),
                            'top5_changed': False, 'tickets_changed': False, 'fixed_tickets_changed': False})
    runtime = {'source_commit': PR17_SHA, 'features': ['feature1'],
               'excel_validation': {'source_race_count': 2, 'predicted_race_count': 1, 'unsupported_race_count': 1},
               'population': {'predicted_horses': 1, 'excluded_races': ['race2']}}
    return report, runtime


@pytest.mark.parametrize('mutation,error', [
    ('missing_column', 'COMPARISON_COLUMNS_INCOMPLETE'), ('implementation', 'COMPARISON_IDENTITY_MISMATCH'),
    ('commit', 'REFERENCE_COMMIT_MISMATCH'), ('tickets', 'WINDOWS_LINUX_MISMATCH'),
    ('reason', 'RACE_CLASSIFICATION_MISMATCH'), ('empty', 'EMPTY_COMPARISON'),
])
def test_acceptance_rejects_false_match(mutation, error):
    from nas_agent.acceptance_host import comparison_summary
    report, runtime = comparison_fixture()
    reference = deepcopy(runtime)
    if mutation == 'missing_column':
        report['unavailable_columns']['left'] = ['score_raw', 'feature1']
    elif mutation == 'implementation':
        report['attribution']['implementation'] = 'DIFFERENT'
    elif mutation == 'commit':
        reference['source_commit'] = 'a' * 40
    elif mutation == 'tickets':
        report['races'][0]['tickets_changed'] = True
    elif mutation == 'reason':
        report['races'][1]['left']['bet_row'][0]['対象外理由'] = 'different'
    else:
        report.update(common_horses=0, races=[])
    if mutation in {'missing_column', 'implementation'}:
        # 旧判定が差分ゼロとして通す代表例。不足列/異実装でもdata/modelだけは同じ。
        assert report['attribution']['data'] == report['attribution']['model'] == 'SAME'
        assert not report['horse_differences'] and not report['left_only'] and not report['right_only']
        assert not any(r['top5_changed'] or r['fixed_tickets_changed'] for r in report['races'])
    with pytest.raises(Rejected, match=error):
        comparison_summary(report, reference, runtime, PR17_SHA)


def test_acceptance_requires_candidate_arm64_evidence():
    from nas_agent.acceptance_host import arm64_verified, comparison_summary
    report, runtime = comparison_fixture()
    assert comparison_summary(report, runtime, runtime, PR17_SHA)['windows_comparison'] == 'MATCH'
    assert not arm64_verified({'platform': 'Linux-aarch64'}, None)  # 未確認はPASSにしない。
    assert not arm64_verified({}, {'architecture': 'arm64', 'os': 'linux'})
    assert arm64_verified({'platform': 'Linux-6.1-aarch64-with-glibc2.36'}, {'architecture': 'arm64', 'os': 'linux'})
    with pytest.raises(Rejected, match='ARM64'):
        arm64_verified({'platform': 'Linux-x86_64'}, {'architecture': 'amd64', 'os': 'linux'})


@pytest.mark.parametrize('recover', [False, True])
def test_terminal_and_recovery_barrier_rollback_together(setup, recover):
    import sqlite3
    config, _, api, job, _ = setup
    job['job_type'] = 'issue16_acceptance'
    state = Worker(config, api, CountingBackend(config)).state
    state.observe(31, 'a' * 64, job)
    state.set(job['job_id'], 'RUNNING')
    # barrier保存時に故障を注入し、終端状態と履歴もロールバックすることを確認する。
    state.db.execute("CREATE TRIGGER fail_barrier BEFORE INSERT ON meta WHEN NEW.key='recovery_required' BEGIN SELECT RAISE(ABORT, 'injected'); END")
    state.db.commit()
    with pytest.raises(sqlite3.DatabaseError):
        if recover:
            state.recover()
        else:
            state.set(job['job_id'], 'FAILED', error='TIMEOUT')
    assert state.get(job['job_id'])['status'] == 'RUNNING'
    assert state.get(job['job_id'])['history'][-1]['status'] == 'RUNNING'
    assert state.meta('recovery_required') is None
    state.db.execute('DROP TRIGGER fail_barrier')
    state.db.commit()
    state.recover()
    assert state.get(job['job_id'])['status'] == 'INTERRUPTED'
    assert state.meta('recovery_required') == '1'


def test_audit_includes_raceday_odds(tmp_path, monkeypatch):
    from nas_agent import acceptance_host
    odds = tmp_path / 'data/ozzu_csv/OZZU_20260809.csv'
    odds.parent.mkdir(parents=True)
    odds.write_text('synthetic odds', encoding='utf-8')
    monkeypatch.setattr(acceptance_host.subprocess, 'check_output', lambda *a, **k: 'fixture-container')
    monkeypatch.setattr(acceptance_host, 'docker_json', lambda *a: {})
    before = acceptance_host.audit(tmp_path, '20260809', {})
    odds.write_text('changed synthetic odds', encoding='utf-8')
    after = acceptance_host.audit(tmp_path, '20260809', {})
    name = 'data/ozzu_csv/OZZU_20260809.csv'
    assert name in before['files'] and before['files'][name] != after['files'][name]


def test_spool_paging_reaches_new_job_and_report_after_1000_old_files(setup):
    from nas_agent.report import record
    config, poll_config, api, job, _ = setup
    approve(config, api)
    worker, poller = Worker(config, api, CountingBackend(config)), Poller(poll_config, api)
    spool = Path(config['spool_dir'])
    worker.state.observe(31, request(api.rows[31])[1], job)
    worker.state.set(job['job_id'], 'SUCCEEDED')
    payload = record(config, worker.state.get(job['job_id']))
    for number in range(1000, 2001):
        api.rows[number] = {'title': 'not a job', 'number': number}
        atomic_json(spool / 'inbox' / f'{number}.json', {'issue_number': number})
        atomic_json(spool / 'outbox' / f'{number}.json', {**payload, 'issue_number': number})
    # 1001件目の有効依頼は辞書順で最後。古い履歴は削除しない。
    api.add({**job, 'job_id': 'health_new'}, number=3000)
    approve(config, api, number=3000)
    atomic_json(spool / 'inbox/3000.json', {'issue_number': 3000})
    worker.cycle()
    worker.cycle()
    assert len(worker.backend.calls) == 1
    assert worker.state.get('health_new')['status'] == 'SUCCEEDED'
    # 過去の終端報告1000件も処理済みのまま残し、新規報告へ進む。
    from nas_agent.report import comment
    from nas_agent.schema import digest
    for number in range(1000, 2001):
        old = {**payload, 'issue_number': number}
        marker, _ = comment(old)
        poller.state.meta('report:' + digest(marker), json.dumps({'revision': digest(old)}))
    poller.cycle()
    poller.cycle()
    assert any('health_new' in row['body'] for row in api.comments)
    assert (spool / 'outbox/1000.json').is_file()


@pytest.mark.parametrize('case', ['valid', 'worker_owner', 'writable_parent', 'worker_parent', 'symlink', 'worker_approval'])
def test_admin_policy_owner_parent_and_writer_boundary(tmp_path, monkeypatch, case):
    from types import SimpleNamespace
    import stat
    from nas_agent import files
    path = (tmp_path / 'admin/policy.json').absolute()
    monkeypatch.setattr(files, 'POSIX', True)
    monkeypatch.setattr(files.os, 'geteuid', lambda: 1001 if case == 'worker_approval' else 0, raising=False)
    def metadata(item):
        mode = stat.S_IFREG | 0o640 if item == path else stat.S_IFDIR | 0o750
        owner = 0
        if case == 'worker_owner' and item == path:
            owner = 1001
        if case == 'worker_parent' and item == path.parent:
            owner = 1001
        if case == 'writable_parent' and item == path.parent:
            mode |= 0o020
        if case == 'symlink' and item == path.parent:
            mode = stat.S_IFLNK | 0o777
        return SimpleNamespace(st_mode=mode, st_uid=owner)
    monkeypatch.setattr(Path, 'lstat', metadata)
    if case == 'valid':
        files.validate_admin_file(path, for_write=True)
    else:
        with pytest.raises(Rejected, match='ADMIN'):
            files.validate_admin_file(path, for_write=True)


def test_admin_cli_approval_does_not_use_worker_config_or_state(setup, monkeypatch, capsys):
    from nas_agent import __main__ as cli
    _, _, api, _, policy = setup
    monkeypatch.setattr(cli, 'GitHub', lambda *a: api)
    monkeypatch.setattr(cli, 'verify_install', lambda cfg: None)
    monkeypatch.setattr(cli, 'load_config', lambda *a: pytest.fail('admin command must not load worker config'))
    request_hash = request(api.rows[31])[1]
    monkeypatch.setattr(sys, 'argv', ['nas_agent', 'approve', '--policy-file', str(policy), '--agent-commit', 'a' * 40,
                                    '--issue', '31', '--expected-hash', request_hash])
    cli.main()
    assert request_hash in private_json(policy)['approved_requests']
    assert request_hash in capsys.readouterr().out
    assert (policy.parent / '.approval.lock').exists()
