"""模擬GitHub→承認→固定handler→永続状態/報告の最小受入。NASへは接続しない。"""
from copy import deepcopy
from datetime import timedelta
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

    def api(self, path, *, method="GET", payload=None, conditional=False):
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
            return deepcopy(list(self.rows.values()))
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
def setup(tmp_path):
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
    Worker(config, api, backend).cycle()
    poller.cycle()
    assert len(backend.calls) == 1 and len(api.comments) == 1
    assert worker.state.get(job["job_id"])["status"] == "SUCCEEDED"


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
    assert worker.state.get(job["job_id"])["status"] == "SUCCEEDED"
    assert '"horses": 376' in api.comments[0]["body"]
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
