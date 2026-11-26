"""Issue lifecycle tests using simulated time and the real worker ledger."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from nas_agent.files import atomic_json
from nas_agent.github import Unavailable
from nas_agent.schema import AUDIT_SHA, PR17_SHA, request, utcnow
from nas_agent.worker import Worker
from test_full_feature_audit import EXAMPLES, JobIssue


@pytest.fixture
def setup(tmp_path, monkeypatch):
    import nas_agent.worker as worker_module

    monkeypatch.setattr("nas_agent.files.validate_admin_file", lambda *args: None)
    now = utcnow()
    clock = SimpleNamespace(elapsed=0)
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(
        monotonic=lambda: clock.elapsed, time=lambda: now.timestamp() + clock.elapsed,
    ))
    monkeypatch.setattr(worker_module, "utcnow", lambda: now + timedelta(seconds=clock.elapsed))
    job = {"schema_version": 1, "job_id": "recheck", "job_type": "healthcheck", "commit_sha": PR17_SHA,
           "raceday": "20260809", "input_id": "healthcheck", "scope": "offline",
           "expires_at": (now + timedelta(hours=1)).isoformat()}
    api = JobIssue({"number": 31, "title": "[NAS-JOB] recheck", "body": json.dumps(job),
                    "state": "open", "user": {"id": 42, "login": "test-user"}})
    policy = json.loads((EXAMPLES / "policy.json").read_text())
    policy.update(requester_ids=[42], inputs={}, approved_requests=[request(api.document)[1]])
    config = {key: str(tmp_path / name) for key, name in
              {"state_dir": "worker", "spool_dir": "spool", "policy_file": "policy.json"}.items()}
    config["instance_id"] = "0123456789abcdef"
    atomic_json(config["policy_file"], policy)
    atomic_json(tmp_path / "spool/inbox/31.json", {"issue_number": 31})
    return config, api, clock, now.timestamp()


def execute(setup, action, issue_hook):
    config, api, clock, wall_start = setup
    calls = []
    original = api.issue

    def issue(number, *, timeout=None):
        calls.append(clock.elapsed)
        issue_hook(len(calls), wall_start + clock.elapsed)
        return original(number)

    api.issue = issue
    worker = Worker(config, api, SimpleNamespace(run=lambda job, policy, control: action(control)))
    worker.cycle()
    return worker.state.get("recheck"), calls, worker.state


@pytest.mark.parametrize("authenticated,interval", [(True, 60), (False, 120)])
def test_transient_failure_recovers_and_final_check_is_fresh(setup, authenticated, interval):
    _, api, clock, _ = setup
    api.token = "unused-test-token" if authenticated else None

    def issue_hook(call, wall_time):
        if call == 3:
            raise Unavailable("GITHUB_HTTP_ERROR", wall_time + 65, http_status=503)

    def action(control):
        for step in (interval - 1, interval, interval + 64, interval + 65):
            clock.elapsed = step
            control()
        return {}

    row, calls, _ = execute(setup, action, issue_hook)
    assert row["status"] == "SUCCEEDED"
    assert calls == [0, 0, interval, interval + 65, interval + 65]


def test_retry_after_cannot_extend_outage_deadline(setup):
    _, _, clock, _ = setup

    def issue_hook(call, wall_time):
        if call in (3, 4):
            raise Unavailable("GITHUB_RATE_LIMIT", wall_time + 100, rate_limited=True)

    def action(control):
        for step in (120, 219, 220, 299, 300):
            clock.elapsed = step
            control()
        pytest.fail("Outage continued past the deadline")

    row, calls, _ = execute(setup, action, issue_hook)
    assert (row["status"], row["error_code"]) == ("FAILED", "GITHUB_RECHECK_TIMEOUT")
    assert calls == [0, 0, 120, 220]


def test_known_rate_reset_defers_running_request(setup):
    _, api, clock, wall_start = setup
    api.rate_reset_at = wall_start + 155

    def action(control):
        for step in (120, 154, 155):
            clock.elapsed = step
            control()
        return {}

    row, calls, _ = execute(setup, action, lambda *_: None)
    assert row["status"] == "SUCCEEDED"
    assert calls == [0, 0, 155, 155]


@pytest.mark.parametrize("case,status,code", [
    ("closed", "CANCELLED", "ISSUE_CLOSED"),
    ("auth", "FAILED", "GITHUB_AUTH_OR_PERMISSION"),
    ("final_unavailable", "FAILED", "GITHUB_UNAVAILABLE"),
    ("final_changed", "CANCELLED", "ISSUE_REQUEST_CHANGED"),
])
def test_running_and_final_checks_fail_closed(setup, case, status, code):
    _, api, clock, _ = setup

    def issue_hook(call, wall_time):
        if call != 3:
            return
        if case == "closed":
            api.document["state"] = "closed"
        elif case == "auth":
            raise Unavailable(code, wall_time + 100, http_status=401)
        elif case == "final_unavailable":
            raise Unavailable(retry_at=wall_time + 60, transport="timeout")
        else:
            api.document["body"] = api.document["body"].replace(PR17_SHA, "f" * 40)

    def action(control):
        clock.elapsed = 1 if case.startswith("final") else 120
        control()
        assert case.startswith("final")
        return {"summary": {"races": 1}}

    row, calls, state = execute(setup, action, issue_hook)
    assert (row["status"], row["error_code"]) == (status, code)
    assert json.loads(row["result"]) == {}
    assert calls == [0, 0, clock.elapsed]
    if case == "final_unavailable":
        assert json.loads(state.meta("github_failure:recheck"))["phase"] == "final_issue_recheck"


def test_expiry_during_backoff_needs_no_network_check(setup):
    config, api, clock, wall_start = setup
    job = json.loads(api.document["body"])
    job["expires_at"] = datetime.fromtimestamp(wall_start + 150, timezone.utc).isoformat()
    api.document["body"] = json.dumps(job)
    with open(config["policy_file"]) as handle:
        policy = json.load(handle)
    policy["approved_requests"] = [request(api.document)[1]]
    atomic_json(config["policy_file"], policy)

    def issue_hook(call, wall_time):
        if call == 3:
            raise Unavailable(retry_at=wall_time + 90, transport="timeout")

    def action(control):
        clock.elapsed = 120
        control()
        clock.elapsed = 150
        control()
        pytest.fail("Expired job kept running")

    row, calls, _ = execute(setup, action, issue_hook)
    assert (row["status"], row["error_code"]) == ("CANCELLED", "EXPIRED")
    assert calls == [0, 0, 120]


@pytest.mark.parametrize("stage", ["preflight", "final"])
@pytest.mark.parametrize("kind", ["withdrawn", "grant_expired", "input_changed"])
def test_rechecks_local_approval_and_input_pin(setup, stage, kind, monkeypatch):
    config, api, clock, wall_start = setup
    policy_path = Path(config["policy_file"])
    policy = json.loads(policy_path.read_text())
    job = json.loads(api.document["body"])
    if kind == "input_changed":
        job.update(job_type="full_feature_audit", commit_sha=AUDIT_SHA, input_id="audit")
        policy["inputs"]["audit"] = {
            "root": str(policy_path.parent / "fixture"), "raceday": job["raceday"], "manifest_sha256": "a" * 64,
        }
        api.document["body"] = json.dumps(job)
        policy["approved_requests"] = [request(api.document)[1]]
    if kind == "grant_expired":
        policy["approved_requests"] = []
        policy["grants"] = [{
            "job_type": job["job_type"], "commit_sha": job["commit_sha"], "input_id": job["input_id"],
            "from_date": job["raceday"], "through_date": job["raceday"],
            "expires_at": datetime.fromtimestamp(wall_start + 10, timezone.utc).isoformat(),
        }]
        monkeypatch.setattr("nas_agent.schema.utcnow", lambda: datetime.fromtimestamp(wall_start + clock.elapsed, timezone.utc))
    atomic_json(policy_path, policy)

    def issue_hook(call, _):
        if call != (2 if stage == "preflight" else 3):
            return
        if kind == "grant_expired":
            clock.elapsed = 10
            return
        if kind == "input_changed":
            policy["inputs"]["audit"]["manifest_sha256"] = "b" * 64
        else:
            policy["approved_requests"] = []
        atomic_json(policy_path, policy)

    def action(control):
        assert stage == "final"
        return {"summary": {"races": 1}}

    row, _, _ = execute(setup, action, issue_hook)
    assert (row["started_at"] is not None) == (stage == "final")
    assert json.loads(row["result"]) == {}
    if kind == "input_changed":
        assert (row["status"], row["error_code"]) == ("INTERRUPTED", "APPROVED_INPUT_CHANGED")
    elif stage == "preflight":
        assert (row["status"], row["error_code"]) == ("WAIT_APPROVAL", "LOCAL_APPROVAL_REQUIRED")
    else:
        assert (row["status"], row["error_code"]) == ("INTERRUPTED", "LOCAL_APPROVAL_REVOKED")
