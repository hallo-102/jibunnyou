"""Synthetic acceptance tests for Issue #49."""
from datetime import date, timedelta
import json
from pathlib import Path
import sys

import pytest

from nas_agent.artifacts import checksum_text
from nas_agent.backend import Backend
from nas_agent.comparison import sha256, stage_fixture
from nas_agent.config import load_policy
from nas_agent.files import atomic_json
from nas_agent.github import Unavailable
from nas_agent.schema import AUDIT_SHA, Rejected, authorized, request, utcnow
from nas_agent.worker import Worker

from test_ab_comparison import make_fixture
from test_worker_recheck import setup  # noqa: F401


def change_request(config, api, **overrides):
    job = json.loads(api.document["body"]) | overrides
    api.document["body"] = json.dumps(job)
    policy = json.loads(Path(config["policy_file"]).read_text())
    policy["approved_requests"] = [request(api.document)[1]]
    if job["job_type"] not in policy["job_types"]:
        policy["job_types"].append(job["job_type"])
    atomic_json(config["policy_file"], policy)
    return job, policy


def test_interrupted_smoke_bundle_and_allowlisted_exchange(setup, tmp_path):
    config, api, _, _ = setup
    config["agent_commit"] = json.loads(api.document["body"])["commit_sha"]
    job, policy = change_request(config, api, job_type="checkpoint_smoke", input_id="synthetic-v1",
                                 interrupt_after_checkpoint=True)
    root = tmp_path / "exchange"
    destination = root / "results"
    policy["artifact_exchange"] = {"root": str(root), "destination": str(destination),
                                   "jobs": {"checkpoint_smoke": ["checkpoint.json", "nas-agent-status.json"]}}
    atomic_json(config["policy_file"], policy)
    worker = Worker(config, api, None)
    worker.cycle()  # Provisioning can lag behind calculation.
    state_dir = Path(config["state_dir"])
    saved = state_dir / "recovery" / job["job_id"] / "INTERRUPTED"
    assert set(p.name for p in saved.iterdir()) == {
        "checkpoint.json", "nas-agent-status.json", "execution.log", "failure-diagnostics.json", "SHA256SUMS",
    }
    assert (saved / "SHA256SUMS").read_text() == checksum_text(saved)
    assert json.loads((saved / "checkpoint.json").read_text())["completed_count"] == 384
    assert json.loads((saved / "failure-diagnostics.json").read_text())["error_code"] == "SMOKE_CHECKPOINT_INTERRUPTED"
    destination.mkdir(parents=True)
    worker.cycle()
    worker.cycle()
    transferred = destination / f"{job['job_id']}--interrupted"
    assert {p.name for p in transferred.iterdir()} == {"SHA256SUMS", "checkpoint.json", "nas-agent-status.json"}
    assert (transferred / "SHA256SUMS").read_text() == checksum_text(transferred)
    assert sha256(transferred / "checkpoint.json") == sha256(saved / "checkpoint.json")


@pytest.mark.parametrize("failure", ["recover", "recover_within_grace", "closed", "edited", "revoked", "authentication", "expired", "tampered"])
def test_finalization_restarts_without_recalculation(setup, failure, monkeypatch):
    config, api, clock, wall_start = setup
    config["agent_commit"] = json.loads(api.document["body"])["commit_sha"]
    job, policy = change_request(config, api, job_type="checkpoint_smoke", input_id="synthetic-v1")
    if failure == "recover":
        root = Path(config["state_dir"]).parent / "exchange"
        destination = root / "results"
        destination.mkdir(parents=True)
        policy["artifact_exchange"] = {"root": str(root), "destination": str(destination), "jobs": {
            "checkpoint_smoke": ["result.json", "nas-agent-status.json", "failure-diagnostics.json"],
        }}
        atomic_json(config["policy_file"], policy)
    calls = 0
    original = api.issue

    def issue(number, *, timeout=None):
        nonlocal calls
        calls += 1
        if calls == 3:
            retry_at = 60 if failure == "recover_within_grace" else 320
            raise Unavailable("GITHUB_HTTP_ERROR", wall_start + retry_at, http_status=503, transport="http")
        if calls > 3 and failure == "authentication":
            raise Unavailable("GITHUB_AUTH_OR_PERMISSION", wall_start + 400, http_status=401)
        return original(number, timeout=timeout)

    api.issue = issue
    worker = Worker(config, api, None)
    worker.cycle()
    job_id = job["job_id"]
    before = worker.state.get(job_id)
    assert (before["status"], before["error_code"]) == ("PENDING_FINALIZATION", "GITHUB_HTTP_ERROR")
    ledger = Path(config["state_dir"]) / "recovery" / job_id
    result_file = ledger / "PENDING_FINALIZATION" / "result.json"
    checksum = sha256(result_file)
    diagnostic = json.loads((ledger / "latest-github-diagnostic.json").read_text())
    assert (diagnostic["phase"], diagnostic["http_status"], diagnostic["rate_limited"], diagnostic["transport"]) == (
        "final_issue_recheck", 503, False, "http",
    )
    (Path(config["spool_dir"]) / "inbox/31.json").unlink()
    if failure != "recover_within_grace":
        clock.elapsed = 300
        worker.cycle()
        assert worker.state.get(job_id)["error_code"] == "GITHUB_RECHECK_TIMEOUT"
        assert calls == 3
        if failure == "recover":
            pending = destination / f"{job_id}--pending_finalization"
            assert sha256(pending / "result.json") == checksum
            status = json.loads((pending / "nas-agent-status.json").read_text())
            assert (status["status"], status["error_code"]) == ("PENDING_FINALIZATION", "GITHUB_RECHECK_TIMEOUT")
            assert json.loads((pending / "failure-diagnostics.json").read_text())["failure"]["http_status"] == 503
    if failure == "closed":
        api.document["state"] = "closed"
    if failure == "edited":
        api.document["body"] = api.document["body"].replace("20260809", "20260810")
    if failure == "revoked":
        policy["approved_requests"] = []
        atomic_json(config["policy_file"], policy)
    if failure == "tampered":
        result_file.write_text("{}")
    clock.elapsed = 3600 if failure == "expired" else 60 if failure == "recover_within_grace" else 320
    monkeypatch.setattr("nas_agent.worker.run_smoke", lambda *args: pytest.fail("recalculated"))
    worker = Worker(config, api, None)
    worker.cycle()
    after = worker.state.get(job_id)
    expected = {
        "recover": ("SUCCEEDED", None), "recover_within_grace": ("SUCCEEDED", None),
        "closed": ("CANCELLED", "ISSUE_CLOSED"), "edited": ("CANCELLED", "ISSUE_REQUEST_CHANGED"),
        "revoked": ("INTERRUPTED", "LOCAL_APPROVAL_REVOKED"), "expired": ("CANCELLED", "EXPIRED"),
        "authentication": ("FAILED", "GITHUB_AUTH_OR_PERMISSION"),
        "tampered": ("FAILED", "SAVED_ARTIFACT_SHA_MISMATCH"),
    }[failure]
    assert (after["status"], after["error_code"]) == expected
    assert after["started_at"] == before["started_at"]
    if failure != "tampered":
        assert sha256(ledger / after["status"] / "result.json") == checksum
    if failure == "recover":
        assert sha256(destination / f"{job_id}--succeeded" / "result.json") == checksum


def test_168_day_grant_requires_exact_day_input_and_code(setup, tmp_path):
    config, api, _, _ = setup
    job, policy = change_request(config, api, job_type="full_feature_audit", commit_sha=AUDIT_SHA)
    entries = {}
    approved = {}
    for offset in range(168):
        day = (date(2026, 1, 1) + timedelta(days=offset * 2)).strftime("%Y%m%d")
        input_id = "audit_" + day
        approved[day] = input_id
        entries[input_id] = {"raceday": day, "root": str(tmp_path / input_id), "manifest_sha256": "a" * 64}
    policy["inputs"] = entries
    policy["approved_requests"] = []
    policy["grants"] = [{"job_type": "full_feature_audit", "commit_sha": AUDIT_SHA, "inputs": approved,
                         "expires_at": (utcnow() + timedelta(days=1)).isoformat()}]
    atomic_json(config["policy_file"], policy)
    policy = load_policy(config["policy_file"])
    for day, input_id in approved.items():
        assert authorized(policy, job | {"raceday": day, "input_id": input_id}, "", 42)
    day, input_id = next(iter(approved.items()))
    candidate = job | {"raceday": day, "input_id": input_id}
    assert not authorized(policy, candidate | {"commit_sha": "b" * 40}, "", 42)
    assert not authorized(policy, candidate | {"raceday": "20260102"}, "", 42)
    assert not authorized(policy, candidate | {"input_id": "audit_20260103"}, "", 42)
    assert not authorized(policy, candidate, "", 42, edited=True)


@pytest.mark.parametrize("mode,grade", [("formal", "A"), ("reference", "C")])
def test_fixed_fixture_preparation(setup, tmp_path, mode, grade):
    config, api, _, _ = setup
    config.update(agent_commit=json.loads(api.document["body"])["commit_sha"],
                  validation_root=str(tmp_path / "validation"), checkout_root=str(tmp_path / "checkout"),
                  validation_python=sys.executable)
    job, policy = change_request(config, api, job_type="prepare_fixture", input_id="captures")
    Path(config["validation_root"]).mkdir()
    capture = tmp_path / "capture"
    capture.mkdir()
    make_fixture(capture)
    (capture / "index.json").rename(capture / "snapshot_index.json")
    (capture / "evidence.json").rename(capture / "capture_evidence.json")
    index = capture / "snapshot_index.json"
    data = json.loads(index.read_text())
    data["snapshots"][0]["input_evidence_grade"] = grade
    atomic_json(index, data)
    names = ["archive/day.xlsx", "capture_evidence.json", "snapshot_index.json", "settled.xlsx"]
    originals = {name: sha256(capture / name) for name in names}
    atomic_json(capture / "manifest.json", {"schema_version": 1, "evaluation_mode": mode, "files_sha256": originals})
    policy["inputs"] = {job["input_id"]: {"root": str(capture), "raceday": job["raceday"],
                                        "manifest_sha256": sha256(capture / "manifest.json")}}
    atomic_json(config["policy_file"], policy)
    worker = Worker(config, api, Backend(config))
    worker.cycle()
    row = worker.state.get(job["job_id"])
    assert row["status"] == "SUCCEEDED"
    output = Path(config["validation_root"]) / job["job_id"] / "output"
    metadata = json.loads((output / "manifest.json").read_text())
    assert set(metadata["input_evidence"].values()) == {grade}
    assert metadata["evaluation_mode"] == mode
    assert metadata["original_files_sha256"]["archive/source/settled.xlsx"] == originals["settled.xlsx"]
    assert metadata["generated_files_sha256"]["results.json"] == sha256(output / "results.json")
    assert originals == {name: sha256(capture / name) for name in names}
    stage_fixture({"root": output, "manifest_sha256": sha256(output / "manifest.json")},
                  job["raceday"], tmp_path / "staged", lambda: None)


def test_exchange_rejects_escape_and_unapproved_files(setup, tmp_path):
    config, _, _, _ = setup
    policy = json.loads(Path(config["policy_file"]).read_text())
    exchange = {"root": str(tmp_path / "exchange"), "destination": str(tmp_path / "outside"),
                "jobs": {"checkpoint_smoke": ["checkpoint.json"]}}
    policy["artifact_exchange"] = exchange
    atomic_json(config["policy_file"], policy)
    with pytest.raises(Rejected, match="INVALID_ARTIFACT_EXCHANGE"):
        load_policy(config["policy_file"])
    exchange.update(destination=str(tmp_path / "exchange/results"), jobs={"checkpoint_smoke": ["spec.json"]})
    atomic_json(config["policy_file"], policy)
    with pytest.raises(Rejected, match="INVALID_ARTIFACT_ALLOWLIST"):
        load_policy(config["policy_file"])


def test_recovery_rehashes_audit_input_even_with_unchanged_manifest(setup, tmp_path):
    from nas_agent.audit import audit_input_identity

    config, api, clock, wall_start = setup
    job, policy = change_request(config, api, job_type="full_feature_audit", commit_sha=AUDIT_SHA, input_id="audit")
    fixture = tmp_path / "input"
    fixture.mkdir()
    (fixture / "manifest.json").write_text("{}")
    input_file = fixture / "input.csv"
    input_file.write_text("original")
    entry = {"raceday": job["raceday"], "root": str(fixture), "manifest_sha256": sha256(fixture / "manifest.json")}
    policy["inputs"] = {"audit": entry}
    atomic_json(config["policy_file"], policy)
    calls = 0
    original = api.issue

    def issue(number, *, timeout=None):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise Unavailable(retry_at=wall_start + 60, transport="timeout")
        return original(number, timeout=timeout)

    api.issue = issue
    backend = Backend(config)
    backend.run = lambda job, policy, control: {"input_sha256": audit_input_identity(entry)}
    worker = Worker(config, api, backend)
    worker.cycle()
    assert worker.state.get(job["job_id"])["status"] == "PENDING_FINALIZATION"
    input_file.write_text("changed")
    clock.elapsed = 60
    worker.cycle()
    row = worker.state.get(job["job_id"])
    assert (row["status"], row["error_code"]) == ("INTERRUPTED", "APPROVED_INPUT_CHANGED")
