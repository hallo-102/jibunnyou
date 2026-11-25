import hashlib
import json
from datetime import timedelta
from pathlib import Path
import sys

import pytest

from nas_agent.backend import Backend
from nas_agent.files import atomic_json
from nas_agent.poller import Poller
from nas_agent.schema import parse_job, request, utcnow
from nas_agent.worker import Worker

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def example_job():
    return parse_job((EXAMPLES / "full_feature_audit.job.json").read_text(encoding="utf-8"))


def test_audit_schema_accepts_example():
    assert example_job()["job_type"] == "full_feature_audit"


class JobIssue:
    def __init__(self, issue):
        self.document = issue
        self.comments = []

    def issues(self):
        return [self.document]

    def issue(self, number):
        assert number == self.document["number"]
        return self.document

    def identity_allowed(self, issue, ids):
        return issue["user"]["id"] in ids

    def report(self, number, marker, body, previous):
        assert number == self.document["number"]
        self.comments.append(json.loads(body.split("```json\n", 1)[1].split("\n```", 1)[0]))
        return 1


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr("nas_agent.files.validate_admin_file", lambda *args: None)
    job = example_job()
    job["expires_at"] = (utcnow() + timedelta(hours=1)).isoformat()
    api = JobIssue({"number": 27, "title": "[NAS-JOB] " + job["job_id"], "body": json.dumps(job),
                    "state": "open", "user": {"id": 42, "login": "test-user"}})
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "manifest.json").write_text("{}", encoding="utf-8")
    policy = json.loads((EXAMPLES / "policy.json").read_text())
    policy["production_root"] = str(tmp_path / "production")
    policy["requester_ids"] = [42]
    policy["approved_commits"].append(job["commit_sha"])
    policy["approved_requests"].append(request(api.document)[1])
    policy["inputs"][job["input_id"]].update(
        root=str(fixture), manifest_sha256=hashlib.sha256(b"{}").hexdigest())
    config = {
        "instance_id": "0123456789abcdef", "validation_python": sys.executable,
        "state_dir": str(tmp_path / "worker"), "spool_dir": str(tmp_path / "spool"),
        "policy_file": str(tmp_path / "policy.json"), "checkout_root": str(tmp_path / "checkouts"),
        "validation_root": str(tmp_path / "validation"),
    }
    Path(config["validation_root"]).mkdir()
    poller = Poller({**config, "state_dir": str(tmp_path / "poller")}, api)
    return job, api, fixture, policy, config, poller


@pytest.mark.parametrize("missing", ["job_type", "commit", "fixture", "date", "request"])
def test_audit_waits_for_approval(setup, missing):
    job, api, _, policy, config, poller = setup
    if missing == "job_type":
        policy["job_types"].remove(job["job_type"])
    elif missing == "commit":
        policy["approved_commits"].remove(job["commit_sha"])
    elif missing == "fixture":
        del policy["inputs"][job["input_id"]]
    elif missing == "date":
        policy["inputs"][job["input_id"]]["raceday"] = "20260810"
    else:
        policy["approved_requests"].clear()
    atomic_json(config["policy_file"], policy)
    worker = Worker(config, api, None)
    poller.cycle()
    worker.cycle()
    poller.cycle()
    assert api.comments[-1]["status"] == "WAIT_APPROVAL"
    assert api.comments[-1]["error_code"] == "LOCAL_APPROVAL_REQUIRED"


@pytest.mark.parametrize(("status", "code", "error"), [
    ("observed_with_limits", 0, None),
    ("partial", 0, "AUDIT_PARTIAL"),
    ("no_real_data", 2, "AUDIT_NO_REAL_DATA"),
    ("observed_with_limits", 2, "AUDIT_EXIT_STATUS_MISMATCH"),
])
def test_audit_runs_fixed_command_and_reports_summary(setup, monkeypatch, status, code, error):
    job, api, fixture, policy, config, poller = setup
    atomic_json(config["policy_file"], policy)
    (fixture / "settled_bets.csv").touch()
    workspace = Path(config["validation_root"]) / job["job_id"]
    output = workspace / "output"
    source = workspace / "checkout" / job["commit_sha"]

    class AuditCommand:
        def run(self, args, *, cwd, env, control, log, check):
            assert args == [sys.executable, str(source / "scripts/full_feature_audit.py"),
                            "--data-dir", str(fixture), "--output-dir", str(output),
                            "--start-date", "2026-08-09", "--end-date", "2026-08-09",
                            "--settled-bets", str(fixture / "settled_bets.csv")]
            assert cwd == source
            assert log == workspace / "execution.log"
            assert check is False
            (output / "run_manifest.json").write_text(json.dumps({"status": status, "data_dir": str(fixture)}))
            (output / "real_audit_details.csv").write_text(
                "date,race_id,horse_number,horse,file,history_row,metric\n"
                "2026-08-09,123,1,private-horse,book.xlsx,2,date_parsing\n"
                "2026-08-09,123,1,private-horse,book.xlsx,2,include_before_raceday\n")
            (output / "feature_audit_values.csv").write_text(
                "date,race_id,horse_number,horse\n2026-08-09,123,1,private-horse\n")
            (output / "real_audit_summary.csv").write_text(
                "requirement,evaluated,issues\nD-01,2,1\nD-01,3,1\n")
            return code

    def checkout(sha, job_dir, control, *, checkout_root):
        assert sha == job["commit_sha"]
        assert job_dir == workspace
        assert checkout_root == workspace / "checkout"
        return source

    backend = Backend(config, commands=AuditCommand())
    monkeypatch.setattr(backend, "checkout", checkout)
    monkeypatch.setattr(backend, "verify_checkout", lambda *args: None)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    poller.cycle()
    report = api.comments[-1]
    assert report["status"] == ("FAILED" if error else "SUCCEEDED")
    assert report["error_code"] == error
    summary = report["summary"]
    assert summary["audit_status"] == status
    assert summary["exit_code"] == code
    assert summary["elapsed_seconds"] >= 0
    if error != "AUDIT_EXIT_STATUS_MISMATCH":
        assert summary["races"] == summary["horses"] == summary["past_runs"] == 1
        assert len(summary["requirements"]) == 12
        assert summary["requirements"]["D-01"] == {"evaluated": 5, "issues": 2, "issue_pct": 40.0}
        assert summary["requirements"]["D-12"]["issue_pct"] is None
    assert str(fixture) not in json.dumps(report)
    assert "private-horse" not in json.dumps(report)
