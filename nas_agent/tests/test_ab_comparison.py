from datetime import date, timedelta
import json
import os
from pathlib import Path
import sys

import openpyxl
import pytest

from nas_agent.backend import AGENT_CODE, Backend, Commands
from nas_agent.comparison import sha256
from nas_agent.files import atomic_json
from nas_agent.poller import Poller
from nas_agent.schema import COMPARISON_SHA, parse_job, request, utcnow
from nas_agent.worker import Worker
from tools.prepare_ab_comparison import prepare

from test_full_feature_audit import EXAMPLES, JobIssue


def make_fixture(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    workbook = archive / "day.xlsx"
    rid = "202605050101"
    book = openpyxl.Workbook()
    now = book.active
    now.title = "今走レース情報"
    now.append(["レースID", "馬番", "馬名", "頭数", "場所", "コース", "クラス"])
    history = book.create_sheet(rid)
    history.append(["馬名", "日付", "着順", "レース名", "上り", "ラップタイム", "競走状態"])
    cached = book.create_sheet("horses")
    cached.append(["rid_str", "馬番", "master_rating"])
    for number in range(1, 7):
        name = f"試験馬{number}"
        now.append([rid, number, name, 6, "東京", "芝1600", "試験GII"])
        history.append([name, "2026/8/5", number, "試験GII", 32 + number, "11-11-12-12-12-12"])
        history.append([name, "2026-07-26", 1, "試験GI", 32, "12-12-12-11-11-12"])
        history.append([name, "2026-07-24", "中止", None, None, None, "競走中止"])
        history.append([name, "2026-07-23", "取消"])
        cached.append([rid, number, [40, 30, 20, 10, 5, 0][number - 1]])
    book.save(workbook)
    snapshot = {"race_id": rid, "workbook": "archive/day.xlsx",
                "start_at": "2026-08-09T12:00:00+09:00", "observed_at": "2026-08-09T11:55:00+09:00",
                "popularity": {str(number): number for number in range(1, 7)}}
    index = tmp_path / "index.json"
    settled = tmp_path / "settled.xlsx"
    finalized_at = "2026-08-09T12:20:00+09:00"
    atomic_json(index, {"snapshots": [snapshot], "finalized_at": {rid: finalized_at}})
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.append(["レースID", "馬名", "馬番", "着順"])
    for number in range(1, 7):
        sheet.append([rid, f"試験馬{number}", number, number])
    pay = book.create_sheet("払戻")
    pay.append(["レースID", "払戻種別", "組番", "払戻金"])
    for kind, combo, amount in [("単勝", "1", 200), ("複勝", "1", 120), ("複勝", "2", 130),
                                ("3連複", "1-2-3", 500)]:
        pay.append([rid, kind, combo, amount])
    book.save(settled)

    fixture = tmp_path / "fixture"
    digest = prepare(index, settled, fixture, date(2026, 8, 9))
    hashes = json.loads((fixture / "manifest.json").read_text())["files_sha256"]
    assert hashes["archive/source/snapshot_index.json"] == sha256(index)
    assert hashes["archive/source/settled.xlsx"] == sha256(settled)
    assert hashes["archive/inputs/day.xlsx"] == sha256(workbook)
    result = json.loads((fixture / "results.json").read_text())[0]
    assert result["finish_top3"] == [1, 2, 3]
    assert result["payouts"]["3連複"] == {"1-2-3": 500}
    return fixture, digest


@pytest.fixture
def comparison_job(tmp_path, monkeypatch):
    monkeypatch.setattr("nas_agent.files.validate_admin_file", lambda *args: None)
    job = parse_job((EXAMPLES / "ab_comparison.job.json").read_text())
    job["expires_at"] = (utcnow() + timedelta(hours=1)).isoformat()
    api = JobIssue({"number": 35, "title": "[NAS-JOB] " + job["job_id"], "body": json.dumps(job),
                    "state": "open", "user": {"id": 42, "login": "test-user"}})
    fixture, digest = make_fixture(tmp_path)
    policy = json.loads((EXAMPLES / "policy.json").read_text())
    policy.update(production_root=str(tmp_path / "production"), requester_ids=[42], allow_build=False)
    policy["job_types"].append("ab_comparison")
    policy["approved_commits"].append(COMPARISON_SHA)
    policy["approved_requests"].append(request(api.document)[1])
    policy["inputs"][job["input_id"]] = {
        "raceday": job["raceday"], "root": str(fixture), "manifest_sha256": digest,
    }
    config = {key: str(tmp_path / value) for key, value in {
        "state_dir": "state", "spool_dir": "spool", "policy_file": "policy.json",
        "checkout_root": "checkout", "validation_root": "validation",
    }.items()}
    config.update(instance_id="0123456789abcdef", validation_python=sys.executable)
    Path(config["validation_root"]).mkdir()
    return job, api, config, policy, fixture


def execute(data, backend):
    _, api, config, policy, _ = data
    atomic_json(config["policy_file"], policy)
    poller = Poller({**config, "state_dir": str(Path(config["state_dir"]).parent / "poller")}, api)
    worker = Worker(config, api, backend)
    poller.cycle()
    worker.cycle()
    poller.cycle()
    return api.comments[-1]


@pytest.mark.parametrize("tamper", ["manifest.json", "run.json", "archive/inputs/day.xlsx"])
def test_sha_mismatch_stops_before_checkout(comparison_job, monkeypatch, tamper):
    _, _, config, _, fixture = comparison_job
    with (fixture / tamper).open("ab") as handle:
        handle.write(b" ")
    backend = Backend(config)
    monkeypatch.setattr(backend, "checkout", lambda *args, **kwargs: pytest.fail("checkout ran"))
    report = execute(comparison_job, backend)
    assert report["status"] == "FAILED"
    assert report["error_code"] == ("INPUT_MANIFEST_MISMATCH" if tamper == "manifest.json"
                                    else "COMPARISON_INPUT_SHA_MISMATCH")
    assert report["artifacts_sha256"] == {}


def test_unapproved_comparison_input_waits(comparison_job):
    job, _, _, policy, _ = comparison_job
    del policy["inputs"][job["input_id"]]
    report = execute(comparison_job, None)
    assert report["status"] == "WAIT_APPROVAL"


@pytest.mark.parametrize("case", ["success", "parity", "oversize", "output_sha"])
def test_comparison_artifacts(comparison_job, monkeypatch, case):
    job, _, config, _, _ = comparison_job
    workspace = Path(config["validation_root"]) / job["job_id"]
    output = workspace / "output"
    source = workspace / "checkout" / COMPARISON_SHA

    class ComparisonCommand:
        def run(self, args, *, cwd, env, control, log, check):
            assert args == [sys.executable, "-m", "tools.compare_ab_models",
                            "--config", str(workspace / "input/run.json"), "--output", str(output)]
            assert cwd == source
            assert env["KEIBA_SCORING_MODEL_VERSION"] == "legacy"
            assert log == workspace / "execution.log"
            assert not check
            for name in ("comparison.json", "comparison.csv", "exclusions.json"):
                (output / name).write_text("[]")
            inputs = {key: sha256(workspace / "input" / name) for key, name in {
                "config_sha256": "run.json", "snapshots_sha256": "snapshots.json", "results_sha256": "results.json",
            }.items()}
            hashes = {path.name: sha256(path) for path in output.iterdir()}
            atomic_json(output / "manifest.json", {"code_sha": COMPARISON_SHA, "inputs": inputs, "output_sha256": hashes})
            if case == "oversize":
                monkeypatch.setattr("nas_agent.comparison.MAX_ARTIFACT_BYTES", 1)
            if case == "output_sha":
                (output / "comparison.json").write_text("[1]")
            return 2 if case == "parity" else 0

    def checkout(sha, job_dir, control, *, checkout_root, full_history):
        assert (sha, job_dir, checkout_root) == (COMPARISON_SHA, workspace, workspace / "checkout")
        assert full_history
        return source

    backend = Backend(config, commands=ComparisonCommand())
    monkeypatch.setattr(backend, "checkout", checkout)
    monkeypatch.setattr(backend, "verify_checkout", lambda *args: None)
    report = execute(comparison_job, backend)
    assert report["status"] == ("SUCCEEDED" if case == "success" else "FAILED")
    assert report["error_code"] == {
        "success": None, "parity": "COMPARISON_PARITY_FAILED",
        "oversize": "COMPARISON_ARTIFACT_LIMIT", "output_sha": "COMPARISON_OUTPUT_SHA_MISMATCH",
    }[case]
    assert report["artifacts_sha256"] == (
        {path.name: sha256(path) for path in output.iterdir()} if case in {"success", "parity"} else {}
    )


@pytest.mark.skipif(os.getenv("JIBUNNYOU_AB_INTEGRATION") != "1", reason="requires a local copy of PR #34 and its dependencies")
def test_fixed_comparator_with_synthetic_workbooks(comparison_job):
    job, _, config, _, _ = comparison_job
    class LocalFetch(Commands):
        def run(self, args, **kwargs):
            if args[0] == "git" and args[-3:] == ["fetch", "origin", COMPARISON_SHA]:
                args = [*args[:-2], str(AGENT_CODE), args[-1]]
            return super().run(args, **kwargs)

    report = execute(comparison_job, Backend(config, commands=LocalFetch()))
    workspace = Path(config["validation_root"]) / job["job_id"]
    log = workspace / "execution.log"
    assert report["status"] == "SUCCEEDED", (report, log.read_text() if log.exists() else "")
    assert len(report["artifacts_sha256"]) == 17
    output = workspace / "output"
    comparison = json.loads((output / "comparison.json").read_text())
    assert {row["method"] for row in comparison} == set("ABCD")
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["coverage"]["train"]["loaded_races"] == 1
