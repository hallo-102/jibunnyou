from datetime import date

import pandas as pd
import pytest

from tools.ab_comparison_inputs import load_races, read_results, training_set
from tools.ab_comparison_metrics import race_metrics
from tools.compare_ab_models import read_json, save_json
from tools.prepare_ab_comparison import prepare


DAY = "2026-08-08"
PAYOUTS = {"単勝": {"1": 250}, "3連複": {"1-2-3": 1000}}


def result_row(grade, number=1, available_by=DAY):
    row = {
        "race_id": f"2026050501{number:02}",
        "race_date": DAY,
        "finish_top3": [1, 2, 3],
        "payouts": PAYOUTS,
        "evidence_grade": grade,
    }
    if grade == "A":
        row["finalized_at"] = f"{DAY}T12:30:00+09:00"
    if grade == "B":
        row.update(available_by_date=available_by, evidence_source="NAS保存台帳の記録ID")
    if grade == "C":
        row["evidence_source"] = "JRA公式結果の保存資料"
        row["jra_official_result"] = {key: row[key] for key in ("race_id", "race_date", "finish_top3", "payouts")}
    return row


def race_frame():
    return pd.DataFrame({
        "馬番": [1, 2, 3], "人気": [1, 2, 3], "distance_m": 1600,
        "place_name": "東京", "race_class": "試験", "name_norm": ["a", "b", "c"],
    })


@pytest.mark.parametrize("grade", ["B", "C", "D"])
def test_retrospective_settlement(grade):
    row = result_row(grade)
    results = read_results([row], {})
    metrics = race_metrics(race_frame(), results.get(row["race_id"]), 100, 3)
    assert metrics["win_hit"] is (True if grade != "D" else None)
    assert metrics["ticket_payout"] == (1000 if grade != "D" else None)


def test_training_cutoff_uses_proven_availability():
    rows = [
        result_row("A", 1), result_row("B", 2), result_row("B", 3, "2026-08-09"),
        result_row("B", 4, "2026-08-10"), result_row("C", 5), result_row("D", 6),
    ]
    frame = race_frame()
    races = {row["race_id"]: {"stage": "train", "frames": {"legacy": frame}} for row in rows}
    results = read_results(rows, {})
    _, entries = training_set(races, results, date(2026, 8, 9), "legacy")
    assert entries["rid_str"].unique().tolist() == [row["race_id"] for row in rows[:2]]


def test_official_result_must_match_and_date_is_not_a_timestamp():
    row = result_row("C")
    row["jra_official_result"]["payouts"] = {"単勝": {"1": 260}}
    with pytest.raises(ValueError, match="JRA"):
        read_results([row], {})
    row = result_row("B")
    row["available_by_date"] += "T00:00:00+09:00"
    with pytest.raises(ValueError):
        read_results([row], {})


def test_prepare_records_evidence_without_inventing_times(tmp_path):
    rows = [result_row(grade, i) for i, grade in enumerate("ABCD", 1)]
    snapshots = [{"race_id": row["race_id"], "input_evidence_grade": grade,
                  "start_at": f"{DAY}T12:00:00+09:00"} for grade, row in zip("ABCD", rows)]
    for snapshot in snapshots[:2]:
        snapshot.update(observed_at=f"{DAY}T11:50:00+09:00", workbook="snapshot.xlsx")
    (tmp_path / "snapshot.xlsx").write_bytes(b"archived bytes")
    config = tmp_path / "config.json"
    save_json(config, {"snapshots": "snapshots.json", "results": "results.json"})
    save_json(tmp_path / "snapshots.json", snapshots)
    save_json(tmp_path / "results.json", rows)
    output = tmp_path / "fixture"
    manifest = prepare(config, output)
    assert read_json(output / "manifest.json") == manifest
    assert list(manifest["snapshot_evidence_grades"].values()) == list("ABCD")
    proofs = list(manifest["result_evidence"].values())
    assert proofs[0]["finalized_at"] == f"{DAY}T12:30:00+09:00"
    assert all(proof["finalized_at"] is None for proof in proofs[1:])
    assert [proof["available_by_date"] for proof in proofs] == [None, DAY, None, None]
    assert read_json(output / "results.json") == rows
    assert "archive/0.xlsx" in manifest["files_sha256"]


def test_unverified_inputs_stay_out_of_strict_comparison(tmp_path):
    day = date.fromisoformat(DAY)
    snapshots = [
        {"race_id": result_row(grade, index)["race_id"], "input_evidence_grade": grade,
         "start_at": f"{DAY}T12:00:00+09:00"}
        for index, grade in enumerate("BCD", 1)
    ]
    races, _, _, excluded = load_races(tmp_path, snapshots, {"validation": (day, day)}, {})
    assert not races
    assert [row["reason"] for row in excluded] == [f"input_evidence_grade_{grade}" for grade in "BCD"]


def test_prepare_defaults_missing_input_grade_to_c_even_for_legacy_config(tmp_path):
    rid = result_row("A")["race_id"]
    config = tmp_path / "config.json"
    save_json(config, {"schema_version": 1, "snapshots": "snapshots.json", "results": "results.json"})
    save_json(tmp_path / "snapshots.json", [{"race_id": rid, "start_at": f"{DAY}T12:00:00+09:00",
                                            "observed_at": f"{DAY}T11:55:00+09:00"}])
    save_json(tmp_path / "results.json", [])
    output = tmp_path / "fixture"
    manifest = prepare(config, output)
    assert manifest["schema_version"] == 2
    assert manifest["snapshot_evidence_grades"] == {rid: "C"}
    assert read_json(output / "run.json")["schema_version"] == 2
    assert read_json(output / "snapshots.json")[0]["input_evidence_grade"] == "C"


def test_reference_does_not_load_b_for_training_and_enforces_prestart_time(tmp_path):
    row = {"race_id": result_row("A")["race_id"], "input_evidence_grade": "B",
           "start_at": f"{DAY}T12:00:00+09:00", "observed_at": f"{DAY}T12:01:00+09:00"}
    day = date.fromisoformat(DAY)
    races, _, _, excluded = load_races(tmp_path, [row], {"train": (day, day)}, {}, reference=True)
    assert not races
    assert excluded[0]["reason"] == "input_evidence_grade_B"
    with pytest.raises(ValueError, match="発走前"):
        load_races(tmp_path, [row], {"validation": (day, day)}, {}, reference=True)


def test_reference_report_is_separate_from_formal_report(tmp_path):
    from model_registry import sha256_file
    from tools.ab_comparison_inputs import features
    from tools.compare_ab_models import run

    rows = [result_row("A", i) for i in range(1, 6)]
    snapshots = []
    path = tmp_path / "snapshot.xlsx"
    now = [{"レースID": row["race_id"], "馬番": n, "馬名": f"馬{n}", "頭数": 3,
            "場所": "東京", "コース": "芝1600", "クラス": "試験GII"}
           for row in rows for n in range(1, 4)]
    pd.DataFrame(now).to_excel(path, sheet_name=features.NOW_SHEET, index=False)
    for grade, row in zip(("A", "B", "C", "D", None), rows):
        snapshot = {"race_id": row["race_id"], "start_at": f"{DAY}T12:00:00+09:00",
                    "observed_at": f"{DAY}T11:55:00+09:00", "workbook": path.name,
                    "workbook_sha256": sha256_file(path), "popularity": {"1": 1, "2": 2, "3": 3}}
        if grade is not None:
            snapshot["input_evidence_grade"] = grade
        snapshots.append(snapshot)
    save_json(tmp_path / "snapshots.json", snapshots)
    save_json(tmp_path / "results.json", rows)
    config = {"schema_version": 2, "snapshots": "snapshots.json", "results": "results.json",
              "weight_mode": "ab_only", "box_size": 3}
    config_path = tmp_path / "config.json"
    output = tmp_path / "report"
    save_json(config_path, config)
    formal = run(config_path, output)
    original = (output / "comparison.csv").read_bytes()
    save_json(config_path, {**config, "evaluation_mode": "reference"})
    reference = run(config_path, output)

    assert formal["evaluation_mode"] == "formal"
    assert formal["inputs"]["snapshot_evidence_grades"][rows[-1]["race_id"]] == "C"
    assert reference["evaluation_mode"] == "reference"
    assert reference["validation_selection"]["status"] == "disabled_reference"
    assert read_json(output / "reference/validation_selection.json") == []
    assert (output / "comparison.csv").read_bytes() == original
    assert {row["input_evidence_grade"] for row in read_json(output / "races.json")} == {"A"}
    assert {row["input_evidence_grade"] for row in read_json(output / "reference/races.json")} == {"A", "B"}
    reference_summary = read_json(output / "reference/comparison.json")
    assert {row["evaluation_mode"] for row in reference_summary} == {"reference"}
    assert {row["races"] for row in reference_summary if row["stage"] == "validation"} == {2}
