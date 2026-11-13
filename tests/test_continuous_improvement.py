"""固定保存・未確定結果・日付分離・再開の境界を検証する。"""
from datetime import datetime
import json
from pathlib import Path

import pandas as pd
import pytest

from continuous_improvement.archive import JST, freeze, sha256, verify_archive
from continuous_improvement.review import load_results, review


@pytest.fixture
def race(tmp_path):
    # 全成果物を一時フォルダに作り、既存データは変更しない。
    day = "20260809"
    for folder in ("data/input", "data/output/status", "data/master", "data/ozzu_csv"):
        (tmp_path / folder).mkdir(parents=True)
    entries = pd.DataFrame({"レースID": ["202601010501"] * 3, "馬番": [1, 2, 3],
                            "発走時刻": ["10:00"] * 3, "場所": ["札幌"] * 3,
                            "コース": ["芝1600"] * 3, "クラス": ["1勝クラス"] * 3,
                            "馬場": ["良"] * 3})
    entries.to_excel(tmp_path / f"data/input/馬の競走成績_{day}.xlsx", sheet_name="今走レース情報", index=False)
    prediction = pd.DataFrame({"rid_str": ["202601010501"] * 3, "馬番": [1, 2, 3],
                               "rank": [1, 2, 3], "total": [300., 200., 100.],
                               "特徴量A": [90., 60., 30.], "人気": [1, 2, 3], "単勝オッズ": [2., 4., 6.]})
    output = tmp_path / f"data/output/馬の競走成績_with_feat_{day}.xlsx"
    with pd.ExcelWriter(output) as writer:
        prediction.to_excel(writer, sheet_name="TARGET", index=False)
        entries.to_excel(writer, sheet_name="今走レース情報", index=False)
    (tmp_path / f"data/ozzu_csv/OZZU_{day}.csv").write_text("odds\n2\n", encoding="utf-8")
    results = pd.DataFrame({"レースID": ["202601010501"] * 3, "馬番": [1, 2, 3],
                            "着順": [1, 2, "中止"], "払戻種別": ["単勝", "複勝", "複勝"],
                            "組番": [1, 1, 2], "払戻金": [200, 110, 140]})
    results.to_excel(tmp_path / "results.xlsx", sheet_name=day, index=False)
    results.to_excel(tmp_path / "data/master/racedata_results.xlsx", sheet_name="20260808", index=False)
    (tmp_path / f"data/output/status/READY_{day}.json").write_text(json.dumps({"status": "READY", "sha256": sha256(output)}), encoding="utf-8")
    metadata = {"runtime_id": "test", "code_hashes": {}, "features": ["feature_a"],
                "feature_names": {"feature_a": "特徴量A"}, "scoring_model_version": "legacy",
                "weights": {"common": {"__default__": {"feature_a": 1.}}, "surface": {}}}
    return tmp_path, day, metadata


def test_pre_is_immutable_and_preserves_original(race):
    root, day, metadata = race
    pre = freeze(root, day, metadata, now=datetime(2026, 8, 9, 8, tzinfo=JST))
    before = (pre / "prediction.xlsx").read_bytes()
    assert freeze(root, day, metadata) == pre
    assert (pre / "prediction.xlsx").read_bytes() == before
    (pre / "odds.csv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA不一致"):
        verify_archive(pre)


def test_late_pre_is_rejected_and_backfill_is_separated(race):
    root, day, metadata = race
    with pytest.raises(ValueError, match="発走後"):
        freeze(root, day, metadata, now=datetime(2026, 8, 9, 10, tzinfo=JST))
    pre = freeze(root, day, metadata, retrospective=True)
    assert verify_archive(pre)["mode"] == "retrospective"


def test_full_review_preserves_pre_and_counts_dnf_as_loss(race):
    root, day, metadata = race
    pre = freeze(root, day, metadata, retrospective=True)
    before = sha256(pre / "prediction.xlsx")
    out = review(pre, root / "results.xlsx")
    report = json.loads((out / "review.json").read_text(encoding="utf-8"))
    assert report["overall"]["win_roi"] == 2.0
    assert report["overall"]["place_roi"] == 1.1
    assert report["overall"]["horses"] == 3
    assert report["legacy_forward_generation"] is None
    assert report["existing_ticket_roi"] is None
    assert sha256(pre / "prediction.xlsx") == before
    assert not report["stratified"][0]["sample_sufficient"]
    assert pd.read_csv(out / "race_horse_results.csv")["result_status"].tolist() == ["FINISHED", "FINISHED", "DNF"]


def test_incomplete_result_never_becomes_success(race):
    root, day, metadata = race
    pre = freeze(root, day, metadata, retrospective=True)
    path = root / "results.xlsx"
    df = pd.read_excel(path)
    df.iloc[:2].to_excel(path, sheet_name=day, index=False)
    with pytest.raises(ValueError, match="全レース結果未完了"):
        review(pre, path)


def test_duplicate_and_unknown_results_are_rejected(race):
    root, day, _ = race
    path = root / "results.xlsx"
    df = pd.read_excel(path)
    df.loc[2, "着順"] = "未確定"
    df.to_excel(path, sheet_name=day, index=False)
    with pytest.raises(ValueError, match="未確定"):
        load_results(path, day)


def test_ready_hash_mismatch_stops_freeze(race):
    root, day, metadata = race
    path = root / f"data/output/馬の競走成績_with_feat_{day}.xlsx"
    with path.open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(ValueError, match="READY"):
        freeze(root, day, metadata, now=datetime(2026, 8, 9, 8, tzinfo=JST))


def test_result_popularity_headers_do_not_collide(race):
    # 既存マスタには出走馬人気「人 気」と払戻人気「人気」が共存する。
    root, day, _ = race
    path = root / "results.xlsx"
    frame = pd.read_excel(path)
    frame["人 気"] = [1, 2, 3]
    frame["人気"] = ["1人気", "1人気", "2人気"]
    frame.to_excel(path, sheet_name=day, index=False)
    horses, _ = load_results(path, day)
    assert len(horses) == 3


def test_automatic_optimizer_keeps_approved_candidate_in_shadow(tmp_path):
    # ゲート合格候補であっても、自動運用モードはbestへ発行しない。
    from tokutyouryou_keisann.runner import _publish_candidate_and_best
    candidate = tmp_path / "candidate_feature_weights_20261113.py"
    best = tmp_path / "best_feature_weights_20261113.py"
    result = _publish_candidate_and_best(candidate, best, {"__default__": {"avg_finish": -1.}}, "adopted", shadow_only=True)
    assert result[0] == "shadow_only"
    assert candidate.exists()
    assert not best.exists()
    assert not result[3]


def test_outside_raceday_is_quarantined_without_changing_source(race):
    root, day, metadata = race
    pre = freeze(root, day, metadata, retrospective=True)
    path = root / "results.xlsx"
    frame = pd.read_excel(path)
    extra = frame.copy()
    extra["レースID"] = "202601010401"
    pd.concat([frame, extra]).to_excel(path, sheet_name=day, index=False)
    before = sha256(path)
    out = review(pre, path)
    report = json.loads((out / "review.json").read_text(encoding="utf-8"))
    assert report["unrelated_result_races_quarantined"] == ["202601010401"]
    assert report["all_races"] == 1
    assert sha256(path) == before


def test_existing_fixed_tickets_are_settled_without_regeneration(race):
    from continuous_improvement.review import settle_existing
    root, day, _ = race
    workbook = root / f"data/output/馬の競走成績_with_feat_{day}.xlsx"
    ticket = {"レースID": "202601010501", "3連複_金額": 100,
              "3連複1点目_馬番1": 1, "3連複1点目_馬番2": 2, "3連複1点目_馬番3": 3}
    with pd.ExcelWriter(workbook, engine="openpyxl", mode="a") as writer:
        pd.DataFrame([ticket]).to_excel(writer, sheet_name="回収率重視_買い目候補", index=False)
    horses, _ = load_results(root / "results.xlsx", day)
    settled = settle_existing(workbook, {("202601010501", "3連複", "1-2-3"): 1000}, horses)
    assert settled["purchase_yen"] == 100
    assert settled["return_yen"] == 1000
    assert settled["roi"] == 10.0


def test_future_result_master_is_rejected(race):
    root, day, metadata = race
    pd.read_excel(root / "results.xlsx").to_excel(root / "data/master/racedata_results.xlsx", sheet_name=day, index=False)
    with pytest.raises(ValueError, match="対象日以降"):
        freeze(root, day, metadata, now=datetime(2026, 8, 9, 8, tzinfo=JST))


@pytest.mark.parametrize(("kind", "numbers", "expected"), [
    ("単勝", ["1", "2"], ["1", "2"]),
    ("複勝", ["2", "3", "4"], ["2", "3", "4"]),
    ("3連単", ["3", "2", "1"], ["3→2→1"]),
    ("3連複", ["1", "2", "3", "1", "2", "4"], ["1-2-3", "1-2-4"]),
])
def test_scraper_preserves_dead_heat_and_ordered_payouts(kind, numbers, expected):
    import importlib.util
    script = Path(__file__).resolve().parents[1] / "etc_py/10_kekka_scraper_20260102.py"
    spec = importlib.util.spec_from_file_location("result_scraper_test", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.payout_combinations(numbers, kind) == expected


def test_failed_morning_retries_are_durable_and_bounded(race, monkeypatch):
    from continuous_improvement import service
    root, day, _ = race
    (root / f"data/output/status/SCHEDULED_ATTEMPT_{day}.json").write_text('{"status":"FAILED"}', encoding="utf-8")
    calls = []
    def failed(*args, **kwargs):
        # 朝処理の失敗を模擬し、実際の収集や配信を起動しない。
        calls.append(args)
        raise RuntimeError("prediction failed")
    monkeypatch.setattr(service, "run", failed)
    record = {}
    service.recover_morning(root, datetime(2026, 8, 9, 8, tzinfo=JST), record)
    assert record["morning_retries"] == 1
    service.recover_morning(root, datetime(2026, 8, 9, 8, 1, tzinfo=JST), {})
    assert len(calls) == 1
    service.recover_morning(root, datetime(2026, 8, 9, 8, 16, tzinfo=JST), {})
    service.recover_morning(root, datetime(2026, 8, 9, 8, 32, tzinfo=JST), {})
    service.recover_morning(root, datetime(2026, 8, 9, 8, 48, tzinfo=JST), {})
    assert len(calls) == 3


def test_history_excludes_retrospective_and_deduplicates_model_day(tmp_path):
    from continuous_improvement.history import aggregate
    from continuous_improvement.archive import write_json
    sample = {"day": "20261114", "runtime_id": "runtime", "model_role": "production", "mode": "prospective",
              "overall": {"rank1_bets": 1, "races": 1, "win_return_yen": 200, "place_return_yen": 110},
              "stratified": [], "existing_ticket_roi": None}
    for version in ("one", "two"):
        write_json(tmp_path / f"prospective/20261114/post/runtime/{version}/review.json", sample)
    write_json(tmp_path / "retrospective/20261114/post/runtime/three/review.json", {**sample, "mode": "retrospective"})
    result = aggregate(tmp_path)
    assert result["models"][0]["races"] == 1
    assert result["models"][0]["win_roi"] == 2.0
    assert not result["production_promotion_allowed"]


def test_canonical_results_follow_entry_date_and_preserve_raw(race):
    from continuous_improvement.result_dates import canonicalize
    root, day, _ = race
    source = root / "mixed.xlsx"
    good = pd.read_excel(root / "results.xlsx")
    bad = good.copy()
    bad.loc[0, "払戻金"] = 999
    with pd.ExcelWriter(source) as writer:
        bad.to_excel(writer, sheet_name="20260808", index=False)
        good.to_excel(writer, sheet_name=day, index=False)
    before = sha256(source)
    out = root / "canonical.xlsx"
    report = canonicalize(source, [root / "data/input"], out)
    assert pd.ExcelFile(out).sheet_names == [day]
    assert pd.read_excel(out).loc[0, "払戻金"] == 200
    assert report["resolved_duplicates"]
    assert sha256(source) == before


def test_refund_does_not_treat_dnf_as_scratched(race):
    from continuous_improvement.review import settle_existing
    root, day, _ = race
    workbook = root / f"data/output/馬の競走成績_with_feat_{day}.xlsx"
    ticket = {"レースID": "202601010501", "3連複_金額": 100,
              "3連複1点目_馬番1": 1, "3連複1点目_馬番2": 2, "3連複1点目_馬番3": 3}
    with pd.ExcelWriter(workbook, engine="openpyxl", mode="a") as writer:
        pd.DataFrame([ticket]).to_excel(writer, sheet_name="回収率重視_買い目候補", index=False)
    horses, _ = load_results(root / "results.xlsx", day)
    lost = settle_existing(workbook, {("202601010501", "3連複", "4-5-6"): 1000}, horses)
    assert lost["return_yen"] == 0
    horses.loc[horses["馬番"].eq(3), "result_status"] = "SCRATCHED"
    refund = settle_existing(workbook, {}, horses)
    assert refund["return_yen"] == 100


def test_optimizer_waits_for_forward_profit_sample(tmp_path):
    from continuous_improvement.optimization import maybe_run
    result = maybe_run(tmp_path, datetime(2026, 11, 13, 8, tzinfo=JST))
    assert result["status"] == "WAITING_SAMPLE"
    assert not result["auto_promote"]


def test_only_validated_candidate_can_activate_in_isolated_shadow(tmp_path):
    import shutil
    from continuous_improvement.optimization import activate_candidate
    from continuous_improvement.archive import write_json
    project = Path(__file__).resolve().parents[1]
    sandbox = tmp_path / "optimization/job/source"
    sandbox.mkdir(parents=True)
    for path in project.glob("*.py"):
        shutil.copy2(path, sandbox / path.name)
    for folder in ("config", "yosou_py", "tokutyouryou_keisann"):
        shutil.copytree(project / folder, sandbox / folder)
    original = (project / "config/production_model.json").read_bytes()
    spec = json.loads(original)
    candidate = sandbox / "yosou_py/candidate_feature_weights_test.py"
    shutil.copy2(project / spec["weight_file"], candidate)
    decision_path = sandbox / "data/output/weight_adoption/decision_test.json"
    decision = {"validation_decision": "rejected", "candidate_roundtrip_match": True,
                "candidate_weight_file": str(candidate), "candidate_weight_sha256": sha256(candidate),
                "reloaded_candidate_normalized_sha256": spec["effective_sha256"]}
    write_json(decision_path, decision)
    assert activate_candidate(tmp_path, sandbox) is None
    decision["validation_decision"] = "adopted"
    write_json(decision_path, decision)
    active = activate_candidate(tmp_path, sandbox)
    assert active["production_promoted"] is False
    assert Path(active["source"]) != sandbox
    assert Path(active["source"]).is_relative_to(sandbox.parent)
    assert (project / "config/production_model.json").read_bytes() == original


def test_master_pair_rolls_back_both_files_after_final_validation_failure(tmp_path):
    from continuous_improvement.bootstrap import publish_pair
    master = tmp_path / "data/master"
    master.mkdir(parents=True)
    (master / "racedata_results.xlsx").write_bytes(b"old results")
    (master / "race_levels.xlsx").write_bytes(b"old levels")
    results, levels = tmp_path / "new_results.xlsx", tmp_path / "new_levels.xlsx"
    results.write_bytes(b"new results")
    levels.write_bytes(b"new levels")
    calls = []
    def validator(path, day):
        # 置換後の検査に失敗させ、実際に両マスタが元へ戻ることを確認する。
        calls.append(path)
        if len(calls) > 1:
            raise ValueError("final validation failed")
        return {"races": 1}
    with pytest.raises(ValueError, match="final validation"):
        publish_pair(tmp_path, results, levels, "20260809", validator=validator)
    assert (master / "racedata_results.xlsx").read_bytes() == b"old results"
    assert (master / "race_levels.xlsx").read_bytes() == b"old levels"


def test_master_update_preserves_existing_access_metadata(tmp_path, monkeypatch):
    import os
    import shutil
    import stat
    from continuous_improvement.bootstrap import publish_pair
    master = tmp_path / "data/master"
    master.mkdir(parents=True)
    sources = []
    for name in ("racedata_results.xlsx", "race_levels.xlsx"):
        target = master / name
        target.write_bytes(b"old")
        target.chmod(0o644)
        if os.name == "posix" and os.getuid() == 0:
            os.chown(target, 1004, 10)
        source = tmp_path / name
        source.write_bytes(b"new")
        source.chmod(0o600)
        sources.append(source)
    calls = []
    original = shutil.copystat
    def spy(source, destination, **kwargs):
        # 新しい内容の権限ではなく、置換前の本番マスタのメタデータを使うことを検査する。
        calls.append((Path(source), Path(destination)))
        return original(source, destination, **kwargs)
    monkeypatch.setattr(shutil, "copystat", spy)
    publish_pair(tmp_path, *sources, "20260809", validator=lambda *args: {"races": 1})
    for source in sources:
        target = master / source.name
        assert target.read_bytes() == b"new"
        assert (target, master / (source.name + ".bootstrap.tmp")) in calls
        if os.name == "posix":
            assert stat.S_IMODE(target.stat().st_mode) == 0o644
            if os.getuid() == 0:
                assert (target.stat().st_uid, target.stat().st_gid) == (1004, 10)


def test_shadow_market_completion_preserves_ranks_and_raw_popularity(race, monkeypatch):
    import importlib
    from continuous_improvement.shadow import complete_market_columns
    root, day, _ = race
    prediction = root / f"data/output/馬の競走成績_with_feat_{day}.xlsx"
    target = pd.read_excel(prediction, sheet_name="TARGET")
    target["馬名"] = ["A", "B", "C"]
    target["score"] = [70., 60., 50.]
    target["人気"] = [90, 60, 30]
    now = pd.read_excel(prediction, sheet_name="今走レース情報")
    now["馬名"] = ["A", "B", "C"]
    with pd.ExcelWriter(prediction, engine="openpyxl") as writer:
        target.to_excel(writer, sheet_name="TARGET", index=False)
        now.to_excel(writer, sheet_name="今走レース情報", index=False)
    pd.DataFrame({"date": [day] * 3, "racecourse": ["札幌"] * 3, "race": ["1R"] * 3,
                  "name": ["A", "B", "C"], "bet_type": ["単勝"] * 3,
                  "combination": [1, 2, 3], "odds": [2., 4., 6.]}).to_csv(root / "odds.csv", index=False)
    runner = importlib.import_module("1_keibayosou_best_import_roi_runner")
    monkeypatch.setattr(runner, "ODDS_CSV", runner.ODDS_CSV)
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    seen = []
    def inspect(path):
        # 買い目生成より前に、0〜100の平均人気ではなく実際の人気が入っていることを検証する。
        seen.extend(pd.read_excel(path, sheet_name="今走レース情報")["人気"].tolist())
    monkeypatch.setattr(pipeline, "append_roi_focus_bet_sheet_to_excel", inspect)
    complete_market_columns(root, prediction, day)
    assert seen == [1, 2, 3]
    assert pd.read_excel(prediction, sheet_name="TARGET")["rank"].tolist() == [1, 2, 3]


@pytest.mark.parametrize("already_saved", [False, True])
def test_late_shadow_is_missed_but_valid_saved_shadow_can_resume(race, monkeypatch, already_saved):
    import shutil
    from continuous_improvement import shadow
    from continuous_improvement.archive import write_json
    root, day, metadata = race
    pre = freeze(root, day, metadata, now=datetime(2026, 8, 9, 8, tzinfo=JST))
    class Clock:
        @staticmethod
        def now(tz):
            # 発走後に再起動した状況を固定し、テスト実行日の時計に依存させない。
            return datetime(2026, 8, 9, 10, tzinfo=JST)
    monkeypatch.setattr(shadow, "datetime", Clock)
    if already_saved:
        adapter = Path(shadow.__file__)
        destination = pre.parent / ("shadow_pre_" + sha256(adapter)[:12])
        destination.mkdir()
        shutil.copy2(pre / "prediction.xlsx", destination / "prediction.xlsx")
        shutil.copy2(pre / "runtime.json", destination / "runtime.json")
        shutil.copy2(adapter, destination / "prediction_adapter.py")
        write_json(destination / "manifest.json", {"prediction_sha256": sha256(destination / "prediction.xlsx"),
                   "runtime_sha256": sha256(destination / "runtime.json"), "adapter_sha256": sha256(adapter),
                   "production_pre_sha256": sha256(pre / "manifest.json")})
        assert shadow.predict(pre) == destination
    else:
        with pytest.raises(ValueError, match="MISSED"):
            shadow.predict(pre)
