# -*- coding: utf-8 -*-
"""本番モデルとシャドー比較モデルの分離を検証する回帰テスト。"""

from __future__ import annotations

import importlib
import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from model_registry import ModelConfigError, load_model_spec, sha256_file
from shadow_validation import (
    build_shadow_pre_payload,
    finalize_shadow_payload,
    load_forward_validation_policy,
    validate_shadow_isolation_policy,
)
from tokutyouryou_keisann import baseline


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_explicit_production_config_wins_even_when_newer_best_exists() -> None:
    """最新日付ファイルがあっても明示設定した20260730だけを本番にする。"""
    production_config = importlib.import_module("1_keibayosou_config")

    assert (PROJECT_ROOT / "yosou_py" / "best_feature_weights_20260817.py").is_file()
    assert production_config.ACTIVE_FEATURE_WEIGHTS_FILE == (
        "yosou_py/best_feature_weights_20260730.py"
    )


def test_default_baseline_uses_explicit_production_model() -> None:
    """採用履歴の最新モデルではなく、現在の本番モデルをbaselineにする。"""
    selection = baseline.select_baseline_weight_file()

    assert selection.path == (
        PROJECT_ROOT / "yosou_py" / "best_feature_weights_20260730.py"
    ).resolve()
    assert selection.method == "production_model_config"


def _write_minimal_weight_file(path: Path) -> None:
    """設定読込テスト用の最小bestファイルを書き出す。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "FEATURE_WEIGHTS = {'__default__': {'feature_a': 1.0}}\n"
        "FEATURE_WEIGHTS_BY_PLACE_SURFACE = {}\n",
        encoding="utf-8",
    )


def _write_model_config(root: Path, weight_file: str, file_sha256: str) -> Path:
    """設定ファイル単体の安全性テスト用JSONを作成する。"""
    config_path = root / "config" / "production_model.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "role": "production",
                "model_name": "best_feature_weights_20260730",
                "weight_file": weight_file,
                "file_sha256": file_sha256,
                "effective_sha256": "0" * 64,
                "activated_at": "2026-08-17T07:30:00+09:00",
                "activation_reason": "test",
                "previous_model_name": "previous",
                "scoring_model_version": "legacy",
                "expected_group_count": 1,
                "expected_element_count": 1,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return config_path


def test_missing_production_config_stops_without_fallback(tmp_path: Path) -> None:
    """設定不在時は最新bestへfallbackせず停止する。"""
    with pytest.raises(ModelConfigError, match="設定ファイルが存在しません"):
        load_model_spec(tmp_path, "config/production_model.json", expected_role="production")


def test_production_file_sha_mismatch_stops(tmp_path: Path) -> None:
    """本番bestのファイルSHAが不一致なら停止する。"""
    weight_path = tmp_path / "yosou_py" / "best_feature_weights_20260730.py"
    _write_minimal_weight_file(weight_path)
    config_path = _write_model_config(
        tmp_path,
        "yosou_py/best_feature_weights_20260730.py",
        "f" * 64,
    )

    with pytest.raises(ModelConfigError, match="SHA-256が設定と一致しません"):
        load_model_spec(tmp_path, config_path, expected_role="production")


@pytest.mark.parametrize(
    "weight_file",
    [
        "../outside/best_feature_weights_20260730.py",
        str((PROJECT_ROOT / "yosou_py" / "best_feature_weights_20260730.py").resolve()),
    ],
)
def test_production_path_must_be_project_relative_and_inside_root(
    tmp_path: Path,
    weight_file: str,
) -> None:
    """絶対パスとプロジェクト外参照は拒否する。"""
    config_path = _write_model_config(tmp_path, weight_file, "0" * 64)

    with pytest.raises(ModelConfigError):
        load_model_spec(tmp_path, config_path, expected_role="production")


def test_configured_production_file_sha_matches_actual_file() -> None:
    """固定した20260730の既知ファイルSHAと実ファイルが一致する。"""
    production_config = importlib.import_module("1_keibayosou_config")
    spec = production_config.PRODUCTION_MODEL_SPEC

    assert spec.file_sha256 == (
        "2fdaf28273c2f2bbcb0052a09227f4be6d32a7e4817121df168613995cc66f32"
    )
    assert sha256_file(spec.resolved_weight_path) == spec.file_sha256
    assert spec.effective_sha256 == (
        "6606ea4fa469e03ecc7a0000d031a914ca4ab10de04dfa132f0ba7d33ab70a3e"
    )


def test_optimizer_best_save_does_not_change_production_config(tmp_path: Path) -> None:
    """candidate/best保存と本番昇格を分離し、明示設定を変更しない。"""
    runner = importlib.import_module("tokutyouryou_keisann.runner")
    production_config_path = PROJECT_ROOT / "config" / "production_model.json"
    before = production_config_path.read_bytes()
    candidate_path = tmp_path / "candidate_feature_weights_20260818.py"
    best_path = tmp_path / "best_feature_weights_20260818.py"
    weights = {"__default__": {feature: 0.0 for feature in runner.FEAT_COLS}}

    decision, _, adopted_path, updated = runner._publish_candidate_and_best(
        candidate_path,
        best_path,
        weights,
        "adopted",
    )

    assert decision == "adopted"
    assert updated is True
    assert adopted_path == str(best_path)
    assert best_path.is_file()
    assert production_config_path.read_bytes() == before


def test_shadow_policy_rejects_auto_purchase_and_all_production_writes() -> None:
    """シャドーに自動購入または本番出力を許可する設定は停止する。"""
    base_policy = {
        "allow_production_rank_write": False,
        "allow_production_bet_write": False,
        "allow_auto_purchase": False,
        "allow_official_email": False,
        "allow_production_selection": False,
        "allow_optimizer_baseline": False,
        "allow_weight_optimization": False,
    }
    validate_shadow_isolation_policy(base_policy)

    for flag in base_policy:
        unsafe_policy = dict(base_policy)
        unsafe_policy[flag] = True
        with pytest.raises(ModelConfigError, match=flag):
            validate_shadow_isolation_policy(unsafe_policy)


def test_shadow_scores_and_bets_do_not_mutate_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """シャドーの順位・買い目を計算しても本番結果はセル単位で不変。"""
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    ranking = importlib.import_module("1_keibayosou_ranking")
    monkeypatch.setattr(pipeline, "FEAT_COLS", ["feature"])

    def fake_apply_weights(values, weights=None, place="", surface=""):
        """テスト用に指定重みだけを適用する。"""
        selected = 1.0 if weights is None else float(weights.get("feature", 0.0))
        return {"feature": float(values["feature"]) * selected}

    def fake_five_block(frame: pd.DataFrame) -> pd.DataFrame:
        """順位の共通同点解消列だけを付加する。"""
        out = frame.copy()
        out["risk_score"] = 0.0
        out["data_confidence"] = 100.0
        out["five_block_raw_score"] = out["total"]
        out["five_block_score_raw"] = out["score_raw"]
        out["five_block_rank"] = ranking.create_unique_rank_series(
            out,
            "rid_str",
            "five_block_score_raw",
            risk_score_col="risk_score",
            extra_penalty_col="extra_penalty",
            data_confidence_col="data_confidence",
            horse_number_col="馬番",
        )
        out["five_block_score"] = out["five_block_score_raw"].round(2)
        return out

    monkeypatch.setattr(pipeline, "apply_weights", fake_apply_weights)
    monkeypatch.setattr(pipeline, "score_sum", lambda values: float(values["feature"]))
    monkeypatch.setattr(pipeline, "calc_rest_dist_risk", lambda row: 0.0)
    monkeypatch.setattr(
        pipeline,
        "calc_extra_penalty_components",
        lambda row, rest_dist_risk: {
            "popular_underperformer": 0.0,
            "good_loser": 0.0,
            "ta_n": 0.0,
            "close_loss": 0.0,
            "rest_distance": 0.0,
        },
    )
    monkeypatch.setattr(pipeline, "compute_five_block_scores", fake_five_block)
    features = pd.DataFrame(
        {
            "rid_str": ["202608220101"] * 5,
            "馬番": [1, 2, 3, 4, 5],
            "馬名": ["A", "B", "C", "D", "E"],
            "feature": [5.0, 4.0, 3.0, 2.0, 1.0],
        }
    )
    production = pipeline.compute_scores_with_pipeline_logic(
        features,
        place_map={},
        surface_map={},
        calc_fav_risk=lambda row: 0.0,
        alpha=0.0,
        extra_alpha=0.0,
    )
    production_snapshot = production.copy(deep=True)
    production_bets = pipeline._build_bet_sheet(
        production,
        pd.DataFrame({"rid_str": ["202608220101"]}),
    )
    production_bets_snapshot = production_bets.copy(deep=True)

    shadow = pipeline.compute_scores_with_pipeline_logic(
        features.copy(deep=True),
        place_map={},
        surface_map={},
        calc_fav_risk=lambda row: 0.0,
        alpha=0.0,
        extra_alpha=0.0,
        weights_map={"__default__": {"feature": -1.0}},
    )
    shadow_bets = pipeline._build_bet_sheet(
        shadow,
        pd.DataFrame({"rid_str": ["202608220101"]}),
    )

    pdt.assert_frame_equal(production, production_snapshot, check_exact=True)
    pdt.assert_frame_equal(production_bets, production_bets_snapshot, check_exact=True)
    assert production_bets.loc[0, "1位馬番"] == 1
    assert shadow_bets.loc[0, "1位馬番"] == 5


def test_pre_result_shadow_payload_never_contains_finish_or_payout() -> None:
    """結果確定前JSONは着順・払戻・収支をnullに固定する。"""
    production_config = importlib.import_module("1_keibayosou_config")
    shadow_spec = load_model_spec(
        PROJECT_ROOT,
        PROJECT_ROOT / "config" / "shadow_model.json",
        expected_role="shadow",
    )
    predictions = pd.DataFrame(
        {
            "rid_str": ["202608220101"] * 5,
            "馬番": [1, 2, 3, 4, 5],
            "馬名": ["A", "B", "C", "D", "E"],
            "rank": [1, 2, 3, 4, 5],
            "score": [70.0, 65.0, 60.0, 55.0, 50.0],
            # 入力に結果らしい列があっても出力にコピーしない。
            "着順": [1, 2, 3, 4, 5],
            "払戻": [9999, 0, 0, 0, 0],
        }
    )
    bets = pd.DataFrame(
        {
            "レースID": ["202608220101"],
            "ランク(S/A/B)": ["A"],
            "判定": ["購入"],
            "gap12": [5.0],
            **{f"{rank}位馬番": [rank] for rank in range(1, 6)},
        }
    )
    empty_roi = pd.DataFrame(columns=["レースID"])

    payload = build_shadow_pre_payload(
        comparison_date="20260822",
        production_spec=production_config.PRODUCTION_MODEL_SPEC,
        shadow_spec=shadow_spec,
        production_predictions=predictions,
        shadow_predictions=predictions.copy(),
        production_bets=bets,
        shadow_bets=bets.copy(),
        production_roi_bets=empty_roi,
        shadow_roi_bets=empty_roi.copy(),
        compared_at="2026-08-22T09:00:00+09:00",
    )

    assert payload["result_status"] == "pre_result"
    assert payload["races"][0]["actual_finish"] is None
    assert payload["races"][0]["payout"] is None
    assert payload["races"][0]["production_profit"] is None
    assert payload["races"][0]["shadow_profit"] is None


def test_prediction_excel_and_json_record_production_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """予想Excel・サイドカーJSONへ本番モデル名と2種SHAを記録する。"""
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    source = tmp_path / "source.xlsx"
    output = tmp_path / "prediction.xlsx"
    pd.DataFrame({"source": [1]}).to_excel(source, index=False)
    predictions = pd.DataFrame(
        {
            "rid_str": ["202608220101"],
            "馬番": [1],
            "馬名": ["A"],
            "rank": [1],
            "score": [50.0],
            "score_raw": [50.0],
            "best_weight_rank": [1],
            "best_weight_score": [50.0],
            "five_block_rank": [1],
            "five_block_score": [50.0],
            "five_block_score_raw": [50.0],
        }
    )
    monkeypatch.setattr(pipeline, "validate_prediction_ranks", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline, "select_top5_predictions", lambda *args, **kwargs: predictions)
    monkeypatch.setattr(
        pipeline,
        "_build_feature_sheet_for_export",
        lambda frame, feat_cols, names: frame.copy(),
    )
    monkeypatch.setattr(
        pipeline,
        "build_feature_health_diagnostics",
        lambda *args, **kwargs: (
            pd.DataFrame(
                columns=["特徴量名", "現在の重み", "ランキングへの実質寄与"]
            ),
            pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(pipeline, "_build_bet_sheet", lambda *args, **kwargs: pd.DataFrame())

    pipeline.write_features_to_excel(
        str(source),
        str(output),
        predictions,
        predictions.copy(),
        pd.DataFrame(),
    )

    metadata_sheet = pd.read_excel(output, sheet_name=pipeline.MODEL_METADATA_SHEET)
    sidecar = json.loads(output.with_suffix(".model.json").read_text(encoding="utf-8"))
    for field, expected in pipeline.PRODUCTION_MODEL_METADATA.items():
        assert metadata_sheet.loc[0, field] == expected
        assert sidecar[field] == expected


def test_post_result_is_written_from_copy_and_settles_each_model() -> None:
    """preを変更せず、確定後だけ着順・払戻・モデル別収支を付加する。"""
    production_config = importlib.import_module("1_keibayosou_config")
    shadow_spec = load_model_spec(
        PROJECT_ROOT,
        PROJECT_ROOT / "config" / "shadow_model.json",
        expected_role="shadow",
    )
    predictions = pd.DataFrame(
        {
            "rid_str": ["202608220101"] * 5,
            "馬番": [1, 2, 3, 4, 5],
            "馬名": ["A", "B", "C", "D", "E"],
            "rank": [1, 2, 3, 4, 5],
            "score": [70.0, 65.0, 60.0, 55.0, 50.0],
        }
    )
    bets = pd.DataFrame(
        {
            "レースID": ["202608220101"],
            **{f"{rank}位馬番": [rank] for rank in range(1, 6)},
        }
    )
    production_roi = pd.DataFrame(
        {
            "レースID": ["202608220101"],
            "3連複1点目_馬番1": [1],
            "3連複1点目_馬番2": [3],
            "3連複1点目_馬番3": [2],
            "3連複2点目_馬番1": [1],
            "3連複2点目_馬番2": [3],
            "3連複2点目_馬番3": [4],
            "3連複3点目_馬番1": [1],
            "3連複3点目_馬番2": [3],
            "3連複3点目_馬番3": [5],
            "3連複_金額": [300],
        }
    )
    shadow_roi = production_roi.copy()
    shadow_roi.loc[0, "3連複1点目_馬番3"] = 4
    pre = build_shadow_pre_payload(
        comparison_date="20260822",
        production_spec=production_config.PRODUCTION_MODEL_SPEC,
        shadow_spec=shadow_spec,
        production_predictions=predictions,
        shadow_predictions=predictions.copy(),
        production_bets=bets,
        shadow_bets=bets.copy(),
        production_roi_bets=production_roi,
        shadow_roi_bets=shadow_roi,
    )
    before = deepcopy(pre)
    entries = pd.DataFrame(
        {
            "rid_str": ["202608220101"] * 5,
            "name_norm": ["A", "B", "C", "D", "E"],
            "着順_num": [1, 2, 3, 4, 5],
            "馬番_int": [1, 2, 3, 4, 5],
        }
    )
    payouts = pd.DataFrame(
        {
            "rid_str": ["202608220101"],
            "払戻種別": ["3連複"],
            "組番_norm": ["1-2-3"],
            "払戻金_int": [2400],
        }
    )

    post = finalize_shadow_payload(
        pre,
        entries,
        payouts,
        finalized_at="2026-08-22T17:00:00+09:00",
    )

    assert pre == before
    assert post["result_status"] == "post_result"
    assert post["races"][0]["actual_finish"][0]["horse_number"] == 1
    assert post["races"][0]["production_profit"]["profit_yen"] == 2100
    assert post["races"][0]["shadow_profit"]["profit_yen"] == -300


def test_forward_validation_policy_and_frozen_file_hashes_are_valid() -> None:
    """開始日・判定閾値・予想ロジックの固定SHAがすべて一致する。"""
    policy = load_forward_validation_policy()

    assert policy["start_date"] == "20260822"
    assert policy["prediction_interim_gate"]["minimum_racing_days"] == 8
    assert policy["prediction_interim_gate"]["minimum_races"] == 300
    assert policy["profit_final_gate"]["minimum_current_trifecta_races"] == 50
    assert len(policy["frozen_files"]) >= 13
