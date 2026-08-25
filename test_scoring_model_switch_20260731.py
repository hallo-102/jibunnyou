"""新best重みモデルの本番既定化に対する回帰テスト。"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pandas.testing as pdt


ROOT = Path(__file__).resolve().parent
BEST_WEIGHT_FILE = ROOT / "yosou_py" / "best_feature_weights_20260730.py"
COMPARISON_DIR = ROOT / "data" / "output" / "model_comparison"
EXPECTED_PROTECTED_HASHES = {
    BEST_WEIGHT_FILE: "2fdaf28273c2f2bbcb0052a09227f4be6d32a7e4817121df168613995cc66f32",
    COMPARISON_DIR / "best_vs_five_block_20260730.xlsx": (
        "004a8a4748c2a612e70d34f48ad8485441d0f2d8a08fa1a4d9155ee0ac865acb"
    ),
    COMPARISON_DIR / "best_vs_five_block_20260730.json": (
        "f1076f6f76cb49738e35d7c1c71916a1f943a764fba6e2607f7958896e664ff6"
    ),
    COMPARISON_DIR / "best_vs_five_block_20260730_report.md": (
        "fc15669dcf10cace51778947aff7c61bc0802aee029fc15eb0af0c283b10ea27"
    ),
    ROOT / "data" / "output" / "weight_adoption" / "decision_20260730_213416.json": (
        "14569e09e25e45b9dcb01eb81a5da74a36a66acf2f1d4263a93e14b3d7bcacb5"
    ),
}
EXPECTED_EFFECTIVE_WEIGHTS_HASH = (
    "6606ea4fa469e03ecc7a0000d031a914ca4ab10de04dfa132f0ba7d33ab70a3e"
)


def _sha256(path: Path) -> str:
    """ファイルのSHA-256を返す。"""

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_config_in_subprocess(model_value: str | None) -> tuple[dict[str, object], str]:
    """環境変数を分離したプロセスで設定を読み、結果と標準出力を返す。"""

    code = """
import importlib
import json
config = importlib.import_module("1_keibayosou_config")
config.print_scoring_model_status()
print(json.dumps({
    "model": config.SCORING_MODEL_VERSION,
    "weight_file": config.ACTIVE_FEATURE_WEIGHTS_FILE,
}, ensure_ascii=False))
"""
    env = os.environ.copy()
    if model_value is None:
        env.pop("KEIBA_SCORING_MODEL_VERSION", None)
    else:
        env["KEIBA_SCORING_MODEL_VERSION"] = model_value
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout.splitlines()[-1]), completed.stdout


def _build_synthetic_features(config) -> pd.DataFrame:
    """重みモデルと5ブロックモデルの両方を計算できる決定的な特徴量を作る。"""

    rows: list[dict[str, object]] = []
    for horse_index in range(7):
        row: dict[str, object] = {
            feature: float(
                (
                    (horse_index + 3) * (feature_index * 17 + 11)
                    + horse_index * feature_index
                )
                % 101
            )
            / 10.0
            for feature_index, feature in enumerate(config.FEAT_COLS)
        }
        row.update(
            {
                "rid_str": "202607310101",
                "馬番": horse_index + 1,
                "馬名": f"回帰テスト馬{horse_index + 1}",
                "人気": horse_index + 1,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _compute_default_scores():
    """現在の本番既定設定で決定的なテストスコアを計算する。"""

    config = importlib.import_module("1_keibayosou_config")
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    features = _build_synthetic_features(config)
    scores = pipeline.compute_scores_with_pipeline_logic(
        features,
        place_map={"202607310101": "東京"},
        surface_map={"202607310101": "芝"},
        calc_fav_risk=lambda row: float(row["人気"]) / 10.0,
    )
    return config, pipeline, scores


def test_default_selects_new_best_and_prints_clear_status() -> None:
    """環境変数なしでは新bestを選び、読込ファイルと上書きOFFを表示する。"""

    result, stdout = _load_config_in_subprocess(None)
    assert result == {
        "model": "legacy",
        "weight_file": "best_feature_weights_20260730.py",
    }
    assert "新best重みモデル" in stdout


def test_environment_override_and_invalid_fallback() -> None:
    """five_blockは明示選択でき、無効値は警告して新bestへ戻す。"""

    five_result, _ = _load_config_in_subprocess("five_block")
    invalid_result, invalid_stdout = _load_config_in_subprocess("not-a-model")
    assert five_result["model"] == "five_block"
    assert invalid_result["model"] == "legacy"
    assert "[WARN]" in invalid_stdout
    assert "安全側の新best重みモデル" in invalid_stdout


def test_best_has_101_features_and_place_surface_priority() -> None:
    """101特徴量を維持し、競馬場×芝ダート別重みを最優先する。"""

    config = importlib.import_module("1_keibayosou_config")
    features = importlib.import_module("1_keibayosou_features")
    assert len(config.FEAT_COLS) == 101
    assert len(set(config.FEAT_COLS)) == 101
    expected_features = set(config.FEAT_COLS)
    for weights in [
        *config.FEATURE_WEIGHTS.values(),
        *config.FEATURE_WEIGHTS_BY_PLACE_SURFACE.values(),
    ]:
        assert set(weights) == expected_features
    selected = features._select_weights("東京", "芝")
    assert selected is config.FEATURE_WEIGHTS_BY_PLACE_SURFACE[("東京", "芝")]
    assert selected is not config.FEATURE_WEIGHTS["東京"]


def test_effective_weights_hash_matches_adopted_best() -> None:
    """本番規則でマージ・補正した実効重みが採用時のハッシュと一致する。"""

    baseline = importlib.import_module("tokutyouryou_keisann.baseline")
    effective_hash = baseline.normalized_weights_sha256(
        baseline.active_production_weights()
    )
    assert effective_hash == EXPECTED_EFFECTIVE_WEIGHTS_HASH


def test_disabled_features_are_zero_in_every_effective_group() -> None:
    """無効特徴量は競馬場別・競馬場×芝ダート別の全グループで0に固定する。"""

    config = importlib.import_module("1_keibayosou_config")
    groups = list(config.FEATURE_WEIGHTS.values())
    groups.extend(config.FEATURE_WEIGHTS_BY_PLACE_SURFACE.values())
    for weights in groups:
        for feature in config.DISABLED_RANKING_FEATURES:
            assert float(weights.get(feature, 0.0)) == 0.0


def test_default_final_rank_equals_new_best_and_keeps_five_block_reference() -> None:
    """既定の最終順位は新bestと一致し、5ブロック参照列も残る。"""

    _, _, scores = _compute_default_scores()
    pdt.assert_series_equal(scores["rank"], scores["best_weight_rank"], check_names=False)
    pdt.assert_series_equal(scores["score"], scores["best_weight_score"], check_names=False)
    pdt.assert_series_equal(scores["legacy_rank"], scores["best_weight_rank"], check_names=False)
    assert not scores["rank"].equals(scores["five_block_rank"])
    assert {"five_block_raw_score", "five_block_score", "five_block_rank"} <= set(scores.columns)


def test_explicit_five_block_overwrites_only_final_columns() -> None:
    """five_block明示時だけ最終列を切替え、新best参照列は上書きしない。"""

    _, pipeline, default_scores = _compute_default_scores()
    original_model = pipeline.SCORING_MODEL_VERSION
    try:
        pipeline.SCORING_MODEL_VERSION = "five_block"
        _, _, five_scores = _compute_default_scores()
    finally:
        pipeline.SCORING_MODEL_VERSION = original_model
    pdt.assert_series_equal(five_scores["rank"], five_scores["five_block_rank"], check_names=False)
    pdt.assert_series_equal(five_scores["score"], five_scores["five_block_score"], check_names=False)
    pdt.assert_series_equal(
        five_scores["best_weight_rank"],
        default_scores["best_weight_rank"],
        check_names=False,
    )


def test_scoring_is_deterministic() -> None:
    """同一入力を2回計算した結果が完全一致する。"""

    _, _, first = _compute_default_scores()
    _, _, second = _compute_default_scores()
    columns = [
        "total",
        "score",
        "rank",
        "best_weight_total",
        "best_weight_score",
        "best_weight_rank",
        "five_block_raw_score",
        "five_block_score",
        "five_block_rank",
    ]
    pdt.assert_frame_equal(first[columns], second[columns], check_exact=True)
    first_top5 = first.loc[first["rank"] <= 5, ["rid_str", "馬番"]].reset_index(drop=True)
    second_top5 = second.loc[second["rank"] <= 5, ["rid_str", "馬番"]].reset_index(drop=True)
    pdt.assert_frame_equal(first_top5, second_top5, check_exact=True)


def test_protected_best_and_comparison_artifacts_are_unchanged() -> None:
    """best重み本体と採用判断の比較成果物を変更していない。"""

    for path, expected_hash in EXPECTED_PROTECTED_HASHES.items():
        assert path.is_file()
        assert _sha256(path) == expected_hash
