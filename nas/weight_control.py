"""同一の共通特徴量・補正で、明示した2組の重みだけを変える診断。"""
from contextlib import contextmanager
import hashlib
import importlib
import json

import numpy as np
import pandas as pd

from continuous_improvement.archive import sha256, write_json
from model_registry import normalized_weights_sha256
from nas.workspace import CODE_ROOT

MODELS = {
    "20260730": ("2e070de3ab4a53c39661b74dcc9f6ea1ada339ac1ac586c44afd05f37ab55717", "6606ea4fa469e03ecc7a0000d031a914ca4ab10de04dfa132f0ba7d33ab70a3e"),
    "20260817": ("bb086384eb58c5abae6ef7104de94e83005c318b3be3900ef5c4df15fca1020d", "8325f5c0748aa49076631b02424f2104a7924d0b39a4148d4703f687a937a789"),
}


def weights(day):
    """本番registryを変更せず、共通configの既存merge/符号補正を利用する。"""
    config = importlib.import_module("1_keibayosou_config")
    path = CODE_ROOT / "yosou_py" / f"best_feature_weights_{day}.py"
    file_sha, effective_sha = MODELS[day]
    if sha256(path) != file_sha:
        raise ValueError("統制比較の明示重みファイルSHA不一致")
    common, surface = config._load_feature_weights_from_explicit_path(path)
    common = config._enforce_empirical_weight_signs(config._merge_feature_weights(config.BUILTIN_FEATURE_WEIGHTS, common))
    surface = config._enforce_empirical_weight_signs(config._merge_feature_weights_by_place_surface(
        config.BUILTIN_FEATURE_WEIGHTS_BY_PLACE_SURFACE, surface, common.get("__default__", {})))
    merged = {**common, **surface}
    if normalized_weights_sha256(merged) != effective_sha:
        raise ValueError("統制比較の実効重みSHA不一致")
    return merged


def controlled_scores(compute, frame, kwargs):
    """同じDataFrame/補正関数/場所・馬場mapを使用し、重みだけを変える。"""
    original = frame.copy(deep=True)
    results = {day: compute(frame.copy(deep=True), **kwargs, weights_map=weights(day)) for day in MODELS}
    pd.testing.assert_frame_equal(frame, original)
    a, b = results.values()
    correction_columns = [c for c in a if c.startswith("penalty_") or c in {
        "favorite_risk", "rest_dist_risk", "extra_penalty", "dl_bonus", "risk_score", "data_confidence"}]
    pd.testing.assert_frame_equal(a[correction_columns], b[correction_columns], check_exact=True)
    return results, correction_columns


@contextmanager
def capture_control(pipeline, run_dir):
    """pipeline入力を一度だけ採取し、通常予想の戻り値は変更しない。"""
    compute = pipeline.compute_scores_with_pipeline_logic
    if pipeline.SCORING_MODEL_VERSION != "legacy":
        raise ValueError("統制比較は固定したlegacy候補だけを対象とします")

    def capture(frame, **kwargs):
        normal = compute(frame, **kwargs)
        results, corrections = controlled_scores(compute, frame, kwargs)
        # 明示20260817が通常候補と一致しなければ診断の前提が壊れている。
        pd.testing.assert_frame_equal(normal[["total", "score_raw", "rank"]], results["20260817"][["total", "score_raw", "rank"]], check_exact=True)
        left, right = results.values()
        rows = []
        for pos in range(len(left)):
            a, b = left.iloc[pos], right.iloc[pos]
            rows.append({"race_id": str(a.rid_str), "horse_number": int(a["馬番"]),
                         "20260730": {c: float(a[c]) for c in ("total", "score_raw", "rank")},
                         "20260817": {c: float(b[c]) for c in ("total", "score_raw", "rank")}})
        race_changes = []
        for rid, group in left.groupby("rid_str"):
            other = right.loc[right.rid_str.eq(rid)]
            top = lambda data, n: sorted(data.loc[data["rank"].le(n), "馬番"].astype(int).tolist())
            race_changes.append({"race_id": str(rid), "rank1_changed": top(group, 1) != top(other, 1),
                                 "top5_changed": top(group, 5) != top(other, 5)})
        feature_bytes = frame.to_json(orient="split", double_precision=15, force_ascii=False).encode("utf-8")
        report = {"scope": "offline", "comparison": "same_common_features_and_corrections_weights_only",
                  "input_feature_sha256": hashlib.sha256(feature_bytes).hexdigest(),
                  "models": {day: {"file_sha256": pair[0], "effective_sha256": pair[1]} for day, pair in MODELS.items()},
                  "corrections_equal": True, "correction_columns": corrections,
                  "normal_candidate_equal": True, "horses": len(frame), "races": len(race_changes),
                  "rank_changed_horses": int(left["rank"].ne(right["rank"]).sum()),
                  "score_changed_horses": int((~np.isclose(left.score_raw, right.score_raw, atol=1e-9, rtol=1e-9)).sum()),
                  "rank1_changed_races": sum(r["rank1_changed"] for r in race_changes),
                  "top5_changed_races": sum(r["top5_changed"] for r in race_changes),
                  "horse_results": rows, "race_results": race_changes,
                  "limitations": ["旧NASの特徴量を再現する比較ではない", "買い目再生成・ROI/精度改善の認定は行わない"]}
        write_json(run_dir / "weight_control.json", report)
        return normal

    pipeline.compute_scores_with_pipeline_logic = capture
    try:
        yield
    finally:
        pipeline.compute_scores_with_pipeline_logic = compute
