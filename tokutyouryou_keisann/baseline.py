# -*- coding: utf-8 -*-
"""本番採用済み重みの選択、厳格読込、本番実効重みとの一致確認。"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .common import WeightsMap
from .config import PROJECT_ROOT, PY_DIR


BEST_FILE_PATTERN = re.compile(r"^best_feature_weights_(\d{8})\.py$")
BASELINE_ERROR_CODE = "baseline_weight_load_failed"
ROUNDTRIP_DECIMAL_PLACES = 10
ROUNDTRIP_ABS_TOLERANCE = 5.000001e-11


class BaselineWeightError(RuntimeError):
    """baselineを安全に評価できない場合の例外。"""

    def __init__(self, message: str, code: str = BASELINE_ERROR_CODE) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BaselineSelection:
    """優先順位に従って特定したbaselineファイル。"""

    path: Path
    method: str


@dataclass(frozen=True)
class BaselineWeightInfo:
    """評価に渡す実効重みと、その根拠となるファイル情報。"""

    path: Path
    method: str
    sha256: str
    modified_at: str
    weights_map: WeightsMap
    common_weight_count: int
    common_group_count: int
    place_surface_group_count: int
    production_path: Path
    production_sha256: str
    effective_sha256: str
    production_effective_sha256: str
    effective_group_count: int
    effective_weight_count: int
    production_group_count: int
    production_weight_count: int
    transform_stats: WeightTransformStats
    production_transform_stats: WeightTransformStats
    production_weights_match: bool


@dataclass(frozen=True)
class WeightMapComparison:
    """2つの実効WeightsMapの正規化ハッシュと差分診断。"""

    left_normalized_sha256: str
    right_normalized_sha256: str
    match: bool
    differing_group_count: int
    differing_feature_count: int
    differences: list[dict[str, Any]]
    missing_from_right: list[dict[str, str]]
    added_in_right: list[dict[str, str]]
    max_absolute_difference: float


@dataclass(frozen=True)
class WeightTransformStats:
    """本番共通処理で適用した補完・符号補正・0固定の件数。"""

    missing_common_group_completion_count: int
    missing_place_surface_group_completion_count: int
    missing_feature_completion_count: int
    empirical_sign_correction_count: int
    disabled_feature_zero_count: int


def sha256_file(path: Path) -> str:
    """ファイル内容のSHA-256を返す。"""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_best_file(path: Path) -> bool:
    """candidateや不採用ファイルをbaseline候補から除外する。"""
    return bool(BEST_FILE_PATTERN.fullmatch(path.name))


def _resolve_recorded_path(value: str) -> Path:
    """判断JSONに保存された相対・絶対パスを絶対パスへ揃える。"""
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _latest_adopted_best() -> Path | None:
    """最新のadopted判断JSONが示すbestファイルを返す。"""
    decision_dir = PROJECT_ROOT / "data" / "output" / "weight_adoption"
    if not decision_dir.exists():
        return None
    records: list[tuple[float, Path]] = []
    for decision_path in decision_dir.glob("decision_*.json"):
        try:
            payload = json.loads(decision_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not (
            payload.get("adopted") is True
            or str(payload.get("decision", "")).lower() == "adopted"
        ):
            continue
        raw_path = (
            payload.get("adopted_weight_file")
            or payload.get("best_weight_file")
        )
        if not raw_path:
            continue
        candidate = _resolve_recorded_path(str(raw_path))
        if not _is_best_file(candidate):
            continue
        created = payload.get("created_at") or payload.get("timestamp")
        try:
            sort_value = datetime.fromisoformat(str(created)).timestamp()
        except Exception:
            sort_value = decision_path.stat().st_mtime
        records.append((sort_value, candidate))
    return max(records, key=lambda item: item[0])[1] if records else None


def production_best_path() -> Path:
    """明示的な本番モデル設定が参照するbestファイルを返す。"""
    production_config = importlib.import_module("1_keibayosou_config")
    spec = getattr(production_config, "PRODUCTION_MODEL_SPEC", None)
    if spec is None:
        raise BaselineWeightError("本番モデル設定が読み込まれていません")
    path = Path(spec.resolved_weight_path).resolve()
    if not _is_best_file(path):
        raise BaselineWeightError(f"本番runnerの重みファイル名が不正です: {path}")
    if not path.is_file():
        raise BaselineWeightError(f"本番runnerの重みファイルが存在しません: {path}")
    return path


def select_baseline_weight_file(
    explicit_path: str | Path | None = None,
) -> BaselineSelection:
    """仕様の優先順位どおりにbaselineを1つだけ決定する。"""
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path = path.resolve()
        if not _is_best_file(path):
            raise BaselineWeightError(
                f"--baseline-weight-fileはbest_feature_weights_YYYYMMDD.pyだけ指定できます: {path}"
            )
        return BaselineSelection(path, "cli")

    # 採用履歴JSONやファイル名の日付は、本番選択の根拠にしない。
    # 設定不備時も最新ファイルへfallbackせず、安全に停止する。
    return BaselineSelection(production_best_path(), "production_model_config")


def _load_module(path: Path) -> Any:
    """副作用を限定した固有名で重みモジュールを読み込む。"""
    module_name = f"_keiba_baseline_{sha256_file(path)[:16]}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise BaselineWeightError(f"Pythonモジュールを作成できません: {path}")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise BaselineWeightError(f"Pythonモジュールを読み込めません: {path}: {exc}") from exc
    return module


def _strict_weight_group(value: Any, label: str) -> dict[str, float]:
    """重みgroupを有限floatへ変換し、不正値を拒否する。"""
    if not isinstance(value, dict):
        raise BaselineWeightError(f"{label}がdictではありません")
    converted: dict[str, float] = {}
    for feature, raw_weight in value.items():
        if isinstance(raw_weight, bool):
            raise BaselineWeightError(f"{label}[{feature!r}]が数値ではありません")
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError) as exc:
            raise BaselineWeightError(
                f"{label}[{feature!r}]を数値化できません: {raw_weight!r}"
            ) from exc
        if not math.isfinite(weight):
            raise BaselineWeightError(f"{label}[{feature!r}]が有限数ではありません")
        converted[str(feature)] = weight
    return converted


def _strict_external_maps(module: Any) -> tuple[dict[str, dict[str, float]], dict[tuple[str, str], dict[str, float]]]:
    """保存モジュールの2変数を厳格に正規化する。"""
    if not hasattr(module, "FEATURE_WEIGHTS"):
        raise BaselineWeightError("FEATURE_WEIGHTSが存在しません")
    raw_common = module.FEATURE_WEIGHTS
    if not isinstance(raw_common, dict):
        raise BaselineWeightError("FEATURE_WEIGHTSがdictではありません")
    common: dict[str, dict[str, float]] = {}
    for key, value in raw_common.items():
        if not isinstance(key, str):
            raise BaselineWeightError(f"FEATURE_WEIGHTSのkeyが文字列ではありません: {key!r}")
        common[key] = _strict_weight_group(value, f"FEATURE_WEIGHTS[{key!r}]")
    if "__default__" not in common:
        raise BaselineWeightError("FEATURE_WEIGHTSに__default__が存在しません")

    raw_by_surface = getattr(module, "FEATURE_WEIGHTS_BY_PLACE_SURFACE", {})
    if raw_by_surface is None:
        raw_by_surface = {}
    if not isinstance(raw_by_surface, dict):
        raise BaselineWeightError("FEATURE_WEIGHTS_BY_PLACE_SURFACEがdictではありません")
    by_surface: dict[tuple[str, str], dict[str, float]] = {}
    for key, value in raw_by_surface.items():
        if not isinstance(key, tuple) or len(key) != 2:
            raise BaselineWeightError(
                f"FEATURE_WEIGHTS_BY_PLACE_SURFACEのkey形式が不正です: {key!r}"
            )
        normalized_key = (str(key[0]), str(key[1]))
        by_surface[normalized_key] = _strict_weight_group(
            value, f"FEATURE_WEIGHTS_BY_PLACE_SURFACE[{key!r}]"
        )
    return common, by_surface


def _effective_map_from_file_with_stats(
    path: Path,
) -> tuple[WeightsMap, int, int, int, WeightTransformStats]:
    """本番と同じ統合・符号補正規則でWeightsMapと適用件数を構築する。"""
    if not path.exists() or not path.is_file():
        raise BaselineWeightError(f"baselineファイルが存在しません: {path}")
    module = _load_module(path)
    external_common, external_by_surface = _strict_external_maps(module)
    production_config = importlib.import_module("1_keibayosou_config")

    common = production_config._merge_feature_weights(
        production_config.BUILTIN_FEATURE_WEIGHTS,
        external_common,
    )
    by_surface = production_config._merge_feature_weights_by_place_surface(
        production_config.BUILTIN_FEATURE_WEIGHTS_BY_PLACE_SURFACE,
        external_by_surface,
        common.get("__default__", {}),
    )
    common_before_enforcement = {
        key: dict(weights) for key, weights in common.items()
    }
    by_surface_before_enforcement = {
        key: dict(weights) for key, weights in by_surface.items()
    }
    common = production_config._enforce_empirical_weight_signs(common)
    by_surface = production_config._enforce_empirical_weight_signs(by_surface)

    missing_feature_completion_count = 0
    for key, weights in common_before_enforcement.items():
        source = external_common.get(key, {})
        missing_feature_completion_count += len(set(weights) - set(source))
    for key, weights in by_surface_before_enforcement.items():
        source = external_by_surface.get(key, {})
        missing_feature_completion_count += len(set(weights) - set(source))

    empirical_sign_correction_count = 0
    disabled_feature_zero_count = 0
    before_maps: dict[Any, dict[str, float]] = {
        **common_before_enforcement,
        **by_surface_before_enforcement,
    }
    after_maps: dict[Any, dict[str, float]] = {**common, **by_surface}
    disabled_features = set(production_config.DISABLED_RANKING_FEATURES)
    sign_guard = dict(production_config.EMPIRICAL_WEIGHT_SIGN_GUARD)
    for key, before_weights in before_maps.items():
        after_weights = after_maps[key]
        for feature, before_value in before_weights.items():
            after_value = float(after_weights.get(feature, 0.0))
            before_float = float(before_value)
            if feature in disabled_features:
                if before_float != 0.0 and after_value == 0.0:
                    disabled_feature_zero_count += 1
            elif feature in sign_guard and before_float != after_value:
                empirical_sign_correction_count += 1

    weights_map: WeightsMap = {
        key: dict(weights) for key, weights in common.items()
    }
    weights_map.update({
        key: dict(weights) for key, weights in by_surface.items()
    })
    common_count = len(common.get("__default__", {}))
    stats = WeightTransformStats(
        missing_common_group_completion_count=len(set(common) - set(external_common)),
        missing_place_surface_group_completion_count=len(
            set(by_surface) - set(external_by_surface)
        ),
        missing_feature_completion_count=missing_feature_completion_count,
        empirical_sign_correction_count=empirical_sign_correction_count,
        disabled_feature_zero_count=disabled_feature_zero_count,
    )
    return weights_map, common_count, len(common), len(by_surface), stats


def _effective_map_from_file(path: Path) -> tuple[WeightsMap, int, int, int]:
    """既存呼出し向けに実効WeightsMapとgroup件数を返す。"""
    weights_map, common_count, common_groups, by_surface_groups, _ = (
        _effective_map_from_file_with_stats(path)
    )
    return weights_map, common_count, common_groups, by_surface_groups


def load_effective_weights_file(path: str | Path) -> WeightsMap:
    """candidate/baselineを本番configと同じmerge・符号補正で読み込む。"""
    weights_map, _, _, _ = _effective_map_from_file(Path(path).resolve())
    return weights_map


def _canonical_group_key(key: Any) -> dict[str, Any]:
    """文字列場所と場所×芝ダtupleを衝突しないJSON表現へ変換する。"""
    if isinstance(key, tuple) and len(key) == 2:
        return {"type": "place_surface", "place": str(key[0]), "surface": str(key[1])}
    return {"type": "common", "name": str(key)}


def _group_sort_key(key: Any) -> str:
    """グループを決定論的に並べる。"""
    return json.dumps(
        _canonical_group_key(key),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def normalized_weights_sha256(
    weights_map: WeightsMap,
    decimal_places: int = ROUNDTRIP_DECIMAL_PLACES,
) -> str:
    """キー順と保存精度を正規化した実効WeightsMapのSHA-256を返す。"""
    groups = []
    for group_key in sorted(weights_map, key=_group_sort_key):
        weights = weights_map[group_key]
        features = [
            [str(feature), format(float(weights[feature]), f".{decimal_places}f")]
            for feature in sorted(weights)
        ]
        groups.append({"group": _canonical_group_key(group_key), "features": features})
    canonical = json.dumps(
        groups,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def compare_weights_maps(
    left: WeightsMap,
    right: WeightsMap,
    tolerance: float = ROUNDTRIP_ABS_TOLERANCE,
    difference_limit: int = 100,
) -> WeightMapComparison:
    """不足・追加・値差を全件集計し、絶対差上位を返す。"""
    differences: list[dict[str, Any]] = []
    missing_from_right: list[dict[str, str]] = []
    added_in_right: list[dict[str, str]] = []
    differing_groups: set[str] = set()
    max_absolute_difference = 0.0

    for group_key in sorted(set(left) | set(right), key=_group_sort_key):
        group_label = _canonical_group_key(group_key)
        group_id = _group_sort_key(group_key)
        left_group = left.get(group_key, {})
        right_group = right.get(group_key, {})
        for feature in sorted(set(left_group) | set(right_group)):
            item_base = {"group": group_label, "feature": str(feature)}
            if feature not in right_group:
                missing_from_right.append(
                    {"group": group_id, "feature": str(feature)}
                )
                differences.append(
                    {
                        **item_base,
                        "difference_type": "missing_from_reloaded",
                        "left_value": float(left_group[feature]),
                        "right_value": None,
                        "absolute_difference": None,
                    }
                )
                differing_groups.add(group_id)
                continue
            if feature not in left_group:
                added_in_right.append(
                    {"group": group_id, "feature": str(feature)}
                )
                differences.append(
                    {
                        **item_base,
                        "difference_type": "added_on_reload",
                        "left_value": None,
                        "right_value": float(right_group[feature]),
                        "absolute_difference": None,
                    }
                )
                differing_groups.add(group_id)
                continue
            left_value = float(left_group[feature])
            right_value = float(right_group[feature])
            absolute_difference = abs(left_value - right_value)
            if absolute_difference > tolerance:
                max_absolute_difference = max(
                    max_absolute_difference, absolute_difference
                )
                differences.append(
                    {
                        **item_base,
                        "difference_type": "value_changed",
                        "left_value": left_value,
                        "right_value": right_value,
                        "absolute_difference": absolute_difference,
                    }
                )
                differing_groups.add(group_id)

    differences.sort(
        key=lambda row: (
            row["absolute_difference"] is None,
            -(float(row["absolute_difference"] or 0.0)),
            json.dumps(row["group"], ensure_ascii=False, sort_keys=True),
            row["feature"],
        )
    )
    return WeightMapComparison(
        left_normalized_sha256=normalized_weights_sha256(left),
        right_normalized_sha256=normalized_weights_sha256(right),
        match=not differences,
        differing_group_count=len(differing_groups),
        differing_feature_count=len(differences),
        differences=differences[: max(0, int(difference_limit))],
        missing_from_right=missing_from_right,
        added_in_right=added_in_right,
        max_absolute_difference=max_absolute_difference,
    )


def active_production_weights() -> WeightsMap:
    """現在import済みの本番runner実効重みをWeightsMapで返す。"""
    production_config = importlib.import_module("1_keibayosou_config")
    result: WeightsMap = {
        key: {str(feature): float(value) for feature, value in weights.items()}
        for key, weights in production_config.FEATURE_WEIGHTS.items()
    }
    result.update({
        key: {str(feature): float(value) for feature, value in weights.items()}
        for key, weights in production_config.FEATURE_WEIGHTS_BY_PLACE_SURFACE.items()
    })
    return result


def weights_equal(left: WeightsMap, right: WeightsMap, tolerance: float = 1e-12) -> bool:
    """key・特徴量・値が同じ実効重みか確認する。"""
    return compare_weights_maps(
        left,
        right,
        tolerance=tolerance,
        difference_limit=0,
    ).match


def _format_weight_mismatch_diagnostics(
    *,
    selection: BaselineSelection,
    production_path: Path,
    baseline_weights: WeightsMap,
    production_weights: WeightsMap,
    comparison: WeightMapComparison,
    transform_stats: WeightTransformStats,
    production_transform_stats: WeightTransformStats,
) -> str:
    """不一致時に必要なパス・SHA・key差分・正規化処理差を整形する。"""
    baseline_group_count = len(baseline_weights)
    production_group_count = len(production_weights)
    baseline_weight_count = sum(len(group) for group in baseline_weights.values())
    production_weight_count = sum(len(group) for group in production_weights.values())
    baseline_stats = asdict(transform_stats)
    production_stats = asdict(production_transform_stats)
    return "\n".join(
        [
            "選択baselineのパスまたは実効重みが本番runnerと一致しません",
            f"production_file_name={production_path.name}",
            f"production_path={production_path}",
            f"production_resolved_path={production_path.resolve()}",
            f"baseline_file_name={selection.path.name}",
            f"baseline_path={selection.path}",
            f"baseline_resolved_path={selection.path.resolve()}",
            f"production_file_sha256={sha256_file(production_path)}",
            f"baseline_file_sha256={sha256_file(selection.path)}",
            f"production_effective_sha256={comparison.right_normalized_sha256}",
            f"baseline_effective_sha256={comparison.left_normalized_sha256}",
            f"production_group_count={production_group_count}",
            f"baseline_group_count={baseline_group_count}",
            f"production_weight_count={production_weight_count}",
            f"baseline_weight_count={baseline_weight_count}",
            f"differing_group_count={comparison.differing_group_count}",
            f"differing_feature_count={comparison.differing_feature_count}",
            "weight_differences="
            + json.dumps(comparison.differences, ensure_ascii=False, sort_keys=True),
            "missing_from_production="
            + json.dumps(comparison.missing_from_right, ensure_ascii=False, sort_keys=True),
            "added_to_production="
            + json.dumps(comparison.added_in_right, ensure_ascii=False, sort_keys=True),
            "baseline_transformations="
            + json.dumps(baseline_stats, ensure_ascii=False, sort_keys=True),
            "production_transformations="
            + json.dumps(production_stats, ensure_ascii=False, sort_keys=True),
            f"normalization_application_difference={baseline_stats != production_stats}",
        ]
    )


def load_baseline_weights(
    selection: BaselineSelection,
    verify_production: bool = True,
) -> BaselineWeightInfo:
    """選択ファイルを読み、本番と不一致なら安全側で失敗させる。"""
    (
        weights_map,
        common_count,
        common_groups,
        by_surface_groups,
        transform_stats,
    ) = _effective_map_from_file_with_stats(selection.path)
    production_path = production_best_path()
    production_weights = active_production_weights()
    (
        production_file_weights,
        _,
        _,
        _,
        production_transform_stats,
    ) = _effective_map_from_file_with_stats(production_path)
    production_internal_comparison = compare_weights_maps(
        production_file_weights,
        production_weights,
        tolerance=0.0,
        difference_limit=100,
    )
    if not production_internal_comparison.match:
        raise BaselineWeightError(
            "本番runnerの選択ファイルから再構築した実効重みがimport済み本番重みと一致しません\n"
            + _format_weight_mismatch_diagnostics(
                selection=BaselineSelection(production_path, "production_internal_check"),
                production_path=production_path,
                baseline_weights=production_file_weights,
                production_weights=production_weights,
                comparison=production_internal_comparison,
                transform_stats=production_transform_stats,
                production_transform_stats=production_transform_stats,
            )
        )
    comparison = compare_weights_maps(
        weights_map,
        production_weights,
        tolerance=0.0,
        difference_limit=100,
    )
    effective_sha_match = (
        comparison.left_normalized_sha256
        == comparison.right_normalized_sha256
    )
    matches = (
        selection.path.resolve() == production_path.resolve()
        and comparison.match
        and effective_sha_match
    )
    if verify_production and not matches:
        raise BaselineWeightError(
            _format_weight_mismatch_diagnostics(
                selection=selection,
                production_path=production_path,
                baseline_weights=weights_map,
                production_weights=production_weights,
                comparison=comparison,
                transform_stats=transform_stats,
                production_transform_stats=production_transform_stats,
            )
        )
    modified_at = datetime.fromtimestamp(
        selection.path.stat().st_mtime, tz=timezone.utc
    ).isoformat()
    return BaselineWeightInfo(
        path=selection.path.resolve(),
        method=selection.method,
        sha256=sha256_file(selection.path),
        modified_at=modified_at,
        weights_map=weights_map,
        common_weight_count=common_count,
        common_group_count=common_groups,
        place_surface_group_count=by_surface_groups,
        production_path=production_path,
        production_sha256=sha256_file(production_path),
        effective_sha256=comparison.left_normalized_sha256,
        production_effective_sha256=comparison.right_normalized_sha256,
        effective_group_count=len(weights_map),
        effective_weight_count=sum(len(group) for group in weights_map.values()),
        production_group_count=len(production_weights),
        production_weight_count=sum(
            len(group) for group in production_weights.values()
        ),
        transform_stats=transform_stats,
        production_transform_stats=production_transform_stats,
        production_weights_match=matches,
    )
