# -*- coding: utf-8 -*-
"""本番・シャドーモデル設定の厳格読込と重み整合性検証。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
BEST_WEIGHT_PATTERN = re.compile(r"^best_feature_weights_\d{8}\.py$")


class ModelConfigError(RuntimeError):
    """モデル設定または重み検証が安全要件を満たさない場合の例外。"""


@dataclass(frozen=True)
class ModelSpec:
    """相対パスとSHAで固定したモデル設定。"""

    role: str
    model_name: str
    weight_file: str
    file_sha256: str
    effective_sha256: str
    activated_at: str
    activation_reason: str
    previous_model_name: str
    scoring_model_version: str
    expected_group_count: int
    expected_element_count: int
    project_root: Path
    config_path: Path
    resolved_weight_path: Path
    raw: dict[str, Any]


def sha256_file(path: str | Path) -> str:
    """ファイル内容のSHA-256を返す。"""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_group_key(key: Any) -> dict[str, str]:
    """実効重みgroupを型衝突しないJSON表現へ変換する。"""
    if isinstance(key, tuple) and len(key) == 2:
        return {
            "type": "place_surface",
            "place": str(key[0]),
            "surface": str(key[1]),
        }
    return {"type": "common", "name": str(key)}


def _group_sort_key(key: Any) -> str:
    """実効重みgroupの決定論的ソートキーを返す。"""
    return json.dumps(
        _canonical_group_key(key),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def normalized_weights_sha256(
    weights_map: Mapping[Any, Mapping[str, float]],
    decimal_places: int = 10,
) -> str:
    """group・特徴量順と保存精度を固定した実効重みSHA-256を返す。"""
    groups: list[dict[str, Any]] = []
    for group_key in sorted(weights_map, key=_group_sort_key):
        weights = weights_map[group_key]
        features = [
            [str(feature), format(float(weights[feature]), f".{decimal_places}f")]
            for feature in sorted(weights)
        ]
        groups.append(
            {"group": _canonical_group_key(group_key), "features": features}
        )
    canonical = json.dumps(
        groups,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _required_text(payload: dict[str, Any], field: str) -> str:
    """必須文字列を空文字も含めて検証する。"""
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ModelConfigError(f"モデル設定の必須文字列が不正です: {field}")
    return value.strip()


def _validate_sha(value: str, field: str) -> str:
    """小文字64桁のSHA-256だけを受け付ける。"""
    normalized = value.strip().lower()
    if not SHA256_PATTERN.fullmatch(normalized):
        raise ModelConfigError(f"モデル設定のSHA-256形式が不正です: {field}")
    return normalized


def _resolve_project_relative_file(project_root: Path, relative_value: str) -> Path:
    """絶対パスとプロジェクト外参照を拒否し、実在ファイルを返す。"""
    relative_path = Path(relative_value)
    if relative_path.is_absolute():
        raise ModelConfigError(
            f"重みファイルはプロジェクト相対パスで指定してください: {relative_value}"
        )
    project_root = project_root.resolve()
    resolved = (project_root / relative_path).resolve()
    try:
        resolved.relative_to(project_root)
    except ValueError as exc:
        raise ModelConfigError(
            f"プロジェクト外の重みファイルは使用できません: {relative_value}"
        ) from exc
    if not resolved.is_file():
        raise ModelConfigError(f"指定した重みファイルが存在しません: {resolved}")
    if not BEST_WEIGHT_PATTERN.fullmatch(resolved.name):
        raise ModelConfigError(
            f"best_feature_weights_YYYYMMDD.py以外は使用できません: {resolved.name}"
        )
    return resolved


def load_model_spec(
    project_root: str | Path,
    config_path: str | Path,
    *,
    expected_role: str | None = None,
) -> ModelSpec:
    """設定ファイルと重みファイルSHAを検証し、fallbackなしで読み込む。"""
    root = Path(project_root).resolve()
    config = Path(config_path)
    if not config.is_absolute():
        config = root / config
    config = config.resolve()
    if not config.is_file():
        raise ModelConfigError(f"モデル設定ファイルが存在しません: {config}")
    try:
        payload = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelConfigError(f"モデル設定ファイルを読み込めません: {config}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ModelConfigError(f"モデル設定のルートはobjectである必要があります: {config}")

    role = _required_text(payload, "role")
    if expected_role is not None and role != expected_role:
        raise ModelConfigError(
            f"モデルroleが不一致です: expected={expected_role} actual={role}"
        )
    model_name = _required_text(payload, "model_name")
    weight_file = _required_text(payload, "weight_file")
    file_sha256 = _validate_sha(_required_text(payload, "file_sha256"), "file_sha256")
    effective_sha256 = _validate_sha(
        _required_text(payload, "effective_sha256"), "effective_sha256"
    )
    activated_at = _required_text(payload, "activated_at")
    try:
        datetime.fromisoformat(activated_at)
    except ValueError as exc:
        raise ModelConfigError(f"activated_atがISO-8601形式ではありません: {activated_at}") from exc
    activation_reason = _required_text(payload, "activation_reason")
    previous_model_name = str(payload.get("previous_model_name", "")).strip()
    scoring_model_version = _required_text(payload, "scoring_model_version")
    try:
        expected_group_count = int(payload["expected_group_count"])
        expected_element_count = int(payload["expected_element_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ModelConfigError(
            "expected_group_count / expected_element_countが不正です"
        ) from exc
    if expected_group_count <= 0 or expected_element_count <= 0:
        raise ModelConfigError("重みgroup数・要素数は正数で指定してください")

    resolved_weight_path = _resolve_project_relative_file(root, weight_file)
    actual_file_sha256 = sha256_file(resolved_weight_path)
    if actual_file_sha256 != file_sha256:
        raise ModelConfigError(
            "重みファイルSHA-256が設定と一致しません: "
            f"path={resolved_weight_path} expected={file_sha256} actual={actual_file_sha256}"
        )

    return ModelSpec(
        role=role,
        model_name=model_name,
        weight_file=Path(weight_file).as_posix(),
        file_sha256=file_sha256,
        effective_sha256=effective_sha256,
        activated_at=activated_at,
        activation_reason=activation_reason,
        previous_model_name=previous_model_name,
        scoring_model_version=scoring_model_version,
        expected_group_count=expected_group_count,
        expected_element_count=expected_element_count,
        project_root=root,
        config_path=config,
        resolved_weight_path=resolved_weight_path,
        raw=dict(payload),
    )


def validate_effective_weights(
    spec: ModelSpec,
    weights_map: Mapping[Any, Mapping[str, float]],
    *,
    scoring_model_version: str,
) -> dict[str, Any]:
    """実効SHA・group数・要素数・scoring版を一括検証する。"""
    if scoring_model_version != spec.scoring_model_version:
        raise ModelConfigError(
            "SCORING_MODEL_VERSIONがモデル設定と一致しません: "
            f"expected={spec.scoring_model_version} actual={scoring_model_version}"
        )
    group_count = len(weights_map)
    element_count = sum(len(group) for group in weights_map.values())
    if group_count != spec.expected_group_count:
        raise ModelConfigError(
            "実効重みgroup数が設定と一致しません: "
            f"expected={spec.expected_group_count} actual={group_count}"
        )
    if element_count != spec.expected_element_count:
        raise ModelConfigError(
            "実効重み要素数が設定と一致しません: "
            f"expected={spec.expected_element_count} actual={element_count}"
        )
    effective_sha256 = normalized_weights_sha256(weights_map)
    if effective_sha256 != spec.effective_sha256:
        raise ModelConfigError(
            "実効重みSHA-256が設定と一致しません: "
            f"expected={spec.effective_sha256} actual={effective_sha256}"
        )
    return {
        "group_count": group_count,
        "element_count": element_count,
        "effective_sha256": effective_sha256,
    }


def model_metadata(spec: ModelSpec) -> dict[str, Any]:
    """ログ・Excel・JSONへ共通記録するモデル識別情報を返す。"""
    prefix = "production" if spec.role == "production" else "shadow"
    return {
        f"{prefix}_model_name": spec.model_name,
        f"{prefix}_weight_file": spec.weight_file,
        f"{prefix}_file_sha256": spec.file_sha256,
        f"{prefix}_effective_sha256": spec.effective_sha256,
        "scoring_model_version": spec.scoring_model_version,
    }


def write_json_atomic(path: str | Path, payload: dict[str, Any]) -> None:
    """同一ディレクトリの一時ファイルからJSONを原子的に置換する。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
    except Exception:
        try:
            Path(temporary_name).unlink(missing_ok=True)
        finally:
            raise
