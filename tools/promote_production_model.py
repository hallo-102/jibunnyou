# -*- coding: utf-8 -*-
"""検証完了後にだけ使う、明示的な本番モデル昇格CLI。"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_registry import (  # noqa: E402
    ModelConfigError,
    load_model_spec,
    normalized_weights_sha256,
    sha256_file,
    write_json_atomic,
)


PRODUCTION_CONFIG_PATH = PROJECT_ROOT / "config" / "production_model.json"
BEST_PATTERN = re.compile(r"^best_feature_weights_\d{8}\.py$")
SHA_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _resolve_weight_file(relative_value: str) -> Path:
    """本番昇格対象をプロジェクト内のbest相対パスに限定する。"""
    relative = Path(relative_value)
    if relative.is_absolute():
        raise ModelConfigError("昇格対象はプロジェクト相対パスで指定してください")
    root = PROJECT_ROOT.resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ModelConfigError("プロジェクト外の重みは昇格できません") from exc
    if not resolved.is_file() or not BEST_PATTERN.fullmatch(resolved.name):
        raise ModelConfigError(f"昇格対象bestファイルが不正です: {resolved}")
    return resolved


def build_promoted_config(
    *,
    model_name: str,
    weight_file: str,
    expected_file_sha256: str,
    expected_effective_sha256: str,
    activation_reason: str,
) -> dict[str, object]:
    """昇格前にファイルと実効重みを再計算し、新設定を組み立てる。"""
    if not model_name.strip() or not activation_reason.strip():
        raise ModelConfigError("モデル名と昇格理由は必須です")
    if not SHA_PATTERN.fullmatch(expected_file_sha256):
        raise ModelConfigError("--expected-file-sha256の形式が不正です")
    if not SHA_PATTERN.fullmatch(expected_effective_sha256):
        raise ModelConfigError("--expected-effective-sha256の形式が不正です")

    resolved = _resolve_weight_file(weight_file)
    actual_file_sha = sha256_file(resolved)
    if actual_file_sha != expected_file_sha256:
        raise ModelConfigError(
            "昇格対象のファイルSHAが指定値と一致しません: "
            f"expected={expected_file_sha256} actual={actual_file_sha}"
        )

    baseline = importlib.import_module("tokutyouryou_keisann.baseline")
    weights_map = baseline.load_effective_weights_file(resolved)
    actual_effective_sha = normalized_weights_sha256(weights_map)
    if actual_effective_sha != expected_effective_sha256:
        raise ModelConfigError(
            "昇格対象の実効重みSHAが指定値と一致しません: "
            f"expected={expected_effective_sha256} actual={actual_effective_sha}"
        )

    current = load_model_spec(
        PROJECT_ROOT,
        PRODUCTION_CONFIG_PATH,
        expected_role="production",
    )
    production_config = importlib.import_module("1_keibayosou_config")
    return {
        "schema_version": 1,
        "role": "production",
        "model_name": model_name.strip(),
        "weight_file": resolved.relative_to(PROJECT_ROOT.resolve()).as_posix(),
        "file_sha256": actual_file_sha,
        "effective_sha256": actual_effective_sha,
        "activated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "activation_reason": activation_reason.strip(),
        "previous_model_name": current.model_name,
        "scoring_model_version": production_config.SCORING_MODEL_VERSION,
        "expected_group_count": len(weights_map),
        "expected_element_count": sum(len(group) for group in weights_map.values()),
    }


def main() -> None:
    """昇格意思と事前計算SHAを必須にして本番設定だけを更新する。"""
    parser = argparse.ArgumentParser(
        description="前向き検証完了後の明示的な本番モデル昇格"
    )
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--weight-file", required=True)
    parser.add_argument("--expected-file-sha256", required=True)
    parser.add_argument("--expected-effective-sha256", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--confirm-forward-validation-complete",
        action="store_true",
        help="前向き検証と事前ゲート完了を明示確認します。",
    )
    args = parser.parse_args()
    if not args.confirm_forward_validation_complete:
        raise SystemExit(
            "昇格を中止しました: --confirm-forward-validation-complete が必要です"
        )

    payload = build_promoted_config(
        model_name=args.model_name,
        weight_file=args.weight_file,
        expected_file_sha256=args.expected_file_sha256,
        expected_effective_sha256=args.expected_effective_sha256,
        activation_reason=args.reason,
    )
    write_json_atomic(PRODUCTION_CONFIG_PATH, payload)
    print(
        json.dumps(
            {
                "production_config_updated": str(PRODUCTION_CONFIG_PATH),
                "model_name": payload["model_name"],
                "weight_file": payload["weight_file"],
                "file_sha256": payload["file_sha256"],
                "effective_sha256": payload["effective_sha256"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
