# -*- coding: utf-8 -*-
"""シャドー比較のpre JSONを保存したまま、確定結果をpost JSONへ照合するCLI。"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_registry import ModelConfigError, write_json_atomic  # noqa: E402
from shadow_validation import finalize_shadow_payload  # noqa: E402
from tokutyouryou_keisann.common import load_results_all_sheets  # noqa: E402


def _read_pre_payload(path: Path) -> dict:
    """pre JSONをobjectとして読み、元ファイルは更新しない。"""
    if not path.is_file():
        raise ModelConfigError(f"pre_result JSONが存在しません: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ModelConfigError(f"pre_result JSONのルートがobjectではありません: {path}")
    return payload


def main() -> None:
    """結果Excelの全シートを共通loaderで読み、別名post JSONを作る。"""
    parser = argparse.ArgumentParser(description="シャドー前向き検証の確定結果照合")
    parser.add_argument("--pre-json", required=True)
    parser.add_argument("--results-xlsx", required=True)
    args = parser.parse_args()

    pre_path = Path(args.pre_json).resolve()
    results_path = Path(args.results_xlsx).resolve()
    pre_payload = _read_pre_payload(pre_path)
    result_entries, result_payouts = load_results_all_sheets(str(results_path))
    post_payload = finalize_shadow_payload(
        pre_payload,
        result_entries,
        result_payouts,
    )
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    post_path = pre_path.with_name(
        f"shadow_comparison_{post_payload['comparison_date']}_post_{timestamp}.json"
    )
    if post_path == pre_path:
        raise ModelConfigError("post_resultはpre_resultを上書きできません")
    write_json_atomic(post_path, post_payload)
    print(f"[OK] post_result JSON: {post_path}")


if __name__ == "__main__":
    main()
