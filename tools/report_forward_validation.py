# -*- coding: utf-8 -*-
"""事前固定指標だけで本番・シャドーの前向き検証を集計する。"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_registry import ModelConfigError, write_json_atomic  # noqa: E402
from shadow_validation import (  # noqa: E402
    load_forward_validation_policy,
    load_shadow_runtime,
)


def _load_latest_post_per_day(output_dir: Path) -> list[dict[str, Any]]:
    """同一日の再照合を重複集計せず、最新postだ1つに固定する。"""
    selected: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in output_dir.iterdir() if path.is_dir()) if output_dir.exists() else []:
        candidates = sorted(day_dir.glob("shadow_comparison_*_post_*.json"))
        if not candidates:
            continue
        latest = candidates[-1]
        payload = json.loads(latest.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("result_status") != "post_result":
            raise ModelConfigError(f"post_result JSONが不正です: {latest}")
        payload["_source_path"] = str(latest)
        selected.append(payload)
    return selected


def _maximum_drawdown(profits: list[int]) -> int:
    """初期累積収支0を高値に含め、最大ドローダウン額を返す。"""
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for profit in profits:
        cumulative += int(profit)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return max_drawdown


def _horse_number(row: Mapping[str, Any] | None) -> int | None:
    """予想・結果recordから馬番を安全に整数化する。"""
    if not isinstance(row, Mapping):
        return None
    value = row.get("horse_number")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _metrics_for_model(races: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    """主要指標・安全指標を予定外の条件層別なしで集計する。"""
    evaluated = 0
    top3_complete_count = 0
    top5_point_sum = 0.0
    rank1_place_count = 0
    rank1_finishes: list[float] = []
    purchase_yen = 0
    return_yen = 0
    eligible_trifecta_races = 0
    chronological_profits: list[int] = []
    daily_profit: dict[str, int] = defaultdict(int)
    monthly_profit: dict[str, int] = defaultdict(int)

    for race in sorted(races, key=lambda row: (str(row.get("race_date", "")), str(row.get("race_id", "")))):
        actual_finish = race.get("actual_finish")
        if not isinstance(actual_finish, list) or not actual_finish:
            continue
        actual_by_horse: dict[int, float] = {}
        actual_top3: list[int] = []
        for actual in actual_finish:
            horse = _horse_number(actual)
            if horse is None or not isinstance(actual, Mapping):
                continue
            try:
                finish = float(actual.get("finish"))
            except (TypeError, ValueError):
                continue
            actual_by_horse[horse] = finish
            if finish in (1.0, 2.0, 3.0):
                actual_top3.append(horse)
        top5_raw = race.get(f"{prefix}_top5")
        if isinstance(top5_raw, list):
            top5 = [
                horse
                for horse in (_horse_number(row) for row in top5_raw)
                if horse is not None
            ]
        else:
            top5 = []
        if len(actual_top3) == 3 and top5:
            evaluated += 1
            top3_complete_count += int(set(actual_top3).issubset(set(top5)))
            top5_point_sum += sum(
                weight
                for finish, weight in ((1.0, 3.0), (2.0, 2.0), (3.0, 1.0))
                for horse, actual in actual_by_horse.items()
                if actual == finish and horse in top5
            )
            rank1 = top5[0]
            if rank1 in actual_by_horse:
                rank1_finishes.append(actual_by_horse[rank1])
                rank1_place_count += int(actual_by_horse[rank1] <= 3.0)

        profit = race.get(f"{prefix}_profit")
        if isinstance(profit, Mapping):
            stake = int(profit.get("purchase_yen", 0) or 0)
            returned = int(profit.get("return_yen", 0) or 0)
            race_profit = int(profit.get("profit_yen", returned - stake) or 0)
            purchase_yen += stake
            return_yen += returned
            if stake > 0:
                eligible_trifecta_races += 1
                chronological_profits.append(race_profit)
                race_date = str(race.get("race_date", ""))
                daily_profit[race_date] += race_profit
                monthly_profit[race_date[:6]] += race_profit

    racing_days = len({str(race.get("race_date", "")) for race in races})
    black_days = sum(1 for profit in daily_profit.values() if profit > 0)
    return {
        "races": evaluated,
        "racing_days": racing_days,
        "current_trifecta_races": eligible_trifecta_races,
        "current_trifecta_purchase_yen": purchase_yen,
        "current_trifecta_return_yen": return_yen,
        "current_trifecta_profit_yen": return_yen - purchase_yen,
        "current_trifecta_roi": (return_yen / purchase_yen) if purchase_yen else None,
        "current_trifecta_max_drawdown_yen": _maximum_drawdown(chronological_profits),
        "top3_complete_count": top3_complete_count,
        "top3_complete_rate": (top3_complete_count / evaluated) if evaluated else None,
        "top5_point_sum": top5_point_sum,
        "top5_point_rate": (top5_point_sum / (evaluated * 6.0)) if evaluated else None,
        "rank1_place_count": rank1_place_count,
        "rank1_place_rate": (
            rank1_place_count / len(rank1_finishes) if rank1_finishes else None
        ),
        "rank1_average_finish": (
            sum(rank1_finishes) / len(rank1_finishes) if rank1_finishes else None
        ),
        "daily_profit_yen": dict(sorted(daily_profit.items())),
        "monthly_profit_yen": dict(sorted(monthly_profit.items())),
        "black_days": black_days,
        "black_day_rate": (black_days / len(daily_profit)) if daily_profit else None,
    }


def main() -> None:
    """日次post JSONを集約し、昇格せず判定可否状態と差を出力する。"""
    policy = load_forward_validation_policy()
    runtime = load_shadow_runtime()
    daily_payloads = _load_latest_post_per_day(runtime.output_dir)
    races = [
        dict(race)
        for payload in daily_payloads
        for race in payload.get("races", [])
        if isinstance(race, Mapping)
    ]
    race_ids = [str(race.get("race_id", "")) for race in races]
    if len(race_ids) != len(set(race_ids)):
        raise ModelConfigError("前向き検証集計に重複race_idがあります")

    production = _metrics_for_model(races, "production")
    shadow = _metrics_for_model(races, "shadow")
    gate = policy["prediction_interim_gate"]
    profit_gate = policy["profit_final_gate"]
    latest_date = max((str(race.get("race_date", "")) for race in races), default="")
    prediction_gate_ready = (
        production["racing_days"] >= int(gate["minimum_racing_days"])
        and production["races"] >= int(gate["minimum_races"])
    )
    profit_gate_ready = (
        production["current_trifecta_races"]
        >= int(profit_gate["minimum_current_trifecta_races"])
        or latest_date >= str(profit_gate["validation_end_date"])
    )
    report = {
        "schema_version": 1,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "start_date": policy["start_date"],
        "latest_result_date": latest_date,
        "source_post_files": [payload["_source_path"] for payload in daily_payloads],
        "production": production,
        "shadow": shadow,
        "shadow_minus_production": {
            key: (
                None
                if production.get(key) is None or shadow.get(key) is None
                else shadow[key] - production[key]
            )
            for key in (
                "current_trifecta_profit_yen",
                "current_trifecta_roi",
                "current_trifecta_max_drawdown_yen",
                "top3_complete_rate",
                "rank1_place_rate",
                "rank1_average_finish",
                "top5_point_rate",
            )
        },
        "prediction_interim_gate_ready": prediction_gate_ready,
        "profit_final_gate_ready": profit_gate_ready,
        "promotion_allowed_by_this_report": False,
        "insufficient_profit_sample_action": (
            None
            if profit_gate_ready
            else profit_gate["insufficient_sample_action"]
        ),
        "fixed_primary_metrics": policy["primary_metrics"],
        "fixed_safety_metrics": policy["safety_metrics"],
        "selection_policy": policy["selection_policy"],
    }
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    output_path = runtime.output_dir / f"forward_validation_report_{timestamp}.json"
    write_json_atomic(output_path, report)
    print(json.dumps({"report": str(output_path), **report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
