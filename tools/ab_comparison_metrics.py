"""同一馬券条件での回収率と、欠損を区別した集計。"""

from collections import defaultdict
from itertools import combinations

from race_purchase_filter.core import JUDGE_BUY, is_excluded_race
from race_purchase_filter.evaluation_utils import integer


def purchase_decision(ordered, race, rid, assessment):
    """既存購入フィルターの第1判定を比較順位に対して適用する。"""
    if is_excluded_race(ordered.iloc[0]["race_class"]):
        return "対象外"
    local = race["start"]
    observed = race["observed"]
    horses = [
        {"horse_no": int(h["馬番"]), "horse_name": h["馬名"], "production_rank": int(h["rank"]),
         "popularity_rank": integer(h["人気"]), "popularity_observed_at": observed.isoformat(),
         "ai_observed_at": observed.isoformat()}
        for h in ordered.to_dict("records")
    ]
    context = {"race_id": rid, "date": local.date(), "start_time": local.strftime("%H:%M:%S"),
               "field_size": len(horses), "horses": horses}
    issues, missing = assessment._roster_issues(context)
    statuses = [assessment._first_horse(h, context, observed)["is_mismatch"] for h in horses]
    return assessment._judge(statuses.count(True), statuses.count(None) + missing, issues)


def race_metrics(ordered, result, unit, box_size):
    top5 = [int(n) for n in ordered["馬番"].iloc[:5]]
    pick = top5[0]
    prices = result["payouts"] if result else {}
    finish = result["top"] if result else None
    box = list(combinations(sorted(top5[:box_size]), 3))
    first = ordered.iloc[0]
    popularity = integer(first["人気"])
    distance = integer(first["distance_m"])
    return {
        "top5": top5,
        "rank1_popularity": popularity,
        "popularity_band": ("1" if popularity == 1 else "2–3" if popularity <= 3 else
                            "4–5" if popularity <= 5 else "6+") if popularity else "unknown",
        "place": str(first["place_name"]) or "unknown",
        "distance": str(distance) if distance else "unknown",
        "race_class": str(first["race_class"]) or "unknown",
        "win_hit": pick == finish[0] if finish else None,
        "win_payout": prices["単勝"].get(str(pick), 0) * (unit // 100) if "単勝" in prices else None,
        "place_hit": str(pick) in prices["複勝"] if prices.get("複勝") else None,
        "place_payout": prices["複勝"].get(str(pick), 0) * (unit // 100) if prices.get("複勝") else None,
        "winner_in_top5": finish[0] in top5 if finish else None,
        "top5_slots_captured": len(set(finish) & set(top5)) if finish else None,
        "top5_complete": set(finish).issubset(top5) if finish else None,
        "ticket_investment": len(box) * unit if len(top5) >= box_size else None,
        "ticket_payout": sum(prices["3連複"].get("-".join(map(str, key)), 0) for key in box)
        * (unit // 100) if "3連複" in prices and len(top5) >= box_size else None,
    }


def percent(numerator, denominator):
    return numerator / denominator * 100 if denominator else None


def money(rows):
    settled = [row for row in rows if row["ticket_payout"] is not None]
    investment = sum(row["ticket_investment"] for row in settled)
    payout = sum(row["ticket_payout"] for row in settled)
    cumulative = peak = drawdown = streak = longest = 0
    for row in settled:
        cumulative += row["ticket_payout"] - row["ticket_investment"]
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
        streak = streak + 1 if row["ticket_payout"] == 0 else 0
        longest = max(longest, streak)
    return {
        "purchase_races": len(rows), "settled_races": len(settled),
        "missing_payout_races": len(rows) - len(settled),
        "planned_investment_yen": sum(row["ticket_investment"] for row in rows),
        "investment_yen": investment, "payout_yen": payout, "profit_yen": payout - investment,
        "roi_pct": percent(payout, investment),
        "max_drawdown_yen": drawdown if settled else None,
        "max_consecutive_losses": longest if settled else None,
    }


def summarize(rows, unit, high_payout_n):
    summary = {"races": len(rows), "horses": sum(row["horses"] for row in rows)}
    for metric in ("win_hit", "place_hit", "winner_in_top5", "top5_slots_captured", "top5_complete"):
        values = [row[metric] for row in rows if row[metric] is not None]
        slots = 3 if metric == "top5_slots_captured" else 1
        summary[metric + "_sample"] = len(values)
        summary[metric + "_pct"] = percent(sum(values), len(values) * slots)
    for kind in ("win", "place"):
        known = [row[kind + "_payout"] for row in rows if row[kind + "_payout"] is not None]
        summary[kind + "_roi_sample"] = len(known)
        summary[kind + "_roi_pct"] = percent(sum(known), len(known) * unit)

    for label, eligible in (
        ("all", [row for row in rows if row["ticket_investment"] is not None]),
        ("filtered", [row for row in rows if row["common_purchase"] and row["ticket_investment"] is not None]),
    ):
        highest = sorted(
            (row for row in eligible if row["ticket_payout"]),
            key=lambda row: (-row["ticket_payout"], row["race_id"]),
        )[:high_payout_n]
        high_ids = {row["race_id"] for row in highest}
        full = money(eligible)
        high_money = money(highest)
        remaining = money([row for row in eligible if row["race_id"] not in high_ids])
        summary.update({f"{label}_{key}": value for key, value in full.items()})
        summary.update({f"{label}_without_top_payout_{key}": value for key, value in remaining.items()})
        summary[f"{label}_top_payout_race_ids"] = [row["race_id"] for row in highest]
        summary[f"{label}_top_payout_yen"] = high_money["payout_yen"]
        summary[f"{label}_top_payout_share_pct"] = percent(high_money["payout_yen"], full["payout_yen"])
    return summary


def grouped_summary(race_rows, unit, high_payout_n):
    comparison, breakdown = [], []
    for (baseline, method), rows in race_rows.items():
        for stage in ("validation", "forward"):
            cohort = [row for row in rows if row["stage"] == stage]
            fields = {"baseline": baseline, "method": method, "stage": stage}
            comparison.append({**fields, **summarize(cohort, unit, high_payout_n)})
            for dimension in ("month", "place", "distance", "race_class", "popularity_band"):
                groups = defaultdict(list)
                for row in cohort:
                    groups[row[dimension]].append(row)
                for value, group in sorted(groups.items()):
                    breakdown.append({**fields, "dimension": dimension, "value": value,
                                      **summarize(group, unit, high_payout_n)})
    return comparison, breakdown


def common_purchase(decisions):
    return all(decision == JUDGE_BUY for decision in decisions)
