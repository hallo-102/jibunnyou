"""前向き実績だけを日付・実行コードSHA別に重複なく蓄積する。"""
from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path

from .archive import write_json


def aggregate(base: Path) -> dict:
    selected = {}
    for path in (base / "prospective").glob("*/post/*/*/review.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("mode") != "prospective":
            continue
        key = payload["day"], payload["runtime_id"], payload["model_role"]
        if key not in selected or path.stat().st_mtime_ns > selected[key][0].stat().st_mtime_ns:
            selected[key] = path, payload
    models = defaultdict(list)
    for _, payload in selected.values():
        models[(payload["runtime_id"], payload["model_role"])].append(payload)
    rows = []
    for (runtime_id, role), days in sorted(models.items()):
        days.sort(key=lambda p: p["day"])
        bets = sum(p["overall"]["rank1_bets"] for p in days)
        races = sum(p["overall"]["races"] for p in days)
        purchase = returned = cumulative = peak = drawdown = trifecta_races = 0
        conditions = defaultdict(lambda: {"races": 0, "days": set()})
        for payload in days:
            tickets = payload.get("existing_ticket_roi") or {}
            for detail in tickets.get("details", []):
                current = detail.get("by_type", {}).get("3連複", {})
                if current.get("purchase_yen", 0):
                    trifecta_races += 1
                    purchase += current["purchase_yen"]
                    returned += current["return_yen"]
                    cumulative += current["profit_yen"]
                    peak = max(peak, cumulative)
                    drawdown = max(drawdown, peak - cumulative)
            for condition in payload["stratified"]:
                key = json.dumps(condition["conditions"], sort_keys=True, ensure_ascii=False)
                conditions[key]["races"] += condition["races"]
                conditions[key]["days"].add(payload["day"])
        rows.append({"runtime_id": runtime_id, "role": role, "days": [p["day"] for p in days],
                     "races": races, "rank1_bets": bets,
                     "win_roi": sum(p["overall"]["win_return_yen"] for p in days) / (100 * bets) if bets else None,
                     "place_roi": sum(p["overall"]["place_return_yen"] for p in days) / (100 * bets) if bets else None,
                     "current_trifecta_races": trifecta_races, "current_trifecta_roi": returned / purchase if purchase else None,
                     "current_trifecta_profit_yen": returned - purchase, "current_trifecta_max_drawdown_yen": drawdown,
                     "prediction_sample_ready": len(days) >= 8 and races >= 300,
                     "profit_sample_ready": trifecta_races >= 50,
                     "conditions": [{"conditions": json.loads(k), "races": v["races"], "days": len(v["days"]),
                                     "sample_sufficient": v["races"] >= 35} for k,v in sorted(conditions.items())],
                     "production_promotion_allowed": False})
    output = {"schema_version": 1, "scope": "prospective_only", "legacy_forward_generation": None,
              "models": rows, "production_promotion_allowed": False}
    write_json(base / "history.json", output)
    return output
