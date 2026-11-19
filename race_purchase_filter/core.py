"""Pure logic for race purchase suitability filter.

No GUI imports. No automatic calculation of evaluation-disagreement count:
that formula is unconfirmed in the original source, so the count is manual
input only (see README).
"""
from __future__ import annotations

# Judgement labels
JUDGE_BUY = "購入候補"
JUDGE_SKIP = "見送り候補"
JUDGE_HOLD = "判定保留"
C_STATUS = "判断保留・未算出"
UNKNOWN = "不明"
UNCALCULATED = "未計算"

CONDITION_VERSION_DEFAULT = "v1-manual-disagreement-2026-11-21"

EXCLUDED_KEYWORDS = ("新馬", "未勝利")


def judge_disagreement(count):
    """Provisional judgement from MANUAL disagreement count only.

    0-1 -> 購入候補, >=2 -> 見送り候補, None/unknown -> 判定保留.
    This is a verification judgement, not an automatic purchase decision.
    """
    if count is None:
        return JUDGE_HOLD
    # bool is subclass of int; reject it explicitly
    if isinstance(count, bool):
        return JUDGE_HOLD
    try:
        value = int(count)
    except (TypeError, ValueError):
        return JUDGE_HOLD
    # Reject non-integral floats / strings that are not plain ints via str check for strings
    if isinstance(count, str) and count.strip() == "":
        return JUDGE_HOLD
    if isinstance(count, float) and not count.is_integer():
        return JUDGE_HOLD
    if value < 0:
        return JUDGE_HOLD
    if value <= 1:
        return JUDGE_BUY
    return JUDGE_SKIP


def judge_c():
    """C method threshold is undetermined: always hold / uncalculated."""
    return C_STATUS


def is_excluded_race(race_name_or_class, exclude_enabled=True):
    """Return True if race name/class contains 新馬 or 未勝利 and setting is on."""
    if not exclude_enabled:
        return False
    if not race_name_or_class:
        return False
    text = str(race_name_or_class)
    return any(kw in text for kw in EXCLUDED_KEYWORDS)


def parse_horse_numbers(text):
    """Parse '3,5,7' or '3 5 7' or list into list[int]; empty -> []."""
    if text is None:
        return []
    if isinstance(text, (list, tuple)):
        result = []
        for item in text:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result
    s = str(text).replace("、", ",").replace(" ", ",")
    result = []
    for part in s.split(","):
        part = part.strip()
        if part == "":
            continue
        try:
            result.append(int(part))
        except ValueError:
            continue
    return result


def parse_combination(combination):
    """Parse '3-5' / '3-5-7' into list[int]; invalid -> None."""
    if combination is None:
        return None
    if isinstance(combination, (list, tuple)):
        try:
            return [int(x) for x in combination]
        except (TypeError, ValueError):
            return None
    s = str(combination).strip()
    if s == "":
        return None
    parts = s.split("-")
    result = []
    for part in parts:
        part = part.strip()
        if part == "":
            return None
        try:
            result.append(int(part))
        except ValueError:
            return None
    return result


def check_ticket_hit(ticket_type, combination, finish_top3):
    """Return True/False for hit, or None if uncalculable / unknown.

    finish_top3: [1st, 2nd, 3rd] horse numbers (ints).
    Result-sheet '的中〇' column is never used; this is the only hit logic.
    """
    if not finish_top3 or len(finish_top3) < 3:
        return None
    try:
        finish = [int(x) for x in finish_top3[:3]]
    except (TypeError, ValueError):
        return None
    combo = parse_combination(combination)
    if combo is None:
        return None
    t = str(ticket_type).strip() if ticket_type else ""
    if t == "単勝":
        if len(combo) != 1:
            return None
        return combo[0] == finish[0]
    if t == "複勝":
        if len(combo) != 1:
            return None
        return combo[0] in finish
    if t in ("枠連", "馬連"):
        if len(combo) != 2:
            return None
        return set(combo) == set(finish[:2])
    if t == "ワイド":
        if len(combo) != 2:
            return None
        return set(combo).issubset(set(finish))
    if t == "馬単":
        if len(combo) != 2:
            return None
        return combo == finish[:2]
    if t == "3連複":
        if len(combo) != 3:
            return None
        return set(combo) == set(finish)
    if t == "3連単":
        if len(combo) != 3:
            return None
        return combo == finish
    return None


def compute_top5_capture(finish_top3, predicted_top5):
    """Count of finish top3 horses included in predicted TOP5.

    Returns None (未計算) if predicted_top5 is missing/empty.
    """
    if not predicted_top5:
        return None
    try:
        top5 = [int(x) for x in predicted_top5]
        finish = [int(x) for x in finish_top3[:3]]
    except (TypeError, ValueError):
        return None
    if not top5:
        return None
    return sum(1 for h in finish if h in top5)


def _aggregate_subset(races):
    """races: list of dicts with keys investment, payout, top5_capture.

    investment/payout may be None (unknown) -> then totals become None.
    Never fill unknown with 0.
    """
    race_count = len(races)
    # If any payout/investment is unknown, totals are uncalculable.
    has_unknown_money = any(
        r.get("investment") is None or r.get("payout") is None for r in races
    )
    if race_count == 0:
        investment = 0
        payout = 0
        profit = 0
        recovery = None
    elif has_unknown_money:
        investment = None
        payout = None
        profit = None
        recovery = None
    else:
        investment = sum(r["investment"] for r in races)
        payout = sum(r["payout"] for r in races)
        profit = payout - investment
        recovery = (payout / investment * 100.0) if investment else None
    # TOP5 capture: only races where capture is recorded
    recorded = [r for r in races if r.get("top5_capture") is not None]
    if recorded:
        capture_total = sum(r["top5_capture"] for r in recorded)
        denominator_races = len(recorded)
        denominator_slots = denominator_races * 3
        capture_rate = capture_total / denominator_slots * 100.0 if denominator_slots else None
    else:
        capture_total = None
        denominator_races = 0
        denominator_slots = 0
        capture_rate = None
    return {
        "race_count": race_count,
        "investment": investment,
        "payout": payout,
        "profit": profit,
        "recovery_rate": recovery,
        "top5_capture_total": capture_total,
        "top5_recorded_races": denominator_races,
        "top5_denominator_slots": denominator_slots,
        "top5_capture_rate": capture_rate,
    }


def _race_money_and_capture(result, snapshot):
    """Extract per-race investment/payout/capture from a result dict."""
    tickets = result.get("tickets") or []
    if not tickets:
        return None  # not a purchased/recorded race for aggregation
    investment = 0
    payout = 0
    money_unknown = False
    for tk in tickets:
        amount = tk.get("amount")
        pay = tk.get("payout")
        if amount is None or pay is None:
            money_unknown = True
        else:
            try:
                investment += int(amount)
                payout += int(pay)
            except (TypeError, ValueError):
                money_unknown = True
    # TOP5 capture: prefer explicit value, else compute from top5 if present
    capture = result.get("top5_capture_count")
    if capture is None:
        top5 = result.get("predicted_top5") or (snapshot.get("predicted_top5") if snapshot else None)
        finish = result.get("finish_top3")
        if top5 and finish:
            capture = compute_top5_capture(finish, top5)
    else:
        try:
            capture = int(capture)
        except (TypeError, ValueError):
            capture = None
    if money_unknown:
        return {"investment": None, "payout": None, "top5_capture": capture}
    return {"investment": investment, "payout": payout, "top5_capture": capture}


def aggregate_methods(snapshots, results):
    """Aggregate A/B/C comparison.

    Premise (must be shown in UI/README): same original data, same ticket
    type, same amount across methods.

    A: all races with result + tickets recorded (excluding target-excluded races)
    B: subset of A with manual disagreement count 0-1; >=2 and unknown excluded
    C: always 未算出 (threshold undetermined); never compute recovery/profit.
    """
    snap_by_race = {}
    for s in snapshots or []:
        rid = s.get("race_id")
        if rid is not None:
            snap_by_race[str(rid)] = s
    a_races = []
    b_races = []
    b_excluded = 0
    excluded_target_count = 0
    for r in results or []:
        rid = str(r.get("race_id")) if r.get("race_id") is not None else None
        snap = snap_by_race.get(rid, {}) if rid else {}
        # Target-excluded races are distinguished and kept out of comparison
        if snap.get("is_excluded_target"):
            # Still count it as excluded-target if it has tickets
            if r.get("tickets"):
                excluded_target_count += 1
            continue
        extracted = _race_money_and_capture(r, snap)
        if extracted is None:
            continue
        a_races.append(extracted)
        dc = snap.get("disagreement_count")
        if dc is not None and not isinstance(dc, bool):
            try:
                dc_int = int(dc)
                if 0 <= dc_int <= 1:
                    b_races.append(extracted)
                else:
                    b_excluded += 1
            except (TypeError, ValueError):
                b_excluded += 1
        else:
            b_excluded += 1
    a = _aggregate_subset(a_races)
    b = _aggregate_subset(b_races)
    b["excluded_count"] = b_excluded
    a["excluded_count"] = 0
    c = {
        "status": "未算出",
        "race_count": None,
        "investment": None,
        "payout": None,
        "profit": None,
        "recovery_rate": None,
        "top5_capture_total": None,
        "top5_recorded_races": 0,
        "top5_denominator_slots": 0,
        "top5_capture_rate": None,
        "excluded_count": None,
    }
    return {"A": a, "B": b, "C": c, "excluded_target_count": excluded_target_count}


def list_places(races):
    """競馬場の一覧を読込順で返す（「不明」は除く）。"""
    places = []
    for r in races or []:
        p = r.get("place")
        if p and p != UNKNOWN and p not in places:
            places.append(p)
    return places


def filter_races_by_place(races, place):
    """place が None/空/「すべて」なら全件、それ以外はその競馬場のレースのみ返す。"""
    races = list(races or [])
    if place is None or str(place).strip() in ("", "すべて"):
        return races
    return [r for r in races if r.get("place") == place]


def format_money(value):
    return UNKNOWN if value is None else str(value)


def format_rate(value):
    return UNCALCULATED if value is None else f"{value:.1f}%"
