# -*- coding: utf-8 -*-
"""Immutable pre-result rank comparison and result settlement.

This module does not alter the production ranking, bets or model settings.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

SCHEMA = 1


def _atomic_exclusive_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with path.open("xb") as handle:
        handle.write(raw)


def _validate_horses(horses: list[dict]) -> None:
    if not horses or len(horses) != len({int(h["horse_number"]) for h in horses}):
        raise ValueError("馬番の重複または出走馬欠損")
    for key in ("production_rank", "conditional_rank"):
        ranks = sorted(int(h[key]) for h in horses)
        if ranks != list(range(1, len(horses) + 1)):
            raise ValueError(f"{key}の重複・欠損・連番不一致")
    top5 = lambda key: {int(h["horse_number"]) for h in horses if int(h[key]) <= 5}
    if top5("production_rank") != top5("conditional_rank"):
        raise ValueError("AI上位5頭集合が変わっています")


def record_pre(path: str | Path, *, race_date: str, generation: str,
               recorded_at: str, races: list[dict]) -> str:
    """Create-only record. Never replace an existing pre-result prediction."""
    if len(race_date) != 8 or not race_date.isdigit() or not generation.startswith("cr_fwd_"):
        raise ValueError("前向き世代/日付が不正")
    timestamp = datetime.fromisoformat(recorded_at)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("記録時刻にはタイムゾーンが必要")
    if not races or len({str(r["race_id"]) for r in races}) != len(races):
        raise ValueError("レース重複・欠損")
    for race in races:
        _validate_horses(race["horses"])
        if "finish" in str(race) or "payout" in str(race):
            raise ValueError("preに結果・払戻情報は含められません")
    payload = {
        "schema_version": SCHEMA, "scope": "production_forward",
        "generation": generation, "race_date": race_date, "recorded_at": recorded_at,
        "status": "pre_result", "races": races,
    }
    output = Path(path)
    _atomic_exclusive_json(output, payload)
    return hashlib.sha256(output.read_bytes()).hexdigest()


def settle_post(pre_path: str | Path, post_path: str | Path, *,
                actual: dict[str, dict[int, dict]], finalized_at: str) -> None:
    """Settle a frozen pre snapshot into a distinct, create-only post snapshot."""
    pre_path = Path(pre_path)
    pre_bytes = pre_path.read_bytes()
    pre = json.loads(pre_bytes)
    if pre.get("scope") != "production_forward" or pre.get("status") != "pre_result":
        raise ValueError("前向きpre以外は精算できません")
    if set(actual) != {str(r["race_id"]) for r in pre["races"]}:
        raise ValueError("結果のレース集合がpreと一致しません")
    done = []
    for race in pre["races"]:
        rid = str(race["race_id"])
        entries = actual[rid]
        horse_ids = {int(h["horse_number"]) for h in race["horses"]}
        if set(entries) != horse_ids:
            raise ValueError(f"{rid}: 出走馬の欠損/余分な馬があります")
        finishes = [int(entry["finish"]) for entry in entries.values() if entry.get("finish") is not None]
        if finishes.count(1) != 1 or len(finishes) != len(set(finishes)):
            raise ValueError(f"{rid}: 着順重複・勝ち馬欠損")
        row = dict(race)
        row["actual"] = {str(number): entries[number] for number in sorted(entries)}
        done.append(row)
    payload = {**pre, "status": "post_result", "finalized_at": finalized_at,
               "pre_sha256": hashlib.sha256(pre_bytes).hexdigest(), "races": done}
    _atomic_exclusive_json(Path(post_path), payload)


def summarize_post(paths: list[str | Path], *, generation: str) -> dict:
    """Paired comparison; strict scope/generation and unique race IDs."""
    seen = set()
    total = {"races": 0, "changed": 0, "production_wins": 0,
             "conditional_wins": 0, "production_places": 0, "conditional_places": 0}
    for path in paths:
        item = json.loads(Path(path).read_text(encoding="utf-8"))
        if item.get("scope") != "production_forward" or item.get("generation") != generation or item.get("status") != "post_result":
            raise ValueError("異なる世代・offline・preは集計できません")
        for race in item["races"]:
            rid = str(race["race_id"])
            if rid in seen:
                raise ValueError("レース重複")
            seen.add(rid)
            actual = race["actual"]
            ranked = race["horses"]
            production = next(h for h in ranked if int(h["production_rank"]) == 1)
            conditional = next(h for h in ranked if int(h["conditional_rank"]) == 1)
            pf = actual[str(production["horse_number"])]["finish"]
            cf = actual[str(conditional["horse_number"])]["finish"]
            total["races"] += 1
            total["changed"] += int(production["horse_number"] != conditional["horse_number"])
            total["production_wins"] += int(pf == 1)
            total["conditional_wins"] += int(cf == 1)
            total["production_places"] += int(pf is not None and pf <= 3)
            total["conditional_places"] += int(cf is not None and cf <= 3)
    return total
