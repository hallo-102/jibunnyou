"""既存出馬表で日付を確定し、隣接日混在結果を原本のまま整理する。"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re

import pandas as pd

from .archive import sha256, write_json
from .review import key, canonical


def canonicalize(source: Path, inputs: list[Path], output: Path, overrides: list[dict] | None = None) -> dict:
    if source.resolve() == output.resolve():
        raise ValueError("結果原本は上書きできません")
    date_by_race = {}
    conflicts = []
    evidence = []
    overrides = overrides or []
    for folder in inputs:
        for path in sorted(folder.glob("*.xlsx")):
            match = re.fullmatch(r"馬の競走成績_(?:with_feat_)?(\d{8})", path.stem)
            if not match:
                continue
            day = match.group(1)
            frame = pd.read_excel(path, sheet_name="今走レース情報", usecols=["レースID"])
            for rid in frame["レースID"].dropna().map(key).unique():
                actual = next((item["actual_date"] for item in overrides if rid.startswith(item["race_id_prefix"])), day)
                if rid in date_by_race and date_by_race[rid] != actual:
                    conflicts.append({"race_id": rid, "dates": [date_by_race[rid], actual]})
                date_by_race[rid] = actual
            evidence.append({"file": str(path), "sha256": sha256(path), "day": day})
    if conflicts:
        raise ValueError(f"開催日証拠が競合しています: {conflicts[:5]}")
    occurrences = defaultdict(list)
    with pd.ExcelFile(source) as excel:
        for day in excel.sheet_names:
            if not re.fullmatch(r"\d{8}", day):
                continue
            frame = pd.read_excel(excel, sheet_name=day)
            if "レースID" not in frame:
                raise ValueError(f"結果にレースID列がありません: {day}")
            frame["レースID"] = frame["レースID"].map(key)
            for rid, group in frame.groupby("レースID", sort=False):
                if not re.fullmatch(r"\d{12}", rid):
                    raise ValueError(f"結果レースIDが不正です: {rid}")
                occurrences[rid].append((day, group))
    selected = defaultdict(list)
    quarantined, resolved = [], []
    for rid, candidates in occurrences.items():
        expected = next((item["actual_date"] for item in overrides if rid.startswith(item["race_id_prefix"])), date_by_race.get(rid))
        days = [day for day, _ in candidates]
        if expected:
            exact = [(day, frame) for day, frame in candidates if day == expected]
            if len(exact) == 1:
                chosen = exact[0][1]
            elif len(candidates) == 1:
                # レースIDと開催日の対応が出馬表で確定し、結果原本も1つなら日付だけ移す。
                chosen = candidates[0][1]
            else:
                reference = candidates[0][1].reset_index(drop=True)
                if not all(reference.equals(frame.reset_index(drop=True)) for _, frame in candidates[1:]):
                    quarantined.append({"race_id": rid, "expected_day": expected, "source_days": days,
                                        "reason": "正しい日付の原本がなく、複数の結果内容が競合"})
                    continue
                chosen = reference
            normalized = canonical(chosen)
            finish = pd.to_numeric(normalized.get("着順", pd.Series(dtype=float)), errors="coerce")
            if not finish.eq(1).any():
                quarantined.append({"race_id": rid, "expected_day": expected, "source_days": days,
                                    "reason": "確定結果がなく、出馬表・発走前データだけが混在"})
                continue
            selected[expected].append(chosen)
            if len(candidates) > 1 or expected not in days:
                resolved.append({"race_id": rid, "day": expected, "excluded_source_days": [d for d in days if d != expected]})
        elif len(candidates) == 1:
            normalized = canonical(candidates[0][1])
            finish = pd.to_numeric(normalized.get("着順", pd.Series(dtype=float)), errors="coerce")
            if not finish.eq(1).any():
                quarantined.append({"race_id": rid, "expected_day": candidates[0][0], "source_days": days,
                                    "reason": "確定結果がなく、出馬表・発走前データだけが混在"})
                continue
            selected[candidates[0][0]].append(candidates[0][1])
        else:
            quarantined.append({"race_id": rid, "source_days": days, "reason": "出馬表による日付照合ができない"})
    report = {"source_sha256": sha256(source), "evidence": evidence, "resolved_duplicates": resolved,
              "quarantined": quarantined, "selected_races": sum(len(v) for v in selected.values()),
              "selected_dates": sorted(selected), "official_overrides": overrides, "production_updated": False}
    # 未解決レースは監査記録を残し、不完全な新マスタの発行を拒否する。
    write_json(output.with_suffix(".audit.json"), report)
    if quarantined:
        raise ValueError(f"日付未確定{len(quarantined)}レース。監査JSONを保存し、マスタ発行を停止しました")
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for day, frames in sorted(selected.items()):
            pd.concat(frames, ignore_index=True).to_excel(writer, sheet_name=day, index=False)
    report["output_sha256"] = sha256(output)
    write_json(output.with_suffix(".audit.json"), report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date-overrides", type=Path)
    args = parser.parse_args()
    overrides = json.loads(args.date_overrides.read_text(encoding="utf-8"))["overrides"] if args.date_overrides else []
    result = canonicalize(args.source, args.inputs, args.output, overrides)
    print(json.dumps({"races": result["selected_races"], "days": len(result["selected_dates"]),
                      "resolved_duplicates": len(result["resolved_duplicates"])}, ensure_ascii=False))
