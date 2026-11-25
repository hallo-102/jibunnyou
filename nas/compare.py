"""固定された二つの予想を馬・レース単位で比較する。原因の断定はしない。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from continuous_improvement.archive import sha256, verify_archive, write_json
from continuous_improvement.review import key
from shadow_validation import _bet_record

ATOL = 1e-9
RTOL = 1e-9


def numeric_equal(left: object, right: object) -> bool:
    """欠損同士は一致。スコア・特徴量の比較だけに許容差を適用する。"""
    a, b = pd.to_numeric(pd.Series([left, right]), errors="coerce")
    return bool((pd.isna(a) and pd.isna(b)) or np.isclose(a, b, atol=ATOL, rtol=RTOL, equal_nan=True))


def load(path: Path, features: list[str], names: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """順位は保存原本の値を読み、PR #11の回顧順位へ置換しない。"""
    with pd.ExcelFile(path) as book:
        target = pd.read_excel(book, sheet_name="TARGET")
        bets = pd.read_excel(book, sheet_name="買い目_レース別1行")
        roi = pd.read_excel(book, sheet_name="回収率重視_買い目候補") if "回収率重視_買い目候補" in book.sheet_names else pd.DataFrame()
    target["rid_str"] = target["rid_str"].map(key)
    target["馬番"] = pd.to_numeric(target["馬番"], errors="raise").astype(int)
    if target.duplicated(["rid_str", "馬番"]).any():
        raise ValueError("比較対象の馬キーが重複")
    additions = {feature: target[names[feature]] for feature in features
                 if feature not in target and names.get(feature) in target}
    target = pd.concat([target, pd.DataFrame(additions, index=target.index)], axis=1)
    for frame in (bets, roi):
        if "レースID" in frame:
            frame["レースID"] = frame["レースID"].map(key)
            if frame["レースID"].duplicated().any():
                raise ValueError("比較対象の買い目レースキーが重複")
    return target.set_index(["rid_str", "馬番"]), bets, roi


def fixed_tickets(roi: pd.DataFrame, rid: str) -> dict:
    """score/gapの差と区別し、固定券の馬番と購入金額だけを比較する。"""
    rows = roi.loc[roi["レースID"].eq(rid)] if "レースID" in roi else roi.iloc[:0]
    if rows.empty:
        return {}
    row = rows.iloc[0]
    output = {}
    for kind, width, count in (("3連複", 3, 3), ("馬連", 2, 2), ("ワイド", 2, 2)):
        tickets = []
        for i in range(1, count + 1):
            values = [row.get(f"{kind}{i}点目_馬番{j}") for j in range(1, width + 1)]
            if all(pd.notna(value) for value in values):
                tickets.append(sorted(int(value) for value in values))
        amount = row.get(f"{kind}_金額", 0)
        output[kind] = {"tickets": sorted(tickets), "purchase_yen": int(amount) if pd.notna(amount) else 0}
    return output


def compare(left: Path, right: Path, output: Path, metadata: dict, *, provenance: dict, left_raw: Path | None = None) -> dict:
    """モデル差/実装差/データ差の可能性を、固定情報とともに別々に記録する。"""
    if output.resolve() in {left.resolve(), right.resolve()}:
        raise ValueError("比較結果で入力を上書きできません")
    features = metadata["features"]
    a, ab, ar = load(left, features, metadata["feature_names"])
    b, bb, br = load(right, features, metadata["feature_names"])
    trace_sha = None
    if left_raw is not None:
        trace_manifest = json.loads((left_raw.parent / "trace_manifest.json").read_text(encoding="utf-8"))
        trace_sha = sha256(left_raw)
        if trace_manifest["raw_scores_sha256"] != trace_sha or trace_manifest["prediction_sha256"] != sha256(left):
            raise ValueError("旧NASの丸め前スコア診断と予想のSHA不一致")
        trace = pd.read_json(left_raw, orient="records")
        trace["rid_str"] = trace["rid_str"].map(key)
        trace = trace.set_index(["rid_str", "馬番"])
        if trace.index.has_duplicates or set(trace.index) != set(a.index):
            raise ValueError("旧NAS診断の馬集合が予想と不一致")
        a = pd.concat([a.drop(columns=["score_raw", "total"], errors="ignore"), trace[["score_raw", "total"]].reindex(a.index)], axis=1)
    requested = [*features, "total", "score_raw", "score", "rank"]
    columns = [c for c in requested if c in a and c in b]
    common = a.index.intersection(b.index)
    differences = []
    for idx in common:
        changed = {}
        for col in columns:
            av, bv = a.loc[idx, col], b.loc[idx, col]
            equal = (av == bv) if col == "rank" else numeric_equal(av, bv)
            if not equal:
                changed[col] = {"left": None if pd.isna(av) else float(av), "right": None if pd.isna(bv) else float(bv)}
        if changed:
            differences.append({"race_id": idx[0], "horse_number": int(idx[1]), "changes": changed})
    race_ids = sorted(set(ab.get("レースID", [])) | set(bb.get("レースID", [])))
    races = []
    for rid in race_ids:
        left_race = a.loc[a.index.get_level_values(0) == rid]
        right_race = b.loc[b.index.get_level_values(0) == rid]
        record = {"race_id": rid}
        for label, frame, bets, roi in (("left", left_race, ab, ar), ("right", right_race, bb, br)):
            # dense順位の旧TOP5が6頭以上でも、その事実を保持する。
            top5 = frame.loc[pd.to_numeric(frame["rank"], errors="coerce").le(5)].index.get_level_values(1).tolist()
            rows = bets.loc[bets["レースID"].eq(rid)] if "レースID" in bets else bets.iloc[:0]
            record[label] = {"horses": len(frame), "top5": sorted(int(v) for v in top5),
                             "tickets": _bet_record(bets, roi, rid),
                             "fixed_tickets": fixed_tickets(roi, rid),
                             "bet_row": json.loads(rows.to_json(orient="records", force_ascii=False)),
                             "roi_bet_row": json.loads(roi.loc[roi["レースID"].eq(rid)].to_json(orient="records", force_ascii=False)) if "レースID" in roi else []}
        record["top5_changed"] = record["left"]["top5"] != record["right"]["top5"]
        record["tickets_changed"] = record["left"]["tickets"] != record["right"]["tickets"]
        record["roi_bets_changed"] = record["left"]["roi_bet_row"] != record["right"]["roi_bet_row"]
        record["fixed_tickets_changed"] = record["left"]["fixed_tickets"] != record["right"]["fixed_tickets"]
        races.append(record)
    report = {"scope": "offline", "legacy_forward_generation": None, "atol": ATOL, "rtol": RTOL,
              "source_sha256": {"left": sha256(left), "right": sha256(right), "left_raw": trace_sha}, "provenance": provenance,
              "attribution": {"model": provenance.get("model", "unverified"),
                              "implementation": provenance.get("implementation", "unverified"),
                              "data": provenance.get("data", "unverified"),
                              "rule": "複数条件が異なる比較は因果を断定せず、同一入力/同一モデル再実行で切り分ける"},
              "common_horses": len(common), "left_only": [list(x) for x in a.index.difference(b.index)],
              "right_only": [list(x) for x in b.index.difference(a.index)],
              "unavailable_columns": {"left": [c for c in requested if c not in a], "right": [c for c in requested if c not in b]},
              "horse_differences": differences, "races": races}
    write_json(output, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", type=Path, required=True)
    parser.add_argument("--right-run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--left-raw", type=Path, help="legacy_replayが固定保存した丸め前スコア診断")
    parser.add_argument("--provenance", type=Path, required=True, help="model/implementation/dataの検証状態JSON")
    args = parser.parse_args()
    verify_archive(args.right_run)
    runtime = json.loads((args.right_run / "runtime.json").read_text(encoding="utf-8"))
    result = compare(args.left, args.right_run / "prediction.xlsx", args.output, runtime,
                     provenance=json.loads(args.provenance.read_text(encoding="utf-8")), left_raw=args.left_raw)
    print(json.dumps({"common_horses": result["common_horses"], "changed_horses": len(result["horse_differences"])}))
