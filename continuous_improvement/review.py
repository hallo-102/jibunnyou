"""固定済み予想をレースID×馬番で照合し、条件別・特徴量別に評価する。"""
from __future__ import annotations

import json
import math
from pathlib import Path
import re

import numpy as np
import pandas as pd

from .archive import sha256, verify_archive, write_json


def key(value: object) -> str:
    # Excelが数値保存したIDも、競馬場の開催回を含む12桁IDのまま扱う。
    return re.sub(r"\.0$", "", str(value).strip())


def number(value: object) -> float | None:
    try:
        result = float(str(value).replace(",", "").replace("円", ""))
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def canonical(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    # 人気は馬の人気と払戻人気で意味が異なり、同じ名前へ潰してはいけない。
    frame.columns = ["出走馬人気" if str(c).strip() == "人 気" else re.sub(r"\s+", "", str(c)) for c in frame.columns]
    if frame.columns.duplicated().any():
        raise ValueError("正規化後のヘッダーが重複しています")
    return frame


def load_results(path: Path, day: str) -> tuple[pd.DataFrame, dict]:
    # 結果マスタの開催日シートだけを使い、同じrace_idの別日誤混入を防ぐ。
    raw = canonical(pd.read_excel(path, sheet_name=day))
    required = {"レースID", "馬番", "着順", "払戻種別", "組番", "払戻金"}
    if not required.issubset(raw):
        raise ValueError(f"結果必須列不足: {sorted(required - set(raw))}")
    raw["rid_str"] = raw["レースID"].map(key)
    horses = raw.loc[raw["馬番"].notna()].copy()
    horses["馬番"] = pd.to_numeric(horses["馬番"], errors="raise").astype(int)
    horses["finish"] = pd.to_numeric(horses["着順"], errors="coerce")
    # 取消・除外・中止・失格を欠測結果と区別して保存する。
    statuses = {"取消": "SCRATCHED", "除外": "EXCLUDED", "中止": "DNF", "失格": "DISQUALIFIED"}
    horses["result_status"] = ["FINISHED" if pd.notna(f) else next((v for k, v in statuses.items() if k in str(s)), "UNKNOWN")
                                for f, s in zip(horses["finish"], horses["着順"])]
    horses["実馬場"] = horses.get("レース情報", pd.Series("", index=horses.index)).astype(str).str.extract(r"馬場[:：]\s*(不良|稍重|重|良)", expand=False)
    if horses.duplicated(["rid_str", "馬番"]).any() or horses["result_status"].eq("UNKNOWN").any():
        raise ValueError("結果の主キー重複、または未確定着順を検知しました")
    payouts = {}
    for _, row in raw.loc[raw["払戻種別"].notna()].iterrows():
        kind = str(row["払戻種別"])
        combo = "-".join(str(int(x)) for x in re.findall(r"\d+", key(row["組番"])))
        amount = number(row["払戻金"])
        if amount is None or amount <= 0 or not combo:
            raise ValueError("未確定・不正払戻を検知しました")
        payout_key = (row["rid_str"], kind, combo)
        if payout_key in payouts and payouts[payout_key] != amount:
            raise ValueError("同じ払戻キーの金額が競合しています")
        payouts[payout_key] = amount
    for rid in horses["rid_str"].unique():
        if not any(k[0] == rid and k[1] == "単勝" for k in payouts) or not any(k[0] == rid and k[1] == "複勝" for k in payouts):
            raise ValueError(f"確定単複払戻が不足しています: {rid}")
    return horses, payouts


def predictions(path: Path, metadata: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = pd.read_excel(path, sheet_name="TARGET")
    frame["rid_str"] = frame["rid_str"].map(key)
    frame["馬番"] = pd.to_numeric(frame["馬番"], errors="raise").astype(int)
    if frame.duplicated(["rid_str", "馬番"]).any():
        raise ValueError("予想の主キーが重複しています")
    frame["pred_rank"] = pd.to_numeric(frame["rank"], errors="raise")
    if frame["pred_rank"].isna().any() or frame["pred_rank"].le(0).any():
        raise ValueError("予想順位に欠損または不正値があります")
    # 旧NAS本番では表示スコアを丸めた同点が同順位になることがある。
    # 発走前の原本は変更せず、回顧専用に実効total・馬番で一意化する。
    # 既存の異なる順位を逆転させず、同順位の内部だけを決定的に並べる。
    frame["original_pred_rank"] = frame["pred_rank"]
    frame["rank_tiebreak_applied"] = False
    duplicate = frame.duplicated(["rid_str", "pred_rank"], keep=False)
    if duplicate.any():
        if "total" not in frame:
            raise ValueError("順位重複の解消に必要なtotal列がありません")
        frame["total"] = pd.to_numeric(frame["total"], errors="coerce")
        if frame.loc[duplicate, "total"].isna().any():
            raise ValueError("順位重複馬のtotalが欠損しています")
        affected = set(frame.loc[duplicate, "rid_str"])
        ranked = frame.loc[frame["rid_str"].isin(affected)].sort_values(
            ["rid_str", "original_pred_rank", "total", "馬番"],
            ascending=[True, True, False, True], kind="stable",
        )
        frame.loc[ranked.index, "pred_rank"] = ranked.groupby("rid_str").cumcount().add(1).to_numpy()
        frame.loc[frame["rid_str"].isin(affected), "rank_tiebreak_applied"] = True
    if frame.duplicated(["rid_str", "pred_rank"]).any():
        raise ValueError("回顧用順位の一意化に失敗しました")
    # 日本語出力列を既存設定の対応表から内部特徴量名へ戻す。
    additions = {}
    for feature in metadata["features"]:
        label = metadata["feature_names"].get(feature, feature)
        if feature not in frame:
            additions[feature] = pd.to_numeric(frame[label], errors="coerce") if label in frame else np.nan
    frame = pd.concat([frame, pd.DataFrame(additions, index=frame.index)], axis=1)
    context = pd.read_excel(path, sheet_name="今走レース情報")
    context["rid_str"] = context["レースID"].map(key)
    context = context[["rid_str", "場所", "コース", "クラス", "馬場"]].drop_duplicates()
    if context["rid_str"].duplicated().any():
        raise ValueError("同一レースの条件情報が競合しています")
    frame = frame.merge(context, on="rid_str", how="left", validate="many_to_one")
    frame["芝ダ"] = frame["コース"].astype(str).str.extract(r"(芝|ダ|障)", expand=False).fillna("不明")
    distance = pd.to_numeric(frame["コース"].astype(str).str.extract(r"(\d{3,4})", expand=False), errors="coerce")
    frame["距離帯"] = pd.cut(distance, [0, 1400, 1800, 2200, float("inf")], labels=["短距離", "マイル", "中距離", "長距離"]).astype("string").fillna("不明")
    frame["評価区分"] = np.where(frame["クラス"].astype(str).str.contains("新馬|未勝利"), "新馬・未勝利", "主要評価")
    for col in ("人気", "単勝オッズ"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce") if col in frame else np.nan
    frame["人気帯"] = pd.cut(frame["人気"], [0, 3, 6, 10, float("inf")], labels=["1-3", "4-6", "7-10", "11+"]).astype("string").fillna("不明")
    frame["オッズ帯"] = pd.cut(frame["単勝オッズ"], [0, 3, 10, 30, float("inf")], labels=["3未満相当", "3-10", "10-30", "30超"]).astype("string").fillna("不明")
    return frame, context


def metrics(frame: pd.DataFrame, payouts: dict, rank_col: str = "pred_rank") -> dict:
    settled = frame.loc[~frame["result_status"].isin(["SCRATCHED", "EXCLUDED"])].copy()
    top = settled.loc[settled[rank_col].eq(1)]
    correlation = []
    captured, actual_count, complete = 0, 0, 0
    for _, race in settled.groupby("rid_str"):
        valid = race.dropna(subset=["finish", rank_col])
        if len(valid) > 1:
            corr = valid[rank_col].rank().corr(valid["finish"].rank())
            if pd.notna(corr):
                correlation.append(float(corr))
        winners = race.loc[race["finish"].le(3)]
        caught = int(winners[rank_col].le(5).sum())
        captured += caught
        actual_count += len(winners)
        complete += int(bool(len(winners)) and caught == len(winners))
    win_return = sum(payouts.get((row.rid_str, "単勝", str(row.馬番)), 0) for row in top.itertuples())
    place_return = sum(payouts.get((row.rid_str, "複勝", str(row.馬番)), 0) for row in top.itertuples())
    return {"races": int(frame["rid_str"].nunique()), "horses": len(frame), "rank1_bets": len(top),
            "rank1_win_rate": float(top["finish"].eq(1).mean()) if len(top) else None,
            "rank1_place_rate": float(top["finish"].le(3).mean()) if len(top) else None,
            "top5_top3_capture_rate": captured / actual_count if actual_count else None,
            "top3_complete_races": complete,
            "rank_finish_spearman": float(np.mean(correlation)) if correlation else None,
            "win_return_yen": win_return, "place_return_yen": place_return,
            "win_roi": win_return / (100 * len(top)) if len(top) else None,
            "place_roi": place_return / (100 * len(top)) if len(top) else None}


def settle_existing(workbook: Path, payouts: dict, horses: pd.DataFrame) -> dict | None:
    # 既存の買い目抽出・3連複精算を再利用し、買い目を結果から再生成しない。
    from shadow_validation import _bet_record, _settle_current_trifecta
    with pd.ExcelFile(workbook) as excel:
        if "回収率重視_買い目候補" not in excel.sheet_names:
            return None
        roi = pd.read_excel(excel, sheet_name="回収率重視_買い目候補")
    details = []
    for _, row in roi.iterrows():
        rid = key(row["レースID"])
        record = _bet_record(pd.DataFrame(), roi, rid)
        current = record["current_trifecta"]
        if current["purchase_yen"] != len(current["tickets"]) * 100:
            raise ValueError("固定3連複の購入金額と100円単位の買い目が一致しません")
        result = {"race_id": rid, "by_type": {}}
        scratched = set(horses.loc[horses["rid_str"].eq(rid) & horses["result_status"].isin(["SCRATCHED", "EXCLUDED"]), "馬番"])
        for kind, count in (("3連複", 3), ("馬連", 2), ("ワイド", 2)):
            tickets = current["tickets"] if kind == "3連複" else []
            if kind != "3連複":
                for index in range(1, count + 1):
                    values = [number(row.get(f"{kind}{index}点目_馬番{position}")) for position in (1, 2)]
                    if all(v is not None for v in values):
                        tickets.append(sorted(int(v) for v in values))
            purchase = int(number(row.get(f"{kind}_金額")) or 0)
            if purchase != 100 * len(tickets) or len({tuple(t) for t in tickets}) != len(tickets):
                raise ValueError(f"固定{kind}の購入金額・組合せが不正です")
            if not tickets:
                continue
            # JRA公式の返還規則: https://www.jra.go.jp/kouza/baken/
            # 出走取消・競走除外を含む固定馬番券だけを返還し、競走中止は返還しない。
            refund_yen = 100 * sum(bool(set(ticket) & scratched) for ticket in tickets)
            mapping = {combo: int(amount) for (race, bet, combo), amount in payouts.items() if race == rid and bet == kind}
            if not mapping and refund_yen != purchase:
                raise ValueError(f"購入券種の確定払戻が不足しています: {rid} {kind}")
            settled = _settle_current_trifecta(record, mapping) if kind == "3連複" else {
                "purchase_yen": purchase,
                "return_yen": sum(mapping.get("-".join(map(str, ticket)), 0) for ticket in tickets)}
            result["by_type"][kind] = {"purchase_yen": purchase, "return_yen": settled["return_yen"] + refund_yen,
                                       "refund_yen": refund_yen,
                                       "profit_yen": settled["return_yen"] + refund_yen - purchase}
        result["purchase_yen"] = sum(x["purchase_yen"] for x in result["by_type"].values())
        result["return_yen"] = sum(x["return_yen"] for x in result["by_type"].values())
        details.append(result)
    invest = sum(x["purchase_yen"] for x in details)
    returned = sum(x["return_yen"] for x in details)
    return {"purchase_yen": invest, "return_yen": returned, "profit_yen": returned - invest,
            "roi": returned / invest if invest else None, "races": len(details), "details": details}


def rank1_metrics(frame: pd.DataFrame, payouts: dict, rank_col: str = "pred_rank") -> dict:
    # 個別除外の反復では必要な1位指標だけを計算し、全順位相関の再計算を避ける。
    top = frame.loc[frame[rank_col].eq(1) & ~frame["result_status"].isin(["SCRATCHED", "EXCLUDED"])]
    count = len(top)
    return {"rank1_place_rate": float(top["finish"].le(3).mean()) if count else None,
            "win_roi": sum(payouts.get((r.rid_str, "単勝", str(r.馬番)), 0) for r in top.itertuples()) / (100 * count) if count else None,
            "place_roi": sum(payouts.get((r.rid_str, "複勝", str(r.馬番)), 0) for r in top.itertuples()) / (100 * count) if count else None}


def review(pre: Path, results: Path, *, minimum_races: int = 35, shadow: Path | None = None) -> Path:
    manifest = verify_archive(pre)
    model_folder = shadow or pre
    metadata = json.loads((model_folder / "runtime.json").read_text(encoding="utf-8"))
    if shadow:
        from .shadow import verify_shadow
        shadow_manifest = verify_shadow(shadow)
        if shadow_manifest["production_pre_sha256"] != sha256(pre / "manifest.json"):
            raise ValueError("Shadowと本番固定保存のSHA不一致")
    horses, payouts = load_results(results, manifest["day"])
    pred, context = predictions(model_folder / "prediction.xlsx", metadata)
    expected = pd.read_excel(pre / "input.xlsx", sheet_name="今走レース情報")
    expected["rid_str"] = expected["レースID"].map(key)
    expected["馬番"] = pd.to_numeric(expected["馬番"], errors="raise").astype(int)
    # 旧マスタには同じ日付シートへ隣接開催日が混入した実データがある。
    # 原本を保持し、入力に存在しないレースは日次評価から明示的に隔離する。
    unrelated_races = sorted(set(horses["rid_str"]) - set(expected["rid_str"]))
    horses = horses.loc[horses["rid_str"].isin(expected["rid_str"])].copy()
    expected_keys = set(zip(expected["rid_str"], expected["馬番"]))
    result_keys = set(zip(horses["rid_str"], horses["馬番"]))
    if expected_keys != result_keys:
        raise ValueError(f"全レース結果未完了: missing={len(expected_keys-result_keys)} extra={len(result_keys-expected_keys)}")
    joined = pred.merge(horses[["rid_str", "馬番", "finish", "result_status", "実馬場"]], on=["rid_str", "馬番"], how="left", validate="one_to_one")
    joined["馬場"] = joined["実馬場"].combine_first(joined["馬場"])
    if joined["result_status"].isna().any():
        raise ValueError("予想と結果の一意結合に失敗しました")
    overall = metrics(joined, payouts)
    ticket_result = settle_existing(model_folder / "prediction.xlsx", payouts, horses)
    stratified = []
    for dimensions in [["評価区分"], ["場所"], ["芝ダ"], ["距離帯"], ["クラス"], ["馬場"], ["人気帯"], ["オッズ帯"], ["場所", "芝ダ", "距離帯", "評価区分"]]:
        for values, group in joined.groupby(dimensions, dropna=False, observed=True):
            values = values if isinstance(values, tuple) else (values,)
            item = {"conditions": {k: str(v) for k, v in zip(dimensions, values)}, **metrics(group, payouts)}
            item["sample_sufficient"] = item["races"] >= minimum_races
            item["rank1_roi_sample_sufficient"] = item["rank1_bets"] >= minimum_races
            # 人気・オッズ帯は軸である予想1位の帯へ、既存買い目の収支を帰属させる。
            group_races = set(group.loc[group["pred_rank"].eq(1), "rid_str"])
            ticket_rows = [row for row in (ticket_result or {}).get("details", []) if row["race_id"] in group_races]
            invested = sum(row["purchase_yen"] for row in ticket_rows)
            returned = sum(row["return_yen"] for row in ticket_rows)
            item["fixed_ticket_purchase_yen"] = invested
            item["fixed_ticket_return_yen"] = returned
            item["fixed_ticket_roi"] = returned / invested if invested else None
            item["fixed_ticket_roi_sample_sufficient"] = len(ticket_rows) >= minimum_races
            stratified.append(item)
    features = []
    correlations = joined[metadata["features"]].corr()
    condition_groups = list(joined.groupby(["場所", "芝ダ", "距離帯", "評価区分"], dropna=False, observed=True))
    baselines = [rank1_metrics(group, payouts) for _, group in condition_groups]
    for feature in metadata["features"]:
        values = pd.to_numeric(joined[feature], errors="coerce")
        effects = []
        for group_index, (condition, group) in enumerate(condition_groups):
            weights = metadata["weights"]["surface"].get(f"{condition[0]}|{condition[1]}", metadata["weights"]["common"].get(str(condition[0]), metadata["weights"]["common"].get("__default__", {})))
            effect = {"conditions": list(map(str, condition)), "races": int(group["rid_str"].nunique())}
            effect["sample_sufficient"] = effect["races"] >= minimum_races
            if "total" in group and metadata["scoring_model_version"] == "legacy":
                # 加法スコアから当該寄与だけを外す診断。買い目条件の採用判定には流用しない。
                altered = group.copy()
                altered["without_total"] = pd.to_numeric(altered["total"], errors="coerce") - pd.to_numeric(altered[feature], errors="coerce").fillna(0) * float(weights.get(feature, 0))
                altered = altered.sort_values(["without_total", "pred_rank", "馬番"], ascending=[False, True, True], kind="stable")
                altered["without_rank"] = altered.groupby("rid_str").cumcount() + 1
                baseline, removed = baselines[group_index], rank1_metrics(altered, payouts, "without_rank")
                effect.update({"rank1_place_delta": (removed["rank1_place_rate"] or 0) - (baseline["rank1_place_rate"] or 0),
                               "win_roi_delta": (removed["win_roi"] or 0) - (baseline["win_roi"] or 0),
                               "place_roi_delta": (removed["place_roi"] or 0) - (baseline["place_roi"] or 0)})
            effects.append(effect)
        predictive = values.rank().corr(-joined["finish"].rank()) if values.nunique() > 1 else None
        features.append({"feature": feature, "missing_rate": float(values.isna().mean()),
                         "pooled_spearman_with_negative_finish": number(predictive),
                         "mean": number(values.mean()), "std": number(values.std()),
                         "min": number(values.min()), "max": number(values.max()), "ablation": effects})
    high_correlations = [{"left": a, "right": b, "correlation": float(correlations.loc[a,b])}
                         for i,a in enumerate(correlations) for b in correlations.columns[i+1:]
                         if pd.notna(correlations.loc[a,b]) and abs(correlations.loc[a,b]) >= 0.9]
    candidates = [{"type": "missing_feature", "feature": f["feature"], "missing_rate": f["missing_rate"], "action": "原因確認後に修正PR。productionへ自動反映しない"}
                  for f in features if f["missing_rate"] >= 0.5]
    candidates += [{"type": "correlated_features", **x, "action": "Shadow候補として個別除外を比較"} for x in high_correlations]
    output = pre.parent / "post" / metadata["runtime_id"][:16] / sha256(results)[:16]
    output.mkdir(parents=True, exist_ok=True)
    # 別名Excelに実着順・状態だけを追記し、予想や特徴量のセルは一切上書きしない。
    from openpyxl import load_workbook
    workbook = load_workbook(model_folder / "prediction.xlsx")
    sheet = workbook["TARGET"]
    headers = {str(c.value): c.column for c in sheet[1]}
    first_new = sheet.max_column + 1
    sheet.cell(1, first_new, "確定着順")
    sheet.cell(1, first_new + 1, "確定状態")
    lookup = joined.set_index(["rid_str", "馬番"])
    for row in range(2, sheet.max_row + 1):
        rid = key(sheet.cell(row, headers["rid_str"]).value)
        horse = int(sheet.cell(row, headers["馬番"]).value)
        actual = lookup.loc[(rid, horse)]
        sheet.cell(row, first_new, number(actual["finish"]))
        sheet.cell(row, first_new + 1, actual["result_status"])
    raw_results = pd.read_excel(results, sheet_name=manifest["day"])
    from openpyxl.utils.dataframe import dataframe_to_rows
    actual_sheet = workbook.create_sheet("確定結果_原本")
    for row in dataframe_to_rows(raw_results.astype(object).where(raw_results.notna(), None), index=False, header=True):
        actual_sheet.append(row)
    workbook.save(output / f"馬の競走成績_with_feat_{manifest['day']}_with_result.xlsx")
    joined.to_csv(output / "race_horse_results.csv", index=False, encoding="utf-8-sig")
    excluded = sorted(set(expected["rid_str"]) - set(pred["rid_str"]))
    report = {"schema_version": 1, "day": manifest["day"], "mode": manifest["mode"],
              "runtime_id": metadata["runtime_id"], "model_role": "shadow" if shadow else "production",
              "pre_manifest_sha256": sha256(pre / "manifest.json"),
              "results_sha256": sha256(results), "legacy_forward_generation": None,
              "auto_promote": False, "overall": overall, "stratified": stratified,
              "features": features, "high_correlations": high_correlations, "candidates": candidates,
              "all_races": int(expected["rid_str"].nunique()), "excluded_races": excluded,
              "unrelated_result_races_quarantined": unrelated_races,
              "excluded_results": horses.loc[horses["rid_str"].isin(excluded)].fillna("").astype(str).to_dict("records"),
              "existing_ticket_roi": ticket_result,
              "rank_tiebreak_audit": pred.loc[pred["rank_tiebreak_applied"],
                  ["rid_str", "馬番", "original_pred_rank", "pred_rank", "total"]].to_dict("records"),
              "limitations": ["既存買い目は固定済み回収率重視候補の3連複・馬連・ワイドを100円単位で精算。実購入の証明ではない", "個別除外は加法スコア診断。買い目再生成・完全同点の本番順位再現は別検証", "過去日再計算はretrospective専用。既存世代の前向き実績に算入しない"]}
    write_json(output / "review.json", report)
    (output / "回顧.md").write_text(f"# {manifest['day']} 全レース回顧\n\n区分: {manifest['mode']}\n\n全{report['all_races']}レース、予想対象{overall['races']}、対象外{len(excluded)}。\n\n1位勝率 {overall['rank1_win_rate']}、複勝率 {overall['rank1_place_rate']}、単勝回収率 {overall['win_roi']}、複勝回収率 {overall['place_roi']}。\n\n改善候補 {len(candidates)}件。条件別・全特徴量の結果はreview.json。production採用はユーザー判断。\n", encoding="utf-8")
    verify_archive(pre)
    return output
