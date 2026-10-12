# -*- coding: utf-8 -*-
"""Non-maiden conditional re-ranking. Keeps the production ranks intact.

Output columns are comparison-only until a separate out-of-sample adoption decision.
"""
from __future__ import annotations

from pathlib import Path
import re
import pandas as pd
from openpyxl import load_workbook

COEFFICIENT = 0.30
MAX_SCORE_GAP = 4.0
MIN_FORM_ADVANTAGE = 10.0
SHEET_NAME = "条件付き再ランキング"


def _rid(value: object) -> str:
    # Excel由来の整数IDがfloat化した場合、末尾の小数部をIDへ混入させない。
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    s = re.sub(r"\D", "", text)
    return s.zfill(12) if s else ""


def _key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "rid_str" not in out:
        if "レースID" not in out:
            raise ValueError("rid_str/レースID がありません")
        out["rid_str"] = out["レースID"]
    out["rid_str"] = out["rid_str"].map(_rid)
    out["馬番"] = pd.to_numeric(out["馬番"], errors="coerce").astype("Int64")
    return out


def build_conditional_ranking(target: pd.DataFrame, now: pd.DataFrame):
    """Return horse-level ranks and race audit. Never uses actual results."""
    t, n = _key(target), _key(now)
    score_col = "score_raw" if "score_raw" in t.columns else "score"
    form_col = next((name for name in ("recent_form_score", "近走状態スコア") if name in t.columns), None)
    required = [score_col, "rank", "馬番", "rid_str"]
    if form_col is None:
        raise ValueError("近走状態スコア（recent_form_score）がTARGETにありません")
    missing = [c for c in required if c not in t.columns]
    if missing:
        raise ValueError(f"再ランキングに必要な列がありません: {missing}")
    class_cols = [c for c in ("クラス", "レース種別", "レース名") if c in n]
    if not class_cols:
        raise ValueError("クラス判定列がありません。誤適用を防ぐため停止します")
    cls = (n.groupby("rid_str", dropna=False)[class_cols]
           .first().fillna("").astype(str))
    cls["判定文字列"] = cls.agg(" ".join, axis=1)
    t["rank"] = pd.to_numeric(t["rank"], errors="coerce")
    t[score_col] = pd.to_numeric(t[score_col], errors="coerce")
    t[form_col] = pd.to_numeric(t[form_col], errors="coerce")
    t["条件付き順位"] = t["rank"].astype("Int64")
    t["条件付き順位変更"] = False
    t["条件付き変更理由"] = "変更なし"
    audits = []
    for rid, group in t.groupby("rid_str", sort=True):
        info = cls["判定文字列"].get(rid, "").strip()
        original = group.sort_values(["rank", "馬番"], kind="mergesort")
        if original.empty:
            continue
        leader = original.iloc[0]
        reason = "変更なし"
        selected = leader
        if not info:
            reason = "クラス情報不足: 変更なし"
        elif "未勝利" in info or "新馬" in info:
            reason = "未勝利・新馬: 対象外"
        elif pd.isna(leader[score_col]) or pd.isna(leader[form_col]):
            reason = "AI1位のスコア欠損: 変更なし"
        else:
            top5 = original.loc[original["rank"].between(2, 5)].copy()
            top5["_gap"] = leader[score_col] - top5[score_col]
            top5["_form_diff"] = top5[form_col] - leader[form_col]
            top5["_corrected"] = top5[score_col] + COEFFICIENT * (top5[form_col] - 50.0)
            leader_corrected = leader[score_col] + COEFFICIENT * (leader[form_col] - 50.0)
            valid = top5.loc[
                top5["_gap"].between(0, MAX_SCORE_GAP)
                & (top5["_form_diff"] >= MIN_FORM_ADVANTAGE)
                & (top5["_corrected"] > leader_corrected)
            ]
            if not valid.empty:
                selected = valid.sort_values(
                    ["_corrected", "rank", "馬番"], ascending=[False, True, True],
                    kind="mergesort"
                ).iloc[0]
                chosen_rank = int(selected["rank"])
                old_ranks = pd.to_numeric(t.loc[group.index, "rank"], errors="coerce")
                t.loc[group.index, "条件付き順位"] = old_ranks.map(
                    lambda x: 1 if pd.notna(x) and int(x) == chosen_rank
                    else (int(x) + 1 if pd.notna(x) and int(x) < chosen_rank else x)
                ).astype("Int64").values
                t.loc[group.index, "条件付き順位変更"] = True
                reason = (f"総合差={float(selected['_gap']):.2f}≤{MAX_SCORE_GAP};"
                          f"近走差=+{float(selected['_form_diff']):.2f}≥{MIN_FORM_ADVANTAGE}")
        t.loc[group.index, "条件付き変更理由"] = reason
        audits.append({
            "レースID": rid, "クラス情報": info,
            "現行1位馬番": leader["馬番"], "現行1位馬名": leader.get("馬名", ""),
            "条件付き1位馬番": selected["馬番"], "条件付き1位馬名": selected.get("馬名", ""),
            "変更": int(leader["馬番"] != selected["馬番"]), "理由": reason,
            "補正係数": COEFFICIENT, "最大総合差": MAX_SCORE_GAP,
            "最低近走差": MIN_FORM_ADVANTAGE,
        })
        ranks = pd.to_numeric(t.loc[group.index, "条件付き順位"], errors="raise")
        assert sorted(ranks.astype(int).tolist()) == list(range(1, len(group) + 1))
        assert set(t.loc[group.index].nsmallest(min(5, len(group)), "条件付き順位")["馬番"]) == set(original.head(5)["馬番"])
    return t[["rid_str", "馬番", "条件付き順位", "条件付き順位変更", "条件付き変更理由"]], pd.DataFrame(audits)


def append_conditional_ranking_sheet(excel_path: str) -> int:
    """Append comparison columns without rewriting existing workbook cells."""
    path = Path(excel_path)
    target = pd.read_excel(path, sheet_name="TARGET", engine="openpyxl")
    now = pd.read_excel(path, sheet_name="今走レース情報", engine="openpyxl")
    rank, audit = build_conditional_ranking(target, now)
    rank_lookup = {
        (str(row.rid_str), int(row.馬番)): (
            int(row.条件付き順位),
            bool(row.条件付き順位変更),
            str(row.条件付き変更理由),
        )
        for row in rank.itertuples(index=False)
    }

    wb = load_workbook(path)
    comparison_headers = ("条件付き順位", "条件付き順位変更", "条件付き変更理由")
    for sheet_name in ("TARGET", "今走レース情報"):
        ws = wb[sheet_name]
        headers = {cell.value: cell.column for cell in ws[1] if isinstance(cell.value, str)}
        if "馬番" not in headers:
            raise ValueError(f"{sheet_name}: 馬番列がありません")
        rid_column = headers.get("rid_str", headers.get("レースID"))
        if rid_column is None:
            raise ValueError(f"{sheet_name}: レースID列がありません")
        number_column = headers["馬番"]
        write_columns = {}
        for header in comparison_headers:
            if header in headers:
                write_columns[header] = headers[header]
            else:
                col = ws.max_column + 1
                ws.cell(1, col, header)
                write_columns[header] = col
        unmatched = []
        for row_idx in range(2, ws.max_row + 1):
            rid = _rid(ws.cell(row_idx, rid_column).value)
            horse = ws.cell(row_idx, number_column).value
            if not rid or horse is None:
                continue
            try:
                key = (rid, int(horse))
            except (TypeError, ValueError):
                unmatched.append(row_idx)
                continue
            values = rank_lookup.get(key)
            if values is None:
                unmatched.append(row_idx)
                continue
            for header, value in zip(comparison_headers, values):
                ws.cell(row_idx, write_columns[header], value)
        if unmatched:
            raise ValueError(
                f"{sheet_name}: 条件付き順位を照合できない行が{len(unmatched)}件あります。"
                f"最初の10件={unmatched[:10]}。保存を中止します"
            )

    if SHEET_NAME in wb:
        del wb[SHEET_NAME]
    audit_ws = wb.create_sheet(SHEET_NAME)
    audit_ws.append(list(audit.columns))
    for row in audit.itertuples(index=False, name=None):
        audit_ws.append([
            None if pd.isna(value) else (value.item() if hasattr(value, "item") else value)
            for value in row
        ])
    wb.save(path)
    count = int(audit["変更"].sum()) if not audit.empty else 0
    print(f"[INFO] 条件付き再ランキング: {count}/{len(audit)}レースで1位変更（既存セル保持）")
    return count
