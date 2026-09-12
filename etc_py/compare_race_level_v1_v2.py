# -*- coding: utf-8 -*-
"""
00_Export_To_Excel_4_v2.py が生成した race_levels.xlsx を使い、
v1 と v2 の「レース前rating」の予測力を時系列リークなしで比較する。

評価単位は各レース。各レース直前の pre_rating / pre_rating_v2 のみで順位付けし、
実着順・単勝オッズを使って以下を検証する。

- rating 1位馬の勝率 / 3着内率
- rating 上位3頭に勝ち馬が含まれる率
- rating 上位3頭が実際の3着以内を何頭拾ったか
- rating 上位3頭が実際の1〜3着を完全に含む率
- レース内 rating と実着順の Spearman 相関
- rating 1位を単勝100円購入した場合の回収率
- rating 1位馬の人気帯別成績
- 年別成績
- v1/v2で1位評価馬が異なるレースの直接対決

注意:
複勝払戻額は現行出力に無いため、複勝回収率は計算しない。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

STAKE_YEN = 100


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def popularity_bucket(pop) -> str:
    try:
        p = int(float(pop))
    except Exception:
        return "不明"
    if p == 1:
        return "1人気"
    if p <= 3:
        return "2-3人気"
    if p <= 5:
        return "4-5人気"
    if p <= 9:
        return "6-9人気"
    return "10人気以下"


def load_joined_entries(xlsx_path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    required = {"entries", "entries_v2", "races"}
    missing = required - set(xls.sheet_names)
    if missing:
        raise ValueError(
            f"必要シートがありません: {sorted(missing)}。"
            "先に 00_Export_To_Excel_4_v2.py を実行してください。"
        )

    v1 = pd.read_excel(xls, sheet_name="entries")
    v2 = pd.read_excel(xls, sheet_name="entries_v2")
    races = pd.read_excel(xls, sheet_name="races")

    for df in (v1, v2, races):
        if "race_id" in df.columns:
            df["race_id"] = df["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    for df in (v1, v2):
        if "horse_id" in df.columns:
            df["horse_id"] = to_num(df["horse_id"]).astype("Int64")

    req_v1 = {"race_id", "horse_id", "rank", "odds", "pop", "pre_rating"}
    req_v2 = {"race_id", "horse_id", "pre_rating_v2"}
    miss1 = req_v1 - set(v1.columns)
    miss2 = req_v2 - set(v2.columns)
    if miss1:
        raise ValueError(f"entries の必須列不足: {sorted(miss1)}")
    if miss2:
        raise ValueError(f"entries_v2 の必須列不足: {sorted(miss2)}")

    keep_v1 = [
        "race_id", "horse_id", "rank", "odds", "pop", "pre_rating",
        "number", "weight", "gap_from_winner_sec",
    ]
    keep_v1 = [c for c in keep_v1 if c in v1.columns]
    keep_v2 = [
        "race_id", "horse_id", "pre_rating_v2", "pre_overall_rating_v2",
        "pre_surface_rating_v2", "pre_distance_rating_v2", "distance_band",
    ]
    keep_v2 = [c for c in keep_v2 if c in v2.columns]

    merged = v1[keep_v1].merge(
        v2[keep_v2], on=["race_id", "horse_id"], how="inner", validate="one_to_one"
    )

    race_cols = [
        "race_id", "date", "start_time", "place", "class", "ground",
        "distance", "distance_band", "baba", "race_name",
    ]
    race_cols = [c for c in race_cols if c in races.columns]
    race_meta = races[race_cols].drop_duplicates("race_id")
    if "distance_band" in merged.columns and "distance_band" in race_meta.columns:
        race_meta = race_meta.rename(columns={"distance_band": "race_distance_band"})
    merged = merged.merge(race_meta, on="race_id", how="left", validate="many_to_one")

    merged["rank"] = to_num(merged["rank"])
    merged["odds"] = to_num(merged["odds"])
    merged["pop"] = to_num(merged["pop"])
    merged["pre_rating"] = to_num(merged["pre_rating"])
    merged["pre_rating_v2"] = to_num(merged["pre_rating_v2"])

    merged = merged[
        merged["rank"].notna()
        & (merged["rank"] > 0)
        & merged["pre_rating"].notna()
        & merged["pre_rating_v2"].notna()
    ].copy()

    if "date" in merged.columns:
        merged["date"] = merged["date"].astype(str).str.replace(r"\.0$", "", regex=True)
        merged["year"] = pd.to_numeric(merged["date"].str[:4], errors="coerce").astype("Int64")
    else:
        merged["year"] = pd.Series([pd.NA] * len(merged), dtype="Int64")

    return merged


def evaluate_one_model(
    entries: pd.DataFrame,
    rating_col: str,
    model_name: str,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    rows: List[Dict] = []

    for race_id, g0 in entries.groupby("race_id", sort=False):
        g = g0.copy()
        if g.empty:
            continue

        g = g.sort_values([rating_col, "horse_id"], ascending=[False, True], kind="mergesort")
        top1 = g.iloc[0]
        top3 = g.head(3)

        actual_top3 = set(g.loc[g["rank"] <= 3, "horse_id"].dropna().astype(int).tolist())
        pred_top3 = set(top3["horse_id"].dropna().astype(int).tolist())
        top3_hit_count = len(pred_top3 & actual_top3)
        winner_ids = set(g.loc[g["rank"] == 1, "horse_id"].dropna().astype(int).tolist())

        if g[rating_col].nunique(dropna=True) >= 2 and g["rank"].nunique(dropna=True) >= 2:
            spearman = g[rating_col].corr(-g["rank"], method="spearman")
        else:
            spearman = float("nan")

        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else float("nan")
        top1_win = int(float(top1["rank"]) == 1.0)
        top1_place = int(float(top1["rank"]) <= 3.0)
        win_return = STAKE_YEN * odds if top1_win and pd.notna(odds) and odds > 0 else 0.0

        row = {
            "model": model_name,
            "race_id": str(race_id),
            "field_size": int(len(g)),
            "top1_horse_id": int(top1["horse_id"]),
            "top1_rating": float(top1[rating_col]),
            "top1_rank": int(top1["rank"]),
            "top1_pop": int(top1["pop"]) if pd.notna(top1["pop"]) else pd.NA,
            "top1_odds": odds,
            "top1_win": top1_win,
            "top1_place": top1_place,
            "top3_contains_winner": int(bool(pred_top3 & winner_ids)),
            "top3_actual_top3_hit_count": top3_hit_count,
            "top3_complete": int(bool(actual_top3) and actual_top3.issubset(pred_top3)),
            "spearman_rating_vs_finish": spearman,
            "win_stake_yen": STAKE_YEN,
            "win_return_yen": win_return,
            "win_profit_yen": win_return - STAKE_YEN,
            "pop_bucket": popularity_bucket(top1["pop"]),
        }
        for c in ["date", "year", "start_time", "place", "class", "ground", "distance", "baba", "race_name"]:
            if c in g.columns:
                row[c] = g[c].iloc[0]
        rows.append(row)

    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, {
            "model": model_name,
            "race_count": 0,
            "top1_win_rate": float("nan"),
            "top1_place_rate": float("nan"),
            "top3_contains_winner_rate": float("nan"),
            "avg_top3_actual_top3_hit_count": float("nan"),
            "top3_complete_rate": float("nan"),
            "mean_spearman": float("nan"),
            "win_bet_count": 0,
            "win_stake_yen": 0,
            "win_return_yen": 0.0,
            "win_profit_yen": 0.0,
            "win_roi_pct": float("nan"),
        }

    stake = float(detail["win_stake_yen"].sum())
    ret = float(detail["win_return_yen"].sum())
    summary = {
        "model": model_name,
        "race_count": int(len(detail)),
        "top1_win_rate": float(detail["top1_win"].mean()),
        "top1_place_rate": float(detail["top1_place"].mean()),
        "top3_contains_winner_rate": float(detail["top3_contains_winner"].mean()),
        "avg_top3_actual_top3_hit_count": float(detail["top3_actual_top3_hit_count"].mean()),
        "top3_complete_rate": float(detail["top3_complete"].mean()),
        "mean_spearman": float(detail["spearman_rating_vs_finish"].mean()),
        "win_bet_count": int(len(detail)),
        "win_stake_yen": int(stake),
        "win_return_yen": float(ret),
        "win_profit_yen": float(ret - stake),
        "win_roi_pct": float((ret / stake) * 100.0) if stake > 0 else float("nan"),
    }
    return detail, summary


def grouped_metrics(detail: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if detail.empty or group_col not in detail.columns:
        return pd.DataFrame()
    rows = []
    for (model, group_value), g in detail.groupby(["model", group_col], dropna=False, sort=False):
        stake = float(g["win_stake_yen"].sum())
        ret = float(g["win_return_yen"].sum())
        rows.append({
            "model": model,
            group_col: group_value,
            "race_count": int(len(g)),
            "top1_win_rate": float(g["top1_win"].mean()),
            "top1_place_rate": float(g["top1_place"].mean()),
            "top3_contains_winner_rate": float(g["top3_contains_winner"].mean()),
            "avg_top3_actual_top3_hit_count": float(g["top3_actual_top3_hit_count"].mean()),
            "mean_spearman": float(g["spearman_rating_vs_finish"].mean()),
            "win_stake_yen": int(stake),
            "win_return_yen": float(ret),
            "win_profit_yen": float(ret - stake),
            "win_roi_pct": float((ret / stake) * 100.0) if stake > 0 else float("nan"),
        })
    return pd.DataFrame(rows)


def build_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty or set(summary_df["model"]) != {"v1", "v2"}:
        return pd.DataFrame()
    s = summary_df.set_index("model")
    metrics = [
        "top1_win_rate",
        "top1_place_rate",
        "top3_contains_winner_rate",
        "avg_top3_actual_top3_hit_count",
        "top3_complete_rate",
        "mean_spearman",
        "win_roi_pct",
        "win_profit_yen",
    ]
    rows = []
    for m in metrics:
        v1 = float(s.loc["v1", m])
        v2 = float(s.loc["v2", m])
        rows.append({
            "metric": m,
            "v1": v1,
            "v2": v2,
            "v2_minus_v1": v2 - v1,
            "better": "v2" if v2 > v1 else ("v1" if v1 > v2 else "same"),
        })
    return pd.DataFrame(rows)


def build_head_to_head(v1: pd.DataFrame, v2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = [
        "race_id", "top1_horse_id", "top1_rating", "top1_rank", "top1_pop", "top1_odds",
        "top1_win", "top1_place", "win_profit_yen",
    ]
    h = v1[cols].merge(v2[cols], on="race_id", suffixes=("_v1", "_v2"), how="inner")
    h["same_top1"] = h["top1_horse_id_v1"] == h["top1_horse_id_v2"]
    h["v2_finish_improvement"] = h["top1_rank_v1"] - h["top1_rank_v2"]
    h["v2_profit_improvement_yen"] = h["win_profit_yen_v2"] - h["win_profit_yen_v1"]

    diff = h[~h["same_top1"]].copy()
    if diff.empty:
        summary = pd.DataFrame([{
            "different_top1_races": 0,
            "v1_only_wins": 0,
            "v2_only_wins": 0,
            "v1_only_places": 0,
            "v2_only_places": 0,
            "avg_v2_finish_improvement": float("nan"),
            "total_v2_profit_improvement_yen": 0.0,
        }])
    else:
        summary = pd.DataFrame([{
            "different_top1_races": int(len(diff)),
            "v1_only_wins": int(((diff["top1_win_v1"] == 1) & (diff["top1_win_v2"] == 0)).sum()),
            "v2_only_wins": int(((diff["top1_win_v2"] == 1) & (diff["top1_win_v1"] == 0)).sum()),
            "v1_only_places": int(((diff["top1_place_v1"] == 1) & (diff["top1_place_v2"] == 0)).sum()),
            "v2_only_places": int(((diff["top1_place_v2"] == 1) & (diff["top1_place_v1"] == 0)).sum()),
            "avg_v2_finish_improvement": float(diff["v2_finish_improvement"].mean()),
            "total_v2_profit_improvement_yen": float(diff["v2_profit_improvement_yen"].sum()),
        }])
    return h, summary


def auto_width_excel(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    ws = writer.book[sheet_name]
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for idx, col in enumerate(df.columns, start=1):
        sample = [str(col)] + [str(v) for v in df[col].head(300).fillna("").tolist()]
        width = min(max(len(x) for x in sample) + 2, 45)
        ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = width


def compare(input_xlsx: Path, output_xlsx: Path) -> None:
    entries = load_joined_entries(input_xlsx)
    if entries.empty:
        raise ValueError("比較可能な出走データが0件です。")

    v1_detail, v1_summary = evaluate_one_model(entries, "pre_rating", "v1")
    v2_detail, v2_summary = evaluate_one_model(entries, "pre_rating_v2", "v2")

    detail = pd.concat([v1_detail, v2_detail], ignore_index=True)
    summary = pd.DataFrame([v1_summary, v2_summary])
    comparison = build_comparison(summary)
    popularity = grouped_metrics(detail, "pop_bucket")
    yearly = grouped_metrics(detail, "year")
    head_to_head, head_summary = build_head_to_head(v1_detail, v2_detail)

    parameters = pd.DataFrame([
        {"item": "入力", "value": str(input_xlsx)},
        {"item": "評価rating v1", "value": "entries.pre_rating（各レース直前）"},
        {"item": "評価rating v2", "value": "entries_v2.pre_rating_v2（各レース直前）"},
        {"item": "単勝購入", "value": "各モデルrating 1位を各レース100円"},
        {"item": "払戻計算", "value": "1着時のみ 単勝オッズ×100円"},
        {"item": "複勝ROI", "value": "複勝払戻データが無いため未計算"},
        {"item": "Spearman", "value": "rating と -実着順のレース内順位相関。高いほど良い"},
        {"item": "未来情報", "value": "rating順位決定にはレース後情報を使用しない"},
    ])

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        comparison.to_excel(writer, sheet_name="v1_vs_v2", index=False)
        head_summary.to_excel(writer, sheet_name="head_to_head_summary", index=False)
        head_to_head.to_excel(writer, sheet_name="head_to_head_detail", index=False)
        popularity.to_excel(writer, sheet_name="by_popularity", index=False)
        yearly.to_excel(writer, sheet_name="by_year", index=False)
        detail.to_excel(writer, sheet_name="race_detail", index=False)
        parameters.to_excel(writer, sheet_name="README", index=False)

        for sheet, df in [
            ("summary", summary),
            ("v1_vs_v2", comparison),
            ("head_to_head_summary", head_summary),
            ("head_to_head_detail", head_to_head),
            ("by_popularity", popularity),
            ("by_year", yearly),
            ("race_detail", detail),
            ("README", parameters),
        ]:
            auto_width_excel(writer, sheet, df)

    print(f"[done] v1/v2比較完了: {output_xlsx}")
    print("\n=== summary ===")
    print(summary.to_string(index=False))
    print("\n=== v1_vs_v2 ===")
    print(comparison.to_string(index=False))
    print("\n=== head_to_head ===")
    print(head_summary.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="race level / horse rating v1 vs v2 比較")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/master/race_levels.xlsx"),
        help="00_Export_To_Excel_4_v2.py が生成したExcel",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="比較結果Excel。未指定なら入力ファイルと同じフォルダへ *_v1_v2_compare.xlsx",
    )
    args = parser.parse_args()

    input_xlsx = args.input.resolve()
    if not input_xlsx.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {input_xlsx}")

    output_xlsx = args.out.resolve() if args.out else input_xlsx.with_name(
        f"{input_xlsx.stem}_v1_v2_compare.xlsx"
    )
    compare(input_xlsx, output_xlsx)


if __name__ == "__main__":
    main()
