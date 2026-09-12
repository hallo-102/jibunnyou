# -*- coding: utf-8 -*-
"""
race_levels.xlsx の v2 出力を使い、後続成績から過去レースの強さを再評価する。

出力:
- race_confirmation_v2:
    現在までの全後続成績を使った回顧用 confirmed race level
- entries_confirmed_v2:
    各出走時点で前走レースの confirmed level を算出したリーク防止特徴量

重要:
entries_confirmed_v2 は current race date より前に判明した後続成績だけを使う。
したがって過去バックテストでも未来情報を混ぜない。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

NEXT_START_LIMIT = 2
FOLLOWUP_DAYS = 120
CONFIRM_ADJ_CAP = 20.0

CLASS_STRENGTH = {
    "未勝利": -30.0,
    "新馬": -10.0,
    "1勝クラス": 10.0,
    "2勝クラス": 35.0,
    "3勝クラス": 55.0,
    "ｵｰﾌﾟﾝ": 75.0,
    "Ｇ３": 100.0,
    "Ｇ２": 130.0,
    "Ｇ１": 160.0,
}


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def num(v) -> Optional[float]:
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def normalize_class(v) -> str:
    s = "" if v is None or pd.isna(v) else str(v).strip().replace(" ", "")
    s = (
        s.replace("G1", "Ｇ１").replace("GI", "Ｇ１")
        .replace("G2", "Ｇ２").replace("GII", "Ｇ２")
        .replace("G3", "Ｇ３").replace("GIII", "Ｇ３")
        .replace("オープン", "ｵｰﾌﾟﾝ").replace("OPEN", "ｵｰﾌﾟﾝ").replace("OP", "ｵｰﾌﾟﾝ")
    )
    return s


def class_strength(v) -> float:
    return CLASS_STRENGTH.get(normalize_class(v), 0.0)


def parse_date(v) -> Optional[pd.Timestamp]:
    if v is None or pd.isna(v):
        return None
    s = str(v).strip().replace(".0", "")
    try:
        return pd.to_datetime(s, format="%Y%m%d")
    except Exception:
        try:
            return pd.to_datetime(s)
        except Exception:
            return None


def rank_component(rank, field_size) -> Optional[float]:
    r = num(rank)
    f = num(field_size)
    if r is None or f is None or f <= 1 or r <= 0:
        return None
    # 1着=+1、最下位=-1 の線形スコア
    return clamp(1.0 - 2.0 * ((r - 1.0) / (f - 1.0)), -1.0, 1.0)


def load_data(path: Path):
    xls = pd.ExcelFile(path, engine="openpyxl")
    required = {"races", "entries_v2", "ratings_history_v2", "race_levels_v2"}
    missing = required - set(xls.sheet_names)
    if missing:
        raise ValueError(f"必要シートがありません: {sorted(missing)}。先に 00_Export_To_Excel_4_v2.py を実行してください。")

    races = pd.read_excel(xls, sheet_name="races")
    entries = pd.read_excel(xls, sheet_name="entries_v2")
    hist = pd.read_excel(xls, sheet_name="ratings_history_v2")
    levels = pd.read_excel(xls, sheet_name="race_levels_v2")
    xls.close()

    for df in (races, entries, hist, levels):
        if "race_id" in df.columns:
            df["race_id"] = df["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    for df in (entries, hist):
        if "horse_id" in df.columns:
            df["horse_id"] = pd.to_numeric(df["horse_id"], errors="coerce").astype("Int64")

    races = races.copy()
    races["race_date_ts"] = races["date"].map(parse_date)
    race_meta = races.set_index("race_id").to_dict("index")

    if "field_size" not in levels.columns:
        field_counts = entries.groupby("race_id")["horse_id"].count().rename("field_size")
        levels = levels.merge(field_counts, on="race_id", how="left")

    # history に着順・頭数・未来レースクラスを付与
    ecols = [c for c in ["race_id", "horse_id", "rank"] if c in entries.columns]
    h = hist.merge(entries[ecols], on=["race_id", "horse_id"], how="left", suffixes=("", "_entry"))
    if "rank_entry" in h.columns and "rank" not in h.columns:
        h["rank"] = h["rank_entry"]
    h["date_ts"] = h["race_id"].map(lambda rid: race_meta.get(str(rid), {}).get("race_date_ts"))
    h["race_class"] = h["race_id"].map(lambda rid: race_meta.get(str(rid), {}).get("class"))
    field_map = entries.groupby("race_id")["horse_id"].count().to_dict()
    h["field_size"] = h["race_id"].map(field_map)
    h = h.sort_values(["horse_id", "date_ts", "race_id"]).reset_index(drop=True)

    return races, entries, h, levels, race_meta


def build_followup_index(hist: pd.DataFrame) -> Dict[int, List[Dict]]:
    out: Dict[int, List[Dict]] = {}
    for hid, g in hist.groupby("horse_id", dropna=True):
        rows = []
        for _, r in g.sort_values(["date_ts", "race_id"]).iterrows():
            rows.append(r.to_dict())
        out[int(hid)] = rows
    return out


def collect_followups(
    horse_id: int,
    source_race_id: str,
    source_date: pd.Timestamp,
    cutoff_date: Optional[pd.Timestamp],
    index: Dict[int, List[Dict]],
) -> List[Dict]:
    rows = []
    for r in index.get(int(horse_id), []):
        d = r.get("date_ts")
        if d is None or pd.isna(d) or d <= source_date:
            continue
        if str(r.get("race_id")) == str(source_race_id):
            continue
        if (d - source_date).days > FOLLOWUP_DAYS:
            break
        if cutoff_date is not None and d >= cutoff_date:
            continue
        rows.append(r)
        if len(rows) >= NEXT_START_LIMIT:
            break
    return rows


def followup_row_score(row: Dict, source_class) -> Tuple[float, Dict[str, float]]:
    delta = num(row.get("adjusted_delta_v2"))
    delta_comp = clamp((delta or 0.0) / 20.0, -1.0, 1.0)

    rank_comp = rank_component(row.get("rank"), row.get("field_size"))
    rank_comp = 0.0 if rank_comp is None else rank_comp

    class_move = class_strength(row.get("race_class")) - class_strength(source_class)
    class_comp = clamp(class_move / 80.0, -1.0, 1.0)

    # ratingの期待超過を主軸に、着順と格上挑戦も補助的に評価
    total = (0.55 * delta_comp) + (0.30 * rank_comp) + (0.15 * class_comp)
    return clamp(total, -1.0, 1.0), {
        "delta_component": delta_comp,
        "rank_component": rank_comp,
        "class_component": class_comp,
    }


def confirm_one_race(
    source_race_id: str,
    source_date: pd.Timestamp,
    source_class,
    source_final_level: Optional[float],
    starters: pd.DataFrame,
    cutoff_date: Optional[pd.Timestamp],
    followup_index: Dict[int, List[Dict]],
) -> Dict:
    scores: List[float] = []
    deltas: List[float] = []
    ranks: List[float] = []
    starter_with_followup = 0
    wins = 0
    top3s = 0
    total_followups = 0

    for _, s in starters.iterrows():
        if pd.isna(s.get("horse_id")):
            continue
        fups = collect_followups(
            int(s["horse_id"]), source_race_id, source_date,
            cutoff_date, followup_index,
        )
        if not fups:
            continue
        starter_with_followup += 1
        for f in fups:
            sc, _ = followup_row_score(f, source_class)
            scores.append(sc)
            d = num(f.get("adjusted_delta_v2"))
            if d is not None:
                deltas.append(d)
            r = num(f.get("rank"))
            if r is not None:
                ranks.append(r)
                if int(r) == 1:
                    wins += 1
                if int(r) <= 3:
                    top3s += 1
            total_followups += 1

    field_size = max(int(len(starters)), 1)
    coverage = starter_with_followup / field_size
    observation_strength = min(total_followups / max(field_size, 1), 1.0)
    confidence = clamp((0.65 * coverage) + (0.35 * observation_strength), 0.0, 1.0)
    raw_confirmation = float(sum(scores) / len(scores)) if scores else 0.0
    adjustment = clamp(raw_confirmation * CONFIRM_ADJ_CAP * confidence, -CONFIRM_ADJ_CAP, CONFIRM_ADJ_CAP)

    base_level = num(source_final_level)
    confirmed = None if base_level is None else base_level + adjustment

    return {
        "race_id": str(source_race_id),
        "source_date": source_date.strftime("%Y%m%d"),
        "source_class": source_class,
        "base_final_race_level_v2": base_level,
        "confirmation_raw_score": raw_confirmation,
        "confirmation_confidence": confidence,
        "confirmation_adjustment_v2": adjustment,
        "confirmed_race_level_v2": confirmed,
        "field_size": field_size,
        "starter_with_followup": starter_with_followup,
        "followup_coverage": coverage,
        "followup_starts": total_followups,
        "followup_adjusted_delta_mean": (sum(deltas) / len(deltas)) if deltas else None,
        "followup_win_rate": (wins / total_followups) if total_followups else None,
        "followup_top3_rate": (top3s / total_followups) if total_followups else None,
        "cutoff_date": None if cutoff_date is None else cutoff_date.strftime("%Y%m%d"),
    }


def build_hindsight(levels, entries, race_meta, followup_index):
    rows = []
    level_map = levels.set_index("race_id").to_dict("index")
    for rid, starters in entries.groupby("race_id"):
        meta = race_meta.get(str(rid), {})
        d = meta.get("race_date_ts")
        if d is None or pd.isna(d):
            continue
        lv = level_map.get(str(rid), {})
        base = lv.get("final_race_level_score_v2", lv.get("race_level_score_v2"))
        rows.append(confirm_one_race(
            str(rid), d, meta.get("class"), base, starters,
            cutoff_date=None, followup_index=followup_index,
        ))
    return pd.DataFrame(rows)


def build_leak_safe_entry_features(levels, entries, race_meta, followup_index):
    level_map = levels.set_index("race_id").to_dict("index")
    entries2 = entries.copy()
    entries2["race_date_ts"] = entries2["race_id"].map(lambda rid: race_meta.get(str(rid), {}).get("race_date_ts"))
    entries2 = entries2.sort_values(["horse_id", "race_date_ts", "race_id"]).reset_index(drop=True)

    prev_by_horse: Dict[int, Dict] = {}
    out = []
    for _, cur in entries2.sort_values(["race_date_ts", "race_id", "horse_id"]).iterrows():
        row = cur.to_dict()
        hid = None if pd.isna(cur.get("horse_id")) else int(cur["horse_id"])
        current_date = cur.get("race_date_ts")
        prev = prev_by_horse.get(hid) if hid is not None else None

        feat = {
            "prev_race_id": None,
            "prev_base_final_race_level_v2": None,
            "prev_confirmation_adjustment_v2": None,
            "prev_confirmed_race_level_v2": None,
            "prev_confirmation_confidence": None,
            "prev_followup_starts": 0,
            "prev_followup_coverage": 0.0,
            "prev_followup_adjusted_delta_mean": None,
            "prev_followup_win_rate": None,
            "prev_followup_top3_rate": None,
        }

        if prev is not None and current_date is not None and not pd.isna(current_date):
            prev_rid = str(prev["race_id"])
            prev_date = prev["race_date_ts"]
            prev_meta = race_meta.get(prev_rid, {})
            prev_starters = entries2[entries2["race_id"] == prev_rid]
            lv = level_map.get(prev_rid, {})
            base = lv.get("final_race_level_score_v2", lv.get("race_level_score_v2"))
            c = confirm_one_race(
                prev_rid,
                prev_date,
                prev_meta.get("class"),
                base,
                prev_starters,
                cutoff_date=current_date,
                followup_index=followup_index,
            )
            feat = {
                "prev_race_id": prev_rid,
                "prev_base_final_race_level_v2": c["base_final_race_level_v2"],
                "prev_confirmation_adjustment_v2": c["confirmation_adjustment_v2"],
                "prev_confirmed_race_level_v2": c["confirmed_race_level_v2"],
                "prev_confirmation_confidence": c["confirmation_confidence"],
                "prev_followup_starts": c["followup_starts"],
                "prev_followup_coverage": c["followup_coverage"],
                "prev_followup_adjusted_delta_mean": c["followup_adjusted_delta_mean"],
                "prev_followup_win_rate": c["followup_win_rate"],
                "prev_followup_top3_rate": c["followup_top3_rate"],
            }

        row.update(feat)
        row.pop("race_date_ts", None)
        out.append(row)

        if hid is not None:
            prev_by_horse[hid] = cur.to_dict()

    return pd.DataFrame(out)


def write_sheets(path: Path, hindsight: pd.DataFrame, leak_safe: pd.DataFrame):
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        hindsight.to_excel(writer, sheet_name="race_confirmation_v2", index=False)
        leak_safe.to_excel(writer, sheet_name="entries_confirmed_v2", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(Path(__file__).resolve().parents[1] / "data" / "master" / "race_levels.xlsx"))
    args = parser.parse_args()
    path = Path(args.input)
    if not path.exists():
        raise FileNotFoundError(path)

    races, entries, hist, levels, race_meta = load_data(path)
    followup_index = build_followup_index(hist)

    hindsight = build_hindsight(levels, entries, race_meta, followup_index)
    leak_safe = build_leak_safe_entry_features(levels, entries, race_meta, followup_index)
    write_sheets(path, hindsight, leak_safe)

    print(f"[done] race_confirmation_v2 rows={len(hindsight)}")
    print(f"[done] entries_confirmed_v2 rows={len(leak_safe)}")
    if not hindsight.empty:
        s = pd.to_numeric(hindsight["confirmation_adjustment_v2"], errors="coerce").dropna()
        if not s.empty:
            print(f"[verify] confirmation_adjustment_v2 min={s.min():.3f} median={s.median():.3f} max={s.max():.3f}")
        c = pd.to_numeric(hindsight["confirmation_confidence"], errors="coerce").dropna()
        if not c.empty:
            print(f"[verify] confirmation_confidence median={c.median():.3f} max={c.max():.3f}")


if __name__ == "__main__":
    main()
