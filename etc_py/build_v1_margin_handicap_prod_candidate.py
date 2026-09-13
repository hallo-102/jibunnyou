# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

BETA_HANDICAP = 4.0


def load_context_module():
    path = Path(__file__).with_name("evaluate_v1_margin_weight_context.py")
    spec = importlib.util.spec_from_file_location("weight_context_prod", path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def norm_rid(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def main() -> None:
    p = argparse.ArgumentParser(description="build V1_MARGIN + HANDICAP_REL production candidate")
    p.add_argument("--input", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--raw", type=Path, default=Path("data/master/racedata_results_clean_v3.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/race_levels_v1_margin_handicap_prod_candidate.xlsx"))
    args = p.parse_args()

    src = args.input.resolve()
    raw_path = args.raw.resolve()
    out = args.out.resolve()
    if not src.exists():
        raise FileNotFoundError(src)
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)
    if src == out:
        raise ValueError("--out must differ from --input")

    xls = pd.ExcelFile(src, engine="openpyxl")
    sheets = {name: pd.read_excel(xls, sheet_name=name) for name in xls.sheet_names}
    xls.close()
    if "entries" not in sheets or "horses" not in sheets:
        raise ValueError("entries/horses sheet is required")

    entries = sheets["entries"].copy()
    need = {"race_id", "horse_id", "pre_rating", "weight"}
    miss = need - set(entries.columns)
    if miss:
        raise ValueError(f"entries columns missing: {sorted(miss)}")

    entries["_row_order"] = range(len(entries))
    entries["_rid"] = entries["race_id"].map(norm_rid)
    entries["_hid"] = pd.to_numeric(entries["horse_id"], errors="coerce").astype("Int64")
    entries["_pre"] = pd.to_numeric(entries["pre_rating"], errors="coerce")
    entries["_weight"] = pd.to_numeric(entries["weight"], errors="coerce")
    if entries.duplicated(["_rid", "_hid"]).any():
        raise ValueError("duplicate race_id+horse_id in entries")

    horses = sheets["horses"].copy()
    if "id" not in horses.columns or "name" not in horses.columns:
        raise ValueError("horses id/name columns are required")
    horses["_hid"] = pd.to_numeric(horses["id"], errors="coerce").astype("Int64")
    horses["_horse_name"] = horses["name"].astype(str).str.strip()
    horse_map = horses[["_hid", "_horse_name"]].drop_duplicates("_hid")
    work = entries.merge(horse_map, on="_hid", how="left", validate="many_to_one")

    ctxmod = load_context_module()
    raw = ctxmod.load_raw_context(raw_path)
    raw = raw[["race_id", "horse_name", "weight_type"]].copy()
    raw["_rid"] = raw["race_id"].map(norm_rid)
    raw["_horse_name"] = raw["horse_name"].astype(str).str.strip()
    raw = raw[["_rid", "_horse_name", "weight_type"]].drop_duplicates(["_rid", "_horse_name"])
    work = work.merge(raw, on=["_rid", "_horse_name"], how="left", validate="many_to_one")
    work["weight_type"] = work["weight_type"].fillna("OTHER")

    mean_weight = work.groupby("_rid")["_weight"].transform("mean")
    rel = (work["_weight"] - mean_weight).fillna(0.0)
    work["prod_handicap_rel"] = rel.where(work["weight_type"] == "HANDICAP", 0.0)
    work["pre_rating_prod_candidate"] = work["_pre"] + BETA_HANDICAP * work["prod_handicap_rel"]
    work["prod_candidate_rank"] = pd.Series(pd.NA, index=work.index, dtype="Int64")

    valid = work["pre_rating_prod_candidate"].notna()
    for _, group in work[valid].groupby("_rid", sort=False):
        ordered = group.sort_values(["pre_rating_prod_candidate", "_hid"], ascending=[False, True], kind="mergesort")
        work.loc[ordered.index, "prod_candidate_rank"] = pd.Series(range(1, len(ordered) + 1), index=ordered.index, dtype="Int64")

    work = work.sort_values("_row_order", kind="mergesort")
    result = sheets["entries"].copy()
    result["prod_weight_type"] = work["weight_type"].to_numpy()
    result["prod_handicap_rel"] = work["prod_handicap_rel"].to_numpy()
    result["pre_rating_prod_candidate"] = work["pre_rating_prod_candidate"].to_numpy()
    result["prod_candidate_rank"] = work["prod_candidate_rank"].to_numpy()
    sheets["entries"] = result
    sheets["prod_candidate_config"] = pd.DataFrame([
        {"item": "model", "value": "V1_MARGIN_HANDICAP_PROD_CANDIDATE"},
        {"item": "handicap_beta", "value": BETA_HANDICAP},
        {"item": "formula", "value": "pre_rating + 4.0 * HANDICAP_REL"},
        {"item": "pre_rating_overwrite", "value": "NO"},
    ])

    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name[:31], index=False)

    handicap_entries = int((result["prod_weight_type"] == "HANDICAP").sum())
    print(f"[done] {out}")
    print(f"[model] V1_MARGIN_HANDICAP_PROD_CANDIDATE beta={BETA_HANDICAP:+.1f}")
    print(f"[entries] total={len(result)} handicap_entries={handicap_entries}")
    print("[safety] original pre_rating preserved")


if __name__ == "__main__":
    main()
