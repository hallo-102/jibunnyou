from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .config import load_settings
from .contracts import canonicalize, load_race_table
from .features import build_features
from .odds import add_expected_value, analyze_odds
from .prediction import predict
from .race_selector import select_value_races
from .shadow import append_shadow_log, build_shadow_bets
from .strategies import build_strategy_bets, cap_bets
from .tracking import log_metrics, log_params, tracking_run
from .validation import validate_races


def run_pipeline(input_path: str | Path, race_date: str, settings_path: str | Path | None = None) -> dict:
    settings = load_settings(settings_path)
    df = canonicalize(load_race_table(input_path))
    validation = validate_races(df, settings.section("validation"))
    validation.raise_for_error()

    with tracking_run(settings.section("tracking"), settings.project_root, f"run_{race_date}"):
        featured = build_features(df)
        predicted = predict(featured, settings.section("prediction"), settings.project_root)
        enriched = add_expected_value(predicted, settings.section("odds"))
        odds_summary = analyze_odds(enriched, settings.section("odds"))
        race_selection = select_value_races(enriched, settings.section("race_selection"))

        legacy_shadow_bets = build_shadow_bets(enriched, settings.section("shadow"))
        strategy_bets = cap_bets(
            build_strategy_bets(enriched, settings.section("strategy")),
            settings.section("strategy"),
        )

        output_dir = settings.project_root / "data" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        prediction_path = output_dir / f"predictions_{race_date}.xlsx"
        odds_path = output_dir / f"odds_summary_{race_date}.csv"
        race_selection_path = output_dir / f"race_selection_{race_date}.csv"
        strategy_path = output_dir / f"strategy_bets_{race_date}.json"

        with pd.ExcelWriter(prediction_path, engine="openpyxl") as writer:
            enriched.to_excel(writer, index=False, sheet_name="predictions")
            odds_summary.to_excel(writer, index=False, sheet_name="odds_summary")
            race_selection.to_excel(writer, index=False, sheet_name="race_selection")
            pd.DataFrame([b.to_dict() for b in strategy_bets]).to_excel(writer, index=False, sheet_name="strategy_bets")

        odds_summary.to_csv(odds_path, index=False, encoding="utf-8-sig")
        race_selection.to_csv(race_selection_path, index=False, encoding="utf-8-sig")
        strategy_path.write_text(
            json.dumps([b.to_dict() for b in strategy_bets], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        shadow_cfg = settings.section("shadow")
        shadow_log_path = settings.project_root / str(shadow_cfg.get("output_jsonl", "data/runtime/shadow_bets.jsonl"))
        append_shadow_log(legacy_shadow_bets, shadow_log_path, race_date)

        metrics = {
            "races": int(enriched["race_id"].nunique()),
            "horses": int(len(enriched)),
            "value_candidates": int(enriched["value_candidate"].sum()),
            "buy_candidate_races": int(race_selection["buy_candidate"].sum()) if not race_selection.empty else 0,
            "shadow_bets": int(len(legacy_shadow_bets)),
            "strategy_bets": int(len(strategy_bets)),
            "strategy_stake_yen": int(sum(b.stake_yen for b in strategy_bets)),
            "avg_top3_concentration": float(odds_summary["top3_concentration"].mean()) if not odds_summary.empty else 0.0,
            "avg_max_gap_ratio": float(odds_summary["max_gap_ratio"].mean()) if not odds_summary.empty else 0.0,
        }
        log_metrics(metrics)
        log_params({"race_date": race_date, "input_path": str(input_path), "mode": settings.section("app").get("mode", "SHADOW")})

    return {
        "validation": validation,
        "predictions": enriched,
        "odds_summary": odds_summary,
        "race_selection": race_selection,
        "bets": legacy_shadow_bets,
        "strategy_bets": strategy_bets,
        "prediction_path": prediction_path,
        "odds_path": odds_path,
        "race_selection_path": race_selection_path,
        "strategy_path": strategy_path,
        "metrics": metrics,
    }
