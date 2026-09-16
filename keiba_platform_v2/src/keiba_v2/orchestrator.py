from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .combination import add_combination_expected_value
from .config import load_settings
from .contracts import canonicalize, load_race_table
from .features import build_features
from .history import HistoryStore, attach_history_features
from .odds import add_expected_value, analyze_odds
from .prediction import predict
from .race_selector import select_value_races
from .shadow import append_shadow_log, build_shadow_bets
from .storage import RunStore
from .strategies import build_strategy_bets, cap_bets
from .tracking import log_metrics, log_params, tracking_run
from .validation import validate_races


def _load_combination_odds(path: str | Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(src)
    return pd.read_csv(src, encoding="utf-8-sig")


def _safe_tag(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_\-]+", "_", str(value)).strip("_") or "run"


def run_pipeline(
    input_path: str | Path,
    race_date: str,
    settings_path: str | Path | None = None,
    combination_odds_path: str | Path | None = None,
    output_tag: str | None = None,
) -> dict:
    settings = load_settings(settings_path)
    app_cfg = settings.section("app")
    run_id = f"{race_date}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    store = RunStore(settings.project_root / str(app_cfg.get("runtime_db", "data/runtime/keiba_v2.sqlite3")))
    history_store = HistoryStore(settings.project_root / str(app_cfg.get("history_db", "data/runtime/history.sqlite3")))
    store.start_run(run_id, race_date)

    try:
        df = canonicalize(load_race_table(input_path))
        validation = validate_races(df, settings.section("validation"))
        validation.raise_for_error()

        horse_ids = df["horse_id"].fillna("").astype(str).tolist() if "horse_id" in df.columns else []
        history = history_store.load_for_horses(horse_ids)
        df = attach_history_features(df, history, n_recent=int(settings.section("history").get("n_recent", 5)))

        with tracking_run(settings.section("tracking"), settings.project_root, f"run_{race_date}"):
            featured = build_features(df)
            predicted = predict(featured, settings.section("prediction"), settings.project_root)
            enriched = add_expected_value(predicted, settings.section("odds"))
            odds_summary = analyze_odds(enriched, settings.section("odds"))
            race_selection = select_value_races(enriched, settings.section("race_selection"))

            combo_raw = _load_combination_odds(combination_odds_path)
            combo_ev = add_combination_expected_value(enriched, combo_raw) if combo_raw is not None and not combo_raw.empty else pd.DataFrame()

            legacy_shadow_bets = build_shadow_bets(enriched, settings.section("shadow"))
            strategy_bets = cap_bets(
                build_strategy_bets(enriched, settings.section("strategy"), combo_ev),
                settings.section("strategy"),
            )

            output_dir = settings.project_root / "data" / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            tag = _safe_tag(output_tag or race_date)
            prediction_path = output_dir / f"predictions_{tag}.xlsx"
            odds_path = output_dir / f"odds_summary_{tag}.csv"
            race_selection_path = output_dir / f"race_selection_{tag}.csv"
            strategy_path = output_dir / f"strategy_bets_{tag}.json"
            combo_ev_path = output_dir / f"combination_ev_{tag}.csv"

            with pd.ExcelWriter(prediction_path, engine="openpyxl") as writer:
                enriched.to_excel(writer, index=False, sheet_name="predictions")
                odds_summary.to_excel(writer, index=False, sheet_name="odds_summary")
                race_selection.to_excel(writer, index=False, sheet_name="race_selection")
                pd.DataFrame([b.to_dict() for b in strategy_bets]).to_excel(writer, index=False, sheet_name="strategy_bets")
                if not combo_ev.empty:
                    combo_ev.to_excel(writer, index=False, sheet_name="combination_ev")

            odds_summary.to_csv(odds_path, index=False, encoding="utf-8-sig")
            race_selection.to_csv(race_selection_path, index=False, encoding="utf-8-sig")
            combo_ev.to_csv(combo_ev_path, index=False, encoding="utf-8-sig")
            strategy_path.write_text(
                json.dumps([b.to_dict() for b in strategy_bets], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            shadow_cfg = settings.section("shadow")
            shadow_log_path = settings.project_root / str(shadow_cfg.get("output_jsonl", "data/runtime/shadow_bets.jsonl"))
            append_shadow_log(legacy_shadow_bets, shadow_log_path, race_date)
            store.save_strategy_bets(run_id, [b.to_dict() for b in strategy_bets])

            metrics = {
                "races": int(enriched["race_id"].nunique()),
                "horses": int(len(enriched)),
                "history_rows_used": int(len(history)),
                "value_candidates": int(enriched["value_candidate"].sum()),
                "buy_candidate_races": int(race_selection["buy_candidate"].sum()) if not race_selection.empty else 0,
                "combination_value_candidates": int((combo_ev["expected_value"] >= float(settings.section("strategy").get("min_expected_value", 1.08))).sum()) if not combo_ev.empty else 0,
                "shadow_bets": int(len(legacy_shadow_bets)),
                "strategy_bets": int(len(strategy_bets)),
                "strategy_stake_yen": int(sum(b.stake_yen for b in strategy_bets)),
                "avg_top3_concentration": float(odds_summary["top3_concentration"].mean()) if not odds_summary.empty else 0.0,
                "avg_max_gap_ratio": float(odds_summary["max_gap_ratio"].mean()) if not odds_summary.empty else 0.0,
            }
            log_metrics(metrics)
            log_params({
                "race_date": race_date,
                "input_path": str(input_path),
                "combination_odds_path": str(combination_odds_path or ""),
                "mode": app_cfg.get("mode", "SHADOW"),
                "run_id": run_id,
                "output_tag": tag,
            })
            store.finish_run(run_id, "SUCCESS", metrics)

        return {
            "run_id": run_id,
            "validation": validation,
            "predictions": enriched,
            "odds_summary": odds_summary,
            "combination_ev": combo_ev,
            "race_selection": race_selection,
            "bets": legacy_shadow_bets,
            "strategy_bets": strategy_bets,
            "prediction_path": prediction_path,
            "odds_path": odds_path,
            "race_selection_path": race_selection_path,
            "strategy_path": strategy_path,
            "combination_ev_path": combo_ev_path,
            "metrics": metrics,
        }
    except Exception as exc:
        store.finish_run(run_id, "FAILED", {"error": str(exc)})
        raise
