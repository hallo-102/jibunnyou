# -*- coding: utf-8 -*-
"""本番予想と分離した前向きシャドー比較の保存処理。"""

from __future__ import annotations

import importlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from model_registry import (
    ModelConfigError,
    ModelSpec,
    load_model_spec,
    model_metadata,
    sha256_file,
    validate_effective_weights,
    write_json_atomic,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SHADOW_CONFIG_PATH = PROJECT_ROOT / "config" / "shadow_model.json"
FORWARD_CONFIG_PATH = PROJECT_ROOT / "config" / "forward_validation.json"
SHADOW_DENY_FLAGS = (
    "allow_production_rank_write",
    "allow_production_bet_write",
    "allow_auto_purchase",
    "allow_official_email",
    "allow_production_selection",
    "allow_optimizer_baseline",
    "allow_weight_optimization",
)


@dataclass(frozen=True)
class ShadowRuntime:
    """検証済みシャドーモデルと出力先。"""

    spec: ModelSpec
    weights_map: Mapping[Any, Mapping[str, float]]
    output_dir: Path


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    """JSONルートがobjectであることまで検証して読む。"""
    if not path.is_file():
        raise ModelConfigError(f"{label}が存在しません: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelConfigError(f"{label}を読み込めません: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ModelConfigError(f"{label}のルートはobjectである必要があります: {path}")
    return payload


def _resolve_project_relative_dir(value: Any) -> Path:
    """シャドー出力先をプロジェクト内相対パスに制限する。"""
    if not isinstance(value, str) or not value.strip():
        raise ModelConfigError("shadow output_dirが未設定です")
    relative = Path(value)
    if relative.is_absolute():
        raise ModelConfigError("shadow output_dirはプロジェクト相対パスで指定してください")
    root = PROJECT_ROOT.resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ModelConfigError("shadow output_dirはプロジェクト内に指定してください") from exc
    return resolved


def validate_shadow_isolation_policy(payload: Mapping[str, Any]) -> None:
    """本番系への接続を許可するflagは、未定義もTrueも拒否する。"""
    for flag in SHADOW_DENY_FLAGS:
        if flag not in payload:
            raise ModelConfigError(f"シャドー分離flagが未定義です: {flag}")
        if payload[flag] is not False:
            raise ModelConfigError(f"シャドーは{flag}=falseである必要があります")


def load_shadow_runtime() -> ShadowRuntime:
    """シャドーのファイルSHA・実効SHA・分離policyを検証する。"""
    spec = load_model_spec(
        PROJECT_ROOT,
        SHADOW_CONFIG_PATH,
        expected_role="shadow",
    )
    validate_shadow_isolation_policy(spec.raw)
    output_dir = _resolve_project_relative_dir(spec.raw.get("output_dir"))

    # baselineの共通読込処理は、本番と同じ補完・符号ガードを適用する。
    baseline = importlib.import_module("tokutyouryou_keisann.baseline")
    weights_map = baseline.load_effective_weights_file(spec.resolved_weight_path)
    production_config = importlib.import_module("1_keibayosou_config")
    validate_effective_weights(
        spec,
        weights_map,
        scoring_model_version=production_config.SCORING_MODEL_VERSION,
    )
    return ShadowRuntime(spec=spec, weights_map=weights_map, output_dir=output_dir)


def _run_git(*args: str) -> subprocess.CompletedProcess[str]:
    """プロジェクトルートでGitを実行し、Windowsでも安定した結果を返す。"""
    try:
        return subprocess.run(
            ["git", *args],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except OSError as exc:
        raise ModelConfigError(f"Gitコマンドを実行できません: {exc}") from exc


def _git_index_blob_sha1(relative_value: str) -> str:
    """Git indexに登録されたblob SHA-1を返し、未コミット変更があれば拒否する。"""
    git_path = Path(relative_value).as_posix()

    worktree = _run_git("diff", "--quiet", "--", git_path)
    if worktree.returncode == 1:
        raise ModelConfigError(
            f"前向き検証の固定ファイルに未コミット変更があります: path={relative_value}"
        )
    if worktree.returncode != 0:
        raise ModelConfigError(
            "前向き検証の固定ファイル状態を確認できません: "
            f"path={relative_value} stderr={worktree.stderr.strip()}"
        )

    staged = _run_git("diff", "--cached", "--quiet", "--", git_path)
    if staged.returncode == 1:
        raise ModelConfigError(
            f"前向き検証の固定ファイルに未コミットのステージ変更があります: path={relative_value}"
        )
    if staged.returncode != 0:
        raise ModelConfigError(
            "前向き検証の固定ファイルのステージ状態を確認できません: "
            f"path={relative_value} stderr={staged.stderr.strip()}"
        )

    result = _run_git("rev-parse", f":{git_path}")
    actual_sha = result.stdout.strip().lower()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", actual_sha):
        raise ModelConfigError(
            "前向き検証のGit index blob SHAを取得できません: "
            f"path={relative_value} stderr={result.stderr.strip()}"
        )
    return actual_sha


def _select_forward_validation_generation(payload: Mapping[str, Any]) -> dict[str, Any]:
    """schema v1はそのまま、schema v2はactive_generationだけを実行対象として返す。"""
    schema_version = int(payload.get("schema_version", 1) or 1)
    if schema_version == 1:
        return dict(payload)
    if schema_version != 2:
        raise ModelConfigError(f"未対応の前向き検証schema_versionです: {schema_version}")

    active_generation = payload.get("active_generation")
    generations = payload.get("generations")
    if not isinstance(active_generation, str) or not active_generation.strip():
        raise ModelConfigError("前向き検証active_generationが未設定です")
    if not isinstance(generations, dict) or not generations:
        raise ModelConfigError("前向き検証generationsが未設定です")
    selected = generations.get(active_generation)
    if not isinstance(selected, dict):
        raise ModelConfigError(
            f"active_generationに対応する世代設定がありません: {active_generation}"
        )

    policy = dict(selected)
    policy["schema_version"] = schema_version
    policy["active_generation"] = active_generation
    policy["generation_history"] = list(generations.keys())
    return policy


def load_forward_validation_policy() -> dict[str, Any]:
    """前向き検証の有効世代・開始日・閾値・固定指標を検証する。"""
    raw_payload = _read_json_object(FORWARD_CONFIG_PATH, "前向き検証設定")
    payload = _select_forward_validation_generation(raw_payload)

    start_date = str(payload.get("start_date", ""))
    if not re.fullmatch(r"\d{8}", start_date):
        raise ModelConfigError("前向き検証start_dateはYYYYMMDDで指定してください")
    if str(payload.get("optimizer_holdout_start_date", "")) != start_date:
        raise ModelConfigError("最適化HOLDOUT開始日と前向き検証開始日が不一致です")

    prediction_gate = payload.get("prediction_interim_gate")
    profit_gate = payload.get("profit_final_gate")
    if not isinstance(prediction_gate, dict) or (
        int(prediction_gate.get("minimum_racing_days", 0)) != 8
        or int(prediction_gate.get("minimum_races", 0)) != 300
        or prediction_gate.get("require_both") is not True
    ):
        raise ModelConfigError("予測精度の中間判定閾値が事前定義と不一致です")
    if not isinstance(profit_gate, dict) or (
        int(profit_gate.get("minimum_current_trifecta_races", 0)) != 50
        or not re.fullmatch(r"\d{8}", str(profit_gate.get("validation_end_date", "")))
        or profit_gate.get("insufficient_sample_action") != "do_not_promote_by_roi_only"
    ):
        raise ModelConfigError("利益面の本判定閾値が事前定義と不一致です")

    required_primary = {
        "current_trifecta_profit",
        "current_trifecta_roi",
        "current_trifecta_max_drawdown",
        "top3_complete_rate",
    }
    required_safety = {
        "rank1_place_rate",
        "rank1_average_finish",
        "top5_point_rate",
        "daily_monthly_consistency",
    }
    if set(payload.get("primary_metrics", [])) != required_primary:
        raise ModelConfigError("主要指標が事前定義と不一致です")
    if set(payload.get("safety_metrics", [])) != required_safety:
        raise ModelConfigError("安全指標が事前定義と不一致です")

    frozen_files = payload.get("frozen_files")
    if not isinstance(frozen_files, dict) or not frozen_files:
        raise ModelConfigError("前向き検証の固定ファイルSHAが未設定です")

    hash_algorithm = str(payload.get("hash_algorithm", "sha256")).strip().lower()
    if hash_algorithm not in {"sha256", "git_blob_sha1"}:
        raise ModelConfigError(
            f"前向き検証hash_algorithmが未対応です: {hash_algorithm}"
        )

    root = PROJECT_ROOT.resolve()
    for relative_value, expected_sha in frozen_files.items():
        if not isinstance(relative_value, str) or not isinstance(expected_sha, str):
            raise ModelConfigError("固定ファイルの相対パスまたはSHAが不正です")
        relative = Path(relative_value)
        if relative.is_absolute():
            raise ModelConfigError(f"固定ファイルは相対パスで指定してください: {relative_value}")
        resolved = (root / relative).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ModelConfigError(f"プロジェクト外の固定ファイルは使えません: {relative_value}") from exc
        if not resolved.is_file():
            raise ModelConfigError(f"前向き検証の固定ファイルが存在しません: {resolved}")

        if hash_algorithm == "git_blob_sha1":
            actual_sha = _git_index_blob_sha1(relative_value)
        else:
            actual_sha = sha256_file(resolved)
        if actual_sha != expected_sha:
            generation = payload.get("active_generation", "legacy")
            raise ModelConfigError(
                "前向き検証の固定ファイルSHAが不一致です: "
                f"generation={generation} algorithm={hash_algorithm} "
                f"path={relative_value} expected={expected_sha} actual={actual_sha}"
            )
    return payload


def _json_value(value: Any) -> Any:
    """pandas/numpy型をJSONで安定保存できる値へ変換する。"""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _race_key(value: Any) -> str:
    """レースIDのExcel由来の.0や空白を除く。"""
    text = str(value).strip()
    return text[:-2] if text.endswith(".0") else text


def _top5_rows(prediction_df: pd.DataFrame, race_id: str) -> list[dict[str, Any]]:
    """指定レースの予想1〜5位とスコアを保存用に抜き出す。"""
    work = prediction_df.copy()
    work["_race_key"] = work["rid_str"].map(_race_key)
    work["_rank"] = pd.to_numeric(work.get("rank"), errors="coerce")
    race = work[(work["_race_key"] == race_id) & work["_rank"].between(1, 5)].copy()
    race = race.sort_values("_rank", kind="mergesort")
    return [
        {
            "rank": int(row["_rank"]),
            "horse_number": _json_value(row.get("馬番")),
            "horse_name": _json_value(row.get("馬名")),
            "score": _json_value(row.get("score")),
        }
        for _, row in race.iterrows()
    ]


def _bet_record(
    bet_df: pd.DataFrame,
    roi_df: pd.DataFrame,
    race_id: str,
) -> dict[str, Any]:
    """予測時点のランク判定と現行3連複買い目だけを抜き出す。"""
    rank_bet: dict[str, Any] | None = None
    if not bet_df.empty and "レースID" in bet_df.columns:
        matched = bet_df[bet_df["レースID"].map(_race_key) == race_id]
        if not matched.empty:
            row = matched.iloc[0]
            rank_bet = {
                "rank_label": _json_value(row.get("ランク(S/A/B)")),
                "judgment": _json_value(row.get("判定")),
                "gap12": _json_value(row.get("gap12")),
                "top5_horse_numbers": [
                    _json_value(row.get(f"{rank}位馬番")) for rank in range(1, 6)
                ],
            }

    trifecta_tickets: list[list[int]] = []
    purchase_yen = 0
    if not roi_df.empty and "レースID" in roi_df.columns:
        matched = roi_df[roi_df["レースID"].map(_race_key) == race_id]
        if not matched.empty:
            row = matched.iloc[0]
            for ticket_no in range(1, 4):
                values = [
                    _json_value(row.get(f"3連複{ticket_no}点目_馬番{position}"))
                    for position in range(1, 4)
                ]
                if all(value is not None for value in values):
                    trifecta_tickets.append(sorted(int(value) for value in values))
            purchase_yen = int(_json_value(row.get("3連複_金額")) or 0)

    return {
        "rank_bet": rank_bet,
        "current_trifecta": {
            "tickets": trifecta_tickets,
            "purchase_yen": purchase_yen,
        },
    }


def build_shadow_pre_payload(
    *,
    comparison_date: str,
    production_spec: ModelSpec,
    shadow_spec: ModelSpec,
    production_predictions: pd.DataFrame,
    shadow_predictions: pd.DataFrame,
    production_bets: pd.DataFrame,
    shadow_bets: pd.DataFrame,
    production_roi_bets: pd.DataFrame,
    shadow_roi_bets: pd.DataFrame,
    compared_at: str | None = None,
) -> dict[str, Any]:
    """結果列を受け取らず、結果確定前シャドーJSONを組み立てる。"""
    if not re.fullmatch(r"\d{8}", comparison_date):
        raise ModelConfigError("シャドー比較日はYYYYMMDDで指定してください")
    if production_spec.role != "production" or shadow_spec.role != "shadow":
        raise ModelConfigError("本番とシャドーのmodel roleが不正です")

    production_races = {
        _race_key(value) for value in production_predictions.get("rid_str", pd.Series(dtype=object))
    }
    shadow_races = {
        _race_key(value) for value in shadow_predictions.get("rid_str", pd.Series(dtype=object))
    }
    if production_races != shadow_races:
        raise ModelConfigError(
            "本番とシャドーのレース集合が一致しません: "
            f"production_only={sorted(production_races - shadow_races)} "
            f"shadow_only={sorted(shadow_races - production_races)}"
        )

    records: list[dict[str, Any]] = []
    for race_id in sorted(production_races):
        production_top5 = _top5_rows(production_predictions, race_id)
        shadow_top5 = _top5_rows(shadow_predictions, race_id)
        records.append(
            {
                "race_id": race_id,
                "race_date": comparison_date,
                "production_model_name": production_spec.model_name,
                "shadow_model_name": shadow_spec.model_name,
                "production_top5": production_top5,
                "shadow_top5": shadow_top5,
                "production_rank1": production_top5[0] if production_top5 else None,
                "shadow_rank1": shadow_top5[0] if shadow_top5 else None,
                "production_scores": [row["score"] for row in production_top5],
                "shadow_scores": [row["score"] for row in shadow_top5],
                "production_bets": _bet_record(
                    production_bets,
                    production_roi_bets,
                    race_id,
                ),
                "shadow_bets": _bet_record(shadow_bets, shadow_roi_bets, race_id),
                # 結果確定前ファイルに未来情報を入れない。
                "actual_finish": None,
                "payout": None,
                "production_profit": None,
                "shadow_profit": None,
            }
        )

    comparison_time = compared_at or datetime.now().astimezone().isoformat(timespec="seconds")
    return {
        "schema_version": 1,
        "result_status": "pre_result",
        "comparison_date": comparison_date,
        "compared_at": comparison_time,
        "production_model": model_metadata(production_spec),
        "shadow_model": model_metadata(shadow_spec),
        "isolation": {
            "production_rank_affected": False,
            "production_score_affected": False,
            "production_bet_affected": False,
            "auto_purchase_allowed": False,
            "official_email_allowed": False,
            "optimizer_use_allowed": False,
        },
        "races": records,
    }


def write_shadow_pre_payload(
    runtime: ShadowRuntime,
    payload: dict[str, Any],
) -> Path:
    """日付ディレクトリに結果確定前JSONを追記型で保存する。"""
    if payload.get("result_status") != "pre_result":
        raise ModelConfigError("結果確定前のシャドー出力以外はこの関数で保存できません")
    comparison_date = str(payload.get("comparison_date", ""))
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    output_path = (
        runtime.output_dir
        / comparison_date
        / f"shadow_comparison_{comparison_date}_pre_{timestamp}.json"
    )
    write_json_atomic(output_path, payload)
    return output_path


def _settle_current_trifecta(
    bet_record: Mapping[str, Any],
    trifecta_payouts: Mapping[str, int],
) -> dict[str, Any]:
    """予測時に固定した現行3連複だけを100円単位で精算する。"""
    current = bet_record.get("current_trifecta", {})
    if not isinstance(current, Mapping):
        current = {}
    raw_tickets = current.get("tickets", [])
    tickets = raw_tickets if isinstance(raw_tickets, list) else []
    purchase_yen = int(current.get("purchase_yen", 0) or 0)
    return_yen = 0
    hit_tickets: list[str] = []
    for ticket in tickets:
        if not isinstance(ticket, list) or len(ticket) != 3:
            continue
        combo = "-".join(str(number) for number in sorted(int(value) for value in ticket))
        payout_yen = int(trifecta_payouts.get(combo, 0))
        if payout_yen > 0:
            return_yen += payout_yen
            hit_tickets.append(combo)
    return {
        "purchase_yen": purchase_yen,
        "return_yen": return_yen,
        "profit_yen": return_yen - purchase_yen,
        "hit": bool(hit_tickets),
        "hit_tickets": hit_tickets,
    }


def finalize_shadow_payload(
    pre_payload: Mapping[str, Any],
    result_entries: pd.DataFrame,
    result_payouts: pd.DataFrame,
    *,
    finalized_at: str | None = None,
) -> dict[str, Any]:
    """確定結果をpre JSONのコピーへ照合し、post用payloadを作る。"""
    if pre_payload.get("result_status") != "pre_result":
        raise ModelConfigError("結果照合元はpre_result JSONである必要があります")
    races = pre_payload.get("races")
    if not isinstance(races, list):
        raise ModelConfigError("pre_result JSONのracesが不正です")

    entries = result_entries.copy()
    payouts = result_payouts.copy()
    entries["_race_key"] = entries.get("rid_str", pd.Series(dtype=object)).map(_race_key)
    payouts["_race_key"] = payouts.get("rid_str", pd.Series(dtype=object)).map(_race_key)
    post_races: list[dict[str, Any]] = []
    for raw_race in races:
        if not isinstance(raw_race, Mapping):
            raise ModelConfigError("pre_result JSONのrace recordが不正です")
        race = dict(raw_race)
        race_id = _race_key(race.get("race_id"))
        race_entries = entries[entries["_race_key"] == race_id].copy()
        race_payouts = payouts[payouts["_race_key"] == race_id].copy()
        if race_entries.empty:
            raise ModelConfigError(f"シャドー結果照合に着順がありません: race_id={race_id}")
        trifecta_rows = race_payouts[
            race_payouts["払戻種別"].astype(str).str.contains("3連複", na=False)
        ]
        if trifecta_rows.empty:
            raise ModelConfigError(f"シャドー結果照合に3連複払戻がありません: race_id={race_id}")

        race_entries["着順_num"] = pd.to_numeric(
            race_entries["着順_num"],
            errors="coerce",
        )
        race_entries = race_entries.sort_values("着順_num", kind="mergesort")
        race["actual_finish"] = [
            {
                "finish": _json_value(row.get("着順_num")),
                "horse_number": _json_value(row.get("馬番_int")),
                "horse_name_normalized": _json_value(row.get("name_norm")),
            }
            for _, row in race_entries.iterrows()
            if pd.notna(row.get("着順_num"))
        ]
        race["payout"] = [
            {
                "bet_type": _json_value(row.get("払戻種別")),
                "combination": _json_value(row.get("組番_norm")),
                "payout_yen_per_100": int(_json_value(row.get("払戻金_int")) or 0),
            }
            for _, row in race_payouts.iterrows()
        ]
        trifecta_map = {
            str(row["組番_norm"]): int(row["払戻金_int"])
            for _, row in trifecta_rows.iterrows()
        }
        race["production_profit"] = _settle_current_trifecta(
            race.get("production_bets", {}),
            trifecta_map,
        )
        race["shadow_profit"] = _settle_current_trifecta(
            race.get("shadow_bets", {}),
            trifecta_map,
        )
        post_races.append(race)

    output = dict(pre_payload)
    output["result_status"] = "post_result"
    output["finalized_at"] = finalized_at or datetime.now().astimezone().isoformat(
        timespec="seconds"
    )
    output["races"] = post_races
    return output
