from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from itertools import combinations
from uuid import uuid4

from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import Session

from app.db.models import (
    AiBetStrategy,
    AiHorseEvaluation,
    AiPredictionRun,
    BetCandidate,
    FinalPrediction,
    PredictionResult,
    PredictionRun,
    Race,
    RaceEntry,
)
from app.schemas.api import AiRunSummary
from app.services.history import record_bet_status_change


ENTRY_PREDICTION_RUN_PREFIX = "entry-"


@dataclass(frozen=True)
class _HorseOpinion:
    prediction: PredictionResult
    adjust: float
    action: str
    reason: str
    risk: str
    role_hint: str
    confidence: float


def run_ai_explain(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
    prediction_run_id: str | None = None,
    model_name: str | None = None,
    prompt_version: str | None = None,
) -> AiRunSummary:
    """Save AI explanation runs that describe Python predictions without changing ranks."""

    grouped = _latest_python_results_by_race(db, race_date, race_id, prediction_run_id)
    summary = AiRunSummary(race_date=race_date, race_id=race_id, ai_mode="ai_explain")
    for result_race_id, results in grouped.items():
        if not results:
            continue
        top = results[0]
        run = _new_ai_run(
            db=db,
            race_id=result_race_id,
            race_date=_race_date_for_results(db, results),
            ai_mode="ai_explain",
            python_prediction_run_id=top.prediction_run_id,
            model_name=model_name or "heuristic-explain-v1",
            prompt_version=prompt_version or "ai-explain-v1",
        )
        run.race_summary = (
            f"Python予想は{top.horse_no}番 {top.horse_name} を最上位に評価。"
            f"score={top.prediction_score or 0:.2f}、推定馬券内率={top.estimated_in3_rate or 0:.2f}を中心に説明します。"
        )
        run.pace_prediction = "AI説明では展開補正なし"
        run.python_trust_level = "説明のみ"
        run.raw_request = _request_payload(db, result_race_id, results)
        run.raw_response = {"race_summary": run.race_summary, "ai_mode": "ai_explain"}
        run.finished_at = datetime.utcnow()
        summary.runs += 1
    db.commit()
    return summary


def run_ai_second_opinion(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
    prediction_run_id: str | None = None,
    model_name: str | None = None,
    prompt_version: str | None = None,
) -> AiRunSummary:
    """Inspect Python predictions from a separate AI second-opinion perspective."""

    grouped = _latest_python_results_by_race(db, race_date, race_id, prediction_run_id)
    summary = AiRunSummary(race_date=race_date, race_id=race_id, ai_mode="ai_second_opinion")
    for result_race_id, results in grouped.items():
        if not results:
            continue
        opinions = _build_second_opinions(results)
        run = _new_ai_run(
            db=db,
            race_id=result_race_id,
            race_date=_race_date_for_results(db, results),
            ai_mode="ai_second_opinion",
            python_prediction_run_id=results[0].prediction_run_id,
            model_name=model_name or "heuristic-second-opinion-v1",
            prompt_version=prompt_version or "ai-second-opinion-v1",
        )
        final_rows = _rank_final_predictions(opinions)
        response = _response_payload(db, result_race_id, opinions, final_rows)
        run.race_summary = response["race_summary"]
        run.pace_prediction = response["pace_prediction"]
        run.python_trust_level = response["python_trust_level"]
        run.raw_request = _request_payload(db, result_race_id, results)
        run.raw_response = response
        run.finished_at = datetime.utcnow()

        for final_rank, opinion in final_rows:
            prediction = opinion.prediction
            ai_rank = final_rank
            db.add(
                AiHorseEvaluation(
                    ai_run_id=run.id,
                    race_id=result_race_id,
                    horse_no=prediction.horse_no,
                    horse_name=prediction.horse_name,
                    python_rank=prediction.prediction_rank,
                    ai_rank=ai_rank,
                    ai_action=opinion.action,
                    ai_adjust_score=opinion.adjust,
                    ai_reason=opinion.reason,
                    ai_risk=opinion.risk,
                    ai_bet_role=_role_for_final_rank(final_rank, opinion),
                    ai_confidence=opinion.confidence,
                )
            )
            db.add(
                FinalPrediction(
                    ai_run_id=run.id,
                    race_id=result_race_id,
                    horse_no=prediction.horse_no,
                    horse_name=prediction.horse_name,
                    python_rank=prediction.prediction_rank,
                    python_score=prediction.prediction_score,
                    ai_rank=ai_rank,
                    ai_adjust_score=opinion.adjust,
                    final_score=round((prediction.prediction_score or 0.0) + opinion.adjust, 2),
                    final_rank=final_rank,
                    final_bet_role=_role_for_final_rank(final_rank, opinion),
                )
            )

        summary.runs += 1
        summary.evaluations += len(opinions)
        summary.final_predictions += len(final_rows)

    db.commit()
    return summary


def run_ai_bet_correction(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
    ai_run_id: str | None = None,
    stake_per_point: int = 500,
    max_race_amount: int = 3000,
    max_day_amount: int = 12000,
) -> AiRunSummary:
    """Create AI-corrected bet strategy and bet candidate from final predictions."""

    runs = _target_second_opinion_runs(db, race_date, race_id, ai_run_id)
    summary = AiRunSummary(race_date=race_date, race_id=race_id, ai_mode="ai_bet_correction")
    day_total = _current_day_bet_total(db, race_date)

    for run in runs:
        finals = list(
            db.scalars(
                select(FinalPrediction)
                .where(FinalPrediction.ai_run_id == run.id)
                .order_by(FinalPrediction.final_rank, FinalPrediction.horse_no)
            )
        )
        if not finals:
            summary.warnings.append(f"final predictions not found: race_id={run.race_id}")
            continue

        existing_candidate = db.scalar(
            select(BetCandidate)
            .where(
                BetCandidate.prediction_run_id == (run.python_prediction_run_id or run.id),
                BetCandidate.race_id == run.race_id,
                BetCandidate.strategy.like("AI補正%"),
            )
            .order_by(BetCandidate.created_at.desc())
            .limit(1)
        )
        if existing_candidate is not None:
            if existing_candidate.status == "candidate":
                day_total += existing_candidate.total_amount
            summary.runs += 1
            summary.strategies += 1
            continue

        strategy = db.scalar(
            select(AiBetStrategy)
            .where(AiBetStrategy.ai_run_id == run.id)
            .order_by(AiBetStrategy.created_at.desc())
            .limit(1)
        )
        if strategy is None:
            strategy = _build_ai_strategy(run, finals)
            db.add(strategy)
            db.flush()

        candidate = _build_ai_bet_candidate(
            run=run,
            finals=finals,
            strategy=strategy,
            stake_per_point=stake_per_point,
            max_race_amount=max_race_amount,
            max_day_amount=max_day_amount,
            current_day_total=day_total,
        )
        db.add(candidate)
        db.flush()
        record_bet_status_change(
            db,
            bet_candidate_id=candidate.id,
            old_status=None,
            new_status=candidate.status,
            reason=candidate.reason or candidate.skip_reason,
            prediction_run_id=candidate.prediction_run_id,
        )
        if candidate.status == "candidate":
            day_total += candidate.total_amount

        summary.runs += 1
        summary.strategies += 1

    db.commit()
    return summary


def _latest_python_results_by_race(
    db: Session,
    race_date: date | None,
    race_id: str | None,
    prediction_run_id: str | None,
) -> dict[str, list[PredictionResult]]:
    if prediction_run_id:
        stmt = select(PredictionResult).where(PredictionResult.prediction_run_id == prediction_run_id)
        if race_id:
            stmt = stmt.where(PredictionResult.race_id == race_id)
        return _group_results(list(db.scalars(stmt.order_by(PredictionResult.race_id, PredictionResult.prediction_rank))))

    run_stmt = select(PredictionRun).where(PredictionRun.status == "completed")
    if race_date:
        run_stmt = run_stmt.where(PredictionRun.race_date == race_date)
    if race_id:
        run_stmt = run_stmt.where(or_(PredictionRun.race_id == race_id, PredictionRun.race_id.is_(None)))
    runs = list(db.scalars(run_stmt.order_by(PredictionRun.finished_at.desc(), PredictionRun.created_at.desc())))

    grouped: dict[str, list[PredictionResult]] = {}
    for run in runs:
        stmt = select(PredictionResult).where(PredictionResult.prediction_run_id == run.id)
        if race_id:
            stmt = stmt.where(PredictionResult.race_id == race_id)
        for result_race_id, results in _group_results(
            list(db.scalars(stmt.order_by(PredictionResult.race_id, PredictionResult.prediction_rank)))
        ).items():
            grouped.setdefault(result_race_id, results)
    for result_race_id, results in _entry_prediction_results_by_race(
        db=db,
        race_date=race_date,
        race_id=race_id,
        skip_race_ids=set(grouped),
    ).items():
        grouped.setdefault(result_race_id, results)
    return grouped


def _entry_prediction_results_by_race(
    db: Session,
    race_date: date | None,
    race_id: str | None,
    skip_race_ids: set[str],
) -> dict[str, list[PredictionResult]]:
    entry_stmt = (
        select(RaceEntry)
        .join(Race, Race.race_id == RaceEntry.race_id)
        .where(or_(RaceEntry.prediction_rank.is_not(None), RaceEntry.prediction_score.is_not(None)))
    )
    if race_date is not None:
        entry_stmt = entry_stmt.where(Race.race_date == race_date)
    if race_id is not None:
        entry_stmt = entry_stmt.where(RaceEntry.race_id == race_id)
    if skip_race_ids:
        entry_stmt = entry_stmt.where(RaceEntry.race_id.not_in(skip_race_ids))

    grouped_entries: dict[str, list[RaceEntry]] = {}
    entries = list(db.scalars(entry_stmt.order_by(RaceEntry.race_id, RaceEntry.horse_no)))
    for entry in entries:
        grouped_entries.setdefault(entry.race_id, []).append(entry)

    run_ids: list[str] = []
    for result_race_id, race_entries in grouped_entries.items():
        ordered_entries = sorted(
            race_entries,
            key=lambda item: (
                item.prediction_rank or 999,
                -1 * (item.prediction_score or 0.0),
                item.horse_no,
            ),
        )
        run = _upsert_entry_prediction_run(db, result_race_id, ordered_entries)
        run_ids.append(run.id)
        db.execute(
            delete(PredictionResult).where(
                PredictionResult.prediction_run_id == run.id,
                PredictionResult.race_id == result_race_id,
            )
        )
        for entry in ordered_entries:
            db.add(_prediction_result_from_entry(run.id, entry))

    if not run_ids:
        return {}

    db.flush()
    result_stmt = (
        select(PredictionResult)
        .where(PredictionResult.prediction_run_id.in_(run_ids))
        .order_by(PredictionResult.race_id, PredictionResult.prediction_rank, PredictionResult.horse_no)
    )
    return _group_results(list(db.scalars(result_stmt)))


def _upsert_entry_prediction_run(
    db: Session,
    race_id: str,
    entries: list[RaceEntry],
) -> PredictionRun:
    run_id = f"{ENTRY_PREDICTION_RUN_PREFIX}{race_id}"
    race = db.get(Race, race_id)
    finished_at = datetime.utcnow()
    prediction_run = db.get(PredictionRun, run_id)
    if prediction_run is None:
        prediction_run = PredictionRun(id=run_id, created_at=finished_at)
        db.add(prediction_run)

    prediction_run.status = "completed"
    prediction_run.race_date = race.race_date if race is not None else None
    prediction_run.race_id = race_id
    prediction_run.prediction_version = "race-entry-v1"
    prediction_run.feature_version = "race-entry-feature-v1"
    prediction_run.weight_version = "legacy-weight-v1"
    prediction_run.model_version = "race-entry-import"
    prediction_run.source_file = "race_entries"
    prediction_run.output_file = None
    prediction_run.result_json_file = None
    prediction_run.manifest_file = None
    prediction_run.input_checksum = None
    prediction_run.result_count = len(entries)
    prediction_run.matched_count = len(entries)
    prediction_run.mismatch_count = 0
    prediction_run.message = "race_entriesのPython予想列からAI入力用に補完"
    prediction_run.started_at = finished_at
    prediction_run.finished_at = finished_at
    return prediction_run


def _prediction_result_from_entry(prediction_run_id: str, entry: RaceEntry) -> PredictionResult:
    raw = entry.raw if isinstance(entry.raw, dict) else {}
    risk_score = _entry_risk_score(raw)
    risk_flag = risk_score > 0
    return PredictionResult(
        prediction_run_id=prediction_run_id,
        race_id=entry.race_id,
        horse_no=entry.horse_no,
        horse_name=entry.horse_name,
        popularity=entry.popularity,
        win_odds=entry.win_odds,
        place_odds=entry.place_odds,
        prediction_rank=entry.prediction_rank,
        prediction_score=entry.prediction_score,
        estimated_in3_rate=entry.estimated_in3_rate,
        expected_value=entry.expected_value,
        risk_flag=risk_flag,
        risk_score=risk_score,
        risk_reason=_entry_risk_reason(raw, risk_score),
        evaluation_reason=_entry_evaluation_reason(entry),
        feature_summary=_entry_feature_summary(raw),
        raw=raw,
    )


def _entry_risk_score(raw: dict) -> float:
    values = [
        _raw_float(raw, "favorite_risk"),
        _raw_float(raw, "extra_penalty"),
        _raw_float(raw, "rest_dist_risk"),
    ]
    return float(sum(value for value in values if value is not None and value > 0))


def _entry_risk_reason(raw: dict, risk_score: float) -> str:
    reasons: list[str] = []
    if (_raw_float(raw, "favorite_risk") or 0) > 0:
        reasons.append("人気先行リスク")
    if (_raw_float(raw, "extra_penalty") or 0) > 0:
        reasons.append("追加ペナルティ")
    if (_raw_float(raw, "rest_dist_risk") or 0) > 0:
        reasons.append("休養・距離変化リスク")
    if not reasons:
        return "危険馬判定なし"
    return f"{'、'.join(reasons)}（risk_score={risk_score:.2f}）"


def _entry_evaluation_reason(entry: RaceEntry) -> str:
    parts: list[str] = []
    if entry.prediction_rank is not None:
        parts.append(f"Python順位{entry.prediction_rank}位")
    if entry.prediction_score is not None:
        parts.append(f"score {entry.prediction_score:.2f}")
    if entry.estimated_in3_rate is not None:
        parts.append(f"推定馬券内率 {entry.estimated_in3_rate:.2f}")
    if entry.expected_value is not None:
        parts.append(f"期待値 {entry.expected_value:.2f}")
    return " / ".join(parts) if parts else "RaceEntry補完による評価"


def _entry_feature_summary(raw: dict) -> dict:
    keys = [
        "total",
        "score",
        "dl_rank",
        "dl_prob",
        "favorite_risk",
        "extra_penalty",
        "rest_dist_risk",
        "推定馬券内率_オッズ補正後",
        "期待値",
    ]
    return {key: raw.get(key) for key in keys if key in raw}


def _raw_float(raw: dict, key: str) -> float | None:
    value = raw.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _group_results(results: list[PredictionResult]) -> dict[str, list[PredictionResult]]:
    grouped: dict[str, list[PredictionResult]] = {}
    for result in results:
        grouped.setdefault(result.race_id, []).append(result)
    for race_results in grouped.values():
        race_results.sort(key=lambda item: (item.prediction_rank or 999, item.horse_no))
    return grouped


def _new_ai_run(
    db: Session,
    race_id: str,
    race_date: date | None,
    ai_mode: str,
    python_prediction_run_id: str | None,
    model_name: str,
    prompt_version: str,
) -> AiPredictionRun:
    run = AiPredictionRun(
        id=str(uuid4()),
        race_id=race_id,
        race_date=race_date,
        ai_mode=ai_mode,
        model_name=model_name,
        prompt_version=prompt_version,
        python_prediction_run_id=python_prediction_run_id,
        status="success",
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.flush()
    return run


def _race_date_for_results(db: Session, results: list[PredictionResult]) -> date | None:
    prediction_run = db.get(PredictionRun, results[0].prediction_run_id)
    if prediction_run is not None and prediction_run.race_date is not None:
        return prediction_run.race_date
    race = db.get(Race, results[0].race_id)
    return race.race_date if race is not None else None


def _build_second_opinions(results: list[PredictionResult]) -> list[_HorseOpinion]:
    opinions = [_opinion_for_result(result) for result in results]
    if not any(opinion.adjust > 0 for opinion in opinions):
        target = max(opinions, key=lambda item: ((item.prediction.expected_value or 0), item.prediction.win_odds or 0))
        opinions = [_replace_opinion(opinion, 1.0, "やや上げ", "別視点で最低1頭は上げ評価として監視") if opinion is target else opinion for opinion in opinions]
    if not any(opinion.adjust < 0 for opinion in opinions):
        target = min(opinions, key=lambda item: (item.prediction.expected_value or 0, -(item.prediction.popularity or 99)))
        opinions = [_replace_opinion(opinion, -1.0, "やや下げ", "別視点で最低1頭は下げ評価として監視") if opinion is target else opinion for opinion in opinions]
    return opinions


def _opinion_for_result(result: PredictionResult) -> _HorseOpinion:
    adjust = 0.0
    reasons: list[str] = []
    risks: list[str] = []
    role_hint = "押さえ"

    if result.risk_flag:
        adjust -= 3
        risks.append("Python危険馬フラグあり")
        role_hint = "消し"
    if result.risk_flag and (result.prediction_rank or 99) <= 3:
        adjust -= 2
        reasons.append("Python上位だが軸には危険要素が強い")
    if (result.popularity or 99) <= 3 and (result.expected_value or 0) < 0.8:
        adjust -= 3
        reasons.append("人気先行でオッズ妙味が薄い")
        role_hint = "消し"
    if not result.risk_flag and (result.expected_value or 0) >= 1.2 and (result.popularity or 0) >= 5:
        adjust += 3
        reasons.append("人気より期待値が高く穴で拾う価値がある")
        role_hint = "穴"
    if not result.risk_flag and (result.prediction_rank or 99) <= 2 and (result.estimated_in3_rate or 0) >= 50:
        adjust += 2
        reasons.append("Python上位かつ馬券内率が高く相手以上に扱える")
        role_hint = "軸"
    if not result.risk_flag and (result.win_odds or 0) >= 10 and (result.expected_value or 0) >= 0.9:
        adjust += 1
        reasons.append("単勝オッズに対して期待値面の妙味が残る")
        role_hint = "穴" if role_hint == "押さえ" else role_hint

    adjust = max(-8.0, min(5.0, adjust))
    action = "上げ" if adjust > 0 else "下げ" if adjust < 0 else "据え置き"
    if not reasons:
        reasons.append("Python数値は尊重するが、AI逆張り視点で大きな補正材料は限定的")
    if not risks:
        risks.append("展開・馬場・位置取り次第で評価が変動")
    confidence = round(max(0.45, min(0.85, 0.58 + abs(adjust) * 0.04)), 2)
    return _HorseOpinion(
        prediction=result,
        adjust=adjust,
        action=action,
        reason=" / ".join(reasons),
        risk=" / ".join(risks),
        role_hint=role_hint,
        confidence=confidence,
    )


def _replace_opinion(opinion: _HorseOpinion, adjust: float, action: str, extra_reason: str) -> _HorseOpinion:
    return _HorseOpinion(
        prediction=opinion.prediction,
        adjust=adjust,
        action=action,
        reason=f"{opinion.reason} / {extra_reason}",
        risk=opinion.risk,
        role_hint=opinion.role_hint,
        confidence=opinion.confidence,
    )


def _rank_final_predictions(opinions: list[_HorseOpinion]) -> list[tuple[int, _HorseOpinion]]:
    sorted_rows = sorted(
        opinions,
        key=lambda opinion: (
            -((opinion.prediction.prediction_score or 0.0) + opinion.adjust),
            opinion.prediction.prediction_rank or 999,
            opinion.prediction.horse_no,
        ),
    )
    return [(index + 1, opinion) for index, opinion in enumerate(sorted_rows)]


def _role_for_final_rank(final_rank: int, opinion: _HorseOpinion) -> str:
    if opinion.role_hint == "消し" or opinion.adjust <= -5:
        return "消し"
    if final_rank == 1 and opinion.adjust >= 0:
        return "軸"
    if final_rank <= 3:
        return "相手本線"
    if opinion.role_hint == "穴" or opinion.adjust >= 3:
        return "穴"
    if final_rank <= 7:
        return "押さえ"
    return "消し"


def _request_payload(db: Session, race_id: str, results: list[PredictionResult]) -> dict:
    race = db.get(Race, race_id)
    return {
        "race_id": race_id,
        "race_info": {
            "place": race.venue if race else None,
            "course": race.course if race else None,
            "track_condition": race.track_condition if race else None,
            "horse_count": race.headcount if race else len(results),
        },
        "horses": [
            {
                "horse_number": result.horse_no,
                "horse_name": result.horse_name,
                "python_rank": result.prediction_rank,
                "python_score": result.prediction_score,
                "estimated_place_rate": result.estimated_in3_rate,
                "expected_value": result.expected_value,
                "popularity": result.popularity,
                "win_odds": result.win_odds,
                "place_odds": result.place_odds,
                "danger_flag": result.risk_flag,
            }
            for result in results
        ],
    }


def _response_payload(
    db: Session,
    race_id: str,
    opinions: list[_HorseOpinion],
    final_rows: list[tuple[int, _HorseOpinion]],
) -> dict:
    race = db.get(Race, race_id)
    upgrades = [opinion for opinion in opinions if opinion.adjust > 0]
    downgrades = [opinion for opinion in opinions if opinion.adjust < 0]
    dangerous = [
        opinion
        for opinion in opinions
        if opinion.prediction.risk_flag or ((opinion.prediction.popularity or 99) <= 3 and opinion.adjust < 0)
    ]
    values = [
        opinion
        for opinion in opinions
        if opinion.adjust > 0 and ((opinion.prediction.popularity or 0) >= 5 or (opinion.prediction.win_odds or 0) >= 10)
    ]
    return {
        "race_summary": _race_summary_text(race, upgrades, downgrades),
        "pace_prediction": _pace_prediction(race),
        "python_trust_level": _python_trust_level(downgrades),
        "ai_top_horses": [
            {
                "horse_number": opinion.prediction.horse_no,
                "horse_name": opinion.prediction.horse_name,
                "python_rank": opinion.prediction.prediction_rank,
                "ai_rank": final_rank,
                "action": opinion.action,
                "ai_adjust_score": opinion.adjust,
                "reason": opinion.reason,
                "risk": opinion.risk,
                "bet_role": _role_for_final_rank(final_rank, opinion),
                "confidence": opinion.confidence,
            }
            for final_rank, opinion in final_rows[:8]
        ],
        "upgrade_horses": [_simple_horse_payload(opinion) for opinion in upgrades],
        "downgrade_horses": [_simple_horse_payload(opinion) for opinion in downgrades],
        "dangerous_favorites": [_simple_horse_payload(opinion) for opinion in dangerous],
        "value_horses": [_simple_horse_payload(opinion) for opinion in values],
    }


def _simple_horse_payload(opinion: _HorseOpinion) -> dict:
    return {
        "horse_number": opinion.prediction.horse_no,
        "horse_name": opinion.prediction.horse_name,
        "reason": opinion.reason,
    }


def _race_summary_text(
    race: Race | None,
    upgrades: list[_HorseOpinion],
    downgrades: list[_HorseOpinion],
) -> str:
    race_label = f"{race.venue}{race.race_number}R" if race and race.venue and race.race_number else "対象レース"
    return (
        f"{race_label}はPython上位をそのまま固定せず、"
        f"上げ候補{len(upgrades)}頭・下げ候補{len(downgrades)}頭を分けて確認する。"
    )


def _pace_prediction(race: Race | None) -> str:
    course = race.course if race else ""
    if course and "1200" in course:
        return "ややハイペース想定"
    if course and ("2600" in course or "2000" in course):
        return "スローペース寄り想定"
    return "展開は標準想定"


def _python_trust_level(downgrades: list[_HorseOpinion]) -> str:
    severe = sum(1 for opinion in downgrades if opinion.adjust <= -5)
    if severe >= 2:
        return "低い"
    if downgrades:
        return "普通"
    return "高い"


def _target_second_opinion_runs(
    db: Session,
    race_date: date | None,
    race_id: str | None,
    ai_run_id: str | None,
) -> list[AiPredictionRun]:
    if ai_run_id:
        run = db.get(AiPredictionRun, ai_run_id)
        return [run] if run is not None else []

    stmt = (
        select(AiPredictionRun)
        .where(AiPredictionRun.ai_mode == "ai_second_opinion", AiPredictionRun.status == "success")
        .order_by(AiPredictionRun.created_at.desc())
    )
    if race_date:
        stmt = stmt.where(AiPredictionRun.race_date == race_date)
    if race_id:
        stmt = stmt.where(AiPredictionRun.race_id == race_id)
    runs = list(db.scalars(stmt))
    latest_by_race: dict[str, AiPredictionRun] = {}
    for run in runs:
        latest_by_race.setdefault(run.race_id, run)
    return list(latest_by_race.values())


def _build_ai_strategy(run: AiPredictionRun, finals: list[FinalPrediction]) -> AiBetStrategy:
    usable = [final for final in finals if final.final_bet_role != "消し"]
    axis = [final.horse_no for final in usable if final.final_bet_role == "軸"][:1]
    if not axis and usable:
        axis = [usable[0].horse_no]
    second = [
        final.horse_no
        for final in usable
        if final.horse_no not in axis and final.final_bet_role in {"相手本線", "軸"}
    ][:2]
    main = [
        final.horse_no
        for final in usable
        if final.horse_no not in axis + second and final.final_bet_role in {"相手本線", "押さえ"}
    ][:4]
    wide = [
        final.horse_no
        for final in usable
        if final.horse_no not in axis + second + main and final.final_bet_role in {"押さえ", "穴"}
    ][:4]
    excluded = [final.horse_no for final in finals if final.final_bet_role == "消し"]
    all_opponents = second + main + wide
    bet_count = len(_strategy_combinations(axis, all_opponents))
    suggestion = (
        f"AI補正後は{axis[0]}番を軸候補に、"
        f"{'-'.join(str(no) for no in all_opponents[:6]) or '相手不足'}を相手にする。"
    ) if axis else "軸候補不足のため見送り寄り"
    return AiBetStrategy(
        ai_run_id=run.id,
        race_id=run.race_id,
        strategy_type="3連複",
        main_axis=axis,
        second_axis_candidates=second,
        main_opponents=main,
        wide_opponents=wide,
        exclude_candidates=excluded,
        suggestion=suggestion,
        bet_count=bet_count,
    )


def _build_ai_bet_candidate(
    run: AiPredictionRun,
    finals: list[FinalPrediction],
    strategy: AiBetStrategy,
    stake_per_point: int,
    max_race_amount: int,
    max_day_amount: int,
    current_day_total: int,
) -> BetCandidate:
    opponents = (strategy.second_axis_candidates or []) + (strategy.main_opponents or []) + (strategy.wide_opponents or [])
    combo_payload = _strategy_combinations(strategy.main_axis or [], opponents)
    total_amount = len(combo_payload) * stake_per_point
    top = finals[0]
    rank = _bet_rank(top.final_score)
    status = "candidate" if combo_payload else "skipped"
    skip_reason = None
    if not combo_payload:
        skip_reason = "AI補正後の軸または相手候補が不足"
    elif run.python_trust_level == "低い":
        status = "skipped"
        skip_reason = "AIがPython信頼度を低いと判断"
    elif total_amount > max_race_amount:
        status = "blocked"
        skip_reason = f"1レース上限 {max_race_amount:,}円 を超過"
    elif current_day_total + total_amount > max_day_amount:
        status = "blocked"
        skip_reason = f"1日上限 {max_day_amount:,}円 を超過"
    return BetCandidate(
        prediction_run_id=run.python_prediction_run_id or run.id,
        source_type="legacy_ai",
        race_id=run.race_id,
        race_date=run.race_date,
        rank=rank,
        status=status,
        bet_type="3連複",
        strategy="AI補正 3連複フォーメーション",
        strategy_mode="formation",
        bet_rule_version="legacy-ai-bet-v1",
        axis_horse_nos=strategy.main_axis or [],
        opponent_horse_nos=opponents,
        combinations=combo_payload,
        points=len(combo_payload),
        stake_per_point=stake_per_point,
        total_amount=total_amount,
        max_race_amount=max_race_amount,
        max_day_amount=max_day_amount,
        expected_value=None,
        reason=f"AI補正run={run.id}: {strategy.suggestion}",
        skip_reason=skip_reason,
        warning_codes=["AUTOMATIC_PURCHASE_DISABLED", "LEGACY_AI_COMPATIBILITY"],
        requires_confirmation=True,
        purchase_execution_enabled=False,
    )


def _strategy_combinations(axis: list[int], opponents: list[int]) -> list[list[int]]:
    if not axis:
        return []
    unique_opponents = [horse_no for horse_no in dict.fromkeys(opponents) if horse_no not in axis]
    if len(axis) >= 2:
        return [[axis[0], axis[1], opponent] for opponent in unique_opponents]
    return [[axis[0], left, right] for left, right in combinations(unique_opponents, 2)]


def _bet_rank(score: float) -> str:
    if score >= 62:
        return "S"
    if score >= 56:
        return "A"
    if score >= 45:
        return "B"
    return "SKIP"


def _current_day_bet_total(db: Session, race_date: date | None) -> int:
    if race_date is None:
        return 0
    return int(
        db.scalar(
            select(func.coalesce(func.sum(BetCandidate.total_amount), 0)).where(
                BetCandidate.race_date == race_date,
                BetCandidate.status.in_(("candidate", "planned", "purchased", "awaiting_result")),
            )
        )
        or 0
    )
