from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from itertools import combinations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.db.models import (
    BetCandidate,
    BetSettlement,
    AiAnalysis,
    AiAnalysisOutput,
    PredictionResult,
    PredictionRun,
    Race,
    RaceResult,
    ReviewNote,
)
from app.schemas.api import (
    AnalyticsSummaryRead,
    AnalyticsBreakdownRead,
    BetGenerationSummary,
    BetStatusUpdate,
    RaceResultCreate,
    ReviewNoteCreate,
)
from app.schemas.ai_integration import IntegrationResponse
from app.legacy_bridge.normalization import normalize_horse_name
from app.services.ai_independent import payload_sha256
from app.services.history import record_bet_status_change


BET_RULE_VERSION = "bet-rules-v1.0.0"
SUPPORTED_BET_SOURCES = {"python", "ai_integrated"}
SUPPORTED_BET_TYPES = {"3連複", "ワイド"}
SUPPORTED_STRATEGY_MODES = {"formation", "box", "wheel"}


@dataclass(frozen=True)
class _BetSource:
    source_type: str
    results: list[PredictionResult]
    source_snapshot_hash: str
    ai_analysis_id: str | None = None
    manual_review_required: bool = False
    integrated_score_by_horse: dict[int, float] | None = None


BET_STATUSES = {
    "draft",
    "candidate",
    "planned",
    "purchased",
    "awaiting_result",
    "settled",
    "skipped",
    "blocked",
    "cancelled",
    "review_required",
}

ALLOWED_BET_TRANSITIONS = {
    "draft": {"candidate", "skipped", "blocked"},
    "candidate": {"review_required", "planned", "skipped", "cancelled", "blocked"},
    "review_required": {"planned", "skipped", "cancelled"},
    "planned": {"purchased", "skipped", "cancelled"},
    "purchased": {"awaiting_result", "settled"},
    "awaiting_result": {"settled"},
    "settled": set(),
    "skipped": set(),
    "cancelled": set(),
    "blocked": set(),
}


def generate_bet_candidates(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
    prediction_run_id: str | None = None,
    stake_per_point: int = 500,
    max_race_amount: int = 3000,
    max_day_amount: int = 12000,
    source_modes: list[str] | None = None,
    bet_types: list[str] | None = None,
    strategy_modes: list[str] | None = None,
    ai_analysis_id: str | None = None,
    max_points: int = 20,
    allow_manual_review: bool = False,
) -> BetGenerationSummary:
    """Generate bounded previews from explicitly separated Python/AI sources."""

    resolved_sources = source_modes or ["python"]
    resolved_bet_types = bet_types or ["3連複"]
    resolved_strategy_modes = strategy_modes or ["formation"]
    _validate_plan_options(
        source_modes=resolved_sources,
        bet_types=resolved_bet_types,
        strategy_modes=resolved_strategy_modes,
        stake_per_point=stake_per_point,
        max_race_amount=max_race_amount,
        max_day_amount=max_day_amount,
        max_points=max_points,
    )

    results_by_race = _latest_prediction_results_by_race(
        db,
        race_date=race_date,
        race_id=race_id,
        prediction_run_id=prediction_run_id,
    )
    summary = BetGenerationSummary(
        race_date=race_date,
        race_id=race_id,
        prediction_run_id=prediction_run_id,
    )
    day_total = _planned_total_for_date(db, race_date)

    for result_race_id, results in sorted(results_by_race.items()):
        if not results:
            continue
        sources, source_warnings = _bet_sources(
            db,
            results=results,
            source_modes=resolved_sources,
            ai_analysis_id=ai_analysis_id,
        )
        summary.warnings.extend(source_warnings)
        for source in sources:
            for bet_type in resolved_bet_types:
                for strategy_mode in resolved_strategy_modes:
                    strategy = _strategy_label(source.source_type, bet_type, strategy_mode)
                    existing_candidate = db.scalar(
                        select(BetCandidate)
                        .where(
                            BetCandidate.prediction_run_id == results[0].prediction_run_id,
                            BetCandidate.race_id == result_race_id,
                            BetCandidate.source_type == source.source_type,
                            BetCandidate.bet_type == bet_type,
                            BetCandidate.strategy == strategy,
                        )
                        .order_by(BetCandidate.created_at.desc())
                        .limit(1)
                    )
                    if existing_candidate is not None:
                        _count_candidate(summary, existing_candidate)
                        continue
                    candidate = _build_candidate(
                        db=db,
                        source=source,
                        bet_type=bet_type,
                        strategy_mode=strategy_mode,
                        stake_per_point=stake_per_point,
                        max_race_amount=max_race_amount,
                        max_day_amount=max_day_amount,
                        max_points=max_points,
                        current_day_total=day_total,
                        allow_manual_review=allow_manual_review,
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
                    _count_candidate(summary, candidate)
                    if candidate.status == "candidate":
                        day_total += candidate.total_amount

    db.commit()
    return summary


def update_bet_status(db: Session, bet_id: int, payload: BetStatusUpdate) -> BetCandidate:
    """Update manual purchase workflow status for one bet candidate."""

    if payload.status not in BET_STATUSES:
        raise ValueError(f"unsupported bet status: {payload.status}")
    candidate = db.get(BetCandidate, bet_id)
    if candidate is None:
        raise LookupError("bet candidate not found")

    old_status = candidate.status
    if payload.status == old_status:
        return candidate
    allowed_targets = ALLOWED_BET_TRANSITIONS.get(old_status, set())
    if payload.status not in allowed_targets:
        raise ValueError(f"invalid bet status transition: {old_status} -> {payload.status}")

    candidate.status = payload.status
    if payload.reason:
        if payload.status in {"skipped", "blocked"}:
            candidate.skip_reason = payload.reason
        else:
            candidate.reason = payload.reason
    candidate.updated_at = datetime.utcnow()
    db.add(candidate)
    record_bet_status_change(
        db,
        bet_candidate_id=candidate.id,
        old_status=old_status,
        new_status=payload.status,
        reason=payload.reason,
        prediction_run_id=candidate.prediction_run_id,
        changed_by="local_user",
    )
    db.commit()
    db.refresh(candidate)
    return candidate


def upsert_race_result(db: Session, payload: RaceResultCreate) -> RaceResult:
    """Create or update the saved race result and payout information."""

    race_date = payload.race_date
    if race_date is None:
        race = db.get(Race, payload.race_id)
        race_date = race.race_date if race is not None else None

    result = db.scalar(select(RaceResult).where(RaceResult.race_id == payload.race_id))
    if result is None:
        result = RaceResult(race_id=payload.race_id)
        db.add(result)

    result.race_date = race_date
    result.result_status = payload.result_status
    result.finish_order = payload.finish_order
    result.payout_amount = payload.payout_amount
    result.payout_type = payload.payout_type
    result.payouts_json = [item.model_dump(mode="json") for item in payload.payouts]
    result.cancelled_horse_nos = payload.cancelled_horse_nos
    result.disqualified_horse_nos = payload.disqualified_horse_nos
    result.has_dead_heat = payload.has_dead_heat
    result.confirmed_at = (
        datetime.now(timezone.utc) if payload.result_status == "confirmed" else None
    )
    result.source_file = payload.source_file
    result.raw = payload.raw
    result.imported_at = datetime.utcnow()
    db.commit()
    db.refresh(result)
    return result


def settle_bets_for_race(db: Session, race_id: str) -> list[BetSettlement]:
    """Settle bet candidates for one race using the saved result."""

    result = db.scalar(select(RaceResult).where(RaceResult.race_id == race_id))
    if result is None:
        raise LookupError("race result not found")
    if result.result_status == "provisional":
        raise ValueError("暫定結果では精算できません")

    payout_items = list(result.payouts_json or [])
    if not payout_items and result.payout_amount and len(result.finish_order or []) >= 3:
        # Phase 7以前の単一3連複払戻を構造化形式へ読み替える後方互換。
        payout_items = [
            {
                "bet_type": result.payout_type,
                "combination": sorted((result.finish_order or [])[:3]),
                "payout_per_100": result.payout_amount,
                "status": "normal",
                "legacy_total": True,
            }
        ]
    if result.result_status == "cancelled" and not payout_items:
        raise ValueError("中止レースの精算には返還情報が必要です")
    payout_by_ticket = {
        (item["bet_type"], tuple(sorted(item["combination"]))): item
        for item in payout_items
    }

    candidates = list(
        db.scalars(
            select(BetCandidate)
            .where(
                BetCandidate.race_id == race_id,
                BetCandidate.points > 0,
                BetCandidate.status.not_in(("skipped", "blocked")),
            )
            .order_by(BetCandidate.created_at.desc())
        )
    )
    settlements: list[BetSettlement] = []
    for candidate in candidates:
        combinations_payload = candidate.combinations or []
        matched_details: list[dict] = []
        for combo in combinations_payload:
            normalized_combo = tuple(sorted(combo))
            payout = payout_by_ticket.get((candidate.bet_type, normalized_combo))
            if payout is None:
                continue
            paid = (
                int(payout["payout_per_100"])
                if payout.get("legacy_total")
                else int(payout["payout_per_100"] * candidate.stake_per_point / 100)
            )
            matched_details.append(
                {
                    "bet_type": candidate.bet_type,
                    "combination": list(normalized_combo),
                    "payout_per_100": payout["payout_per_100"],
                    "stake_amount": candidate.stake_per_point,
                    "paid_amount": paid,
                    "status": payout.get("status", "normal"),
                }
            )
        hit_count = len(matched_details)
        is_hit = hit_count > 0
        payout_amount = sum(item["paid_amount"] for item in matched_details)
        stake_amount = candidate.total_amount
        profit_loss = payout_amount - stake_amount
        roi = round((payout_amount / stake_amount) * 100, 2) if stake_amount else 0.0
        settlement = db.scalar(
            select(BetSettlement).where(BetSettlement.bet_candidate_id == candidate.id)
        )
        if settlement is None:
            settlement = BetSettlement(
                bet_candidate_id=candidate.id,
                race_id=candidate.race_id,
            )
            db.add(settlement)

        settlement.race_date = candidate.race_date
        settlement.bet_type = candidate.bet_type
        settlement.source_type = candidate.source_type
        settlement.is_hit = is_hit
        settlement.hit_count = hit_count
        settlement.winning_combinations = [item["combination"] for item in matched_details]
        settlement.payout_details_json = matched_details
        settlement.result_status = result.result_status
        settlement.payout_amount = payout_amount
        settlement.stake_amount = stake_amount
        settlement.profit_loss = profit_loss
        settlement.roi = roi
        settlement.message = "的中" if is_hit else "不的中"
        settlement.settled_at = datetime.utcnow()
        old_status = candidate.status
        candidate.status = "settled"
        candidate.updated_at = datetime.utcnow()
        record_bet_status_change(
            db,
            bet_candidate_id=candidate.id,
            old_status=old_status,
            new_status="settled",
            reason="結果取込による自動精算",
            prediction_run_id=candidate.prediction_run_id,
        )
        _upsert_auto_review_note(db, candidate, result, settlement)
        settlements.append(settlement)

    db.commit()
    for settlement in settlements:
        db.refresh(settlement)
    return settlements


def create_review_note(db: Session, payload: ReviewNoteCreate) -> ReviewNote:
    """Create a manual review note."""

    note = ReviewNote(
        race_id=payload.race_id,
        race_date=payload.race_date,
        bet_candidate_id=payload.bet_candidate_id,
        prediction_run_id=payload.prediction_run_id,
        note=payload.note,
        ai_vs_result=payload.ai_vs_result,
    )
    db.add(note)
    db.commit()
    db.refresh(note)
    return note


def analytics_summary(
    db: Session,
    race_date: date | None = None,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    source_type: str | None = None,
    bet_type: str | None = None,
    venue: str | None = None,
    course: str | None = None,
    race_class: str | None = None,
    prediction_model: str | None = None,
    ai_model: str | None = None,
    group_by: list[str] | None = None,
) -> AnalyticsSummaryRead:
    """Summarize filtered performance and source/condition breakdowns."""

    if race_date is not None:
        date_from = race_date
        date_to = race_date
    all_candidates = list(db.scalars(select(BetCandidate).order_by(BetCandidate.created_at)))
    context_by_candidate: dict[int, dict[str, str]] = {}
    candidates: list[BetCandidate] = []
    for candidate in all_candidates:
        race = db.get(Race, candidate.race_id)
        run = db.get(PredictionRun, candidate.prediction_run_id)
        analysis = db.get(AiAnalysis, candidate.ai_analysis_id) if candidate.ai_analysis_id else None
        context = {
            "source_type": candidate.source_type,
            "bet_type": candidate.bet_type,
            "venue": race.venue if race and race.venue else "不明",
            "course": race.course if race and race.course else "不明",
            "race_class": race.race_class if race and race.race_class else "不明",
            "prediction_model": run.model_version if run else "不明",
            "ai_model": analysis.model_name if analysis else "対象外",
        }
        if date_from and (candidate.race_date is None or candidate.race_date < date_from):
            continue
        if date_to and (candidate.race_date is None or candidate.race_date > date_to):
            continue
        filters = {
            "source_type": source_type,
            "bet_type": bet_type,
            "venue": venue,
            "course": course,
            "race_class": race_class,
            "prediction_model": prediction_model,
            "ai_model": ai_model,
        }
        if any(value is not None and context[key] != value for key, value in filters.items()):
            continue
        candidates.append(candidate)
        context_by_candidate[candidate.id] = context

    candidate_ids = {candidate.id for candidate in candidates}
    settlements = [
        settlement
        for settlement in db.scalars(select(BetSettlement).order_by(BetSettlement.settled_at))
        if settlement.bet_candidate_id in candidate_ids
    ]
    stake_amount = sum(settlement.stake_amount for settlement in settlements)
    payout_amount = sum(settlement.payout_amount for settlement in settlements)
    profit_loss = payout_amount - stake_amount
    roi = round((payout_amount / stake_amount) * 100, 2) if stake_amount else 0.0
    hit_rate = round((sum(1 for item in settlements if item.is_hit) / len(settlements)) * 100, 2) if settlements else 0.0
    losing_streak = 0
    max_losing_streak = 0
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for settlement in settlements:
        losing_streak = 0 if settlement.is_hit else losing_streak + 1
        max_losing_streak = max(max_losing_streak, losing_streak)
        cumulative += settlement.profit_loss
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    settlement_by_candidate = {item.bet_candidate_id: item for item in settlements}
    breakdown: list[AnalyticsBreakdownRead] = []
    dimensions = group_by or ["source_type", "bet_type"]
    supported_dimensions = {
        "source_type", "bet_type", "venue", "course", "race_class", "prediction_model", "ai_model"
    }
    for dimension in dimensions:
        if dimension not in supported_dimensions:
            raise ValueError(f"unsupported analytics group: {dimension}")
        groups: dict[str, list[BetCandidate]] = defaultdict(list)
        for candidate in candidates:
            groups[context_by_candidate[candidate.id][dimension]].append(candidate)
        for value, group_candidates in sorted(groups.items()):
            group_settlements = [
                settlement_by_candidate[candidate.id]
                for candidate in group_candidates
                if candidate.id in settlement_by_candidate
            ]
            group_stake = sum(item.stake_amount for item in group_settlements)
            group_payout = sum(item.payout_amount for item in group_settlements)
            group_hits = sum(1 for item in group_settlements if item.is_hit)
            breakdown.append(
                AnalyticsBreakdownRead(
                    dimension=dimension,
                    value=value,
                    bets=len(group_candidates),
                    settled_bets=len(group_settlements),
                    hits=group_hits,
                    hit_rate=round(group_hits / len(group_settlements) * 100, 2) if group_settlements else 0,
                    stake_amount=group_stake,
                    payout_amount=group_payout,
                    profit_loss=group_payout - group_stake,
                    roi=round(group_payout / group_stake * 100, 2) if group_stake else 0,
                )
            )
    return AnalyticsSummaryRead(
        race_date=race_date,
        date_from=date_from,
        date_to=date_to,
        bets=len(candidates),
        settled_bets=len(settlements),
        hits=sum(1 for settlement in settlements if settlement.is_hit),
        hit_rate=hit_rate,
        stake_amount=stake_amount,
        payout_amount=payout_amount,
        profit_loss=profit_loss,
        roi=roi,
        max_consecutive_losses=max_losing_streak,
        max_drawdown=max_drawdown,
        breakdown=breakdown,
    )


def _latest_prediction_results_by_race(
    db: Session,
    race_date: date | None,
    race_id: str | None,
    prediction_run_id: str | None,
) -> dict[str, list[PredictionResult]]:
    if prediction_run_id is not None:
        stmt = select(PredictionResult).where(PredictionResult.prediction_run_id == prediction_run_id)
        if race_id is not None:
            stmt = stmt.where(PredictionResult.race_id == race_id)
        results = list(db.scalars(stmt.order_by(PredictionResult.race_id, PredictionResult.prediction_rank)))
        return _group_results_by_race(results)

    run_stmt = select(PredictionRun).where(PredictionRun.status == "completed")
    if race_date is not None:
        run_stmt = run_stmt.where(PredictionRun.race_date == race_date)
    if race_id is not None:
        run_stmt = run_stmt.where(or_(PredictionRun.race_id == race_id, PredictionRun.race_id.is_(None)))
    runs = list(db.scalars(run_stmt.order_by(PredictionRun.finished_at.desc(), PredictionRun.created_at.desc())))

    results_by_race: dict[str, list[PredictionResult]] = {}
    for run in runs:
        stmt = select(PredictionResult).where(PredictionResult.prediction_run_id == run.id)
        if race_id is not None:
            stmt = stmt.where(PredictionResult.race_id == race_id)
        results = list(db.scalars(stmt.order_by(PredictionResult.race_id, PredictionResult.prediction_rank)))
        for result_race_id, race_results in _group_results_by_race(results).items():
            if result_race_id not in results_by_race:
                results_by_race[result_race_id] = race_results
    return results_by_race


def _group_results_by_race(results: list[PredictionResult]) -> dict[str, list[PredictionResult]]:
    grouped: dict[str, list[PredictionResult]] = {}
    for result in results:
        grouped.setdefault(result.race_id, []).append(result)
    for race_results in grouped.values():
        race_results.sort(key=lambda item: (item.prediction_rank or 999, item.horse_no))
    return grouped


def _validate_plan_options(
    *,
    source_modes: list[str],
    bet_types: list[str],
    strategy_modes: list[str],
    stake_per_point: int,
    max_race_amount: int,
    max_day_amount: int,
    max_points: int,
) -> None:
    if not source_modes or set(source_modes) - SUPPORTED_BET_SOURCES:
        raise ValueError("source_modesに未対応の値があります")
    if not bet_types or set(bet_types) - SUPPORTED_BET_TYPES:
        raise ValueError("bet_typesに未対応の値があります")
    if not strategy_modes or set(strategy_modes) - SUPPORTED_STRATEGY_MODES:
        raise ValueError("strategy_modesに未対応の値があります")
    if any(len(values) != len(set(values)) for values in (source_modes, bet_types, strategy_modes)):
        raise ValueError("買い目生成optionに重複があります")
    if stake_per_point < 100 or stake_per_point % 100 != 0:
        raise ValueError("1点金額は100円以上かつ100円単位にしてください")
    if max_race_amount < stake_per_point or max_day_amount < max_race_amount:
        raise ValueError("資金上限は 1点金額 <= 1レース上限 <= 1日上限 にしてください")
    if max_points < 1:
        raise ValueError("最大点数は1以上にしてください")


def _bet_sources(
    db: Session,
    *,
    results: list[PredictionResult],
    source_modes: list[str],
    ai_analysis_id: str | None,
) -> tuple[list[_BetSource], list[str]]:
    sources: list[_BetSource] = []
    warnings: list[str] = []
    prediction_run_id = results[0].prediction_run_id
    race_id = results[0].race_id
    if "python" in source_modes:
        python_payload = {
            "schema_version": "python_bet_source_v1",
            "prediction_run_id": prediction_run_id,
            "race_id": race_id,
            "results": [
                {
                    "horse_no": result.horse_no,
                    "horse_name": result.horse_name,
                    "prediction_rank": result.prediction_rank,
                    "prediction_score": result.prediction_score,
                    "expected_value": result.expected_value,
                    "risk_flag": result.risk_flag,
                }
                for result in results
            ],
        }
        sources.append(
            _BetSource(
                source_type="python",
                results=results,
                source_snapshot_hash=payload_sha256(python_payload),
            )
        )

    if "ai_integrated" not in source_modes:
        return sources, warnings

    stmt = (
        select(AiAnalysis, AiAnalysisOutput)
        .join(AiAnalysisOutput, AiAnalysisOutput.analysis_id == AiAnalysis.id)
        .where(
            AiAnalysis.race_id == race_id,
            AiAnalysis.prediction_run_id == prediction_run_id,
            AiAnalysis.status == "succeeded",
            AiAnalysisOutput.stage == "integration",
            AiAnalysisOutput.is_locked.is_(True),
            AiAnalysisOutput.python_result_visible.is_(True),
        )
    )
    if ai_analysis_id is not None:
        stmt = stmt.where(AiAnalysis.id == ai_analysis_id)
    row = db.execute(
        stmt.order_by(AiAnalysis.analysis_sequence.desc()).limit(1)
    ).first()
    if row is None:
        warnings.append(f"{race_id}: 固定済みAI統合結果がないためAI統合案を作成しません")
        return sources, warnings

    analysis, output = row
    integration = IntegrationResponse.model_validate(output.output_json)
    result_by_horse = {result.horse_no: result for result in results}
    if {horse.horse_no for horse in integration.horses} != set(result_by_horse):
        warnings.append(f"{race_id}: AI統合結果とPython予想の出走馬集合が違うためAI統合案を停止")
        return sources, warnings

    ordered_results: list[PredictionResult] = []
    score_by_horse: dict[int, float] = {}
    for horse in sorted(integration.horses, key=lambda item: item.integrated_rank):
        result = result_by_horse[horse.horse_no]
        if normalize_horse_name(horse.horse_name) != normalize_horse_name(result.horse_name):
            warnings.append(f"{race_id}: AI統合結果の馬番・馬名不一致によりAI統合案を停止")
            return sources, warnings
        if horse.integrated_score is None:
            warnings.append(f"{race_id}: 旧統合結果に統合scoreがないためAI統合案を停止")
            return sources, warnings
        ordered_results.append(result)
        score_by_horse[horse.horse_no] = horse.integrated_score

    sources.append(
        _BetSource(
            source_type="ai_integrated",
            results=ordered_results,
            source_snapshot_hash=output.output_hash,
            ai_analysis_id=analysis.id,
            manual_review_required=integration.manual_review_required,
            integrated_score_by_horse=score_by_horse,
        )
    )
    return sources, warnings


def _build_combinations(
    ordered_horse_nos: list[int],
    *,
    bet_type: str,
    strategy_mode: str,
) -> tuple[list[int], list[int], list[list[int]]]:
    if bet_type == "3連複" and strategy_mode == "formation":
        axes = ordered_horse_nos[:2]
        opponents = ordered_horse_nos[2:6]
        combos = [[axes[0], axes[1], opponent] for opponent in opponents] if len(axes) == 2 else []
    elif bet_type == "3連複" and strategy_mode == "wheel":
        axes = ordered_horse_nos[:1]
        opponents = ordered_horse_nos[1:5]
        combos = [[axes[0], left, right] for left, right in combinations(opponents, 2)] if axes else []
    elif bet_type == "3連複":
        axes = []
        opponents = ordered_horse_nos[:5]
        combos = [list(combo) for combo in combinations(opponents, 3)]
    elif bet_type == "ワイド" and strategy_mode == "formation":
        axes = ordered_horse_nos[:2]
        opponents = ordered_horse_nos[2:5]
        combos = [[axis, opponent] for axis in axes for opponent in opponents]
    elif bet_type == "ワイド" and strategy_mode == "wheel":
        axes = ordered_horse_nos[:1]
        opponents = ordered_horse_nos[1:5]
        combos = [[axes[0], opponent] for opponent in opponents] if axes else []
    else:
        axes = []
        opponents = ordered_horse_nos[:5]
        combos = [list(combo) for combo in combinations(opponents, 2)]

    normalized = sorted({tuple(sorted(combo)) for combo in combos})
    return axes, opponents, [list(combo) for combo in normalized]


def _source_label(source_type: str) -> str:
    return {"python": "Python案", "ai_integrated": "AI統合案"}.get(source_type, source_type)


def _strategy_label(source_type: str, bet_type: str, strategy_mode: str) -> str:
    mode_label = {
        "formation": "2頭軸フォーメーション" if bet_type == "ワイド" else "2頭軸流し",
        "wheel": "1頭軸流し",
        "box": "5頭BOX",
    }[strategy_mode]
    return f"{_source_label(source_type)} {bet_type} {mode_label}"


def _count_candidate(summary: BetGenerationSummary, candidate: BetCandidate) -> None:
    summary.generated += 1
    if candidate.status == "candidate":
        summary.candidates += 1
        summary.total_planned_amount += candidate.total_amount
    elif candidate.status == "blocked":
        summary.blocked += 1
    elif candidate.status == "review_required":
        summary.review_required += 1
    else:
        summary.skipped += 1


def _build_candidate(
    db: Session,
    source: _BetSource,
    bet_type: str,
    strategy_mode: str,
    stake_per_point: int,
    max_race_amount: int,
    max_day_amount: int,
    max_points: int,
    current_day_total: int,
    allow_manual_review: bool,
) -> BetCandidate:
    results = source.results
    race_id = results[0].race_id
    prediction_run_id = results[0].prediction_run_id
    prediction_run = db.get(PredictionRun, prediction_run_id)
    race = db.get(Race, race_id)
    race_date = prediction_run.race_date if prediction_run is not None else race.race_date if race else None
    safe_results = [result for result in results if not result.risk_flag]
    risk_count = len(results) - len(safe_results)
    rank_target = safe_results[0] if safe_results else results[0]
    rank = _candidate_rank(rank_target)

    minimum_runners = 3 if bet_type == "3連複" else 2
    if len(results) < minimum_runners:
        return _skip_candidate(
            prediction_run_id=prediction_run_id,
            race_id=race_id,
            race_date=race_date,
            source=source,
            bet_type=bet_type,
            strategy_mode=strategy_mode,
            rank="SKIP",
            stake_per_point=stake_per_point,
            max_race_amount=max_race_amount,
            max_day_amount=max_day_amount,
            reason=f"{bet_type}に必要な出走頭数が不足しているため見送り",
        )
    if rank == "SKIP" or len(safe_results) < minimum_runners:
        return _skip_candidate(
            prediction_run_id=prediction_run_id,
            race_id=race_id,
            race_date=race_date,
            source=source,
            bet_type=bet_type,
            strategy_mode=strategy_mode,
            rank=rank,
            stake_per_point=stake_per_point,
            max_race_amount=max_race_amount,
            max_day_amount=max_day_amount,
            reason="軸候補または相手候補の信頼度が不足",
        )

    axes, opponents, combo_payload = _build_combinations(
        [result.horse_no for result in safe_results],
        bet_type=bet_type,
        strategy_mode=strategy_mode,
    )
    strategy = _strategy_label(source.source_type, bet_type, strategy_mode)

    points = len(combo_payload)
    total_amount = points * stake_per_point
    source_reason = (
        f"統合score={source.integrated_score_by_horse.get(rank_target.horse_no, 0):.2f}、"
        if source.integrated_score_by_horse is not None
        else ""
    )
    reason = (
        f"{_source_label(source.source_type)}で{rank_target.horse_no}番 {rank_target.horse_name} を中心視。"
        f"{source_reason}"
        f"score={rank_target.prediction_score or 0:.2f}、Python買い目ランク={rank}、危険馬除外={risk_count}頭。"
    )
    status = "candidate"
    skip_reason = None
    warning_codes = ["AUTOMATIC_PURCHASE_DISABLED"]
    if source.manual_review_required:
        warning_codes.append("AI_MANUAL_REVIEW_REQUIRED")
        if not allow_manual_review:
            status = "review_required"
            skip_reason = "AI統合結果に重大不一致があるため手動確認が必要"
    if points == 0:
        status = "skipped"
        warning_codes.append("COMBINATION_NOT_AVAILABLE")
        skip_reason = "選択した券種・方式の組合せを作成できません"
    elif points > max_points:
        status = "blocked"
        warning_codes.append("POINT_LIMIT_EXCEEDED")
        skip_reason = f"最大点数 {max_points}点 を超過"
    elif total_amount > max_race_amount:
        status = "blocked"
        warning_codes.append("RACE_BUDGET_EXCEEDED")
        skip_reason = f"1レース上限 {max_race_amount:,}円 を超過"
    elif current_day_total + total_amount > max_day_amount:
        status = "blocked"
        warning_codes.append("DAY_BUDGET_EXCEEDED")
        skip_reason = f"1日上限 {max_day_amount:,}円 を超過"

    return BetCandidate(
        prediction_run_id=prediction_run_id,
        source_type=source.source_type,
        ai_analysis_id=source.ai_analysis_id,
        race_id=race_id,
        race_date=race_date,
        rank=rank,
        status=status,
        bet_type=bet_type,
        strategy=strategy,
        strategy_mode=strategy_mode,
        bet_rule_version=BET_RULE_VERSION,
        axis_horse_nos=axes,
        opponent_horse_nos=opponents,
        combinations=combo_payload,
        points=points,
        stake_per_point=stake_per_point,
        total_amount=total_amount,
        max_race_amount=max_race_amount,
        max_day_amount=max_day_amount,
        expected_value=rank_target.expected_value,
        reason=reason,
        skip_reason=skip_reason,
        warning_codes=warning_codes,
        requires_confirmation=True,
        purchase_execution_enabled=False,
        source_snapshot_hash=source.source_snapshot_hash,
    )


def _candidate_rank(top: PredictionResult) -> str:
    score = top.prediction_score or 0.0
    if top.risk_flag:
        return "SKIP"
    if score >= 62:
        return "S"
    if score >= 56:
        return "A"
    if score >= 45:
        return "B"
    return "SKIP"


def _skip_candidate(
    prediction_run_id: str,
    race_id: str,
    race_date: date | None,
    source: _BetSource,
    bet_type: str,
    strategy_mode: str,
    rank: str,
    stake_per_point: int,
    max_race_amount: int,
    max_day_amount: int,
    reason: str,
) -> BetCandidate:
    return BetCandidate(
        prediction_run_id=prediction_run_id,
        source_type=source.source_type,
        ai_analysis_id=source.ai_analysis_id,
        race_id=race_id,
        race_date=race_date,
        rank=rank,
        status="skipped",
        bet_type=bet_type,
        strategy=_strategy_label(source.source_type, bet_type, strategy_mode),
        strategy_mode=strategy_mode,
        bet_rule_version=BET_RULE_VERSION,
        axis_horse_nos=[],
        opponent_horse_nos=[],
        combinations=[],
        points=0,
        stake_per_point=stake_per_point,
        total_amount=0,
        max_race_amount=max_race_amount,
        max_day_amount=max_day_amount,
        reason=reason,
        skip_reason=reason,
        warning_codes=["AUTOMATIC_PURCHASE_DISABLED", "INSUFFICIENT_CANDIDATES"],
        requires_confirmation=True,
        purchase_execution_enabled=False,
        source_snapshot_hash=source.source_snapshot_hash,
    )


def _planned_total_for_date(db: Session, race_date: date | None) -> int:
    if race_date is None:
        return 0
    return sum(
        db.scalars(
            select(BetCandidate.total_amount).where(
                BetCandidate.race_date == race_date,
                BetCandidate.status.in_(("candidate", "planned", "purchased", "awaiting_result")),
            )
        )
    )


def _upsert_auto_review_note(
    db: Session,
    candidate: BetCandidate,
    result: RaceResult,
    settlement: BetSettlement,
) -> None:
    existing = db.scalar(
        select(ReviewNote).where(
            ReviewNote.bet_candidate_id == candidate.id,
            ReviewNote.race_id == candidate.race_id,
        )
    )
    ai_vs_result = _ai_vs_result_payload(db, candidate, result)
    note_text = (
        f"{candidate.rank}ランク {candidate.strategy} は"
        f"{'的中' if settlement.is_hit else '不的中'}。"
        f"投資{settlement.stake_amount:,}円、払戻{settlement.payout_amount:,}円、"
        f"損益{settlement.profit_loss:,}円。"
    )
    if existing is None:
        existing = ReviewNote(
            race_id=candidate.race_id,
            race_date=candidate.race_date,
            bet_candidate_id=candidate.id,
            prediction_run_id=candidate.prediction_run_id,
            note=note_text,
            ai_vs_result=ai_vs_result,
        )
        db.add(existing)
    else:
        existing.note = note_text
        existing.ai_vs_result = ai_vs_result
        existing.updated_at = datetime.utcnow()


def _ai_vs_result_payload(db: Session, candidate: BetCandidate, result: RaceResult) -> dict:
    predictions = list(
        db.scalars(
            select(PredictionResult)
            .where(
                PredictionResult.prediction_run_id == candidate.prediction_run_id,
                PredictionResult.race_id == candidate.race_id,
            )
            .order_by(PredictionResult.prediction_rank)
        )
    )
    finish_index = {horse_no: index + 1 for index, horse_no in enumerate(result.finish_order or [])}
    return {
        "finish_order": result.finish_order,
        "prediction_top5": [
            {
                "horse_no": prediction.horse_no,
                "horse_name": prediction.horse_name,
                "prediction_rank": prediction.prediction_rank,
                "finish_position": finish_index.get(prediction.horse_no),
            }
            for prediction in predictions[:5]
        ],
    }
