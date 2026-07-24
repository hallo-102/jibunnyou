from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.api.v1.endpoints.jobs import create_job
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    AiBetStrategy,
    AiHorseEvaluation,
    AiPredictionRun,
    FinalPrediction,
    JobRun,
)
from app.schemas.api import (
    AiBetStrategyRead,
    AiComparisonIntegrationRunRequest,
    AiHorseEvaluationRead,
    AiIndependentAnalysisRead,
    AiIndependentRunRequest,
    AiIntegrationAnalysisRead,
    AiPredictionRunRead,
    AiRaceStatusRead,
    AiRunRequest,
    AiRunSummary,
    FinalPredictionRead,
    JobCreate,
    JobRead,
)
from app.services.ai_opinion import run_ai_bet_correction, run_ai_explain, run_ai_second_opinion

router = APIRouter()


@router.post(
    "/ai/independent-analysis",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_independent_analysis(
    payload: AiIndependentRunRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)],
    db: Session = Depends(get_db),
) -> JobRun:
    """Reject the retired API prediction operation while keeping history readable."""

    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail="APIによるAI予想は廃止されました。ChatGPT手動予想を使用してください",
    )


@router.get(
    "/ai/independent-analyses",
    response_model=list[AiIndependentAnalysisRead],
)
def list_independent_analyses(
    race_date: date | None = None,
    race_id: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[AiIndependentAnalysisRead]:
    """List immutable independent analysis history."""

    stmt = select(AiAnalysis).where(AiAnalysis.parent_analysis_id.is_(None))
    if race_date is not None:
        stmt = stmt.where(AiAnalysis.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(AiAnalysis.race_id == race_id)
    stmt = stmt.order_by(AiAnalysis.created_at.desc()).limit(limit)
    return [_independent_detail(db, item) for item in db.scalars(stmt)]


@router.get("/ai/runs/{analysis_id}", response_model=AiIndependentAnalysisRead)
def get_independent_analysis(
    analysis_id: str,
    db: Session = Depends(get_db),
) -> AiIndependentAnalysisRead:
    """Return one independent run and its locked result."""

    analysis = db.get(AiAnalysis, analysis_id)
    if analysis is None or analysis.parent_analysis_id is not None:
        raise HTTPException(status_code=404, detail="independent AI analysis not found")
    return _independent_detail(db, analysis)


@router.get(
    "/races/{race_id}/ai-independent-analysis",
    response_model=AiIndependentAnalysisRead | None,
)
def get_latest_race_independent_analysis(
    race_id: str,
    db: Session = Depends(get_db),
) -> AiIndependentAnalysisRead | None:
    """Return the newest independent result for one race."""

    analysis = db.scalar(
        select(AiAnalysis)
        .where(
            AiAnalysis.race_id == race_id,
            AiAnalysis.parent_analysis_id.is_(None),
        )
        .order_by(AiAnalysis.analysis_sequence.desc())
        .limit(1)
    )
    return _independent_detail(db, analysis) if analysis is not None else None


@router.post(
    "/ai/comparison-integration",
    response_model=JobRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_comparison_integration(
    payload: AiComparisonIntegrationRunRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)],
    db: Session = Depends(get_db),
) -> JobRun:
    """Reject the retired API integration operation while preserving old results."""

    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail="APIによるAI比較・統合は廃止されました。ChatGPT手動予想を使用してください",
    )


@router.get(
    "/ai/integration-analyses",
    response_model=list[AiIntegrationAnalysisRead],
)
def list_integration_analyses(
    race_date: date | None = None,
    race_id: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[AiIntegrationAnalysisRead]:
    """List comparison/integration run history without mutating its parent result."""

    stmt = select(AiAnalysis).where(AiAnalysis.parent_analysis_id.is_not(None))
    if race_date is not None:
        stmt = stmt.where(AiAnalysis.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(AiAnalysis.race_id == race_id)
    stmt = stmt.order_by(AiAnalysis.created_at.desc()).limit(limit)
    return [_integration_detail(db, item) for item in db.scalars(stmt)]


@router.get(
    "/ai/integration-runs/{analysis_id}",
    response_model=AiIntegrationAnalysisRead,
)
def get_integration_analysis(
    analysis_id: str,
    db: Session = Depends(get_db),
) -> AiIntegrationAnalysisRead:
    """Return one comparison/integration run with both immutable output stages."""

    analysis = db.get(AiAnalysis, analysis_id)
    if analysis is None or analysis.parent_analysis_id is None:
        raise HTTPException(status_code=404, detail="AI integration analysis not found")
    return _integration_detail(db, analysis)


@router.get(
    "/races/{race_id}/ai-integration-analysis",
    response_model=AiIntegrationAnalysisRead | None,
)
def get_latest_race_integration_analysis(
    race_id: str,
    db: Session = Depends(get_db),
) -> AiIntegrationAnalysisRead | None:
    """Return the newest comparison/integration run for one race."""

    analysis = db.scalar(
        select(AiAnalysis)
        .where(
            AiAnalysis.race_id == race_id,
            AiAnalysis.parent_analysis_id.is_not(None),
        )
        .order_by(AiAnalysis.analysis_sequence.desc())
        .limit(1)
    )
    return _integration_detail(db, analysis) if analysis is not None else None


@router.get("/ai/runs", response_model=list[AiPredictionRunRead])
def list_ai_runs(
    race_date: date | None = None,
    race_id: str | None = None,
    ai_mode: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[AiPredictionRun]:
    """List AI run history."""

    stmt = select(AiPredictionRun).order_by(AiPredictionRun.created_at.desc()).limit(limit)
    if race_date is not None:
        stmt = stmt.where(AiPredictionRun.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(AiPredictionRun.race_id == race_id)
    if ai_mode is not None:
        stmt = stmt.where(AiPredictionRun.ai_mode == ai_mode)
    return list(db.scalars(stmt))


@router.get("/ai/statuses", response_model=list[AiRaceStatusRead])
def list_ai_statuses(
    race_date: date | None = None,
    db: Session = Depends(get_db),
) -> list[AiRaceStatusRead]:
    """List latest AI second-opinion status per race."""

    stmt = select(AiPredictionRun).where(AiPredictionRun.ai_mode == "ai_second_opinion")
    if race_date is not None:
        stmt = stmt.where(AiPredictionRun.race_date == race_date)
    runs = list(db.scalars(stmt.order_by(AiPredictionRun.created_at.desc())))
    latest_by_race: dict[str, AiPredictionRun] = {}
    for run in runs:
        latest_by_race.setdefault(run.race_id, run)

    statuses: list[AiRaceStatusRead] = []
    for run in latest_by_race.values():
        evaluations = list(
            db.scalars(
                select(AiHorseEvaluation)
                .where(AiHorseEvaluation.ai_run_id == run.id)
                .order_by(AiHorseEvaluation.ai_rank)
            )
        )
        final_count = len(
            list(db.scalars(select(FinalPrediction.id).where(FinalPrediction.ai_run_id == run.id)))
        )
        ranked = [evaluation for evaluation in evaluations if evaluation.python_rank is not None]
        full_match = bool(ranked) and all(evaluation.ai_rank == evaluation.python_rank for evaluation in ranked)
        statuses.append(
            AiRaceStatusRead(
                race_id=run.race_id,
                latest_run_id=run.id,
                ai_mode=run.ai_mode,
                status=run.status,
                evaluations=len(evaluations),
                final_predictions=final_count,
                has_upgrade=any(evaluation.ai_adjust_score > 0 for evaluation in evaluations),
                has_downgrade=any(evaluation.ai_adjust_score < 0 for evaluation in evaluations),
                python_rank_full_match=full_match,
                created_at=run.created_at,
            )
        )
    return statuses


@router.post("/ai/explain", response_model=AiRunSummary, status_code=status.HTTP_201_CREATED)
def create_ai_explain(payload: AiRunRequest, db: Session = Depends(get_db)) -> AiRunSummary:
    """Create AI explanation run."""

    return run_ai_explain(
        db,
        race_date=payload.race_date,
        race_id=payload.race_id,
        prediction_run_id=payload.prediction_run_id,
        model_name=payload.model_name,
        prompt_version=payload.prompt_version,
    )


@router.post("/ai/second-opinion", response_model=AiRunSummary, status_code=status.HTTP_201_CREATED)
def create_ai_second_opinion(payload: AiRunRequest, db: Session = Depends(get_db)) -> AiRunSummary:
    """Create AI second-opinion run."""

    return run_ai_second_opinion(
        db,
        race_date=payload.race_date,
        race_id=payload.race_id,
        prediction_run_id=payload.prediction_run_id,
        model_name=payload.model_name,
        prompt_version=payload.prompt_version,
    )


@router.post("/ai/bet-correction", response_model=AiRunSummary, status_code=status.HTTP_201_CREATED)
def create_ai_bet_correction(payload: AiRunRequest, db: Session = Depends(get_db)) -> AiRunSummary:
    """Create AI-corrected bet strategy."""

    return run_ai_bet_correction(
        db,
        race_date=payload.race_date,
        race_id=payload.race_id,
        ai_run_id=payload.ai_run_id,
    )


@router.get("/races/{race_id}/ai-evaluations", response_model=list[AiHorseEvaluationRead])
def list_race_ai_evaluations(
    race_id: str,
    ai_run_id: str | None = None,
    db: Session = Depends(get_db),
) -> list[AiHorseEvaluation]:
    """List AI horse evaluations for one race."""

    target_run_id = ai_run_id or _latest_ai_run_id(db, race_id)
    if target_run_id is None:
        return []
    stmt = (
        select(AiHorseEvaluation)
        .where(AiHorseEvaluation.race_id == race_id, AiHorseEvaluation.ai_run_id == target_run_id)
        .order_by(AiHorseEvaluation.ai_rank, AiHorseEvaluation.horse_no)
    )
    return list(db.scalars(stmt))


@router.get("/races/{race_id}/final-predictions", response_model=list[FinalPredictionRead])
def list_race_final_predictions(
    race_id: str,
    ai_run_id: str | None = None,
    db: Session = Depends(get_db),
) -> list[FinalPrediction]:
    """List final predictions for one race."""

    target_run_id = ai_run_id or _latest_ai_run_id(db, race_id)
    if target_run_id is None:
        return []
    stmt = (
        select(FinalPrediction)
        .where(FinalPrediction.race_id == race_id, FinalPrediction.ai_run_id == target_run_id)
        .order_by(FinalPrediction.final_rank, FinalPrediction.horse_no)
    )
    return list(db.scalars(stmt))


@router.get("/races/{race_id}/ai-bet-strategy", response_model=AiBetStrategyRead | None)
def get_race_ai_bet_strategy(
    race_id: str,
    ai_run_id: str | None = None,
    db: Session = Depends(get_db),
) -> AiBetStrategy | None:
    """Get latest AI bet strategy for one race."""

    target_run_id = ai_run_id or _latest_ai_run_id(db, race_id)
    if target_run_id is None:
        return None
    return db.scalar(
        select(AiBetStrategy)
        .where(AiBetStrategy.race_id == race_id, AiBetStrategy.ai_run_id == target_run_id)
        .order_by(AiBetStrategy.created_at.desc())
        .limit(1)
    )


def _latest_ai_run_id(db: Session, race_id: str) -> str | None:
    return db.scalar(
        select(AiPredictionRun.id)
        .where(AiPredictionRun.race_id == race_id, AiPredictionRun.ai_mode == "ai_second_opinion")
        .order_by(AiPredictionRun.created_at.desc())
        .limit(1)
    )


def _independent_detail(db: Session, analysis: AiAnalysis) -> AiIndependentAnalysisRead:
    output = db.scalar(
        select(AiAnalysisOutput).where(
            AiAnalysisOutput.analysis_id == analysis.id,
            AiAnalysisOutput.stage == "independent",
        )
    )
    return AiIndependentAnalysisRead(
        id=analysis.id,
        race_id=analysis.race_id,
        race_date=analysis.race_date,
        analysis_sequence=analysis.analysis_sequence,
        status=analysis.status,
        model_name=analysis.model_name,
        prompt_version=analysis.prompt_version,
        input_data_version=analysis.input_data_version,
        input_snapshot_hash=analysis.input_snapshot_hash,
        prompt_tokens=analysis.prompt_tokens,
        completion_tokens=analysis.completion_tokens,
        duration_ms=analysis.duration_ms,
        error_message=analysis.error_message,
        rerun_reason=analysis.rerun_reason,
        started_at=analysis.started_at,
        finished_at=analysis.finished_at,
        created_at=analysis.created_at,
        output=output.output_json if output is not None else None,
        output_hash=output.output_hash if output is not None else None,
        output_locked=bool(output and output.is_locked),
    )


def _integration_detail(db: Session, analysis: AiAnalysis) -> AiIntegrationAnalysisRead:
    outputs = {
        output.stage: output
        for output in db.scalars(
            select(AiAnalysisOutput).where(AiAnalysisOutput.analysis_id == analysis.id)
        )
    }
    comparison = outputs.get("comparison")
    integration = outputs.get("integration")
    return AiIntegrationAnalysisRead(
        id=analysis.id,
        race_id=analysis.race_id,
        race_date=analysis.race_date,
        prediction_run_id=analysis.prediction_run_id,
        independent_analysis_id=analysis.parent_analysis_id,
        analysis_sequence=analysis.analysis_sequence,
        status=analysis.status,
        model_name=analysis.model_name,
        prompt_version=analysis.prompt_version,
        input_data_version=analysis.input_data_version,
        input_snapshot_hash=analysis.input_snapshot_hash,
        prompt_tokens=analysis.prompt_tokens,
        completion_tokens=analysis.completion_tokens,
        duration_ms=analysis.duration_ms,
        error_message=analysis.error_message,
        rerun_reason=analysis.rerun_reason,
        started_at=analysis.started_at,
        finished_at=analysis.finished_at,
        created_at=analysis.created_at,
        comparison=comparison.output_json if comparison is not None else None,
        comparison_output_hash=comparison.output_hash if comparison is not None else None,
        comparison_locked=bool(comparison and comparison.is_locked),
        integration=integration.output_json if integration is not None else None,
        integration_output_hash=integration.output_hash if integration is not None else None,
        integration_locked=bool(integration and integration.is_locked),
    )
