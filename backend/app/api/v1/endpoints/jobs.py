import json
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.core.logging import redact_text
from app.db.models import JobRun
from app.db.models import RaceResult
from app.legacy_bridge.excel_importer import import_latest_race_workbook
from app.legacy_bridge.legacy_runner import LEGACY_COLLECTION_SCRIPTS
from app.legacy_bridge.prediction_runner import run_prediction_job
from app.schemas.api import JobCreate, JobRead
from app.services.ai_opinion import run_ai_bet_correction, run_ai_explain, run_ai_second_opinion
from app.services.ai_independent import run_independent_analysis
from app.services.ai_integration import run_comparison_integration
from app.services.betting import generate_bet_candidates, settle_bets_for_race
from app.services.collector import run_collection_pipeline
from app.services.data_quality import has_blocking_quality_status, run_data_quality_checks
from app.services.history import record_audit, record_job_log
from app.services.idempotency import (
    IdempotencyConflict,
    IdempotencyInProgress,
    begin_idempotent_request,
    complete_idempotent_request,
)

router = APIRouter()

SUPPORTED_JOB_TYPES = {
    "collection.race_info",
    "collection.past_performances",
    "collection.odds",
    "collection.training",
    "collection.results",
    "maintenance.import_excel",
    "prediction.feature_generation",
    "prediction.python",
    "prediction.run",
    "prediction.risk_evaluation",
    "ai.explain",
    "ai.independent",
    "ai.compare_integrate",
    "ai.second_opinion",
    "ai.bet_correction",
    "bet.generate",
    "result.settlement",
    "result.review_generation",
    "maintenance.data_quality_check",
    "maintenance.backup",
}


@router.get("/jobs", response_model=list[JobRead])
def list_jobs(
    db: Session = Depends(get_db),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[JobRun]:
    """List job runs."""

    stmt = select(JobRun).order_by(JobRun.created_at.desc()).limit(limit)
    return list(db.scalars(stmt))


@router.post("/jobs", response_model=JobRead, status_code=status.HTTP_202_ACCEPTED)
def create_job_endpoint(
    payload: JobCreate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)],
    db: Session = Depends(get_db),
) -> JobRun:
    """Create a job through the public idempotent API contract."""

    return create_job(payload, db, idempotency_key=idempotency_key)


def create_job(
    payload: JobCreate,
    db: Session,
    idempotency_key: str | None = None,
    idempotency_scope: str = "POST:/api/v1/jobs",
) -> JobRun:
    """Create a job and run the Phase 4 synchronous MVP worker path."""

    if payload.job_type not in SUPPORTED_JOB_TYPES:
        raise HTTPException(status_code=422, detail="unsupported job_type")
    duplicate = _find_active_duplicate(db, payload)
    if duplicate is not None and not payload.force:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"same job is already active: job_id={duplicate.id}",
        )

    idempotency_record = None
    if idempotency_key is not None:
        try:
            idempotency_record, replay = begin_idempotent_request(
                db,
                scope=idempotency_scope,
                idempotency_key=idempotency_key,
                payload=payload,
            )
        except (IdempotencyConflict, IdempotencyInProgress, ValueError) as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        if replay:
            replayed_job = db.get(JobRun, idempotency_record.resource_id)
            if replayed_job is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="idempotent response resource is no longer available",
                )
            return replayed_job

    use_collector_queue = (
        payload.job_type in LEGACY_COLLECTION_SCRIPTS
        and get_settings().job_execution_mode == "queue"
    )
    use_prediction_queue = (
        payload.job_type in {"prediction.run", "prediction.python"}
        and get_settings().job_execution_mode == "queue"
    )
    use_ai_queue = (
        payload.job_type in {"ai.independent", "ai.compare_integrate"}
        and get_settings().job_execution_mode == "queue"
    )
    use_worker_queue = use_collector_queue or use_prediction_queue or use_ai_queue
    if use_collector_queue:
        queue_name = "collector"
        task_name = "keiba_ai_studio.collector.run"
    elif use_prediction_queue:
        queue_name = "prediction"
        task_name = "keiba_ai_studio.prediction.run"
    elif payload.job_type == "ai.independent":
        queue_name = "ai"
        task_name = "keiba_ai_studio.ai.independent"
    else:
        queue_name = "ai"
        task_name = "keiba_ai_studio.ai.compare_integrate"
    job = JobRun(
        job_type=payload.job_type,
        status="queued" if use_worker_queue else "running",
        race_date=payload.race_date,
        race_id=payload.race_id,
        force=payload.force,
        params=payload.params,
        started_at=None if use_worker_queue else datetime.utcnow(),
    )
    db.add(job)
    db.flush()
    record_job_log(
        db,
        job_run_id=job.id,
        level="INFO",
        event_code="JOB_QUEUED" if use_worker_queue else "JOB_STARTED",
        message=(
            f"job accepted by {queue_name} queue"
            if use_worker_queue
            else "job execution started"
        ),
        context={
            "job_type": job.job_type,
            "race_date": job.race_date.isoformat() if job.race_date else None,
            "race_id": job.race_id,
        },
    )
    record_audit(
        db,
        action="create_job",
        entity_type="job_runs",
        entity_id=job.id,
        after={"status": job.status, "job_type": job.job_type},
    )
    db.commit()
    db.refresh(job)

    if use_worker_queue:
        try:
            celery_app.send_task(
                task_name,
                args=[job.id],
                queue=queue_name,
            )
        except Exception as exc:
            # Queue投入失敗は同期実行へフォールバックせず、二重実行を避けて再実行対象にする。
            job.status = "failed"
            job.message = redact_text(f"{queue_name} queue dispatch failed: {exc}")
            job.finished_at = datetime.utcnow()
            record_job_log(
                db,
                job_run_id=job.id,
                level="ERROR",
                event_code="JOB_QUEUE_DISPATCH_FAILED",
                message=f"{queue_name} queue dispatch failed",
                context={"queue": queue_name, "error_type": exc.__class__.__name__},
            )
            db.commit()
            db.refresh(job)

        if idempotency_record is not None:
            complete_idempotent_request(
                db,
                idempotency_record,
                response_status=status.HTTP_202_ACCEPTED,
                response_body=JobRead.model_validate(job).model_dump(mode="json"),
                resource_type="job_runs",
                resource_id=job.id,
            )
        return job

    try:
        if _requires_quality_gate(payload.job_type) and has_blocking_quality_status(
            db,
            race_date=payload.race_date,
            race_id=payload.race_id,
        ):
            raise RuntimeError("REDのデータ品質状態があるため、このジョブは実行できません")

        if payload.job_type in LEGACY_COLLECTION_SCRIPTS:
            job.message = _json_message(run_collection_pipeline(db, job))
        elif payload.job_type == "maintenance.import_excel":
            summary = import_latest_race_workbook(db, race_date=payload.race_date)
            quality_summary = run_data_quality_checks(
                db,
                race_date=payload.race_date or summary.race_date,
                race_id=payload.race_id,
            )
            job.message = _json_message(
                {
                    "import": summary.model_dump(mode="json"),
                    "quality": quality_summary.model_dump(mode="json"),
                }
            )
        elif payload.job_type == "prediction.feature_generation":
            summary = import_latest_race_workbook(
                db,
                race_date=payload.race_date,
                prefer_feature_file=True,
            )
            quality_summary = run_data_quality_checks(
                db,
                race_date=payload.race_date or summary.race_date,
                race_id=payload.race_id,
            )
            job.message = _json_message(
                {
                    "import": summary.model_dump(mode="json"),
                    "quality": quality_summary.model_dump(mode="json"),
                }
            )
        elif payload.job_type == "maintenance.data_quality_check":
            quality_summary = run_data_quality_checks(
                db,
                race_date=payload.race_date,
                race_id=payload.race_id,
            )
            job.message = quality_summary.model_dump_json()
        elif payload.job_type in {"prediction.run", "prediction.python"}:
            prediction_summary = run_prediction_job(
                db,
                prediction_run_id=job.id,
                race_date=payload.race_date,
                race_id=payload.race_id,
                params=payload.params,
                force=payload.force,
            )
            job.message = _json_message(prediction_summary)
        elif payload.job_type == "ai.independent":
            if not payload.race_id:
                raise ValueError("ai.independentにはrace_idが必要です")
            independent_summary = run_independent_analysis(
                db,
                race_id=payload.race_id,
                job_run_id=job.id,
                rerun_reason=(payload.params or {}).get("rerun_reason"),
            )
            job.message = _json_message(independent_summary)
        elif payload.job_type == "ai.compare_integrate":
            if not payload.race_id:
                raise ValueError("ai.compare_integrateにはrace_idが必要です")
            params = payload.params or {}
            integration_summary = run_comparison_integration(
                db,
                race_id=payload.race_id,
                job_run_id=job.id,
                independent_analysis_id=params.get("independent_analysis_id"),
                prediction_run_id=params.get("prediction_run_id"),
                rerun_reason=params.get("rerun_reason"),
            )
            job.message = _json_message(integration_summary)
        elif payload.job_type == "ai.explain":
            params = payload.params or {}
            ai_summary = run_ai_explain(
                db,
                race_date=payload.race_date,
                race_id=payload.race_id,
                prediction_run_id=params.get("prediction_run_id"),
                model_name=params.get("model_name"),
                prompt_version=params.get("prompt_version"),
            )
            job.message = ai_summary.model_dump_json()
        elif payload.job_type == "ai.second_opinion":
            params = payload.params or {}
            ai_summary = run_ai_second_opinion(
                db,
                race_date=payload.race_date,
                race_id=payload.race_id,
                prediction_run_id=params.get("prediction_run_id"),
                model_name=params.get("model_name"),
                prompt_version=params.get("prompt_version"),
            )
            job.message = ai_summary.model_dump_json()
        elif payload.job_type == "ai.bet_correction":
            params = payload.params or {}
            ai_summary = run_ai_bet_correction(
                db,
                race_date=payload.race_date,
                race_id=payload.race_id,
                ai_run_id=params.get("ai_run_id"),
                stake_per_point=int(params.get("stake_per_point", 500)),
                max_race_amount=int(params.get("max_race_amount", 3000)),
                max_day_amount=int(params.get("max_day_amount", 12000)),
            )
            job.message = ai_summary.model_dump_json()
        elif payload.job_type == "bet.generate":
            params = payload.params or {}
            bet_summary = generate_bet_candidates(
                db,
                race_date=payload.race_date,
                race_id=payload.race_id,
                prediction_run_id=params.get("prediction_run_id"),
                source_modes=params.get("source_modes"),
                bet_types=params.get("bet_types"),
                strategy_modes=params.get("strategy_modes"),
                ai_analysis_id=params.get("ai_analysis_id"),
                stake_per_point=int(params.get("stake_per_point", 500)),
                max_race_amount=int(params.get("max_race_amount", 3000)),
                max_day_amount=int(params.get("max_day_amount", 12000)),
                max_points=int(params.get("max_points", 20)),
                allow_manual_review=bool(params.get("allow_manual_review", False)),
            )
            job.message = bet_summary.model_dump_json()
        elif payload.job_type == "result.settlement":
            race_ids = [payload.race_id] if payload.race_id else _result_race_ids(db, payload.race_date)
            settled = []
            for result_race_id in race_ids:
                settled.extend(settle_bets_for_race(db, result_race_id))
            job.message = _json_message(
                {
                    "settled": len(settled),
                    "hits": sum(1 for settlement in settled if settlement.is_hit),
                    "stake_amount": sum(settlement.stake_amount for settlement in settled),
                    "payout_amount": sum(settlement.payout_amount for settlement in settled),
                    "profit_loss": sum(settlement.profit_loss for settlement in settled),
                }
            )
        else:
            job.message = "job type registered; worker implementation is scheduled for later phases"

        job.status = "completed"
    except Exception as exc:
        db.rollback()
        job = db.get(JobRun, job.id)
        if job is None:
            raise
        job.status = "failed"
        job.message = redact_text(f"{exc.__class__.__name__}: {exc}")
    finally:
        job.finished_at = datetime.utcnow()
        db.add(job)
        record_job_log(
            db,
            job_run_id=job.id,
            level="INFO" if job.status == "completed" else "ERROR",
            event_code="JOB_COMPLETED" if job.status == "completed" else "JOB_FAILED",
            message="job execution completed" if job.status == "completed" else "job execution failed",
            context={"status": job.status},
        )
        db.commit()
        db.refresh(job)

    if idempotency_record is not None:
        complete_idempotent_request(
            db,
            idempotency_record,
            response_status=status.HTTP_202_ACCEPTED,
            response_body=JobRead.model_validate(job).model_dump(mode="json"),
            resource_type="job_runs",
            resource_id=job.id,
        )

    return job


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: str, db: Session = Depends(get_db)) -> JobRun:
    """Get one job run."""

    job = db.get(JobRun, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.post("/jobs/{job_id}/retry", response_model=JobRead, status_code=status.HTTP_202_ACCEPTED)
def retry_job(
    job_id: str,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)],
    db: Session = Depends(get_db),
) -> JobRun:
    """Retry an existing job using the unified job API contract."""

    original = db.get(JobRun, job_id)
    if original is None:
        raise HTTPException(status_code=404, detail="job not found")

    payload = JobCreate(
        job_type=original.job_type,
        race_date=original.race_date,
        race_id=original.race_id,
        force=True,
        params=original.params,
    )
    return create_job(
        payload,
        db,
        idempotency_key=idempotency_key,
        idempotency_scope=f"POST:/api/v1/jobs/{job_id}/retry",
    )


def _find_active_duplicate(db: Session, payload: JobCreate) -> JobRun | None:
    stmt = (
        select(JobRun)
        .where(
            JobRun.job_type == payload.job_type,
            JobRun.race_date == payload.race_date,
            JobRun.race_id == payload.race_id,
            JobRun.status.in_(("queued", "running")),
        )
        .order_by(JobRun.created_at.desc())
        .limit(1)
    )
    return db.scalar(stmt)


def _requires_quality_gate(job_type: str) -> bool:
    return job_type in {
        "prediction.run",
        "prediction.python",
        "prediction.risk_evaluation",
        "ai.independent",
        "ai.compare_integrate",
        "ai.explain",
        "ai.second_opinion",
        "ai.bet_correction",
        "bet.generate",
    }


def _result_race_ids(db: Session, race_date) -> list[str]:
    stmt = select(RaceResult.race_id)
    if race_date is not None:
        stmt = stmt.where(RaceResult.race_date == race_date)
    return list(db.scalars(stmt.order_by(RaceResult.race_id)))


def _json_message(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)
