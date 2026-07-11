from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.db.models import PredictionResult, PredictionRun
from app.schemas.api import PredictionRaceStatusRead, PredictionResultRead, PredictionRunRead

router = APIRouter()


@router.get("/prediction-runs", response_model=list[PredictionRunRead])
def list_prediction_runs(
    race_date: date | None = None,
    race_id: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[PredictionRun]:
    """List prediction run history."""

    stmt = select(PredictionRun).order_by(PredictionRun.created_at.desc()).limit(limit)
    if race_date is not None:
        stmt = stmt.where(PredictionRun.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(PredictionRun.race_id == race_id)
    return list(db.scalars(stmt))


@router.get("/prediction-runs/{prediction_run_id}", response_model=PredictionRunRead)
def get_prediction_run(prediction_run_id: str, db: Session = Depends(get_db)) -> PredictionRun:
    """Get one prediction run."""

    prediction_run = db.get(PredictionRun, prediction_run_id)
    if prediction_run is None:
        raise HTTPException(status_code=404, detail="prediction run not found")
    return prediction_run


@router.get("/prediction-statuses", response_model=list[PredictionRaceStatusRead])
def list_prediction_statuses(
    race_date: date | None = None,
    db: Session = Depends(get_db),
) -> list[PredictionRaceStatusRead]:
    """List latest prediction status per race."""

    run_stmt = select(PredictionRun).where(PredictionRun.status == "completed")
    if race_date is not None:
        run_stmt = run_stmt.where(PredictionRun.race_date == race_date)
    runs = list(db.scalars(run_stmt.order_by(PredictionRun.finished_at.desc(), PredictionRun.created_at.desc())))

    statuses: dict[str, PredictionRaceStatusRead] = {}
    for prediction_run in runs:
        result_stmt = select(PredictionResult).where(
            PredictionResult.prediction_run_id == prediction_run.id
        )
        if prediction_run.race_id:
            result_stmt = result_stmt.where(PredictionResult.race_id == prediction_run.race_id)
        results = list(db.scalars(result_stmt.order_by(PredictionResult.race_id, PredictionResult.prediction_rank)))
        race_ids = sorted({result.race_id for result in results})
        for result_race_id in race_ids:
            if result_race_id in statuses:
                continue
            race_results = [result for result in results if result.race_id == result_race_id]
            top = next((result for result in race_results if result.prediction_rank == 1), race_results[0])
            statuses[result_race_id] = PredictionRaceStatusRead(
                race_id=result_race_id,
                latest_run_id=prediction_run.id,
                status=prediction_run.status,
                result_count=len(race_results),
                predicted_at=prediction_run.finished_at,
                top_horse_no=top.horse_no,
                top_horse_name=top.horse_name,
                top_score=top.prediction_score,
            )
    return list(statuses.values())


@router.get("/races/{race_id}/prediction-results", response_model=list[PredictionResultRead])
def list_race_prediction_results(
    race_id: str,
    prediction_run_id: str | None = None,
    db: Session = Depends(get_db),
) -> list[PredictionResult]:
    """List prediction results for one race from the latest or specified run."""

    target_run_id = prediction_run_id
    if target_run_id is None:
        target_run_id = db.scalar(
            select(PredictionResult.prediction_run_id)
            .join(PredictionRun, PredictionRun.id == PredictionResult.prediction_run_id)
            .where(PredictionResult.race_id == race_id, PredictionRun.status == "completed")
            .order_by(PredictionRun.finished_at.desc(), PredictionRun.created_at.desc())
            .limit(1)
        )
    if target_run_id is None:
        return []

    stmt = (
        select(PredictionResult)
        .where(
            PredictionResult.race_id == race_id,
            PredictionResult.prediction_run_id == target_run_id,
        )
        .order_by(PredictionResult.prediction_rank, PredictionResult.horse_no)
    )
    return list(db.scalars(stmt))
