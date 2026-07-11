from app.core.celery_app import celery_app
from app.legacy_bridge.prediction_runner import execute_queued_prediction_job
from app.services.ai_independent import execute_queued_ai_job
from app.services.collector import execute_queued_collection_job


@celery_app.task(name="keiba_ai_studio.ping")
def ping() -> str:
    """Return a small response to verify Celery worker boot."""

    return "pong"


@celery_app.task(
    name="keiba_ai_studio.collector.run",
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_collection_job(job_id: str) -> dict[str, str]:
    """Execute one persisted collection job on the dedicated collector queue."""

    return execute_queued_collection_job(job_id)


@celery_app.task(
    name="keiba_ai_studio.prediction.run",
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_prediction_job(job_id: str) -> dict[str, str]:
    """Execute one persisted prediction job on the dedicated prediction queue."""

    return execute_queued_prediction_job(job_id)


@celery_app.task(
    name="keiba_ai_studio.ai.independent",
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_independent_ai_job(job_id: str) -> dict[str, str]:
    """Execute one persisted independent analysis on the dedicated AI queue."""

    return execute_queued_ai_job(job_id)


@celery_app.task(
    name="keiba_ai_studio.ai.compare_integrate",
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_comparison_integration_job(job_id: str) -> dict[str, str]:
    """Execute comparison and guarded integration on the dedicated AI queue."""

    return execute_queued_ai_job(job_id)
