from app.core.celery_app import celery_app
from app.worker import (
    ping,
    run_collection_job,
    run_comparison_integration_job,
    run_independent_ai_job,
    run_prediction_job,
)


def test_celery_uses_json_and_tokyo_timezone() -> None:
    """Worker messages must use JSON and the configured operating timezone."""

    assert celery_app.conf.task_serializer == "json"
    assert celery_app.conf.result_serializer == "json"
    assert celery_app.conf.timezone == "Asia/Tokyo"
    assert celery_app.conf.enable_utc is False
    assert celery_app.conf.worker_hijack_root_logger is False
    assert celery_app.conf.task_track_started is True
    assert celery_app.conf.task_acks_late is True
    assert celery_app.conf.worker_prefetch_multiplier == 1
    assert celery_app.conf.broker_connection_retry_on_startup is True
    assert celery_app.conf.task_default_queue == "default"
    assert celery_app.conf.task_routes["keiba_ai_studio.collector.*"]["queue"] == "collector"
    assert celery_app.conf.task_routes["keiba_ai_studio.prediction.*"]["queue"] == "prediction"
    assert celery_app.conf.task_routes["keiba_ai_studio.ai.*"]["queue"] == "ai"
    assert run_collection_job.name == "keiba_ai_studio.collector.run"
    assert run_prediction_job.name == "keiba_ai_studio.prediction.run"
    assert run_independent_ai_job.name == "keiba_ai_studio.ai.independent"
    assert run_comparison_integration_job.name == "keiba_ai_studio.ai.compare_integrate"


def test_ping_task_runs_without_queue_connection() -> None:
    """The boot verification task can execute synchronously in a unit test."""

    assert ping.run() == "pong"
