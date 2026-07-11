from celery import Celery

from app.core.config import get_settings
from app.core.logging import configure_logging


settings = get_settings()
configure_logging()

celery_app = Celery(
    "keiba_ai_studio",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Tokyo",
    enable_utc=False,
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=False,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_default_queue="default",
    task_routes={
        "keiba_ai_studio.collector.*": {"queue": "collector"},
        "keiba_ai_studio.prediction.*": {"queue": "prediction"},
        "keiba_ai_studio.ai.*": {"queue": "ai"},
    },
)
