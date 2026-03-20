"""
Project: QF5214 Polymarket Data Pipeline
File: celery_app.py
Author: Xu
Created: 2026-03-14

Description:
Celery application configuration
"""

from celery import Celery
from celery.schedules import crontab

from app.config.settings import (
    APP_NAME,
    APP_TIMEZONE,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
)

celery_app = Celery(
    APP_NAME,
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.ingestion_tasks",
        "app.tasks.consume_tasks",
        "app.tasks.aggregation_tasks",
    ],
)

celery_app.conf.update(
    timezone=APP_TIMEZONE,
    enable_utc=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

celery_app.conf.beat_schedule = {
    "ingest-test-raw-data-every-minute": {
        "task": "app.tasks.ingestion_tasks.ingest_test_raw_data",
        "schedule": crontab(minute="*"),
    },
}
