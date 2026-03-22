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
    FETCH_MARKETS_ENABLED,
)

celery_app = Celery(
    APP_NAME,
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.ingestion_tasks",
        "app.tasks.consume_tasks",
        "app.tasks.aggregation_tasks",
        "app.tasks.common_task",
        "app.tasks.backfill_task",
    ],
)

celery_app.conf.update(
    timezone=APP_TIMEZONE,
    enable_utc=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
)

beat_schedule = {
    # "ingest-test-raw-data-every-minute": {
    #     "task": "app.tasks.ingestion_tasks.ingest_test_raw_data",
    #     "schedule": crontab(minute="*"),
    # },
    # "collect-kafka-metrics-every-5-minutes": {
    #     "task": "app.tasks.common_task.collect_kafka_metrics",
    #     "schedule": crontab(minute="*/5"),
    # },
    # "refresh-target-market-every-10-minutes": {
    #     "task": "app.tasks.common_task.refresh_target_market",
    #     "schedule": crontab(minute="*/10"),
    # },
    # "aggregate-hour-data-01-every-hour": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_hour_data_01",
    #     "schedule": crontab(minute=0, hour="*"),
    # },
    # "aggregate-day-data-01-every-day": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_day_data_01",
    #     "schedule": crontab(day_of_week="*", hour=0, minute=0),
    # },
    # "refresh-dimensions-every-ten-minutes": {
    #     "task": "app.tasks.aggregation_tasks.refresh_dimensions",
    #     "schedule": crontab(minute=0, hour="*/6"),
    # },
    # "consume-backfill-price-change-data-every-minutes": {
    #     "task": "app.tasks.backfill_task.consume_backfill_price_change_data_task",
    #     "schedule": crontab(minute="*"),
    # },
    # "produce-backfill-price-change-data-every-10-minutes": {
    #     "task": "app.tasks.backfill_task.produce_backfill_price_change_data_task",
    #     "schedule": crontab(minute="*/10"),
    # },
    # "consume-backfill-book-snapshot-data-every-minute": {
    #     "task": "app.tasks.backfill_task.consume_backfill_book_snapshot_data_task",
    #     "schedule": crontab(minute="*"),
    # },
    # "produce-backfill-book-snapshot-data-every-10-minutes": {
    #     "task": "app.tasks.backfill_task.produce_backfill_book_snapshot_data_task",
    #     "schedule": crontab(minute="*/10"),
    # },

    # temp unused
    # "aggregate-hour-data-02-every-hour": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_hour_data_02",
    #     "schedule": crontab(minute=0, hour="*"),
    # },
    # "aggregate-hour-data-03-every-hour": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_hour_data_03",
    #     "schedule": crontab(minute=0, hour="*"),
    # },
    # "aggregate-hour-data-04-every-hour": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_hour_data_04",
    #     "schedule": crontab(minute=0, hour="*"),
    # },
    # "aggregate-day-data-02-every-day": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_day_data_02",
    #     "schedule": crontab(day_of_week="*", hour=0, minute=0),
    # },
    # "aggregate-day-data-03-every-day": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_day_data_03",
    #     "schedule": crontab(day_of_week="*", hour=0, minute=0),
    # },
    # "aggregate-day-data-04-every-day": {
    #     "task": "app.tasks.aggregation_tasks.aggregate_day_data_04",
    #     "schedule": crontab(day_of_week="*", hour=0, minute=0),
    # },
}

# if FETCH_MARKETS_ENABLED:
#     beat_schedule["ingest-polymarket-markets-to-kafka"] = {
#         "task": "app.tasks.ingestion_tasks.ingest_polymarket_markets",
#         "schedule": crontab(minute="*/1"),
#     }
#     beat_schedule["consume-polymarket-markets-raw-to-clickhouse"] = {
#         "task": "app.tasks.consume_tasks.consume_polymarket_markets_raw_to_clickhouse",
#         "schedule": crontab(minute="1-59/1"),
#     }

celery_app.conf.beat_schedule = beat_schedule
