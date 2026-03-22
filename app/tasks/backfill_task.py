"""
Project: QF5214 Polymarket Data Pipeline
File: aggregation_tasks.py
Description: Celery tasks for scheduled aggregation
"""

from app.celery.celery_app import celery_app
from app.services.backfill_service import BackfillService
from app.utils.wrapper_utils import log_task_run


@celery_app.task(
    name="app.tasks.backfill_task.produce_backfill_price_change_data_task",
    time_limit=900
)
@log_task_run(task_name="produce_backfill_price_change_data_task", task_type="backfill")
def produce_backfill_price_change_data_task():
    """
    Produce backfill price change data.
    """
    BackfillService().ingest_backfill_price_change_data()


@celery_app.task(
    name="app.tasks.backfill_task.consume_backfill_price_change_data_task",
    time_limit=360
)
@log_task_run(task_name="consume_backfill_price_change_data_task", task_type="backfill")
def consume_backfill_price_change_data_task():
    """
    Consume backfill price change data.
    """
    BackfillService().consume_backfill_price_change_data()


@celery_app.task(
    name="app.tasks.backfill_task.produce_backfill_book_snapshot_data_task",
    time_limit=900
)
@log_task_run(task_name="produce_backfill_book_snapshot_data_task", task_type="backfill")
def produce_backfill_book_snapshot_data_task():
    """
    Produce backfill book snapshot data.
    """
    BackfillService().ingest_backfill_book_snapshot_data()


@celery_app.task(
    name="app.tasks.backfill_task.consume_backfill_book_snapshot_data_task",
    time_limit=360
)
@log_task_run(task_name="consume_backfill_book_snapshot_data_task", task_type="backfill")
def consume_backfill_book_snapshot_data_task():
    """
    Consume backfill book snapshot data.
    """
    BackfillService().consume_backfill_book_snapshot_data()
