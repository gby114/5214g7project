"""
Project: QF5214 Polymarket Data Pipeline
File: aggregation_tasks.py
Description: Celery tasks for scheduled aggregation
"""

import time
from app.celery.celery_app import celery_app
from app.services.aggregation_service import aggregation_service
from app.utils.wrapper_utils import log_task_run


@celery_app.task(
    name="app.tasks.aggregation_tasks.aggregate_test_raw_data",
    time_limit=120
)
@log_task_run(task_name="aggregate_test_raw_data", task_type="aggregation")
def aggregate_test_raw_data():
    """Aggregate test raw data."""
    aggregation_service.aggregate_test_raw_data()


@celery_app.task(
    name="app.tasks.aggregation_tasks.aggregate_hour_data_01",
    time_limit=120
)
@log_task_run(task_name="aggregate_hour_data_01", task_type="aggregation")
def aggregate_hour_data_01():
    """
    Hourly aggregation pipeline:
    1. Market snapshots → fact_market_snapshot_hourly
    2. Outcome snapshots → fact_outcome_snapshot_hourly
    """
    aggregation_service.aggregate_placeholder1_data()
    time.sleep(1)
    aggregation_service.aggregate_placeholder2_data()


@celery_app.task(
    name="app.tasks.aggregation_tasks.aggregate_day_data_01",
    time_limit=120
)
@log_task_run(task_name="aggregate_day_data_01", task_type="aggregation")
def aggregate_day_data_01():
    """
    Daily aggregation pipeline:
    1. fact_market_snapshot_hourly → fact_market_snapshot_daily
    2. fact_outcome_snapshot_hourly → fact_outcome_snapshot_daily
    """
    aggregation_service.aggregate_placeholder3_data()
    time.sleep(1)
    aggregation_service.aggregate_placeholder4_data()


@celery_app.task(
    name="app.tasks.aggregation_tasks.refresh_dimensions",
    time_limit=120
)
@log_task_run(task_name="refresh_dimensions", task_type="aggregation")
def refresh_dimensions():
    """
    Refresh all dimension tables:
    dim_market, dim_outcome, dim_source
    """
    aggregation_service.populate_dim_market()
    time.sleep(1)
    aggregation_service.populate_dim_outcome()
    time.sleep(1)
    aggregation_service.populate_dim_source()
    