"""
Project: QF5214 Polymarket Data Pipeline
File: aggregation_tasks.py
Author: Xu
Created: 2026-03-14

Description:
Aggregation task definitions
"""
import time
from app.celery.celery_app import celery_app
from app.services.aggregation_service import aggregation_service
from app.utils.wrapper_utils import log_task_run


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_test_raw_data", time_limit=360)
# @log_task_run(task_name="aggregate_test_raw_data", task_type="aggregation")
# def aggregate_test_raw_data():
#     """
#     Aggregate test raw data.
#     """
#     aggregation_service.aggregate_test_raw_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_hour_data_01", time_limit=720)
# @log_task_run(task_name="aggregate_hour_data_01", task_type="aggregation")
# def aggregate_hour_data_01():
#     """
#     Aggregate hourly data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()



# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_hour_data_02", time_limit=720)
# @log_task_run(task_name="aggregate_hour_data_02", task_type="aggregation")
# def aggregate_hour_data_02():
#     """
#     Aggregate hourly data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_hour_data_03", time_limit=720)
# @log_task_run(task_name="aggregate_hour_data_03", task_type="aggregation")
# def aggregate_hour_data_03():
#     """
#     Aggregate hourly data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_hour_data_04", time_limit=720)
# @log_task_run(task_name="aggregate_hour_data_04", task_type="aggregation")
# def aggregate_hour_data_04():
#     """
#     Aggregate hourly data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_day_data_01", time_limit=720)
# @log_task_run(task_name="aggregate_day_data_01", task_type="aggregation")
# def aggregate_day_data_01():
#     """
#     Aggregate daily data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_day_data_02", time_limit=720)
# @log_task_run(task_name="aggregate_day_data_02", task_type="aggregation")
# def aggregate_day_data_02():
#     """
#     Aggregate daily data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_day_data_03", time_limit=720)
# @log_task_run(task_name="aggregate_day_data_03", task_type="aggregation")
# def aggregate_day_data_03():
#     """
#     Aggregate daily data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()


# @celery_app.task(name="app.tasks.aggregation_tasks.aggregate_day_data_04", time_limit=720)
# @log_task_run(task_name="aggregate_day_data_04", task_type="aggregation")
# def aggregate_day_data_04():
#     """
#     Aggregate daily data.
#     """
#     aggregation_service.aggregate_placeholder1_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder2_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder3_data()
#     time.sleep(1)
#     aggregation_service.aggregate_placeholder4_data()
