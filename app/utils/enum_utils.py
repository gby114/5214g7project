"""
Project: QF5214 Polymarket Data Pipeline
File: enum_utils.py
Author: Xu
Created: 2026-03-14

Description:
Enum utility functions
"""


from enum import Enum


class TASK_CONFIG_NAME(Enum):
    TEST_RAW = "test_raw"
    TEST_RAW_HOUR = "test_raw_hour"
    TEST_RAW_DAY = "test_raw_day"
    TASK_RUN_LOG = "task_run_log"
    BACKFILL_JOB_01 = "backfill_job_01"
