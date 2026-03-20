"""
Project: QF5214 Polymarket Data Pipeline
File: clickhouse_tables.py
Author: Xu
Created: 2026-03-14

Description:
ClickHouse table definitions for the project.
"""

from enum import Enum

TEST_RAW_TABLE_COLS = [
    "source",
    "exchange",
    "entity_id",
    "metric_value",
    "metric_count",
    "ck_insert_time",
]


TEST_RAW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_raw
(
    source String,
    exchange String,
    entity_id String,
    metric_value Float64,
    metric_count UInt64,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(ck_insert_time)
ORDER BY (entity_id, ck_insert_time)
TTL ck_insert_time + INTERVAL 1 DAY DELETE
"""

TEST_RAW_HOUR_TABLE_COLS = [
    "source",
    "exchange",
    "entity_id",
    "hour_bucket",
    "sum_value",
    "avg_value",
    "event_count",
    "ck_insert_time",
]


TEST_RAW_HOUR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_raw_hour
(
    source String,
    exchange String,
    entity_id String,
    hour_bucket DateTime,
    sum_value Float64,
    avg_value Float64,
    event_count UInt64,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(ck_insert_time)
ORDER BY (entity_id, hour_bucket, ck_insert_time)
TTL ck_insert_time + INTERVAL 1 DAY DELETE
"""

TEST_RAW_DAY_TABLE_COLS = [
    "source",
    "exchange",
    "entity_id",
    "day_bucket",
    "sum_value",
    "avg_value",
    "event_count",
    "ck_insert_time",
]


TEST_RAW_DAY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_raw_day
(
    source String,
    exchange String,
    entity_id String,
    day_bucket Date,
    sum_value Float64,
    avg_value Float64,
    event_count UInt64,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(ck_insert_time)
ORDER BY (entity_id, day_bucket, ck_insert_time)
TTL ck_insert_time + INTERVAL 1 DAY DELETE
"""

TASK_RUN_LOG_TABLE_COLS = [
    "task_name",
    "task_type",
    "status",
    "start_time",
    "end_time",
    "duration_seconds",
    "processed_count",
    "error_message",
    "ck_insert_time",
]


TASK_RUN_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS task_run_log
(
    task_name String,
    task_type String,
    status String,
    start_time DateTime,
    end_time DateTime,
    duration_seconds Float64,
    processed_count UInt64 DEFAULT 0,
    error_message String DEFAULT '',
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(ck_insert_time)
ORDER BY (task_name, start_time)
TTL ck_insert_time + INTERVAL 30 DAY DELETE
"""

CLICKHOUSE_TABLE_QUERIES = [
    TEST_RAW_TABLE_SQL,
    TEST_RAW_HOUR_TABLE_SQL,
    TEST_RAW_DAY_TABLE_SQL,
    TASK_RUN_LOG_TABLE_SQL,
]


class CLICKHOUSE_TABLE_COLS_ENUM(Enum):
    TEST_RAW = TEST_RAW_TABLE_COLS
    TEST_RAW_HOUR = TEST_RAW_HOUR_TABLE_COLS
    TEST_RAW_DAY = TEST_RAW_DAY_TABLE_COLS
    TASK_RUN_LOG = TASK_RUN_LOG_TABLE_COLS
