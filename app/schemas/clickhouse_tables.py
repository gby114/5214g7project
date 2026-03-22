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

POLYMARKET_MARKET_DIM_TABLE_COLS = [
    "market_id",
    "event_id",
    "title",
    "description",
    "url",
    "image",
    "category",
    "tags_json",
    "updated_at",
    "ck_insert_time",
]

POLYMARKET_MARKET_DIM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_market_dim
(
    market_id String,
    event_id String,
    title String,
    description String,
    url String,
    image String,
    category String,
    tags_json String,
    updated_at DateTime,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (market_id)
"""

POLYMARKET_MARKET_SNAPSHOT_TABLE_COLS = [
    "source",
    "exchange",
    "market_id",
    "captured_at",
    "resolution_date",
    "volume24h",
    "volume",
    "liquidity",
    "open_interest",
    "fetch_params_json",
    "ck_insert_time",
]

POLYMARKET_MARKET_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_market_snapshot
(
    source String,
    exchange String,
    market_id String,
    captured_at DateTime,
    resolution_date String,
    volume24h Float64,
    volume Float64,
    liquidity Float64,
    open_interest Float64,
    fetch_params_json String,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(captured_at)
ORDER BY (market_id, captured_at)
TTL captured_at + INTERVAL 90 DAY DELETE
"""

POLYMARKET_OUTCOME_SNAPSHOT_TABLE_COLS = [
    "source",
    "exchange",
    "market_id",
    "outcome_id",
    "label",
    "captured_at",
    "price",
    "price_change24h",
    "metadata_json",
    "ck_insert_time",
]

POLYMARKET_OUTCOME_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_outcome_snapshot
(
    source String,
    exchange String,
    market_id String,
    outcome_id String,
    label String,
    captured_at DateTime,
    price Float64,
    price_change24h Float64,
    metadata_json String,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(captured_at)
ORDER BY (market_id, outcome_id, captured_at)
TTL captured_at + INTERVAL 90 DAY DELETE
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


KAFKA_CLUSTER_METRIC_SNAPSHOT_TABLE_COLS = [
    "collected_at",
    "collected_date",
    "collected_hour",
    "cluster_name",
    "bootstrap_servers",
    "broker_count",
    "topic_count",
    "internal_topic_count",
    "partition_count",
    "under_replicated_partition_count",
    "offline_partition_count",
    "estimated_message_count",
    "ck_insert_time",
]


KAFKA_CLUSTER_METRIC_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kafka_cluster_metric_snapshot
(
    collected_at DateTime,
    collected_date Date,
    collected_hour DateTime,

    cluster_name String,
    bootstrap_servers String,

    broker_count UInt16,
    topic_count UInt32,
    internal_topic_count UInt32,
    partition_count UInt32,

    under_replicated_partition_count UInt32,
    offline_partition_count UInt32,

    estimated_message_count Int64,

    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(collected_date)
ORDER BY (cluster_name, collected_at)
TTL collected_at + INTERVAL 30 DAY DELETE
"""


KAFKA_TOPIC_METRIC_SNAPSHOT_TABLE_COLS = [
    "collected_at",
    "collected_date",
    "collected_hour",
    "cluster_name",
    "topic_name",
    "is_internal",
    "partition_count",
    "replication_factor",
    "min_isr_count",
    "avg_isr_count",
    "under_replicated_partition_count",
    "offline_partition_count",
    "log_start_offset_sum",
    "log_end_offset_sum",
    "estimated_message_count",
    "ck_insert_time",
]


KAFKA_TOPIC_METRIC_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kafka_topic_metric_snapshot
(
    collected_at DateTime,
    collected_date Date,
    collected_hour DateTime,

    cluster_name String,
    topic_name String,
    is_internal UInt8,

    partition_count UInt16,
    replication_factor UInt16,

    min_isr_count UInt16,
    avg_isr_count Float64,

    under_replicated_partition_count UInt16,
    offline_partition_count UInt16,

    log_start_offset_sum Int64,
    log_end_offset_sum Int64,
    estimated_message_count Int64,

    ck_insert_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(collected_date)
ORDER BY (cluster_name, topic_name, collected_at)
TTL collected_at + INTERVAL 30 DAY DELETE
"""


CLICKHOUSE_TABLE_QUERIES = [
    TEST_RAW_TABLE_SQL,
    TEST_RAW_HOUR_TABLE_SQL,
    TEST_RAW_DAY_TABLE_SQL,
    POLYMARKET_MARKET_DIM_TABLE_SQL,
    POLYMARKET_MARKET_SNAPSHOT_TABLE_SQL,
    POLYMARKET_OUTCOME_SNAPSHOT_TABLE_SQL,
    TASK_RUN_LOG_TABLE_SQL,
    KAFKA_CLUSTER_METRIC_SNAPSHOT_TABLE_SQL,
    KAFKA_TOPIC_METRIC_SNAPSHOT_TABLE_SQL,
]


class CLICKHOUSE_TABLE_COLS_ENUM(Enum):
    TEST_RAW = TEST_RAW_TABLE_COLS
    TEST_RAW_HOUR = TEST_RAW_HOUR_TABLE_COLS
    TEST_RAW_DAY = TEST_RAW_DAY_TABLE_COLS
    POLYMARKET_MARKET_DIM = POLYMARKET_MARKET_DIM_TABLE_COLS
    POLYMARKET_MARKET_SNAPSHOT = POLYMARKET_MARKET_SNAPSHOT_TABLE_COLS
    POLYMARKET_OUTCOME_SNAPSHOT = POLYMARKET_OUTCOME_SNAPSHOT_TABLE_COLS
    TASK_RUN_LOG = TASK_RUN_LOG_TABLE_COLS

# ─────────────────────────────────────────
# IMPORT AND REGISTER STAR SCHEMA TABLES
# ─────────────────────────────────────────
from app.schemas.star_schema import STAR_SCHEMA_TABLES

# Combined list of ALL tables to create on init
ALL_TABLE_QUERIES = CLICKHOUSE_TABLE_QUERIES + STAR_SCHEMA_TABLES
KAFKA_CLUSTER_METRIC_SNAPSHOT = KAFKA_CLUSTER_METRIC_SNAPSHOT_TABLE_COLS
KAFKA_TOPIC_METRIC_SNAPSHOT = KAFKA_TOPIC_METRIC_SNAPSHOT_TABLE_COLS