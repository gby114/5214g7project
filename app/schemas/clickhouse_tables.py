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

POLYMARKET_TARGET_MARKET_TABLE_COLS = [
    "market_id",
    "outcome_id",
    "volume",
    "ck_insert_time",
]
POLYMARKET_TARGET_MARKET_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_target_market
(
    market_id String,
    outcome_id String,
    volume Float64,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
ORDER BY (market_id, outcome_id);
"""

BACKFILL_PRICE_CHANGE_TABLE_COLS = [
    "update_type",
    "market_id",
    "token_id",
    "side",
    "best_bid",
    "best_ask",
    "event_timestamp",
    "change_price",
    "change_size",
    "change_side",
    "ck_insert_time",
]
BACKFILL_PRICE_CHANGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_backfill_price_change
(
    update_type String,
    market_id String,
    token_id String,
    side String,
    best_bid Float64,
    best_ask Float64,
    event_timestamp Float64,
    change_price Float64,
    change_size Float64,
    change_side String,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(ck_insert_time)
ORDER BY (market_id, token_id, event_timestamp, side)
TTL ck_insert_time + INTERVAL 1 DAY;
"""

BACKFILL_BOOK_SNAPSHOT_TABLE_COLS = [
    "update_type",
    "market_id",
    "token_id",
    "side",
    "best_bid",
    "best_ask",
    "event_timestamp",
    "bids",
    "asks",
    "ck_insert_time",
]
BACKFILL_BOOK_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS polymarket_backfill_book_snapshot
(
    update_type String,
    market_id String,
    token_id String,
    side String,
    best_bid Float64,
    best_ask Float64,
    event_timestamp Float64,
    bids String,
    asks String,
    ck_insert_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(ck_insert_time)
ORDER BY (market_id, token_id, event_timestamp, side)
TTL ck_insert_time + INTERVAL 1 DAY;
"""

# 1. Price Hourly Fact - Captures OHLCV and Market Sentiment
FACT_BACKFILL_PRICE_HOURLY_TABLE_COLS = [
    "market_id", "token_id", "hour_bucket", "open_price", "close_price",
    "high_price", "low_price", "total_volume", "trade_count", 
    "buy_volume", "sell_volume", "ck_insert_time",
]


FACT_BACKFILL_PRICE_HOURLY_SQL = """
CREATE TABLE IF NOT EXISTS fact_backfill_price_hourly
(
    market_id       String,
    token_id        String,
    hour_bucket     DateTime,
    open_price      Float64,
    close_price     Float64,
    high_price      Float64,
    low_price       Float64,
    total_volume    Float64,
    trade_count     UInt64,
    buy_volume      Float64,
    sell_volume     Float64,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(hour_bucket)
ORDER BY (market_id, token_id, hour_bucket)
TTL hour_bucket + INTERVAL 90 DAY DELETE
"""

# 2. Price Daily Fact - Roll-up for long-term trends
FACT_BACKFILL_PRICE_DAILY_TABLE_COLS = [
    "market_id", "token_id", "day_bucket", "open_price", "close_price",
    "high_price", "low_price", "daily_volume", "daily_trades", "net_flow", "ck_insert_time",
]

FACT_BACKFILL_PRICE_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS fact_backfill_price_daily
(
    market_id       String,
    token_id        String,
    day_bucket      Date,
    open_price      Float64,
    close_price     Float64,
    high_price      Float64,
    low_price       Float64,
    daily_volume    Float64,
    daily_trades    UInt64,
    net_flow        Float64,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (market_id, token_id, day_bucket)
TTL day_bucket + INTERVAL 365 DAY DELETE
"""

# 3. Book Hourly Fact - Analyzes liquidity and spread health
FACT_BACKFILL_BOOK_HOURLY_TABLE_COLS = [
    "market_id", "token_id", "hour_bucket", "avg_spread", 
    "avg_mid_price", "price_volatility", "snapshot_count", "ck_insert_time",
]


FACT_BACKFILL_BOOK_HOURLY_SQL = """
CREATE TABLE IF NOT EXISTS fact_backfill_book_hourly
(
    market_id       String,
    token_id        String,
    hour_bucket     DateTime,
    avg_spread      Float64,
    avg_mid_price   Float64,
    price_volatility Float64,
    snapshot_count  UInt64,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(hour_bucket)
ORDER BY (market_id, token_id, hour_bucket)
TTL hour_bucket + INTERVAL 90 DAY DELETE
"""

# 4. Book Daily Fact - Summarizes market stability
FACT_BACKFILL_BOOK_DAILY_TABLE_COLS = [
    "market_id", "token_id", "day_bucket", "avg_daily_spread", 
    "avg_daily_volatility", "max_daily_spread", "snapshot_count", "ck_insert_time",
]


FACT_BACKFILL_BOOK_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS fact_backfill_book_daily
(
    market_id            String,
    token_id             String,
    day_bucket           Date,
    avg_daily_spread     Float64,
    avg_daily_volatility Float64,
    max_daily_spread     Float64,
    snapshot_count       UInt64,
    ck_insert_time       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (market_id, token_id, day_bucket)
TTL day_bucket + INTERVAL 365 DAY DELETE
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
    POLYMARKET_TARGET_MARKET_TABLE_SQL,
    BACKFILL_PRICE_CHANGE_TABLE_SQL,
    BACKFILL_BOOK_SNAPSHOT_TABLE_SQL,
    FACT_BACKFILL_PRICE_HOURLY_SQL,
    FACT_BACKFILL_PRICE_DAILY_SQL,
    FACT_BACKFILL_BOOK_HOURLY_SQL,
    FACT_BACKFILL_BOOK_DAILY_SQL,
]


class CLICKHOUSE_TABLE_COLS_ENUM(Enum):
    TEST_RAW = TEST_RAW_TABLE_COLS
    TEST_RAW_HOUR = TEST_RAW_HOUR_TABLE_COLS
    TEST_RAW_DAY = TEST_RAW_DAY_TABLE_COLS
    POLYMARKET_MARKET_DIM = POLYMARKET_MARKET_DIM_TABLE_COLS
    POLYMARKET_MARKET_SNAPSHOT = POLYMARKET_MARKET_SNAPSHOT_TABLE_COLS
    POLYMARKET_OUTCOME_SNAPSHOT = POLYMARKET_OUTCOME_SNAPSHOT_TABLE_COLS
    TASK_RUN_LOG = TASK_RUN_LOG_TABLE_COLS
    POLYMARKET_TARGET_MARKET = POLYMARKET_TARGET_MARKET_TABLE_COLS
    KAFKA_CLUSTER_METRIC_SNAPSHOT = KAFKA_CLUSTER_METRIC_SNAPSHOT_TABLE_COLS
    KAFKA_TOPIC_METRIC_SNAPSHOT = KAFKA_TOPIC_METRIC_SNAPSHOT_TABLE_COLS
    POLYMARKET_BACKFILL_PRICE_CHANGE = BACKFILL_PRICE_CHANGE_TABLE_COLS
    POLYMARKET_BACKFILL_BOOK_SNAPSHOT = BACKFILL_BOOK_SNAPSHOT_TABLE_COLS
    FACT_BACKFILL_PRICE_HOURLY = FACT_BACKFILL_PRICE_HOURLY_TABLE_COLS
    FACT_BACKFILL_PRICE_DAILY = FACT_BACKFILL_PRICE_DAILY_TABLE_COLS
    FACT_BACKFILL_BOOK_HOURLY = FACT_BACKFILL_BOOK_HOURLY_TABLE_COLS
    FACT_BACKFILL_BOOK_DAILY = FACT_BACKFILL_BOOK_DAILY_TABLE_COLS


# ─────────────────────────────────────────
# IMPORT AND REGISTER STAR SCHEMA TABLES
# ─────────────────────────────────────────
from app.schemas.star_schema import STAR_SCHEMA_TABLES

# Combined list of ALL tables to create on init
CLICKHOUSE_TABLE_QUERIES = CLICKHOUSE_TABLE_QUERIES + STAR_SCHEMA_TABLES
