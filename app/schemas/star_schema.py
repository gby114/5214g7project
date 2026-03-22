"""
Project: QF5214 Polymarket Data Pipeline
File: star_schema.py
Author: gby
Created: 2026-03-22

Description:
Star schema table definitions for aggregation layer.
Dimension tables and Fact tables for Polymarket data.
"""

# ─────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────

DIM_MARKET_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dim_market
(
    market_id       String,
    event_id        String,
    title           String,
    category        String,
    tags_json       String,
    updated_at      DateTime,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (market_id)
"""

DIM_OUTCOME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dim_outcome
(
    outcome_id      String,
    market_id       String,
    label           String,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
ORDER BY (outcome_id, market_id)
"""

DIM_SOURCE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dim_source
(
    source_id       String,
    source          String,
    exchange        String,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
ORDER BY (source_id)
"""

DIM_TIME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dim_time
(
    time_id         DateTime,
    hour            UInt8,
    day             UInt8,
    week            UInt8,
    month           UInt8,
    quarter         UInt8,
    year            UInt16,
    is_weekend      UInt8,
    day_of_week     String,
    ck_insert_time  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
ORDER BY (time_id)
"""

# ─────────────────────────────────────────
# FACT TABLES
# ─────────────────────────────────────────

FACT_MARKET_HOURLY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_market_snapshot_hourly
(
    source              String,
    exchange            String,
    market_id           String,
    hour_bucket         DateTime,
    avg_volume24h       Float64,
    max_volume24h       Float64,
    min_volume24h       Float64,
    avg_volume          Float64,
    max_volume          Float64,
    avg_liquidity       Float64,
    max_liquidity       Float64,
    avg_open_interest   Float64,
    snapshot_count      UInt64,
    ck_insert_time      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(hour_bucket)
ORDER BY (market_id, source, exchange, hour_bucket)
TTL hour_bucket + INTERVAL 90 DAY DELETE
"""

FACT_MARKET_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_market_snapshot_daily
(
    source              String,
    exchange            String,
    market_id           String,
    day_bucket          Date,
    avg_volume24h       Float64,
    max_volume24h       Float64,
    min_volume24h       Float64,
    avg_volume          Float64,
    max_volume          Float64,
    avg_liquidity       Float64,
    max_liquidity       Float64,
    avg_open_interest   Float64,
    snapshot_count      UInt64,
    ck_insert_time      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (market_id, source, exchange, day_bucket)
TTL day_bucket + INTERVAL 365 DAY DELETE
"""

FACT_OUTCOME_HOURLY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_outcome_snapshot_hourly
(
    source              String,
    exchange            String,
    market_id           String,
    outcome_id          String,
    label               String,
    hour_bucket         DateTime,
    open_price          Float64,
    close_price         Float64,
    high_price          Float64,
    low_price           Float64,
    avg_price           Float64,
    price_change        Float64,
    avg_price_change24h Float64,
    snapshot_count      UInt64,
    ck_insert_time      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toDate(hour_bucket)
ORDER BY (market_id, outcome_id, source, exchange, hour_bucket)
TTL hour_bucket + INTERVAL 90 DAY DELETE
"""

FACT_OUTCOME_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fact_outcome_snapshot_daily
(
    source              String,
    exchange            String,
    market_id           String,
    outcome_id          String,
    label               String,
    day_bucket          Date,
    open_price          Float64,
    close_price         Float64,
    high_price          Float64,
    low_price           Float64,
    avg_price           Float64,
    price_change        Float64 DEFAULT 0,
    avg_price_change24h Float64,
    snapshot_count      UInt64,
    ck_insert_time      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ck_insert_time)
PARTITION BY toYYYYMM(day_bucket)
ORDER BY (market_id, outcome_id, source, exchange, day_bucket)
TTL day_bucket + INTERVAL 365 DAY DELETE
"""

# ─────────────────────────────────────────
# ALL TABLES COMBINED
# ─────────────────────────────────────────

STAR_SCHEMA_TABLES = [
    DIM_TIME_TABLE_SQL,
    DIM_MARKET_TABLE_SQL,
    DIM_OUTCOME_TABLE_SQL,
    DIM_SOURCE_TABLE_SQL,
    FACT_MARKET_HOURLY_TABLE_SQL,
    FACT_MARKET_DAILY_TABLE_SQL,
    FACT_OUTCOME_HOURLY_TABLE_SQL,
    FACT_OUTCOME_DAILY_TABLE_SQL,
]


# ─────────────────────────────────────────
# TEST — run this file directly to test
# ─────────────────────────────────────────
if __name__ == "__main__":
    from app.clients.clickhouse_client import ClickHouseClient

    print("Creating all star schema tables...")
    client = ClickHouseClient()
    for sql in STAR_SCHEMA_TABLES:
        client.execute(sql)
    print("All star schema tables created successfully!")

    print("\nVerifying tables exist...")
    tables = client.query_rows("SHOW TABLES")
    for t in tables:
        print(" -", t)