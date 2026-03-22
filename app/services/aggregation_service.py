"""
Project: QF5214 Polymarket Data Pipeline
File: aggregation_service.py
Description: Aggregation service for processing and transforming
             Polymarket raw data into star schema fact/dim tables.
"""

from app.clients.clickhouse_client import ClickHouseClient
from datetime import datetime, timedelta
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.utils.time_utils import (
    get_now,
    get_utc_now,
    round_datetime,
    str_to_datetime,
    datetime_to_str
)

logger = setup_logger(__name__)


class AggregationService:
    """
    Aggregation service for processing and transforming data.
    Implements star schema with fact and dimension tables.
    """

    # ─────────────────────────────────────────
    # EXISTING TEST METHOD (keep as is)
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_test_raw_data(cls):
        """
        Aggregate fake test raw data into hourly ClickHouse table.
        """
        logger.info("Aggregating test raw data starting")

        end_time = get_utc_now()
        start_time = end_time - timedelta(hours=5)

        query = f"""
            SELECT
                entity_id,
                exchange,
                toStartOfHour(ck_insert_time) AS hour_bucket,
                avg(metric_value) AS avg_value,
                min(metric_value) AS min_value,
                max(metric_value) AS max_value,
                sum(metric_count) AS total_count
            FROM test_raw
            WHERE ck_insert_time >= toDateTime('{start_time.strftime("%Y-%m-%d %H:%M:%S")}')
            AND ck_insert_time < toDateTime('{end_time.strftime("%Y-%m-%d %H:%M:%S")}')
            GROUP BY
                entity_id,
                exchange,
                hour_bucket
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No data to aggregate")
            return

        logger.info("Aggregated %s rows", len(rows))

        now = get_utc_now()
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="test_raw_hour",
            rows=rows,
            column_names=[
                "entity_id",
                "exchange",
                "hour_bucket",
                "avg_value",
                "min_value",
                "max_value",
                "total_count",
                "ck_insert_time",
            ],
        )

        logger.info("Aggregating test raw data finished")

    # ─────────────────────────────────────────
    # DIMENSION TABLE POPULATION
    # ─────────────────────────────────────────

    @classmethod
    def populate_dim_market(cls):
        """
        Populate dim_market from polymarket_market_dim.
        Dimension table for star schema — market metadata.
        """
        logger.info("Populating dim_market starting")

        query = """
            SELECT
                market_id,
                event_id,
                title,
                category,
                tags_json,
                updated_at
            FROM polymarket_market_dim
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No market dim data found")
            return

        now = get_utc_now()
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="dim_market",
            rows=rows,
            column_names=[
                "market_id",
                "event_id",
                "title",
                "category",
                "tags_json",
                "updated_at",
                "ck_insert_time",
            ],
        )

        logger.info("Populating dim_market finished: %s rows", len(rows))

    @classmethod
    def populate_dim_outcome(cls):
        """
        Populate dim_outcome from distinct outcomes
        in polymarket_outcome_snapshot.
        Dimension table for star schema — outcome metadata.
        """
        logger.info("Populating dim_outcome starting")

        query = """
            SELECT DISTINCT
                outcome_id,
                market_id,
                label
            FROM polymarket_outcome_snapshot
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No outcome dim data found")
            return

        now = get_utc_now()
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="dim_outcome",
            rows=rows,
            column_names=[
                "outcome_id",
                "market_id",
                "label",
                "ck_insert_time",
            ],
        )

        logger.info("Populating dim_outcome finished: %s rows", len(rows))

    @classmethod
    def populate_dim_source(cls):
        """
        Populate dim_source from distinct sources
        in polymarket_market_snapshot.
        Dimension table for star schema — source/exchange metadata.
        """
        logger.info("Populating dim_source starting")

        query = """
            SELECT DISTINCT
                concat(source, '_', exchange) AS source_id,
                source,
                exchange
            FROM polymarket_market_snapshot
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No source dim data found")
            return

        now = round_datetime(get_utc_now(), "day")
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="dim_source",
            rows=rows,
            column_names=[
                "source_id",
                "source",
                "exchange",
                "ck_insert_time",
            ],
        )

        logger.info("Populating dim_source finished: %s rows", len(rows))

    # ─────────────────────────────────────────
    # PLACEHOLDER 1 — MARKET HOURLY AGGREGATION
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_placeholder1_data(cls):
        """
        Aggregate polymarket market snapshots into hourly fact table.
        FACT TABLE: fact_market_snapshot_hourly
        """
        logger.info("Aggregating placeholder1 data starting")

        end_time = round_datetime(get_utc_now(), "hour")
        start_time = end_time - timedelta(hours=1)

        query = f"""
            SELECT
                source,
                exchange,
                market_id,
                toStartOfHour(captured_at)  AS hour_bucket,
                avg(volume24h)              AS avg_volume24h,
                max(volume24h)              AS max_volume24h,
                min(volume24h)              AS min_volume24h,
                avg(volume)                 AS avg_volume,
                max(volume)                 AS max_volume,
                avg(liquidity)              AS avg_liquidity,
                max(liquidity)              AS max_liquidity,
                avg(open_interest)          AS avg_open_interest,
                count()                     AS snapshot_count
            FROM polymarket_market_snapshot
            WHERE captured_at >= toDateTime('{start_time.strftime("%Y-%m-%d %H:%M:%S")}')
              AND captured_at <  toDateTime('{end_time.strftime("%Y-%m-%d %H:%M:%S")}')
            GROUP BY
                source, exchange, market_id, hour_bucket
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No market hourly data to aggregate")
            return

        now = end_time
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="fact_market_snapshot_hourly",
            rows=rows,
            column_names=[
                "source",
                "exchange",
                "market_id",
                "hour_bucket",
                "avg_volume24h",
                "max_volume24h",
                "min_volume24h",
                "avg_volume",
                "max_volume",
                "avg_liquidity",
                "max_liquidity",
                "avg_open_interest",
                "snapshot_count",
                "ck_insert_time",
            ],
        )

        logger.info("Aggregating placeholder1 data finished: %s rows", len(rows))

    # ─────────────────────────────────────────
    # PLACEHOLDER 2 — OUTCOME HOURLY AGGREGATION
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_placeholder2_data(cls):
        """
        Aggregate polymarket outcome snapshots into hourly fact table.
        FACT TABLE: fact_outcome_snapshot_hourly
        """
        logger.info("Aggregating placeholder2 data starting")

        end_time = round_datetime(get_utc_now(), "hour")
        start_time = end_time - timedelta(hours=1)

        query = f"""
            SELECT
                source,
                exchange,
                market_id,
                outcome_id,
                label,
                toStartOfHour(captured_at)          AS hour_bucket,
                argMin(price, captured_at)           AS open_price,
                argMax(price, captured_at)           AS close_price,
                max(price)                           AS high_price,
                min(price)                           AS low_price,
                avg(price)                           AS avg_price,
                argMax(price, captured_at)
                    - argMin(price, captured_at)     AS price_change,
                avg(price_change24h)                 AS avg_price_change24h,
                count()                              AS snapshot_count
            FROM polymarket_outcome_snapshot
            WHERE captured_at >= toDateTime('{start_time.strftime("%Y-%m-%d %H:%M:%S")}')
              AND captured_at <  toDateTime('{end_time.strftime("%Y-%m-%d %H:%M:%S")}')
            GROUP BY
                source, exchange, market_id,
                outcome_id, label, hour_bucket
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No outcome hourly data to aggregate")
            return

        now = end_time
        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="fact_outcome_snapshot_hourly",
            rows=rows,
            column_names=[
                "source",
                "exchange",
                "market_id",
                "outcome_id",
                "label",
                "hour_bucket",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "price_change",
                "avg_price",
                "avg_price_change24h",
                "snapshot_count",
                "ck_insert_time",
            ],
        )

        logger.info("Aggregating placeholder2 data finished: %s rows", len(rows))

    # ─────────────────────────────────────────
    # PLACEHOLDER 3 — MARKET DAILY AGGREGATION
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_placeholder3_data(cls):
        """
        Roll up hourly market fact data into daily fact table.
        FACT TABLE: fact_market_snapshot_daily
        Reads from: fact_market_snapshot_hourly
        """
        logger.info("Aggregating placeholder3 data starting")

        now = round_datetime(get_utc_now(), "day")
        target = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        query = f"""
            SELECT
                source,
                exchange,
                market_id,
                toDate(hour_bucket)         AS day_bucket,
                avg(avg_volume24h)          AS avg_volume24h,
                max(max_volume24h)          AS max_volume24h,
                min(min_volume24h)          AS min_volume24h,
                avg(avg_volume)             AS avg_volume,
                max(max_volume)             AS max_volume,
                avg(avg_liquidity)          AS avg_liquidity,
                max(max_liquidity)          AS max_liquidity,
                avg(avg_open_interest)      AS avg_open_interest,
                sum(snapshot_count)         AS snapshot_count
            FROM fact_market_snapshot_hourly
            WHERE toDate(hour_bucket) = toDate('{target.strftime("%Y-%m-%d")}')
            GROUP BY
                source, exchange, market_id, day_bucket
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No market daily data to aggregate")
            return

        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="fact_market_snapshot_daily",
            rows=rows,
            column_names=[
                "source",
                "exchange",
                "market_id",
                "day_bucket",
                "avg_volume24h",
                "max_volume24h",
                "min_volume24h",
                "avg_volume",
                "max_volume",
                "avg_liquidity",
                "max_liquidity",
                "avg_open_interest",
                "snapshot_count",
                "ck_insert_time",
            ],
        )

        logger.info("Aggregating placeholder3 data finished: %s rows", len(rows))

    # ─────────────────────────────────────────
    # PLACEHOLDER 4 — OUTCOME DAILY AGGREGATION
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_placeholder4_data(cls):
        """
        Roll up hourly outcome fact data into daily fact table.
        FACT TABLE: fact_outcome_snapshot_daily
        Reads from: fact_outcome_snapshot_hourly
        """
        logger.info("Aggregating placeholder4 data starting")

        now = round_datetime(get_utc_now(), "day")
        target = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        query = f"""
            SELECT
                source,
                exchange,
                market_id,
                outcome_id,
                label,
                toDate(hour_bucket)         AS day_bucket,
                min(open_price)             AS open_price,
                max(close_price)            AS close_price,
                max(high_price)             AS high_price,
                min(low_price)              AS low_price,
                avg(avg_price)              AS avg_price,
                avg(avg_price_change24h)    AS avg_price_change24h,
                sum(snapshot_count)         AS snapshot_count
            FROM fact_outcome_snapshot_hourly
            WHERE toDate(hour_bucket) = toDate('{target.strftime("%Y-%m-%d")}')
            GROUP BY
                source, exchange, market_id,
                outcome_id, label, day_bucket
        """

        rows = ClickHouseClient().query_rows(query)

        if not rows:
            logger.info("No outcome daily data to aggregate")
            return

        for row in rows:
            row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="fact_outcome_snapshot_daily",
            rows=rows,
            column_names=[
                "source",
                "exchange",
                "market_id",
                "outcome_id",
                "label",
                "day_bucket",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "avg_price",
                "avg_price_change24h",
                "snapshot_count",
                "ck_insert_time",
            ],
        )

        logger.info("Aggregating placeholder4 data finished: %s rows", len(rows))

    # ─────────────────────────────────────────
    # BACKFILL DATA AGGREGATION METHODS
    # ─────────────────────────────────────────

    @classmethod
    def aggregate_backfill_price_hourly(cls):
        """
        [1/4] Price Change Hourly Aggregation
        Processes raw trade stream from polymarket_backfill_price_change into OHLCV.
        """
        logger.info("Starting Price Change Hourly aggregation...")
        end_time = round_datetime(get_utc_now(), "hour")
        start_time = end_time - timedelta(hours=24) 

        query = f"""
            SELECT
                market_id,
                token_id,
                toStartOfHour(ck_insert_time) AS hour_bucket,
                argMin(change_price, event_timestamp) AS open_price,
                argMax(change_price, event_timestamp) AS close_price,
                max(change_price) AS high_price,
                min(change_price) AS low_price,
                sum(change_price * change_size) AS total_volume,
                count() AS trade_count,
                -- Breakdown by side to analyze market sentiment
                sumIf(change_size, change_side = 'BUY') AS buy_volume,
                sumIf(change_size, change_side = 'SELL') AS sell_volume
            FROM polymarket_backfill_price_change
            WHERE ck_insert_time >= toDateTime('{start_time.strftime("%Y-%m-%d %H:%M:%S")}')
              AND ck_insert_time <  toDateTime('{end_time.strftime("%Y-%m-%d %H:%M:%S")}')
            GROUP BY market_id, token_id, hour_bucket
        """
        rows = ClickHouseClient().query_rows(query)
        if not rows:
            logger.info("No price change data found for the current window.")
            return

        now = get_utc_now()
        for row in rows: row["ck_insert_time"] = now

        ClickHouseClient().insert_rows(
            table="fact_backfill_price_hourly",
            rows=rows,
            column_names=[
                "market_id", "token_id", "hour_bucket", "open_price", "close_price",
                "high_price", "low_price", "total_volume", "trade_count", 
                "buy_volume", "sell_volume", "ck_insert_time"
            ]
        )
        logger.info("Successfully aggregated %s price hourly rows", len(rows))

    @classmethod
    def aggregate_backfill_price_daily(cls):
        logger.info("Starting Price Change Daily roll-up...")
        now = get_utc_now() 
        lookback_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                market_id,
                token_id,
                toDate(hour_bucket) AS day_bucket,
                argMin(open_price, hour_bucket) AS open_price,
                argMax(close_price, hour_bucket) AS close_price,
                max(high_price) AS high_price,
                min(low_price) AS low_price,
                sum(total_volume) AS daily_volume,
                sum(trade_count) AS daily_trades,
                sum(buy_volume) - sum(sell_volume) AS net_flow
            FROM fact_backfill_price_hourly
            WHERE toDate(hour_bucket) >= '{lookback_date}'  
            GROUP BY market_id, token_id, day_bucket
        """
        rows = ClickHouseClient().query_rows(query)
        if rows:
            for row in rows: row["ck_insert_time"] = now
            ClickHouseClient().insert_rows(
                table="fact_backfill_price_daily",
                rows=rows,
                column_names=["market_id", "token_id", "day_bucket", "open_price", "close_price", 
                              "high_price", "low_price", "daily_volume", "daily_trades", "net_flow", "ck_insert_time"]
            )

    @classmethod
    def aggregate_backfill_book_hourly(cls):
        """
        [3/4] Book Snapshot Hourly Aggregation
        Analyzes market depth health, average spreads, and price volatility.
        """
        logger.info("Starting Book Snapshot Hourly aggregation...")
        end_time = round_datetime(get_utc_now(), "hour")
        start_time = end_time - timedelta(hours=24)

        query = f"""
            SELECT
                market_id,
                token_id,
                toStartOfHour(ck_insert_time) AS hour_bucket,
                avg(best_ask - best_bid) AS avg_spread,
                avg((best_bid + best_ask) / 2) AS avg_mid_price,
                stddevPop((best_bid + best_ask) / 2) AS price_volatility,
                count() AS snapshot_count
            FROM polymarket_backfill_book_snapshot
            WHERE ck_insert_time >= toDateTime('{start_time.strftime("%Y-%m-%d %H:%M:%S")}')
              AND ck_insert_time <  toDateTime('{end_time.strftime("%Y-%m-%d %H:%M:%S")}')
            GROUP BY market_id, token_id, hour_bucket
        """
        rows = ClickHouseClient().query_rows(query)
        if rows:
            now = get_utc_now()
            for row in rows: row["ck_insert_time"] = now
            ClickHouseClient().insert_rows(
                table="fact_backfill_book_hourly",
                rows=rows,
                column_names=["market_id", "token_id", "hour_bucket", "avg_spread", 
                              "avg_mid_price", "price_volatility", "snapshot_count", "ck_insert_time"]
            )

    @classmethod
    def aggregate_backfill_book_daily(cls):
        logger.info("Starting Book Snapshot Daily roll-up...")
        now = get_utc_now() 
        lookback_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

        query = f"""
            SELECT
                market_id,
                token_id,
                toDate(hour_bucket) AS day_bucket,
                avg(avg_spread) AS avg_daily_spread,
                avg(price_volatility) AS avg_daily_volatility,
                max(avg_spread) AS max_daily_spread,
                sum(snapshot_count) AS snapshot_count
            FROM fact_backfill_book_hourly
            WHERE toDate(hour_bucket) >= '{lookback_date}'
            GROUP BY market_id, token_id, day_bucket
        """
        rows = ClickHouseClient().query_rows(query)
        if rows:
            for row in rows: row["ck_insert_time"] = now
            ClickHouseClient().insert_rows(
                table="fact_backfill_book_daily",
                rows=rows,
                column_names=["market_id", "token_id", "day_bucket", "avg_daily_spread", 
                              "avg_daily_volatility", "max_daily_spread", "snapshot_count", "ck_insert_time"]
            )


# Single instance to import elsewhere
aggregation_service = AggregationService()

# ─────────────────────────────────────────
# TEST — run this file directly to test
# ─────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 40)
    print("Test 1 — Creating star schema tables...")
    from app.schemas.star_schema import STAR_SCHEMA_TABLES
    from app.clients.clickhouse_client import ClickHouseClient
    client = ClickHouseClient()
    for sql in STAR_SCHEMA_TABLES:
        client.command(sql)
    print("Tables created!")

    print("=" * 40)
    print("Test 2 — Market hourly aggregation...")
    AggregationService.aggregate_placeholder1_data()

    print("=" * 40)
    print("Test 3 — Outcome hourly aggregation...")
    AggregationService.aggregate_placeholder2_data()

    print("=" * 40)
    print("Test 4 — Market daily aggregation...")
    AggregationService.aggregate_placeholder3_data()

    print("=" * 40)
    print("Test 5 — Outcome daily aggregation...")
    AggregationService.aggregate_placeholder4_data()

    print("=" * 40)
    print("All tests done!")

    # ─────────────────────────────────────────
    # history data
    # —————————————————————————————————————————

    from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_QUERIES
    

    # ─────────────────────────────────────────
    # STEP 1: Database Initialization
    # ─────────────────────────────────────────
    print("=" * 50)
    print("Initializing ClickHouse Tables...")
    for sql in CLICKHOUSE_TABLE_QUERIES:
        # Using command() for DDL statements (CREATE TABLE)
        client.command(sql)
    print("Initialization complete. All tables verified.")

    # ─────────────────────────────────────────
    # STEP 2: Aggregate Price Change Data
    # ─────────────────────────────────────────
    print("\n" + "=" * 50)
    print("Running Price Change Aggregations")
    print("=" * 50)
    
    # IMPORTANT: Run Hourly first, because Daily depends on it (Roll-up)
    print("-> Aggregating: Price Hourly...")
    AggregationService.aggregate_backfill_price_hourly()
    
    print("-> Aggregating: Price Daily...")
    AggregationService.aggregate_backfill_price_daily()

    # ─────────────────────────────────────────
    # STEP 3: Aggregate Book Snapshot Data
    # ─────────────────────────────────────────
    print("\n" + "=" * 50)
    print("Running Book Snapshot Aggregations")
    print("=" * 50)
    
    print("-> Aggregating: Book Hourly...")
    AggregationService.aggregate_backfill_book_hourly()
    
    print("-> Aggregating: Book Daily...")
    AggregationService.aggregate_backfill_book_daily()

    print("\n" + "=" * 50)
    print("All backfill aggregation tasks finished!")
    print("=" * 50)