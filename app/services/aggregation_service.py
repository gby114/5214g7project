"""
Project: QF5214 Polymarket Data Pipeline
File: aggregation_service.py
Author: Xu
Created: 2026-03-14

Description:
Aggregation service utilities
"""


from app.clients.clickhouse_client import ClickHouseClient
import random
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
    """

    @classmethod
    def aggregate_test_raw_data(cls):
        """
        Aggregate fake test raw data into hourly ClickHouse table.
        """
        logger.info("Aggregating test raw data starting")

        end_time = get_utc_now()
        start_time = end_time - timedelta(hours=5)

        # ClickHouse SQL
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

        # Add insert time
        now = get_utc_now()
        for row in rows:
            row["ck_insert_time"] = now

        # Write to hourly table
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

    @classmethod
    def aggregate_placeholder1_data(cls):
        """
        Aggregate placeholder1 data into hourly ClickHouse table.
        """
        logger.info("Aggregating placeholder1 data starting")

    @classmethod
    def aggregate_placeholder2_data(cls):
        """
        Aggregate placeholder2 data into hourly ClickHouse table.
        """
        logger.info("Aggregating placeholder2 data starting")

    @classmethod
    def aggregate_placeholder3_data(cls):
        """
        Aggregate placeholder3 data into hourly ClickHouse table.
        """
        logger.info("Aggregating placeholder3 data starting")

    @classmethod
    def aggregate_placeholder4_data(cls):
        """
        Aggregate placeholder4 data into hourly ClickHouse table.
        """
        logger.info("Aggregating placeholder4 data starting")






aggregation_service = AggregationService()
