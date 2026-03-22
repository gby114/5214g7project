"""
Project: QF5214 Polymarket Data Pipeline
File: filter_service.py
Author: Xu
Created: 2026-03-22

Description:
Filter service for selecting target markets and outcomes
"""

from datetime import timedelta

from app.clients.clickhouse_client import ClickHouseClient
from app.logging.logger import setup_logger
from app.utils.time_utils import get_utc_now, round_datetime


logger = setup_logger(__name__)


class FilterService:
    """
    Service for filtering high-volume markets and storing target outcomes.
    """

    def __init__(self) -> None:
        logger.info("Initializing FilterService")
        self.clickhouse_client = ClickHouseClient()

    def refresh_target_market(
        self,
        window_minutes: int = 10,
        volume_threshold: int = 10000000,
    ) -> None:
        """
        Refresh target market table using recent snapshot data only.

        Logic:
            1. Read polymarket_market_snapshot within recent time window
            2. Filter market_id with volume > threshold
            3. Read polymarket_outcome_snapshot within recent time window
            4. Join by market_id
            5. Insert into polymarket_target_market

        Args:
            window_minutes: Recent time window in minutes
            volume_threshold: Volume threshold
        """
        logger.info(
            "Start refreshing polymarket_target_market window_minutes=%s volume_threshold=%s",
            window_minutes,
            volume_threshold,
        )

        end_time = round_datetime(get_utc_now(), "minute")
        start_time = end_time - timedelta(minutes=window_minutes)

        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            "Filtering target market with time window start_time=%s end_time=%s",
            start_time_str,
            end_time_str,
        )

        query = f"""
            INSERT INTO polymarket_target_market
            SELECT
                o.market_id,
                o.outcome_id,
                m.volume,
                now() AS ck_insert_time
            FROM
            (
                SELECT
                    market_id,
                    outcome_id
                FROM polymarket_outcome_snapshot
                WHERE ck_insert_time >= toDateTime('{start_time_str}')
                AND ck_insert_time < toDateTime('{end_time_str}')
                GROUP BY
                    market_id,
                    outcome_id
            ) AS o
            ANY INNER JOIN
            (
                SELECT
                    market_id,
                    volume
                FROM
                (
                    SELECT
                        market_id,
                        max(volume) AS volume
                    FROM polymarket_market_snapshot
                    WHERE ck_insert_time >= toDateTime('{start_time_str}')
                    AND ck_insert_time < toDateTime('{end_time_str}')
                    GROUP BY market_id
                ) AS market_volume
                WHERE volume > {volume_threshold}
            ) AS m
            ON o.market_id = m.market_id
            """

        logger.info("Executing ClickHouse insert for polymarket_target_market")

        try:
            self.clickhouse_client.command(query)
            logger.info("Refresh polymarket_target_market success")
        except Exception as e:
            logger.error("Refresh polymarket_target_market failed: %s", str(e))
            raise


filter_service = FilterService()
