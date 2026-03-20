"""
Project: QF5214 Polymarket Data Pipeline
File: ingestion_service.py
Author: Xu
Created: 2026-03-14

Description:
Polymarket API ingestion service utilities
"""
from app.clients.clickhouse_client import clickhouse_client
import random
from datetime import datetime, timedelta
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM

logger = setup_logger(__name__)


class IngestionService:
    """
    Service for ingesting data from various sources.
    """

    @classmethod
    def insert_test_raw_data(cls, n: int = 1):
        """
        Ingest fake test raw data into ClickHouse.

        Args:
            n: number of rows to generate
        """
        logger.info(f"Generating {n} fake test_raw rows")

        now = datetime.utcnow()

        sources = ["pmxt", "polymarket", "kalshi"]
        exchanges = ["poly", "kalshi", "limitless"]

        rows = []

        for _ in range(n):
            row = {
                "source": random.choice(sources),
                "exchange": random.choice(exchanges),
                "entity_id": f"entity_{random.randint(1, 10)}",
                "metric_value": round(random.uniform(0, 100), 4),
                "metric_count": random.randint(1, 1000),
                "ck_insert_time": now - timedelta(seconds=random.randint(0, 3600)),
            }
            rows.append(row)

        logger.info(f"Inserting {len(rows)} rows into test_raw")

        clickhouse_client.insert_rows(
            table="test_raw",
            rows=rows,
            column_names=CLICKHOUSE_TABLE_COLS_ENUM.TEST_RAW.value,
        )

        logger.info("Finished inserting test_raw data")


ingestion_service = IngestionService()
