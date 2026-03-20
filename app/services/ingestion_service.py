"""
Project: QF5214 Polymarket Data Pipeline
File: ingestion_service.py
Author: Xu
Created: 2026-03-14

Description:
Polymarket API ingestion service utilities
"""
import random
import pandas as pd
from datetime import datetime, timedelta
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.clients.polymarket_client import PolymarketClient
from app.clients.backfill_client import BackfillClient
from app.clients.clickhouse_client import ClickHouseClient
from app.utils.common_utils import get_task_config, update_task_config


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

        ClickHouseClient().insert_rows(
            table="test_raw",
            rows=rows,
            column_names=CLICKHOUSE_TABLE_COLS_ENUM.TEST_RAW.value,
        )

        logger.info("Finished inserting test_raw data")

    @classmethod
    def injest_backfill_data(cls):
        """
        Ingest backfill data into ClickHouse.
        """
        logger.info("Generating backfill rows")

        task_config = get_task_config("backfill_orderbook", {
            "is_open": True,
            "start_time": datetime(2026, 3, 19, 22, 0, 0).isoformat(),
            "end_time": datetime(2026, 3, 20, 2, 0, 0).isoformat(),
        })
        logger.info(f"Task config: {task_config}")

        if not task_config["is_open"]:
            logger.info("Task is not open, skipping")
            return

        start = datetime.fromisoformat(task_config["start_time"])
        end = datetime.fromisoformat(task_config["end_time"])

        backfill_client = BackfillClient()

        urls = backfill_client.build_orderbook_urls(
            start_time=start,
            end_time=end,
        )

        logger.info("Generated URLs:")
        for u in urls:
            logger.info(u)

        first_url = urls[0]

        with backfill_client.download_as_tempfile(first_url) as temp_path:
            logger.info(f"Temp parquet file: {temp_path}")
            df = pd.read_parquet(temp_path)
            logger.info(df.head())

        logger.info("Finished inserting backfill data")


ingestion_service = IngestionService()
