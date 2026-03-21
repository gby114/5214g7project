"""
Project: QF5214 Polymarket Data Pipeline
File: ingestion_service.py
Author: Xu
Created: 2026-03-14

Description:
Polymarket API ingestion service utilities
"""
import dataclasses
import random
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.config.settings import KAFKA_TOPIC_POLYMARKET_MARKETS_RAW
from app.clients.polymarket_client import polymarket_client
from app.clients.backfill_client import BackfillClient
from app.clients.clickhouse_client import ClickHouseClient
from app.clients.kafka_client import KafkaProducerClient
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

    @staticmethod
    def _market_to_plain_dict(market: object) -> dict:
        if dataclasses.is_dataclass(market) and not isinstance(market, type):
            return dataclasses.asdict(market)
        if isinstance(market, dict):
            return market
        raise TypeError(
            f"Unsupported market type {type(market)!r}; expected dataclass or dict"
        )

    # util 构造可选参数
    @staticmethod
    def _build_polymarket_markets_fetch_kwargs(
        *,
        query: str | None = None,
        slug: str | None = None,
        offset: int | None = None,
        sort: str = "volume",
        limit: int = 500,
        search_in: str | None = None,
    ) -> dict:
        fetch_kwargs: dict = {"sort": sort, "limit": limit}
        if query is not None:
            fetch_kwargs["query"] = query
        if slug is not None:
            fetch_kwargs["slug"] = slug
        if offset is not None:
            fetch_kwargs["offset"] = offset
        if search_in is not None:
            fetch_kwargs["search_in"] = search_in
        return fetch_kwargs

    @staticmethod
    def _market_id(market: object) -> str:
        if isinstance(market, dict):
            mid = market.get("market_id") or market.get("marketId")
        else:
            mid = getattr(market, "market_id", None) or getattr(
                market, "marketId", None
            )
        if mid is None:
            raise ValueError("Market payload is missing market_id / marketId")
        return str(mid)

    @classmethod
    def publish_polymarket_markets_to_kafka(
        cls,
        *,
        query: str | None = None,
        slug: str | None = None,
        offset: int | None = None,
        sort: str = "volume",
        limit: int = 2000,
        search_in: str | None = None,
        source: str = "pmxt",
        exchange: str = "polymarket",
        topic: str | None = None,
    ) -> int:
        """
        Fetch markets via PMXT and publish one JSON message per market to Kafka.

        Message envelope: schema_version, source, exchange, entity_type,
        captured_at (UTC ISO-8601), fetch_params, market_id, payload (raw dict).
        """
        fetch_kwargs = cls._build_polymarket_markets_fetch_kwargs(
            query=query,
            slug=slug,
            offset=offset,
            sort=sort,
            limit=limit,
            search_in=search_in,
        )
        topic_name = KAFKA_TOPIC_POLYMARKET_MARKETS_RAW

        logger.info(
            "Fetching Polymarket markets for Kafka publish (topic=%s): %s",
            topic_name,
            fetch_kwargs,
        )
        markets = polymarket_client.fetch_markets(**fetch_kwargs)
        if not markets:
            logger.warning("fetch_markets returned no markets; nothing sent to Kafka")
            return 0

        captured_at = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        messages: list[dict] = []
        for market in markets:
            market_id = cls._market_id(market)
            messages.append(
                {
                    "schema_version": 1,
                    "source": source,
                    "exchange": exchange,
                    "entity_type": "unified_market",
                    "captured_at": captured_at,
                    "fetch_params": fetch_kwargs,
                    "market_id": market_id,
                    "payload": cls._market_to_plain_dict(market),
                }
            )

        logger.info(
            "Publishing %s messages to Kafka topic '%s'",
            len(messages),
            topic_name,
        )
        with KafkaProducerClient() as producer:
            producer.send_batch(topic_name, messages, key_field="market_id")
            producer.flush()

        return len(messages)

ingestion_service = IngestionService()
