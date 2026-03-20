"""
Project: QF5214 Polymarket Data Pipeline
File: bootstrap_service.py
Author: Xu
Created: 2026-03-14

Description:
Bootstrap service for initializing infrastructure resources.
"""

from app.clients.kafka_admin_client import KafkaAdminServiceClient
from app.clients.clickhouse_client import ClickHouseClient
from app.config.settings import (
    KAFKA_TOPIC_MARKETS,
    KAFKA_TOPIC_PRICES,
)
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class BootstrapService:
    """
    Service for bootstrapping infrastructure resources such as
    Kafka topics and ClickHouse tables.
    """

    def __init__(self) -> None:
        logger.info("Initializing BootstrapService")

        self.kafka_admin_client = KafkaAdminServiceClient()
        self.clickhouse_client = ClickHouseClient()

    def bootstrap_all(self) -> None:
        """
        Bootstrap all required infrastructure resources.
        """
        logger.info("Starting infrastructure bootstrap")

        topics = [
            {
                "name": KAFKA_TOPIC_MARKETS,
                "num_partitions": 3,
                "replication_factor": 3,
            },
            {
                "name": KAFKA_TOPIC_PRICES,
                "num_partitions": 3,
                "replication_factor": 3,
            },
        ]

        logger.info("Bootstrapping Kafka topics")
        self.kafka_admin_client.create_topics(topics)
        logger.info("Kafka topics bootstrap completed")

        logger.info("Bootstrapping ClickHouse tables")
        self.clickhouse_client.create_tables()
        logger.info("ClickHouse tables bootstrap completed")

        logger.info("Infrastructure bootstrap completed successfully")
