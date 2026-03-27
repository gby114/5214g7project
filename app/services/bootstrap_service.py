"""
Project: QF5214 Polymarket Data Pipeline
File: bootstrap_service.py
Author: Xu
Created: 2026-03-14

Description:
Bootstrap service for initializing infrastructure resources
"""


from app.clients.kafka_admin_client import KafkaAdminServiceClient
from app.clients.clickhouse_client import ClickHouseClient
from app.clients.mysql_client import MySQLClient
from app.config.settings import (
    KAFKA_TOPIC_POLYMARKET_MARKETS_RAW,
    KAFKA_TOPIC_MARKETS,
    KAFKA_TOPIC_PRICES,
    KAFKA_TOPIC_1,
    KAFKA_TOPIC_2,
    KAFKA_TOPIC_3,
    KAFKA_TOPIC_4,
    KAFKA_TOPIC_7,
    KAFKA_TOPIC_8,
    KAFKA_TOPIC_9,
)
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class BootstrapService:
    """
    Service for bootstrapping infrastructure resources:
    - Kafka topics
    - ClickHouse tables
    - MySQL tables
    """

    def __init__(self) -> None:
        logger.info("Initializing BootstrapService")

        # Kafka admin client 可以保留（它不是长连接问题）
        self.kafka_admin_client = KafkaAdminServiceClient()

    def bootstrap_all(self) -> None:
        """
        Bootstrap all required infrastructure resources.
        """
        logger.info("Starting infrastructure bootstrap")

        self._bootstrap_kafka()
        self._bootstrap_clickhouse()
        self._bootstrap_mysql()
        self._bootstrap_clickhouse_import()

        logger.info("Infrastructure bootstrap completed successfully")

    # ===============================
    # Kafka
    # ===============================
    def _bootstrap_kafka(self) -> None:
        topics = [
            {"name": KAFKA_TOPIC_POLYMARKET_MARKETS_RAW, "num_partitions": 3, "replication_factor": 3},
            {"name": KAFKA_TOPIC_MARKETS, "num_partitions": 3, "replication_factor": 3},
            {"name": KAFKA_TOPIC_PRICES, "num_partitions": 3, "replication_factor": 3},
            {"name": KAFKA_TOPIC_1, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_2, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_3, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_4, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_7, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_8, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
            {"name": KAFKA_TOPIC_9, "num_partitions": 4, "replication_factor": 3, "config": {"retention.ms": "86400000"}},
        ]

        logger.info("Bootstrapping Kafka topics")
        self.kafka_admin_client.create_topics(topics)
        logger.info("Kafka topics bootstrap completed")

    # ===============================
    # ClickHouse
    # ===============================
    def _bootstrap_clickhouse(self) -> None:
        logger.info("Bootstrapping ClickHouse tables")

        client = ClickHouseClient()
        client.create_tables()

        logger.info("ClickHouse tables bootstrap completed")

    # ===============================
    # MySQL
    # ===============================
    def _bootstrap_mysql(self) -> None:
        logger.info("Bootstrapping MySQL tables")

        client = MySQLClient()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS task_config (
            task_name VARCHAR(255) PRIMARY KEY,
            config JSON NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP
        );
        """

        client.execute(create_table_sql)

        logger.info("MySQL tables bootstrap completed")

    # ===============================
    # ClickHouse Import
    # ===============================
    def _bootstrap_clickhouse_import(self) -> None:
        """
        Import ClickHouse business data from local files.
        """
        logger.info("Bootstrapping ClickHouse data import")

        client = ClickHouseClient()

        tables_to_import = [
            "polymarket_backfill_book_snapshot",
            "polymarket_backfill_price_change",
            "polymarket_target_market",
        ]

        for table in tables_to_import:
            try:
                logger.info("Importing table data table=%s", table)
                client.import_table_data(table)
                logger.info("Imported table data successfully table=%s", table)

            except Exception as e:
                logger.error(
                    "Failed to import table data table=%s error=%s",
                    table,
                    str(e),
                )

        logger.info("ClickHouse data import bootstrap completed")
