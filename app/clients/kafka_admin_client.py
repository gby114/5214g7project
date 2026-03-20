"""
Project: QF5214 Polymarket Data Pipeline
File: kafka_admin_client.py
Author: Xu
Created: 2026-03-14

Description:
Kafka admin client for creating and managing topics.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from app.config.settings import KAFKA_BOOTSTRAP_SERVERS
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class KafkaAdminServiceClient:
    """
    Kafka admin client for topic management.
    """

    def __init__(self) -> None:
        self.bootstrap_servers = [
            server.strip()
            for server in KAFKA_BOOTSTRAP_SERVERS.split(",")
            if server.strip()
        ]

        logger.info(
            "Initializing KafkaAdminServiceClient (bootstrap_servers=%s)",
            self.bootstrap_servers,
        )

    def create_topics(
        self,
        topics: list[dict],
    ) -> None:
        """
        Create Kafka topics.

        Args:
            topics: list of topic configs
        """

        logger.info("Creating Kafka topics")

        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="polymarket_kafka_admin",
        )

        new_topics = [
            NewTopic(
                name=topic["name"],
                num_partitions=topic["num_partitions"],
                replication_factor=topic["replication_factor"],
            )
            for topic in topics
        ]

        for topic in topics:
            logger.info(
                "Preparing topic '%s' (partitions=%s, replication=%s)",
                topic["name"],
                topic["num_partitions"],
                topic["replication_factor"],
            )

        try:
            admin_client.create_topics(
                new_topics=new_topics,
                validate_only=False,
            )

            logger.info("Kafka topics created successfully")

        except TopicAlreadyExistsError:

            logger.warning(
                "Some Kafka topics already exist. Skipping creation."
            )

        except Exception as e:

            logger.error(
                "Failed to create Kafka topics: %s",
                str(e),
            )
            raise

        finally:
            admin_client.close()

            logger.info("Kafka admin client closed")
