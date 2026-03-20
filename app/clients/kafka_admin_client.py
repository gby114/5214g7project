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
        Create Kafka topics if they do not already exist.

        Args:
            topics: List of topic configs.
                Example:
                [
                    {
                        "name": "topic_a",
                        "num_partitions": 4,
                        "replication_factor": 3,
                    }
                ]
        """
        logger.info("Creating Kafka topics")

        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="polymarket_kafka_admin",
        )

        try:
            existing_topics = set(admin_client.list_topics())

            logger.info(
                "Existing Kafka topics: %s",
                sorted(existing_topics),
            )

            topics_to_create: list[NewTopic] = []

            for topic in topics:
                logger.info("topic config = %s", topic)

                topic_name = topic["name"]
                num_partitions = topic["num_partitions"]
                replication_factor = topic["replication_factor"]

                if topic_name in existing_topics:
                    logger.info(
                        "Topic '%s' already exists, skipping",
                        topic_name,
                    )
                    continue

                logger.info(
                    "Preparing topic '%s' (partitions=%s, replication=%s)",
                    topic_name,
                    num_partitions,
                    replication_factor,
                )

                topics_to_create.append(
                    NewTopic(
                        name=topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                        topic_configs=topic.get("config", {}),
                    )
                )

            if not topics_to_create:
                logger.info("No new Kafka topics need to be created")
                return

            admin_client.create_topics(
                new_topics=topics_to_create,
                validate_only=False,
            )

            logger.info(
                "Kafka topics created successfully: %s",
                [topic.name for topic in topics_to_create],
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
