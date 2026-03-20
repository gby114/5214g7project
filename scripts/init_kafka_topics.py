"""
Project: QF5214 Polymarket Data Pipeline
File: init_kafka_topics.py
Author: Xu
Created: 2026-03-14

Description:
Initialize Kafka topics for the project.
"""

from app.clients.kafka_admin_client import KafkaAdminServiceClient
from app.config.settings import (
    KAFKA_TOPIC_MARKETS,
    KAFKA_TOPIC_PRICES,
)


def main() -> None:
    client = KafkaAdminServiceClient()

    topics = [
        {
            "name": KAFKA_TOPIC_MARKETS,
            "num_partitions": 4,
            "replication_factor": 3,
        },
        {
            "name": KAFKA_TOPIC_PRICES,
            "num_partitions": 4,
            "replication_factor": 3,
        },
    ]

    client.create_topics(topics)


if __name__ == "__main__":
    main()
