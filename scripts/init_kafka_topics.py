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
    KAFKA_TOPIC_1,
    KAFKA_TOPIC_2,
    KAFKA_TOPIC_3,
    KAFKA_TOPIC_4,
    KAFKA_TOPIC_7,
    KAFKA_TOPIC_8,
    KAFKA_TOPIC_9,
)


def main() -> None:
    client = KafkaAdminServiceClient()

    topics = [
        {
            "name": KAFKA_TOPIC_MARKETS,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_PRICES,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_1,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_2,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_3,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_4,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_7,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_8,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
        {
            "name": KAFKA_TOPIC_9,
            "num_partitions": 4,
            "replication_factor": 3,
            "config": {
                "retention.ms": "86400000",
            },
        },
    ]

    client.create_topics(topics)


if __name__ == "__main__":
    main()
