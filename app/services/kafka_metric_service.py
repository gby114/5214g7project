"""
Project: QF5214 Polymarket Data Pipeline
File: kafka_metric_service.py
Author: Xu
Created: 2026-03-21

Description:
Collect Kafka cluster/topic metrics and write snapshot data to ClickHouse.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from app.config.settings import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USERNAME,
    KAFKA_BOOTSTRAP_SERVERS,
)
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class KafkaMetricService:
    """
    Collect Kafka metrics and persist snapshots into ClickHouse.
    """

    def __init__(self) -> None:
        self.bootstrap_servers = [
            server.strip()
            for server in KAFKA_BOOTSTRAP_SERVERS.split(",")
            if server.strip()
        ]
        self.cluster_name = "polymarket_kafka"

        logger.info(
            "Initializing KafkaMetricService (bootstrap_servers=%s)",
            self.bootstrap_servers,
        )

    def _get_admin_client(self) -> KafkaAdminClient:
        return KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="polymarket_kafka_metric_admin",
        )

    def _get_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=False,
            request_timeout_ms=20000,
            consumer_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )

    def _get_clickhouse_client(self):
        return clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

    def _safe_datetime_now(self) -> datetime:
        return datetime.now()

    def _build_partition_offset_map(
        self,
        consumer: KafkaConsumer,
        topic_partitions: list[TopicPartition],
    ) -> tuple[dict[TopicPartition, int], dict[TopicPartition, int]]:
        """
        Return beginning_offsets and end_offsets for all topic partitions.
        """
        if not topic_partitions:
            return {}, {}

        try:
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            end_offsets = consumer.end_offsets(topic_partitions)
            return beginning_offsets, end_offsets
        except Exception as e:
            logger.exception("Failed to fetch partition offsets: %s", str(e))
            return {}, {}

    def _describe_topics(
        self,
        admin_client: KafkaAdminClient,
        topic_names: list[str],
    ) -> list[dict]:
        """
        Describe topics via kafka admin client.
        """
        if not topic_names:
            return []

        try:
            return admin_client.describe_topics(topic_names)
        except Exception as e:
            logger.exception("Failed to describe Kafka topics: %s", str(e))
            raise

    def collect_all(self) -> None:
        """
        Collect both cluster-level and topic-level Kafka metrics.
        """
        logger.info("Start collecting Kafka metrics")

        admin_client = self._get_admin_client()
        consumer = self._get_consumer()
        clickhouse_client = self._get_clickhouse_client()

        try:
            collected_at = self._safe_datetime_now()
            collected_date = collected_at.date()
            collected_hour = collected_at.replace(
                minute=0,
                second=0,
                microsecond=0,
            )

            topic_names = sorted(admin_client.list_topics())

            logger.info("Fetched %s Kafka topics", len(topic_names))

            topic_descriptions = self._describe_topics(
                admin_client=admin_client,
                topic_names=topic_names,
            )

            all_topic_partitions: list[TopicPartition] = []

            for topic_desc in topic_descriptions:
                topic_name = topic_desc["topic"]
                partitions = topic_desc.get("partitions", [])

                for partition in partitions:
                    all_topic_partitions.append(
                        TopicPartition(
                            topic=topic_name,
                            partition=partition["partition"],
                        )
                    )

            beginning_offsets, end_offsets = self._build_partition_offset_map(
                consumer=consumer,
                topic_partitions=all_topic_partitions,
            )

            topic_rows: list[list] = []
            cluster_agg = {
                "broker_count": 0,
                "topic_count": 0,
                "internal_topic_count": 0,
                "partition_count": 0,
                "under_replicated_partition_count": 0,
                "offline_partition_count": 0,
                "estimated_message_count": 0,
            }

            broker_ids = set()

            for topic_desc in topic_descriptions:
                topic_name = topic_desc["topic"]
                is_internal = 1 if topic_name.startswith("__") else 0
                partitions = topic_desc.get("partitions", [])

                partition_count = len(partitions)
                replication_factors = []
                isr_counts = []
                under_replicated_partition_count = 0
                offline_partition_count = 0
                log_start_offset_sum = 0
                log_end_offset_sum = 0
                estimated_message_count = 0

                for partition in partitions:
                    partition_id = partition["partition"]
                    replicas = partition.get("replicas", [])
                    isr = partition.get("isr", [])
                    leader = partition.get("leader", -1)

                    for broker_id in replicas:
                        broker_ids.add(broker_id)
                    if leader not in (-1, None):
                        broker_ids.add(leader)

                    replication_factor = len(replicas)
                    isr_count = len(isr)

                    replication_factors.append(replication_factor)
                    isr_counts.append(isr_count)

                    if leader in (-1, None):
                        offline_partition_count += 1

                    if isr_count < replication_factor:
                        under_replicated_partition_count += 1

                    tp = TopicPartition(topic_name, partition_id)
                    begin_offset = int(beginning_offsets.get(tp, 0))
                    end_offset = int(end_offsets.get(tp, 0))

                    log_start_offset_sum += begin_offset
                    log_end_offset_sum += end_offset
                    estimated_message_count += max(end_offset - begin_offset, 0)

                replication_factor_value = (
                    max(replication_factors) if replication_factors else 0
                )
                min_isr_count = min(isr_counts) if isr_counts else 0
                avg_isr_count = (
                    sum(isr_counts) / len(isr_counts) if isr_counts else 0.0
                )

                topic_rows.append(
                    [
                        collected_at,
                        collected_date,
                        collected_hour,
                        self.cluster_name,
                        topic_name,
                        is_internal,
                        partition_count,
                        replication_factor_value,
                        min_isr_count,
                        avg_isr_count,
                        under_replicated_partition_count,
                        offline_partition_count,
                        log_start_offset_sum,
                        log_end_offset_sum,
                        estimated_message_count,
                    ]
                )

                cluster_agg["topic_count"] += 1
                cluster_agg["internal_topic_count"] += is_internal
                cluster_agg["partition_count"] += partition_count
                cluster_agg[
                    "under_replicated_partition_count"
                ] += under_replicated_partition_count
                cluster_agg["offline_partition_count"] += offline_partition_count
                cluster_agg["estimated_message_count"] += estimated_message_count

            cluster_agg["broker_count"] = len(broker_ids)

            cluster_row = [
                [
                    collected_at,
                    collected_date,
                    collected_hour,
                    self.cluster_name,
                    ",".join(self.bootstrap_servers),
                    cluster_agg["broker_count"],
                    cluster_agg["topic_count"],
                    cluster_agg["internal_topic_count"],
                    cluster_agg["partition_count"],
                    cluster_agg["under_replicated_partition_count"],
                    cluster_agg["offline_partition_count"],
                    cluster_agg["estimated_message_count"],
                ]
            ]

            self._insert_cluster_snapshot(
                clickhouse_client=clickhouse_client,
                rows=cluster_row,
            )
            self._insert_topic_snapshot(
                clickhouse_client=clickhouse_client,
                rows=topic_rows,
            )

            logger.info(
                "Kafka metrics collected successfully "
                "(topics=%s, brokers=%s, partitions=%s)",
                cluster_agg["topic_count"],
                cluster_agg["broker_count"],
                cluster_agg["partition_count"],
            )

        except Exception as e:
            logger.exception("Failed to collect Kafka metrics: %s", str(e))
            raise

        finally:
            try:
                admin_client.close()
            except Exception:
                logger.exception("Failed to close Kafka admin client")

            try:
                consumer.close()
            except Exception:
                logger.exception("Failed to close Kafka consumer")

            try:
                clickhouse_client.close()
            except Exception:
                logger.exception("Failed to close ClickHouse client")

            logger.info("KafkaMetricService resources closed")

    def _insert_cluster_snapshot(
        self,
        clickhouse_client,
        rows: list[list],
    ) -> None:
        if not rows:
            logger.info("No cluster snapshot rows to insert")
            return

        clickhouse_client.insert(
            table="kafka_cluster_metric_snapshot",
            data=rows,
            column_names=[
                "collected_at",
                "collected_date",
                "collected_hour",
                "cluster_name",
                "bootstrap_servers",
                "broker_count",
                "topic_count",
                "internal_topic_count",
                "partition_count",
                "under_replicated_partition_count",
                "offline_partition_count",
                "estimated_message_count",
            ],
        )

        logger.info("Inserted %s cluster snapshot row(s)", len(rows))

    def _insert_topic_snapshot(
        self,
        clickhouse_client,
        rows: list[list],
    ) -> None:
        if not rows:
            logger.info("No topic snapshot rows to insert")
            return

        clickhouse_client.insert(
            table="kafka_topic_metric_snapshot",
            data=rows,
            column_names=[
                "collected_at",
                "collected_date",
                "collected_hour",
                "cluster_name",
                "topic_name",
                "is_internal",
                "partition_count",
                "replication_factor",
                "min_isr_count",
                "avg_isr_count",
                "under_replicated_partition_count",
                "offline_partition_count",
                "log_start_offset_sum",
                "log_end_offset_sum",
                "estimated_message_count",
            ],
        )

        logger.info("Inserted %s topic snapshot row(s)", len(rows))
