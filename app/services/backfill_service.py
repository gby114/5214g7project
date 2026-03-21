"""
Project: QF5214 Polymarket Data Pipeline
File: backfill_service.py
Author: Xu
Created: 2026-03-21

Description:
Backfill service for ingesting Polymarket orderbook parquet data into ClickHouse.
"""

from __future__ import annotations
import re
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from app.clients.backfill_client import BackfillClient
from app.clients.clickhouse_client import ClickHouseClient
from app.clients.kafka_client import KafkaProducerClient
from app.logging.logger import setup_logger
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.utils.common_utils import get_task_config, update_task_config, get_target_market_ids
from app.config.settings import KAFKA_TOPIC_1
from time import perf_counter


logger = setup_logger(__name__)


class BackfillService:
    """
    Service for backfilling Polymarket orderbook parquet data into ClickHouse.
    """

    TABLE_NAME = "polymarket_orderbook_raw"
    DEFAULT_SOURCE = "polymarket"
    DEFAULT_EXCHANGE = "polymarket"

    @classmethod
    def _extract_file_hour_from_url(cls, url: str) -> datetime:
        """
        Extract parquet file hour from URL as UTC timezone-aware datetime.

        Example:
            https://r2.pmxt.dev/polymarket_orderbook_2026-03-19T23.parquet
            -> 2026-03-19 23:00:00+00:00
        """
        match = re.search(
            r"polymarket_orderbook_(\d{4}-\d{2}-\d{2}T\d{2})\.parquet",
            url,
        )
        if not match:
            raise ValueError(f"Cannot extract file hour from url: {url}")

        return datetime.strptime(
            match.group(1),
            "%Y-%m-%dT%H",
        ).replace(tzinfo=timezone.utc)

    @classmethod
    def ingest_backfill_data(cls) -> int:
        """
        Ingest configured backfill parquet data into Kafka.

        Returns:
            Total produced row count.
        """
        logger.info("Starting backfill ingestion")

        update_task_config(
            "backfill_orderbook",
            {
                "is_open": True,
                "start_time": datetime(
                    2026, 3, 20, 4, 0, 0,
                    tzinfo=ZoneInfo("Asia/Singapore"),
                ).isoformat(),
                "end_time": datetime(
                    2026, 3, 20, 9, 0, 0,
                    tzinfo=ZoneInfo("Asia/Singapore"),
                ).isoformat(),
                "chunk_minutes": 30,
                "max_urls_per_run": 2,
            },
        )

        task_config = get_task_config(
            "backfill_orderbook",
            {
                "is_open": True,
                "start_time": datetime(
                    2026, 3, 20, 7, 0, 0,
                    tzinfo=ZoneInfo("Asia/Singapore"),
                ).isoformat(),
                "end_time": datetime(
                    2026, 3, 20, 8, 0, 0,
                    tzinfo=ZoneInfo("Asia/Singapore"),
                ).isoformat(),
                "chunk_minutes": 30,
                "max_urls_per_run": 2,
            },
        )
        logger.info("Backfill task config: %s", task_config)

        if not task_config["is_open"]:
            logger.info("Backfill task is not open, skipping")
            return 0

        task_started_at = perf_counter()

        start_time = datetime.fromisoformat(task_config["start_time"])
        end_time = datetime.fromisoformat(task_config["end_time"])
        chunk_minutes = int(task_config.get("chunk_minutes", 5))
        max_urls_per_run = int(task_config.get("max_urls_per_run", 2))

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")

        if end_time <= start_time:
            logger.warning(
                "Invalid backfill window start_time=%s end_time=%s",
                start_time,
                end_time,
            )
            return 0

        backfill_client = BackfillClient()

        urls = backfill_client.build_orderbook_urls(
            start_time=start_time,
            end_time=end_time,
        )

        if max_urls_per_run > 0:
            urls = urls[:max_urls_per_run]

        logger.info("Backfill URLs to process count=%s", len(urls))

        total_inserted_rows = 0
        total_chunk_count = 0

        start_time_utc = start_time.astimezone(timezone.utc)
        end_time_utc = end_time.astimezone(timezone.utc)

        # 👉 建议：Kafka client 提到最外层（更真实的性能）
        with KafkaProducerClient() as kafka_client:

            for url_index, url in enumerate(urls):
                url_started_at = perf_counter()
                url_produced_rows = 0
                url_chunk_count = 0

                file_hour_utc = cls._extract_file_hour_from_url(url)
                file_start_time_utc = file_hour_utc
                file_end_time_utc = file_hour_utc + timedelta(hours=1)

                window_start_time = max(start_time_utc, file_start_time_utc)
                window_end_time = min(end_time_utc, file_end_time_utc)

                logger.info(
                    "Processing backfill url_index=%s url=%s "
                    "file_start_time_utc=%s file_end_time_utc=%s "
                    "window_start_time_utc=%s window_end_time_utc=%s",
                    url_index,
                    url,
                    file_start_time_utc,
                    file_end_time_utc,
                    window_start_time,
                    window_end_time,
                )

                if window_end_time <= window_start_time:
                    logger.info("Skipping empty backfill window for url=%s", url)
                    continue

                for chunk_index, df in enumerate(
                    backfill_client.read_orderbook_parquet_by_time_chunks(
                        url=url,
                        start_time=window_start_time,
                        end_time=window_end_time,
                        chunk_minutes=chunk_minutes,
                        select_sqls=[
                            "json_extract_string(data, '$.update_type') AS update_type",
                            "json_extract_string(data, '$.market_id') AS market_id",
                            "json_extract_string(data, '$.token_id') AS token_id",
                            "json_extract_string(data, '$.side') AS side",
                            "json_extract_string(data, '$.best_bid') AS best_bid",
                            "json_extract_string(data, '$.best_ask') AS best_ask",
                            "CAST(json_extract(data, '$.timestamp') AS DOUBLE) AS timestamp",
                            "json_extract_string(data, '$.change_price') AS change_price",
                            "json_extract_string(data, '$.change_size') AS change_size",
                            "json_extract_string(data, '$.change_side') AS change_side",
                        ],
                        extra_where_sql="update_type = 'price_change'",
                        time_column="timestamp_received",
                    )
                ):
                    chunk_started_at = perf_counter()
                    raw_row_count = len(df)

                    df = cls._clean_df(df)
                    cleaned_row_count = len(df)

                    df = cls._filter_df(df)
                    filtered_row_count = len(df)

                    if df.empty:
                        chunk_duration_minutes = (perf_counter() - chunk_started_at) / 60

                        logger.info(
                            "Finished chunk url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s produced=0 "
                            "chunk_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            chunk_duration_minutes,
                        )
                        continue

                    kafka_values = df.to_dict(orient="records")
                    kafka_value_count = len(kafka_values)

                    if not kafka_values:
                        chunk_duration_minutes = (perf_counter() - chunk_started_at) / 60

                        logger.info(
                            "Finished chunk url=%s chunk_index=%s "
                            "raw=%s cleaned=%s filtered=%s produced=0 "
                            "chunk_min=%.4f",
                            url,
                            chunk_index,
                            raw_row_count,
                            cleaned_row_count,
                            filtered_row_count,
                            chunk_duration_minutes,
                        )
                        continue

                    kafka_send_started_at = perf_counter()

                    # kafka_client.send_batch(
                    #     topic=KAFKA_TOPIC_1,
                    #     values=kafka_values,
                    #     key_field="market_id",
                    # )
                    # kafka_client.flush()

                    kafka_send_duration_minutes = (perf_counter() - kafka_send_started_at) / 60
                    chunk_duration_minutes = (perf_counter() - chunk_started_at) / 60

                    # 👉 吞吐（非常关键）
                    rows_per_min = (
                        kafka_value_count / chunk_duration_minutes
                        if chunk_duration_minutes > 0 else 0
                    )

                    total_inserted_rows += kafka_value_count
                    total_chunk_count += 1
                    url_produced_rows += kafka_value_count
                    url_chunk_count += 1
                    raw_rows_per_min = (
                        raw_row_count / chunk_duration_minutes
                        if chunk_duration_minutes > 0 else 0
                    )
                    cleaned_rows_per_min = (
                        cleaned_row_count / chunk_duration_minutes
                        if chunk_duration_minutes > 0 else 0
                    )
                    filtered_rows_per_min = (
                        filtered_row_count / chunk_duration_minutes
                        if chunk_duration_minutes > 0 else 0
                    )
                    produced_rows_per_min = (
                        kafka_value_count / chunk_duration_minutes
                        if chunk_duration_minutes > 0 else 0
                    )

                    logger.info(
                        "Finished chunk url=%s chunk_index=%s "
                        "raw=%s cleaned=%s filtered=%s produced=%s "
                        "kafka_send_min=%.4f chunk_min=%.4f raw_rpm=%.2f cleaned_rpm=%.2f "
                        "filtered_rpm=%.2f produced_rpm=%.2f total=%s",
                        url,
                        chunk_index,
                        raw_row_count,
                        cleaned_row_count,
                        filtered_row_count,
                        kafka_value_count,
                        kafka_send_duration_minutes,
                        chunk_duration_minutes,
                        raw_rows_per_min,
                        cleaned_rows_per_min,
                        filtered_rows_per_min,
                        produced_rows_per_min,
                        total_inserted_rows,
                    )

                url_duration_minutes = (perf_counter() - url_started_at) / 60

                logger.info(
                    "Finished url url_index=%s url=%s chunk_count=%s produced=%s "
                    "duration_min=%.4f avg_chunk_min=%.4f",
                    url_index,
                    url,
                    url_chunk_count,
                    url_produced_rows,
                    url_duration_minutes,
                    (url_duration_minutes / url_chunk_count) if url_chunk_count > 0 else 0.0,
                )

        task_duration_minutes = (perf_counter() - task_started_at) / 60

        logger.info(
            "Finished backfill ingestion total_rows=%s total_chunks=%s "
            "duration_min=%.4f avg_chunk_min=%.4f",
            total_inserted_rows,
            total_chunk_count,
            task_duration_minutes,
            (task_duration_minutes / total_chunk_count) if total_chunk_count > 0 else 0.0,
        )

        return total_inserted_rows

    @staticmethod
    def _explode_data_column(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame by expanding `data` JSON column into columns.

        - Parse JSON string in `data`
        - Normalize into flat columns
        - Drop original columns except useful metadata if needed
        """
        if df.empty:
            return df

        # 1. parse JSON safely
        parsed_data = []
        for val in df["data"]:
            if isinstance(val, str):
                try:
                    parsed_data.append(json.loads(val))
                except Exception:
                    parsed_data.append({})
            elif isinstance(val, dict):
                parsed_data.append(val)
            else:
                parsed_data.append({})

        # 2. flatten JSON → DataFrame
        data_df = pd.json_normalize(parsed_data)

        # 3. （可选）补充一些上层字段（推荐保留）
        data_df["timestamp_received"] = df["timestamp_received"].values
        data_df["timestamp_created_at"] = df["timestamp_created_at"].values

        return data_df

    @staticmethod
    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep 1 row per second per market_id based on data.timestamp.
        """

        if df.empty:
            return df

        # 1. 转 timestamp（float → datetime）
        df["event_time"] = pd.to_datetime(
            df["timestamp"],
            unit="s",
            utc=True,
        )

        # 2. 秒级 bucket
        df["ts_sec"] = df["event_time"].dt.floor("s")

        # 3. 排序（保留最新）
        df = df.sort_values(
            by="event_time",
            ascending=False,
        )

        # 4. 每个 token_id 每秒保留一条
        df = df.drop_duplicates(
            subset=["token_id", "ts_sec"],
            keep="first",
        )

        # 5. 清理
        df = df.drop(columns=["ts_sec"])

        return df

    @staticmethod
    def _filter_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter invalid / unwanted rows before sending to Kafka.

        Rules:
        - drop empty market_id
        - drop missing timestamp
        - keep only selected update_type
        - optional: filter invalid prices
        """

        if df.empty:
            return df

        original_count = len(df)

        # 1. 必须字段
        df = df[
            df["market_id"].notna() &
            df["timestamp"].notna()
        ]

        # 2. update_type 过滤（核心）
        df = df[
            df["update_type"].isin([
                "price_change",
                "book_snapshot",
            ])
        ]

        # 3. optional：价格过滤（建议打开）
        if "best_bid" in df.columns:
            df = df[df["best_bid"].notna()]

        if "best_ask" in df.columns:
            df = df[df["best_ask"].notna()]

        # 转 float（避免字符串比较问题）
        for col in ["best_bid", "best_ask"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 去掉无效价格
        if "best_bid" in df.columns:
            df = df[df["best_bid"] > 0]

        if "best_ask" in df.columns:
            df = df[df["best_ask"] > 0]

        logger.info(
            "Filter df rows: before=%s after=%s dropped=%s",
            original_count,
            len(df),
            original_count - len(df),
        )

        df = df[[
            "update_type",
            "token_id",
            "side",
            "best_bid",
            "best_ask",
            "timestamp",
            "change_price",
            "change_size",
            "change_side",
        ]]

        target_market_ids = get_target_market_ids()
        df = df[df['token_id'].isin(target_market_ids)]
        return df

    @staticmethod
    def _split_df_by_update_type(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        Split dataframe by update_type.
        """
        if df.empty or "update_type" not in df.columns:
            return {}

        result: dict[str, pd.DataFrame] = {}

        for update_type, group_df in df.groupby("update_type"):
            if pd.isna(update_type):
                continue
            result[str(update_type)] = group_df.reset_index(drop=True)

        return result

    @staticmethod
    def _transform_price_change_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform price_change messages from data JSON into structured dataframe.
        """
        if df.empty:
            return df

        parsed = df["data"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        out = pd.json_normalize(parsed)

        expected_cols = [
            "update_type",
            "market_id",
            "token_id",
            "side",
            "best_bid",
            "best_ask",
            "timestamp",
            "change_price",
            "change_size",
            "change_side",
        ]

        for col in expected_cols:
            if col not in out.columns:
                out[col] = None

        return out[expected_cols]

    @staticmethod
    def _transform_book_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform book_snapshot messages from data JSON into structured dataframe.
        """
        if df.empty:
            return df

        parsed = df["data"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
        out = pd.json_normalize(parsed)

        return out
