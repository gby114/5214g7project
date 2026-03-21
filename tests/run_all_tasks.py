"""
Manual pipeline runner for local debug.

It executes all currently defined Celery task "actions" in a local, sequential way:
- ingestion tasks (test_raw + optional Polymarket markets fetch)
- consume tasks (optional Kafka raw -> ClickHouse parse/load)
- aggregation tasks (test_raw aggregation + placeholder aggregation chains)

This script does NOT start Celery. It calls the underlying service logic directly.
"""

from __future__ import annotations

import argparse
import time
from typing import Callable

from app.clients.clickhouse_client import ClickHouseClient
from app.services.aggregation_service import aggregation_service
from app.services.consume_service import consume_service
from app.services.ingestion_service import ingestion_service
from app.utils.wrapper_utils import log_task_run


def _run_logged(task_name: str, task_type: str, fn: Callable[[], object]) -> object:
    wrapped = log_task_run(task_name=task_name, task_type=task_type)(fn)
    return wrapped()


def _run_placeholder_chain(sleep_between_calls: float) -> None:
    # The Celery tasks for hour/day aggregation 01~04 are just placeholder chains.
    aggregation_service.aggregate_placeholder1_data()
    if sleep_between_calls:
        time.sleep(sleep_between_calls)
    aggregation_service.aggregate_placeholder2_data()
    if sleep_between_calls:
        time.sleep(sleep_between_calls)
    aggregation_service.aggregate_placeholder3_data()
    if sleep_between_calls:
        time.sleep(sleep_between_calls)
    aggregation_service.aggregate_placeholder4_data()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all pipeline tasks locally.")
    parser.add_argument(
        "--ensure-clickhouse-tables",
        action="store_true",
        default=True,
        help="Create ClickHouse tables if they don't exist (needed for task_run_log).",
    )
    parser.add_argument(
        "--write-task-log",
        action="store_true",
        default=True,
        help="Write per-task run logs into ClickHouse task_run_log (same as Celery decorator).",
    )
    parser.add_argument(
        "--sleep-between-placeholder-calls",
        type=float,
        default=0.0,
        help="Sleep between placeholder aggregation steps (matches Celery tasks when >0).",
    )

    # Ingestion: Polymarket markets (requires pmxt + node + pmxtjs on this host)
    parser.add_argument(
        "--do-polymarket",
        action="store_true",
        default=False,
        help="If enabled, run PMXT fetch_markets -> Kafka topic (network + PMXT + broker).",
    )
    parser.add_argument(
        "--markets-query",
        type=str,
        default="",
        help="Optional fetch_markets query filter (omit for unfiltered list).",
    )
    parser.add_argument(
        "--markets-slug",
        type=str,
        default="",
        help="Optional market slug/ticker for direct lookup.",
    )
    parser.add_argument(
        "--markets-offset",
        type=int,
        default=None,
        help="Optional pagination offset for fetch_markets.",
    )
    parser.add_argument(
        "--markets-sort",
        type=str,
        default="volume",
        help="fetch_markets sort: volume | liquidity | newest",
    )
    parser.add_argument(
        "--markets-limit",
        type=int,
        default=2000,
        help="fetch_markets limit (default 2000).",
    )
    parser.add_argument(
        "--markets-search-in",
        type=str,
        default="",
        help="Optional search_in: title | description | both",
    )
    parser.add_argument(
        "--do-polymarket-consume",
        action="store_true",
        default=False,
        help="If enabled, consume raw Kafka messages and insert parsed snapshots into ClickHouse.",
    )

    # Ingestion test_raw generation
    parser.add_argument(
        "--test-raw-n",
        type=int,
        default=1,
        help="Number of rows to generate for test_raw ingestion.",
    )

    args = parser.parse_args()

    if args.ensure_clickhouse_tables:
        ClickHouseClient().create_tables()

    def run(task_name: str, task_type: str, fn: Callable[[], object]) -> None:
        if args.write_task_log:
            _run_logged(task_name, task_type, fn)
        else:
            fn()

    print("=== Ingestion ===")
    run(
        task_name="ingest_test_raw_data",
        task_type="ingestion",
        fn=lambda: ingestion_service.insert_test_raw_data(n=args.test_raw_n),
    )

    if args.do_polymarket:
        mkw: dict = {
            "sort": args.markets_sort,
            "limit": args.markets_limit,
        }
        if args.markets_query.strip():
            mkw["query"] = args.markets_query.strip()
        if args.markets_slug.strip():
            mkw["slug"] = args.markets_slug.strip()
        if args.markets_offset is not None:
            mkw["offset"] = args.markets_offset
        if args.markets_search_in.strip():
            mkw["search_in"] = args.markets_search_in.strip()

        run(
            task_name="ingest_polymarket_markets",
            task_type="ingestion",
            fn=lambda: ingestion_service.publish_polymarket_markets_to_kafka(**mkw),
        )
        if args.do_polymarket_consume:
            run(
                task_name="consume_polymarket_markets_raw_to_clickhouse",
                task_type="consume",
                fn=lambda: consume_service.consume_polymarket_markets_raw_to_clickhouse(
                    max_messages_per_partition=200,
                    max_workers=3,
                ),
            )
    else:
        print("Skip PMXT ingestion (pass --do-polymarket to enable).")

    print("=== Aggregation ===")
    run(
        task_name="aggregate_test_raw_data",
        task_type="aggregation",
        fn=lambda: aggregation_service.aggregate_test_raw_data(),
    )

    # hour_data_01~04
    for i in range(1, 5):
        run(
            task_name=f"aggregate_hour_data_0{i}",
            task_type="aggregation",
            fn=lambda: _run_placeholder_chain(args.sleep_between_placeholder_calls),
        )

    # day_data_01~04
    for i in range(1, 5):
        run(
            task_name=f"aggregate_day_data_0{i}",
            task_type="aggregation",
            fn=lambda: _run_placeholder_chain(args.sleep_between_placeholder_calls),
        )

    print("All tasks completed.")


if __name__ == "__main__":
    main()

