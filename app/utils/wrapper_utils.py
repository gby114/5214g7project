"""
Project: QF5214 Polymarket Data Pipeline
File: task_log_decorator.py
Author: Xu
Created: 2026-03-14

Description:
Decorator for logging scheduled task execution into ClickHouse
"""

from functools import wraps
from time import perf_counter
from typing import Any, Callable

from app.clients.clickhouse_client import ClickHouseClient
from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_COLS_ENUM
from app.logging.logger import setup_logger
from app.utils.time_utils import get_now


logger = setup_logger(__name__)


def log_task_run(
    task_name: str,
    task_type: str,
) -> Callable:
    """
    Decorator for logging task execution into ClickHouse.

    Args:
        task_name: Task name shown in monitoring table.
        task_type: Task type, e.g. ingestion / aggregation / backfill.

    Returns:
        Wrapped function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = get_now()
            start_perf = perf_counter()

            status = "success"
            error_message = ""
            processed_count = 1

            logger.info("Task started: %s", task_name)

            try:
                result = func(*args, **kwargs)

                if isinstance(result, int):
                    processed_count = result
                elif isinstance(result, dict) and "processed_count" in result:
                    processed_count = int(result["processed_count"])

                return result

            except Exception as e:
                status = "failed"
                error_message = str(e)
                logger.error("Task failed: %s, error=%s", task_name, error_message)
                raise

            finally:
                end_time = get_now()
                duration_seconds = perf_counter() - start_perf

                row = {
                    "task_name": task_name,
                    "task_type": task_type,
                    "status": status,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration_seconds": duration_seconds,
                    "processed_count": processed_count,
                    "error_message": error_message,
                    "ck_insert_time": end_time,
                }

                try:
                    ClickHouseClient().insert_rows(
                        table="task_run_log",
                        rows=[row],
                        column_names=CLICKHOUSE_TABLE_COLS_ENUM.TASK_RUN_LOG.value,
                    )
                    logger.info("Task log inserted: %s", task_name)

                except Exception as log_error:
                    logger.error(
                        "Failed to insert task log for %s: %s",
                        task_name,
                        str(log_error),
                    )

        return wrapper

    return decorator
