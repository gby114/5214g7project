"""
Project: QF5214 Polymarket Data Pipeline
File: clickhouse_client.py
Author: Xu
Created: 2026-03-14

Description:
ClickHouse client for executing queries and generic data insertion.
"""

from typing import Any
import clickhouse_connect

from app.config.settings import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USERNAME,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)

from app.schemas.clickhouse_tables import CLICKHOUSE_TABLE_QUERIES
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class ClickHouseClient:
    """
    ClickHouse client for executing queries and generic data insertion.
    """

    def __init__(self) -> None:
        logger.info(
            "Initializing ClickHouse client (host=%s, port=%s, database=%s)",
            CLICKHOUSE_HOST,
            CLICKHOUSE_PORT,
            CLICKHOUSE_DATABASE,
        )

        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

    def command(self, query: str) -> None:
        """
        Execute a ClickHouse command.
        """
        logger.debug("Executing ClickHouse command")

        try:
            self.client.command(query)
        except Exception as e:
            logger.error("ClickHouse command failed: %s", str(e))
            raise

    def create_tables(self) -> None:
        """
        Create all required ClickHouse tables.
        """

        logger.info("Creating ClickHouse tables")

        for query in CLICKHOUSE_TABLE_QUERIES:
            logger.debug("Running table creation SQL")
            self.command(query)

        logger.info("ClickHouse tables created successfully")

    def insert_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        column_names: list[str],
    ) -> None:
        """
        Insert rows into a ClickHouse table.
        """

        if not rows:
            logger.debug("No rows to insert for table %s", table)
            return

        row_count = len(rows)

        logger.info(
            "Inserting %s rows into ClickHouse table '%s'",
            row_count,
            table,
        )

        data = []

        for row in rows:
            values = []

            for column_name in column_names:
                if column_name not in row:
                    logger.error(
                        "Missing column '%s' in row for table '%s'",
                        column_name,
                        table,
                    )
                    raise KeyError(
                        f"Missing required column '{column_name}' for table '{table}'"
                    )

                values.append(row[column_name])

            data.append(values)

        try:
            self.client.insert(
                table=table,
                data=data,
                column_names=column_names,
            )

            logger.info(
                "Successfully inserted %s rows into '%s'",
                row_count,
                table,
            )

        except Exception as e:
            logger.error(
                "Failed to insert rows into ClickHouse table '%s': %s",
                table,
                str(e),
            )
            raise

    def query_rows(self, query: str) -> list[dict]:
        """
        Execute a ClickHouse query and return the results as a list of dictionaries.
        """

        logger.debug("Executing ClickHouse query")

        try:
            result = self.client.query(query)

            columns = result.column_names
            rows = result.result_rows

            row_count = len(rows)

            logger.info(
                "ClickHouse query executed successfully, returned %s rows",
                row_count,
            )

            return [
                dict(zip(columns, row))
                for row in rows
            ]

        except Exception as e:
            logger.error("ClickHouse query failed: %s", str(e))
            raise


clickhouse_client = ClickHouseClient()
