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
from pathlib import Path
import json
from typing import Any


try:
    import zstandard as zstd
except ImportError:  # pragma: no cover
    zstd = None


logger = setup_logger(__name__)


BUSINESS_DATA_DIR = Path("business_data")


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

    def _get_business_data_file_path(self, table: str) -> Path:
        """
        Build export/import file path under business_data directory.

        Args:
            table: ClickHouse table name

        Returns:
            Path object
        """
        BUSINESS_DATA_DIR.mkdir(parents=True, exist_ok=True)
        return BUSINESS_DATA_DIR / f"{table}.native.zst"

    def _get_business_data_meta_file_path(self, table: str) -> Path:
        """
        Build metadata file path under business_data directory.

        Args:
            table: ClickHouse table name

        Returns:
            Path object
        """
        data_file = self._get_business_data_file_path(table)
        return data_file.with_suffix(data_file.suffix + ".meta.json")

    def export_table_data(
        self,
        table: str,
        database: str | None = None,
        where_sql: str | None = None,
        chunk_size: int = 1024 * 1024,
        overwrite: bool = True,
    ) -> str:
        """
        Export a ClickHouse table to business_data/{table}.native.zst.

        Args:
            table: Table name.
            database: Optional database name. Defaults to client database.
            where_sql: Optional WHERE clause without the "WHERE" keyword.
            chunk_size: Streaming chunk size in bytes.
            overwrite: Whether to overwrite existing file.

        Returns:
            Final output file path.
        """
        if zstd is None:
            raise ImportError(
                "zstandard is required for export_table_data. "
                "Install it with: pip install zstandard"
            )

        db = database or CLICKHOUSE_DATABASE
        qualified_table = f"{db}.{table}"
        output_file = self._get_business_data_file_path(table)
        meta_file = self._get_business_data_meta_file_path(table)

        if overwrite:
            if output_file.exists():
                logger.info("Removing existing export file: %s", str(output_file))
                output_file.unlink()

            if meta_file.exists():
                logger.info("Removing existing metadata file: %s", str(meta_file))
                meta_file.unlink()

        query = f"SELECT * FROM {qualified_table}"
        if where_sql:
            query += f" WHERE {where_sql}"

        logger.info(
            "Exporting ClickHouse table '%s' to file '%s'",
            qualified_table,
            str(output_file),
        )

        columns_query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db}' AND table = '{table}'
        ORDER BY position
        """
        columns_meta = self.query_rows(columns_query)

        row_count_query = f"SELECT count() AS cnt FROM {qualified_table}"
        if where_sql:
            row_count_query += f" WHERE {where_sql}"
        row_count = self.query_rows(row_count_query)[0]["cnt"]

        raw_bytes_written = 0

        try:
            with self.client.raw_stream(query=query, fmt="Native") as source, open(output_file, "wb") as fh:
                compressor = zstd.ZstdCompressor(level=9)
                with compressor.stream_writer(fh) as writer:
                    while True:
                        chunk = source.read(chunk_size)
                        if not chunk:
                            break
                        writer.write(chunk)
                        raw_bytes_written += len(chunk)

            meta = {
                "database": db,
                "table": table,
                "format": "Native",
                "compression": "zstd",
                "row_count": row_count,
                "columns": columns_meta,
            }

            meta_file.write_text(
                json.dumps(meta, ensure_ascii=False, indent=4),
                encoding="utf-8",
            )

            logger.info(
                "Exported table '%s' successfully row_count=%s raw_bytes=%s output_file='%s' meta_file='%s'",
                qualified_table,
                row_count,
                raw_bytes_written,
                str(output_file),
                str(meta_file),
            )

            return str(output_file)

        except Exception as e:
            logger.error(
                "Failed to export ClickHouse table '%s' to '%s': %s",
                qualified_table,
                str(output_file),
                str(e),
            )
            raise

    def import_table_data(
        self,
        table: str,
        database: str | None = None,
    ) -> None:
        """
        Import business_data/{table}.native.zst back into ClickHouse.

        Args:
            table: Target table name.
            database: Optional database name. Defaults to client database.
        """
        if zstd is None:
            raise ImportError(
                "zstandard is required for import_table_data. "
                "Install it with: pip install zstandard"
            )

        db = database or CLICKHOUSE_DATABASE
        qualified_table = f"{db}.{table}"
        input_file = self._get_business_data_file_path(table)

        if not input_file.exists():
            raise FileNotFoundError(f"Input file does not exist: {input_file}")

        logger.info(
            "Importing ClickHouse data file '%s' into table '%s'",
            str(input_file),
            qualified_table,
        )

        try:
            with open(input_file, "rb") as fh:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(fh) as reader:
                    self.client.raw_insert(
                        table=qualified_table,
                        insert_block=reader,
                        fmt="Native",
                    )

            logger.info(
                "Imported ClickHouse data file '%s' into table '%s' successfully",
                str(input_file),
                qualified_table,
            )

        except Exception as e:
            logger.error(
                "Failed to import ClickHouse data file '%s' into table '%s': %s",
                str(input_file),
                qualified_table,
                str(e),
            )
            raise
