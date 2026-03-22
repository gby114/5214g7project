"""
Project: QF5214 Polymarket Data Pipeline
File: backfill_client.py
Author: Xu
Created: 2026-03-14

Description:
Backfill client for generating parquet URLs, downloading files as temp files,
and reading remote parquet files in DuckDB time-based chunks.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

import duckdb
import pandas as pd
import requests

from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class BackfillClient:
    """
    Client for generating PMXT parquet backfill URLs, downloading them as temp files,
    and reading remote parquet files with DuckDB in time chunks.
    """

    def __init__(
        self,
        base_url: str = "https://r2.pmxt.dev",
        timeout: int = 120,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        """
        Initialize backfill client.

        Args:
            base_url: Base URL for parquet archive.
            timeout: HTTP request timeout in seconds.
            chunk_size: Stream download chunk size in bytes.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.chunk_size = chunk_size

        logger.info(
            "Initializing BackfillClient (base_url=%s, timeout=%s, chunk_size=%s)",
            self.base_url,
            self.timeout,
            self.chunk_size,
        )

    def build_orderbook_url(self, dt: datetime) -> str:
        """
        Build a single hourly Polymarket orderbook parquet URL.

        Args:
            dt: Datetime used to generate the parquet hour URL.

        Returns:
            URL string.
        """
        dt_utc = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        url = f"{self.base_url}/polymarket_orderbook_{dt_utc.strftime('%Y-%m-%dT%H')}.parquet"
        logger.info("Built orderbook URL for datetime=%s url=%s", dt_utc, url)
        return url

    def build_orderbook_urls(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[str]:
        """
        Build all hourly parquet URLs within a time range.

        File naming convention:
            polymarket_orderbook_YYYY-MM-DDTHH.parquet
        means data in [HH:00, HH+1:00) UTC.

        Note:
            start_time is inclusive
            end_time is exclusive
        """
        if end_time <= start_time:
            raise ValueError("end_time must be greater than start_time")

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")

        start_utc = start_time.astimezone(timezone.utc)
        end_utc = end_time.astimezone(timezone.utc)

        current = start_utc.replace(minute=0, second=0, microsecond=0)
        end_boundary = end_utc.replace(minute=0, second=0, microsecond=0)

        if end_utc > end_boundary:
            end_boundary += timedelta(hours=1)

        urls: list[str] = []

        while current < end_boundary:
            urls.append(self.build_orderbook_url(current))
            current += timedelta(hours=1)

        logger.info(
            "Built %s backfill URLs from %s to %s",
            len(urls),
            start_time,
            end_time,
        )

        return urls

    @contextmanager
    def download_as_tempfile(self, url: str) -> Iterator[Path]:
        """
        Download a parquet file into a temporary file and yield its path.

        The temporary file is removed automatically after use.

        Args:
            url: Source parquet URL.

        Yields:
            Path to temporary parquet file.
        """
        logger.info("Starting parquet download to temporary file url=%s", url)

        suffix = Path(url).suffix or ".parquet"

        with NamedTemporaryFile(mode="wb", suffix=suffix, delete=True) as temp_file:
            total_bytes = 0

            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()

                logger.info(
                    "HTTP download started url=%s status_code=%s content_length=%s",
                    url,
                    response.status_code,
                    response.headers.get("content-length"),
                )

                for chunk_index, chunk in enumerate(
                    response.iter_content(chunk_size=self.chunk_size)
                ):
                    if chunk:
                        temp_file.write(chunk)
                        total_bytes += len(chunk)

                        logger.info(
                            "Downloaded parquet chunk url=%s chunk_index=%s chunk_bytes=%s total_bytes=%s",
                            url,
                            chunk_index,
                            len(chunk),
                            total_bytes,
                        )

            temp_file.flush()

            logger.info(
                "Temporary parquet ready url=%s temp_file=%s size_bytes=%s",
                url,
                temp_file.name,
                total_bytes,
            )

            yield Path(temp_file.name)

        logger.info("Temporary parquet file cleaned up url=%s", url)

    def read_orderbook_parquet_by_time_chunks(
        self,
        url: str,
        start_time: datetime,
        end_time: datetime,
        chunk_minutes: int = 5,
        select_sqls: list[str] | None = None,
        extra_where_sql: str | None = None,
        time_column: str = "timestamp_received",
    ) -> Iterator[pd.DataFrame]:
        """
        Read a remote parquet file with DuckDB and yield DataFrames chunked by
        time windows.

        This method supports selecting either:
        - all columns (default)
        - custom SQL expressions via select_sqls

        Example select_sqls:
            [
                "json_extract_string(data, '$.update_type') AS update_type",
                "json_extract_string(data, '$.token_id') AS token_id",
                "json_extract_string(data, '$.side') AS side",
                "json_extract_string(data, '$.best_bid') AS best_bid",
                "json_extract_string(data, '$.best_ask') AS best_ask",
                "CAST(json_extract(data, '$.timestamp') AS DOUBLE) AS timestamp",
                "json_extract_string(data, '$.change_price') AS change_price",
                "json_extract_string(data, '$.change_size') AS change_size",
                "json_extract_string(data, '$.change_side') AS change_side",
            ]

        Note:
            start_time is inclusive
            end_time is exclusive

        Args:
            url: Remote parquet URL.
            start_time: Start datetime for filtering.
            end_time: End datetime for filtering.
            chunk_minutes: Time window size for each chunk.
            select_sqls: Optional list of DuckDB SQL select expressions.
                If None, SELECT * is used.
            extra_where_sql: Optional extra SQL condition appended with AND.
            time_column: Name of the timestamp column to filter on.

        Yields:
            Pandas DataFrame for each time chunk.
        """
        if end_time <= start_time:
            raise ValueError("end_time must be greater than start_time")

        if chunk_minutes <= 0:
            raise ValueError("chunk_minutes must be greater than 0")

        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("start_time and end_time must be timezone-aware")

        select_sql = "*"
        if select_sqls:
            select_sql = ",\n                        ".join(select_sqls)

        logger.info(
            "Starting DuckDB parquet chunk read "
            "url=%s start_time=%s end_time=%s chunk_minutes=%s "
            "select_sqls=%s extra_where_sql=%s time_column=%s",
            url,
            start_time,
            end_time,
            chunk_minutes,
            select_sqls,
            extra_where_sql,
            time_column,
        )

        current_start = start_time
        total_chunks = 0
        total_rows = 0

        con = duckdb.connect()

        try:
            logger.info("DuckDB connection opened for url=%s", url)

            while current_start < end_time:
                current_end = min(
                    current_start + timedelta(minutes=chunk_minutes),
                    end_time,
                )

                where_clauses = [
                    f"{time_column} >= TIMESTAMPTZ '{current_start.isoformat(sep=' ')}'",
                    f"{time_column} < TIMESTAMPTZ '{current_end.isoformat(sep=' ')}'",
                ]

                if extra_where_sql:
                    where_clauses.append(f"({extra_where_sql})")

                where_sql = " AND ".join(where_clauses)

                query = f"""
                    SELECT
                        {select_sql}
                    FROM read_parquet('{url}')
                    WHERE {where_sql}
                """

                logger.info(
                    "DuckDB chunk query starting "
                    "url=%s chunk_index=%s chunk_start=%s chunk_end=%s",
                    url,
                    total_chunks,
                    current_start,
                    current_end,
                )
                logger.info("DuckDB chunk query sql=%s", query.strip())

                df = con.execute(query).fetch_df()

                row_count = len(df)
                total_rows += row_count

                logger.info(
                    "DuckDB chunk query finished "
                    "url=%s chunk_index=%s chunk_start=%s chunk_end=%s "
                    "row_count=%s cumulative_rows=%s",
                    url,
                    total_chunks,
                    current_start,
                    current_end,
                    row_count,
                    total_rows,
                )

                if row_count > 0:
                    yield df

                total_chunks += 1
                current_start = current_end

            logger.info(
                "Finished DuckDB parquet chunk read "
                "url=%s total_chunks=%s total_rows=%s",
                url,
                total_chunks,
                total_rows,
            )

        finally:
            con.close()
            logger.info("DuckDB connection closed for url=%s", url)

    def count_rows_duckdb(
        self,
        url: str,
    ) -> int:
        """
        Count total rows in a remote parquet file using DuckDB.

        Args:
            url: Remote parquet URL.

        Returns:
            Total row count.
        """
        logger.info("Starting DuckDB row count url=%s", url)

        con = duckdb.connect()

        try:
            query = f"""
                SELECT count(*)
                FROM read_parquet('{url}')
            """

            logger.info("DuckDB count query sql=%s", query.strip())

            row_count = con.execute(query).fetchone()[0]

            logger.info(
                "DuckDB row count finished url=%s row_count=%s",
                url,
                row_count,
            )

            return int(row_count)

        finally:
            con.close()
            logger.info("DuckDB connection closed after count url=%s", url)


backfill_client = BackfillClient()


if __name__ == "__main__":
    start = datetime(2026, 3, 19, 22, 0, 0)
    end = datetime(2026, 3, 20, 2, 0, 0)

    urls = backfill_client.build_orderbook_urls(
        start_time=start,
        end_time=end,
    )

    print("Generated URLs:")
    for u in urls:
        print(u)

    first_url = urls[0]

    print("\n=== Temp file download test ===")
    with backfill_client.download_as_tempfile(first_url) as temp_path:
        print(f"Temp parquet file: {temp_path}")

    print("\n=== DuckDB row count test ===")
    row_count = backfill_client.count_rows_duckdb(first_url)
    print(f"Row count: {row_count}")

    print("\n=== DuckDB chunk read test ===")
    chunk_start = datetime(2026, 3, 19, 22, 49, 0)
    chunk_end = datetime(2026, 3, 19, 22, 55, 0)

    for idx, df in enumerate(
        backfill_client.read_orderbook_parquet_by_time_chunks(
            url=first_url,
            start_time=chunk_start,
            end_time=chunk_end,
            chunk_minutes=2,
            selected_columns=[
                "timestamp_received",
                "timestamp_created_at",
                "market_id",
                "update_type",
                "data",
            ],
        )
    ):
        print(f"\nChunk {idx} rows={len(df)}")
        print(df.head())
