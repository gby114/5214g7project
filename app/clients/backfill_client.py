"""
Project: QF5214 Polymarket Data Pipeline
File: backfill_client.py
Author: Xu
Created: 2026-03-14

Description:
Backfill client for generating parquet URLs and downloading files as temp files
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator

import requests

from app.logging.logger import setup_logger


logger = setup_logger(__name__)


class BackfillClient:
    """
    Client for generating PMXT parquet backfill URLs and downloading them as temp files.
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
            "Initializing BackfillClient (base_url=%s, timeout=%s)",
            self.base_url,
            self.timeout,
        )

    def build_orderbook_url(self, dt: datetime) -> str:
        """
        Build a single hourly Polymarket orderbook parquet URL.

        Args:
            dt: Datetime used to generate the parquet hour URL.

        Returns:
            URL string.
        """
        dt = dt.replace(minute=0, second=0, microsecond=0)
        return f"{self.base_url}/polymarket_orderbook_{dt.strftime('%Y-%m-%dT%H')}.parquet"

    def build_orderbook_urls(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[str]:
        """
        Build all hourly parquet URLs within a time range.

        Note:
            start_time is inclusive
            end_time is exclusive

        Args:
            start_time: Start datetime.
            end_time: End datetime.

        Returns:
            List of hourly parquet URLs.
        """
        if end_time <= start_time:
            raise ValueError("end_time must be greater than start_time")

        urls: list[str] = []

        current = start_time.replace(minute=0, second=0, microsecond=0)
        end = end_time.replace(minute=0, second=0, microsecond=0)

        while current < end:
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
        logger.info("Downloading parquet file to temporary file: %s", url)

        suffix = Path(url).suffix or ".parquet"

        with NamedTemporaryFile(mode="wb", suffix=suffix, delete=True) as temp_file:
            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()

                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if chunk:
                        temp_file.write(chunk)

            temp_file.flush()

            logger.info("Temporary parquet ready: %s", temp_file.name)

            yield Path(temp_file.name)


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

    with backfill_client.download_as_tempfile(first_url) as temp_path:
        print(f"Temp parquet file: {temp_path}")
