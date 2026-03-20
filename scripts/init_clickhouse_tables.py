"""
Project: QF5214 Polymarket Data Pipeline
File: init_clickhouse_tables.py
Author: Xu
Created: 2026-03-14

Description:
Initialize ClickHouse tables for the project.
"""

from app.clients.clickhouse_client import ClickHouseClient


def main() -> None:
    client = ClickHouseClient()
    client.create_tables()


if __name__ == "__main__":
    main()
