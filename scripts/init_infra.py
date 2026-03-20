"""
Project: QF5214 Polymarket Data Pipeline
File: init_infra.py
Author: Xu
Created: 2026-03-14

Description:
Initialize Kafka topics and ClickHouse tables.
"""

from app.services.bootstrap_service import BootstrapService


def main() -> None:
    service = BootstrapService()
    service.bootstrap_all()


if __name__ == "__main__":
    main()
