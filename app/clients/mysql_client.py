"""
Project: QF5214 Polymarket Data Pipeline
File: mysql_client.py
Author: Xu
Created: 2026-03-14

Description:
MySQL client for database operations
"""

import pymysql
from app.config.settings import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)


class MySQLClient:
    def __init__(self):
        """
        Initialize MySQL client.
        """
        self.conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def query(self, query: str, params=None) -> list[dict]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of dictionaries representing rows
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def execute(self, query: str, params=None) -> None:
        """
        Execute an INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query string
            params: Query parameters
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
