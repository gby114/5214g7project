"""
Project: QF5214 Polymarket Data Pipeline
File: common_utils.py
Author: Xu
Created: 2026-03-14

Description:
Common utility functions
"""
import json
from app.clients.mysql_client import MySQLClient
from app.logging.logger import setup_logger


logger = setup_logger(__name__)


def get_task_config(task_name: str, default_config: dict) -> dict:
    """
    Get task config from MySQL.
    If not exists, insert default config and return it.

    Args:
        task_name: Celery task name
        default_config: default config dict

    Returns:
        config dict
    """

    client = MySQLClient()

    # ===============================
    # 1. 查询
    # ===============================
    query = """
        SELECT config
        FROM task_config
        WHERE task_name = %s
        LIMIT 1
    """

    rows = client.query(query, (task_name,))

    if rows:
        logger.info("Loaded config from DB for task: %s", task_name)

        config = rows[0]["config"]

        # MySQL JSON 有时返回 string，有时是 dict（保险处理）
        if isinstance(config, str):
            return json.loads(config)

        return config

    # ===============================
    # 2. 不存在 → 插入默认值
    # ===============================
    logger.warning(
        "Config not found for task: %s, inserting default config",
        task_name,
    )

    insert_query = """
        INSERT INTO task_config (task_name, config)
        VALUES (%s, %s)
    """

    client.execute(
        insert_query,
        (
            task_name,
            json.dumps(default_config),
        ),
    )

    return default_config


def update_task_config(task_name: str, config: dict) -> None:
    """
    Update task config in MySQL.
    If not exists, insert it.

    Args:
        task_name: Celery task name
        config: config dict to update
    """

    client = MySQLClient()

    query = """
        INSERT INTO task_config (task_name, config)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            config = VALUES(config)
    """

    client.execute(
        query,
        (
            task_name,
            json.dumps(config),
        ),
    )

    logger.info("Updated config for task: %s", task_name)
