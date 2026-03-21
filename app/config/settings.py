"""
Project: QF5214 Polymarket Data Pipeline
File: settings.py
Author: Xu
Created: 2026-03-14

Description:
Application configuration settings
"""

import os
from dotenv import load_dotenv


ENV_FILE = os.getenv("ENV_FILE", ".env.local")
load_dotenv(ENV_FILE)


def _get_env(key: str, default: str | None = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


# ========================
# Application
# ========================

APP_NAME = _get_env("APP_NAME", "polymarket-data-pipeline")
APP_ENV = _get_env("APP_ENV", "local")
APP_TIMEZONE = _get_env("APP_TIMEZONE", "Asia/Singapore")


# ========================
# Polymarket API
# ========================

POLYMARKET_API = _get_env("POLYMARKET_API", "https://gamma-api.polymarket.com")
POLYMARKET_TIMEOUT_SECONDS = int(_get_env("POLYMARKET_TIMEOUT_SECONDS", "10"))


# ========================
# Kafka
# ========================

KAFKA_BOOTSTRAP_SERVERS = _get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Raw UnifiedMarket JSON, one message per market (producer: Celery fetch_markets)
KAFKA_TOPIC_POLYMARKET_MARKETS_RAW = _get_env(
    "KAFKA_TOPIC_POLYMARKET_MARKETS_RAW",
    "polymarket.markets.raw",
)

KAFKA_TOPIC_MARKETS = _get_env("KAFKA_TOPIC_MARKETS", "polymarket.market_metadata")
KAFKA_TOPIC_PRICES = _get_env("KAFKA_TOPIC_PRICES", "polymarket.price_snapshot")
KAFKA_TOPIC_1 = _get_env("KAFKA_TOPIC_1", "polymarket.topic1")
KAFKA_TOPIC_2 = _get_env("KAFKA_TOPIC_2", "polymarket.topic2")
KAFKA_TOPIC_3 = _get_env("KAFKA_TOPIC_3", "polymarket.topic3")
KAFKA_TOPIC_4 = _get_env("KAFKA_TOPIC_4", "polymarket.topic4")
KAFKA_TOPIC_7 = _get_env("KAFKA_TOPIC_7", "polymarket.topic7")
KAFKA_TOPIC_8 = _get_env("KAFKA_TOPIC_8", "polymarket.topic8")
KAFKA_TOPIC_9 = _get_env("KAFKA_TOPIC_9", "polymarket.topic9")


# ========================
# Redis (Celery Broker)
# ========================

REDIS_HOST = _get_env("REDIS_HOST", "localhost")
REDIS_PORT = int(_get_env("REDIS_PORT", "6379"))

REDIS_URL = _get_env("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

CELERY_BROKER_URL = _get_env("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = _get_env("CELERY_RESULT_BACKEND", REDIS_URL)


# ========================
# ClickHouse (Analytics DB)
# ========================

CLICKHOUSE_HOST = _get_env("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(_get_env("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USERNAME = _get_env("CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = _get_env("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = _get_env("CLICKHOUSE_DATABASE", "default")


# ========================
# MySQL (Relational metadata)
# ========================

MYSQL_HOST = _get_env("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_get_env("MYSQL_PORT", "3306"))
MYSQL_USER = _get_env("MYSQL_USER", "root")
MYSQL_PASSWORD = _get_env("MYSQL_PASSWORD", "password")
MYSQL_DATABASE = _get_env("MYSQL_DATABASE", "polymarket")


# ========================
# MongoDB (Raw JSON storage)
# ========================

MONGODB_HOST = _get_env("MONGODB_HOST", "localhost")
MONGODB_PORT = int(_get_env("MONGODB_PORT", "27017"))
MONGODB_DATABASE = _get_env("MONGODB_DATABASE", "polymarket")

MONGODB_URI = _get_env("MONGODB_URI", f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}")


# ========================
# Feature Flags
# ========================

FETCH_MARKETS_ENABLED = _get_env("FETCH_MARKETS_ENABLED", "true").lower() == "true"

FETCH_PRICES_ENABLED = _get_env("FETCH_PRICES_ENABLED", "true").lower() == "true"
