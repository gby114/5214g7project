"""
Project: QF5214 Polymarket Data Pipeline
File: redis_client.py
Author: Xu
Created: 2026-03-14

Description:
Redis client for caching market data
"""

import os
import json
from contextlib import contextmanager
from typing import Any, Optional

import redis
from app.logging.logger import setup_logger

logger = setup_logger(__name__)


class RedisClient:
    """
    Redis client wrapper with connection pool and utility methods.
    """

    def __init__(self):
        self.host = os.getenv("REDIS_HOST", "redis")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.db = int(os.getenv("REDIS_DB", 0))

        logger.info(
            f"Initializing Redis client (host={self.host}, port={self.port}, db={self.db})"
        )

        self.pool = redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=True,
            max_connections=10,
        )

    @contextmanager
    def get_client(self):
        """
        Context manager for Redis connection.
        """
        client = redis.Redis(connection_pool=self.pool)
        try:
            yield client
        finally:
            client.close()

    # ========================
    # Basic KV Operations
    # ========================

    def set(self, key: str, value: Any, ex: Optional[int] = None):
        """
        Set key-value in Redis.

        Args:
            key: redis key
            value: any JSON-serializable value
            ex: expiration in seconds
        """
        with self.get_client() as client:
            val = json.dumps(value) if not isinstance(value, str) else value
            client.set(key, val, ex=ex)

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from Redis.
        """
        with self.get_client() as client:
            val = client.get(key)
            if val is None:
                return None
            try:
                return json.loads(val)
            except Exception:
                return val

    def delete(self, key: str):
        """
        Delete key from Redis.
        """
        with self.get_client() as client:
            client.delete(key)

    # ========================
    # Hash Operations
    # ========================

    def hset(self, name: str, key: str, value: Any):
        with self.get_client() as client:
            val = json.dumps(value) if not isinstance(value, str) else value
            client.hset(name, key, val)

    def hget(self, name: str, key: str) -> Optional[Any]:
        with self.get_client() as client:
            val = client.hget(name, key)
            if val is None:
                return None
            try:
                return json.loads(val)
            except Exception:
                return val

    def hgetall(self, name: str) -> dict:
        with self.get_client() as client:
            data = client.hgetall(name)
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except Exception:
                    result[k] = v
            return result

    # ========================
    # List Operations
    # ========================

    def lpush(self, key: str, value: Any):
        with self.get_client() as client:
            val = json.dumps(value) if not isinstance(value, str) else value
            client.lpush(key, val)

    def rpush(self, key: str, value: Any):
        with self.get_client() as client:
            val = json.dumps(value) if not isinstance(value, str) else value
            client.rpush(key, val)

    def lrange(self, key: str, start: int = 0, end: int = -1):
        with self.get_client() as client:
            values = client.lrange(key, start, end)
            result = []
            for v in values:
                try:
                    result.append(json.loads(v))
                except Exception:
                    result.append(v)
            return result

    # ========================
    # Utility
    # ========================

    def exists(self, key: str) -> bool:
        with self.get_client() as client:
            return client.exists(key) == 1

    def expire(self, key: str, seconds: int):
        with self.get_client() as client:
            client.expire(key, seconds)

    def ping(self) -> bool:
        try:
            with self.get_client() as client:
                return client.ping()
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False


redis_client = RedisClient()
