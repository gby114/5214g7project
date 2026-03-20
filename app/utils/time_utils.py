"""
Project: QF5214 Polymarket Data Pipeline
File: time_utils.py
Author: Xu
Created: 2026-03-14

Description:
Time utility functions
"""

from datetime import datetime, timezone, timedelta
from typing import Optional


def get_now() -> datetime:
    """
    Get current local datetime.

    Returns:
        datetime: current local time (naive)
    """
    return datetime.now()


def get_utc_now() -> datetime:
    """
    Get current UTC datetime.

    Returns:
        datetime: current UTC time (timezone-aware)
    """
    return datetime.now(timezone.utc)


def round_datetime(
    dt: datetime,
    round_to: str = "minute",
) -> datetime:
    """
    Round datetime to nearest unit.

    Args:
        dt: input datetime
        round_to: one of ["minute", "hour", "day"]

    Returns:
        datetime: rounded datetime
    """

    if round_to == "minute":
        return dt.replace(second=0, microsecond=0)

    if round_to == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)

    if round_to == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    raise ValueError(f"Unsupported round_to: {round_to}")


def str_to_datetime(
    dt_str: str,
    fmt: str = "%Y-%m-%d %H:%M:%S",
    tz_aware: bool = False,
) -> datetime:
    """
    Convert string to datetime.

    Args:
        dt_str: datetime string
        fmt: format string
        tz_aware: whether to convert to UTC timezone-aware datetime

    Returns:
        datetime: parsed datetime
    """

    dt = datetime.strptime(dt_str, fmt)

    if tz_aware:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def datetime_to_str(
    dt: datetime,
    fmt: str = "%Y-%m-%d %H:%M:%S",
) -> str:
    """
    Convert datetime to string.

    Args:
        dt: datetime object
        fmt: format string

    Returns:
        str: formatted datetime string
    """
    return dt.strftime(fmt)
