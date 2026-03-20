"""
Project: QF5214 Polymarket Data Pipeline
File: validators.py
Author: Xu
Created: 2026-03-14

Description:
Data validation utilities
"""

from typing import Any


def if_null_str(value: Any, default: str = "") -> str:
    """
    Return default if value is None or empty string.

    Args:
        value: Input value
        default: Default string

    Returns:
        str
    """
    if value is None:
        return default

    if isinstance(value, str) and value.strip() == "":
        return default

    return str(value)


def if_null_int(value: Any, default: int = 0) -> int:
    """
    Return default if value is None or invalid int.

    Args:
        value: Input value
        default: Default int

    Returns:
        int
    """
    if value is None:
        return default

    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def if_null_float(value: Any, default: float = 0.0) -> float:
    """
    Return default if value is None or invalid float.

    Args:
        value: Input value
        default: Default float

    Returns:
        float
    """
    if value is None:
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default
