"""
Project: QF5214 Polymarket Data Pipeline
File: transformers.py
Author: Xu
Created: 2026-03-14

Description:
Data transformation utilities
"""


"""
Project: QF5214 Polymarket Data Pipeline
File: transformers.py
Author: Xu
Created: 2026-03-14

Description:
Data transformation utilities
"""

from datetime import datetime
from typing import Any


def to_int(value: Any, default: int = 0) -> int:
    """
    Convert value to int safely.

    Args:
        value: Input value.
        default: Fallback value when conversion fails.

    Returns:
        Converted int value.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """
    Convert value to float safely.

    Args:
        value: Input value.
        default: Fallback value when conversion fails.

    Returns:
        Converted float value.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_str(value: Any, default: str = "") -> str:
    """
    Convert value to string safely.

    Args:
        value: Input value.
        default: Fallback value when input is None.

    Returns:
        Converted string value.
    """
    if value is None:
        return default

    return str(value)


def normalize_str(value: Any, default: str = "") -> str:
    """
    Normalize string by converting to str and stripping spaces.

    Args:
        value: Input value.
        default: Fallback value when input is None.

    Returns:
        Normalized string.
    """
    if value is None:
        return default

    return str(value).strip()


def to_bool(value: Any, default: bool = False) -> bool:
    """
    Convert value to bool safely.

    Args:
        value: Input value.
        default: Fallback value when conversion fails.

    Returns:
        Converted bool value.
    """
    if isinstance(value, bool):
        return value

    if value is None:
        return default

    normalized = str(value).strip().lower()

    if normalized in {"true", "1", "yes", "y"}:
        return True

    if normalized in {"false", "0", "no", "n"}:
        return False

    return default
