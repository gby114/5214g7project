"""
Project: QF5214 Polymarket Data Pipeline
File: logger.py
Author: Xu
Created: 2026-03-14

Description:
Central logging configuration for the application.
"""

import logging
import sys


def setup_logger(name: str) -> logging.Logger:
    """
    Create and configure a logger.

    Args:
        name: logger name

    Returns:
        Configured logger
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
