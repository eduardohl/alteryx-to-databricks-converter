"""Structured logging setup for a2d."""

from __future__ import annotations

import logging
import sys


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the application."""
    level = logging.DEBUG if verbose else logging.INFO

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger = logging.getLogger("a2d")
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
