"""
Centralised logging configuration for the Polymarket ETL pipeline.

Every module calls get_logger(__name__) to get a named logger.
Named loggers let you see exactly which module emitted each log line,
making it easy to trace failures back to their source:

    2026-04-25 10:01:07 [WARNING ] fetcher      - Retry 1/3 for event_id=318829
    2026-04-25 10:01:12 [ERROR   ] bronze.loader - Insert failed: ...

Log levels:
    DEBUG   — per-request detail (URL, params). Off by default, set DEBUG=true in .env.
    INFO    — pipeline milestones: phase start/end, row counts, timing.
    WARNING — recoverable issues: retries, null fields defaulted, partial failures.
    ERROR   — unrecoverable failures written to bronze.fetch_errors.
"""

import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger with consistent formatting across all modules.

    Args:
        name: Module name — pass __name__ from the calling module.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    # Guard against adding duplicate handlers if get_logger is called
    # multiple times for the same name (e.g. during testing).
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)-16s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # DEBUG=true in .env enables verbose per-request logging.
    # In production (Docker), leave DEBUG unset — only INFO+ is shown.
    level = logging.DEBUG if os.getenv("DEBUG", "").lower() == "true" else logging.INFO
    logger.setLevel(level)

    return logger
