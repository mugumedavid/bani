"""Structured JSON-lines logging configuration (Section 16.1).

Configures Python's logging module to emit JSON-lines format logs with
timestamp, level, component, event, and contextual fields.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any


class JSONFormatter(logging.Formatter):
    """Log formatter that emits JSON-lines format."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON.

        Args:
            record: The log record to format.

        Returns:
            JSON-formatted log line.
        """
        log_obj: dict[str, Any] = {
            "timestamp": record.created,
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }

        # Include extra fields if present
        if hasattr(record, "event"):
            event = getattr(record, "event", None)
            if event is not None:
                log_obj["event"] = event

        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        # Add any extra attributes
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "exc_info",
                "exc_text",
                "stack_info",
                "getMessage",
                "event",
            ):
                if not key.startswith("_"):
                    log_obj[key] = value

        return json.dumps(log_obj)


def setup_logging(level: str = "INFO") -> None:
    """Configure the root logger for JSON-lines output.

    Args:
        level: Log level (e.g., "DEBUG", "INFO", "WARNING", "ERROR").
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add JSON-formatted stderr handler
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
