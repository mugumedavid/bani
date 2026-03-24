"""Tests for structured JSON-lines logging."""

from __future__ import annotations

import json
import logging

from bani.infra.logging import JSONFormatter, setup_logging


def test_json_formatter_formats_as_json() -> None:
    """Test that JSONFormatter outputs valid JSON."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")
    record = logger.makeRecord(
        name="test_logger",
        level=logging.INFO,
        fn="test.py",
        lno=42,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    output = formatter.format(record)

    # Should be valid JSON
    obj = json.loads(output)
    assert isinstance(obj, dict)
    assert obj["message"] == "Test message"
    assert obj["level"] == "INFO"
    assert obj["component"] == "test_logger"


def test_json_formatter_includes_timestamp() -> None:
    """Test that JSONFormatter includes a timestamp."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")
    record = logger.makeRecord(
        name="test_logger",
        level=logging.INFO,
        fn="test.py",
        lno=42,
        msg="Test",
        args=(),
        exc_info=None,
    )

    output = formatter.format(record)
    obj = json.loads(output)

    assert "timestamp" in obj
    assert isinstance(obj["timestamp"], float)


def test_json_formatter_includes_level() -> None:
    """Test that JSONFormatter includes the log level."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")

    for level, level_name in [
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARNING"),
        (logging.ERROR, "ERROR"),
    ]:
        record = logger.makeRecord(
            name="test_logger",
            level=level,
            fn="test.py",
            lno=42,
            msg="Test",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        obj = json.loads(output)
        assert obj["level"] == level_name


def test_json_formatter_includes_component() -> None:
    """Test that JSONFormatter includes the component (logger name)."""
    formatter = JSONFormatter()
    logger = logging.getLogger("bani.application")

    record = logger.makeRecord(
        name="bani.application",
        level=logging.INFO,
        fn="test.py",
        lno=42,
        msg="Test",
        args=(),
        exc_info=None,
    )

    output = formatter.format(record)
    obj = json.loads(output)
    assert obj["component"] == "bani.application"


def test_json_formatter_formats_message_with_args() -> None:
    """Test that JSONFormatter correctly formats messages with arguments."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")
    record = logger.makeRecord(
        name="test_logger",
        level=logging.INFO,
        fn="test.py",
        lno=42,
        msg="User %s logged in from %s",
        args=("alice", "192.168.1.1"),
        exc_info=None,
    )

    output = formatter.format(record)
    obj = json.loads(output)
    assert obj["message"] == "User alice logged in from 192.168.1.1"


def test_json_formatter_includes_extra_fields() -> None:
    """Test that JSONFormatter includes extra fields in log records."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")
    record = logger.makeRecord(
        name="test_logger",
        level=logging.INFO,
        fn="test.py",
        lno=42,
        msg="Test",
        args=(),
        exc_info=None,
    )
    # Add custom field
    record.user_id = 123
    record.action = "login"

    output = formatter.format(record)
    obj = json.loads(output)
    assert obj["user_id"] == 123
    assert obj["action"] == "login"


def test_setup_logging_configures_root_logger() -> None:
    """Test that setup_logging configures the root logger."""
    # Clear any existing handlers
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    setup_logging("DEBUG")

    # Should have one handler
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert isinstance(handler.formatter, JSONFormatter)
    assert root.level == logging.DEBUG


def test_setup_logging_respects_log_level() -> None:
    """Test that setup_logging respects the log level parameter."""
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    setup_logging("WARNING")
    assert root.level == logging.WARNING

    for handler in root.handlers[:]:
        root.removeHandler(handler)

    setup_logging("ERROR")
    assert root.level == logging.ERROR


def test_setup_logging_removes_old_handlers() -> None:
    """Test that setup_logging removes existing handlers."""
    root = logging.getLogger()
    old_handler = logging.StreamHandler()
    root.addHandler(old_handler)

    # Should have 1 handler before setup_logging
    assert len(root.handlers) >= 1

    setup_logging("INFO")

    # Should have exactly 1 handler after
    assert len(root.handlers) == 1
    assert root.handlers[0] != old_handler


def test_json_formatter_outputs_one_json_per_line() -> None:
    """Test that JSONFormatter outputs exactly one JSON object per record."""
    formatter = JSONFormatter()
    logger = logging.getLogger("test_logger")

    records = [
        logger.makeRecord(
            name="test_logger",
            level=logging.INFO,
            fn="test.py",
            lno=42,
            msg="Message 1",
            args=(),
            exc_info=None,
        ),
        logger.makeRecord(
            name="test_logger",
            level=logging.INFO,
            fn="test.py",
            lno=42,
            msg="Message 2",
            args=(),
            exc_info=None,
        ),
    ]

    for record in records:
        output = formatter.format(record)
        # Should be single line with valid JSON
        lines = output.strip().split("\n")
        assert len(lines) == 1
        obj = json.loads(lines[0])
        assert isinstance(obj, dict)
