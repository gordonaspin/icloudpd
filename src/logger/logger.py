"""Logging helpers and custom formatters used by the application.

This module provides utility functions for configuring logging from a JSON
configuration file, handlers for uncaught exceptions, and custom logging
filters/formatters used across the project.

Public API
- `setup_logging(logging_config)` — configure logging from a config file.
- `handle_unhandled_exception` / `handle_thread_exception` — hooks to log
    uncaught exceptions.
- `MyJSONFormatter` — JSON formatter for structured logging.
- `NonErrorFilter`, `KeywordFilter` — helpers to filter or mask logs.
"""
from pathlib import Path
import atexit
import datetime as dt
import json
import logging
from logging import Logger, Handler, LogRecord
import logging.config
import sys
import threading
from typing import Any, Type, override
from types import TracebackType


import constants

def setup_logging(logging_config: Path) -> Path:
    """Configure logging using a JSON config file.

    Loads the JSON logging configuration from `logging_config`, ensures any
    directories referenced by file handlers exist, applies the configuration,
    registers shutdown hooks for queue listeners, and installs exception
    hooks for unhandled exceptions and thread exceptions.

    Returns the folder path used by file handlers (last handler encountered
    with a `filename`), or raises SystemExit if the config file is missing.
    """
    try:
        with open(logging_config, encoding="utf-8") as f_in:
            config: dict[str, Any] = json.load(f_in)
    except FileNotFoundError:
        print(f"logging config file {logging_config} not found")
        sys.exit(constants.ExitCode.EXIT_FAILED_CLICK_USAGE.value)

    folder_path: Path = Path()
    for _, handler in config['handlers'].items():
        file: str = handler.get('filename', None)
        if file:
            folder_path = Path(file).parent
            folder_path.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(config)
    queue_handler: Handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

    sys.excepthook = handle_unhandled_exception
    threading.excepthook = handle_thread_exception
    logging.getLogger().info("logging configured")

    return folder_path

def handle_unhandled_exception(exc_type: Type[BaseException],
                               exc_value: BaseException,
                               exc_traceback: TracebackType) -> None:
    """
    Handler for unhandled exceptions that will write to the logs.
    """
    # Check if it's a KeyboardInterrupt and call the default hook if it is
    # This allows the program to exit normally with Ctrl+C
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # Log the exception with the traceback
    # Using logger.exception() is a shortcut that automatically adds exc_info
    logger: Logger = logging.getLogger("unhandled")
    logger.critical("unhandled exception occurred",
                    exc_info=(exc_type, exc_value, exc_traceback))

def handle_thread_exception(args: Any) -> None:
    """
    Custom exception hook to handle uncaught exceptions in threads.
    """
    logger: Logger = logging.getLogger("unhandled")
    logger.critical("exception in thread: %s",
                    args.thread.name,
                    exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

LOG_RECORD_BUILTIN_ATTRS: list[str] = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class MyJSONFormatter(logging.Formatter):
    """Structured JSON formatter for logging records.

    The formatter converts `LogRecord` instances to JSON objects, including
    configured keys and any extra attributes present on the record.
    """
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None) -> MyJSONFormatter:
        super().__init__()
        self.fmt_keys: dict[str, str] = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """Format a LogRecord as a JSON string.

        This builds a dictionary representation via `_prepare_log_dict` and
        serializes it to JSON.
        """
        message: dict[str, str | Any] = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord) -> dict[str, str | Any]:
        """Prepare a dictionary from a LogRecord suitable for JSON serialization.

        Extracts configured fields, timestamps, exception and stack traces,
        and any extra attributes attached to the LogRecord.
        """
        always_fields: dict[str, str] = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message: dict[str, str | Any] = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message

class NonErrorFilter(logging.Filter):
    """Filter that allows only non-error (INFO and below) records.

    Returns True for records at INFO level or below, False otherwise.
    """
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        """Return True when the record should be logged (level <= INFO)."""
        return record.levelno <= logging.INFO

class KeywordFilter(logging.Filter):
    """Filter that masks configured keywords in log messages.

    Keywords registered via `add_keyword`/`add_keywords` will be replaced with
    asterisks of the same length when present in a log message.
    """
    _keywords = []
    def __init__(self, name: str="KeywordFilter") -> KeywordFilter:
        """Initialize the filter with an optional name (passed to base class)."""
        super().__init__(name)

    @override
    def filter(self, record: LogRecord) -> bool | logging.LogRecord:
        """Mask any configured keywords in the record's message and allow it.

        Always returns True to let the record pass through after masking.
        """
        message: str = record.getMessage()
        for keyword in self._keywords:
            if keyword in message:
                record.msg = message.replace(keyword, "*" * len(keyword))
                record.args = []
                break

        return True

    @classmethod
    def add_keyword(cls, keyword: str) -> None:
        """Register a single keyword to be masked in future log messages."""
        cls._keywords.append(keyword)

    @classmethod
    def add_keywords(cls, keywords: list[str]) -> None:
        """Register multiple keywords to be masked in future log messages."""
        cls._keywords.extend(keywords)
