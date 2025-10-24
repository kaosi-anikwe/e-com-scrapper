"""
utils/logger.py

Configurable logging utility for ecommerce_scrapers project.
Provides `get_logger()` which returns a configured `logging.Logger` instance
with file rotation (size-based or time-based), optional console output,
and an optional simple JSON formatter.

Example:
    from utils.logger import get_logger
    logger = get_logger("amazon", log_file="logs/amazon.log", rotation="size")
    logger.info("started")

"""
from __future__ import annotations

import json
import logging
import logging.handlers
from pathlib import Path
from typing import Optional


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for logs.

    Produces one-line JSON objects with timestamp, level, logger name and message.
    """

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - small helper
        record_dict = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Include exception info when present
        if record.exc_info:
            record_dict["exc_info"] = self.formatException(record.exc_info)
        # Include any extra attributes
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in ("name", "msg", "args", "levelname", "levelno", "pathname",
                         "filename", "module", "exc_info", "exc_text", "stack_info",
                         "lineno", "funcName", "created", "msecs", "relativeCreated",
                         "thread", "threadName", "processName", "process")
        }
        if extras:
            record_dict["extra"] = extras # type: ignore
        return json.dumps(record_dict, default=str)


def get_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    rotation: str = "size",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    when: str = "midnight",
    interval: int = 1,
    console: bool = True,
    use_json: bool = False,
    fmt: Optional[str] = None,
    datefmt: str = "%Y-%m-%d %H:%M:%S",
) -> logging.Logger:
    """Return a configured logger.

    Parameters
    ----------
    name: str
        Logger name (also used for default file name when log_file is None).
    log_file: Optional[str]
        Path to the log file. If omitted, defaults to `logs/{name}.log`.
    level: int
        Logging level from the `logging` module.
    rotation: str
        One of `"size"` (RotatingFileHandler) or `"time"` (TimedRotatingFileHandler).
    max_bytes: int
        Max bytes for size-based rotation.
    backup_count: int
        Number of backup files to keep.
    when: str
        When to rotate for time-based rotation (see TimedRotatingFileHandler docs).
    interval: int
        Interval multiplier for time-based rotation.
    console: bool
        Add a console (StreamHandler) in addition to the file handler.
    use_json: bool
        Use JSON formatter for logs.
    fmt: Optional[str]
        Format string for human-readable logs. Defaults to "%(asctime)s - %(name)s - %(levelname)s - %(message)s".
    datefmt: str
        Date format used in logs.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove all existing handlers so reconfiguring works predictably
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # Ensure logs directory
    if not log_file:
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = logs_dir / f"{name}.log"
        log_file = str(log_file_path)
    else:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Choose formatter
    if use_json:
        formatter = JsonFormatter(datefmt=datefmt)
    else:
        human_fmt = fmt or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(human_fmt, datefmt=datefmt)

    # File handler with rotation
    if rotation == "time":
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_file,
            when=when,
            interval=interval,
            backupCount=backup_count,
            utc=False,
        )
    else:
        # default to size-based rotation
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )

    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Optional console handler
    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Propagate False so logs don't get duplicated by root logger
    logger.propagate = False

    return logger


# Convenience aliases
def get_amazon_logger(**kwargs) -> logging.Logger:
    return get_logger("amazon", **kwargs)


def get_etsy_logger(**kwargs) -> logging.Logger:
    return get_logger("etsy", **kwargs)


def get_shopify_logger(**kwargs) -> logging.Logger:
    return get_logger("shopify", **kwargs)


def get_ebay_logger(**kwargs) -> logging.Logger:
    return get_logger("ebay", **kwargs)
