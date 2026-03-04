import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging(config: dict) -> None:
    """Initialize logging with rotating file handlers and console output."""
    log_config = config.get("logging", {})
    level_str = log_config.get("level", "INFO")
    max_bytes = log_config.get("max_file_size_mb", 10) * 1024 * 1024
    backup_count = log_config.get("backup_count", 5)

    level = getattr(logging, level_str.upper(), logging.INFO)

    os.makedirs("logs", exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    # General system log
    system_handler = RotatingFileHandler(
        "logs/system.log", maxBytes=max_bytes, backupCount=backup_count
    )
    system_handler.setLevel(level)
    system_handler.setFormatter(formatter)
    root_logger.addHandler(system_handler)

    # Error-only log
    error_handler = RotatingFileHandler(
        "logs/errors.log", maxBytes=max_bytes, backupCount=backup_count
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    # Trade log (separate logger)
    trade_logger = logging.getLogger("trades")
    trade_handler = RotatingFileHandler(
        "logs/trades.log", maxBytes=max_bytes, backupCount=backup_count
    )
    trade_handler.setLevel(logging.DEBUG)
    trade_handler.setFormatter(formatter)
    trade_logger.addHandler(trade_handler)
    trade_logger.propagate = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
