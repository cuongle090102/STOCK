"""Logging configuration for the Vietnamese Algorithmic Trading System."""

import json
import logging
import logging.config
import os
import sys
from pathlib import Path
from typing import Any, Dict

from loguru import logger


class InterceptHandler(logging.Handler):
    """Intercept standard logging messages toward Loguru sinks."""

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record using Loguru."""
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: str = "logs/vnalgo.log",
    rotation: str = "1 day",
    retention: str = "30 days",
) -> None:
    """Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log format (json, text)
        log_file: Log file path
        rotation: Log rotation schedule
        retention: Log retention period
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_file).parent
    log_path.mkdir(parents=True, exist_ok=True)

    # Remove default loguru handler
    logger.remove()

    # Get log level from environment or use provided default
    level = os.getenv("LOG_LEVEL", log_level).upper()

    # Define log formats
    if log_format.lower() == "json":
        log_format_str = (
            '{"time": "{time:YYYY-MM-DD HH:mm:ss.SSS}", '
            '"level": "{level}", '
            '"module": "{module}", '
            '"function": "{function}", '
            '"line": {line}, '
            '"message": "{message}"}'
        )
    else:
        log_format_str = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

    # Add console handler
    logger.add(
        sys.stdout,
        level=level,
        format=log_format_str,
        colorize=log_format.lower() != "json",
        serialize=log_format.lower() == "json",
    )

    # Add file handler
    logger.add(
        log_file,
        level=level,
        format=log_format_str,
        rotation=rotation,
        retention=retention,
        compression="gz",
        serialize=log_format.lower() == "json",
    )

    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Set logging levels for noisy libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.WARNING)

    logger.info(f"Logging initialized with level: {level}")


def get_logger(name: str) -> Any:
    """Get a logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logger.bind(name=name)


def log_function_call(func_name: str, **kwargs: Any) -> None:
    """Log function call with parameters.
    
    Args:
        func_name: Function name
        **kwargs: Function parameters
    """
    logger.info(f"Calling {func_name}", extra={"parameters": kwargs})


def log_performance(func_name: str, duration: float, **metrics: Any) -> None:
    """Log performance metrics.
    
    Args:
        func_name: Function name
        duration: Execution duration in seconds
        **metrics: Additional performance metrics
    """
    logger.info(
        f"Performance: {func_name} completed in {duration:.4f}s",
        extra={"duration": duration, "metrics": metrics}
    )


def log_trading_event(event_type: str, **data: Any) -> None:
    """Log trading-specific events.
    
    Args:
        event_type: Type of trading event
        **data: Event data
    """
    logger.info(
        f"Trading Event: {event_type}",
        extra={"event_type": event_type, "data": data}
    )


def log_market_data(symbol: str, timestamp: str, **data: Any) -> None:
    """Log market data events.
    
    Args:
        symbol: Stock symbol
        timestamp: Data timestamp
        **data: Market data
    """
    logger.debug(
        f"Market Data: {symbol}",
        extra={"symbol": symbol, "timestamp": timestamp, "data": data}
    )


def log_risk_alert(alert_type: str, severity: str, **details: Any) -> None:
    """Log risk management alerts.
    
    Args:
        alert_type: Type of risk alert
        severity: Alert severity (LOW, MEDIUM, HIGH, CRITICAL)
        **details: Alert details
    """
    log_level = {
        "LOW": logger.info,
        "MEDIUM": logger.warning,
        "HIGH": logger.error,
        "CRITICAL": logger.critical,
    }.get(severity.upper(), logger.warning)

    log_level(
        f"Risk Alert: {alert_type}",
        extra={"alert_type": alert_type, "severity": severity, "details": details}
    )


class StructuredLogger:
    """Structured logger for consistent logging across the application."""

    def __init__(self, name: str):
        """Initialize structured logger.
        
        Args:
            name: Logger name
        """
        self.logger = logger.bind(component=name)
        self.name = name

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self.logger.critical(message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, **kwargs)

    def trading_event(self, event_type: str, **data: Any) -> None:
        """Log trading event."""
        self.logger.info(
            f"Trading Event: {event_type}",
            event_type=event_type,
            **data
        )

    def market_data(self, symbol: str, **data: Any) -> None:
        """Log market data."""
        self.logger.debug(
            f"Market Data: {symbol}",
            symbol=symbol,
            **data
        )

    def performance(self, operation: str, duration: float, **metrics: Any) -> None:
        """Log performance metrics."""
        self.logger.info(
            f"Performance: {operation} - {duration:.4f}s",
            operation=operation,
            duration=duration,
            **metrics
        )

    def risk_alert(self, alert_type: str, severity: str, **details: Any) -> None:
        """Log risk alert."""
        log_func = {
            "LOW": self.logger.info,
            "MEDIUM": self.logger.warning,
            "HIGH": self.logger.error,
            "CRITICAL": self.logger.critical,
        }.get(severity.upper(), self.logger.warning)

        log_func(
            f"Risk Alert: {alert_type}",
            alert_type=alert_type,
            severity=severity,
            **details
        )