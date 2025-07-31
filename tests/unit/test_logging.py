"""Unit tests for logging utilities."""

import json
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import os

from src.utils.logging import (
    setup_logging,
    get_logger,
    log_function_call,
    log_performance,
    log_trading_event,
    log_market_data,
    log_risk_alert,
    StructuredLogger
)


@pytest.mark.unit
class TestLoggingSetup:
    """Test logging setup functionality."""
    
    def test_setup_logging_default(self):
        """Test default logging setup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            setup_logging(log_file=log_file)
            
            # Check if log file is created
            assert Path(log_file).exists()
    
    def test_setup_logging_json_format(self):
        """Test JSON format logging setup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            setup_logging(log_format="json", log_file=log_file)
            
            # Check if log file is created
            assert Path(log_file).exists()
    
    def test_setup_logging_custom_level(self):
        """Test custom log level setup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            setup_logging(log_level="ERROR", log_file=log_file)
            
            # Check if log file is created
            assert Path(log_file).exists()
    
    @patch.dict(os.environ, {"LOG_LEVEL": "WARNING"})
    def test_setup_logging_env_override(self):
        """Test environment variable override for log level."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            setup_logging(log_level="INFO", log_file=log_file)
            
            # Environment should override the parameter
            assert Path(log_file).exists()


@pytest.mark.unit
class TestUtilityFunctions:
    """Test logging utility functions."""
    
    def test_get_logger(self):
        """Test logger creation."""
        logger = get_logger("test_component")
        assert logger is not None
    
    @patch('src.utils.logging.logger')
    def test_log_function_call(self, mock_logger):
        """Test function call logging."""
        log_function_call("test_function", param1="value1", param2=42)
        
        mock_logger.info.assert_called_once()
        args, kwargs = mock_logger.info.call_args
        assert "test_function" in args[0]
        assert "parameters" in kwargs["extra"]
    
    @patch('src.utils.logging.logger')
    def test_log_performance(self, mock_logger):
        """Test performance logging."""
        log_performance("test_operation", 1.234, records_processed=1000)
        
        mock_logger.info.assert_called_once()
        args, kwargs = mock_logger.info.call_args
        assert "test_operation" in args[0]
        assert "1.234" in args[0]
        assert "duration" in kwargs["extra"]
        assert "metrics" in kwargs["extra"]
    
    @patch('src.utils.logging.logger')
    def test_log_trading_event(self, mock_logger):
        """Test trading event logging."""
        log_trading_event("ORDER_PLACED", symbol="VIC", quantity=1000, price=100.5)
        
        mock_logger.info.assert_called_once()
        args, kwargs = mock_logger.info.call_args
        assert "ORDER_PLACED" in args[0]
        assert "event_type" in kwargs["extra"]
        assert "data" in kwargs["extra"]
    
    @patch('src.utils.logging.logger')
    def test_log_market_data(self, mock_logger):
        """Test market data logging."""
        log_market_data("VIC", "2024-01-01T09:00:00", price=100.5, volume=1000)
        
        mock_logger.debug.assert_called_once()
        args, kwargs = mock_logger.debug.call_args
        assert "VIC" in args[0]
        assert "symbol" in kwargs["extra"]
        assert "timestamp" in kwargs["extra"]
    
    @patch('src.utils.logging.logger')
    def test_log_risk_alert_low(self, mock_logger):
        """Test low severity risk alert logging."""
        log_risk_alert("POSITION_LIMIT", "LOW", current=0.04, limit=0.05)
        
        mock_logger.info.assert_called_once()
        args, kwargs = mock_logger.info.call_args
        assert "POSITION_LIMIT" in args[0]
    
    @patch('src.utils.logging.logger')
    def test_log_risk_alert_critical(self, mock_logger):
        """Test critical severity risk alert logging."""
        log_risk_alert("DRAWDOWN_BREACH", "CRITICAL", drawdown=0.15, limit=0.10)
        
        mock_logger.critical.assert_called_once()
        args, kwargs = mock_logger.critical.call_args
        assert "DRAWDOWN_BREACH" in args[0]


@pytest.mark.unit
class TestStructuredLogger:
    """Test StructuredLogger class."""
    
    def test_structured_logger_creation(self):
        """Test StructuredLogger creation."""
        logger = StructuredLogger("test_component")
        assert logger.name == "test_component"
        assert logger.logger is not None
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_info(self, mock_logger):
        """Test info logging."""
        structured_logger = StructuredLogger("test_component")
        structured_logger.info("Test message", key="value")
        
        # Verify the bound logger's info method was called
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_debug(self, mock_logger):
        """Test debug logging."""
        structured_logger = StructuredLogger("test_component")
        structured_logger.debug("Debug message", debug_data=123)
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_warning(self, mock_logger):
        """Test warning logging."""
        structured_logger = StructuredLogger("test_component")
        structured_logger.warning("Warning message", warning_code="W001")
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_error(self, mock_logger):
        """Test error logging."""
        structured_logger = StructuredLogger("test_component")
        structured_logger.error("Error message", error_code="E001")
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_critical(self, mock_logger):
        """Test critical logging."""
        structured_logger = StructuredLogger("test_component")
        structured_logger.critical("Critical message", error_code="C001")
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_trading_event(self, mock_logger):
        """Test trading event logging."""
        structured_logger = StructuredLogger("trading")
        structured_logger.trading_event(
            "ORDER_FILLED", 
            symbol="VIC", 
            quantity=1000, 
            price=100.5
        )
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_market_data(self, mock_logger):
        """Test market data logging."""
        structured_logger = StructuredLogger("market_data")
        structured_logger.market_data("VIC", price=100.5, volume=1000000)
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_performance(self, mock_logger):
        """Test performance logging."""
        structured_logger = StructuredLogger("performance")
        structured_logger.performance(
            "data_processing", 
            2.345, 
            records=10000, 
            throughput=4274
        )
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_risk_alert_medium(self, mock_logger):
        """Test medium severity risk alert."""
        structured_logger = StructuredLogger("risk")
        structured_logger.risk_alert(
            "VOLATILITY_SPIKE", 
            "MEDIUM", 
            current_vol=0.25, 
            threshold=0.20
        )
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_risk_alert_high(self, mock_logger):
        """Test high severity risk alert."""
        structured_logger = StructuredLogger("risk")
        structured_logger.risk_alert(
            "MARGIN_CALL", 
            "HIGH", 
            required_margin=500000, 
            available_cash=450000
        )
        
        assert mock_logger.bind.called
    
    @patch('src.utils.logging.logger')
    def test_structured_logger_unknown_severity(self, mock_logger):
        """Test unknown severity defaults to warning."""
        structured_logger = StructuredLogger("risk")
        structured_logger.risk_alert(
            "UNKNOWN_ALERT", 
            "UNKNOWN_SEVERITY", 
            data="test"
        )
        
        assert mock_logger.bind.called