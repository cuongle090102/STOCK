"""Pytest configuration and fixtures."""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pytest
import pandas as pd
from unittest.mock import Mock

from src.ingestion.base import DataSource, MarketDataPoint


@pytest.fixture(scope="session")
def test_config():
    """Test configuration fixture."""
    return {
        "database_url": "sqlite:///:memory:",
        "redis_url": "redis://localhost:6379/15",  # Use test database
        "log_level": "DEBUG",
        "testing": True,
    }


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    dates = pd.date_range(start='2024-01-01', end='2024-01-10', freq='D')
    
    data = []
    for i, date in enumerate(dates):
        base_price = 100 + i
        data.append({
            'symbol': 'VIC',
            'timestamp': date,
            'open_price': base_price,
            'high_price': base_price + 2,
            'low_price': base_price - 1.5,
            'close_price': base_price + 0.5,
            'volume': 1000000 + i * 10000,
            'value': (base_price + 0.5) * (1000000 + i * 10000)
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_market_data_points():
    """Sample market data points for testing."""
    return [
        MarketDataPoint(
            symbol="VIC",
            timestamp=datetime.now(),
            open_price=100.0,
            high_price=102.0,
            low_price=98.5,
            close_price=101.0,
            volume=1000000,
            value=101000000.0
        ),
        MarketDataPoint(
            symbol="VNM",
            timestamp=datetime.now(),
            open_price=85.0,
            high_price=87.0,
            low_price=84.0,
            close_price=86.5,
            volume=500000,
            value=43250000.0
        )
    ]


@pytest.fixture
def mock_data_source():
    """Mock data source for testing."""
    class MockDataSource(DataSource):
        def __init__(self):
            super().__init__("mock_source", {"test": True})
            self._connected = False
        
        def connect(self) -> bool:
            self._connected = True
            return True
        
        def disconnect(self) -> None:
            self._connected = False
        
        def get_historical_data(
            self,
            symbol: str,
            start_date: datetime,
            end_date: datetime,
            interval: str = "1D"
        ) -> pd.DataFrame:
            # Generate mock data
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            data = []
            
            for i, date in enumerate(dates):
                base_price = 100 + i
                data.append({
                    'symbol': symbol,
                    'timestamp': date,
                    'open_price': base_price,
                    'high_price': base_price + 2,
                    'low_price': base_price - 1.5,
                    'close_price': base_price + 0.5,
                    'volume': 1000000 + i * 10000,
                    'value': (base_price + 0.5) * (1000000 + i * 10000)
                })
            
            return pd.DataFrame(data)
        
        def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
            return [
                MarketDataPoint(
                    symbol=symbol,
                    timestamp=datetime.now(),
                    open_price=100.0,
                    high_price=102.0,
                    low_price=98.5,
                    close_price=101.0,
                    volume=1000000,
                    value=101000000.0
                )
                for symbol in symbols
            ]
        
        def validate_symbol(self, symbol: str) -> bool:
            # Mock validation - accept common Vietnamese stocks
            valid_symbols = ["VIC", "VNM", "HPG", "VCB", "FPT", "MSN", "CTG"]
            return symbol.upper() in valid_symbols
    
    return MockDataSource()


@pytest.fixture
def failing_data_source():
    """Mock failing data source for testing error handling."""
    class FailingDataSource(DataSource):
        def __init__(self):
            super().__init__("failing_source", {"test": True})
        
        def connect(self) -> bool:
            raise ConnectionError("Connection failed")
        
        def disconnect(self) -> None:
            pass
        
        def get_historical_data(
            self,
            symbol: str,
            start_date: datetime,
            end_date: datetime,
            interval: str = "1D"
        ) -> pd.DataFrame:
            raise ValueError("Data fetch failed")
        
        def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
            raise TimeoutError("Real-time data timeout")
        
        def validate_symbol(self, symbol: str) -> bool:
            raise ValueError("Symbol validation failed")
    
    return FailingDataSource()


@pytest.fixture
def vietnamese_stock_symbols():
    """Common Vietnamese stock symbols for testing."""
    return [
        "VIC",  # Vingroup
        "VNM",  # Vietnam Dairy Products
        "HPG",  # Hoa Phat Group
        "VCB",  # Vietcombank
        "FPT",  # FPT Corporation
        "MSN",  # Masan Group
        "CTG",  # Vietnam Technological and Commercial Joint Stock Bank
        "GAS",  # PetroVietnam Gas
        "PLX",  # Petrolimex
        "SAB"   # Sabeco
    ]


@pytest.fixture
def market_hours():
    """Vietnamese market trading hours."""
    return {
        "pre_market_start": "08:30",
        "market_open": "09:00",
        "market_close": "15:00",
        "timezone": "Asia/Ho_Chi_Minh"
    }


@pytest.fixture
def trading_calendar():
    """Vietnamese trading calendar for testing."""
    # Mock trading calendar - excludes weekends and some holidays
    base_date = datetime(2024, 1, 1)
    trading_days = []
    
    for i in range(365):
        current_date = base_date + timedelta(days=i)
        # Exclude weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:
            trading_days.append(current_date.date())
    
    return trading_days


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Setup test environment variables."""
    test_env_vars = {
        "APP_ENV": "testing",
        "LOG_LEVEL": "DEBUG",
        "DATABASE_URL": "sqlite:///:memory:",
        "REDIS_URL": "redis://localhost:6379/15",
        "TRADING_MODE": "PAPER",
        "ENABLE_METRICS_COLLECTION": "false",
    }
    
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    mock_redis = Mock()
    mock_redis.get.return_value = None
    mock_redis.set.return_value = True
    mock_redis.exists.return_value = False
    mock_redis.delete.return_value = 1
    mock_redis.ping.return_value = True
    
    return mock_redis


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    mock_producer = Mock()
    mock_producer.send.return_value = Mock()
    mock_producer.flush.return_value = None
    mock_producer.close.return_value = None
    
    return mock_producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer for testing."""
    mock_consumer = Mock()
    mock_consumer.subscribe.return_value = None
    mock_consumer.poll.return_value = {}
    mock_consumer.close.return_value = None
    
    return mock_consumer


# Performance testing fixtures
@pytest.fixture
def performance_test_data():
    """Large dataset for performance testing."""
    symbols = ["VIC", "VNM", "HPG", "VCB", "FPT"]
    dates = pd.date_range(start='2020-01-01', end='2024-01-01', freq='D')
    
    data = []
    for symbol in symbols:
        for i, date in enumerate(dates):
            base_price = 100 + (hash(symbol) % 50)
            data.append({
                'symbol': symbol,
                'timestamp': date,
                'open_price': base_price + (i % 10) - 5,
                'high_price': base_price + (i % 10) - 3,
                'low_price': base_price + (i % 10) - 7,
                'close_price': base_price + (i % 10) - 4,
                'volume': 1000000 + (i % 500000),
                'value': (base_price + (i % 10) - 4) * (1000000 + (i % 500000))
            })
    
    return pd.DataFrame(data)


# Mark fixtures for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )