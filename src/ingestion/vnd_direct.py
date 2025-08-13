"""VND Direct API integration for Vietnamese market data."""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp
import pandas as pd
import requests
from pydantic import BaseModel, Field

from src.ingestion.base import DataSource, MarketDataPoint
from src.utils.logging import StructuredLogger


class VNDDirectConfig(BaseModel):
    """Configuration for VND Direct API."""
    
    base_url: str = "https://api.vndirect.com.vn"
    api_key: Optional[str] = None
    timeout: int = 30
    rate_limit: int = 60  # requests per minute
    retry_attempts: int = 3
    retry_delay: float = 1.0


class VNDDirectResponse(BaseModel):
    """Standard VND Direct API response structure."""
    
    code: str
    message: str
    data: List[Dict[str, Any]] = Field(default_factory=list)
    total: Optional[int] = None


class VNDDirectClient(DataSource):
    """VND Direct API client for Vietnamese market data."""
    
    # VN30 stocks - primary targets
    VN30_SYMBOLS = [
        "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
        "VRE", "TPB", "MWG", "SSI", "POW", "TCB", "VHM", "ACB", "SAB", "KDH",
        "PDR", "DGC", "VJC", "SBT", "NVL", "PNJ", "DHG", "VIB", "EIB", "HDB"
    ]
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize VND Direct client.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("VNDirect", config)
        self.config_model = VNDDirectConfig(**config)
        self.session: Optional[requests.Session] = None
        self.last_request_time = 0.0
        self.request_count = 0
        self.rate_limit_window_start = time.time()
        
    def _ensure_rate_limit(self) -> None:
        """Ensure we don't exceed rate limits."""
        current_time = time.time()
        
        # Reset counter if window has passed
        if current_time - self.rate_limit_window_start >= 60:
            self.request_count = 0
            self.rate_limit_window_start = current_time
        
        # Check if we've hit rate limit
        if self.request_count >= self.config_model.rate_limit:
            sleep_time = 60 - (current_time - self.rate_limit_window_start)
            if sleep_time > 0:
                self.logger.warning(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
                self.request_count = 0
                self.rate_limit_window_start = time.time()
        
        # Increment request count
        self.request_count += 1
        
    def connect(self) -> bool:
        """Establish connection to VND Direct API.
        
        Returns:
            True if connection successful
        """
        try:
            self.session = requests.Session()
            
            # Set default headers
            headers = {
                "User-Agent": "VN-Algo-Trading/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            
            if self.config_model.api_key:
                headers["Authorization"] = f"Bearer {self.config_model.api_key}"
            
            self.session.headers.update(headers)
            
            # Test connection with a simple request
            response = self._make_request("/hose/stock/symbols")
            
            if response and response.get("code") == "200":
                self.logger.info("Successfully connected to VND Direct API")
                return True
            else:
                self.logger.error(f"Failed to connect: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to VND Direct API."""
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("Disconnected from VND Direct API")
    
    def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request to VND Direct API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response data
        """
        if not self.session:
            raise RuntimeError("Not connected to VND Direct API")
        
        self._ensure_rate_limit()
        
        url = urljoin(self.config_model.base_url, endpoint)
        
        for attempt in range(self.config_model.retry_attempts):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config_model.timeout
                )
                response.raise_for_status()
                
                data = response.json()
                self.logger.debug(f"API request successful: {endpoint}")
                return data
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.config_model.retry_attempts - 1:
                    time.sleep(self.config_model.retry_delay * (2 ** attempt))
                else:
                    self.logger.error(f"All retry attempts failed for {endpoint}")
                    return None
    
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> pd.DataFrame:
        """Fetch historical market data from VND Direct.
        
        Args:
            symbol: Stock symbol (e.g., 'VIC', 'VNM')
            start_date: Start date
            end_date: End date
            interval: Data interval (1D only supported)
            
        Returns:
            DataFrame with historical OHLCV data
        """
        if interval != "1D":
            raise ValueError("VND Direct only supports daily (1D) interval")
        
        # Format dates for API
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        params = {
            "symbol": symbol.upper(),
            "from": start_str,
            "to": end_str
        }
        
        try:
            self.logger.info(f"Fetching historical data for {symbol} from {start_str} to {end_str}")
            
            # Use stock price history endpoint
            response = self._make_request("/hose/stock/prices", params)
            
            if not response or response.get("code") != "200":
                self.logger.error(f"API error for {symbol}: {response}")
                return pd.DataFrame()
            
            data = response.get("data", [])
            if not data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Standardize column names and data types
            column_mapping = {
                "date": "timestamp",
                "open": "open_price",
                "high": "high_price", 
                "low": "low_price",
                "close": "close_price",
                "volume": "volume",
                "value": "value"
            }
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_columns = ["timestamp", "open_price", "high_price", "low_price", "close_price", "volume"]
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return pd.DataFrame()
            
            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Convert price columns to float
            price_columns = ["open_price", "high_price", "low_price", "close_price"]
            for col in price_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert volume to int
            df["volume"] = pd.to_numeric(df["volume"], errors='coerce').fillna(0).astype(int)
            
            # Handle value column (trade value in VND)
            if "value" in df.columns:
                df["value"] = pd.to_numeric(df["value"], errors='coerce')
            
            # Add symbol column
            df["symbol"] = symbol.upper()
            
            # Sort by timestamp
            df = df.sort_values("timestamp").reset_index(drop=True)
            
            # Remove any rows with NaN prices
            df = df.dropna(subset=price_columns)
            
            self.logger.info(f"Successfully fetched {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch real-time market data from VND Direct.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of MarketDataPoint objects
        """
        # VND Direct doesn't provide real-time API in free tier
        # This would typically require WebSocket connection or premium subscription
        self.logger.warning("Real-time data not available in VND Direct free tier")
        
        # For now, return latest available data (current day)
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        data_points = []
        
        for symbol in symbols:
            try:
                df = self.get_historical_data(symbol, yesterday, today)
                if not df.empty:
                    latest = df.iloc[-1]
                    
                    data_point = MarketDataPoint(
                        symbol=latest["symbol"],
                        timestamp=latest["timestamp"],
                        open_price=latest["open_price"],
                        high_price=latest["high_price"],
                        low_price=latest["low_price"],
                        close_price=latest["close_price"],
                        volume=int(latest["volume"]),
                        value=latest.get("value")
                    )
                    
                    data_points.append(data_point)
                    
            except Exception as e:
                self.logger.error(f"Error fetching real-time data for {symbol}: {e}")
        
        return data_points
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists in VND Direct.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if symbol is valid
        """
        try:
            # Check if symbol is in VN30 (quick validation)
            if symbol.upper() in self.VN30_SYMBOLS:
                return True
            
            # For other symbols, try to fetch recent data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            df = self.get_historical_data(symbol, start_date, end_date)
            return not df.empty
            
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {e}")
            return False
    
    def get_vn30_symbols(self) -> List[str]:
        """Get list of VN30 symbols.
        
        Returns:
            List of VN30 stock symbols
        """
        return self.VN30_SYMBOLS.copy()
    
    def get_market_overview(self) -> Dict[str, Any]:
        """Get market overview data.
        
        Returns:
            Market overview information
        """
        try:
            # Get VN-Index data
            response = self._make_request("/hose/index/vnindex")
            
            if response and response.get("code") == "200":
                data = response.get("data", {})
                
                return {
                    "vnindex": {
                        "value": data.get("indexValue"),
                        "change": data.get("change"),
                        "change_percent": data.get("changePercent"),
                        "volume": data.get("totalVolume"),
                        "value": data.get("totalValue"),
                        "timestamp": datetime.now()
                    }
                }
            else:
                self.logger.error(f"Failed to get market overview: {response}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error getting market overview: {e}")
            return {}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on VND Direct API.
        
        Returns:
            Health status dictionary
        """
        try:
            start_time = datetime.now()
            
            # Test with a simple API call
            response = self._make_request("/hose/stock/symbols", {"page": 1, "size": 1})
            
            end_time = datetime.now()
            latency = (end_time - start_time).total_seconds()
            
            if response and response.get("code") == "200":
                return {
                    "source": self.name,
                    "status": "healthy",
                    "latency_seconds": latency,
                    "timestamp": end_time.isoformat(),
                    "connected": True,
                    "rate_limit_remaining": self.config_model.rate_limit - self.request_count
                }
            else:
                return {
                    "source": self.name,
                    "status": "unhealthy",
                    "latency_seconds": latency,
                    "timestamp": end_time.isoformat(),
                    "connected": False,
                    "error": f"API returned: {response}"
                }
                
        except Exception as e:
            return {
                "source": self.name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "connected": False
            }