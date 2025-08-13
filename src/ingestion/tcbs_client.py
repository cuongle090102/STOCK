"""TCBS API wrapper for Vietnamese market data."""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from pydantic import BaseModel

from src.ingestion.base import DataSource, MarketDataPoint
from src.utils.logging import StructuredLogger


class TCBSConfig(BaseModel):
    """Configuration for TCBS API."""
    
    base_url: str = "https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance"
    timeout: int = 30
    rate_limit: int = 100  # requests per minute
    retry_attempts: int = 3
    retry_delay: float = 1.0


class TCBSClient(DataSource):
    """TCBS API client for Vietnamese market data."""
    
    # Popular Vietnamese stocks supported by TCBS
    SUPPORTED_SYMBOLS = [
        # VN30 stocks
        "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
        "VRE", "TPB", "MWG", "SSI", "POW", "TCB", "VHM", "ACB", "SAB", "KDH",
        "PDR", "DGC", "VJC", "SBT", "NVL", "PNJ", "DHG", "VIB", "EIB", "HDB",
        # Additional popular stocks
        "MBB", "STB", "SHB", "OCB", "LPB", "VPB", "TCB", "TPB",
        "REE", "GEX", "DXG", "BCM", "HSG", "NKG", "PAN", "PHR"
    ]
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize TCBS client.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("TCBS", config)
        self.config_model = TCBSConfig(**config)
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
        
        self.request_count += 1
    
    def connect(self) -> bool:
        """Establish connection to TCBS API.
        
        Returns:
            True if connection successful
        """
        try:
            self.session = requests.Session()
            
            # Set default headers
            headers = {
                "User-Agent": "VN-Algo-Trading/1.0",
                "Accept": "application/json"
            }
            
            self.session.headers.update(headers)
            
            # Test connection
            response = self._make_request("/symbols")
            
            if response and isinstance(response, dict):
                self.logger.info("Successfully connected to TCBS API")
                return True
            else:
                self.logger.error(f"Failed to connect to TCBS API: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to TCBS API."""
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("Disconnected from TCBS API")
    
    def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """Make HTTP request to TCBS API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response data
        """
        if not self.session:
            raise RuntimeError("Not connected to TCBS API")
        
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
                
                # TCBS API returns direct JSON data (not wrapped in status codes)
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
        """Fetch historical market data from TCBS.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Data interval (1D only supported by TCBS)
            
        Returns:
            DataFrame with historical OHLCV data
        """
        if interval != "1D":
            self.logger.warning("TCBS only supports daily (1D) interval, using 1D")
        
        # TCBS expects Unix timestamps
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        
        params = {
            "ticker": symbol.upper(),
            "type": "stock",
            "from": start_timestamp,
            "to": end_timestamp
        }
        
        try:
            self.logger.info(f"Fetching historical data for {symbol} from TCBS")
            
            response = self._make_request("/historical", params)
            
            if not response or not isinstance(response, dict):
                self.logger.error(f"Invalid response for {symbol}: {response}")
                return pd.DataFrame()
            
            # TCBS returns data in different format
            data = response.get("data", [])
            if not data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # TCBS column mapping
            column_mapping = {
                "tradingDate": "timestamp",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "volume": "volume",
                "value": "value"
            }
            
            # Rename columns if they exist
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Ensure required columns exist
            required_columns = ["timestamp", "open_price", "high_price", "low_price", "close_price", "volume"]
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return pd.DataFrame()
            
            # Convert timestamp
            if df["timestamp"].dtype == 'int64':
                # Unix timestamp
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')
            else:
                # String format
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Convert price columns to float
            price_columns = ["open_price", "high_price", "low_price", "close_price"]
            for col in price_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert volume to int
            df["volume"] = pd.to_numeric(df["volume"], errors='coerce').fillna(0).astype(int)
            
            # Handle value column
            if "value" in df.columns:
                df["value"] = pd.to_numeric(df["value"], errors='coerce')
            
            # Add symbol column
            df["symbol"] = symbol.upper()
            
            # Sort by timestamp and remove duplicates
            df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
            
            # Remove rows with NaN prices
            df = df.dropna(subset=price_columns)
            
            self.logger.info(f"Successfully fetched {len(df)} records for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch current market data from TCBS.
        
        Note: TCBS doesn't provide true real-time data, returns latest available
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of MarketDataPoint objects
        """
        data_points = []
        
        for symbol in symbols:
            try:
                # Get current stock info
                response = self._make_request(f"/stock/{symbol.upper()}")
                
                if not response:
                    continue
                
                # Extract current data
                current_data = response.get("data", {})
                if not current_data:
                    continue
                
                # Create MarketDataPoint from current data
                data_point = MarketDataPoint(
                    symbol=symbol.upper(),
                    timestamp=datetime.now(),
                    open_price=float(current_data.get("open", 0)),
                    high_price=float(current_data.get("high", 0)),
                    low_price=float(current_data.get("low", 0)),
                    close_price=float(current_data.get("close", 0)),
                    volume=int(current_data.get("volume", 0)),
                    value=float(current_data.get("value", 0))
                )
                
                data_points.append(data_point)
                
            except Exception as e:
                self.logger.error(f"Error fetching current data for {symbol}: {e}")
        
        return data_points
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists in TCBS.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if symbol is valid
        """
        try:
            # Check if symbol is in supported list
            if symbol.upper() in self.SUPPORTED_SYMBOLS:
                return True
            
            # Try to get stock info
            response = self._make_request(f"/stock/{symbol.upper()}")
            
            return response is not None and "data" in response
            
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {e}")
            return False
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """Get company information from TCBS.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with company information
        """
        try:
            response = self._make_request(f"/company/{symbol.upper()}")
            
            if response and "data" in response:
                data = response["data"]
                
                return {
                    "symbol": symbol.upper(),
                    "company_name": data.get("companyName"),
                    "industry": data.get("industry"),
                    "sector": data.get("sector"),
                    "market_cap": data.get("marketCap"),
                    "shares_outstanding": data.get("sharesOutstanding"),
                    "exchange": data.get("exchange", "HOSE")
                }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error getting company info for {symbol}: {e}")
            return {}
    
    def get_financial_ratios(self, symbol: str) -> Dict[str, Any]:
        """Get financial ratios from TCBS.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with financial ratios
        """
        try:
            response = self._make_request(f"/ratios/{symbol.upper()}")
            
            if response and "data" in response:
                data = response["data"]
                
                return {
                    "symbol": symbol.upper(),
                    "pe_ratio": data.get("pe"),
                    "pb_ratio": data.get("pb"),
                    "roe": data.get("roe"),
                    "roa": data.get("roa"),
                    "debt_to_equity": data.get("debtToEquity"),
                    "current_ratio": data.get("currentRatio"),
                    "quick_ratio": data.get("quickRatio")
                }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error getting financial ratios for {symbol}: {e}")
            return {}
    
    def get_dividend_history(self, symbol: str) -> pd.DataFrame:
        """Get dividend history from TCBS.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            DataFrame with dividend history
        """
        try:
            response = self._make_request(f"/dividends/{symbol.upper()}")
            
            if not response or "data" not in response:
                return pd.DataFrame()
            
            data = response["data"]
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # Standardize columns
            column_mapping = {
                "exDate": "ex_date",
                "recordDate": "record_date",
                "paymentDate": "payment_date",
                "cashDividend": "cash_dividend",
                "stockDividend": "stock_dividend"
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Convert date columns
            date_columns = ["ex_date", "record_date", "payment_date"]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert dividend amounts
            if "cash_dividend" in df.columns:
                df["cash_dividend"] = pd.to_numeric(df["cash_dividend"], errors='coerce')
            
            if "stock_dividend" in df.columns:
                df["stock_dividend"] = pd.to_numeric(df["stock_dividend"], errors='coerce')
            
            df["symbol"] = symbol.upper()
            
            return df.sort_values("ex_date", ascending=False).reset_index(drop=True)
            
        except Exception as e:
            self.logger.error(f"Error getting dividend history for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_indices(self) -> Dict[str, Any]:
        """Get Vietnamese market indices from TCBS.
        
        Returns:
            Dictionary with market indices data
        """
        try:
            indices_data = {}
            
            # Get VN-Index
            vnindex_response = self._make_request("/index/VNINDEX")
            if vnindex_response and "data" in vnindex_response:
                vnindex_data = vnindex_response["data"]
                indices_data["VNINDEX"] = {
                    "value": float(vnindex_data.get("close", 0)),
                    "change": float(vnindex_data.get("change", 0)),
                    "change_percent": float(vnindex_data.get("changePercent", 0)),
                    "volume": int(vnindex_data.get("volume", 0)),
                    "timestamp": datetime.now()
                }
            
            # Get VN30
            vn30_response = self._make_request("/index/VN30")
            if vn30_response and "data" in vn30_response:
                vn30_data = vn30_response["data"]
                indices_data["VN30"] = {
                    "value": float(vn30_data.get("close", 0)),
                    "change": float(vn30_data.get("change", 0)),
                    "change_percent": float(vn30_data.get("changePercent", 0)),
                    "volume": int(vn30_data.get("volume", 0)),
                    "timestamp": datetime.now()
                }
            
            return indices_data
            
        except Exception as e:
            self.logger.error(f"Error getting market indices: {e}")
            return {}
    
    def get_top_stocks(self, criteria: str = "volume") -> List[Dict[str, Any]]:
        """Get top stocks by various criteria.
        
        Args:
            criteria: Sorting criteria (volume, value, gainers, losers)
            
        Returns:
            List of top stocks data
        """
        try:
            endpoint_mapping = {
                "volume": "/top/volume",
                "value": "/top/value", 
                "gainers": "/top/gainers",
                "losers": "/top/losers"
            }
            
            endpoint = endpoint_mapping.get(criteria, "/top/volume")
            response = self._make_request(endpoint)
            
            if response and "data" in response:
                return response["data"]
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error getting top stocks by {criteria}: {e}")
            return []