"""SSI FastConnect API client for Vietnamese market data."""

import asyncio
import json
import time
import websocket
from datetime import datetime, timedelta
from threading import Thread, Event
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urljoin

import pandas as pd
import requests
from pydantic import BaseModel

from src.ingestion.base import DataSource, MarketDataPoint
from src.utils.logging import StructuredLogger


class SSIConfig(BaseModel):
    """Configuration for SSI FastConnect API."""
    
    base_url: str = "https://iboard.ssi.com.vn"
    ws_url: str = "wss://iboard.ssi.com.vn/ws"
    consumer_id: Optional[str] = None
    consumer_secret: Optional[str] = None
    timeout: int = 30
    reconnect_interval: int = 5
    heartbeat_interval: int = 30
    max_reconnect_attempts: int = 10


class SSIMessage(BaseModel):
    """SSI WebSocket message structure."""
    
    channel: str
    data: Dict[str, Any]
    timestamp: Optional[datetime] = None


class SSIFastConnectClient(DataSource):
    """SSI FastConnect API client for real-time Vietnamese market data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize SSI FastConnect client.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__("SSI", config)
        self.config_model = SSIConfig(**config)
        self.session: Optional[requests.Session] = None
        self.ws: Optional[websocket.WebSocketApp] = None
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        
        # WebSocket connection state
        self.connected = Event()
        self.should_reconnect = True
        self.reconnect_attempts = 0
        self.ws_thread: Optional[Thread] = None
        
        # Message handlers
        self.message_handlers: Dict[str, Callable] = {}
        self.tick_data_buffer: List[MarketDataPoint] = []
        
    def _authenticate(self) -> bool:
        """Authenticate with SSI API and get access token.
        
        Returns:
            True if authentication successful
        """
        if not self.config_model.consumer_id or not self.config_model.consumer_secret:
            self.logger.error("SSI consumer_id and consumer_secret required for authentication")
            return False
        
        try:
            auth_data = {
                "consumerID": self.config_model.consumer_id,
                "consumerSecret": self.config_model.consumer_secret
            }
            
            response = self.session.post(
                urljoin(self.config_model.base_url, "/api/v2/Market/AccessToken"),
                json=auth_data,
                timeout=self.config_model.timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == 200:
                self.access_token = data["data"]["accessToken"]
                # Token typically expires in 24 hours
                self.token_expires_at = datetime.now() + timedelta(hours=23)
                
                self.logger.info("Successfully authenticated with SSI API")
                return True
            else:
                self.logger.error(f"Authentication failed: {data}")
                return False
                
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            return False
    
    def _is_token_valid(self) -> bool:
        """Check if access token is still valid.
        
        Returns:
            True if token is valid and not expired
        """
        if not self.access_token or not self.token_expires_at:
            return False
        
        return datetime.now() < self.token_expires_at
    
    def connect(self) -> bool:
        """Establish connection to SSI FastConnect API.
        
        Returns:
            True if connection successful
        """
        try:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "VN-Algo-Trading/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json"
            })
            
            # Authenticate and get access token
            if not self._authenticate():
                return False
            
            # Test REST API connection
            test_response = self._make_request("/api/v2/Market/Securities")
            if not test_response:
                return False
            
            self.logger.info("Successfully connected to SSI FastConnect API")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to SSI FastConnect API."""
        # Stop WebSocket connection
        self.should_reconnect = False
        if self.ws:
            self.ws.close()
        
        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5)
        
        # Close REST session
        if self.session:
            self.session.close()
            self.session = None
        
        self.access_token = None
        self.token_expires_at = None
        self.logger.info("Disconnected from SSI FastConnect API")
    
    def _make_request(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request to SSI API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response data
        """
        if not self.session:
            raise RuntimeError("Not connected to SSI API")
        
        # Check if token needs refresh
        if not self._is_token_valid():
            if not self._authenticate():
                return None
        
        # Add authorization header
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        try:
            url = urljoin(self.config_model.base_url, endpoint)
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.config_model.timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == 200:
                return data
            else:
                self.logger.error(f"API error: {data}")
                return None
                
        except Exception as e:
            self.logger.error(f"Request failed for {endpoint}: {e}")
            return None
    
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> pd.DataFrame:
        """Fetch historical market data from SSI.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Data interval (1D, 1H, 15M, 5M, 1M)
            
        Returns:
            DataFrame with historical OHLCV data
        """
        # Map interval to SSI format
        interval_mapping = {
            "1D": "D",
            "1H": "H",
            "15M": "15",
            "5M": "5",
            "1M": "1"
        }
        
        ssi_interval = interval_mapping.get(interval, "D")
        
        params = {
            "symbol": symbol.upper(),
            "fromDate": start_date.strftime("%d/%m/%Y"),
            "toDate": end_date.strftime("%d/%m/%Y"),
            "resolution": ssi_interval,
            "type": "stock"
        }
        
        try:
            self.logger.info(f"Fetching historical data for {symbol} from SSI")
            
            response = self._make_request("/api/v2/Market/IntraDayOHLC", params)
            
            if not response:
                return pd.DataFrame()
            
            data = response.get("data", [])
            if not data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Standardize column names
            column_mapping = {
                "tradingDate": "timestamp",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "volume": "volume",
                "value": "value"
            }
            
            df = df.rename(columns=column_mapping)
            
            # Convert timestamp
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d/%m/%Y")
            
            # Convert numeric columns
            price_columns = ["open_price", "high_price", "low_price", "close_price"]
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors='coerce').fillna(0).astype(int)
            
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
    
    def _on_ws_message(self, ws, message: str) -> None:
        """Handle WebSocket message.
        
        Args:
            ws: WebSocket instance
            message: Raw message string
        """
        try:
            data = json.loads(message)
            
            # Handle different message types
            if "channel" in data:
                channel = data["channel"]
                
                if channel == "securities":
                    self._handle_tick_data(data.get("data", {}))
                elif channel == "heartbeat":
                    self._handle_heartbeat(data)
                
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
    def _handle_tick_data(self, data: Dict[str, Any]) -> None:
        """Handle tick data from WebSocket.
        
        Args:
            data: Tick data dictionary
        """
        try:
            # Convert SSI tick data to MarketDataPoint
            data_point = MarketDataPoint(
                symbol=data.get("sym", ""),
                timestamp=datetime.now(),  # SSI provides real-time data
                open_price=float(data.get("op", 0)),
                high_price=float(data.get("hp", 0)),
                low_price=float(data.get("lp", 0)),
                close_price=float(data.get("cp", 0)),
                volume=int(data.get("vol", 0)),
                value=float(data.get("val", 0))
            )
            
            # Add to buffer
            self.tick_data_buffer.append(data_point)
            
            # Keep buffer size manageable
            if len(self.tick_data_buffer) > 1000:
                self.tick_data_buffer = self.tick_data_buffer[-500:]
            
        except Exception as e:
            self.logger.error(f"Error handling tick data: {e}")
    
    def _handle_heartbeat(self, data: Dict[str, Any]) -> None:
        """Handle heartbeat message.
        
        Args:
            data: Heartbeat data
        """
        self.logger.debug("Received heartbeat from SSI WebSocket")
    
    def _on_ws_error(self, ws, error) -> None:
        """Handle WebSocket error.
        
        Args:
            ws: WebSocket instance
            error: Error object
        """
        self.logger.error(f"WebSocket error: {error}")
        self.connected.clear()
    
    def _on_ws_close(self, ws, close_status_code, close_msg) -> None:
        """Handle WebSocket close.
        
        Args:
            ws: WebSocket instance
            close_status_code: Close status code
            close_msg: Close message
        """
        self.logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected.clear()
        
        if self.should_reconnect and self.reconnect_attempts < self.config_model.max_reconnect_attempts:
            self.reconnect_attempts += 1
            self.logger.info(f"Attempting to reconnect ({self.reconnect_attempts}/{self.config_model.max_reconnect_attempts})")
            time.sleep(self.config_model.reconnect_interval)
            self._connect_websocket()
    
    def _on_ws_open(self, ws) -> None:
        """Handle WebSocket open.
        
        Args:
            ws: WebSocket instance
        """
        self.logger.info("WebSocket connection established")
        self.connected.set()
        self.reconnect_attempts = 0
        
        # Subscribe to market data
        self._subscribe_to_channels()
    
    def _subscribe_to_channels(self) -> None:
        """Subscribe to WebSocket channels."""
        if not self.ws:
            return
        
        # Subscribe to securities channel for tick data
        subscribe_msg = {
            "channel": "sub-securities",
            "data": {
                "symbols": ["VIC", "VNM", "HPG", "VCB", "FPT"]  # Subscribe to top stocks
            }
        }
        
        try:
            self.ws.send(json.dumps(subscribe_msg))
            self.logger.info("Subscribed to securities channel")
        except Exception as e:
            self.logger.error(f"Error subscribing to channels: {e}")
    
    def _connect_websocket(self) -> bool:
        """Connect to SSI WebSocket for real-time data.
        
        Returns:
            True if connection successful
        """
        if not self._is_token_valid():
            if not self._authenticate():
                return False
        
        try:
            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                f"{self.config_model.ws_url}?access_token={self.access_token}",
                on_message=self._on_ws_message,
                on_error=self._on_ws_error,
                on_close=self._on_ws_close,
                on_open=self._on_ws_open
            )
            
            # Start WebSocket in separate thread
            self.ws_thread = Thread(target=self.ws.run_forever, daemon=True)
            self.ws_thread.start()
            
            # Wait for connection
            if self.connected.wait(timeout=10):
                return True
            else:
                self.logger.error("WebSocket connection timeout")
                return False
                
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            return False
    
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch real-time market data from SSI WebSocket.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of MarketDataPoint objects
        """
        # Start WebSocket connection if not already connected
        if not self.connected.is_set():
            if not self._connect_websocket():
                return []
        
        # Return buffered tick data for requested symbols
        symbol_set = set(s.upper() for s in symbols)
        
        filtered_data = [
            point for point in self.tick_data_buffer
            if point.symbol in symbol_set
        ]
        
        return filtered_data
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists in SSI.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if symbol is valid
        """
        try:
            params = {"symbol": symbol.upper()}
            response = self._make_request("/api/v2/Market/Securities", params)
            
            if response and response.get("data"):
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {e}")
            return False
    
    def get_market_status(self) -> Dict[str, Any]:
        """Get current market status from SSI.
        
        Returns:
            Market status information
        """
        try:
            response = self._make_request("/api/v2/Market/MarketStatus")
            
            if response and response.get("data"):
                return response["data"]
            return {}
            
        except Exception as e:
            self.logger.error(f"Error getting market status: {e}")
            return {}