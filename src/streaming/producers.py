"""Kafka producers for Vietnamese market data streaming."""

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel

from src.ingestion.base import MarketDataPoint
from src.ingestion.vietnamese_extractor import VietnameseMarketExtractor
from src.utils.logging import StructuredLogger


class KafkaConfig(BaseModel):
    """Configuration for Kafka producer."""
    
    bootstrap_servers: List[str] = ["localhost:9092"]
    client_id: str = "vn-algo-producer"
    acks: str = "all"  # Wait for all replicas to acknowledge
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 10  # Wait up to 10ms to batch records
    buffer_memory: int = 33554432  # 32MB
    compression_type: str = "snappy"
    max_request_size: int = 1048576  # 1MB
    enable_idempotence: bool = True
    
    # Topic configuration
    topics: Dict[str, str] = {
        "market_ticks": "vn-market-ticks",
        "market_bars": "vn-market-bars", 
        "market_indices": "vn-market-indices",
        "trading_signals": "vn-trading-signals",
        "risk_events": "vn-risk-events"
    }
    
    # Schema Registry
    schema_registry_url: str = "http://localhost:8081"


class MarketDataMessage(BaseModel):
    """Standard market data message format for Kafka."""
    
    source: str
    symbol: str
    timestamp: datetime
    message_type: str  # tick, bar, index, signal, risk
    data: Dict[str, Any]
    sequence_id: Optional[int] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class VietnameseMarketProducer:
    """Kafka producer for Vietnamese market data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize market data producer.
        
        Args:
            config: Kafka configuration dictionary
        """
        self.config = KafkaConfig(**config)
        self.logger = StructuredLogger("MarketProducer")
        self.producer: Optional[KafkaProducer] = None
        self.extractor: Optional[VietnameseMarketExtractor] = None
        self.sequence_counter = 0
        self.is_running = False
        
        # Performance metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.last_batch_time = time.time()
        
    def connect(self) -> bool:
        """Connect to Kafka cluster.
        
        Returns:
            True if connection successful
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=self.config.client_id,
                acks=self.config.acks,
                retries=self.config.retries,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                buffer_memory=self.config.buffer_memory,
                compression_type=self.config.compression_type,
                max_request_size=self.config.max_request_size,
                enable_idempotence=self.config.enable_idempotence,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            # Test connection by getting metadata
            metadata = self.producer.bootstrap_connected()
            if not metadata:
                self.logger.error("Failed to connect to Kafka bootstrap servers")
                return False
            
            self.logger.info(f"Connected to Kafka cluster: {self.config.bootstrap_servers}")
            
            # Initialize data extractor
            self.extractor = VietnameseMarketExtractor()
            connection_results = self.extractor.connect_all_sources()
            connected_sources = sum(connection_results.values())
            
            self.logger.info(f"Connected to {connected_sources} data sources")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Kafka cluster."""
        self.is_running = False
        
        if self.producer:
            try:
                # Wait for all pending messages to be sent
                self.producer.flush(timeout=30)
                self.producer.close(timeout=30)
                self.producer = None
                self.logger.info("Disconnected from Kafka cluster")
            except Exception as e:
                self.logger.error(f"Error disconnecting from Kafka: {e}")
        
        if self.extractor:
            for source in self.extractor.sources.values():
                source.disconnect()
    
    def _get_next_sequence_id(self) -> int:
        """Get next sequence ID for message ordering.
        
        Returns:
            Next sequence ID
        """
        self.sequence_counter += 1
        return self.sequence_counter
    
    def _create_message(
        self, 
        source: str,
        symbol: str,
        message_type: str,
        data: Dict[str, Any]
    ) -> MarketDataMessage:
        """Create standardized market data message.
        
        Args:
            source: Data source name
            symbol: Stock symbol
            message_type: Type of message
            data: Message data
            
        Returns:
            MarketDataMessage object
        """
        return MarketDataMessage(
            source=source,
            symbol=symbol,
            timestamp=datetime.now(),
            message_type=message_type,
            data=data,
            sequence_id=self._get_next_sequence_id()
        )
    
    def send_market_data_point(
        self, 
        data_point: MarketDataPoint, 
        source: str = "unknown"
    ) -> bool:
        """Send market data point to Kafka.
        
        Args:
            data_point: Market data point
            source: Data source name
            
        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.logger.error("Producer not connected")
            return False
        
        try:
            # Create message
            message = self._create_message(
                source=source,
                symbol=data_point.symbol,
                message_type="tick",
                data=data_point.dict()
            )
            
            # Send to appropriate topic
            topic = self.config.topics["market_ticks"]
            key = data_point.symbol  # Partition by symbol
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message.dict()
            )
            
            # Add callback for success/failure tracking
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send market data: {e}")
            self.messages_failed += 1
            return False
    
    def send_ohlcv_bar(
        self, 
        symbol: str,
        timeframe: str,
        bar_data: Dict[str, Any],
        source: str = "aggregated"
    ) -> bool:
        """Send OHLCV bar data to Kafka.
        
        Args:
            symbol: Stock symbol
            timeframe: Bar timeframe (1M, 5M, 15M, 1H, 1D)
            bar_data: OHLCV bar data
            source: Data source name
            
        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.logger.error("Producer not connected")
            return False
        
        try:
            # Add timeframe to data
            enhanced_data = {
                **bar_data,
                "timeframe": timeframe
            }
            
            message = self._create_message(
                source=source,
                symbol=symbol,
                message_type="bar",
                data=enhanced_data
            )
            
            topic = self.config.topics["market_bars"]
            key = f"{symbol}-{timeframe}"
            
            future = self.producer.send(
                topic=topic,
                key=key, 
                value=message.dict()
            )
            
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send bar data: {e}")
            self.messages_failed += 1
            return False
    
    def send_market_index(
        self,
        index_name: str,
        index_data: Dict[str, Any],
        source: str = "composite"
    ) -> bool:
        """Send market index data to Kafka.
        
        Args:
            index_name: Index name (VN-Index, VN30, etc.)
            index_data: Index data
            source: Data source name
            
        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.logger.error("Producer not connected")
            return False
        
        try:
            message = self._create_message(
                source=source,
                symbol=index_name,
                message_type="index",
                data=index_data
            )
            
            topic = self.config.topics["market_indices"]
            key = index_name
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message.dict()
            )
            
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send index data: {e}")
            self.messages_failed += 1
            return False
    
    def send_trading_signal(
        self,
        symbol: str,
        signal_data: Dict[str, Any],
        source: str = "strategy"
    ) -> bool:
        """Send trading signal to Kafka.
        
        Args:
            symbol: Stock symbol
            signal_data: Signal data (action, confidence, etc.)
            source: Signal source/strategy name
            
        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.logger.error("Producer not connected")
            return False
        
        try:
            message = self._create_message(
                source=source,
                symbol=symbol,
                message_type="signal",
                data=signal_data
            )
            
            topic = self.config.topics["trading_signals"]
            key = symbol
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message.dict()
            )
            
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send trading signal: {e}")
            self.messages_failed += 1
            return False
    
    def send_risk_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        symbol: Optional[str] = None
    ) -> bool:
        """Send risk management event to Kafka.
        
        Args:
            event_type: Type of risk event
            event_data: Event data
            symbol: Optional symbol if event is symbol-specific
            
        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.logger.error("Producer not connected")
            return False
        
        try:
            # Add event type to data
            enhanced_data = {
                **event_data,
                "event_type": event_type
            }
            
            message = self._create_message(
                source="risk_manager",
                symbol=symbol or "PORTFOLIO",
                message_type="risk",
                data=enhanced_data
            )
            
            topic = self.config.topics["risk_events"]
            key = symbol or event_type
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message.dict()
            )
            
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send risk event: {e}")
            self.messages_failed += 1
            return False
    
    def _on_send_success(self, record_metadata) -> None:
        """Callback for successful message send.
        
        Args:
            record_metadata: Kafka record metadata
        """
        self.messages_sent += 1
        self.logger.debug(
            f"Message sent successfully: topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, offset={record_metadata.offset}"
        )
    
    def _on_send_error(self, exception) -> None:
        """Callback for failed message send.
        
        Args:
            exception: Exception that occurred
        """
        self.messages_failed += 1
        self.logger.error(f"Failed to send message: {exception}")
    
    def batch_send_historical_data(
        self,
        symbols: List[str],
        days: int = 30,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Send historical data in batches to Kafka.
        
        Args:
            symbols: List of symbols to fetch
            days: Number of days of historical data
            batch_size: Number of records per batch
            
        Returns:
            Results summary
        """
        if not self.extractor:
            self.logger.error("Data extractor not initialized")
            return {"success": False, "error": "Extractor not initialized"}
        
        self.logger.info(f"Starting batch send for {len(symbols)} symbols, {days} days")
        
        start_time = time.time()
        total_records = 0
        success_symbols = []
        failed_symbols = []
        
        from datetime import timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        for symbol in symbols:
            try:
                self.logger.info(f"Processing {symbol}...")
                
                # Get historical data
                df = self.extractor.get_historical_data(symbol, start_date, end_date)
                
                if df.empty:
                    self.logger.warning(f"No data available for {symbol}")
                    failed_symbols.append(symbol)
                    continue
                
                # Send in batches
                records_sent = 0
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    for _, row in batch.iterrows():
                        # Convert to MarketDataPoint
                        data_point = MarketDataPoint(
                            symbol=row['symbol'],
                            timestamp=row['timestamp'],
                            open_price=row['open_price'],
                            high_price=row['high_price'],
                            low_price=row['low_price'],
                            close_price=row['close_price'],
                            volume=int(row['volume']),
                            value=row.get('value')
                        )
                        
                        # Send to Kafka
                        if self.send_market_data_point(data_point, source="historical"):
                            records_sent += 1
                        else:
                            self.logger.error(f"Failed to send record for {symbol}")
                    
                    # Flush batch
                    if self.producer:
                        self.producer.flush()
                    
                    # Small delay between batches to avoid overwhelming Kafka
                    time.sleep(0.1)
                
                total_records += records_sent
                success_symbols.append(symbol)
                self.logger.info(f"Sent {records_sent} records for {symbol}")
                
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                failed_symbols.append(symbol)
        
        elapsed_time = time.time() - start_time
        
        results = {
            "success": True,
            "total_records": total_records,
            "success_symbols": success_symbols,
            "failed_symbols": failed_symbols,
            "elapsed_time": elapsed_time,
            "records_per_second": total_records / elapsed_time if elapsed_time > 0 else 0
        }
        
        self.logger.info(f"Batch send completed: {results}")
        return results
    
    def get_producer_metrics(self) -> Dict[str, Any]:
        """Get producer performance metrics.
        
        Returns:
            Producer metrics dictionary
        """
        current_time = time.time()
        elapsed_time = current_time - self.last_batch_time
        
        metrics = {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "success_rate": self.messages_sent / (self.messages_sent + self.messages_failed) if (self.messages_sent + self.messages_failed) > 0 else 0,
            "messages_per_second": self.messages_sent / elapsed_time if elapsed_time > 0 else 0,
            "sequence_counter": self.sequence_counter,
            "is_connected": self.producer is not None,
            "timestamp": current_time
        }
        
        if self.producer:
            # Get Kafka producer metrics if available
            try:
                kafka_metrics = self.producer.metrics()
                metrics["kafka_metrics"] = {
                    "buffer_available_bytes": kafka_metrics.get("buffer-available-bytes", {}).get("value", 0),
                    "buffer_total_bytes": kafka_metrics.get("buffer-total-bytes", {}).get("value", 0),
                    "record_send_rate": kafka_metrics.get("record-send-rate", {}).get("value", 0),
                    "batch_size_avg": kafka_metrics.get("batch-size-avg", {}).get("value", 0)
                }
            except Exception as e:
                self.logger.debug(f"Could not get Kafka metrics: {e}")
        
        return metrics
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()