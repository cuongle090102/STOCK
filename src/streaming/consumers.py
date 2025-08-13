"""Kafka consumers for Vietnamese market data streaming."""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable, Set
from threading import Thread, Event
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pydantic import BaseModel

from src.streaming.producers import MarketDataMessage
from src.utils.logging import StructuredLogger


class ConsumerConfig(BaseModel):
    """Configuration for Kafka consumer."""
    
    bootstrap_servers: List[str] = ["localhost:9092"]
    group_id: str = "vn-algo-consumers"
    client_id: str = "vn-algo-consumer"
    auto_offset_reset: str = "latest"  # Start from latest messages
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000  # 5 minutes
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    
    # Message processing
    max_retries: int = 3
    retry_backoff_ms: int = 1000


class MessageHandler:
    """Base class for message handlers."""
    
    def __init__(self, name: str):
        """Initialize message handler.
        
        Args:
            name: Handler name
        """
        self.name = name
        self.logger = StructuredLogger(f"Handler.{name}")
        self.processed_count = 0
        self.error_count = 0
    
    def handle_message(self, message: MarketDataMessage) -> bool:
        """Handle a market data message.
        
        Args:
            message: Market data message
            
        Returns:
            True if handled successfully
        """
        try:
            result = self.process_message(message)
            if result:
                self.processed_count += 1
            else:
                self.error_count += 1
            return result
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            self.error_count += 1
            return False
    
    def process_message(self, message: MarketDataMessage) -> bool:
        """Process message - to be implemented by subclasses.
        
        Args:
            message: Market data message
            
        Returns:
            True if processed successfully
        """
        raise NotImplementedError
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics.
        
        Returns:
            Statistics dictionary
        """
        total = self.processed_count + self.error_count
        return {
            "name": self.name,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "success_rate": self.processed_count / total if total > 0 else 0,
            "total_messages": total
        }


class MarketDataHandler(MessageHandler):
    """Handler for market tick data."""
    
    def __init__(self):
        super().__init__("MarketData")
        self.latest_prices: Dict[str, float] = {}
        self.volume_tracker: Dict[str, int] = {}
    
    def process_message(self, message: MarketDataMessage) -> bool:
        """Process market data message.
        
        Args:
            message: Market data message
            
        Returns:
            True if processed successfully
        """
        if message.message_type != "tick":
            return True  # Skip non-tick messages
        
        try:
            symbol = message.symbol
            data = message.data
            
            # Extract price and volume
            close_price = data.get("close_price", 0)
            volume = data.get("volume", 0)
            
            # Update tracking
            self.latest_prices[symbol] = close_price
            self.volume_tracker[symbol] = self.volume_tracker.get(symbol, 0) + volume
            
            self.logger.debug(f"Processed tick for {symbol}: {close_price:,.0f} VND")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing market data: {e}")
            return False
    
    def get_latest_prices(self) -> Dict[str, float]:
        """Get latest prices for all symbols.
        
        Returns:
            Dictionary of symbol -> price
        """
        return self.latest_prices.copy()
    
    def get_volume_summary(self) -> Dict[str, int]:
        """Get volume summary for all symbols.
        
        Returns:
            Dictionary of symbol -> total volume
        """
        return self.volume_tracker.copy()


class TradingSignalHandler(MessageHandler):
    """Handler for trading signals."""
    
    def __init__(self):
        super().__init__("TradingSignal")
        self.active_signals: Dict[str, Dict[str, Any]] = {}
        self.signal_history: List[Dict[str, Any]] = []
    
    def process_message(self, message: MarketDataMessage) -> bool:
        """Process trading signal message.
        
        Args:
            message: Trading signal message
            
        Returns:
            True if processed successfully
        """
        if message.message_type != "signal":
            return True
        
        try:
            symbol = message.symbol
            data = message.data
            
            # Extract signal information
            signal_info = {
                "symbol": symbol,
                "timestamp": message.timestamp,
                "source": message.source,
                "action": data.get("action", "HOLD"),
                "confidence": data.get("confidence", 0.0),
                "price": data.get("price", 0.0),
                "quantity": data.get("quantity", 0),
                "strategy": data.get("strategy", "unknown")
            }
            
            # Update active signals
            self.active_signals[symbol] = signal_info
            
            # Add to history
            self.signal_history.append(signal_info)
            
            # Keep history size manageable
            if len(self.signal_history) > 1000:
                self.signal_history = self.signal_history[-500:]
            
            self.logger.info(
                f"Processed signal for {symbol}: {signal_info['action']} "
                f"(confidence: {signal_info['confidence']:.2f})"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing trading signal: {e}")
            return False
    
    def get_active_signals(self) -> Dict[str, Dict[str, Any]]:
        """Get currently active signals.
        
        Returns:
            Dictionary of active signals by symbol
        """
        return self.active_signals.copy()
    
    def get_signal_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent signal history.
        
        Args:
            limit: Maximum number of signals to return
            
        Returns:
            List of recent signals
        """
        return self.signal_history[-limit:]


class RiskEventHandler(MessageHandler):
    """Handler for risk management events."""
    
    def __init__(self):
        super().__init__("RiskEvent")
        self.risk_alerts: List[Dict[str, Any]] = []
        self.portfolio_metrics: Dict[str, Any] = {}
    
    def process_message(self, message: MarketDataMessage) -> bool:
        """Process risk event message.
        
        Args:
            message: Risk event message
            
        Returns:
            True if processed successfully
        """
        if message.message_type != "risk":
            return True
        
        try:
            data = message.data
            event_type = data.get("event_type", "unknown")
            
            risk_event = {
                "timestamp": message.timestamp,
                "symbol": message.symbol,
                "event_type": event_type,
                "severity": data.get("severity", "INFO"),
                "details": data.get("details", {}),
                "source": message.source
            }
            
            # Add to alerts
            self.risk_alerts.append(risk_event)
            
            # Keep alerts manageable
            if len(self.risk_alerts) > 500:
                self.risk_alerts = self.risk_alerts[-250:]
            
            # Update portfolio metrics if this is a portfolio-level event
            if message.symbol == "PORTFOLIO":
                self.portfolio_metrics.update(data.get("metrics", {}))
            
            # Log based on severity
            severity = risk_event["severity"]
            log_message = f"Risk event: {event_type} for {message.symbol}"
            
            if severity == "CRITICAL":
                self.logger.critical(log_message)
            elif severity == "HIGH":
                self.logger.error(log_message)
            elif severity == "MEDIUM":
                self.logger.warning(log_message)
            else:
                self.logger.info(log_message)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing risk event: {e}")
            return False
    
    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent risk alerts.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of recent risk alerts
        """
        return self.risk_alerts[-limit:]
    
    def get_portfolio_metrics(self) -> Dict[str, Any]:
        """Get current portfolio metrics.
        
        Returns:
            Portfolio metrics dictionary
        """
        return self.portfolio_metrics.copy()


class VietnameseMarketConsumer:
    """Multi-topic Kafka consumer for Vietnamese market data."""
    
    def __init__(self, config: Dict[str, Any], topics: List[str]):
        """Initialize market data consumer.
        
        Args:
            config: Consumer configuration
            topics: List of topics to consume
        """
        self.config = ConsumerConfig(**config)
        self.topics = topics
        self.logger = StructuredLogger("MarketConsumer")
        self.consumer: Optional[KafkaConsumer] = None
        
        # Consumer state
        self.is_running = False
        self.consumer_thread: Optional[Thread] = None
        self.shutdown_event = Event()
        
        # Message handlers
        self.handlers: Dict[str, MessageHandler] = {}
        self.register_default_handlers()
        
        # Performance metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.start_time = time.time()
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def register_default_handlers(self) -> None:
        """Register default message handlers."""
        self.handlers["market_data"] = MarketDataHandler()
        self.handlers["trading_signals"] = TradingSignalHandler()
        self.handlers["risk_events"] = RiskEventHandler()
    
    def register_handler(self, name: str, handler: MessageHandler) -> None:
        """Register a custom message handler.
        
        Args:
            name: Handler name
            handler: Message handler instance
        """
        self.handlers[name] = handler
        self.logger.info(f"Registered handler: {name}")
    
    def connect(self) -> bool:
        """Connect to Kafka cluster and subscribe to topics.
        
        Returns:
            True if connection successful
        """
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                client_id=self.config.client_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                auto_commit_interval_ms=self.config.auto_commit_interval_ms,
                max_poll_records=self.config.max_poll_records,
                max_poll_interval_ms=self.config.max_poll_interval_ms,
                session_timeout_ms=self.config.session_timeout_ms,
                heartbeat_interval_ms=self.config.heartbeat_interval_ms,
                fetch_min_bytes=self.config.fetch_min_bytes,
                fetch_max_wait_ms=self.config.fetch_max_wait_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None
            )
            
            self.logger.info(f"Connected to Kafka, subscribed to topics: {self.topics}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Kafka cluster."""
        self.is_running = False
        self.shutdown_event.set()
        
        # Wait for consumer thread to finish
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=10)
        
        if self.consumer:
            try:
                self.consumer.close()
                self.consumer = None
                self.logger.info("Disconnected from Kafka cluster")
            except Exception as e:
                self.logger.error(f"Error disconnecting from Kafka: {e}")
    
    def start_consuming(self, blocking: bool = True) -> bool:
        """Start consuming messages.
        
        Args:
            blocking: Whether to block the current thread
            
        Returns:
            True if started successfully
        """
        if not self.consumer:
            self.logger.error("Consumer not connected")
            return False
        
        self.is_running = True
        self.start_time = time.time()
        
        if blocking:
            self._consume_loop()
        else:
            self.consumer_thread = Thread(target=self._consume_loop, daemon=True)
            self.consumer_thread.start()
        
        return True
    
    def _consume_loop(self) -> None:
        """Main consumer loop."""
        self.logger.info("Starting consumer loop")
        
        try:
            while self.is_running and not self.shutdown_event.is_set():
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        topic = topic_partition.topic
                        
                        for message in messages:
                            if not self.is_running:
                                break
                            
                            self._process_message(topic, message)
                    
                    # Commit offsets
                    if self.config.enable_auto_commit:
                        self.consumer.commit()
                
                except KafkaError as e:
                    self.logger.error(f"Kafka error in consumer loop: {e}")
                    time.sleep(1)  # Brief pause before retrying
                
                except Exception as e:
                    self.logger.error(f"Unexpected error in consumer loop: {e}")
                    time.sleep(1)
        
        except Exception as e:
            self.logger.error(f"Fatal error in consumer loop: {e}")
        
        finally:
            self.logger.info("Consumer loop stopped")
    
    def _process_message(self, topic: str, raw_message) -> None:
        """Process a single message.
        
        Args:
            topic: Kafka topic
            raw_message: Raw Kafka message
        """
        try:
            # Parse message
            message_data = raw_message.value
            message = MarketDataMessage(**message_data)
            
            # Route to appropriate handlers
            handlers_to_process = self._get_handlers_for_topic(topic)
            
            success = True
            for handler_name in handlers_to_process:
                handler = self.handlers.get(handler_name)
                if handler:
                    if not handler.handle_message(message):
                        success = False
                        self.logger.warning(f"Handler {handler_name} failed to process message")
            
            if success:
                self.messages_processed += 1
            else:
                self.messages_failed += 1
            
            # Log progress periodically
            if self.messages_processed % 100 == 0:
                self.logger.info(f"Processed {self.messages_processed} messages")
        
        except Exception as e:
            self.logger.error(f"Error processing message from {topic}: {e}")
            self.messages_failed += 1
    
    def _get_handlers_for_topic(self, topic: str) -> List[str]:
        """Get handler names for a given topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            List of handler names
        """
        # Map topics to handlers
        topic_handler_map = {
            "vn-market-ticks": ["market_data"],
            "vn-market-bars": ["market_data"],
            "vn-market-indices": ["market_data"],
            "vn-trading-signals": ["trading_signals"],
            "vn-risk-events": ["risk_events"]
        }
        
        return topic_handler_map.get(topic, [])
    
    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.disconnect()
        sys.exit(0)
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Get consumer performance metrics.
        
        Returns:
            Consumer metrics dictionary
        """
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        metrics = {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "success_rate": self.messages_processed / (self.messages_processed + self.messages_failed) if (self.messages_processed + self.messages_failed) > 0 else 0,
            "messages_per_second": self.messages_processed / elapsed_time if elapsed_time > 0 else 0,
            "elapsed_time": elapsed_time,
            "is_running": self.is_running,
            "topics": self.topics,
            "timestamp": current_time
        }
        
        # Add handler metrics
        handler_metrics = {}
        for name, handler in self.handlers.items():
            handler_metrics[name] = handler.get_stats()
        
        metrics["handlers"] = handler_metrics
        
        return metrics
    
    def get_market_data_handler(self) -> Optional[MarketDataHandler]:
        """Get market data handler.
        
        Returns:
            MarketDataHandler instance or None
        """
        return self.handlers.get("market_data")
    
    def get_signal_handler(self) -> Optional[TradingSignalHandler]:
        """Get trading signal handler.
        
        Returns:
            TradingSignalHandler instance or None
        """
        return self.handlers.get("trading_signals")
    
    def get_risk_handler(self) -> Optional[RiskEventHandler]:
        """Get risk event handler.
        
        Returns:
            RiskEventHandler instance or None
        """
        return self.handlers.get("risk_events")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()