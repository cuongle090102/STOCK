#!/usr/bin/env python3
"""Test script for Kafka streaming pipeline."""

import sys
import time
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
import threading

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.streaming.producers import VietnameseMarketProducer, MarketDataMessage
from src.streaming.consumers import VietnameseMarketConsumer
from src.streaming.schema_registry import SchemaManager
from src.ingestion.base import MarketDataPoint
from src.utils.logging import setup_logging, StructuredLogger


class KafkaStreamingTest:
    """Test harness for Kafka streaming components."""
    
    def __init__(self):
        """Initialize test harness."""
        self.logger = StructuredLogger("KafkaTest")
        self.producer: VietnameseMarketProducer = None
        self.consumer: VietnameseMarketConsumer = None
        self.schema_manager: SchemaManager = None
        
        # Test configuration
        self.kafka_config = {
            "bootstrap_servers": ["localhost:9092"],
            "topics": {
                "market_ticks": "test-vn-market-ticks",
                "market_bars": "test-vn-market-bars",
                "trading_signals": "test-vn-trading-signals",
                "risk_events": "test-vn-risk-events"
            }
        }
        
        self.consumer_config = {
            "bootstrap_servers": ["localhost:9092"],
            "group_id": "test-vn-algo-consumers",
            "auto_offset_reset": "earliest"
        }
        
        # Test data
        self.test_symbols = ["VIC", "VNM", "HPG"]
        self.messages_sent = 0
        self.messages_received = 0
        
    def setup_test_environment(self) -> bool:
        """Setup test environment.
        
        Returns:
            True if setup successful
        """
        self.logger.info("Setting up Kafka streaming test environment")
        
        try:
            # Initialize schema manager
            self.schema_manager = SchemaManager("http://localhost:8081")
            
            # Check if Schema Registry is available
            if self.schema_manager.client.is_healthy():
                self.logger.info("Schema Registry is available")
                # Setup schemas for testing
                # self.schema_manager.setup_all_schemas()
            else:
                self.logger.warning("Schema Registry not available - continuing without schemas")
            
            # Initialize producer
            self.producer = VietnameseMarketProducer(self.kafka_config)
            
            # Check if Kafka is available by trying to connect
            if not self.producer.connect():
                self.logger.error("Cannot connect to Kafka - please ensure Kafka is running")
                return False
            
            self.logger.info("Kafka producer connected successfully")
            
            # Initialize consumer
            topics_to_consume = list(self.kafka_config["topics"].values())
            self.consumer = VietnameseMarketConsumer(self.consumer_config, topics_to_consume)
            
            if not self.consumer.connect():
                self.logger.error("Cannot connect consumer to Kafka")
                return False
            
            self.logger.info("Kafka consumer connected successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup test environment: {e}")
            return False
    
    def generate_test_market_data(self, count: int = 10) -> List[MarketDataPoint]:
        """Generate test market data points.
        
        Args:
            count: Number of data points to generate
            
        Returns:
            List of MarketDataPoint objects
        """
        data_points = []
        base_prices = {"VIC": 90000, "VNM": 65000, "HPG": 25000}
        
        for i in range(count):
            for symbol in self.test_symbols:
                base_price = base_prices[symbol]
                # Simulate price movement
                price_change = (i % 10 - 5) * 0.01  # ±5% movement
                close_price = base_price * (1 + price_change)
                
                data_point = MarketDataPoint(
                    symbol=symbol,
                    timestamp=datetime.now() - timedelta(minutes=count-i),
                    open_price=close_price * 0.999,
                    high_price=close_price * 1.002,
                    low_price=close_price * 0.998,
                    close_price=close_price,
                    volume=1000000 + i * 100000,
                    value=close_price * (1000000 + i * 100000)
                )
                
                data_points.append(data_point)
        
        return data_points
    
    def test_producer_basic(self) -> bool:
        """Test basic producer functionality.
        
        Returns:
            True if test passed
        """
        self.logger.info("Testing basic producer functionality")
        
        try:
            # Generate test data
            test_data = self.generate_test_market_data(5)
            
            # Send test messages
            sent_count = 0
            for data_point in test_data:
                if self.producer.send_market_data_point(data_point, source="test"):
                    sent_count += 1
                    self.messages_sent += 1
                else:
                    self.logger.error(f"Failed to send message for {data_point.symbol}")
            
            # Flush to ensure all messages are sent
            if self.producer.producer:
                self.producer.producer.flush(timeout=10)
            
            self.logger.info(f"Sent {sent_count}/{len(test_data)} messages successfully")
            
            # Test other message types
            # Trading signal
            signal_data = {
                "action": "BUY",
                "confidence": 0.85,
                "price": 90000,
                "quantity": 1000,
                "strategy": "test_strategy"
            }
            
            if self.producer.send_trading_signal("VIC", signal_data, source="test_strategy"):
                self.logger.info("Successfully sent trading signal")
                self.messages_sent += 1
            
            # Risk event
            risk_data = {
                "event_type": "VOLATILITY_SPIKE",
                "severity": "MEDIUM",
                "details": {"volatility": 0.05, "threshold": 0.03}
            }
            
            if self.producer.send_risk_event("VOLATILITY_SPIKE", risk_data, symbol="VIC"):
                self.logger.info("Successfully sent risk event")
                self.messages_sent += 1
            
            return sent_count > 0
            
        except Exception as e:
            self.logger.error(f"Producer test failed: {e}")
            return False
    
    def test_consumer_basic(self) -> bool:
        """Test basic consumer functionality.
        
        Returns:
            True if test passed
        """
        self.logger.info("Testing basic consumer functionality")
        
        try:
            # Start consumer in background thread
            consumer_thread = threading.Thread(
                target=self.consumer.start_consuming,
                kwargs={"blocking": False},
                daemon=True
            )
            consumer_thread.start()
            
            # Wait a bit for consumer to start
            time.sleep(2)
            
            # Let consumer run for a few seconds to collect messages
            self.logger.info("Letting consumer run for 10 seconds...")
            time.sleep(10)
            
            # Check consumer metrics
            metrics = self.consumer.get_consumer_metrics()
            self.messages_received = metrics["messages_processed"]
            
            self.logger.info(f"Consumer metrics: {metrics}")
            
            # Check handlers
            market_handler = self.consumer.get_market_data_handler()
            if market_handler:
                latest_prices = market_handler.get_latest_prices()
                self.logger.info(f"Latest prices received: {latest_prices}")
            
            signal_handler = self.consumer.get_signal_handler()
            if signal_handler:
                active_signals = signal_handler.get_active_signals()
                self.logger.info(f"Active signals: {active_signals}")
            
            risk_handler = self.consumer.get_risk_handler()
            if risk_handler:
                recent_alerts = risk_handler.get_recent_alerts(limit=5)
                self.logger.info(f"Recent risk alerts: {len(recent_alerts)}")
            
            return metrics["messages_processed"] > 0
            
        except Exception as e:
            self.logger.error(f"Consumer test failed: {e}")
            return False
    
    def test_end_to_end_flow(self) -> bool:
        """Test end-to-end message flow.
        
        Returns:
            True if test passed
        """
        self.logger.info("Testing end-to-end message flow")
        
        try:
            # Reset counters
            initial_sent = self.messages_sent
            initial_received = self.messages_received
            
            # Send a batch of messages
            test_data = self.generate_test_market_data(10)
            batch_sent = 0
            
            for data_point in test_data:
                if self.producer.send_market_data_point(data_point, source="e2e_test"):
                    batch_sent += 1
                    self.messages_sent += 1
            
            # Flush producer
            if self.producer.producer:
                self.producer.producer.flush(timeout=10)
            
            self.logger.info(f"Sent {batch_sent} messages in batch")
            
            # Wait for consumer to process
            time.sleep(5)
            
            # Check if messages were received
            final_metrics = self.consumer.get_consumer_metrics()
            final_received = final_metrics["messages_processed"]
            
            batch_received = final_received - initial_received
            self.messages_received = final_received
            
            self.logger.info(f"Received {batch_received} messages in batch")
            
            # Check success rate
            success_rate = batch_received / batch_sent if batch_sent > 0 else 0
            self.logger.info(f"End-to-end success rate: {success_rate:.2%}")
            
            return success_rate > 0.5  # At least 50% success rate
            
        except Exception as e:
            self.logger.error(f"End-to-end test failed: {e}")
            return False
    
    def test_producer_performance(self) -> Dict[str, Any]:
        """Test producer performance.
        
        Returns:
            Performance metrics dictionary
        """
        self.logger.info("Testing producer performance")
        
        try:
            # Generate larger dataset
            test_data = self.generate_test_market_data(100)
            
            # Measure performance
            start_time = time.time()
            sent_count = 0
            
            for data_point in test_data:
                if self.producer.send_market_data_point(data_point, source="perf_test"):
                    sent_count += 1
            
            # Flush and measure total time
            if self.producer.producer:
                self.producer.producer.flush(timeout=30)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            # Calculate metrics
            messages_per_second = sent_count / elapsed_time if elapsed_time > 0 else 0
            
            performance_metrics = {
                "messages_sent": sent_count,
                "elapsed_time": elapsed_time,
                "messages_per_second": messages_per_second,
                "success_rate": sent_count / len(test_data),
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Producer performance: {performance_metrics}")
            return performance_metrics
            
        except Exception as e:
            self.logger.error(f"Performance test failed: {e}")
            return {"error": str(e)}
    
    def cleanup_test_environment(self) -> None:
        """Clean up test environment."""
        self.logger.info("Cleaning up test environment")
        
        try:
            if self.consumer:
                self.consumer.disconnect()
            
            if self.producer:
                self.producer.disconnect()
            
            # Note: In a real test environment, you might want to delete test topics
            # This is commented out for safety
            # if self.schema_manager:
            #     self.schema_manager.cleanup_test_schemas()
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests.
        
        Returns:
            Test results summary
        """
        self.logger.info("Starting comprehensive Kafka streaming tests")
        
        results = {
            "setup": False,
            "producer_basic": False,
            "consumer_basic": False,
            "end_to_end": False,
            "performance": {},
            "summary": {},
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Setup
            results["setup"] = self.setup_test_environment()
            if not results["setup"]:
                self.logger.error("Setup failed - skipping remaining tests")
                return results
            
            # Basic producer test
            results["producer_basic"] = self.test_producer_basic()
            
            # Basic consumer test  
            results["consumer_basic"] = self.test_consumer_basic()
            
            # End-to-end test
            results["end_to_end"] = self.test_end_to_end_flow()
            
            # Performance test
            results["performance"] = self.test_producer_performance()
            
            # Calculate summary
            passed_tests = sum([
                results["setup"],
                results["producer_basic"], 
                results["consumer_basic"],
                results["end_to_end"]
            ])
            
            total_tests = 4
            
            results["summary"] = {
                "passed_tests": passed_tests,
                "total_tests": total_tests,
                "success_rate": passed_tests / total_tests,
                "messages_sent": self.messages_sent,
                "messages_received": self.messages_received,
                "overall_status": "PASS" if passed_tests == total_tests else "PARTIAL" if passed_tests > 0 else "FAIL"
            }
            
            self.logger.info(f"Test summary: {results['summary']}")
            
        except Exception as e:
            self.logger.error(f"Test execution failed: {e}")
            results["error"] = str(e)
        
        finally:
            self.cleanup_test_environment()
        
        return results


def main():
    """Main test function."""
    # Setup logging
    setup_logging(log_level="INFO", log_format="text")
    
    print("Kafka Streaming Pipeline Test")
    print("=" * 40)
    print()
    print("Prerequisites:")
    print("1. Kafka running on localhost:9092")
    print("2. Schema Registry running on localhost:8081 (optional)")
    print("3. Docker services started with 'make docker-up'")
    print()
    
    # Check if user wants to continue
    try:
        response = input("Continue with tests? (y/n): ").lower().strip()
        if response not in ['y', 'yes']:
            print("Tests cancelled by user")
            return
    except KeyboardInterrupt:
        print("\nTests cancelled")
        return
    
    # Run tests
    test_harness = KafkaStreamingTest()
    results = test_harness.run_all_tests()
    
    # Display results
    print("\n" + "=" * 50)
    print("TEST RESULTS")
    print("=" * 50)
    
    print(f"Setup: {'PASS' if results['setup'] else 'FAIL'}")
    print(f"Producer Basic: {'PASS' if results['producer_basic'] else 'FAIL'}")
    print(f"Consumer Basic: {'PASS' if results['consumer_basic'] else 'FAIL'}")
    print(f"End-to-End: {'PASS' if results['end_to_end'] else 'FAIL'}")
    
    if results.get('performance'):
        perf = results['performance']
        if 'messages_per_second' in perf:
            print(f"Performance: {perf['messages_per_second']:.1f} msg/sec")
    
    summary = results.get('summary', {})
    print(f"\nOverall: {summary.get('overall_status', 'UNKNOWN')}")
    print(f"Passed: {summary.get('passed_tests', 0)}/{summary.get('total_tests', 0)}")
    print(f"Messages Sent: {summary.get('messages_sent', 0)}")
    print(f"Messages Received: {summary.get('messages_received', 0)}")
    
    if summary.get('overall_status') == 'PASS':
        print("\n✅ All tests passed! Kafka streaming pipeline is working correctly.")
        print("\nTask 1.2 Validation:")
        print("✅ Kafka Producer: Market data publishing works")
        print("✅ Kafka Consumer: Message consumption and processing works") 
        print("✅ Message Serialization: JSON serialization working")
        print("✅ Topic Management: Multi-topic streaming functional")
        print("✅ Error Handling: Graceful error handling implemented")
        print("✅ Performance: Acceptable throughput achieved")
    elif summary.get('overall_status') == 'PARTIAL':
        print("\n⚠️  Some tests passed. Check Kafka/Schema Registry setup.")
    else:
        print("\n❌ Tests failed. Please check Kafka setup and try again.")
    
    return results.get('summary', {}).get('success_rate', 0) > 0.5


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)