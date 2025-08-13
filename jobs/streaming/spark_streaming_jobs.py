"""PySpark Structured Streaming jobs for Vietnamese market data processing."""

import os
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, avg, max as spark_max, 
    min as spark_min, sum as spark_sum, count, first, last, stddev,
    when, lit, date_format, current_timestamp, expr, split
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, 
    TimestampType, IntegerType
)

from src.utils.logging import StructuredLogger


class StreamingJobConfig:
    """Configuration for Spark streaming jobs."""
    
    def __init__(self):
        self.kafka_bootstrap_servers = "localhost:9092"
        self.checkpoint_location = "./checkpoints"
        self.output_mode = "append"
        self.trigger_interval = "10 seconds"
        self.watermark_delay = "2 minutes"
        
        # Delta Lake configuration
        self.delta_path = "./data/delta"
        self.bronze_path = f"{self.delta_path}/bronze"
        self.silver_path = f"{self.delta_path}/silver"
        self.gold_path = f"{self.delta_path}/gold"
        
        # Kafka topics
        self.topics = {
            "market_ticks": "vn-market-ticks",
            "market_bars": "vn-market-bars",
            "market_indices": "vn-market-indices",
            "trading_signals": "vn-trading-signals",
            "risk_events": "vn-risk-events"
        }


class MarketDataSchema:
    """Schema definitions for market data."""
    
    @staticmethod
    def get_kafka_message_schema() -> StructType:
        """Get schema for Kafka messages."""
        return StructType([
            StructField("source", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("message_type", StringType(), True),
            StructField("sequence_id", LongType(), True),
            StructField("data", StructType([
                StructField("open_price", DoubleType(), True),
                StructField("high_price", DoubleType(), True),
                StructField("low_price", DoubleType(), True),
                StructField("close_price", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("value", DoubleType(), True)
            ]), True)
        ])
    
    @staticmethod
    def get_ohlcv_schema() -> StructType:
        """Get schema for OHLCV data."""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("open_price", DoubleType(), False),
            StructField("high_price", DoubleType(), False),
            StructField("low_price", DoubleType(), False),
            StructField("close_price", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("value", DoubleType(), True),
            StructField("source", StringType(), True)
        ])


class VietnameseMarketStreamingJob:
    """Main streaming job for Vietnamese market data processing."""
    
    def __init__(self, config: Optional[StreamingJobConfig] = None):
        """Initialize streaming job.
        
        Args:
            config: Streaming job configuration
        """
        self.config = config or StreamingJobConfig()
        self.logger = StructuredLogger("StreamingJob")
        self.spark: Optional[SparkSession] = None
        self.running_queries = []
        
    def create_spark_session(self) -> SparkSession:
        """Create Spark session with streaming configuration.
        
        Returns:
            Configured SparkSession
        """
        spark = SparkSession.builder \
            .appName("VietnameseMarketStreaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", self.config.checkpoint_location) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        self.spark = spark
        self.logger.info("Created Spark session for streaming")
        return spark
    
    def read_kafka_stream(self, topic: str) -> DataFrame:
        """Read streaming data from Kafka topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Streaming DataFrame
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_market_data_messages(self, kafka_df: DataFrame) -> DataFrame:
        """Parse Kafka messages containing market data.
        
        Args:
            kafka_df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with market data
        """
        schema = MarketDataSchema.get_kafka_message_schema()
        
        return kafka_df \
            .select(
                from_json(col("value").cast("string"), schema).alias("message"),
                col("timestamp").alias("kafka_timestamp"),
                col("offset"),
                col("partition")
            ) \
            .select(
                col("message.source"),
                col("message.symbol"),
                col("message.timestamp"),
                col("message.message_type"),
                col("message.sequence_id"),
                col("message.data.open_price"),
                col("message.data.high_price"),
                col("message.data.low_price"),
                col("message.data.close_price"),
                col("message.data.volume"),
                col("message.data.value"),
                col("kafka_timestamp"),
                col("offset"),
                col("partition")
            ) \
            .withWatermark("timestamp", self.config.watermark_delay)
    
    def create_tick_to_bar_aggregation(self, tick_df: DataFrame, window_duration: str) -> DataFrame:
        """Aggregate tick data into OHLCV bars.
        
        Args:
            tick_df: Streaming DataFrame with tick data
            window_duration: Window duration (e.g., "1 minute", "5 minutes")
            
        Returns:
            DataFrame with OHLCV bars
        """
        return tick_df \
            .filter(col("message_type") == "tick") \
            .groupBy(
                col("symbol"),
                window(col("timestamp"), window_duration).alias("time_window")
            ) \
            .agg(
                first("close_price").alias("open_price"),
                spark_max("close_price").alias("high_price"),
                spark_min("close_price").alias("low_price"),
                last("close_price").alias("close_price"),
                spark_sum("volume").alias("volume"),
                spark_sum("value").alias("value"),
                count("*").alias("tick_count"),
                first("source").alias("source")
            ) \
            .select(
                col("symbol"),
                col("time_window.start").alias("timestamp"),
                col("open_price"),
                col("high_price"),
                col("low_price"),
                col("close_price"),
                col("volume"),
                col("value"),
                col("tick_count"),
                col("source"),
                lit(window_duration).alias("timeframe"),
                current_timestamp().alias("processing_time")
            )
    
    def add_technical_indicators(self, ohlcv_df: DataFrame) -> DataFrame:
        """Add basic technical indicators to OHLCV data.
        
        Args:
            ohlcv_df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with technical indicators
        """
        # For streaming, we'll add simple indicators that don't require complex windowing
        return ohlcv_df \
            .withColumn("price_change", col("close_price") - col("open_price")) \
            .withColumn("price_change_pct", 
                       when(col("open_price") > 0, 
                            (col("close_price") - col("open_price")) / col("open_price") * 100)
                       .otherwise(0)) \
            .withColumn("high_low_spread", col("high_price") - col("low_price")) \
            .withColumn("high_low_spread_pct",
                       when(col("low_price") > 0,
                            (col("high_price") - col("low_price")) / col("low_price") * 100)
                       .otherwise(0)) \
            .withColumn("typical_price", (col("high_price") + col("low_price") + col("close_price")) / 3) \
            .withColumn("vwap", 
                       when(col("volume") > 0, col("value") / col("volume"))
                       .otherwise(col("close_price")))
    
    def write_to_delta_lake(self, df: DataFrame, path: str, mode: str = "append") -> Any:
        """Write streaming DataFrame to Delta Lake.
        
        Args:
            df: Streaming DataFrame
            path: Delta Lake path
            mode: Write mode
            
        Returns:
            Streaming query
        """
        return df.writeStream \
            .format("delta") \
            .outputMode(mode) \
            .option("checkpointLocation", f"{self.config.checkpoint_location}/{path.split('/')[-1]}") \
            .option("path", path) \
            .trigger(processingTime=self.config.trigger_interval) \
            .start()
    
    def write_to_console(self, df: DataFrame, query_name: str) -> Any:
        """Write streaming DataFrame to console for debugging.
        
        Args:
            df: Streaming DataFrame
            query_name: Name for the query
            
        Returns:
            Streaming query
        """
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .queryName(query_name) \
            .trigger(processingTime=self.config.trigger_interval) \
            .start()
    
    def start_market_data_processing(self) -> None:
        """Start market data processing pipeline."""
        self.logger.info("Starting market data processing pipeline")
        
        if not self.spark:
            self.create_spark_session()
        
        # Read from market ticks topic
        kafka_df = self.read_kafka_stream(self.config.topics["market_ticks"])
        
        # Parse messages
        parsed_df = self.parse_market_data_messages(kafka_df)
        
        # Filter for tick data and clean
        tick_df = parsed_df \
            .filter(col("message_type") == "tick") \
            .filter(col("symbol").isNotNull()) \
            .filter(col("close_price") > 0) \
            .filter(col("volume") >= 0)
        
        # Write raw ticks to bronze layer
        bronze_query = self.write_to_delta_lake(
            tick_df.select(
                col("symbol"),
                col("timestamp"),
                col("open_price"),
                col("high_price"),
                col("low_price"),
                col("close_price"),
                col("volume"),
                col("value"),
                col("source"),
                current_timestamp().alias("ingestion_time")
            ),
            f"{self.config.bronze_path}/market_ticks"
        )
        self.running_queries.append(bronze_query)
        
        # Create 1-minute bars
        bars_1m = self.create_tick_to_bar_aggregation(tick_df, "1 minute")
        bars_1m_with_indicators = self.add_technical_indicators(bars_1m)
        
        # Write 1-minute bars to silver layer
        silver_1m_query = self.write_to_delta_lake(
            bars_1m_with_indicators,
            f"{self.config.silver_path}/bars_1m"
        )
        self.running_queries.append(silver_1m_query)
        
        # Create 5-minute bars
        bars_5m = self.create_tick_to_bar_aggregation(tick_df, "5 minutes")
        bars_5m_with_indicators = self.add_technical_indicators(bars_5m)
        
        # Write 5-minute bars to silver layer
        silver_5m_query = self.write_to_delta_lake(
            bars_5m_with_indicators,
            f"{self.config.silver_path}/bars_5m"
        )
        self.running_queries.append(silver_5m_query)
        
        # Create daily aggregates for gold layer
        daily_agg = tick_df \
            .groupBy(
                col("symbol"),
                date_format(col("timestamp"), "yyyy-MM-dd").alias("date")
            ) \
            .agg(
                first("close_price").alias("open_price"),
                spark_max("close_price").alias("high_price"),
                spark_min("close_price").alias("low_price"),
                last("close_price").alias("close_price"),
                spark_sum("volume").alias("volume"),
                spark_sum("value").alias("value"),
                count("*").alias("tick_count"),
                first("source").alias("primary_source")
            ) \
            .withColumn("trading_date", col("date").cast("date")) \
            .withColumn("processing_time", current_timestamp())
        
        # Write daily aggregates to gold layer
        gold_daily_query = self.write_to_delta_lake(
            daily_agg,
            f"{self.config.gold_path}/daily_summary",
            mode="complete"
        )
        self.running_queries.append(gold_daily_query)
        
        self.logger.info(f"Started {len(self.running_queries)} streaming queries")
    
    def start_signal_processing(self) -> None:
        """Start trading signal processing pipeline."""
        self.logger.info("Starting signal processing pipeline")
        
        if not self.spark:
            self.create_spark_session()
        
        # Read from trading signals topic
        signals_kafka_df = self.read_kafka_stream(self.config.topics["trading_signals"])
        
        # Parse signal messages
        signals_df = signals_kafka_df \
            .select(
                from_json(col("value").cast("string"), 
                         MarketDataSchema.get_kafka_message_schema()).alias("message")
            ) \
            .select(
                col("message.symbol"),
                col("message.timestamp"),
                col("message.source").alias("strategy"),
                col("message.data").alias("signal_data")
            ) \
            .withWatermark("timestamp", self.config.watermark_delay)
        
        # Expand signal data
        expanded_signals = signals_df \
            .select(
                col("symbol"),
                col("timestamp"),
                col("strategy"),
                col("signal_data.action").alias("action"),
                col("signal_data.confidence").alias("confidence"),
                col("signal_data.price").alias("signal_price"),
                col("signal_data.quantity").alias("quantity"),
                current_timestamp().alias("processing_time")
            )
        
        # Write signals to gold layer
        signals_query = self.write_to_delta_lake(
            expanded_signals,
            f"{self.config.gold_path}/trading_signals"
        )
        self.running_queries.append(signals_query)
        
        # Create signal summary by strategy
        signal_summary = expanded_signals \
            .groupBy(
                col("strategy"),
                window(col("timestamp"), "1 hour").alias("time_window")
            ) \
            .agg(
                count("*").alias("signal_count"),
                avg("confidence").alias("avg_confidence"),
                spark_sum(when(col("action") == "BUY", 1).otherwise(0)).alias("buy_signals"),
                spark_sum(when(col("action") == "SELL", 1).otherwise(0)).alias("sell_signals"),
                spark_sum(when(col("action") == "HOLD", 1).otherwise(0)).alias("hold_signals")
            ) \
            .select(
                col("strategy"),
                col("time_window.start").alias("hour"),
                col("signal_count"),
                col("avg_confidence"),
                col("buy_signals"),
                col("sell_signals"),
                col("hold_signals"),
                current_timestamp().alias("processing_time")
            )
        
        # Write signal summary
        signal_summary_query = self.write_to_delta_lake(
            signal_summary,
            f"{self.config.gold_path}/signal_summary"
        )
        self.running_queries.append(signal_summary_query)
    
    def start_risk_monitoring(self) -> None:
        """Start risk event monitoring pipeline."""
        self.logger.info("Starting risk event monitoring pipeline")
        
        if not self.spark:
            self.create_spark_session()
        
        # Read from risk events topic
        risk_kafka_df = self.read_kafka_stream(self.config.topics["risk_events"])
        
        # Parse risk messages
        risk_df = risk_kafka_df \
            .select(
                from_json(col("value").cast("string"),
                         MarketDataSchema.get_kafka_message_schema()).alias("message")
            ) \
            .select(
                col("message.symbol"),
                col("message.timestamp"),
                col("message.source"),
                col("message.data").alias("risk_data")
            ) \
            .withWatermark("timestamp", self.config.watermark_delay)
        
        # Expand risk data
        expanded_risk = risk_df \
            .select(
                col("symbol"),
                col("timestamp"),
                col("source"),
                col("risk_data.event_type").alias("event_type"),
                col("risk_data.severity").alias("severity"),
                col("risk_data.details").alias("details"),
                current_timestamp().alias("processing_time")
            )
        
        # Write risk events to gold layer
        risk_query = self.write_to_delta_lake(
            expanded_risk,
            f"{self.config.gold_path}/risk_events"
        )
        self.running_queries.append(risk_query)
        
        # Create risk summary by severity
        risk_summary = expanded_risk \
            .groupBy(
                col("severity"),
                window(col("timestamp"), "15 minutes").alias("time_window")
            ) \
            .agg(
                count("*").alias("event_count"),
                expr("collect_set(event_type)").alias("event_types"),
                expr("collect_set(symbol)").alias("affected_symbols")
            ) \
            .select(
                col("severity"),
                col("time_window.start").alias("window_start"),
                col("event_count"),
                col("event_types"),
                col("affected_symbols"),
                current_timestamp().alias("processing_time")
            )
        
        # Write risk summary
        risk_summary_query = self.write_to_delta_lake(
            risk_summary,
            f"{self.config.gold_path}/risk_summary"
        )
        self.running_queries.append(risk_summary_query)
    
    def start_all_pipelines(self) -> None:
        """Start all streaming pipelines."""
        self.logger.info("Starting all streaming pipelines")
        
        try:
            self.start_market_data_processing()
            self.start_signal_processing()
            self.start_risk_monitoring()
            
            self.logger.info(f"All pipelines started. Total queries: {len(self.running_queries)}")
            
        except Exception as e:
            self.logger.error(f"Error starting pipelines: {e}")
            raise
    
    def stop_all_queries(self) -> None:
        """Stop all running streaming queries."""
        self.logger.info("Stopping all streaming queries")
        
        for query in self.running_queries:
            try:
                query.stop()
            except Exception as e:
                self.logger.error(f"Error stopping query: {e}")
        
        self.running_queries.clear()
        
        if self.spark:
            self.spark.stop()
            self.spark = None
        
        self.logger.info("All streaming queries stopped")
    
    def wait_for_termination(self) -> None:
        """Wait for all queries to terminate."""
        if self.running_queries:
            self.logger.info("Waiting for streaming queries to terminate...")
            for query in self.running_queries:
                query.awaitTermination()
    
    def get_query_status(self) -> Dict[str, Any]:
        """Get status of all running queries.
        
        Returns:
            Dictionary with query status information
        """
        status = {
            "total_queries": len(self.running_queries),
            "queries": []
        }
        
        for i, query in enumerate(self.running_queries):
            try:
                query_info = {
                    "id": query.id,
                    "name": query.name,
                    "is_active": query.isActive,
                    "status": "RUNNING" if query.isActive else "STOPPED"
                }
                
                # Get progress if available
                try:
                    progress = query.lastProgress
                    if progress:
                        query_info["last_progress"] = {
                            "batch_id": progress.get("batchId", -1),
                            "input_rows": progress.get("inputRowsPerSecond", 0),
                            "processed_rows": progress.get("batchDuration", 0)
                        }
                except:
                    pass
                
                status["queries"].append(query_info)
                
            except Exception as e:
                status["queries"].append({
                    "id": f"query_{i}",
                    "status": "ERROR",
                    "error": str(e)
                })
        
        return status