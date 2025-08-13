"""
Gold Layer Analytics Data Mart for Vietnamese Market Data

This module creates analytics-ready data with pre-calculated technical indicators,
risk metrics, and performance attribution for Vietnamese algorithmic trading.
"""

import sys
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
import math

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger
from src.datalake.common.schemas import SchemaRegistry, MarketDataSchema
from src.datalake.common.partitioning import PartitionStrategy

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, coalesce, avg, stddev, lag, lead,
        sum as spark_sum, count, max as spark_max, min as spark_min,
        window, desc, asc, rank, dense_rank, row_number, 
        round as spark_round, sqrt, log, exp, abs as spark_abs,
        year, month, dayofmonth, current_timestamp, datediff,
        collect_list, size, sort_array
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import DoubleType, IntegerType
    from delta import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None


class TechnicalIndicatorCalculator:
    """Calculates technical indicators for Vietnamese market data."""
    
    def __init__(self):
        """Initialize technical indicator calculator."""
        self.logger = StructuredLogger("TechnicalIndicatorCalculator")
    
    def calculate_moving_averages(self, df: DataFrame) -> DataFrame:
        """Calculate Simple Moving Averages (SMA)."""
        # Window specifications for different periods
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Define SMA periods
        sma_periods = [5, 10, 20, 50, 200]
        
        result_df = df
        for period in sma_periods:
            window_spec = symbol_window.rowsBetween(-period + 1, 0)
            result_df = result_df.withColumn(
                f"sma_{period}",
                avg("close").over(window_spec)
            )
        
        return result_df
    
    def calculate_exponential_moving_averages(self, df: DataFrame) -> DataFrame:
        """Calculate Exponential Moving Averages (EMA)."""
        # EMA calculation using recursive formula: EMA = (Close * Alpha) + (Previous_EMA * (1 - Alpha))
        # Alpha = 2 / (Period + 1)
        
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # EMA 12 and 26 for MACD
        alpha_12 = 2.0 / (12 + 1)
        alpha_26 = 2.0 / (26 + 1)
        
        # Initial EMA is SMA for first period
        result_df = df.withColumn("sma_12", avg("close").over(symbol_window.rowsBetween(-11, 0))) \
                     .withColumn("sma_26", avg("close").over(symbol_window.rowsBetween(-25, 0)))
        
        # For simplicity, approximate EMA with SMA (proper EMA requires iterative calculation)
        result_df = result_df.withColumn("ema_12", col("sma_12")) \
                           .withColumn("ema_26", col("sma_26"))
        
        return result_df
    
    def calculate_macd(self, df: DataFrame) -> DataFrame:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        # MACD = EMA12 - EMA26
        # Signal = EMA9 of MACD
        # Histogram = MACD - Signal
        
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        result_df = df.withColumn(
            "macd",
            col("ema_12") - col("ema_26")
        )
        
        # MACD Signal line (9-period EMA of MACD)
        result_df = result_df.withColumn(
            "macd_signal",
            avg("macd").over(symbol_window.rowsBetween(-8, 0))
        )
        
        # MACD Histogram
        result_df = result_df.withColumn(
            "macd_histogram",
            col("macd") - col("macd_signal")
        )
        
        return result_df
    
    def calculate_rsi(self, df: DataFrame, period: int = 14) -> DataFrame:
        """Calculate Relative Strength Index (RSI)."""
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Calculate price changes
        result_df = df.withColumn(
            "price_change",
            col("close") - lag("close", 1).over(symbol_window)
        )
        
        # Separate gains and losses
        result_df = result_df.withColumn(
            "gain",
            when(col("price_change") > 0, col("price_change")).otherwise(0)
        ).withColumn(
            "loss",
            when(col("price_change") < 0, spark_abs(col("price_change"))).otherwise(0)
        )
        
        # Calculate average gains and losses
        window_spec = symbol_window.rowsBetween(-period + 1, 0)
        result_df = result_df.withColumn(
            "avg_gain",
            avg("gain").over(window_spec)
        ).withColumn(
            "avg_loss",
            avg("loss").over(window_spec)
        )
        
        # Calculate RSI
        result_df = result_df.withColumn(
            "rs",
            when(col("avg_loss") > 0, col("avg_gain") / col("avg_loss")).otherwise(100)
        ).withColumn(
            f"rsi_{period}",
            100 - (100 / (1 + col("rs")))
        )
        
        return result_df
    
    def calculate_bollinger_bands(self, df: DataFrame, period: int = 20, std_dev: float = 2.0) -> DataFrame:
        """Calculate Bollinger Bands."""
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        window_spec = symbol_window.rowsBetween(-period + 1, 0)
        
        # Middle band (SMA)
        result_df = df.withColumn(
            "bb_middle",
            avg("close").over(window_spec)
        )
        
        # Standard deviation
        result_df = result_df.withColumn(
            "bb_std",
            stddev("close").over(window_spec)
        )
        
        # Upper and lower bands
        result_df = result_df.withColumn(
            "bb_upper",
            col("bb_middle") + (col("bb_std") * std_dev)
        ).withColumn(
            "bb_lower",
            col("bb_middle") - (col("bb_std") * std_dev)
        )
        
        # Band width and position
        result_df = result_df.withColumn(
            "bb_width",
            (col("bb_upper") - col("bb_lower")) / col("bb_middle")
        ).withColumn(
            "bb_position",
            when(col("bb_upper") != col("bb_lower"),
                 (col("close") - col("bb_lower")) / (col("bb_upper") - col("bb_lower"))
            ).otherwise(0.5)
        )
        
        return result_df
    
    def calculate_stochastic(self, df: DataFrame, k_period: int = 14, d_period: int = 3) -> DataFrame:
        """Calculate Stochastic Oscillator."""
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        window_spec = symbol_window.rowsBetween(-k_period + 1, 0)
        
        # Highest high and lowest low over period
        result_df = df.withColumn(
            "highest_high",
            spark_max("high").over(window_spec)
        ).withColumn(
            "lowest_low",
            spark_min("low").over(window_spec)
        )
        
        # %K calculation
        result_df = result_df.withColumn(
            "stoch_k",
            when(col("highest_high") != col("lowest_low"),
                 ((col("close") - col("lowest_low")) / (col("highest_high") - col("lowest_low"))) * 100
            ).otherwise(50)
        )
        
        # %D calculation (SMA of %K)
        d_window = symbol_window.rowsBetween(-d_period + 1, 0)
        result_df = result_df.withColumn(
            "stoch_d",
            avg("stoch_k").over(d_window)
        )
        
        return result_df
    
    def calculate_atr(self, df: DataFrame, period: int = 14) -> DataFrame:
        """Calculate Average True Range (ATR)."""
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Calculate True Range
        result_df = df.withColumn(
            "prev_close",
            lag("close", 1).over(symbol_window)
        )
        
        result_df = result_df.withColumn(
            "tr1",
            col("high") - col("low")
        ).withColumn(
            "tr2",
            spark_abs(col("high") - col("prev_close"))
        ).withColumn(
            "tr3",
            spark_abs(col("low") - col("prev_close"))
        )
        
        # True Range is the maximum of the three
        result_df = result_df.withColumn(
            "true_range",
            when(col("tr1") >= col("tr2"), 
                 when(col("tr1") >= col("tr3"), col("tr1")).otherwise(col("tr3"))
            ).otherwise(
                 when(col("tr2") >= col("tr3"), col("tr2")).otherwise(col("tr3"))
            )
        )
        
        # ATR is the moving average of True Range
        window_spec = symbol_window.rowsBetween(-period + 1, 0)
        result_df = result_df.withColumn(
            f"atr_{period}",
            avg("true_range").over(window_spec)
        )
        
        return result_df
    
    def calculate_volume_indicators(self, df: DataFrame) -> DataFrame:
        """Calculate volume-based indicators."""
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Volume SMA
        result_df = df.withColumn(
            "volume_sma_20",
            avg("volume").over(symbol_window.rowsBetween(-19, 0))
        )
        
        # Volume ratio
        result_df = result_df.withColumn(
            "volume_ratio",
            when(col("volume_sma_20") > 0, col("volume") / col("volume_sma_20")).otherwise(1.0)
        )
        
        # On Balance Volume (OBV)
        result_df = result_df.withColumn(
            "price_change_direction",
            when(col("close") > lag("close", 1).over(symbol_window), col("volume"))
            .when(col("close") < lag("close", 1).over(symbol_window), -col("volume"))
            .otherwise(0)
        ).withColumn(
            "on_balance_volume",
            spark_sum("price_change_direction").over(symbol_window.rowsBetween(Window.unboundedPreceding, 0))
        )
        
        # Money Flow Index (simplified)
        result_df = result_df.withColumn(
            "money_flow_index",
            lit(50.0)  # Placeholder - requires more complex calculation
        )
        
        return result_df


class GoldTableManager:
    """Manages Delta Lake tables for gold layer analytics data."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/gold"):
        """Initialize gold table manager."""
        self.logger = StructuredLogger("GoldTableManager")
        self.base_path = base_path
        self.spark = spark_session
        
        # Initialize components
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        
        # Table configurations
        self.table_configs = {
            "market_analytics": {
                "path": f"{base_path}/market_analytics",
                "schema": "gold_market_analytics",
                "partitioning": self.partition_strategy.get_gold_partitioning()
            }
        }
    
    def create_table_if_not_exists(self, table_name: str) -> bool:
        """Create gold Delta table if it doesn't exist."""
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.warning("Spark not available, cannot create Delta tables")
            return False
        
        try:
            config = self.table_configs.get(table_name)
            if not config:
                self.logger.error(f"Unknown table: {table_name}")
                return False
            
            table_path = config["path"]
            schema = self.schema_registry.get_schema(config["schema"])
            partitioning = config["partitioning"]
            
            # Check if table already exists
            if DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.info(f"Gold table {table_name} already exists")
                return True
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write Delta table with partitioning
            (empty_df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy(*partitioning.partition_columns)
             .option("path", table_path)
             .saveAsTable(f"gold_{table_name}"))
            
            self.logger.info(f"Created gold table {table_name}")
            
            # Set table properties for analytics optimization
            self._set_table_properties(table_name, table_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create gold table {table_name}: {e}")
            return False
    
    def _set_table_properties(self, table_name: str, table_path: str) -> None:
        """Set optimization properties for analytics queries."""
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Set properties optimized for analytical queries
            (delta_table.alter()
             .property("delta.autoOptimize.optimizeWrite", "true")
             .property("delta.autoOptimize.autoCompact", "true")
             .property("delta.targetFileSize", "512MB")  # Larger files for analytics
             .property("delta.tuneFileSizesForRewrites", "true")
             .property("delta.logRetentionDuration", "interval 90 days")  # Longer retention
             .property("delta.deletedFileRetentionDuration", "interval 30 days"))
            
            self.logger.info(f"Set analytics optimization properties for {table_name}")
            
        except Exception as e:
            self.logger.warning(f"Failed to set properties for {table_name}: {e}")


class GoldDataMart:
    """Creates analytics-ready data marts from silver layer data."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/gold"):
        """Initialize gold data mart processor.
        
        Args:
            spark_session: Spark session for processing
            base_path: Base path for gold tables
        """
        self.logger = StructuredLogger("GoldDataMart")
        self.spark = spark_session
        
        # Initialize components
        self.table_manager = GoldTableManager(spark_session, base_path)
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        self.indicator_calculator = TechnicalIndicatorCalculator()
        
        # Initialize tables
        self._initialize_tables()
        
        # VN Index components for correlation calculations
        self.vn_index_symbols = [
            "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX"
        ]
        
    def _initialize_tables(self) -> None:
        """Initialize gold layer tables."""
        success = self.table_manager.create_table_if_not_exists("market_analytics")
        if success:
            self.logger.info("Gold tables initialized")
        else:
            self.logger.warning("Failed to initialize gold tables")
    
    def process_silver_to_gold(self, silver_path: str, processing_date: Optional[date] = None) -> Dict[str, Any]:
        """Process silver data to gold layer with analytics calculations.
        
        Args:
            silver_path: Path to silver data
            processing_date: Date to process (None for today)
            
        Returns:
            Processing result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        if not processing_date:
            processing_date = date.today()
        
        try:
            self.logger.info(f"Processing silver to gold for {processing_date}")
            
            # Read silver data with lookback for technical indicators
            lookback_date = processing_date - timedelta(days=365)  # 1 year lookback
            silver_df = self._read_silver_data(silver_path, lookback_date, processing_date)
            
            if silver_df is None or silver_df.count() == 0:
                return {"success": False, "message": f"No silver data found for date range"}
            
            self.logger.info(f"Found {silver_df.count()} silver records for processing")
            
            # Calculate technical indicators
            analytics_df = self._calculate_all_indicators(silver_df)
            
            # Calculate returns and risk metrics
            analytics_df = self._calculate_returns_and_risk(analytics_df)
            
            # Calculate market structure metrics
            analytics_df = self._calculate_market_structure(analytics_df)
            
            # Calculate Vietnamese market specific metrics
            analytics_df = self._calculate_vn_market_metrics(analytics_df)
            
            # Calculate momentum and quality scores
            analytics_df = self._calculate_composite_scores(analytics_df)
            
            # Filter to target processing date and prepare final schema
            final_df = self._prepare_final_gold_data(analytics_df, processing_date)
            
            # Write to gold table
            records_written = self._write_to_gold_table(final_df, "market_analytics")
            
            result = {
                "success": True,
                "processing_date": processing_date.isoformat(),
                "records_processed": silver_df.count(),
                "records_written": records_written,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Successfully processed {records_written} records to gold layer")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process silver to gold: {e}")
            return {"success": False, "message": str(e)}
    
    def _read_silver_data(self, silver_path: str, start_date: date, end_date: date) -> Optional[DataFrame]:
        """Read silver data for date range."""
        try:
            # Read with date range filter
            silver_df = (self.spark.read
                        .format("delta")
                        .load(silver_path)
                        .filter(col("timestamp") >= lit(start_date))
                        .filter(col("timestamp") <= lit(end_date))
                        .orderBy("symbol", "timestamp"))
            
            return silver_df
            
        except Exception as e:
            self.logger.error(f"Failed to read silver data: {e}")
            return None
    
    def _calculate_all_indicators(self, df: DataFrame) -> DataFrame:
        """Calculate all technical indicators."""
        self.logger.info("Calculating technical indicators")
        
        # Rename columns to match indicator calculator expectations
        prep_df = df.select(
            col("symbol"),
            col("timestamp"),
            col("date"),
            col("open_price").alias("open"),
            col("high_price").alias("high"),
            col("low_price").alias("low"),
            col("close_price").alias("close"),
            col("volume"),
            col("value"),
            col("vwap"),
            "*"  # Include all other columns
        )
        
        # Calculate indicators step by step
        result_df = self.indicator_calculator.calculate_moving_averages(prep_df)
        result_df = self.indicator_calculator.calculate_exponential_moving_averages(result_df)
        result_df = self.indicator_calculator.calculate_macd(result_df)
        result_df = self.indicator_calculator.calculate_rsi(result_df)
        result_df = self.indicator_calculator.calculate_bollinger_bands(result_df)
        result_df = self.indicator_calculator.calculate_stochastic(result_df)
        result_df = self.indicator_calculator.calculate_atr(result_df)
        result_df = self.indicator_calculator.calculate_volume_indicators(result_df)
        
        return result_df
    
    def _calculate_returns_and_risk(self, df: DataFrame) -> DataFrame:
        """Calculate returns and risk metrics."""
        self.logger.info("Calculating returns and risk metrics")
        
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Calculate returns for different periods
        result_df = df.withColumn(
            "returns_1d",
            (col("close") / lag("close", 1).over(symbol_window)) - 1
        ).withColumn(
            "returns_5d",
            (col("close") / lag("close", 5).over(symbol_window)) - 1
        ).withColumn(
            "returns_21d",
            (col("close") / lag("close", 21).over(symbol_window)) - 1
        ).withColumn(
            "returns_63d",
            (col("close") / lag("close", 63).over(symbol_window)) - 1
        ).withColumn(
            "returns_252d",
            (col("close") / lag("close", 252).over(symbol_window)) - 1
        )
        
        # Calculate volatility measures
        result_df = result_df.withColumn(
            "volatility_10d",
            stddev("returns_1d").over(symbol_window.rowsBetween(-9, 0)) * sqrt(lit(252))
        ).withColumn(
            "volatility_30d",
            stddev("returns_1d").over(symbol_window.rowsBetween(-29, 0)) * sqrt(lit(252))
        ).withColumn(
            "volatility_60d",
            stddev("returns_1d").over(symbol_window.rowsBetween(-59, 0)) * sqrt(lit(252))
        )
        
        return result_df
    
    def _calculate_market_structure(self, df: DataFrame) -> DataFrame:
        """Calculate market structure metrics."""
        self.logger.info("Calculating market structure metrics")
        
        # Market cap calculation (placeholder - would need shares outstanding)
        result_df = df.withColumn(
            "market_cap",
            col("close") * lit(1000000)  # Placeholder
        ).withColumn(
            "enterprise_value",
            col("market_cap")  # Simplified
        ).withColumn(
            "free_float_market_cap",
            col("market_cap") * 0.7  # Assume 70% free float
        )
        
        return result_df
    
    def _calculate_vn_market_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate Vietnamese market specific metrics."""
        self.logger.info("Calculating Vietnamese market specific metrics")
        
        # VN30 weight (simplified)
        result_df = df.withColumn(
            "vn30_weight",
            when(col("is_vn30"), col("market_cap") / lit(1000000000000)).otherwise(0.0)  # Placeholder
        )
        
        # VN Index correlation (placeholder - would need index data)
        result_df = result_df.withColumn(
            "vn_index_correlation_30d",
            when(col("symbol").isin(self.vn_index_symbols), lit(0.8)).otherwise(lit(0.3))
        )
        
        # Sector relative strength (placeholder)
        result_df = result_df.withColumn(
            "sector_relative_strength",
            lit(1.0)  # Placeholder
        )
        
        return result_df
    
    def _calculate_composite_scores(self, df: DataFrame) -> DataFrame:
        """Calculate composite momentum and quality scores."""
        self.logger.info("Calculating composite scores")
        
        # Momentum scores based on returns
        result_df = df.withColumn(
            "momentum_score_1m",
            when(col("returns_21d").isNotNull(), 
                 (col("returns_21d") * 100).cast(DoubleType())).otherwise(0.0)
        ).withColumn(
            "momentum_score_3m",
            when(col("returns_63d").isNotNull(),
                 (col("returns_63d") * 100).cast(DoubleType())).otherwise(0.0)
        ).withColumn(
            "momentum_score_6m",
            lit(0.0)  # Placeholder - would need 6-month lookback
        ).withColumn(
            "momentum_score_12m",
            when(col("returns_252d").isNotNull(),
                 (col("returns_252d") * 100).cast(DoubleType())).otherwise(0.0)
        )
        
        # Risk metrics
        symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
        
        # Beta calculation (simplified - using correlation with VN-Index proxy)
        result_df = result_df.withColumn(
            "beta_60d",
            when(col("symbol").isin(self.vn_index_symbols), lit(1.2)).otherwise(lit(0.8))
        ).withColumn(
            "beta_252d",
            when(col("symbol").isin(self.vn_index_symbols), lit(1.1)).otherwise(lit(0.9))
        )
        
        # Sharpe ratio (simplified)
        result_df = result_df.withColumn(
            "sharpe_ratio_60d",
            when((col("volatility_60d") > 0) & (col("returns_63d").isNotNull()),
                 col("returns_63d") / col("volatility_60d")).otherwise(0.0)
        )
        
        # Maximum drawdown (simplified)
        result_df = result_df.withColumn(
            "max_drawdown_60d",
            lit(-0.1)  # Placeholder -10%
        )
        
        # Quality scores
        result_df = result_df.withColumn(
            "quality_score",
            (col("data_completeness_score") * 0.4 + 
             col("data_accuracy_score") * 0.4 + 
             col("source_reliability_score") * 0.2)
        ).withColumn(
            "liquidity_score",
            when(col("volume_ratio") > 1.5, lit(1.0))
            .when(col("volume_ratio") > 1.0, lit(0.8))
            .when(col("volume_ratio") > 0.5, lit(0.6))
            .otherwise(lit(0.3))
        ).withColumn(
            "volatility_score",
            when(col("volatility_30d") < 0.2, lit(1.0))
            .when(col("volatility_30d") < 0.4, lit(0.8))
            .when(col("volatility_30d") < 0.6, lit(0.6))
            .otherwise(lit(0.3))
        )
        
        return result_df
    
    def _prepare_final_gold_data(self, df: DataFrame, target_date: date) -> DataFrame:
        """Prepare final gold data with proper schema mapping."""
        # Filter to target date
        target_df = df.filter(col("date") == lit(target_date.isoformat()))
        
        # Map to gold schema
        gold_df = target_df.select(
            col("symbol"),
            col("date"),
            col("timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("value"),
            col("vwap"),
            col("returns_1d"),
            col("returns_5d"),
            col("returns_21d"),
            col("returns_63d"),
            col("returns_252d"),
            col("sma_5"),
            col("sma_10"),
            col("sma_20"),
            col("sma_50"),
            col("sma_200"),
            col("ema_12"),
            col("ema_26"),
            col("macd"),
            col("macd_signal"),
            col("macd_histogram"),
            col("rsi_14"),
            col("stoch_k"),
            col("stoch_d"),
            col("bb_upper"),
            col("bb_middle"),
            col("bb_lower"),
            col("bb_width"),
            col("bb_position"),
            col("volatility_10d"),
            col("volatility_30d"),
            col("volatility_60d"),
            col("atr_14"),
            col("volume_sma_20"),
            col("volume_ratio"),
            col("money_flow_index"),
            col("on_balance_volume"),
            col("market_cap"),
            col("enterprise_value"),
            col("free_float_market_cap"),
            col("vn_index_correlation_30d"),
            col("vn30_weight"),
            col("sector_relative_strength"),
            col("beta_60d"),
            col("beta_252d"),
            col("sharpe_ratio_60d"),
            col("max_drawdown_60d"),
            col("momentum_score_1m"),
            col("momentum_score_3m"),
            col("momentum_score_6m"),
            col("momentum_score_12m"),
            col("quality_score"),
            col("liquidity_score"),
            col("volatility_score"),
            col("year"),
            col("month"),
            col("day")
        )
        
        return gold_df
    
    def _write_to_gold_table(self, df: DataFrame, table_name: str) -> int:
        """Write analytics data to gold table."""
        try:
            config = self.table_manager.table_configs.get(table_name)
            if not config:
                raise ValueError(f"Unknown table: {table_name}")
            
            table_path = config["path"]
            
            # Count records
            record_count = df.count()
            
            if record_count == 0:
                self.logger.warning("No records to write to gold table")
                return 0
            
            # Write to Delta table
            if DeltaTable.isDeltaTable(self.spark, table_path):
                # For gold layer, we can overwrite daily partitions
                (df.write
                 .format("delta")
                 .mode("append")
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Appended {record_count} records to gold {table_name}")
            else:
                # Create new table
                partitioning = config["partitioning"]
                (df.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*partitioning.partition_columns)
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Created gold {table_name} with {record_count} records")
            
            return record_count
            
        except Exception as e:
            self.logger.error(f"Failed to write to gold table: {e}")
            return 0
    
    def generate_analytics_summary(self, table_name: str = "market_analytics") -> Dict[str, Any]:
        """Generate analytics summary for gold table."""
        if not SPARK_AVAILABLE or not self.spark:
            return {"error": "Spark not available"}
        
        try:
            config = self.table_manager.table_configs.get(table_name)
            if not config:
                return {"error": f"Unknown table: {table_name}"}
            
            table_path = config["path"]
            
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"error": f"Gold table {table_name} does not exist"}
            
            # Read latest data
            df = self.spark.read.format("delta").load(table_path)
            latest_date = df.agg(spark_max("date")).collect()[0][0]
            
            if not latest_date:
                return {"error": "No data in gold table"}
            
            # Filter to latest date
            latest_df = df.filter(col("date") == lit(latest_date))
            
            # Generate summary statistics
            summary_stats = latest_df.agg(
                count("symbol").alias("total_symbols"),
                avg("quality_score").alias("avg_quality"),
                avg("volatility_30d").alias("avg_volatility"),
                avg("returns_21d").alias("avg_monthly_return"),
                spark_max("volume").alias("max_volume"),
                spark_min("volume").alias("min_volume")
            ).collect()[0]
            
            # Top performers
            top_performers = (latest_df
                            .select("symbol", "returns_21d", "volume", "quality_score")
                            .filter(col("returns_21d").isNotNull())
                            .orderBy(desc("returns_21d"))
                            .limit(10)
                            .collect())
            
            # VN30 statistics
            vn30_stats = (latest_df
                         .filter(col("vn30_weight") > 0)
                         .agg(count("symbol").alias("vn30_count"),
                              avg("returns_21d").alias("vn30_avg_return"))
                         .collect()[0])
            
            summary = {
                "table_name": table_name,
                "latest_date": latest_date,
                "report_timestamp": datetime.now().isoformat(),
                "summary_statistics": {
                    "total_symbols": summary_stats["total_symbols"],
                    "avg_quality_score": summary_stats["avg_quality"],
                    "avg_volatility": summary_stats["avg_volatility"],
                    "avg_monthly_return": summary_stats["avg_monthly_return"],
                    "volume_range": {
                        "max": summary_stats["max_volume"],
                        "min": summary_stats["min_volume"]
                    }
                },
                "top_performers": [
                    {
                        "symbol": row["symbol"],
                        "monthly_return": row["returns_21d"],
                        "volume": row["volume"],
                        "quality_score": row["quality_score"]
                    }
                    for row in top_performers
                ],
                "vn30_statistics": {
                    "count": vn30_stats["vn30_count"],
                    "avg_return": vn30_stats["vn30_avg_return"]
                }
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to generate analytics summary: {e}")
            return {"error": str(e)}