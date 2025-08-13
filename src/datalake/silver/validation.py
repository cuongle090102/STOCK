"""
Silver Layer Data Processing for Vietnamese Market Data

This module processes raw bronze data into validated, cleansed, and
standardized silver layer data with comprehensive data quality checks.
"""

import sys
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
import json
import math

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger
from src.datalake.common.schemas import SchemaRegistry, MarketDataSchema
from src.datalake.common.partitioning import PartitionStrategy
from src.datalake.common.quality import DataQualityValidator

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, lit, when, isnan, isnull, coalesce, avg, stddev, lag, lead,
        sum as spark_sum, count, max as spark_max, min as spark_min,
        window, desc, asc, rank, dense_rank, row_number, round as spark_round,
        year, month, dayofmonth, hour, minute, current_timestamp
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import StringType, DoubleType, IntegerType, BooleanType
    from delta import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None


class SilverTableManager:
    """Manages Delta Lake tables for silver layer validated data."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/silver"):
        """Initialize silver table manager.
        
        Args:
            spark_session: Spark session for Delta operations
            base_path: Base path for Delta tables
        """
        self.logger = StructuredLogger("SilverTableManager")
        self.base_path = base_path
        self.spark = spark_session
        
        # Initialize components
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        
        # VN30 symbols for market classification
        self.vn30_symbols = [
            "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
            "VHM", "TCB", "MWG", "VRE", "SAB", "NVL", "POW", "KDH", "TPB", "SSI",
            "VPB", "PDR", "STB", "HDB", "MBB", "ACB", "VJC", "VND", "GEX", "DGC"
        ]
        
        # Market classification mappings
        self.market_classifications = {
            "sectors": {
                "VIC": "Real Estate", "VHM": "Real Estate", "VRE": "Real Estate", "NVL": "Real Estate", "KDH": "Real Estate",
                "VCB": "Banking", "BID": "Banking", "CTG": "Banking", "TCB": "Banking", "TPB": "Banking", 
                "VPB": "Banking", "STB": "Banking", "HDB": "Banking", "MBB": "Banking", "ACB": "Banking",
                "VNM": "Consumer Goods", "SAB": "Consumer Goods", "MSN": "Consumer Goods",
                "HPG": "Materials", "GAS": "Energy", "PLX": "Energy", "POW": "Utilities",
                "FPT": "Technology", "MWG": "Retail", "SSI": "Financial Services",
                "VJC": "Transportation", "VND": "Retail", "GEX": "Pharmaceuticals", "DGC": "Chemicals"
            },
            "market_caps": {
                "VIC": "LARGE", "VCB": "LARGE", "VNM": "LARGE", "HPG": "LARGE", "VHM": "LARGE",
                "BID": "LARGE", "CTG": "LARGE", "GAS": "LARGE", "TCB": "LARGE", "MSN": "LARGE"
            }
        }
        
        # Table configurations
        self.table_configs = {
            "market_data": {
                "path": f"{base_path}/market_data",
                "schema": "silver_market_data",
                "partitioning": self.partition_strategy.get_silver_partitioning()
            }
        }
    
    def create_table_if_not_exists(self, table_name: str) -> bool:
        """Create silver Delta table if it doesn't exist."""
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
                self.logger.info(f"Silver table {table_name} already exists")
                return True
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write Delta table with partitioning
            (empty_df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy(*partitioning.partition_columns)
             .option("path", table_path)
             .saveAsTable(f"silver_{table_name}"))
            
            self.logger.info(f"Created silver table {table_name}")
            
            # Set table properties for optimization
            self._set_table_properties(table_name, table_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create silver table {table_name}: {e}")
            return False
    
    def _set_table_properties(self, table_name: str, table_path: str) -> None:
        """Set optimization properties for silver table."""
        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Set properties for analytical queries
            (delta_table.alter()
             .property("delta.autoOptimize.optimizeWrite", "true")
             .property("delta.autoOptimize.autoCompact", "true")
             .property("delta.targetFileSize", "256MB")
             .property("delta.tuneFileSizesForRewrites", "true")
             .property("delta.logRetentionDuration", "interval 30 days")
             .property("delta.deletedFileRetentionDuration", "interval 7 days"))
            
            self.logger.info(f"Set optimization properties for {table_name}")
            
        except Exception as e:
            self.logger.warning(f"Failed to set properties for {table_name}: {e}")


class SilverDataProcessor:
    """Processes bronze data into validated silver layer data."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/silver"):
        """Initialize silver data processor.
        
        Args:
            spark_session: Spark session for processing
            base_path: Base path for silver tables
        """
        self.logger = StructuredLogger("SilverDataProcessor")
        self.spark = spark_session
        
        # Initialize components
        self.table_manager = SilverTableManager(spark_session, base_path)
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        self.quality_validator = DataQualityValidator()
        
        # Initialize tables
        self._initialize_tables()
        
        # Processing parameters
        self.max_price_change_pct = 0.15  # 15% max daily price change (higher than limit for validation)
        self.min_volume_threshold = 100    # Minimum volume threshold
        self.max_volume_multiplier = 10.0  # Maximum volume vs average
        
    def _initialize_tables(self) -> None:
        """Initialize silver layer tables."""
        success = self.table_manager.create_table_if_not_exists("market_data")
        if success:
            self.logger.info("Silver tables initialized")
        else:
            self.logger.warning("Failed to initialize silver tables")
    
    def process_bronze_to_silver(self, bronze_path: str, processing_date: Optional[date] = None) -> Dict[str, Any]:
        """Process bronze data to silver layer with validation and cleansing.
        
        Args:
            bronze_path: Path to bronze data
            processing_date: Date to process (None for today)
            
        Returns:
            Processing result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        if not processing_date:
            processing_date = date.today()
        
        try:
            self.logger.info(f"Processing bronze data for {processing_date}")
            
            # Read bronze data
            bronze_df = self._read_bronze_data(bronze_path, processing_date)
            if bronze_df is None or bronze_df.count() == 0:
                return {"success": False, "message": f"No bronze data found for {processing_date}"}
            
            self.logger.info(f"Found {bronze_df.count()} bronze records")
            
            # Data validation and cleansing
            validated_df = self._validate_and_cleanse_data(bronze_df)
            
            # Data enrichment
            enriched_df = self._enrich_market_data(validated_df)
            
            # Calculate derived fields
            processed_df = self._calculate_derived_fields(enriched_df)
            
            # Apply business rules
            final_df = self._apply_business_rules(processed_df)
            
            # Data quality assessment
            quality_score = self._assess_data_quality(final_df)
            
            # Write to silver table
            records_written = self._write_to_silver_table(final_df, "market_data")
            
            result = {
                "success": True,
                "processing_date": processing_date.isoformat(),
                "records_processed": bronze_df.count(),
                "records_written": records_written,
                "quality_score": quality_score,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Successfully processed {records_written} records with quality score {quality_score:.2f}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process bronze to silver: {e}")
            return {"success": False, "message": str(e)}
    
    def _read_bronze_data(self, bronze_path: str, processing_date: date) -> Optional[DataFrame]:
        """Read bronze data for specific date."""
        try:
            # Build partition filter
            year = processing_date.year
            month = processing_date.month
            day = processing_date.day
            
            # Read with partition pruning
            bronze_df = (self.spark.read
                        .format("delta")
                        .load(bronze_path)
                        .filter((col("year") == year) & 
                               (col("month") == month) & 
                               (col("day") == day))
                        .filter(col("is_valid") == True))
            
            return bronze_df
            
        except Exception as e:
            self.logger.error(f"Failed to read bronze data: {e}")
            return None
    
    def _validate_and_cleanse_data(self, df: DataFrame) -> DataFrame:
        """Validate and cleanse bronze data."""
        self.logger.info("Validating and cleansing data")
        
        # Remove records with null required fields
        validated_df = df.filter(
            col("symbol").isNotNull() &
            col("close_price").isNotNull() &
            col("data_timestamp").isNotNull()
        )
        
        # Clean price fields - ensure positive values
        price_cols = ["open_price", "high_price", "low_price", "close_price"]
        for price_col in price_cols:
            validated_df = validated_df.withColumn(
                price_col,
                when(col(price_col) > 0, col(price_col)).otherwise(None)
            )
        
        # Clean volume - ensure non-negative
        validated_df = validated_df.withColumn(
            "volume",
            when(col("volume") >= 0, col("volume")).otherwise(0)
        )
        
        # Fix OHLC inconsistencies
        validated_df = self._fix_ohlc_inconsistencies(validated_df)
        
        # Remove duplicates based on symbol and timestamp
        validated_df = validated_df.dropDuplicates(["symbol", "data_timestamp"])
        
        return validated_df
    
    def _fix_ohlc_inconsistencies(self, df: DataFrame) -> DataFrame:
        """Fix OHLC price inconsistencies."""
        # Ensure Low <= Open, Close <= High
        # If inconsistent, use Close price as reference
        
        fixed_df = df.withColumn(
            "high_price",
            when(
                col("high_price").isNull() | 
                (col("high_price") < col("close_price")),
                col("close_price")
            ).otherwise(col("high_price"))
        ).withColumn(
            "low_price",
            when(
                col("low_price").isNull() | 
                (col("low_price") > col("close_price")),
                col("close_price")
            ).otherwise(col("low_price"))
        ).withColumn(
            "open_price",
            when(
                col("open_price").isNull(),
                col("close_price")
            ).otherwise(col("open_price"))
        )
        
        return fixed_df
    
    def _enrich_market_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with market classification and metadata."""
        self.logger.info("Enriching market data")
        
        # Add market classification
        enriched_df = df.withColumn("is_vn30", col("symbol").isin(self.table_manager.vn30_symbols))
        
        # Add sector information
        sector_mapping = self.table_manager.market_classifications["sectors"]
        sector_expr = None
        for symbol, sector in sector_mapping.items():
            condition = when(col("symbol") == symbol, lit(sector))
            sector_expr = condition if sector_expr is None else sector_expr.otherwise(condition)
        
        enriched_df = enriched_df.withColumn(
            "sector",
            sector_expr.otherwise(lit("Other")) if sector_expr else lit("Other")
        )
        
        # Add market cap category
        market_cap_mapping = self.table_manager.market_classifications["market_caps"]
        market_cap_expr = None
        for symbol, category in market_cap_mapping.items():
            condition = when(col("symbol") == symbol, lit(category))
            market_cap_expr = condition if market_cap_expr is None else market_cap_expr.otherwise(condition)
        
        enriched_df = enriched_df.withColumn(
            "market_cap_category",
            market_cap_expr.otherwise(lit("SMALL")) if market_cap_expr else lit("SMALL")
        )
        
        # Add exchange (default to HOSE for VN30, others to HNX)
        enriched_df = enriched_df.withColumn(
            "exchange",
            when(col("is_vn30"), lit("HOSE")).otherwise(lit("HNX"))
        )
        
        # Add industry (simplified mapping)
        enriched_df = enriched_df.withColumn("industry", col("sector"))  # Simplified
        
        return enriched_df
    
    def _calculate_derived_fields(self, df: DataFrame) -> DataFrame:
        """Calculate derived fields and technical indicators."""
        self.logger.info("Calculating derived fields")
        
        # Window specifications
        symbol_window = Window.partitionBy("symbol").orderBy("data_timestamp")
        symbol_window_desc = Window.partitionBy("symbol").orderBy(desc("data_timestamp"))
        
        # Price changes
        derived_df = df.withColumn(
            "prev_close",
            lag("close_price", 1).over(symbol_window)
        ).withColumn(
            "price_change",
            col("close_price") - col("prev_close")
        ).withColumn(
            "price_change_pct",
            when(col("prev_close") > 0, 
                 (col("close_price") - col("prev_close")) / col("prev_close")
            ).otherwise(0.0)
        )
        
        # Volume Weighted Average Price (VWAP)
        derived_df = derived_df.withColumn(
            "vwap",
            when(col("volume") > 0,
                 ((col("high_price") + col("low_price") + col("close_price")) / 3)
            ).otherwise(col("close_price"))
        )
        
        # Adjusted prices (placeholder - would need corporate action data)
        derived_df = derived_df.withColumn("adj_open", col("open_price")) \
                             .withColumn("adj_high", col("high_price")) \
                             .withColumn("adj_low", col("low_price")) \
                             .withColumn("adj_close", col("close_price")) \
                             .withColumn("adj_volume", col("volume"))
        
        # Bid-ask spread (placeholder)
        derived_df = derived_df.withColumn(
            "bid_ask_spread",
            when((col("ask_price").isNotNull()) & (col("bid_price").isNotNull()),
                 col("ask_price") - col("bid_price")
            ).otherwise(None)
        ).withColumn(
            "bid_ask_spread_pct",
            when((col("bid_ask_spread").isNotNull()) & (col("close_price") > 0),
                 col("bid_ask_spread") / col("close_price")
            ).otherwise(None)
        )
        
        # Calculate additional fields
        derived_df = derived_df.withColumn("trades_count", lit(None).cast(IntegerType())) \
                             .withColumn("avg_trade_size", lit(None).cast(DoubleType())) \
                             .withColumn("turnover_ratio", lit(None).cast(DoubleType()))
        
        # Vietnamese market specific fields
        derived_df = derived_df.withColumn("foreign_ownership_pct", lit(None).cast(DoubleType())) \
                             .withColumn("foreign_ownership_limit_pct", lit(30.0))  # Default 30% limit
        
        # Price limits (±7% for most stocks)
        derived_df = derived_df.withColumn(
            "price_limit_up",
            when(col("prev_close").isNotNull(),
                 col("prev_close") * 1.07
            ).otherwise(None)
        ).withColumn(
            "price_limit_down", 
            when(col("prev_close").isNotNull(),
                 col("prev_close") * 0.93
            ).otherwise(None)
        )
        
        return derived_df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply Vietnamese market business rules and constraints."""
        self.logger.info("Applying business rules")
        
        # Data quality scoring
        quality_df = df.withColumn(
            "data_completeness_score",
            # Score based on field completeness
            ((when(col("open_price").isNotNull(), 1).otherwise(0) +
              when(col("high_price").isNotNull(), 1).otherwise(0) +
              when(col("low_price").isNotNull(), 1).otherwise(0) +
              when(col("volume").isNotNull(), 1).otherwise(0)) / 4.0)
        ).withColumn(
            "data_accuracy_score",
            # Score based on data consistency (OHLC relationships)
            when(
                (col("low_price") <= col("open_price")) &
                (col("open_price") <= col("high_price")) &
                (col("low_price") <= col("close_price")) &
                (col("close_price") <= col("high_price")),
                1.0
            ).otherwise(0.5)  # Partial score for fixed data
        ).withColumn(
            "source_reliability_score",
            # Score based on data source reliability
            when(col("source").isin(["VNDirect", "SSI"]), 1.0)
            .when(col("source") == "TCBS", 0.9)
            .otherwise(0.7)
        )
        
        # Add timestamps and metadata
        processed_df = quality_df.withColumn("last_updated", current_timestamp()) \
                                .withColumn("data_sources", col("source"))  # Could be JSON array
        
        # Final field mapping to silver schema
        silver_df = processed_df.select(
            col("symbol"),
            col("data_timestamp").alias("timestamp"),
            col("data_timestamp").cast("date").cast("string").alias("date"),
            col("exchange"),
            col("sector"),
            col("industry"),
            col("market_cap_category"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("volume"),
            col("value"),
            col("adj_open"),
            col("adj_high"),
            col("adj_low"),
            col("adj_close"),
            col("adj_volume"),
            col("price_change"),
            col("price_change_pct"),
            col("vwap"),
            col("trades_count"),
            col("avg_trade_size"),
            col("bid_ask_spread"),
            col("bid_ask_spread_pct"),
            col("turnover_ratio"),
            col("is_vn30"),
            col("foreign_ownership_pct"),
            col("foreign_ownership_limit_pct"),
            col("price_limit_up"),
            col("price_limit_down"),
            col("data_completeness_score"),
            col("data_accuracy_score"),
            col("source_reliability_score"),
            col("last_updated"),
            col("data_sources"),
            col("year"),
            col("month"),
            col("day")
        )
        
        return silver_df
    
    def _assess_data_quality(self, df: DataFrame) -> float:
        """Assess overall data quality score."""
        try:
            quality_metrics = df.agg(
                avg("data_completeness_score").alias("avg_completeness"),
                avg("data_accuracy_score").alias("avg_accuracy"),
                avg("source_reliability_score").alias("avg_reliability")
            ).collect()[0]
            
            # Weighted average
            completeness = quality_metrics["avg_completeness"] or 0
            accuracy = quality_metrics["avg_accuracy"] or 0  
            reliability = quality_metrics["avg_reliability"] or 0
            
            overall_score = (completeness * 0.4 + accuracy * 0.4 + reliability * 0.2)
            
            return round(overall_score, 3)
            
        except Exception as e:
            self.logger.warning(f"Failed to assess data quality: {e}")
            return 0.0
    
    def _write_to_silver_table(self, df: DataFrame, table_name: str) -> int:
        """Write processed data to silver table."""
        try:
            config = self.table_manager.table_configs.get(table_name)
            if not config:
                raise ValueError(f"Unknown table: {table_name}")
            
            table_path = config["path"]
            
            # Count records before writing
            record_count = df.count()
            
            # Write to Delta table with merge for deduplication
            if DeltaTable.isDeltaTable(self.spark, table_path):
                # Merge operation to handle updates/upserts
                target_table = DeltaTable.forPath(self.spark, table_path)
                
                # For now, simple append - could implement proper merge logic
                (df.write
                 .format("delta")
                 .mode("append")
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Appended {record_count} records to silver {table_name}")
            else:
                # Create new table
                partitioning = config["partitioning"]
                (df.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*partitioning.partition_columns)
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Created silver {table_name} with {record_count} records")
            
            return record_count
            
        except Exception as e:
            self.logger.error(f"Failed to write to silver table: {e}")
            return 0
    
    def run_data_quality_report(self, table_name: str = "market_data") -> Dict[str, Any]:
        """Generate comprehensive data quality report for silver table."""
        if not SPARK_AVAILABLE or not self.spark:
            return {"error": "Spark not available"}
        
        try:
            config = self.table_manager.table_configs.get(table_name)
            if not config:
                return {"error": f"Unknown table: {table_name}"}
            
            table_path = config["path"]
            
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"error": f"Silver table {table_name} does not exist"}
            
            # Read table data
            df = self.spark.read.format("delta").load(table_path)
            
            # Generate quality metrics
            total_records = df.count()
            
            # Quality score distribution
            quality_stats = df.agg(
                avg("data_completeness_score").alias("avg_completeness"),
                avg("data_accuracy_score").alias("avg_accuracy"),
                avg("source_reliability_score").alias("avg_reliability"),
                spark_min("data_completeness_score").alias("min_completeness"),
                spark_max("data_completeness_score").alias("max_completeness")
            ).collect()[0]
            
            # Symbol statistics
            symbol_stats = df.groupBy("symbol").agg(
                count("*").alias("record_count"),
                avg("data_completeness_score").alias("avg_quality")
            ).orderBy(desc("record_count")).limit(10).collect()
            
            # Date coverage
            date_stats = df.agg(
                spark_min("timestamp").alias("min_date"),
                spark_max("timestamp").alias("max_date"),
                count("timestamp").alias("total_records")
            ).collect()[0]
            
            report = {
                "table_name": table_name,
                "report_timestamp": datetime.now().isoformat(),
                "total_records": total_records,
                "quality_metrics": {
                    "avg_completeness": quality_stats["avg_completeness"],
                    "avg_accuracy": quality_stats["avg_accuracy"],
                    "avg_reliability": quality_stats["avg_reliability"],
                    "min_completeness": quality_stats["min_completeness"],
                    "max_completeness": quality_stats["max_completeness"]
                },
                "top_symbols": [
                    {"symbol": row["symbol"], "records": row["record_count"], "quality": row["avg_quality"]}
                    for row in symbol_stats
                ],
                "date_coverage": {
                    "min_date": str(date_stats["min_date"]),
                    "max_date": str(date_stats["max_date"]),
                    "total_records": date_stats["total_records"]
                }
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to generate quality report: {e}")
            return {"error": str(e)}