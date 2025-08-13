"""
Bronze Layer Data Ingestion for Vietnamese Market Data

This module implements raw data ingestion with schema enforcement,
partitioning, and ACID transaction support using Delta Lake.
"""

import sys
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import json

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger
from src.ingestion.base import MarketDataPoint
from src.datalake.common.schemas import SchemaRegistry, MarketDataSchema
from src.datalake.common.partitioning import PartitionStrategy
from src.datalake.common.quality import DataQualityValidator

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth
    from pyspark.sql.types import StructType
    from delta import DeltaTable, configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None


class BronzeTableManager:
    """Manages Delta Lake tables for bronze layer raw data."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/bronze"):
        """Initialize bronze table manager.
        
        Args:
            spark_session: Spark session for Delta operations
            base_path: Base path for Delta tables
        """
        self.logger = StructuredLogger("BronzeTableManager")
        self.base_path = base_path
        self.spark = spark_session
        
        # Initialize components
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        
        # Table configurations
        self.table_configs = {
            "market_data": {
                "path": f"{base_path}/market_data",
                "schema": "bronze_market_data",
                "partitioning": self.partition_strategy.get_bronze_partitioning()
            },
            "trading_signals": {
                "path": f"{base_path}/trading_signals",
                "schema": "trading_signals",
                "partitioning": self.partition_strategy.get_signals_partitioning()
            },
            "risk_events": {
                "path": f"{base_path}/risk_events",
                "schema": "risk_events",
                "partitioning": self.partition_strategy.get_risk_events_partitioning()
            }
        }
    
    def create_table_if_not_exists(self, table_name: str) -> bool:
        """Create Delta table if it doesn't exist.
        
        Args:
            table_name: Name of table to create
            
        Returns:
            True if table was created or already exists
        """
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
                self.logger.info(f"Delta table {table_name} already exists at {table_path}")
                return True
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Add partition columns if not in schema
            partition_cols = partitioning.partition_columns
            for col_name in partition_cols:
                if col_name not in [field.name for field in schema.fields]:
                    if col_name in ["year", "month", "day"]:
                        empty_df = empty_df.withColumn(col_name, lit(None).cast("int"))
            
            # Write Delta table with partitioning
            (empty_df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy(*partition_cols)
             .option("path", table_path)
             .saveAsTable(f"bronze_{table_name}"))
            
            self.logger.info(f"Created Delta table {table_name} at {table_path}")
            
            # Set table properties
            self._set_table_properties(table_name, table_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _set_table_properties(self, table_name: str, table_path: str) -> None:
        """Set table properties for optimization."""
        try:
            config = self.table_configs[table_name]
            partitioning = config["partitioning"]
            
            # Get optimization settings
            file_size_mb = self.partition_strategy.get_optimal_file_size_mb(table_name)
            z_order_cols = self.partition_strategy.get_z_order_columns(table_name)
            retention_policy = self.partition_strategy.get_retention_policy(table_name)
            
            # Set Delta table properties
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Auto-optimize and auto-compaction
            (delta_table.alter()
             .property("delta.autoOptimize.optimizeWrite", "true")
             .property("delta.autoOptimize.autoCompact", "true")
             .property("delta.targetFileSize", f"{file_size_mb}MB")
             .property("delta.tuneFileSizesForRewrites", "true"))
            
            # Data retention
            if retention_policy.get("retention_days"):
                retention_hours = retention_policy["retention_days"] * 24
                (delta_table.alter()
                 .property("delta.logRetentionDuration", f"interval {retention_hours} hours")
                 .property("delta.deletedFileRetentionDuration", f"interval {retention_hours} hours"))
            
            self.logger.info(f"Set table properties for {table_name}")
            
        except Exception as e:
            self.logger.warning(f"Failed to set table properties for {table_name}: {e}")
    
    def optimize_table(self, table_name: str, z_order_columns: Optional[List[str]] = None) -> bool:
        """Optimize Delta table with compaction and Z-ordering.
        
        Args:
            table_name: Name of table to optimize
            z_order_columns: Columns to Z-order by
            
        Returns:
            True if optimization succeeded
        """
        if not SPARK_AVAILABLE or not self.spark:
            return False
        
        try:
            config = self.table_configs.get(table_name)
            if not config:
                return False
            
            table_path = config["path"]
            
            # Check if table exists
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.warning(f"Table {table_name} does not exist")
                return False
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Get Z-order columns
            if not z_order_columns:
                z_order_columns = self.partition_strategy.get_z_order_columns(table_name)
            
            # Optimize table
            if z_order_columns:
                delta_table.optimize().zOrderBy(*z_order_columns).executeCompaction()
                self.logger.info(f"Optimized table {table_name} with Z-order by {z_order_columns}")
            else:
                delta_table.optimize().executeCompaction()
                self.logger.info(f"Optimized table {table_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to optimize table {table_name}: {e}")
            return False
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> bool:
        """Vacuum Delta table to remove old files.
        
        Args:
            table_name: Name of table to vacuum
            retention_hours: Retention period in hours
            
        Returns:
            True if vacuum succeeded
        """
        if not SPARK_AVAILABLE or not self.spark:
            return False
        
        try:
            config = self.table_configs.get(table_name)
            if not config:
                return False
            
            table_path = config["path"]
            
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return False
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.vacuum(retentionHours=retention_hours)
            
            self.logger.info(f"Vacuumed table {table_name} with {retention_hours}h retention")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to vacuum table {table_name}: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about Delta table.
        
        Args:
            table_name: Name of table
            
        Returns:
            Table information dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"error": "Spark not available"}
        
        try:
            config = self.table_configs.get(table_name)
            if not config:
                return {"error": f"Unknown table: {table_name}"}
            
            table_path = config["path"]
            
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"error": f"Table {table_name} does not exist"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Get table details
            history = delta_table.history(1).collect()
            latest_version = history[0] if history else None
            
            # Get table statistics
            df = delta_table.toDF()
            row_count = df.count()
            
            info = {
                "table_name": table_name,
                "table_path": table_path,
                "row_count": row_count,
                "latest_version": latest_version["version"] if latest_version else None,
                "last_modified": latest_version["timestamp"] if latest_version else None,
                "schema": config["schema"],
                "partitioning": config["partitioning"].partition_columns
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            return {"error": str(e)}


class BronzeDataIngestion:
    """Handles raw data ingestion into Bronze layer Delta tables."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None, base_path: str = "./data/delta/bronze"):
        """Initialize bronze data ingestion.
        
        Args:
            spark_session: Spark session for Delta operations
            base_path: Base path for Delta tables
        """
        self.logger = StructuredLogger("BronzeDataIngestion")
        self.spark = spark_session
        
        # Initialize components
        self.table_manager = BronzeTableManager(spark_session, base_path)
        self.schema_registry = SchemaRegistry()
        self.partition_strategy = PartitionStrategy()
        self.quality_validator = DataQualityValidator()
        
        # Create tables if they don't exist
        self._initialize_tables()
    
    def _initialize_tables(self) -> None:
        """Initialize required Delta tables."""
        tables_to_create = ["market_data", "trading_signals", "risk_events"]
        
        for table_name in tables_to_create:
            success = self.table_manager.create_table_if_not_exists(table_name)
            if success:
                self.logger.info(f"Bronze table {table_name} ready")
            else:
                self.logger.warning(f"Failed to initialize table {table_name}")
    
    def ingest_market_data_points(self, data_points: List[MarketDataPoint], source: str = "unknown") -> Dict[str, Any]:
        """Ingest market data points into bronze layer.
        
        Args:
            data_points: List of market data points
            source: Data source identifier
            
        Returns:
            Ingestion result dictionary
        """
        if not data_points:
            return {"success": False, "message": "No data points to ingest"}
        
        try:
            # Convert to bronze schema format
            bronze_records = self._convert_to_bronze_format(data_points, source)
            
            # Validate data quality
            quality_report = self.quality_validator.validate_bronze_data(bronze_records)
            
            # Log quality issues
            if quality_report.failed_validations > 0:
                self.logger.warning(f"Data quality issues found: {quality_report.failed_validations} failed validations")
                for result in quality_report.validation_results:
                    if not result.passed:
                        self.logger.warning(f"Quality check failed: {result.message}")
            
            # Proceed with ingestion if quality score is acceptable
            if quality_report.quality_score < 0.5:
                self.logger.error(f"Data quality too low: {quality_report.quality_score:.2f}")
                return {
                    "success": False,
                    "message": "Data quality below threshold",
                    "quality_report": quality_report
                }
            
            # Ingest to Delta table
            if SPARK_AVAILABLE and self.spark:
                result = self._write_to_delta_table(bronze_records, "market_data")
            else:
                result = self._write_to_file_system(bronze_records, "market_data")
            
            result["quality_report"] = quality_report
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to ingest market data: {e}")
            return {"success": False, "message": str(e)}
    
    def ingest_trading_signals(self, signals: List[Dict[str, Any]], source: str = "strategy") -> Dict[str, Any]:
        """Ingest trading signals into bronze layer.
        
        Args:
            signals: List of trading signal dictionaries
            source: Signal source identifier
            
        Returns:
            Ingestion result dictionary
        """
        if not signals:
            return {"success": False, "message": "No signals to ingest"}
        
        try:
            # Add metadata
            bronze_signals = []
            for signal in signals:
                bronze_signal = signal.copy()
                bronze_signal["source"] = source
                bronze_signal["ingestion_timestamp"] = datetime.now()
                
                # Add partition columns
                timestamp = bronze_signal.get("timestamp", datetime.now())
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp)
                
                partition_values = self.partition_strategy.calculate_partition_values(timestamp)
                bronze_signal.update(partition_values)
                
                bronze_signals.append(bronze_signal)
            
            # Write to Delta table
            if SPARK_AVAILABLE and self.spark:
                result = self._write_to_delta_table(bronze_signals, "trading_signals")
            else:
                result = self._write_to_file_system(bronze_signals, "trading_signals")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to ingest trading signals: {e}")
            return {"success": False, "message": str(e)}
    
    def ingest_risk_events(self, events: List[Dict[str, Any]], source: str = "risk_manager") -> Dict[str, Any]:
        """Ingest risk events into bronze layer.
        
        Args:
            events: List of risk event dictionaries
            source: Event source identifier
            
        Returns:
            Ingestion result dictionary
        """
        if not events:
            return {"success": False, "message": "No events to ingest"}
        
        try:
            # Add metadata
            bronze_events = []
            for event in events:
                bronze_event = event.copy()
                bronze_event["source"] = source
                bronze_event["ingestion_timestamp"] = datetime.now()
                
                # Add partition columns
                timestamp = bronze_event.get("timestamp", datetime.now())
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp)
                
                partition_values = self.partition_strategy.calculate_partition_values(timestamp)
                bronze_event.update(partition_values)
                
                bronze_events.append(bronze_event)
            
            # Write to Delta table
            if SPARK_AVAILABLE and self.spark:
                result = self._write_to_delta_table(bronze_events, "risk_events")
            else:
                result = self._write_to_file_system(bronze_events, "risk_events")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to ingest risk events: {e}")
            return {"success": False, "message": str(e)}
    
    def _convert_to_bronze_format(self, data_points: List[MarketDataPoint], source: str) -> List[Dict[str, Any]]:
        """Convert market data points to bronze schema format."""
        bronze_records = []
        
        for point in data_points:
            # Calculate partition values
            partition_values = self.partition_strategy.calculate_partition_values(point.timestamp)
            
            # Create bronze record
            bronze_record = {
                # Metadata
                "source": source,
                "ingestion_timestamp": datetime.now(),
                "data_timestamp": point.timestamp,
                "sequence_id": None,
                
                # Market data
                "symbol": point.symbol,
                "exchange": "HOSE",  # Default to HOSE
                "market_type": "STOCK",
                
                # OHLCV
                "open_price": point.open_price,
                "high_price": point.high_price,
                "low_price": point.low_price,
                "close_price": point.close_price,
                "volume": point.volume,
                "value": point.value,
                
                # Additional fields
                "bid_price": None,
                "ask_price": None,
                "bid_volume": None,
                "ask_volume": None,
                "trading_session": self._determine_trading_session(point.timestamp),
                "is_trading_halt": False,
                
                # Raw data and quality
                "raw_data": json.dumps(point.__dict__, default=str),
                "is_valid": True,
                "validation_errors": None,
                
                # Partitioning
                **partition_values
            }
            
            bronze_records.append(bronze_record)
        
        return bronze_records
    
    def _determine_trading_session(self, timestamp: datetime) -> str:
        """Determine trading session from timestamp."""
        time_part = timestamp.time()
        
        # Morning session: 09:00-11:30
        if time(9, 0) <= time_part <= time(11, 30):
            return "MORNING"
        # Afternoon session: 13:00-15:00
        elif time(13, 0) <= time_part <= time(15, 0):
            return "AFTERNOON"
        # ATC session: 14:45-15:00
        elif time(14, 45) <= time_part <= time(15, 0):
            return "ATC"
        else:
            return "AFTER_HOURS"
    
    def _write_to_delta_table(self, records: List[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
        """Write records to Delta table.
        
        Args:
            records: Records to write
            table_name: Name of target table
            
        Returns:
            Write result dictionary
        """
        try:
            config = self.table_manager.table_configs.get(table_name)
            if not config:
                return {"success": False, "message": f"Unknown table: {table_name}"}
            
            table_path = config["path"]
            schema = self.schema_registry.get_schema(config["schema"])
            
            # Create DataFrame
            df = self.spark.createDataFrame(records, schema)
            
            # Write to Delta table with merge operation for upserts
            if DeltaTable.isDeltaTable(self.spark, table_path):
                # Merge operation for existing table
                delta_table = DeltaTable.forPath(self.spark, table_path)
                
                # Simple append for now - could implement upsert logic
                (df.write
                 .format("delta")
                 .mode("append")
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Appended {len(records)} records to {table_name}")
            else:
                # Create new table
                (df.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*config["partitioning"].partition_columns)
                 .option("path", table_path)
                 .save())
                
                self.logger.info(f"Created {table_name} with {len(records)} records")
            
            return {
                "success": True,
                "records_written": len(records),
                "table_path": table_path,
                "write_mode": "delta"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table {table_name}: {e}")
            return {"success": False, "message": str(e)}
    
    def _write_to_file_system(self, records: List[Dict[str, Any]], table_name: str) -> Dict[str, Any]:
        """Fallback to write records to file system when Spark not available.
        
        Args:
            records: Records to write
            table_name: Name of target table
            
        Returns:
            Write result dictionary
        """
        try:
            # Create directory structure
            base_dir = Path("./data/bronze") / table_name
            
            # Group by partition
            partitioned_data = {}
            for record in records:
                year = record.get("year", 2024)
                month = record.get("month", 1)
                day = record.get("day", 1)
                
                partition_key = f"year={year}/month={month}/day={day}"
                if partition_key not in partitioned_data:
                    partitioned_data[partition_key] = []
                partitioned_data[partition_key].append(record)
            
            # Write partitioned files
            written_files = []
            for partition, partition_records in partitioned_data.items():
                partition_dir = base_dir / partition
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Write JSON file
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_path = partition_dir / f"data_{timestamp}.json"
                
                with open(file_path, 'w') as f:
                    json.dump(partition_records, f, indent=2, default=str)
                
                written_files.append(str(file_path))
            
            self.logger.info(f"Wrote {len(records)} records to {len(written_files)} files")
            
            return {
                "success": True,
                "records_written": len(records),
                "files_written": written_files,
                "write_mode": "json"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to write to file system: {e}")
            return {"success": False, "message": str(e)}
    
    def get_ingestion_stats(self) -> Dict[str, Any]:
        """Get ingestion statistics across all tables."""
        stats = {
            "timestamp": datetime.now(),
            "tables": {}
        }
        
        for table_name in self.table_manager.table_configs.keys():
            table_info = self.table_manager.get_table_info(table_name)
            stats["tables"][table_name] = table_info
        
        return stats