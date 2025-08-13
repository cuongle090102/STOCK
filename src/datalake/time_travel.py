"""
Delta Lake Time Travel and ACID Transaction Support

This module provides time travel capabilities and ACID transaction management
for the Vietnamese algorithmic trading system using Delta Lake.
"""

import sys
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.utils.logging import StructuredLogger

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, lit, current_timestamp, desc
    from delta import DeltaTable
    from delta.tables import DeltaMergeBuilder
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None


class DeltaTimeTravel:
    """Provides time travel capabilities for Delta Lake tables."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Delta time travel manager.
        
        Args:
            spark_session: Spark session for Delta operations
        """
        self.logger = StructuredLogger("DeltaTimeTravel")
        self.spark = spark_session
        
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.warning("Spark not available - time travel features disabled")
    
    def read_table_at_version(self, table_path: str, version: int) -> Optional[DataFrame]:
        """Read Delta table at specific version.
        
        Args:
            table_path: Path to Delta table
            version: Version number to read
            
        Returns:
            DataFrame at specified version or None if error
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return None
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return None
            
            # Read at specific version
            df = (self.spark.read
                  .format("delta")
                  .option("versionAsOf", version)
                  .load(table_path))
            
            self.logger.info(f"Read table {table_path} at version {version}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read table at version {version}: {e}")
            return None
    
    def read_table_at_timestamp(self, table_path: str, timestamp: datetime) -> Optional[DataFrame]:
        """Read Delta table at specific timestamp.
        
        Args:
            table_path: Path to Delta table
            timestamp: Timestamp to read data as of
            
        Returns:
            DataFrame at specified timestamp or None if error
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return None
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return None
            
            # Format timestamp for Delta Lake
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            
            # Read at specific timestamp
            df = (self.spark.read
                  .format("delta")
                  .option("timestampAsOf", timestamp_str)
                  .load(table_path))
            
            self.logger.info(f"Read table {table_path} at timestamp {timestamp_str}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read table at timestamp {timestamp}: {e}")
            return None
    
    def get_table_history(self, table_path: str, limit: int = 50) -> Optional[DataFrame]:
        """Get history of Delta table versions.
        
        Args:
            table_path: Path to Delta table
            limit: Maximum number of history entries to return
            
        Returns:
            DataFrame with version history or None if error
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return None
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return None
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            history_df = delta_table.history(limit)
            
            self.logger.info(f"Retrieved {limit} history entries for {table_path}")
            return history_df
            
        except Exception as e:
            self.logger.error(f"Failed to get table history: {e}")
            return None
    
    def restore_table_to_version(self, table_path: str, version: int) -> bool:
        """Restore Delta table to specific version.
        
        Args:
            table_path: Path to Delta table
            version: Version to restore to
            
        Returns:
            True if restore succeeded
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return False
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return False
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.restoreToVersion(version)
            
            self.logger.info(f"Restored table {table_path} to version {version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restore table to version {version}: {e}")
            return False
    
    def restore_table_to_timestamp(self, table_path: str, timestamp: datetime) -> bool:
        """Restore Delta table to specific timestamp.
        
        Args:
            table_path: Path to Delta table
            timestamp: Timestamp to restore to
            
        Returns:
            True if restore succeeded
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return False
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return False
            
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.restoreToTimestamp(timestamp_str)
            
            self.logger.info(f"Restored table {table_path} to timestamp {timestamp_str}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restore table to timestamp {timestamp}: {e}")
            return False
    
    def compare_table_versions(self, table_path: str, version1: int, version2: int) -> Dict[str, Any]:
        """Compare two versions of a Delta table.
        
        Args:
            table_path: Path to Delta table
            version1: First version to compare
            version2: Second version to compare
            
        Returns:
            Comparison results dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"error": "Spark not available"}
        
        try:
            # Read both versions
            df1 = self.read_table_at_version(table_path, version1)
            df2 = self.read_table_at_version(table_path, version2)
            
            if df1 is None or df2 is None:
                return {"error": "Failed to read one or both versions"}
            
            # Basic comparison metrics
            count1 = df1.count()
            count2 = df2.count()
            
            # Schema comparison
            schema1 = df1.schema
            schema2 = df2.schema
            schema_changed = schema1 != schema2
            
            comparison = {
                "table_path": table_path,
                "version1": version1,
                "version2": version2,
                "record_counts": {
                    "version1": count1,
                    "version2": count2,
                    "difference": count2 - count1
                },
                "schema_changed": schema_changed,
                "comparison_timestamp": datetime.now().isoformat()
            }
            
            if schema_changed:
                comparison["schema_differences"] = {
                    "version1_fields": [field.name for field in schema1.fields],
                    "version2_fields": [field.name for field in schema2.fields]
                }
            
            self.logger.info(f"Compared versions {version1} and {version2} of {table_path}")
            return comparison
            
        except Exception as e:
            self.logger.error(f"Failed to compare versions: {e}")
            return {"error": str(e)}
    
    def get_table_changes_between_versions(self, table_path: str, start_version: int, end_version: int) -> Optional[DataFrame]:
        """Get changes between two versions of a table.
        
        Args:
            table_path: Path to Delta table
            start_version: Starting version
            end_version: Ending version
            
        Returns:
            DataFrame with changes or None if error
        """
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.error("Spark not available")
            return None
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return None
            
            # Read the Change Data Feed between versions
            changes_df = (self.spark.read
                         .format("delta")
                         .option("readChangeFeed", "true")
                         .option("startingVersion", start_version)
                         .option("endingVersion", end_version)
                         .load(table_path))
            
            self.logger.info(f"Retrieved changes between versions {start_version}-{end_version}")
            return changes_df
            
        except Exception as e:
            self.logger.error(f"Failed to get changes between versions: {e}")
            return None


class DeltaACIDTransactions:
    """Provides ACID transaction support for Delta Lake operations."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Delta ACID transaction manager.
        
        Args:
            spark_session: Spark session for Delta operations
        """
        self.logger = StructuredLogger("DeltaACIDTransactions")
        self.spark = spark_session
        
        if not SPARK_AVAILABLE or not self.spark:
            self.logger.warning("Spark not available - ACID transaction features disabled")
    
    def upsert_market_data(self, table_path: str, new_data: DataFrame, merge_keys: List[str]) -> Dict[str, Any]:
        """Perform UPSERT operation on market data table.
        
        Args:
            table_path: Path to Delta table
            new_data: DataFrame with new data to upsert
            merge_keys: Keys to use for merge condition
            
        Returns:
            Operation result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                self.logger.error(f"Path {table_path} is not a Delta table")
                return {"success": False, "message": "Not a Delta table"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Perform merge operation
            (delta_table.alias("target")
             .merge(new_data.alias("source"), merge_condition)
             .whenMatchedUpdateAll()  # Update all columns when matched
             .whenNotMatchedInsertAll()  # Insert when not matched
             .execute())
            
            # Get operation metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            result = {
                "success": True,
                "operation": "UPSERT",
                "table_path": table_path,
                "merge_keys": merge_keys,
                "metrics": {
                    "num_output_rows": operation_metrics.get("numOutputRows", 0),
                    "num_target_rows_inserted": operation_metrics.get("numTargetRowsInserted", 0),
                    "num_target_rows_updated": operation_metrics.get("numTargetRowsUpdated", 0),
                    "num_target_rows_deleted": operation_metrics.get("numTargetRowsDeleted", 0)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"UPSERT completed: {result['metrics']}")
            return result
            
        except Exception as e:
            self.logger.error(f"UPSERT operation failed: {e}")
            return {"success": False, "message": str(e)}
    
    def delete_records_by_condition(self, table_path: str, condition: str) -> Dict[str, Any]:
        """Delete records from Delta table based on condition.
        
        Args:
            table_path: Path to Delta table
            condition: SQL condition for deletion
            
        Returns:
            Operation result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"success": False, "message": "Not a Delta table"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Perform delete operation
            delta_table.delete(condition)
            
            # Get operation metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            result = {
                "success": True,
                "operation": "DELETE",
                "table_path": table_path,
                "condition": condition,
                "metrics": {
                    "num_deleted_rows": operation_metrics.get("numDeletedRows", 0),
                    "num_copied_rows": operation_metrics.get("numCopiedRows", 0)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"DELETE completed: {result['metrics']}")
            return result
            
        except Exception as e:
            self.logger.error(f"DELETE operation failed: {e}")
            return {"success": False, "message": str(e)}
    
    def update_records_by_condition(self, table_path: str, condition: str, updates: Dict[str, str]) -> Dict[str, Any]:
        """Update records in Delta table based on condition.
        
        Args:
            table_path: Path to Delta table
            condition: SQL condition for update
            updates: Dictionary of column -> value updates
            
        Returns:
            Operation result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"success": False, "message": "Not a Delta table"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Perform update operation
            delta_table.update(condition, updates)
            
            # Get operation metrics
            history = delta_table.history(1).collect()[0]
            operation_metrics = history["operationMetrics"]
            
            result = {
                "success": True,
                "operation": "UPDATE",
                "table_path": table_path,
                "condition": condition,
                "updates": updates,
                "metrics": {
                    "num_updated_rows": operation_metrics.get("numUpdatedRows", 0),
                    "num_copied_rows": operation_metrics.get("numCopiedRows", 0)
                },
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"UPDATE completed: {result['metrics']}")
            return result
            
        except Exception as e:
            self.logger.error(f"UPDATE operation failed: {e}")
            return {"success": False, "message": str(e)}
    
    def transaction_rollback_demo(self, table_path: str) -> Dict[str, Any]:
        """Demonstrate transaction rollback using Delta Lake restore.
        
        Args:
            table_path: Path to Delta table
            
        Returns:
            Demo result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"success": False, "message": "Not a Delta table"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Get current version
            history = delta_table.history(1).collect()
            if not history:
                return {"success": False, "message": "No history available"}
            
            current_version = history[0]["version"]
            
            # Record pre-operation state
            pre_count = delta_table.toDF().count()
            
            # Simulate a "bad" operation - delete some records
            delta_table.delete("close_price > 0")  # This would delete all records!
            
            # Check post-operation state
            post_count = delta_table.toDF().count()
            
            # Rollback to previous version
            time_travel = DeltaTimeTravel(self.spark)
            rollback_success = time_travel.restore_table_to_version(table_path, current_version)
            
            # Verify rollback
            final_count = delta_table.toDF().count() if rollback_success else 0
            
            result = {
                "success": rollback_success,
                "demo_scenario": "Bad DELETE operation rollback",
                "table_path": table_path,
                "original_version": current_version,
                "record_counts": {
                    "pre_operation": pre_count,
                    "post_bad_operation": post_count,
                    "post_rollback": final_count
                },
                "rollback_successful": final_count == pre_count,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(f"Transaction rollback demo completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Transaction rollback demo failed: {e}")
            return {"success": False, "message": str(e)}
    
    def concurrent_write_demo(self, table_path: str) -> Dict[str, Any]:
        """Demonstrate ACID properties with concurrent writes.
        
        Args:
            table_path: Path to Delta table
            
        Returns:
            Demo result dictionary
        """
        if not SPARK_AVAILABLE or not self.spark:
            return {"success": False, "message": "Spark not available"}
        
        try:
            # This is a simplified demo since true concurrency requires multiple Spark sessions
            # In practice, Delta Lake handles concurrent writes with optimistic concurrency control
            
            result = {
                "success": True,
                "demo_scenario": "Concurrent write simulation",
                "table_path": table_path,
                "acid_properties": {
                    "Atomicity": "All operations complete or none do",
                    "Consistency": "Data integrity maintained across operations",
                    "Isolation": "Concurrent operations don't interfere",
                    "Durability": "Committed changes persist through failures"
                },
                "delta_lake_features": {
                    "Optimistic Concurrency Control": "Handles concurrent writes automatically",
                    "Transaction Log": "Ensures atomic operations",
                    "Schema Evolution": "Safe schema changes",
                    "Time Travel": "Access to historical versions"
                },
                "vietnamese_market_benefits": {
                    "Data Consistency": "Ensures market data integrity across sources",
                    "Concurrent Strategy Updates": "Multiple strategies can update positions safely",
                    "Risk Management": "Atomic updates for position limits and exposure",
                    "Audit Trail": "Complete history of all data changes"
                },
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info("Concurrent write demo completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Concurrent write demo failed: {e}")
            return {"success": False, "message": str(e)}


class DeltaLakeManager:
    """High-level manager for Delta Lake operations with time travel and ACID support."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Delta Lake manager.
        
        Args:
            spark_session: Spark session for Delta operations
        """
        self.logger = StructuredLogger("DeltaLakeManager")
        self.spark = spark_session
        
        # Initialize components
        self.time_travel = DeltaTimeTravel(spark_session)
        self.acid_transactions = DeltaACIDTransactions(spark_session)
        
    def create_checkpoint(self, table_path: str, checkpoint_name: str) -> Dict[str, Any]:
        """Create a named checkpoint for a Delta table.
        
        Args:
            table_path: Path to Delta table
            checkpoint_name: Name for the checkpoint
            
        Returns:
            Checkpoint creation result
        """
        try:
            if not SPARK_AVAILABLE or not self.spark:
                return {"success": False, "message": "Spark not available"}
            
            # Get current version
            history = self.time_travel.get_table_history(table_path, 1)
            if history is None:
                return {"success": False, "message": "Failed to get table history"}
            
            current_version = history.collect()[0]["version"]
            
            # In practice, you would store checkpoint metadata in a separate table
            # For demo purposes, we'll just return the version info
            
            checkpoint_info = {
                "success": True,
                "checkpoint_name": checkpoint_name,
                "table_path": table_path,
                "version": current_version,
                "timestamp": datetime.now().isoformat(),
                "note": "In production, checkpoint metadata would be stored in a separate table"
            }
            
            self.logger.info(f"Created checkpoint '{checkpoint_name}' at version {current_version}")
            return checkpoint_info
            
        except Exception as e:
            self.logger.error(f"Failed to create checkpoint: {e}")
            return {"success": False, "message": str(e)}
    
    def restore_from_checkpoint(self, table_path: str, checkpoint_name: str, version: int) -> Dict[str, Any]:
        """Restore table from a named checkpoint.
        
        Args:
            table_path: Path to Delta table
            checkpoint_name: Name of checkpoint to restore from
            version: Version associated with checkpoint
            
        Returns:
            Restore operation result
        """
        try:
            success = self.time_travel.restore_table_to_version(table_path, version)
            
            result = {
                "success": success,
                "checkpoint_name": checkpoint_name,
                "table_path": table_path,
                "restored_version": version,
                "timestamp": datetime.now().isoformat()
            }
            
            if success:
                self.logger.info(f"Restored from checkpoint '{checkpoint_name}' to version {version}")
            else:
                self.logger.error(f"Failed to restore from checkpoint '{checkpoint_name}'")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to restore from checkpoint: {e}")
            return {"success": False, "message": str(e)}
    
    def get_table_metadata(self, table_path: str) -> Dict[str, Any]:
        """Get comprehensive metadata for Delta table.
        
        Args:
            table_path: Path to Delta table
            
        Returns:
            Table metadata dictionary
        """
        try:
            if not SPARK_AVAILABLE or not self.spark:
                return {"error": "Spark not available"}
            
            if not DeltaTable.isDeltaTable(self.spark, table_path):
                return {"error": "Not a Delta table"}
            
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            # Get table info
            df = delta_table.toDF()
            row_count = df.count()
            
            # Get history
            history = delta_table.history(10).collect()
            
            # Get schema
            schema_info = {
                "fields": [{"name": field.name, "type": str(field.dataType)} for field in df.schema.fields],
                "field_count": len(df.schema.fields)
            }
            
            metadata = {
                "table_path": table_path,
                "is_delta_table": True,
                "current_row_count": row_count,
                "schema": schema_info,
                "version_history": [
                    {
                        "version": row["version"],
                        "timestamp": str(row["timestamp"]),
                        "operation": row["operation"],
                        "operation_metrics": row["operationMetrics"]
                    }
                    for row in history
                ],
                "latest_version": history[0]["version"] if history else None,
                "creation_time": str(history[-1]["timestamp"]) if history else None,
                "metadata_timestamp": datetime.now().isoformat()
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to get table metadata: {e}")
            return {"error": str(e)}