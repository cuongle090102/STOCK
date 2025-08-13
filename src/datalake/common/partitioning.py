"""
Delta Lake Partitioning Strategy for Vietnamese Market Data

This module implements optimized partitioning strategies for different data types
and access patterns in the Vietnamese algorithmic trading system.
"""

import sys
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from enum import Enum

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger


class PartitionType(Enum):
    """Supported partition types."""
    DATE = "date"
    SYMBOL = "symbol"
    YEAR_MONTH = "year_month"
    YEAR_MONTH_DAY = "year_month_day"
    HASH = "hash"
    RANGE = "range"


@dataclass
class PartitionConfig:
    """Configuration for table partitioning."""
    partition_columns: List[str]
    partition_type: PartitionType
    num_partitions: Optional[int] = None
    partition_ranges: Optional[Dict[str, Any]] = None
    retention_days: Optional[int] = None
    optimization_enabled: bool = True


class PartitionStrategy:
    """Implements partitioning strategies for Delta Lake tables."""
    
    def __init__(self):
        """Initialize partition strategy."""
        self.logger = StructuredLogger("PartitionStrategy")
        
        # VN30 symbols for optimized partitioning
        self.vn30_symbols = [
            "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
            "VHM", "TCB", "MWG", "VRE", "SAB", "NVL", "POW", "KDH", "TPB", "SSI",
            "VPB", "PDR", "STB", "HDB", "MBB", "ACB", "VJC", "VND", "GEX", "DGC"
        ]
    
    def get_bronze_partitioning(self) -> PartitionConfig:
        """Get partitioning strategy for bronze layer raw data.
        
        Bronze layer optimized for:
        - High write throughput
        - Time-based queries
        - Data retention management
        """
        return PartitionConfig(
            partition_columns=["year", "month", "day"],
            partition_type=PartitionType.YEAR_MONTH_DAY,
            retention_days=30,  # Keep 30 days of raw data
            optimization_enabled=True
        )
    
    def get_silver_partitioning(self) -> PartitionConfig:
        """Get partitioning strategy for silver layer validated data.
        
        Silver layer optimized for:
        - Analytical queries by date and symbol
        - Strategy backtesting (symbol-specific)
        - Data quality monitoring
        """
        return PartitionConfig(
            partition_columns=["year", "month"],
            partition_type=PartitionType.YEAR_MONTH,
            retention_days=365,  # Keep 1 year of validated data
            optimization_enabled=True
        )
    
    def get_gold_partitioning(self) -> PartitionConfig:
        """Get partitioning strategy for gold layer analytics data.
        
        Gold layer optimized for:
        - Dashboard queries
        - Strategy performance analysis
        - Risk management reporting
        """
        return PartitionConfig(
            partition_columns=["year", "month"],
            partition_type=PartitionType.YEAR_MONTH,
            retention_days=1095,  # Keep 3 years of analytics data
            optimization_enabled=True
        )
    
    def get_signals_partitioning(self) -> PartitionConfig:
        """Get partitioning strategy for trading signals data."""
        return PartitionConfig(
            partition_columns=["year", "month", "day"],
            partition_type=PartitionType.YEAR_MONTH_DAY,
            retention_days=90,  # Keep 3 months of signals
            optimization_enabled=True
        )
    
    def get_risk_events_partitioning(self) -> PartitionConfig:
        """Get partitioning strategy for risk events data."""
        return PartitionConfig(
            partition_columns=["year", "month"],
            partition_type=PartitionType.YEAR_MONTH,
            retention_days=730,  # Keep 2 years of risk events
            optimization_enabled=True
        )
    
    def calculate_partition_values(self, timestamp: datetime) -> Dict[str, int]:
        """Calculate partition values from timestamp.
        
        Args:
            timestamp: Timestamp to partition
            
        Returns:
            Dictionary with partition column values
        """
        return {
            "year": timestamp.year,
            "month": timestamp.month,
            "day": timestamp.day
        }
    
    def get_partition_path(self, base_path: str, partition_values: Dict[str, Any]) -> str:
        """Generate partition path from values.
        
        Args:
            base_path: Base table path
            partition_values: Partition column values
            
        Returns:
            Full partition path
        """
        partition_parts = []
        for key, value in partition_values.items():
            partition_parts.append(f"{key}={value}")
        
        partition_path = "/".join(partition_parts)
        return f"{base_path.rstrip('/')}/{partition_path}"
    
    def get_optimal_file_size_mb(self, table_type: str) -> int:
        """Get optimal file size for different table types.
        
        Args:
            table_type: Type of table (bronze, silver, gold)
            
        Returns:
            Optimal file size in MB
        """
        file_sizes = {
            "bronze": 128,    # Smaller files for high write throughput
            "silver": 256,    # Medium files for balanced read/write
            "gold": 512,      # Larger files for analytical queries
            "signals": 64,    # Small files for real-time data
            "risk_events": 32 # Very small files for alerts
        }
        
        return file_sizes.get(table_type, 128)
    
    def get_z_order_columns(self, table_type: str) -> List[str]:
        """Get Z-order optimization columns for table type.
        
        Args:
            table_type: Type of table
            
        Returns:
            List of columns to Z-order by
        """
        z_order_columns = {
            "bronze": ["symbol", "data_timestamp"],
            "silver": ["symbol", "timestamp"],
            "gold": ["symbol", "date"],
            "signals": ["symbol", "timestamp", "strategy_name"],
            "risk_events": ["event_type", "severity", "timestamp"]
        }
        
        return z_order_columns.get(table_type, ["symbol", "timestamp"])
    
    def should_optimize_table(self, table_type: str, hours_since_last_optimize: int) -> bool:
        """Determine if table should be optimized.
        
        Args:
            table_type: Type of table
            hours_since_last_optimize: Hours since last optimization
            
        Returns:
            True if optimization is recommended
        """
        optimization_intervals = {
            "bronze": 6,      # Optimize every 6 hours (high write volume)
            "silver": 12,     # Optimize every 12 hours
            "gold": 24,       # Optimize daily
            "signals": 4,     # Optimize every 4 hours (real-time data)
            "risk_events": 8  # Optimize every 8 hours
        }
        
        interval = optimization_intervals.get(table_type, 12)
        return hours_since_last_optimize >= interval
    
    def get_compaction_strategy(self, table_type: str) -> Dict[str, Any]:
        """Get compaction strategy for table type.
        
        Args:
            table_type: Type of table
            
        Returns:
            Compaction configuration
        """
        strategies = {
            "bronze": {
                "auto_compact": True,
                "target_file_size_mb": 128,
                "max_files_per_partition": 10,
                "compaction_trigger": "size_based"
            },
            "silver": {
                "auto_compact": True,
                "target_file_size_mb": 256,
                "max_files_per_partition": 5,
                "compaction_trigger": "time_based"
            },
            "gold": {
                "auto_compact": True,
                "target_file_size_mb": 512,
                "max_files_per_partition": 3,
                "compaction_trigger": "scheduled"
            },
            "signals": {
                "auto_compact": True,
                "target_file_size_mb": 64,
                "max_files_per_partition": 15,
                "compaction_trigger": "size_based"
            },
            "risk_events": {
                "auto_compact": False,  # Manual compaction for risk events
                "target_file_size_mb": 32,
                "max_files_per_partition": 20,
                "compaction_trigger": "manual"
            }
        }
        
        return strategies.get(table_type, strategies["silver"])
    
    def get_retention_policy(self, table_type: str) -> Dict[str, Any]:
        """Get data retention policy for table type.
        
        Args:
            table_type: Type of table
            
        Returns:
            Retention policy configuration
        """
        policies = {
            "bronze": {
                "retention_days": 30,
                "vacuum_interval_hours": 24,
                "archive_to_cold_storage": True,
                "cold_storage_after_days": 7
            },
            "silver": {
                "retention_days": 365,
                "vacuum_interval_hours": 72,
                "archive_to_cold_storage": True,
                "cold_storage_after_days": 90
            },
            "gold": {
                "retention_days": 1095,  # 3 years
                "vacuum_interval_hours": 168,  # Weekly
                "archive_to_cold_storage": False,
                "cold_storage_after_days": None
            },
            "signals": {
                "retention_days": 90,
                "vacuum_interval_hours": 48,
                "archive_to_cold_storage": True,
                "cold_storage_after_days": 30
            },
            "risk_events": {
                "retention_days": 730,  # 2 years
                "vacuum_interval_hours": 96,
                "archive_to_cold_storage": False,
                "cold_storage_after_days": None
            }
        }
        
        return policies.get(table_type, policies["silver"])
    
    def calculate_partition_pruning_predicates(
        self, 
        start_date: date, 
        end_date: date
    ) -> List[str]:
        """Calculate partition pruning predicates for date range.
        
        Args:
            start_date: Start date for query
            end_date: End date for query
            
        Returns:
            List of partition predicates
        """
        predicates = []
        
        # Year-based predicates
        years = list(range(start_date.year, end_date.year + 1))
        if len(years) == 1:
            predicates.append(f"year = {years[0]}")
        else:
            predicates.append(f"year IN ({', '.join(map(str, years))})")
        
        # Month-based predicates (more complex for date ranges)
        if start_date.year == end_date.year:
            months = list(range(start_date.month, end_date.month + 1))
            if len(months) == 1:
                predicates.append(f"month = {months[0]}")
            else:
                predicates.append(f"month IN ({', '.join(map(str, months))})")
        
        return predicates
    
    def estimate_partition_size(
        self, 
        symbols: List[str], 
        days: int, 
        records_per_symbol_per_day: int = 390
    ) -> Dict[str, Any]:
        """Estimate partition size and file count.
        
        Args:
            symbols: List of symbols
            days: Number of days
            records_per_symbol_per_day: Records per symbol per day
            
        Returns:
            Size estimation
        """
        total_records = len(symbols) * days * records_per_symbol_per_day
        
        # Assume average record size of 500 bytes
        avg_record_size_bytes = 500
        total_size_bytes = total_records * avg_record_size_bytes
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        # Estimate partitions (daily partitioning)
        num_partitions = days
        avg_partition_size_mb = total_size_mb / num_partitions
        
        # Estimate files per partition (target 256MB per file)
        target_file_size_mb = 256
        files_per_partition = max(1, int(avg_partition_size_mb / target_file_size_mb))
        total_files = num_partitions * files_per_partition
        
        return {
            "total_records": total_records,
            "total_size_mb": round(total_size_mb, 2),
            "num_partitions": num_partitions,
            "avg_partition_size_mb": round(avg_partition_size_mb, 2),
            "files_per_partition": files_per_partition,
            "total_files": total_files,
            "recommended_optimization": total_files > 100
        }
    
    def get_vietnamese_market_partition_hints(self) -> Dict[str, Any]:
        """Get Vietnamese market-specific partitioning hints."""
        return {
            "trading_hours": {
                "morning_session": "09:00-11:30",
                "afternoon_session": "13:00-15:00",
                "timezone": "Asia/Ho_Chi_Minh"
            },
            "market_holidays": [
                "New Year", "Tet Holiday", "Hung Kings Day", 
                "Liberation Day", "Labor Day", "National Day"
            ],
            "high_volume_symbols": self.vn30_symbols,
            "partition_recommendations": {
                "vn30_stocks": "Consider symbol-based sub-partitioning for VN30",
                "small_caps": "Use time-based partitioning only",
                "indices": "Separate table with minimal partitioning",
                "intraday_data": "Hourly sub-partitions during trading hours"
            },
            "optimization_schedule": {
                "trading_hours": "Light optimization only",
                "after_market": "Full optimization and compaction",
                "weekends": "Major maintenance and vacuum operations"
            }
        }