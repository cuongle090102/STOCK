"""
Vietnamese Market Data Lake Package

This package implements a three-layer Delta Lake architecture for Vietnamese market data:
- Bronze Layer: Raw data ingestion with schema enforcement
- Silver Layer: Validated and cleansed data
- Gold Layer: Analytics-ready aggregated data

The architecture follows medallion architecture principles with ACID transactions
and time travel capabilities powered by Delta Lake.
"""

from .bronze.ingestion import BronzeDataIngestion
from .silver.validation import SilverDataProcessor  
from .gold.analytics import GoldDataMart
from .common.schemas import MarketDataSchema, SchemaRegistry
from .common.partitioning import PartitionStrategy
from .common.quality import DataQualityValidator

__all__ = [
    "BronzeDataIngestion",
    "SilverDataProcessor", 
    "GoldDataMart",
    "MarketDataSchema",
    "SchemaRegistry",
    "PartitionStrategy",
    "DataQualityValidator"
]

__version__ = "1.0.0"