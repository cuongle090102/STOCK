"""
Vietnamese Market Data Schema Management

This module defines standardized schemas for Vietnamese market data across
the Delta Lake bronze, silver, and gold layers.
"""

import sys
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    TimestampType, BooleanType, IntegerType, DecimalType
)


@dataclass
class MarketDataSchema:
    """Standard schema definitions for Vietnamese market data."""
    
    @staticmethod
    def bronze_market_data() -> StructType:
        """Schema for bronze layer raw market data."""
        return StructType([
            # Data source metadata
            StructField("source", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("data_timestamp", TimestampType(), False),
            StructField("sequence_id", LongType(), True),
            
            # Market data fields
            StructField("symbol", StringType(), False),
            StructField("exchange", StringType(), True),
            StructField("market_type", StringType(), True),  # "STOCK", "INDEX", "ETF"
            
            # OHLCV data
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), False),
            StructField("volume", LongType(), True),
            StructField("value", DoubleType(), True),  # Volume * Price in VND
            
            # Additional tick data
            StructField("bid_price", DoubleType(), True),
            StructField("ask_price", DoubleType(), True),
            StructField("bid_volume", LongType(), True),
            StructField("ask_volume", LongType(), True),
            
            # Trading session info
            StructField("trading_session", StringType(), True),  # "MORNING", "AFTERNOON", "ATC"
            StructField("is_trading_halt", BooleanType(), True),
            
            # Raw data payload (for debugging)
            StructField("raw_data", StringType(), True),
            
            # Data quality flags
            StructField("is_valid", BooleanType(), True),
            StructField("validation_errors", StringType(), True),
            
            # Partitioning fields
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
    
    @staticmethod
    def silver_market_data() -> StructType:
        """Schema for silver layer validated market data."""
        return StructType([
            # Standardized identifiers
            StructField("symbol", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("date", StringType(), False),  # YYYY-MM-DD format
            
            # Market classification
            StructField("exchange", StringType(), False),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("market_cap_category", StringType(), True),  # "LARGE", "MID", "SMALL"
            
            # Validated OHLCV data (VND)
            StructField("open_price", DoubleType(), False),
            StructField("high_price", DoubleType(), False),
            StructField("low_price", DoubleType(), False),
            StructField("close_price", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("value", DoubleType(), False),
            
            # Adjusted prices (for splits/dividends)
            StructField("adj_open", DoubleType(), True),
            StructField("adj_high", DoubleType(), True),
            StructField("adj_low", DoubleType(), True),
            StructField("adj_close", DoubleType(), False),
            StructField("adj_volume", LongType(), True),
            
            # Price changes
            StructField("price_change", DoubleType(), True),
            StructField("price_change_pct", DoubleType(), True),
            
            # Market microstructure
            StructField("vwap", DoubleType(), True),  # Volume Weighted Average Price
            StructField("trades_count", IntegerType(), True),
            StructField("avg_trade_size", DoubleType(), True),
            
            # Liquidity metrics
            StructField("bid_ask_spread", DoubleType(), True),
            StructField("bid_ask_spread_pct", DoubleType(), True),
            StructField("turnover_ratio", DoubleType(), True),
            
            # Vietnamese market specifics
            StructField("is_vn30", BooleanType(), False),
            StructField("foreign_ownership_pct", DoubleType(), True),
            StructField("foreign_ownership_limit_pct", DoubleType(), True),
            StructField("price_limit_up", DoubleType(), True),
            StructField("price_limit_down", DoubleType(), True),
            
            # Data quality metrics
            StructField("data_completeness_score", DoubleType(), False),
            StructField("data_accuracy_score", DoubleType(), False),
            StructField("source_reliability_score", DoubleType(), False),
            
            # Metadata
            StructField("last_updated", TimestampType(), False),
            StructField("data_sources", StringType(), False),  # JSON array of sources
            
            # Partitioning
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
    
    @staticmethod
    def gold_market_analytics() -> StructType:
        """Schema for gold layer analytics-ready data."""
        return StructType([
            # Identifiers
            StructField("symbol", StringType(), False),
            StructField("date", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            
            # Basic OHLCV
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("value", DoubleType(), False),
            StructField("vwap", DoubleType(), False),
            
            # Returns
            StructField("returns_1d", DoubleType(), True),
            StructField("returns_5d", DoubleType(), True),
            StructField("returns_21d", DoubleType(), True),
            StructField("returns_63d", DoubleType(), True),
            StructField("returns_252d", DoubleType(), True),
            
            # Technical indicators
            StructField("sma_5", DoubleType(), True),
            StructField("sma_10", DoubleType(), True),
            StructField("sma_20", DoubleType(), True),
            StructField("sma_50", DoubleType(), True),
            StructField("sma_200", DoubleType(), True),
            
            StructField("ema_12", DoubleType(), True),
            StructField("ema_26", DoubleType(), True),
            StructField("macd", DoubleType(), True),
            StructField("macd_signal", DoubleType(), True),
            StructField("macd_histogram", DoubleType(), True),
            
            StructField("rsi_14", DoubleType(), True),
            StructField("stoch_k", DoubleType(), True),
            StructField("stoch_d", DoubleType(), True),
            
            StructField("bb_upper", DoubleType(), True),
            StructField("bb_middle", DoubleType(), True),
            StructField("bb_lower", DoubleType(), True),
            StructField("bb_width", DoubleType(), True),
            StructField("bb_position", DoubleType(), True),
            
            # Volatility measures
            StructField("volatility_10d", DoubleType(), True),
            StructField("volatility_30d", DoubleType(), True),
            StructField("volatility_60d", DoubleType(), True),
            StructField("atr_14", DoubleType(), True),
            
            # Volume indicators
            StructField("volume_sma_20", DoubleType(), True),
            StructField("volume_ratio", DoubleType(), True),
            StructField("money_flow_index", DoubleType(), True),
            StructField("on_balance_volume", DoubleType(), True),
            
            # Market structure
            StructField("market_cap", DoubleType(), True),
            StructField("enterprise_value", DoubleType(), True),
            StructField("free_float_market_cap", DoubleType(), True),
            
            # Vietnamese market metrics
            StructField("vn_index_correlation_30d", DoubleType(), True),
            StructField("vn30_weight", DoubleType(), True),
            StructField("sector_relative_strength", DoubleType(), True),
            
            # Risk metrics
            StructField("beta_60d", DoubleType(), True),
            StructField("beta_252d", DoubleType(), True),
            StructField("sharpe_ratio_60d", DoubleType(), True),
            StructField("max_drawdown_60d", DoubleType(), True),
            
            # Momentum scores
            StructField("momentum_score_1m", DoubleType(), True),
            StructField("momentum_score_3m", DoubleType(), True),
            StructField("momentum_score_6m", DoubleType(), True),
            StructField("momentum_score_12m", DoubleType(), True),
            
            # Quality scores
            StructField("quality_score", DoubleType(), True),
            StructField("liquidity_score", DoubleType(), True),
            StructField("volatility_score", DoubleType(), True),
            
            # Partitioning
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
    
    @staticmethod
    def trading_signals() -> StructType:
        """Schema for trading signals data."""
        return StructType([
            # Signal identification
            StructField("signal_id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("strategy_name", StringType(), False),
            
            # Signal details
            StructField("signal_type", StringType(), False),  # "BUY", "SELL", "HOLD"
            StructField("confidence", DoubleType(), False),
            StructField("strength", DoubleType(), True),
            StructField("expected_return", DoubleType(), True),
            StructField("expected_risk", DoubleType(), True),
            
            # Position sizing
            StructField("suggested_weight", DoubleType(), True),
            StructField("max_position_size", DoubleType(), True),
            StructField("stop_loss", DoubleType(), True),
            StructField("take_profit", DoubleType(), True),
            
            # Market context
            StructField("market_price", DoubleType(), False),
            StructField("market_volume", LongType(), True),
            StructField("market_regime", StringType(), True),
            
            # Strategy parameters
            StructField("strategy_params", StringType(), True),  # JSON
            StructField("signal_features", StringType(), True),  # JSON
            
            # Metadata
            StructField("is_active", BooleanType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("expires_at", TimestampType(), True),
            
            # Partitioning
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
    
    @staticmethod
    def risk_events() -> StructType:
        """Schema for risk management events."""
        return StructType([
            # Event identification
            StructField("event_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("event_type", StringType(), False),
            StructField("severity", StringType(), False),  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
            
            # Affected entities
            StructField("symbol", StringType(), True),
            StructField("portfolio", StringType(), True),
            StructField("strategy", StringType(), True),
            
            # Risk metrics
            StructField("risk_metric", StringType(), False),
            StructField("current_value", DoubleType(), False),
            StructField("threshold_value", DoubleType(), False),
            StructField("breach_magnitude", DoubleType(), False),
            
            # Actions taken
            StructField("action_required", BooleanType(), False),
            StructField("action_taken", StringType(), True),
            StructField("action_timestamp", TimestampType(), True),
            
            # Event details
            StructField("description", StringType(), False),
            StructField("root_cause", StringType(), True),
            StructField("impact_assessment", StringType(), True),
            
            # Resolution
            StructField("is_resolved", BooleanType(), False),
            StructField("resolution_timestamp", TimestampType(), True),
            StructField("resolution_notes", StringType(), True),
            
            # Partitioning
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])


class SchemaRegistry:
    """Central registry for managing Delta Lake schemas."""
    
    def __init__(self):
        """Initialize schema registry."""
        self.logger = StructuredLogger("SchemaRegistry")
        self._schemas = {
            "bronze_market_data": MarketDataSchema.bronze_market_data(),
            "silver_market_data": MarketDataSchema.silver_market_data(),
            "gold_market_analytics": MarketDataSchema.gold_market_analytics(),
            "trading_signals": MarketDataSchema.trading_signals(),
            "risk_events": MarketDataSchema.risk_events()
        }
    
    def get_schema(self, schema_name: str) -> StructType:
        """Get schema by name.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            PySpark StructType schema
            
        Raises:
            KeyError: If schema not found
        """
        if schema_name not in self._schemas:
            available = list(self._schemas.keys())
            raise KeyError(f"Schema '{schema_name}' not found. Available: {available}")
        
        return self._schemas[schema_name]
    
    def list_schemas(self) -> List[str]:
        """List all available schema names."""
        return list(self._schemas.keys())
    
    def validate_data(self, data: Dict[str, Any], schema_name: str) -> bool:
        """Validate data against schema.
        
        Args:
            data: Data dictionary to validate
            schema_name: Name of schema to validate against
            
        Returns:
            True if data is valid
        """
        try:
            schema = self.get_schema(schema_name)
            
            # Check required fields
            required_fields = [field.name for field in schema.fields if not field.nullable]
            missing_fields = [field for field in required_fields if field not in data]
            
            if missing_fields:
                self.logger.warning(f"Missing required fields: {missing_fields}")
                return False
                
            # Type validation would be done by Spark when writing
            return True
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {e}")
            return False
    
    def get_schema_evolution_rules(self, schema_name: str) -> Dict[str, Any]:
        """Get schema evolution rules for backward compatibility."""
        rules = {
            "bronze_market_data": {
                "allow_add_columns": True,
                "allow_delete_columns": False,
                "allow_type_changes": False,
                "required_fields": ["source", "symbol", "close_price", "data_timestamp"]
            },
            "silver_market_data": {
                "allow_add_columns": True,
                "allow_delete_columns": False,
                "allow_type_changes": False,
                "required_fields": ["symbol", "timestamp", "close_price", "volume"]
            },
            "gold_market_analytics": {
                "allow_add_columns": True,
                "allow_delete_columns": True,
                "allow_type_changes": False,
                "required_fields": ["symbol", "date", "close", "volume"]
            }
        }
        
        return rules.get(schema_name, {
            "allow_add_columns": True,
            "allow_delete_columns": False,
            "allow_type_changes": False,
            "required_fields": []
        })