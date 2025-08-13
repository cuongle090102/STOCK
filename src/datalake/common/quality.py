"""
Data Quality Validation Framework for Vietnamese Market Data

This module implements comprehensive data quality checks and validation
rules specific to Vietnamese market data characteristics.
"""

import sys
from datetime import datetime, date, time
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
import json
import re

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logging import StructuredLogger


class ValidationLevel(Enum):
    """Data validation severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationRule(Enum):
    """Types of validation rules."""
    RANGE_CHECK = "range_check"
    FORMAT_CHECK = "format_check"
    BUSINESS_RULE = "business_rule"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    FRESHNESS = "freshness"
    ACCURACY = "accuracy"


@dataclass
class ValidationResult:
    """Result of a data quality validation."""
    rule_name: str
    rule_type: ValidationRule
    level: ValidationLevel
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    affected_records: int = 0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    table_name: str
    validation_timestamp: datetime
    total_records: int
    passed_validations: int
    failed_validations: int
    quality_score: float
    validation_results: List[ValidationResult] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)


class DataQualityValidator:
    """Comprehensive data quality validator for Vietnamese market data."""
    
    def __init__(self):
        """Initialize data quality validator."""
        self.logger = StructuredLogger("DataQualityValidator")
        
        # Vietnamese market constants
        self.vn30_symbols = [
            "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
            "VHM", "TCB", "MWG", "VRE", "SAB", "NVL", "POW", "KDH", "TPB", "SSI",
            "VPB", "PDR", "STB", "HDB", "MBB", "ACB", "VJC", "VND", "GEX", "DGC"
        ]
        
        self.vietnamese_exchanges = ["HOSE", "HNX", "UPCOM"]
        
        # Trading hours in Vietnam
        self.trading_sessions = {
            "morning": {"start": time(9, 0), "end": time(11, 30)},
            "afternoon": {"start": time(13, 0), "end": time(15, 0)},
            "atc": {"start": time(14, 45), "end": time(15, 0)}
        }
        
        # Price limits (±7% for most stocks)
        self.daily_price_limit = 0.07
        
        # Initialize validation rules
        self.validation_rules = self._initialize_validation_rules()
    
    def _initialize_validation_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize validation rules for different data types."""
        return {
            "symbol_format": {
                "rule_type": ValidationRule.FORMAT_CHECK,
                "level": ValidationLevel.ERROR,
                "pattern": r"^[A-Z]{3,4}$",
                "description": "Symbol must be 3-4 uppercase letters"
            },
            "price_positive": {
                "rule_type": ValidationRule.RANGE_CHECK,
                "level": ValidationLevel.ERROR,
                "min_value": 0,
                "description": "Prices must be positive"
            },
            "price_precision": {
                "rule_type": ValidationRule.FORMAT_CHECK,
                "level": ValidationLevel.WARNING,
                "max_decimals": 0,
                "description": "VND prices should have no decimal places"
            },
            "volume_non_negative": {
                "rule_type": ValidationRule.RANGE_CHECK,
                "level": ValidationLevel.ERROR,
                "min_value": 0,
                "description": "Volume must be non-negative"
            },
            "ohlc_consistency": {
                "rule_type": ValidationRule.BUSINESS_RULE,
                "level": ValidationLevel.ERROR,
                "description": "OHLC prices must satisfy: Low <= Open,Close <= High"
            },
            "price_limit_check": {
                "rule_type": ValidationRule.BUSINESS_RULE,
                "level": ValidationLevel.WARNING,
                "description": "Price changes should respect daily limits"
            },
            "trading_hours": {
                "rule_type": ValidationRule.BUSINESS_RULE,
                "level": ValidationLevel.WARNING,
                "description": "Trades should occur during trading hours"
            },
            "data_freshness": {
                "rule_type": ValidationRule.FRESHNESS,
                "level": ValidationLevel.WARNING,
                "max_delay_minutes": 30,
                "description": "Data should be recent"
            },
            "completeness_required": {
                "rule_type": ValidationRule.COMPLETENESS,
                "level": ValidationLevel.ERROR,
                "required_fields": ["symbol", "timestamp", "close_price"],
                "description": "Required fields must not be null"
            },
            "completeness_optional": {
                "rule_type": ValidationRule.COMPLETENESS,
                "level": ValidationLevel.WARNING,
                "optional_fields": ["volume", "open_price", "high_price", "low_price"],
                "threshold": 0.95,
                "description": "Optional fields should be >95% complete"
            }
        }
    
    def validate_bronze_data(self, data: List[Dict[str, Any]]) -> QualityReport:
        """Validate bronze layer raw market data.
        
        Args:
            data: List of raw market data records
            
        Returns:
            Quality validation report
        """
        if not data:
            return self._create_empty_report("bronze_market_data")
        
        validation_results = []
        total_records = len(data)
        
        # Symbol format validation
        validation_results.extend(self._validate_symbol_format(data))
        
        # Price validations
        validation_results.extend(self._validate_price_fields(data))
        
        # Volume validation
        validation_results.extend(self._validate_volume_fields(data))
        
        # OHLC consistency
        validation_results.extend(self._validate_ohlc_consistency(data))
        
        # Completeness validation
        validation_results.extend(self._validate_data_completeness(data))
        
        # Freshness validation
        validation_results.extend(self._validate_data_freshness(data))
        
        # Business rule validations
        validation_results.extend(self._validate_trading_hours(data))
        validation_results.extend(self._validate_price_limits(data))
        
        return self._create_quality_report("bronze_market_data", total_records, validation_results)
    
    def validate_silver_data(self, data: List[Dict[str, Any]]) -> QualityReport:
        """Validate silver layer processed data.
        
        Args:
            data: List of processed market data records
            
        Returns:
            Quality validation report
        """
        if not data:
            return self._create_empty_report("silver_market_data")
        
        validation_results = []
        total_records = len(data)
        
        # All bronze validations plus additional silver-specific checks
        validation_results.extend(self._validate_symbol_format(data))
        validation_results.extend(self._validate_price_fields(data))
        validation_results.extend(self._validate_volume_fields(data))
        validation_results.extend(self._validate_ohlc_consistency(data))
        validation_results.extend(self._validate_data_completeness(data, level="silver"))
        
        # Silver-specific validations
        validation_results.extend(self._validate_calculated_fields(data))
        validation_results.extend(self._validate_data_consistency(data))
        validation_results.extend(self._validate_market_classification(data))
        
        return self._create_quality_report("silver_market_data", total_records, validation_results)
    
    def validate_gold_data(self, data: List[Dict[str, Any]]) -> QualityReport:
        """Validate gold layer analytics data.
        
        Args:
            data: List of analytics-ready data records
            
        Returns:
            Quality validation report
        """
        if not data:
            return self._create_empty_report("gold_market_analytics")
        
        validation_results = []
        total_records = len(data)
        
        # Basic validations
        validation_results.extend(self._validate_symbol_format(data))
        validation_results.extend(self._validate_data_completeness(data, level="gold"))
        
        # Gold-specific validations
        validation_results.extend(self._validate_technical_indicators(data))
        validation_results.extend(self._validate_return_calculations(data))
        validation_results.extend(self._validate_risk_metrics(data))
        validation_results.extend(self._validate_analytics_consistency(data))
        
        return self._create_quality_report("gold_market_analytics", total_records, validation_results)
    
    def _validate_symbol_format(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate symbol format."""
        results = []
        rule = self.validation_rules["symbol_format"]
        pattern = re.compile(rule["pattern"])
        
        invalid_symbols = []
        for record in data:
            symbol = record.get("symbol", "")
            if not pattern.match(str(symbol)):
                invalid_symbols.append(symbol)
        
        if invalid_symbols:
            results.append(ValidationResult(
                rule_name="symbol_format",
                rule_type=rule["rule_type"],
                level=rule["level"],
                passed=False,
                message=f"Found {len(invalid_symbols)} invalid symbol formats",
                details={"invalid_symbols": invalid_symbols[:10]},  # First 10 examples
                affected_records=len(invalid_symbols)
            ))
        else:
            results.append(ValidationResult(
                rule_name="symbol_format",
                rule_type=rule["rule_type"],
                level=rule["level"],
                passed=True,
                message="All symbols have valid format",
                affected_records=0
            ))
        
        return results
    
    def _validate_price_fields(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate price field values."""
        results = []
        price_fields = ["open_price", "high_price", "low_price", "close_price", "bid_price", "ask_price"]
        
        for field in price_fields:
            negative_prices = 0
            non_integer_prices = 0
            
            for record in data:
                price = record.get(field)
                if price is not None:
                    if price < 0:
                        negative_prices += 1
                    if isinstance(price, float) and price != int(price):
                        non_integer_prices += 1
            
            # Negative price check
            if negative_prices > 0:
                results.append(ValidationResult(
                    rule_name=f"{field}_positive",
                    rule_type=ValidationRule.RANGE_CHECK,
                    level=ValidationLevel.ERROR,
                    passed=False,
                    message=f"Found {negative_prices} negative {field} values",
                    affected_records=negative_prices
                ))
            else:
                results.append(ValidationResult(
                    rule_name=f"{field}_positive",
                    rule_type=ValidationRule.RANGE_CHECK,
                    level=ValidationLevel.ERROR,
                    passed=True,
                    message=f"All {field} values are positive",
                    affected_records=0
                ))
            
            # VND precision check (should be integers)
            if non_integer_prices > 0:
                results.append(ValidationResult(
                    rule_name=f"{field}_precision",
                    rule_type=ValidationRule.FORMAT_CHECK,
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"Found {non_integer_prices} non-integer {field} values (VND should be whole numbers)",
                    affected_records=non_integer_prices
                ))
        
        return results
    
    def _validate_volume_fields(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate volume field values."""
        results = []
        volume_fields = ["volume", "bid_volume", "ask_volume"]
        
        for field in volume_fields:
            negative_volumes = 0
            
            for record in data:
                volume = record.get(field)
                if volume is not None and volume < 0:
                    negative_volumes += 1
            
            if negative_volumes > 0:
                results.append(ValidationResult(
                    rule_name=f"{field}_non_negative",
                    rule_type=ValidationRule.RANGE_CHECK,
                    level=ValidationLevel.ERROR,
                    passed=False,
                    message=f"Found {negative_volumes} negative {field} values",
                    affected_records=negative_volumes
                ))
            else:
                results.append(ValidationResult(
                    rule_name=f"{field}_non_negative",
                    rule_type=ValidationRule.RANGE_CHECK,
                    level=ValidationLevel.ERROR,
                    passed=True,
                    message=f"All {field} values are non-negative",
                    affected_records=0
                ))
        
        return results
    
    def _validate_ohlc_consistency(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate OHLC price consistency."""
        results = []
        inconsistent_records = 0
        
        for record in data:
            open_price = record.get("open_price")
            high_price = record.get("high_price")
            low_price = record.get("low_price")
            close_price = record.get("close_price")
            
            # Skip if any required price is missing
            if any(p is None for p in [open_price, high_price, low_price, close_price]):
                continue
            
            # Check OHLC consistency: Low <= Open,Close <= High
            if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
                inconsistent_records += 1
        
        if inconsistent_records > 0:
            results.append(ValidationResult(
                rule_name="ohlc_consistency",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.ERROR,
                passed=False,
                message=f"Found {inconsistent_records} records with inconsistent OHLC prices",
                affected_records=inconsistent_records
            ))
        else:
            results.append(ValidationResult(
                rule_name="ohlc_consistency",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.ERROR,
                passed=True,
                message="All OHLC prices are consistent",
                affected_records=0
            ))
        
        return results
    
    def _validate_data_completeness(self, data: List[Dict[str, Any]], level: str = "bronze") -> List[ValidationResult]:
        """Validate data completeness."""
        results = []
        
        required_fields = {
            "bronze": ["source", "symbol", "close_price", "data_timestamp"],
            "silver": ["symbol", "timestamp", "close_price", "volume"],
            "gold": ["symbol", "date", "close", "volume"]
        }
        
        optional_fields = {
            "bronze": ["open_price", "high_price", "low_price", "volume"],
            "silver": ["open_price", "high_price", "low_price", "vwap"],
            "gold": ["returns_1d", "sma_20", "rsi_14", "volatility_30d"]
        }
        
        fields_to_check = required_fields.get(level, required_fields["bronze"])
        optional_to_check = optional_fields.get(level, optional_fields["bronze"])
        
        total_records = len(data)
        
        # Check required fields
        for field in fields_to_check:
            missing_count = sum(1 for record in data if record.get(field) is None)
            
            if missing_count > 0:
                results.append(ValidationResult(
                    rule_name=f"{field}_required",
                    rule_type=ValidationRule.COMPLETENESS,
                    level=ValidationLevel.ERROR,
                    passed=False,
                    message=f"Required field '{field}' missing in {missing_count} records",
                    affected_records=missing_count
                ))
            else:
                results.append(ValidationResult(
                    rule_name=f"{field}_required",
                    rule_type=ValidationRule.COMPLETENESS,
                    level=ValidationLevel.ERROR,
                    passed=True,
                    message=f"Required field '{field}' is complete",
                    affected_records=0
                ))
        
        # Check optional fields completeness
        for field in optional_to_check:
            missing_count = sum(1 for record in data if record.get(field) is None)
            completeness_rate = (total_records - missing_count) / total_records if total_records > 0 else 0
            
            if completeness_rate < 0.95:  # Less than 95% complete
                results.append(ValidationResult(
                    rule_name=f"{field}_completeness",
                    rule_type=ValidationRule.COMPLETENESS,
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message=f"Optional field '{field}' only {completeness_rate:.1%} complete",
                    details={"completeness_rate": completeness_rate},
                    affected_records=missing_count
                ))
        
        return results
    
    def _validate_data_freshness(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate data freshness."""
        results = []
        current_time = datetime.now()
        stale_records = 0
        
        for record in data:
            # Check ingestion timestamp or data timestamp
            timestamp_field = record.get("ingestion_timestamp") or record.get("data_timestamp") or record.get("timestamp")
            
            if timestamp_field:
                if isinstance(timestamp_field, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
                    except:
                        continue
                else:
                    timestamp = timestamp_field
                
                age_minutes = (current_time - timestamp).total_seconds() / 60
                if age_minutes > 30:  # More than 30 minutes old
                    stale_records += 1
        
        if stale_records > 0:
            results.append(ValidationResult(
                rule_name="data_freshness",
                rule_type=ValidationRule.FRESHNESS,
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Found {stale_records} stale records (>30 minutes old)",
                affected_records=stale_records
            ))
        else:
            results.append(ValidationResult(
                rule_name="data_freshness",
                rule_type=ValidationRule.FRESHNESS,
                level=ValidationLevel.WARNING,
                passed=True,
                message="All data is fresh (<30 minutes old)",
                affected_records=0
            ))
        
        return results
    
    def _validate_trading_hours(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate that trades occur during trading hours."""
        results = []
        outside_hours = 0
        
        for record in data:
            timestamp_field = record.get("data_timestamp") or record.get("timestamp")
            if not timestamp_field:
                continue
            
            if isinstance(timestamp_field, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
                except:
                    continue
            else:
                timestamp = timestamp_field
            
            trade_time = timestamp.time()
            
            # Check if time falls within trading sessions
            in_trading_hours = (
                self.trading_sessions["morning"]["start"] <= trade_time <= self.trading_sessions["morning"]["end"] or
                self.trading_sessions["afternoon"]["start"] <= trade_time <= self.trading_sessions["afternoon"]["end"]
            )
            
            if not in_trading_hours:
                outside_hours += 1
        
        if outside_hours > 0:
            results.append(ValidationResult(
                rule_name="trading_hours",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Found {outside_hours} trades outside trading hours",
                affected_records=outside_hours
            ))
        else:
            results.append(ValidationResult(
                rule_name="trading_hours",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.WARNING,
                passed=True,
                message="All trades are within trading hours",
                affected_records=0
            ))
        
        return results
    
    def _validate_price_limits(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate price changes against daily limits."""
        results = []
        # This would need previous day's closing price for proper validation
        # For now, just return a placeholder
        
        results.append(ValidationResult(
            rule_name="price_limits",
            rule_type=ValidationRule.BUSINESS_RULE,
            level=ValidationLevel.INFO,
            passed=True,
            message="Price limit validation requires historical reference data",
            affected_records=0
        ))
        
        return results
    
    def _validate_calculated_fields(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate calculated fields in silver data."""
        results = []
        # Placeholder for calculated field validations
        
        results.append(ValidationResult(
            rule_name="calculated_fields",
            rule_type=ValidationRule.ACCURACY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Calculated field validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _validate_data_consistency(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate data consistency across sources."""
        results = []
        # Placeholder for consistency validations
        
        results.append(ValidationResult(
            rule_name="data_consistency",
            rule_type=ValidationRule.CONSISTENCY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Cross-source consistency validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _validate_market_classification(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate market classification fields."""
        results = []
        invalid_exchanges = 0
        
        for record in data:
            exchange = record.get("exchange")
            if exchange and exchange not in self.vietnamese_exchanges:
                invalid_exchanges += 1
        
        if invalid_exchanges > 0:
            results.append(ValidationResult(
                rule_name="exchange_validation",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.WARNING,
                passed=False,
                message=f"Found {invalid_exchanges} records with invalid exchange codes",
                affected_records=invalid_exchanges
            ))
        else:
            results.append(ValidationResult(
                rule_name="exchange_validation",
                rule_type=ValidationRule.BUSINESS_RULE,
                level=ValidationLevel.WARNING,
                passed=True,
                message="All exchange codes are valid",
                affected_records=0
            ))
        
        return results
    
    def _validate_technical_indicators(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate technical indicator calculations."""
        results = []
        # Placeholder for technical indicator validations
        
        results.append(ValidationResult(
            rule_name="technical_indicators",
            rule_type=ValidationRule.ACCURACY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Technical indicator validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _validate_return_calculations(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate return calculations."""
        results = []
        # Placeholder for return calculation validations
        
        results.append(ValidationResult(
            rule_name="return_calculations",
            rule_type=ValidationRule.ACCURACY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Return calculation validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _validate_risk_metrics(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate risk metric calculations."""
        results = []
        # Placeholder for risk metric validations
        
        results.append(ValidationResult(
            rule_name="risk_metrics",
            rule_type=ValidationRule.ACCURACY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Risk metric validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _validate_analytics_consistency(self, data: List[Dict[str, Any]]) -> List[ValidationResult]:
        """Validate analytics data consistency."""
        results = []
        # Placeholder for analytics consistency validations
        
        results.append(ValidationResult(
            rule_name="analytics_consistency",
            rule_type=ValidationRule.CONSISTENCY,
            level=ValidationLevel.INFO,
            passed=True,
            message="Analytics consistency validation not implemented",
            affected_records=0
        ))
        
        return results
    
    def _create_empty_report(self, table_name: str) -> QualityReport:
        """Create empty quality report for no data."""
        return QualityReport(
            table_name=table_name,
            validation_timestamp=datetime.now(),
            total_records=0,
            passed_validations=0,
            failed_validations=0,
            quality_score=0.0,
            validation_results=[],
            summary={"message": "No data to validate"}
        )
    
    def _create_quality_report(
        self, 
        table_name: str, 
        total_records: int, 
        validation_results: List[ValidationResult]
    ) -> QualityReport:
        """Create comprehensive quality report."""
        passed_validations = sum(1 for result in validation_results if result.passed)
        failed_validations = len(validation_results) - passed_validations
        
        # Calculate quality score (0-1)
        if len(validation_results) == 0:
            quality_score = 0.0
        else:
            # Weight by severity
            weighted_score = 0
            total_weight = 0
            
            for result in validation_results:
                weight = {
                    ValidationLevel.CRITICAL: 4,
                    ValidationLevel.ERROR: 3,
                    ValidationLevel.WARNING: 2,
                    ValidationLevel.INFO: 1
                }.get(result.level, 1)
                
                total_weight += weight
                if result.passed:
                    weighted_score += weight
            
            quality_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        # Create summary
        summary = {
            "quality_score": quality_score,
            "total_records": total_records,
            "validation_summary": {
                "critical_failures": sum(1 for r in validation_results if r.level == ValidationLevel.CRITICAL and not r.passed),
                "error_failures": sum(1 for r in validation_results if r.level == ValidationLevel.ERROR and not r.passed),
                "warning_failures": sum(1 for r in validation_results if r.level == ValidationLevel.WARNING and not r.passed),
                "info_items": sum(1 for r in validation_results if r.level == ValidationLevel.INFO)
            },
            "affected_records": sum(r.affected_records for r in validation_results if not r.passed)
        }
        
        return QualityReport(
            table_name=table_name,
            validation_timestamp=datetime.now(),
            total_records=total_records,
            passed_validations=passed_validations,
            failed_validations=failed_validations,
            quality_score=quality_score,
            validation_results=validation_results,
            summary=summary
        )