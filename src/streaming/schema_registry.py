"""Schema Registry integration for Vietnamese market data streaming."""

import json
import requests
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass

from src.utils.logging import StructuredLogger


class SchemaType(Enum):
    """Schema types supported by Schema Registry."""
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


@dataclass
class SchemaVersion:
    """Schema version information."""
    id: int
    version: int
    schema: str
    schema_type: SchemaType
    subject: str


class SchemaRegistryClient:
    """Client for Confluent Schema Registry."""
    
    def __init__(self, base_url: str = "http://localhost:8081"):
        """Initialize Schema Registry client.
        
        Args:
            base_url: Schema Registry base URL
        """
        self.base_url = base_url.rstrip('/')
        self.logger = StructuredLogger("SchemaRegistry")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/vnd.schemaregistry.v1+json"
        })
    
    def is_healthy(self) -> bool:
        """Check if Schema Registry is healthy.
        
        Returns:
            True if healthy
        """
        try:
            response = self.session.get(f"{self.base_url}/subjects")
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Schema Registry health check failed: {e}")
            return False
    
    def get_subjects(self) -> List[str]:
        """Get all subjects in the registry.
        
        Returns:
            List of subject names
        """
        try:
            response = self.session.get(f"{self.base_url}/subjects")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get subjects: {e}")
            return []
    
    def register_schema(
        self, 
        subject: str, 
        schema: Dict[str, Any], 
        schema_type: SchemaType = SchemaType.JSON
    ) -> Optional[int]:
        """Register a new schema.
        
        Args:
            subject: Subject name
            schema: Schema definition
            schema_type: Schema type
            
        Returns:
            Schema ID if successful
        """
        try:
            payload = {
                "schema": json.dumps(schema),
                "schemaType": schema_type.value
            }
            
            response = self.session.post(
                f"{self.base_url}/subjects/{subject}/versions",
                json=payload
            )
            response.raise_for_status()
            
            result = response.json()
            schema_id = result.get("id")
            
            self.logger.info(f"Registered schema for subject '{subject}' with ID {schema_id}")
            return schema_id
            
        except Exception as e:
            self.logger.error(f"Failed to register schema for '{subject}': {e}")
            return None
    
    def get_latest_schema(self, subject: str) -> Optional[SchemaVersion]:
        """Get the latest schema for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            SchemaVersion object if found
        """
        try:
            response = self.session.get(f"{self.base_url}/subjects/{subject}/versions/latest")
            response.raise_for_status()
            
            data = response.json()
            
            return SchemaVersion(
                id=data["id"],
                version=data["version"],
                schema=data["schema"],
                schema_type=SchemaType(data.get("schemaType", "JSON")),
                subject=data["subject"]
            )
            
        except Exception as e:
            self.logger.error(f"Failed to get latest schema for '{subject}': {e}")
            return None
    
    def get_schema_by_id(self, schema_id: int) -> Optional[str]:
        """Get schema by ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema string if found
        """
        try:
            response = self.session.get(f"{self.base_url}/schemas/ids/{schema_id}")
            response.raise_for_status()
            
            data = response.json()
            return data.get("schema")
            
        except Exception as e:
            self.logger.error(f"Failed to get schema with ID {schema_id}: {e}")
            return None
    
    def delete_subject(self, subject: str, permanent: bool = False) -> bool:
        """Delete a subject.
        
        Args:
            subject: Subject name
            permanent: Whether to permanently delete
            
        Returns:
            True if successful
        """
        try:
            url = f"{self.base_url}/subjects/{subject}"
            if permanent:
                url += "?permanent=true"
            
            response = self.session.delete(url)
            response.raise_for_status()
            
            self.logger.info(f"Deleted subject '{subject}' (permanent: {permanent})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to delete subject '{subject}': {e}")
            return False
    
    def check_compatibility(self, subject: str, schema: Dict[str, Any]) -> bool:
        """Check if schema is compatible with the subject.
        
        Args:
            subject: Subject name
            schema: Schema to check
            
        Returns:
            True if compatible
        """
        try:
            payload = {
                "schema": json.dumps(schema)
            }
            
            response = self.session.post(
                f"{self.base_url}/compatibility/subjects/{subject}/versions/latest",
                json=payload
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get("is_compatible", False)
            
        except Exception as e:
            self.logger.error(f"Failed to check compatibility for '{subject}': {e}")
            return False


class VietnameseMarketSchemas:
    """Schema definitions for Vietnamese market data."""
    
    @staticmethod
    def get_market_data_schema() -> Dict[str, Any]:
        """Get market data message schema.
        
        Returns:
            JSON schema for market data messages
        """
        return {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Data source name (VNDirect, SSI, TCBS)"
                },
                "symbol": {
                    "type": "string",
                    "pattern": "^[A-Z]{3,4}$",
                    "description": "Vietnamese stock symbol"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Message timestamp"
                },
                "message_type": {
                    "type": "string",
                    "enum": ["tick", "bar", "index", "signal", "risk"],
                    "description": "Type of market data message"
                },
                "sequence_id": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Message sequence ID for ordering"
                },
                "data": {
                    "type": "object",
                    "properties": {
                        "open_price": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Opening price in VND"
                        },
                        "high_price": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Highest price in VND"
                        },
                        "low_price": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Lowest price in VND"
                        },
                        "close_price": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Closing price in VND"
                        },
                        "volume": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Trading volume"
                        },
                        "value": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Trading value in VND"
                        }
                    },
                    "required": ["close_price", "volume"],
                    "additionalProperties": True
                }
            },
            "required": ["source", "symbol", "timestamp", "message_type", "data"],
            "additionalProperties": False
        }
    
    @staticmethod
    def get_trading_signal_schema() -> Dict[str, Any]:
        """Get trading signal schema.
        
        Returns:
            JSON schema for trading signals
        """
        return {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Strategy or signal source name"
                },
                "symbol": {
                    "type": "string",
                    "pattern": "^[A-Z]{3,4}$",
                    "description": "Vietnamese stock symbol"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "Signal timestamp"
                },
                "message_type": {
                    "type": "string",
                    "const": "signal"
                },
                "sequence_id": {
                    "type": "integer",
                    "minimum": 0
                },
                "data": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["BUY", "SELL", "HOLD"],
                            "description": "Trading action"
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 1,
                            "description": "Signal confidence score"
                        },
                        "price": {
                            "type": "number",
                            "minimum": 0,
                            "description": "Target price in VND"
                        },
                        "quantity": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Recommended quantity"
                        },
                        "strategy": {
                            "type": "string",
                            "description": "Strategy name"
                        },
                        "indicators": {
                            "type": "object",
                            "description": "Technical indicators that generated the signal",
                            "additionalProperties": True
                        }
                    },
                    "required": ["action", "confidence"],
                    "additionalProperties": True
                }
            },
            "required": ["source", "symbol", "timestamp", "message_type", "data"],
            "additionalProperties": False
        }
    
    @staticmethod
    def get_risk_event_schema() -> Dict[str, Any]:
        """Get risk event schema.
        
        Returns:
            JSON schema for risk events
        """
        return {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "const": "risk_manager"
                },
                "symbol": {
                    "type": "string",
                    "description": "Symbol or PORTFOLIO for portfolio-level events"
                },
                "timestamp": {
                    "type": "string",
                    "format": "date-time"
                },
                "message_type": {
                    "type": "string",
                    "const": "risk"
                },
                "sequence_id": {
                    "type": "integer",
                    "minimum": 0
                },
                "data": {
                    "type": "object",
                    "properties": {
                        "event_type": {
                            "type": "string",
                            "enum": [
                                "POSITION_LIMIT_BREACH",
                                "STOP_LOSS_TRIGGERED", 
                                "DRAWDOWN_ALERT",
                                "VOLATILITY_SPIKE",
                                "CORRELATION_BREAKDOWN",
                                "LIQUIDITY_WARNING"
                            ],
                            "description": "Type of risk event"
                        },
                        "severity": {
                            "type": "string",
                            "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                            "description": "Event severity level"
                        },
                        "details": {
                            "type": "object",
                            "description": "Event-specific details",
                            "additionalProperties": True
                        },
                        "metrics": {
                            "type": "object",
                            "description": "Risk metrics at time of event",
                            "properties": {
                                "var_1d": {"type": "number"},
                                "var_5d": {"type": "number"},
                                "drawdown": {"type": "number"},
                                "volatility": {"type": "number"},
                                "beta": {"type": "number"}
                            },
                            "additionalProperties": True
                        }
                    },
                    "required": ["event_type", "severity"],
                    "additionalProperties": True
                }
            },
            "required": ["source", "symbol", "timestamp", "message_type", "data"],
            "additionalProperties": False
        }


class SchemaManager:
    """Manager for Vietnamese market data schemas."""
    
    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        """Initialize schema manager.
        
        Args:
            schema_registry_url: Schema Registry URL
        """
        self.client = SchemaRegistryClient(schema_registry_url)
        self.logger = StructuredLogger("SchemaManager")
        self.schemas = VietnameseMarketSchemas()
    
    def setup_all_schemas(self) -> bool:
        """Setup all Vietnamese market data schemas.
        
        Returns:
            True if all schemas were registered successfully
        """
        self.logger.info("Setting up Vietnamese market data schemas")
        
        if not self.client.is_healthy():
            self.logger.error("Schema Registry is not available")
            return False
        
        # Define subjects and their schemas
        subjects_schemas = {
            "vn-market-ticks-value": self.schemas.get_market_data_schema(),
            "vn-market-bars-value": self.schemas.get_market_data_schema(),
            "vn-market-indices-value": self.schemas.get_market_data_schema(),
            "vn-trading-signals-value": self.schemas.get_trading_signal_schema(),
            "vn-risk-events-value": self.schemas.get_risk_event_schema()
        }
        
        success_count = 0
        total_count = len(subjects_schemas)
        
        for subject, schema in subjects_schemas.items():
            try:
                # Check if schema already exists
                existing_schema = self.client.get_latest_schema(subject)
                
                if existing_schema:
                    # Check compatibility
                    if self.client.check_compatibility(subject, schema):
                        self.logger.info(f"Schema for '{subject}' is compatible with existing version")
                        success_count += 1
                        continue
                    else:
                        self.logger.warning(f"Schema for '{subject}' is not compatible, registering new version")
                
                # Register schema
                schema_id = self.client.register_schema(subject, schema)
                
                if schema_id:
                    success_count += 1
                    self.logger.info(f"Successfully registered schema for '{subject}' (ID: {schema_id})")
                else:
                    self.logger.error(f"Failed to register schema for '{subject}'")
                    
            except Exception as e:
                self.logger.error(f"Error setting up schema for '{subject}': {e}")
        
        success = success_count == total_count
        
        if success:
            self.logger.info(f"Successfully set up all {total_count} schemas")
        else:
            self.logger.error(f"Failed to set up {total_count - success_count} out of {total_count} schemas")
        
        return success
    
    def validate_message(self, subject: str, message: Dict[str, Any]) -> bool:
        """Validate a message against its schema.
        
        Args:
            subject: Schema subject
            message: Message to validate
            
        Returns:
            True if message is valid
        """
        try:
            # Get schema
            schema_version = self.client.get_latest_schema(subject)
            if not schema_version:
                self.logger.error(f"No schema found for subject '{subject}'")
                return False
            
            # Parse schema
            schema_dict = json.loads(schema_version.schema)
            
            # Simple validation - in production, use jsonschema library
            return self._simple_validate(message, schema_dict)
            
        except Exception as e:
            self.logger.error(f"Error validating message for '{subject}': {e}")
            return False
    
    def _simple_validate(self, data: Any, schema: Dict[str, Any]) -> bool:
        """Simple schema validation.
        
        Args:
            data: Data to validate
            schema: JSON schema
            
        Returns:
            True if data matches schema
        """
        # This is a simplified validation - use jsonschema library for production
        if schema.get("type") == "object":
            if not isinstance(data, dict):
                return False
            
            # Check required fields
            required = schema.get("required", [])
            for field in required:
                if field not in data:
                    return False
            
            # Check properties
            properties = schema.get("properties", {})
            for field, value in data.items():
                if field in properties:
                    field_schema = properties[field]
                    if not self._validate_field(value, field_schema):
                        return False
        
        return True
    
    def _validate_field(self, value: Any, field_schema: Dict[str, Any]) -> bool:
        """Validate a single field.
        
        Args:
            value: Field value
            field_schema: Field schema
            
        Returns:
            True if field is valid
        """
        field_type = field_schema.get("type")
        
        if field_type == "string":
            return isinstance(value, str)
        elif field_type == "number":
            return isinstance(value, (int, float))
        elif field_type == "integer":
            return isinstance(value, int)
        elif field_type == "boolean":
            return isinstance(value, bool)
        elif field_type == "object":
            return isinstance(value, dict)
        elif field_type == "array":
            return isinstance(value, list)
        
        return True
    
    def get_schema_subjects(self) -> List[str]:
        """Get all schema subjects.
        
        Returns:
            List of subject names
        """
        return self.client.get_subjects()
    
    def cleanup_test_schemas(self) -> None:
        """Clean up test schemas (for development)."""
        self.logger.info("Cleaning up test schemas")
        
        test_subjects = [
            "vn-market-ticks-value",
            "vn-market-bars-value", 
            "vn-market-indices-value",
            "vn-trading-signals-value",
            "vn-risk-events-value"
        ]
        
        for subject in test_subjects:
            self.client.delete_subject(subject, permanent=True)