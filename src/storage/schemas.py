"""MongoDB schemas and metadata definitions."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DataSource(str, Enum):
    """Data source enumeration."""
    FMP = "fmp"
    USER = "user"


class DataStatus(str, Enum):
    """Data status enumeration."""
    COMPLETE = "complete"
    PARTIAL = "partial"
    INVALID = "invalid"


class DataMetadata(BaseModel):
    """Base metadata for all collections."""
    
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    source: DataSource = Field(default=DataSource.FMP)
    status: DataStatus = Field(default=DataStatus.COMPLETE)
    version: str = Field(default="1.0")


class SymbolMetadata(DataMetadata):
    """Metadata for symbol data."""
    
    symbol: str
    first_date: datetime
    first_price: Optional[float] = None
    last_date: datetime
    data_points: int
    intervals: List[str]  # ["1d", "5min"]
    has_gaps: bool = False
    gap_dates: Optional[List[datetime]] = None
    exchanges: List[str]  # ["NYSE", "NASDAQ"]
    asset_type: str  # "stock", "etf", "index", etc.


class IndexMetadata(DataMetadata):
    """Metadata for index data."""
    
    index_name: str  # "sp500", "nasdaq", "dowjones"
    constituent_count: int
    last_rebalance: datetime
    sectors: Dict[str, int]  # Sector distribution


# MongoDB collection schemas
COLLECTIONS = {
    # Price data collections
    "historical_prices": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["symbol", "date"],
                "properties": {
                    "symbol": {"bsonType": "string"},
                    "date": {"bsonType": "date"},
                    "open": {"bsonType": "double"},
                    "high": {"bsonType": "double"},
                    "low": {"bsonType": "double"},
                    "close": {"bsonType": "double"},
                    "adjClose": {"bsonType": "double"},
                    "volume": {"bsonType": ["int", "double"]},
                    "unadjustedVolume": {"bsonType": ["int", "double"]},
                    "vwap": {"bsonType": "double"},
                    "change": {"bsonType": "double"},
                    "changePercent": {"bsonType": "double"},
                    "changeOverTime": {"bsonType": "double"},
                    "label": {"bsonType": "string"}
                }
            }
        },
        "indexes": [
            {"keys": [("symbol", 1), ("date", 1)], "unique": True},
            {"keys": [("date", 1)], "unique": False}
        ]
    },
    
    # Metadata collections
    "symbol_metadata": {
        "indexes": [
            {
                "keys": [("symbol", 1)],
                "unique": True
            },
            {
                "keys": [("last_updated", -1)]
            },
            {
                "keys": [("status", 1)]
            }
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["symbol", "first_date", "last_date", "data_points", "status"],
                "properties": {
                    "symbol": {"bsonType": "string"},
                    "first_date": {"bsonType": "date"},
                    "last_date": {"bsonType": "date"},
                    "data_points": {"bsonType": "int"},
                    "status": {"bsonType": "string"},
                    "has_gaps": {"bsonType": "bool"},
                    "gap_dates": {
                        "oneOf": [
                            {"bsonType": "array", "items": {"bsonType": "date"}},
                            {"bsonType": "null"}
                        ]
                    },
                    "exchanges": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "asset_type": {"bsonType": "string"}
                }
            }
        }
    },
    
    # Index constituent collections
    "index_constituents": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["symbol", "name", "sector", "sub_sector"],
                "properties": {
                    "symbol": {"bsonType": "string"},
                    "name": {"bsonType": "string"},
                    "sector": {"bsonType": "string"},
                    "sub_sector": {"bsonType": "string"},
                    "head_quarter": {"bsonType": ["string", "null"]},
                    "date_first_added": {"bsonType": ["string", "null"]},
                    "cik": {"bsonType": ["string", "null"]},
                    "founded": {"bsonType": ["string", "null"]},
                    "last_updated": {"bsonType": "date"}
                }
            }
        },
        "indexes": [
            {"keys": [("index", 1), ("symbol", 1)], "unique": True},
            {"keys": [("last_updated", 1)], "unique": False}
        ]
    },
    "company_profiles": {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["symbol", "last_updated"],
                "properties": {
                    "symbol": {"bsonType": "string"},
                    "companyName": {"bsonType": "string"},
                    "ipoDate": {"bsonType": "string"},
                    "last_updated": {"bsonType": "date"}
                }
            }
        },
        "indexes": [
            {"keys": [("symbol", 1)], "unique": True}
        ]
    }
} 