"""MongoDB schemas and metadata definitions."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from beanie import Document, Indexed
from pydantic import Field


class DataSource(str, Enum):
    """Data source enumeration."""
    FMP = "fmp"
    USER = "user"


class DataStatus(str, Enum):
    """Data status enumeration."""
    COMPLETE = "complete"
    PARTIAL = "partial"
    INVALID = "invalid"


class HistoricalPrice(Document):
    """Historical price document model."""
    symbol: str = Indexed(str)
    date: datetime = Indexed(datetime)
    open: float
    high: float
    low: float
    close: float
    adj_close: float | None = None
    volume: int
    unadjusted_volume: int = 0
    change: float
    change_percent: float | None = None
    vwap: float | None = None
    label: str
    change_over_time: float | None = None

    class Settings:
        name = "historical_prices"
        indexes = [
            [("symbol", 1), ("date", 1)],
            [("date", 1)]
        ]


class SymbolMetadata(Document):
    """Symbol metadata document model."""
    symbol: str = Indexed(str, unique=True)
    first_date: datetime
    first_price: float
    last_date: datetime
    data_points: int
    intervals: List[str]
    status: str
    exchanges: List[str]
    asset_type: str
    gap_dates: Optional[List[datetime]] = None
    has_gaps: bool = False

    class Settings:
        name = "symbol_metadata"
        indexes = [
            [("symbol", 1)],
            [("exchanges", 1)],
            [("asset_type", 1)]
        ]


class IndexConstituent(Document):
    """Index constituent document model."""
    index: str = Indexed(str)
    symbol: str = Indexed(str)
    name: str
    sector: str
    sub_sector: str
    head_quarter: Optional[str] = None
    date_first_added: Optional[str] = None
    cik: Optional[str] = None
    founded: Optional[str] = None
    last_updated: datetime = Field(default_factory=lambda: datetime.utcnow())

    class Settings:
        name = "index_constituents"
        indexes = [
            [("index", 1), ("symbol", 1)],
            [("last_updated", 1)]
        ]


class ExchangeSymbol(Document):
    """Exchange symbol document model."""
    exchange: str = Indexed(str)
    symbol: str = Indexed(str)
    name: Optional[str] = None
    price: Optional[float] = None
    changes_percentage: Optional[float] = None
    change: Optional[float] = None
    day_low: Optional[float] = None
    day_high: Optional[float] = None
    year_high: Optional[float] = None
    year_low: Optional[float] = None
    market_cap: Optional[float] = None
    price_avg50: Optional[float] = None
    price_avg200: Optional[float] = None
    volume: Optional[float] = None
    avg_volume: Optional[float] = None
    open: Optional[float] = None
    previous_close: Optional[float] = None
    eps: Optional[float] = None
    pe: Optional[float] = None
    earnings_announcement: Optional[str] = None
    shares_outstanding: Optional[float] = None
    timestamp: Optional[int] = None
    asset_type: Optional[str] = None
    last_updated: datetime = Field(default_factory=lambda: datetime.utcnow())

    class Settings:
        name = "exchange_symbols"
        indexes = [
            [("exchange", 1), ("symbol", 1)],
            [("last_updated", 1)]
        ]


class CompanyProfile(Document):
    """Company profile document model."""
    symbol: str = Indexed(str, unique=True)
    last_updated: datetime = Field(default_factory=lambda: datetime.utcnow())
    data: Dict[str, Any] = Field(default_factory=dict)

    class Settings:
        name = "company_profiles"
        indexes = [
            [("symbol", 1)],
            [("last_updated", 1)]
        ]


# Keep the existing COLLECTIONS configuration for MongoDB validation
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