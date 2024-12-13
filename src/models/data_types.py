"""Data type definitions for FMP API responses."""

from datetime import UTC, datetime
from enum import Enum
from typing import Optional, Protocol, Type, TypeVar

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field


class BaseModel(PydanticBaseModel):
    """Base model with pydantic functionality."""
    def dict(self) -> dict:
        return super().model_dump()
    def json(self) -> str:
        return super().model_dump_json()


class DataMetadata(BaseModel):
    """Base metadata for all collections."""
    
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source: str = Field(default="fmp")
    status: str = Field(default="complete")
    version: str = Field(default="1.0")


# Define base protocol for models
class BaseModelProtocol(Protocol):
    """Protocol for model functionality."""
    def dict(self) -> dict: ...
    def json(self) -> str: ...

# Define type variable for models
ModelType = TypeVar('ModelType', bound=BaseModelProtocol)


class HistoricalPrice(BaseModel):
    """Historical price data model."""
    
    date: datetime
    open: float
    high: float
    low: float
    close: float
    adj_close: float = Field(alias="adjClose")
    volume: int
    unadjusted_volume: int = Field(alias="unadjustedVolume", default=0)
    change: float
    change_percent: float = Field(alias="changePercent")
    vwap: Optional[float] = None
    label: str
    change_over_time: float = Field(alias="changeOverTime")
    symbol: str


Model = Type[BaseModel]  # Define a type alias for model classes


class AssetType(str, Enum):
    """Asset type enumeration."""
    STOCK = "stock"
    ETF = "etf"
    ADR = "adr"
    FOREX = "forex"
    CRYPTO = "crypto"
    REIT = "reit"
    OTHER = "other"


class ExchangeInfo(BaseModel):
    """Exchange symbol information.
    https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apikey={api_key}
    """
    
    symbol: str
    name: Optional[str] = None
    price: Optional[float] = None
    changes_percentage: Optional[float] = Field(alias="changesPercentage", default=None)
    change: Optional[float] = None
    day_low: Optional[float] = Field(alias="dayLow", default=None)
    day_high: Optional[float] = Field(alias="dayHigh", default=None)
    year_high: Optional[float] = Field(alias="yearHigh", default=None)
    year_low: Optional[float] = Field(alias="yearLow", default=None)
    market_cap: Optional[float] = Field(alias="marketCap", default=None)
    price_avg50: Optional[float] = Field(alias="priceAvg50", default=None)
    price_avg200: Optional[float] = Field(alias="priceAvg200", default=None)
    exchange: str
    volume: Optional[float] = None
    avg_volume: Optional[float] = Field(alias="avgVolume", default=None)
    open: Optional[float] = None
    previous_close: Optional[float] = Field(alias="previousClose", default=None)
    eps: Optional[float] = None
    pe: Optional[float] = None
    earnings_announcement: Optional[str] = Field(alias="earningsAnnouncement", default=None)
    shares_outstanding: Optional[float] = Field(alias="sharesOutstanding", default=None)
    timestamp: Optional[int] = None
    asset_type: Optional[AssetType] = Field(default=AssetType.STOCK)


class IndexConstituent(BaseModel):
    """Index constituent information.
    https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={api_key}
    """
    
    symbol: str
    name: str
    sector: str
    sub_sector: str = Field(alias="subSector")
    head_quarter: Optional[str] = Field(alias="headQuarter", default=None)
    date_first_added: Optional[str] = Field(alias="dateFirstAdded", default=None)
    cik: Optional[str] = None
    founded: Optional[str] = None


class HistoricalConstituent(BaseModel):
    """Historical constituent change data model.
    https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey={api_key}
    """
    
    date_added: str = Field(alias="dateAdded")  # e.g. "November 26, 2024"
    added_security: str = Field(alias="addedSecurity")
    removed_ticker: str = Field(alias="removedTicker")
    removed_security: str = Field(alias="removedSecurity")
    date: str  # YYYY-MM-DD format
    symbol: str
    reason: str