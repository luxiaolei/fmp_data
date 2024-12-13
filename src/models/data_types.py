"""Data type definitions for FMP API responses."""

from datetime import datetime
from enum import Enum
from typing import Optional, Protocol, Type, TypeVar, cast


# Define base protocol for models
class BaseModelProtocol(Protocol):
    """Protocol for model functionality."""
    def dict(self) -> dict: ...
    def json(self) -> str: ...

# Define type variable for models
ModelType = TypeVar('ModelType', bound=BaseModelProtocol)

from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field


class BaseModel(PydanticBaseModel):  # type: ignore
    """Base model with pydantic functionality."""
    def dict(self) -> dict:
        return super().model_dump()
    def json(self) -> str:
        return super().model_dump_json()


class HistoricalPrice(BaseModel):  # type: ignore
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
    REIT = "reit"
    OTHER = "other"


class ExchangeInfo(BaseModel):
    """Exchange symbol information."""
    
    symbol: str
    name: str
    exchange: str
    asset_type: AssetType


class IndexConstituent(BaseModel):
    """Index constituent information."""
    
    symbol: str
    name: str
    sector: str
    sub_sector: Optional[str] = None
    weight: Optional[float] = None


class HistoricalConstituent(cast(Model, BaseModel)):  # type: ignore
    """Historical constituent change data model."""
    
    date_added: str = Field(alias="dateAdded")  # YYYY-MM-DD
    added_security: str = Field(alias="addedSecurity")
    removed_ticker: str = Field(alias="removedTicker")
    removed_security: str = Field(alias="removedSecurity")
    date: str  # YYYY-MM-DD
    symbol: str
    reason: str