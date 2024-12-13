"""Storage protocols."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Union

import pandas as pd

from ..models.data_types import ExchangeInfo
from .schemas import SymbolMetadata


class StorageProtocol(Protocol):
    """Protocol for storage implementations."""
    
    storage: Any
    
    async def store_historical_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str = "1d"
    ) -> None:
        """Store historical price data."""
        ...
        
    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Get historical price data from storage."""
        ...
        
    async def store_company_profile(self, profile: Dict) -> None:
        """Store company profile data."""
        ...
        
    async def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """Get company profile from storage."""
        ...
        
    async def store_index_constituents(self, index: str, constituents: pd.DataFrame) -> None:
        """Store index constituents in storage."""
        ...
        
    async def store_exchange_symbols(self, exchange: str, symbols: List[ExchangeInfo]) -> None:
        """Store exchange symbols in storage."""
        ...
        
    async def get_symbol_metadata(self, symbol: str) -> Optional[SymbolMetadata]:
        """Get metadata for a symbol."""
        ...
        
    async def update_symbol_metadata(
        self,
        symbol: str,
        df: pd.DataFrame,
        interval: str = "1d",
        exchange_info: Optional[ExchangeInfo] = None
    ) -> None:
        """Update symbol metadata."""
        ... 