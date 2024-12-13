"""FMP API client implementation."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

import aiohttp
import pandas as pd
from loguru import logger

from ..config.settings import Settings, load_config
from ..models.data_types import AssetType, ExchangeInfo, IndexConstituent
from ..storage.mongo import MongoStorage
from ..utils.date_utils import split_5min_date_range, split_daily_date_range
from ..utils.tasks import background_task


class FMPClient:
    """Financial Modeling Prep API client."""
    
    def __init__(self, settings: Optional[Settings] = None):
        """Initialize FMP client."""
        self.settings = settings or load_config()
        self.session: Optional[aiohttp.ClientSession] = None
        self._logger = logger
        self.storage = MongoStorage(self.settings)
        self._exchange_cache: Dict[str, List[ExchangeInfo]] = {}
        self._background_tasks: Set[asyncio.Task] = set()

    @background_task
    async def _store_data_async(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str = "1d"
    ) -> None:
        """Store data in MongoDB asynchronously."""
        try:
            logger.debug(f"Starting background storage task for {symbol}")
            await self.storage.store_historical_data(df, symbol, interval)
            logger.debug(f"Completed background storage task for {symbol}")
        except Exception as e:
            logger.error(f"Error in background task for {symbol}: {e}", exc_info=True)
        finally:
            # Remove task from set when done
            current_task = asyncio.current_task()
            if current_task:
                self._background_tasks.discard(current_task)
                logger.debug(f"Removed background task for {symbol}")

    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Get historical price data for a symbol.
        
        First tries to get data from MongoDB. If not found or incomplete,
        downloads from FMP API and stores in MongoDB asynchronously.
        """
        # Try to get from MongoDB first
        df = await self.storage.get_historical_data(
            symbol,
            start_date,
            end_date,
            interval
        )
        
        # If we have complete data, return it
        if not df.empty and len(df) >= 2:  # At least 2 points to verify continuity
            start_dt = pd.Timestamp(start_date)
            end_dt = pd.Timestamp(end_date)
            df_start = df["date"].min()
            df_end = df["date"].max()
            
            if df_start <= start_dt and df_end >= end_dt:
                self._logger.debug(f"Found complete data in MongoDB for {symbol}")
                return df
        
        # If not found or incomplete, download from API
        self._logger.debug(f"Downloading data from API for {symbol}")
        df = await self._download_historical_data(symbol, start_date, end_date, interval)
        
        # Store in MongoDB asynchronously
        if not df.empty:
            self._store_data_async(df, symbol, interval)
        
        return df

    async def _download_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Download historical data from FMP API."""
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        
        chunks = self._generate_date_chunks(start_dt, end_dt, interval)
        logger.debug(f"Generated {len(chunks)} date chunks for {symbol}")
        
        all_data = []
        for chunk_start, chunk_end in chunks:
            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d")
            }
            
            endpoint = (
                f"historical-chart/5min/{symbol}"
                if interval == "5min"
                else f"historical-price-full/{symbol}"
            )
            
            logger.debug(f"Fetching {symbol} data from {chunk_start} to {chunk_end}")
            data = await self._get_data(endpoint, params)
            if not data:
                logger.warning(f"No data returned for {symbol} from {chunk_start} to {chunk_end}")
                continue
                
            logger.debug(f"Got response for {symbol}: {data.keys() if isinstance(data, dict) else 'list'}")
            
            if interval == "1d" and "historical" in data:
                df = pd.DataFrame(data["historical"])
            elif interval == "5min":
                df = pd.DataFrame(data)
            else:
                continue
                
            logger.debug(f"Converted to DataFrame with {len(df)} rows and columns: {df.columns.tolist()}")
            all_data.append(df)
            
        if not all_data:
            logger.warning(f"No data collected for {symbol}")
            return pd.DataFrame()
            
        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = symbol
        
        logger.debug(f"Final DataFrame for {symbol}: {len(df)} rows, sample:\n{df.head()}")
        return df.sort_values("date").drop_duplicates(subset=["date"])

    async def __aenter__(self):
        """Create aiohttp session on context manager enter."""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session and wait for background tasks on context manager exit."""
        # Wait for all background tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks)
        
        if self.session:
            await self.session.close()
            self.session = None

    async def _get_data(
        self,
        endpoint: str,
        params: Optional[Dict[str, str]] = None
    ) -> Dict:
        """Get data from FMP API.
        
        Parameters
        ----------
        endpoint : str
            API endpoint
        params : Optional[Dict[str, str]], optional
            Query parameters, by default None
            
        Returns
        -------
        Dict
            API response data
            
        Raises
        ------
        ValueError
            If session is not initialized
        """
        if not self.session:
            raise ValueError("Session not initialized. Use async with context manager.")
            
        # Add API key to params
        params = params or {}
        params["apikey"] = self.settings.api.key
        
        url = f"{self.settings.api.base_url}/{endpoint}"
        
        async with self.session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                text = await response.text()
                self._logger.error(f"Error {response.status}: {text}")
                return {}

    async def get_index_constituents(self, index: str = "sp500") -> pd.DataFrame:
        """Get current constituents of a market index."""
        valid_indexes = {"sp500", "nasdaq", "dowjones"}
        if index.lower() not in valid_indexes:
            raise ValueError(f"Invalid index. Must be one of: {valid_indexes}")
            
        data = await self._get_data(f"{index}_constituent")
        if not data:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([
            IndexConstituent(
                symbol=item["symbol"],
                name=item.get("name", ""),
                sector=item.get("sector", ""),
                sub_sector=item.get("subSector"),
                weight=item.get("weight")
            ).dict()
            for item in data
        ])
        
        return df

    def _generate_date_chunks(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Generate date chunks for data fetching."""
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        
        if interval == "5min":
            return split_5min_date_range(start_dt, end_dt)
        return split_daily_date_range(start_dt, end_dt)

    async def benchmark_m5_current_constituents(
        self, 
        index: str = "sp500",
        batch_size: int = 100
    ) -> None:
        """Benchmark test for getting current constituents' last 5 minutes data."""
        # Get current constituents
        const_df = await self.get_index_constituents(index)
        symbols = const_df["symbol"].tolist()
        
        # Set time range for last 5 minutes
        end_dt = pd.Timestamp.now()
        start_dt = pd.Timestamp(end_dt - pd.Timedelta(minutes=5))
        
        if self._logger:
            self._logger.info(f"Benchmarking {len(symbols)} symbols from {start_dt} to {end_dt}")
        
        # Use Timestamp objects for the API call
        df = await self.get_historical_data(
            symbol="AAPL",  # Example symbol
            start_date=start_dt,
            end_date=end_dt,
            interval="5min"
        )

    async def get_exchange_symbols(self, exchange: str) -> List[ExchangeInfo]:
        """Get list of symbols from a specific exchange.
        
        Parameters
        ----------
        exchange : str
            Exchange name (e.g., 'NASDAQ', 'NYSE', 'AMEX')
            
        Returns
        -------
        List[ExchangeInfo]
            List of exchange symbols with their info
        """
        if exchange in self._exchange_cache:
            return self._exchange_cache[exchange]

        data = await self._get_data(f"symbol/{exchange}")
        if not isinstance(data, list):
            return []

        symbols = []
        for item in data:
            if not item.get("symbol"):
                continue
                
            info = ExchangeInfo(
                symbol=item["symbol"],
                name=item.get("name", ""),
                exchange=exchange,
                asset_type=self._infer_asset_type(
                    exchange=exchange,
                    name=item.get("name", ""),
                    symbol=item["symbol"]
                )
            )
            symbols.append(info)

        self._exchange_cache[exchange] = symbols
        return symbols

    def _infer_asset_type(self, exchange: str, name: str, symbol: str) -> AssetType:
        """Infer asset type based on exchange and symbol characteristics."""
        # ETFs typically end with these
        if any(symbol.endswith(x) for x in ["ETF", "-ETF", "FUND"]):
            return AssetType.ETF
            
        # Common ETF keywords in name
        etf_keywords = ["ETF", "FUND", "TRUST", "ISHARES", "VANGUARD"]
        if any(keyword in name.upper() for keyword in etf_keywords):
            return AssetType.ETF

        # ADRs typically end with these
        if symbol.endswith(("ADR", "ADS")):
            return AssetType.ADR

        # REITs typically have these in name
        if "REIT" in name.upper():
            return AssetType.REIT

        # Default to stock for major exchanges
        if exchange in ["NYSE", "NASDAQ", "AMEX"]:
            return AssetType.STOCK

        return AssetType.OTHER

    async def get_all_exchange_symbols(self) -> List[ExchangeInfo]:
        """Get list of symbols from all major US exchanges.
        
        Returns
        -------
        List[ExchangeInfo]
            List of unique stock symbols with their info
        """
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        all_symbols: List[ExchangeInfo] = []
        
        for exchange in exchanges:
            symbols = await self.get_exchange_symbols(exchange)
            all_symbols.extend(symbols)
        
        # Remove duplicates while preserving order
        seen: Set[str] = set()
        unique_symbols = []
        for symbol in all_symbols:
            if symbol.symbol not in seen:
                seen.add(symbol.symbol)
                unique_symbols.append(symbol)
        
        return sorted(unique_symbols, key=lambda x: x.symbol)