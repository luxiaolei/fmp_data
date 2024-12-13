"""FMP API client implementation."""

import asyncio
from datetime import UTC, datetime
from typing import Dict, List, Optional, Set, Tuple, Union

import aiohttp
import pandas as pd
from loguru import logger
from pydantic import ValidationError
from rich.progress import Progress

from ..config.settings import Settings
from ..models.data_types import AssetType, ExchangeInfo, IndexConstituent
from ..storage.mongo import MongoStorage
from ..storage.protocols import StorageProtocol
from ..utils.date_utils import split_5min_date_range, split_daily_date_range
from ..utils.rate_limiter import RedisRateLimiter
from ..utils.tasks import background_task


class FMPClient:
    """Financial Modeling Prep API client."""
    
    def __init__(
        self,
        settings: Settings,
        redis_url: str = "redis://localhost:6379",
        redis_password: Optional[str] = None
    ):
        """Initialize FMP client."""
        self.settings = settings
        self.session: Optional[aiohttp.ClientSession] = None
        self.storage: StorageProtocol = MongoStorage(settings)
        self._logger = logger
        self._exchange_cache: Dict[str, List[ExchangeInfo]] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        
        # Initialize rate limiter
        self.rate_limiter = RedisRateLimiter(
            redis_url=redis_url,
            redis_password=redis_password
        )

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
        interval: str = "1d",
        progress: Optional[Progress] = None
    ) -> pd.DataFrame:
        """Get historical price data for a symbol.
        
        Parameters
        ----------
        symbol : str
            Trading symbol
        start_date : Union[str, datetime, pd.Timestamp]
            Start date for historical data
        end_date : Union[str, datetime, pd.Timestamp]
            End date for historical data
        interval : str, optional
            Data interval ('1d' or '5min'), by default "1d"
        progress : Optional[Progress], optional
            Rich progress instance for displaying progress, by default None
            
        Returns
        -------
        pd.DataFrame
            Historical price data
            
        #TODO:
        # Add symbol check, raise error if symbol is not in storage
        # Add progress bar
        """
        if not self.storage:
            raise ValueError("Storage not initialized")
        
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        _new_data = False
        
        # First check stored company profile for IPO date
        stored_profile = await self.storage.get_company_profile(symbol)
        if stored_profile and "ipoDate" in stored_profile:
            ipo_date = pd.Timestamp(stored_profile["ipoDate"])
            if start_dt < ipo_date:
                logger.debug(f"{symbol} IPO date from stored profile: {ipo_date}, adjusting start date")
                start_dt = ipo_date
                
                if start_dt > end_dt:
                    logger.debug(f"Skipping {symbol}: IPO date ({ipo_date}) is after requested end date ({end_dt})")
                    return pd.DataFrame()

        # Then check MongoDB for existing data with adjusted start date
        df = await self.storage.get_historical_data(
            symbol,
            start_dt,  # Use adjusted start date
            end_dt,
            interval
        )
        
        # Get metadata to check data availability
        metadata = await self.storage.get_symbol_metadata(symbol)
        
        if df.empty:
            # No data in requested range, check metadata
            if metadata:
                # We have metadata, adjust start date if needed
                if start_dt < metadata.first_date:
                    logger.debug(f"Adjusting start date to first available date: {metadata.first_date}")
                    start_dt = metadata.first_date
            else:
                # No metadata, need to check company profile
                profile = await self.get_company_profile(symbol)
                if profile and "ipoDate" in profile:
                    ipo_date = pd.Timestamp(profile["ipoDate"])
                    if start_dt < ipo_date:
                        logger.debug(f"{symbol} IPO date: {ipo_date}, adjusting start date")
                        start_dt = ipo_date
        else:
            # We have partial data, adjust dates to fill gaps
            df_start = df["date"].min()
            df_end = df["date"].max()
            
            if df_start > start_dt:
                # Need earlier data
                logger.debug(f"Downloading missing data before {df_start}")
                earlier_df = await self._download_historical_data(symbol, start_dt, df_start, interval, progress)
                if not earlier_df.empty:
                    len_before = len(df)
                    df = pd.concat([earlier_df, df]).drop_duplicates(subset=["date"])
                    if len(df) >= len_before:
                        _new_data = True
            
            if df_end < end_dt:
                # Need later data
                logger.debug(f"Downloading missing data after {df_end}")
                later_df = await self._download_historical_data(symbol, df_end, end_dt, interval, progress)
                if not later_df.empty:
                    len_after = len(df)
                    df = pd.concat([df, later_df]).drop_duplicates(subset=["date"])
                    if len(df) >= len_after:
                        _new_data = True
        
        # Check if adjusted start date is after end date
        if start_dt > end_dt:
            logger.info(f"Skipping {symbol}: Start date ({start_dt}) is after end date ({end_dt})")
            return pd.DataFrame()
        
        # Download any missing data
        if df.empty or len(df) < 2:
            logger.debug(f"Downloading complete data for {symbol}")
            df = await self._download_historical_data(symbol, start_dt, end_dt, interval, progress)
            if not df.empty:
                _new_data = True
        
        # Store in MongoDB asynchronously
        if _new_data:
            self._store_data_async(df, symbol, interval)
            
            # Update metadata with first price if needed
            if not metadata or metadata.first_date > df["date"].min():
                await self.storage.update_symbol_metadata(
                    symbol=symbol,
                    df=df,
                    interval=interval
                )
        
        return df

    async def _download_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d",
        progress: Optional[Progress] = None
    ) -> pd.DataFrame:
        """Download historical data from FMP API.
        
        Parameters
        ----------
        symbol : str
            Trading symbol
        start_date : Union[str, datetime, pd.Timestamp]
            Start date for historical data
        end_date : Union[str, datetime, pd.Timestamp]
            End date for historical data
        interval : str, optional
            Data interval ('1d' or '5min'), by default "1d"
        progress : Optional[Progress], optional
            Rich progress instance for displaying progress, by default None
            
        Returns
        -------
        pd.DataFrame
            Historical price data
        """
        if not self.session:
            raise ValueError("Session not initialized")

        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        chunks = self._generate_date_chunks(start_dt, end_dt, interval)
        all_data = []

        # Create chunk task if progress is provided
        chunk_task = None
        if progress is not None:
            chunk_task = progress.add_task(
                f"[cyan]Downloading chunks for {symbol}",
                total=len(chunks),
                visible=False
            )

        for chunk_start, chunk_end in chunks:
            if interval == "1d":
                endpoint = f"historical-price-full/{symbol}"
            else:
                endpoint = f"historical-chart/5min/{symbol}"

            params = {
                "from": chunk_start.strftime("%Y-%m-%d"),
                "to": chunk_end.strftime("%Y-%m-%d"),
                "apikey": self.settings.api.key
            }
            
            data = await self._get_data(endpoint, params)
            
            if not data:
                logger.debug(f"No data returned for {symbol} from {chunk_start} to {chunk_end}")
                if progress is not None and chunk_task is not None:
                    progress.advance(chunk_task)
                continue

            # Parse response based on interval
            if interval == "1d":
                if not isinstance(data, dict) or "historical" not in data:
                    if progress is not None and chunk_task is not None:
                        progress.advance(chunk_task)
                    continue
                chunk_df = pd.DataFrame(data["historical"])
            else:  # 5min
                if not isinstance(data, list):
                    if progress is not None and chunk_task is not None:
                        progress.advance(chunk_task)
                    continue
                chunk_df = pd.DataFrame(data)

            if not chunk_df.empty:
                all_data.append(chunk_df)
            
            if progress is not None and chunk_task is not None:
                progress.advance(chunk_task)

        if not all_data:
            return pd.DataFrame()

        # Combine and process data
        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = symbol
        
        return df.sort_values("date").drop_duplicates(subset=["date"])

    async def __aenter__(self) -> "FMPClient":
        """Enter async context."""
        self.session = aiohttp.ClientSession()
        self.storage = MongoStorage(self.settings)
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
        """Get data from FMP API with rate limiting.
        
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
        logger.debug(f"Making API request to: {url} with params: {params}")
        
        # Wait for rate limit token
        await self.rate_limiter.wait_if_needed()
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"API response status: {response.status}, data type: {type(data)}")
                    return data
                else:
                    text = await response.text()
                    logger.error(f"API error {response.status}: {text}")
                    return {}
        except Exception as e:
            logger.error(f"Request failed: {str(e)}", exc_info=True)
            return {}

    async def get_index_constituents(self, index: str = "sp500") -> pd.DataFrame:
        """Get current constituents of a market index."""
        valid_indexes = {"sp500", "nasdaq", "dowjones"}
        if index.lower() not in valid_indexes:
            raise ValueError(f"Invalid index. Must be one of: {valid_indexes}")
            
        data = await self._get_data(f"{index}_constituent")
        if not data or not isinstance(data, list):
            return pd.DataFrame()
            
        # Convert to DataFrame
        constituents = []
        for item in data:
            if not isinstance(item, dict):
                continue
            try:
                constituent = IndexConstituent(**item)
                constituents.append(constituent.dict())
            except Exception as e:
                logger.warning(f"Error processing constituent data: {e}")
                continue
        
        return pd.DataFrame(constituents)

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
        """Get list of symbols from a specific exchange."""
        data = await self._get_data(f"symbol/{exchange}")
        if not data or not isinstance(data, list):
            return []
        
        symbols = []
        for item in data:
            if not isinstance(item, dict):
                continue
            try:
                info = ExchangeInfo(**item)
                symbols.append(info)
            except ValidationError as e:
                # Log which fields failed validation
                for error in e.errors():
                    logger.warning(
                        f"Validation error for symbol {item.get('symbol', 'UNKNOWN')}: "
                        f"Field '{error['loc'][0]}' - {error['msg']}"
                    )
                continue
                
        return symbols

    def _infer_asset_type(self, exchange: str, name: Optional[str], symbol: str) -> AssetType:
        """Infer asset type from exchange and symbol information.
        
        Parameters
        ----------
        exchange : str
            Exchange code
        name : Optional[str]
            Security name, may be None
        symbol : str
            Trading symbol
        
        Returns
        -------
        AssetType
            Inferred asset type
        """
        # Handle None name case
        name = name or ""
        name_upper = name.upper()
        symbol_upper = symbol.upper()
        
        # ETF keywords
        etf_keywords = ["ETF", "FUND", "TRUST", "ISHARES", "VANGUARD", "SPDR"]
        if any(keyword in name_upper for keyword in etf_keywords):
            return AssetType.ETF
        
        # Forex pairs
        if exchange.upper() in ["FOREX", "FX"]:
            return AssetType.FOREX
        
        # Crypto
        if exchange.upper() in ["CRYPTO", "BINANCE", "COINBASE"]:
            return AssetType.CRYPTO
        
        # Common stock indicators
        if (
            name_upper.endswith(" COMMON STOCK") or 
            name_upper.endswith(" CLASS A") or
            name_upper.endswith(" CLASS B") or
            name_upper.endswith(" INC") or
            name_upper.endswith(" CORP") or
            name_upper.endswith(" PLC")
        ):
            return AssetType.STOCK
        
        # ADR indicators
        if "ADR" in name_upper or "ADS" in name_upper:
            return AssetType.ADR
        
        # Default to stock if no other type is detected
        return AssetType.STOCK

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

    async def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """Get company profile information."""
        if not self.storage:
            raise ValueError("Storage not initialized")
        
        # Try to get from MongoDB first
        profile = await self.storage.get_company_profile(symbol)
        if profile:
            # Check if profile is recent (less than 1 day old)
            last_updated = profile.get("last_updated")
            # Convert last_updated to timezone-aware if it exists
            if last_updated:
                if last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=UTC)
                if (datetime.now(UTC) - last_updated).days < 1:
                    return profile
        
        # If not found or outdated, get from API
        try:
            url = f"{self.settings.api.base_url}/profile/{symbol}"
            params = {"apikey": self.settings.api.key}
            
            logger.debug(f"Fetching profile for {symbol} from {url}")
            data = await self._get_data("profile/" + symbol, params)
            
            if isinstance(data, list) and data:
                profile = data[0]
                # Store in MongoDB asynchronously
                self._store_profile_async(profile)
                return profile
                
            logger.warning(f"No profile data returned for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching profile for {symbol}: {str(e)}", exc_info=True)
            return None

    @background_task
    async def _store_profile_async(self, profile: Dict) -> None:
        """Store company profile in MongoDB asynchronously."""
        try:
            await self.storage.store_company_profile(profile)
        except Exception as e:
            logger.error(f"Error storing profile: {e}", exc_info=True)

    def _get_date_chunks(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Generate appropriate date chunks based on interval.
        
        Parameters
        ----------
        start_date : Union[str, datetime, pd.Timestamp]
            Start date
        end_date : Union[str, datetime, pd.Timestamp]
            End date
        interval : str, optional
            Data interval ('1d' or '5min'), by default "1d"
            
        Returns
        -------
        List[Tuple[pd.Timestamp, pd.Timestamp]]
            List of (start, end) date pairs for chunked API calls
        """
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        
        if interval == "5min":
            # For 5min data, use 5-day chunks
            chunk_size = pd.Timedelta(days=5)
        else:
            # For daily data, use 4-year chunks
            chunk_size = pd.DateOffset(years=4)
        
        date_chunks = []
        current = start_dt
        
        while current < end_dt:
            chunk_end = min(current + chunk_size, end_dt)
            date_chunks.append((current, chunk_end))
            current = chunk_end
            
        return date_chunks