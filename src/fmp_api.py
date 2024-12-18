"""Financial Modeling Prep (FMP) API client.

This module provides an async client for interacting with the Financial Modeling Prep API.
It handles authentication, rate limiting, and data formatting for both daily and 5-minute data.
"""

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union, cast

import aiohttp
import pandas as pd
import redis
import yaml
from loguru import logger
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .utils.date_utils import split_5min_date_range, split_daily_date_range

console = Console()


class RedisRateLimiter:
    """Redis-based rate limiter for API calls."""
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        max_calls: int = 750,
        key_prefix: str = "fmp_api:",
        redis_password: Optional[str] = None
    ):
        """Initialize Redis rate limiter."""
        redis_password = redis_password or os.getenv("REDIS_PASSWORD")
        if not redis_password:
            raise ValueError("Redis password not provided and REDIS_PASSWORD not found in environment variables")
            
        # Parse URL and add password
        from urllib.parse import urlparse
        parsed = urlparse(redis_url)
        redis_url = f"redis://:{redis_password}@{parsed.hostname}:{parsed.port}"
        
        self.redis_client = redis.from_url(redis_url)
        self.max_calls = max_calls
        self.key_prefix = key_prefix
        self.window_size = 60 / max_calls  # Time window per request in seconds
        self._rate_limit_hits = 0

    async def wait_if_needed(self) -> None:
        """Wait until a rate limit token is available."""
        key = f"{self.key_prefix}last_call"
        expiry_ms = max(int(self.window_size * 1000), 1)
        
        while True:
            try:
                # Try to set key with NX (only if not exists)
                if self.redis_client.set(key, '1', px=expiry_ms, nx=True):
                    return
                await asyncio.sleep(self.window_size)
            except redis.RedisError as e:
                logger.error(f"Redis error in rate limiter: {e}")
                return  # On Redis errors, allow the request

@dataclass
class IndexConstituent:
    """Data class for index constituent information."""
    symbol: str
    name: str
    sector: str
    sub_sector: str
    headquarters: str
    date_added: str
    cik: str
    founded: str

    def __init__(
        self,
        symbol: str,
        name: str,
        sector: str,
        sub_sector: str,
        headquarters: str,
        date_added: str,
        cik: str,
        founded: str
    ) -> None:
        """Initialize IndexConstituent."""
        self.symbol = symbol
        self.name = name
        self.sector = sector
        self.sub_sector = sub_sector
        self.headquarters = headquarters
        self.date_added = date_added
        self.cik = cik
        self.founded = founded

@dataclass
class HistoricalConstituent:
    """Data class for historical constituent information."""
    date_added: str  # YYYY-MM-DD
    added_security: str
    removed_ticker: str
    removed_security: str
    date: str  # YYYY-MM-DD
    symbol: str
    reason: str

    def __init__(
        self,
        date_added: str,
        added_security: str,
        removed_ticker: str,
        removed_security: str,
        date: str,
        symbol: str,
        reason: str
    ) -> None:
        """Initialize HistoricalConstituent."""
        self.date_added = date_added
        self.added_security = added_security
        self.removed_ticker = removed_ticker
        self.removed_security = removed_security
        self.date = date
        self.symbol = symbol
        self.reason = reason

class IndexConstituentData(TypedDict):
    """Type hints for index constituent data from API."""
    symbol: str
    name: str
    sector: str
    subSector: str
    headQuarter: str
    dateFirstAdded: str
    cik: str
    founded: str

class HistoricalConstituentData(TypedDict):
    """Type hints for historical constituent data from API."""
    dateAdded: str  # Month DD, YYYY
    addedSecurity: str
    removedTicker: str
    removedSecurity: str
    date: str  # YYYY-MM-DD
    symbol: str
    reason: str

class FMPClient:
    """Async client for Financial Modeling Prep API.
    
    This class provides methods for fetching historical price data,
    exchange symbols, and index constituents from FMP API.
    
    Attributes
    ----------
    api_key : str
        FMP API key for authentication
    base_url : str
        Base URL for FMP API
    session : Optional[aiohttp.ClientSession]
        Aiohttp session for making requests
    rate_limiter : RedisRateLimiter
        Rate limiter for API calls
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://financialmodelingprep.com/api/v3",
        redis_url: str = "redis://localhost:6379",
        redis_password: Optional[str] = None
    ):
        """Initialize FMP API client."""
        api_key = api_key or os.getenv("FMP_API_KEY")
        if not api_key:
            raise ValueError("FMP API key not provided and not found in environment variables")
        self.api_key = api_key
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Initialize rate limiter with password
        self.rate_limiter = RedisRateLimiter(
            redis_url=redis_url,
            redis_password=redis_password
        )
        
        # Load constants
        self._constants = self._load_constants()
        
    def _load_constants(self) -> Dict[str, Any]:
        """Load constants from yaml file."""
        constants_path = Path(__file__).parent / "constant.yaml"
        if not constants_path.exists():
            logger.warning(f"Constants file not found: {constants_path}")
            return {}
            
        with open(constants_path) as f:
            return yaml.safe_load(f)
            
    async def __aenter__(self):
        """Create aiohttp session on context manager enter."""
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session on context manager exit."""
        if self.session:
            await self.session.close()
            self.session = None

    async def _get_data(
        self,
        url: str,
        delay: float = 0.2,
        retries: int = 5
    ) -> Dict[str, Any]:
        """Get data from FMP API with retries and rate limiting."""
        if not self.session:
            raise ValueError("Session not initialized. Use async with context manager.")
            
        for attempt in range(retries):
            try:
                # Wait for rate limit token
                await self.rate_limiter.wait_if_needed()
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limit exceeded
                        # No need to log here, RedisRateLimiter will handle it
                        await asyncio.sleep(delay * (2 ** attempt))
                        continue
                    else:
                        text = await response.text()
                        logger.warning(f"Error {response.status}: {text}")
                await asyncio.sleep(delay)
            except Exception as e:
                if attempt == retries - 1:
                    logger.error(f"Failed after {retries} attempts: {e}")
                    raise
                await asyncio.sleep(delay * (2 ** attempt))
        return {}

    def _generate_date_chunks(
        self,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str
    ) -> List[Tuple[str, str]]:
        """Generate date chunks for data fetching.
        
        Parameters
        ----------
        start_date : Union[str, datetime, pd.Timestamp]
            Start date
        end_date : Union[str, datetime, pd.Timestamp]
            End date
        interval : str
            Data interval ('1d' or '5min')
            
        Returns
        -------
        List[Tuple[str, str]]
            List of (chunk_start, chunk_end) date pairs
        """
        start_dt = pd.Timestamp(start_date)
        end_dt = pd.Timestamp(end_date)
        
        # Get date chunks based on interval
        if interval == "5min":
            date_chunks = split_5min_date_range(start_dt, end_dt)
        else:
            date_chunks = split_daily_date_range(start_dt, end_dt)
            
        # Convert dates to string format
        return [
            (chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
            for chunk_start, chunk_end in date_chunks
        ]

    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d",
        delay: float = 0.2,
        retries: int = 3,
        chunk_size: int = 5  # Number of date chunks to process in parallel
    ) -> pd.DataFrame:
        """Get historical price data for a symbol.
        
        Parameters
        ----------
        symbol : str
            Stock symbol
        start_date : Union[str, datetime, pd.Timestamp]
            Start date
        end_date : Union[str, datetime, pd.Timestamp]
            End date
        interval : str
            Data interval ('1d' or '5min')
        delay : float
            Delay between retries in seconds
        retries : int
            Number of retries on failure
        chunk_size : int
            Number of date chunks to process in parallel
            
        Returns
        -------
        pd.DataFrame
            Historical price data
        """
        if self.session is None:
            raise ValueError("Session not initialized. Use async with context manager.")
            
        # Generate date chunks
        date_chunks = self._generate_date_chunks(start_date, end_date, interval)
        
        # Determine endpoint based on interval
        if interval == "5min":
            endpoint = "historical-chart/5min"
        else:
            endpoint = "historical-price-full"
        
        async def fetch_chunk_data(chunk_start: str, chunk_end: str) -> pd.DataFrame:
            """Fetch data for a single date chunk."""
            try:
                url = f"{self.base_url}/{endpoint}/{symbol}"
                url += f"?from={chunk_start}&to={chunk_end}&apikey={self.api_key}"
                
                data = await self._get_data(url, delay=delay, retries=retries)
                if not data:
                    logger.error(f"Error fetching {symbol}: {data}")
                    return pd.DataFrame()
                
                # Handle different response formats
                if interval == "1d" and "historical" in data:
                    return pd.DataFrame(data["historical"])
                elif interval == "5min":
                    return pd.DataFrame(data)
                
                logger.error(f"Unexpected response format for {symbol}: {data}")
                return pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"Error fetching {symbol}: {str(e)}")
                return pd.DataFrame()
        
        # Process date chunks in batches
        all_data = []
        for i in range(0, len(date_chunks), chunk_size):
            batch = date_chunks[i:i + chunk_size]
            tasks = [
                fetch_chunk_data(chunk_start, chunk_end)
                for chunk_start, chunk_end in batch
            ]
            chunk_data = await asyncio.gather(*tasks)
            all_data.extend(chunk_data)
            
            # Small delay between batches to avoid overwhelming the API
            await asyncio.sleep(delay)
        
        # Combine all chunks
        if not all_data:
            return pd.DataFrame()
            
        df = pd.concat(all_data, ignore_index=True)
        if df.empty:
            return df
            
        # Convert date column
        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = symbol
        
        # Check for NaN values and warn
        nan_cols = df.columns[df.isna().any()].tolist()
        if nan_cols:
            logger.warning(f"Found NaN values in columns {nan_cols} for {symbol}")
        
        # Sort by date and remove duplicates
        df = df.drop_duplicates(subset=["date"]).sort_values("date", ascending=True)
        
        return df

    async def get_exchange_symbols(self) -> List[str]:
        """Get list of symbols from US exchanges.
        
        Returns
        -------
        List[str]
            List of unique stock symbols
        """
        exchanges = self._constants.get("exchanges", [])
        symbols = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching exchange symbols...", total=len(exchanges))
            
            for exchange in exchanges:
                url = f"{self.base_url}/symbol/{exchange}?apikey={self.api_key}"
                data = await self._get_data(url)
                if isinstance(data, list):
                    typed_data = cast(List[Dict[str, Any]], data)
                    symbols.extend([item.get("symbol", "") for item in typed_data])
                progress.advance(task)
                
        # Add market indexes
        symbols.extend([index["symbol"] for index in self._constants.get("indexes", [])])
        return sorted(list(set(symbols)))

    async def get_index_constituents(self, index: str = "sp500") -> pd.DataFrame:
        """Get constituents of a market index.
        
        Parameters
        ----------
        index : str
            Index name ('sp500', 'nasdaq', or 'dowjones')
            
        Returns
        -------
        pd.DataFrame
            Index constituents information
            
        Raises
        ------
        ValueError
            If invalid index name provided
        """
        valid_indexes = {"sp500", "nasdaq", "dowjones"}
        if index.lower() not in valid_indexes:
            raise ValueError(f"Invalid index. Must be one of: {valid_indexes}")
            
        url = f"{self.base_url}/{index}_constituent?apikey={self.api_key}"
        data = await self._get_data(url)
        
        if not data:
            return pd.DataFrame()
            
        constituents = []
        for item in cast(List[IndexConstituentData], data):
            constituent = IndexConstituent(
                symbol=item["symbol"],
                name=item["name"],
                sector=item["sector"],
                sub_sector=item["subSector"],
                headquarters=item["headQuarter"],
                date_added=item["dateFirstAdded"],
                cik=item["cik"],
                founded=item["founded"]
            )
            constituents.append(constituent.__dict__)
            
        return pd.DataFrame(constituents)

    def _parse_date(self, date_str: str) -> str:
        """Convert date string from 'Month DD, YYYY' to 'YYYY-MM-DD'."""
        try:
            dt = datetime.strptime(date_str, "%B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError as e:
            logger.warning(f"Failed to parse date {date_str}: {e}")
            return date_str

    async def get_historical_constituents(self, index: str = "sp500") -> pd.DataFrame:
        """Get historical constituent changes of a market index.
        
        Parameters
        ----------
        index : str
            Index name ('sp500', 'nasdaq', or 'dowjones')
            
        Returns
        -------
        pd.DataFrame
            Historical constituent changes with columns:
            - date_added: When the security was added
            - added_security: Name of added security
            - removed_ticker: Ticker of removed security
            - removed_security: Name of removed security
            - date: Effective date
            - symbol: Ticker symbol
            - reason: Reason for change
        """
        valid_indexes = {"sp500", "nasdaq", "dowjones"}
        if index.lower() not in valid_indexes:
            raise ValueError(f"Invalid index. Must be one of: {valid_indexes}")
            
        url = f"{self.base_url}/historical/{index}_constituent?apikey={self.api_key}"
        data = await self._get_data(url)
        
        if not data:
            return pd.DataFrame()
            
        constituents = []
        for item in cast(List[HistoricalConstituentData], data):
            constituent = HistoricalConstituent(
                date_added=self._parse_date(item["dateAdded"]),
                added_security=item["addedSecurity"],
                removed_ticker=item["removedTicker"],
                removed_security=item["removedSecurity"],
                date=item["date"],
                symbol=item["symbol"],
                reason=item["reason"]
            )
            constituents.append(constituent.__dict__)
            
        return pd.DataFrame(constituents)

    def convert_to_constituent_df(self, hist_df: pd.DataFrame) -> pd.DataFrame:
        """Convert historical changes to constituent membership dataframe.
        
        Parameters
        ----------
        hist_df : pd.DataFrame
            Historical constituent changes dataframe from get_historical_constituents()
            
        Returns
        -------
        pd.DataFrame
            Constituent membership with columns:
            - symbol: Ticker symbol
            - date_added: When the security was added
            - date_removed: When the security was removed (2099-01-01 if still active)
        """
        # Create records for added securities
        added = hist_df[["symbol", "date_added"]].copy()
        
        # Create records for removed securities
        removed = pd.DataFrame({
            "symbol": hist_df["removed_ticker"],
            "date_removed": hist_df["date"]
        }).dropna()  # Drop rows where no security was removed
        
        # Merge added and removed dates
        df = pd.merge(added, removed, on="symbol", how="left")
        
        # Fill missing removal dates with far future date
        df["date_removed"] = df["date_removed"].fillna("2099-01-01")
        
        return df.sort_values(["symbol", "date_added"])

    async def benchmark_m5_current_constituents(
        self,
        index: str = "sp500",
        batch_size: int = 100
    ) -> None:
        """Benchmark performance of fetching 5-minute data for current index constituents.

        This method tests the API's performance by fetching the most recent 5-minute
        data for all current constituents of a specified market index. It processes
        requests in batches and collects performance metrics.

        Parameters
        ----------
        index : str, optional
            Market index to benchmark ('sp500', 'nasdaq', or 'dowjones'), by default "sp500"
        batch_size : int, optional
            Number of symbols to process in parallel, by default 100

        Returns
        -------
        None
            Prints benchmark results to console

        Notes
        -----
        Performance metrics include:
        - Total execution time
        - Requests per second
        - Success/failure counts
        - Total data points retrieved
        - Symbols with NaN values
        - Average time per symbol
        """
        # Get current constituents
        rprint(f"[cyan]Getting current {index.upper()} constituents...")
        const_df = await self.get_index_constituents(index)
        symbols = const_df["symbol"].tolist()
        rprint(f"[green]Found {len(symbols)} constituents")
        
        # Set time range for last 5 minutes
        end_dt = pd.Timestamp.now()
        start_dt = end_dt - pd.Timedelta(minutes=5)
        
        # Benchmark data collection
        start_time = time.time()
        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []
        
        async def fetch_symbol_data(symbol: str) -> Dict[str, Any]:
            """Fetch 5-minute data for a single symbol.

            Parameters
            ----------
            symbol : str
                Stock symbol to fetch data for

            Returns
            -------
            Dict[str, Any]
                Dictionary containing fetch results and statistics
            """
            try:
                df = await self.get_historical_data(
                    symbol,
                    start_date=start_dt,
                    end_date=end_dt,
                    interval="5min"
                )
                return {
                    "symbol": symbol,
                    "rows": len(df),
                    "has_nan": df.isna().any().any(),
                    "success": True
                }
            except Exception as e:
                return {
                    "symbol": symbol,
                    "error": str(e),
                    "success": False
                }

        # Process symbols in batches
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(
                f"[cyan]Fetching 5min data (batch size: {batch_size})...", 
                total=len(symbols)
            )
            
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                tasks = [fetch_symbol_data(symbol) for symbol in batch]
                batch_results = await asyncio.gather(*tasks)
                
                for result in batch_results:
                    if result["success"]:
                        results.append(result)
                    else:
                        errors.append({
                            "symbol": result["symbol"],
                            "error": result["error"]
                        })
                    progress.advance(task)
        
        # Calculate statistics
        total_time = time.time() - start_time
        success_count = len(results)
        error_count = len(errors)
        total_rows = sum(r["rows"] for r in results)
        nan_count = sum(1 for r in results if r["has_nan"])
        requests_per_second = len(symbols) / total_time
        
        # Print benchmark results
        rprint("\n[bold cyan]Benchmark Results:[/bold cyan]")
        rprint(f"[green]Total time: {total_time:.2f} seconds")
        rprint(f"[green]Requests per second: {requests_per_second:.2f}")
        rprint(f"[green]Successful requests: {success_count}")
        rprint(f"[yellow]Failed requests: {error_count}")
        rprint(f"[green]Total data points: {total_rows}")
        rprint(f"[yellow]Symbols with NaN values: {nan_count}")
        rprint(f"[green]Average time per symbol: {total_time/len(symbols):.5f} seconds")
        rprint(f"[green]Batch size: {batch_size}")
        
        if errors:
            rprint("\n[bold red]Errors:[/bold red]")
            for error in errors:
                rprint(f"[red]{error['symbol']}: {error['error']}")

    async def run_benchmark(self) -> None:
        """Run benchmark tests for different chunk sizes."""
        rprint("\n[bold cyan]Running benchmark tests...[/bold cyan]")
        chunk_sizes = [50, 100, 200]
        
        for chunk_size in chunk_sizes:
            # Wait 70 seconds to ensure rate limit window is cleared
            rprint(f"\nWaiting 70 seconds for rate limit window to clear before testing chunk size {chunk_size}...")
            await asyncio.sleep(70)
            
            rprint(f"\n[bold]Testing chunk size: {chunk_size}[/bold]")
            start_time = time.time()
            
            # Run test with current chunk size
            symbols = ["AAPL", "MSFT", "GOOGL"]  # Add more symbols as needed
            tasks = []
            for symbol in symbols:
                task = self.get_historical_data(
                    symbol=symbol,
                    start_date="2023-01-01",
                    end_date="2023-12-31",
                    interval="1d",
                    chunk_size=chunk_size
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            
            elapsed = time.time() - start_time
            rprint(f"Chunk size {chunk_size}: {elapsed:.2f} seconds")


async def main():
    """Example usage of FMPClient."""
    async with FMPClient() as client:
        # Run benchmark tests with different batch sizes
        rprint("\n[bold cyan]Running benchmark tests...[/bold cyan]")
        batch_sizes = [740]
        for batch_size in batch_sizes:
            rprint(f"\n[bold magenta]Testing batch size: {batch_size}[/bold magenta]")
            await client.benchmark_m5_current_constituents("sp500", batch_size=batch_size)
            rprint(f"[bold green]Waiting 60 seconds before next batch...[/bold green]")
            await asyncio.sleep(60)

        # Get historical daily data
        df_daily = await client.get_historical_data(
            "AAPL",
            start_date="2024-01-01",
            end_date="2024-01-31",
            interval="1d"
        )
        print("Daily data:", df_daily.head())
        
        # Get historical 5-min data
        df_5min = await client.get_historical_data(
            "AAPL",
            start_date="2024-01-31",
            end_date="2024-02-01",
            interval="5min"
        )
        print("5-min data:", df_5min.head())
        
        # Get exchange symbols
        symbols = await client.get_exchange_symbols()
        print("Number of symbols:", len(symbols))
        
        # Get S&P 500 constituents
        sp500 = await client.get_index_constituents("sp500")
        print("S&P 500 constituents:", sp500.head())
        
        # Get historical constituent changes
        hist_df = await client.get_historical_constituents("sp500")
        print("\nHistorical changes:")
        print(hist_df.head())
        
        # Convert to constituent membership
        const_df = client.convert_to_constituent_df(hist_df)
        print("\nConstituent membership:")
        print(const_df.head())
        

if __name__ == "__main__":
    asyncio.run(main())
