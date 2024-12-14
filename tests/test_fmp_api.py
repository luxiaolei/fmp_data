"""Tests for FMP API client."""

import asyncio
import os
from datetime import datetime, timedelta
from typing import AsyncGenerator, Generator, Sequence

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from src.api.fmp_client import FMPClient
from src.config.settings import APIConfig, Settings, StorageConfig
from src.models.data_types import ExchangeInfo
from src.storage.mongo import MongoStorage

# Load environment variables
load_dotenv()

@pytest.fixture
def api_key() -> Generator[str, None, None]:
    """Get API key from environment."""
    key = os.getenv("FMP_API_KEY")
    if not key:
        pytest.skip("FMP_API_KEY not set")
    yield key


@pytest_asyncio.fixture
async def storage() -> AsyncGenerator[MongoStorage, None]:
    """Create storage fixture."""
    settings = Settings(
        api=APIConfig(key="test_key"),
        storage=StorageConfig(
            mongodb_uri="mongodb://localhost:27017",
            mongodb_db="test_db",
            redis_url="redis://localhost:6379",
            redis_password=""
        )
    )
    
    async with MongoStorage(settings) as storage:
        yield storage


@pytest_asyncio.fixture
async def client(api_key: str, storage: MongoStorage) -> AsyncGenerator[FMPClient, None]:
    """Create FMP client."""
    settings = Settings(
        api=APIConfig(key=api_key),
        storage=StorageConfig(
            mongodb_uri="mongodb://localhost:27017",
            mongodb_db="test_db",
            redis_url="redis://localhost:6379",
            redis_password=""
        )
    )
    async with FMPClient(settings) as client:
        client.storage = storage  # Use the already initialized storage
        yield client


@pytest.mark.asyncio
async def test_get_historical_data(client: FMPClient) -> None:
    """Test getting historical data."""
    df = await client.get_historical_data("AAPL", "2024-01-01", "2024-01-10")
    assert not df.empty
    assert "close" in df.columns
    assert "volume" in df.columns
    assert len(df) > 0


@pytest.mark.asyncio
async def test_get_historical_data_5min(client: FMPClient) -> None:
    """Test getting 5-minute historical data."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    df = await client.get_historical_data("AAPL", start_date, end_date, interval="5min")
    assert not df.empty
    assert "close" in df.columns
    assert len(df) > 0


@pytest.mark.asyncio
async def test_get_company_profile(client: FMPClient) -> None:
    """Test getting company profile."""
    profile = await client.get_company_profile("AAPL")
    assert profile is not None
    assert isinstance(profile, dict)
    assert profile.get("symbol") == "AAPL"
    assert "companyName" in profile


@pytest.mark.asyncio
async def test_get_index_constituents(client: FMPClient) -> None:
    """Test getting index constituents."""
    for index in ["sp500", "nasdaq", "dowjones"]:
        constituents = await client.get_index_constituents(index)
        assert not constituents.empty
        assert "symbol" in constituents.columns
        assert len(constituents) > 0
        if index == "dowjones":
            assert len(constituents) == 30  # Dow Jones has 30 companies


@pytest.mark.asyncio
async def test_get_exchange_symbols(client: FMPClient, storage: MongoStorage) -> None:
    """Test getting exchange symbols."""
    symbols: Sequence[ExchangeInfo] = await client.get_exchange_symbols(exchange="NYSE")
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert all(isinstance(s, ExchangeInfo) for s in symbols)
    assert all(s.exchange == "NYSE" for s in symbols)
    
    # Store in database
    await storage.store_exchange_symbols(exchange="NYSE", symbols=symbols)


@pytest.mark.asyncio
async def test_get_exchange_symbols_filtered(client: FMPClient, storage: MongoStorage) -> None:
    """Test getting exchange symbols filtered by exchange."""
    for exchange_name in ["NYSE", "NASDAQ", "AMEX"]:
        symbols: Sequence[ExchangeInfo] = await client.get_exchange_symbols(exchange=exchange_name)
        assert len(symbols) > 0
        assert all(isinstance(s, ExchangeInfo) for s in symbols)
        assert all(s.exchange == exchange_name for s in symbols)
        
        # Store in database
        await storage.store_exchange_symbols(exchange=exchange_name, symbols=symbols)  # Add named arguments


@pytest.mark.asyncio
async def test_error_handling(client: FMPClient) -> None:
    """Test error handling."""
    with pytest.raises((ValueError, KeyError, Exception)):
        await client.get_historical_data("INVALID_SYMBOL", "2024-01-01", "2024-01-10")


@pytest.mark.asyncio
async def test_rate_limiting(client: FMPClient) -> None:
    """Test rate limiting behavior."""
    # Make multiple rapid requests
    for _ in range(5):
        df = await client.get_historical_data("AAPL", "2024-01-01", "2024-01-02")
        assert not df.empty  # Should not fail due to rate limiting


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", ["AAPL", "MSFT", "GOOGL"])
async def test_multiple_symbols(client: FMPClient, symbol: str) -> None:
    """Test getting data for multiple symbols."""
    df = await client.get_historical_data(symbol, "2024-01-01", "2024-01-10")
    assert not df.empty
    assert "close" in df.columns


@pytest.mark.asyncio
async def test_batch_historical_data(client: FMPClient) -> None:
    """Test getting historical data for multiple symbols in batch."""
    symbols = ["AAPL", "MSFT", "GOOGL"]
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    
    results = await asyncio.gather(*[
        client.get_historical_data(symbol, start_date, end_date)
        for symbol in symbols
    ])
    
    assert len(results) == len(symbols)
    for df in results:
        assert not df.empty
        assert "close" in df.columns