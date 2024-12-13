"""Integration tests for FMP data client."""

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from loguru import logger

from src.api.fmp_client import FMPClient
from src.config.settings import load_config
from src.storage.mongo import MongoStorage


@pytest.mark.asyncio
async def test_full_pipeline():
    """Test full data pipeline with real API and DB."""
    settings = load_config()
    storage = MongoStorage(settings)
    
    # Initialize collections
    await storage.init_collections()
    
    # Test with a small set of data
    symbols = ["AAPL", "MSFT"]  # Just test with 2 symbols
    ny_tz = ZoneInfo("America/New_York")
    
    # Use a known trading day in the past
    test_date = datetime(2023, 12, 12, tzinfo=ny_tz)  # Use last Tuesday
    start_date = test_date
    end_date = test_date + timedelta(days=1)
    
    logger.info(f"Testing data pipeline from {start_date} to {end_date}")
    
    async with FMPClient(settings) as client:
        # Test daily data
        for symbol in symbols:
            logger.info(f"Fetching daily data for {symbol}")
            df = await client.get_historical_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval="1d"
            )
            assert not df.empty, f"No data returned for {symbol}"
            logger.info(f"Got {len(df)} daily records for {symbol}")
            
            # Wait a bit for background task to complete
            await asyncio.sleep(1)
            
            # Verify data is in MongoDB
            stored_df = await storage.get_historical_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval="1d"
            )
            assert not stored_df.empty, f"Data not stored for {symbol}"
            assert len(stored_df) == len(df), "Data count mismatch"
            
        # Test 5-minute data for market hours
        market_start = test_date.replace(hour=9, minute=30)
        market_end = test_date.replace(hour=16, minute=0)
        
        for symbol in symbols:
            logger.info(f"Fetching 5-min data for {symbol}")
            df = await client.get_historical_data(
                symbol=symbol,
                start_date=market_start,
                end_date=market_end,
                interval="5min"
            )
            assert not df.empty, f"No 5-min data for {symbol}"
            logger.info(f"Got {len(df)} 5-min records for {symbol}")
            
            # Wait a bit for background task to complete
            await asyncio.sleep(1)
            
            # Verify data is in MongoDB
            stored_df = await storage.get_historical_data(
                symbol=symbol,
                start_date=market_start,
                end_date=market_end,
                interval="5min"
            )
            assert not stored_df.empty, f"5-min data not stored for {symbol}"
            assert len(stored_df) == len(df), "5-min data count mismatch"
            
        # Test index constituents
        logger.info("Fetching S&P 500 constituents")
        constituents = await client.get_index_constituents("sp500")
        assert not constituents.empty, "No constituents returned"
        logger.info(f"Got {len(constituents)} S&P 500 constituents")
    
    # Cleanup test data
    logger.info("Cleaning up test data")
    for symbol in symbols:
        await storage.db.historical_prices.delete_many({"symbol": symbol})
        await storage.db.historical_prices_5min.delete_many({"symbol": symbol})
    
    logger.info("Integration test completed successfully")


if __name__ == "__main__":
    asyncio.run(test_full_pipeline()) 