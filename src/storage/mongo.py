"""MongoDB storage for FMP data."""

from datetime import datetime
from typing import Optional, Union

import pandas as pd
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, IndexModel, UpdateOne

from ..config.settings import Settings
from ..models.data_types import AssetType, ExchangeInfo
from ..utils.calendar import ExchangeCalendar
from .schemas import COLLECTIONS, DataStatus, SymbolMetadata


class MongoStorage:
    """MongoDB storage for FMP data."""
    
    def __init__(self, settings: Settings):
        """Initialize MongoDB storage."""
        self.client = AsyncIOMotorClient(settings.storage.mongodb_uri)
        self.db = self.client[settings.storage.mongodb_db]
        
    async def init_collections(self) -> None:
        """Initialize collections with schemas and indexes."""
        for collection_name, config in COLLECTIONS.items():
            # Create collection if it doesn't exist
            if collection_name not in await self.db.list_collection_names():
                await self.db.create_collection(
                    collection_name,
                    validator=config["validator"]
                )
            
            # Create indexes
            collection = self.db[collection_name]
            indexes = [
                IndexModel(
                    [(key, ASCENDING) for key, _ in idx["keys"]],
                    unique=idx.get("unique", False)
                )
                for idx in config["indexes"]
            ]
            await collection.create_indexes(indexes)
                
    async def update_symbol_metadata(
        self,
        symbol: str,
        df: pd.DataFrame,
        interval: str = "1d",
        exchange_info: Optional[ExchangeInfo] = None
    ) -> None:
        """Update symbol metadata."""
        if df.empty:
            return
            
        exchange = (exchange_info.exchange if exchange_info else "NYSE")
        metadata = SymbolMetadata(
            symbol=symbol,
            first_date=df["date"].min().to_pydatetime(),
            last_date=df["date"].max().to_pydatetime(),
            data_points=len(df),
            intervals=[interval],
            status=DataStatus.COMPLETE,
            exchanges=[exchange],
            asset_type=exchange_info.asset_type if exchange_info else AssetType.STOCK,
            gap_dates=[]
        )
        
        # Get expected trading dates
        if interval == "1d":
            expected_dates = set(ExchangeCalendar.get_trading_days(
                metadata.first_date,
                metadata.last_date,
                exchange
            ))
        else:
            expected_dates = set(ExchangeCalendar.get_trading_minutes(
                metadata.first_date,
                metadata.last_date,
                exchange,
                interval
            ))
        
        # Find missing dates
        actual_dates = set(df["date"].dt.to_pydatetime())
        gap_dates = sorted(expected_dates - actual_dates)
        
        if gap_dates:
            metadata.has_gaps = True
            metadata.gap_dates = gap_dates
            metadata.status = DataStatus.PARTIAL
            
            if len(gap_dates) > 10:
                logger.warning(
                    f"{symbol} has {len(gap_dates)} gaps between "
                    f"{metadata.first_date} and {metadata.last_date}"
                )
            
        # Update metadata using model_dump instead of dict
        await self.db.symbol_metadata.update_one(
            {"symbol": symbol},
            {"$set": metadata.model_dump()},
            upsert=True
        )

    async def store_historical_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str = "1d"
    ) -> None:
        """Store historical price data."""
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {symbol}")
            return
            
        # Convert dates to timezone-naive for storage
        df = df.copy()
        df["date"] = df["date"].dt.tz_localize(None)
        logger.debug(f"Storing {len(df)} records for {symbol} with columns: {df.columns.tolist()}")
        logger.debug(f"DataFrame dtypes: {df.dtypes}")
        
        collection = (
            self.db.historical_prices_5min
            if interval == "5min"
            else self.db.historical_prices
        )
        logger.debug(f"Using collection: {collection.name}")
        
        # Convert DataFrame to records
        records = df.to_dict("records")
        logger.debug(f"Sample record for {symbol}: {records[0] if records else 'No records'}")
        
        # Create proper UpdateOne operations
        ops = [
            UpdateOne(
                filter={"symbol": r["symbol"], "date": r["date"]},
                update={"$set": r},
                upsert=True
            )
            for r in records
        ]
        logger.debug(f"Created {len(ops)} update operations")
        
        try:
            result = await collection.bulk_write(ops)
            logger.debug(f"Bulk write result: {result.bulk_api_result}")
            logger.info(
                f"Stored {result.upserted_count} new and "
                f"updated {result.modified_count} existing records "
                f"for {symbol}"
            )
            
            # Update metadata
            await self.update_symbol_metadata(symbol, df, interval)
            
        except Exception as e:
            logger.error(f"Error storing data for {symbol}: {e}", exc_info=True)
            logger.error(f"Failed record sample: {records[0] if records else 'No records'}")
            
    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Get historical price data from storage."""
        collection = (
            self.db.historical_prices_5min
            if interval == "5min"
            else self.db.historical_prices
        )
        logger.debug(f"Getting data from collection: {collection.name}")
        
        # Convert dates to timezone-naive datetime and normalize to start/end of day
        start_dt = pd.Timestamp(start_date).tz_localize(None).normalize()  # Start of day
        end_dt = (
            pd.Timestamp(end_date).tz_localize(None).normalize() + 
            pd.Timedelta(days=1)  # End of day
        )
        logger.debug(f"Query date range (naive): {start_dt} to {end_dt}")
        
        # Query data
        query = {
            "symbol": symbol,
            "date": {"$gte": start_dt, "$lt": end_dt}  # Changed $lte to $lt since we added a day
        }
        logger.debug(f"MongoDB query: {query}")
        
        cursor = collection.find(query, {"_id": 0})
        
        # Convert to DataFrame
        data = await cursor.to_list(length=None)
        logger.debug(f"Query returned {len(data)} records")
        if not data:
            logger.debug("No data found in MongoDB")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        logger.debug(f"Created DataFrame with columns: {df.columns.tolist()}")
        df["date"] = pd.to_datetime(df["date"])
        
        # Convert back to original timezone if needed
        original_tz = pd.Timestamp(start_date).tz
        if original_tz:
            df["date"] = df["date"].dt.tz_localize(original_tz)
            
        return df.sort_values("date") 