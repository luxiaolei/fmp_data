"""MongoDB storage for FMP data."""

from datetime import UTC, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple, TypedDict, Union, cast

import pandas as pd
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, IndexModel, UpdateOne

from ..config.settings import Settings
from ..models.data_types import AssetType, ExchangeInfo
from ..utils.calendar import ExchangeCalendar
from .protocols import StorageProtocol
from .schemas import COLLECTIONS, DataStatus, SymbolMetadata


class IndexConfig(TypedDict):
    """Type definition for index configuration."""
    
    keys: List[Tuple[str, int]]
    unique: bool


class CollectionConfig(TypedDict):
    """Type definition for collection configuration."""
    validator: Dict[str, Any]
    indexes: List[IndexConfig]


# Add type definitions for statistics
class DateRange(TypedDict):
    start: Optional[datetime]
    end: Optional[datetime]

class Statistics(TypedDict):
    total_symbols: int
    total_daily_points: int
    total_5min_points: int
    date_range: DateRange
    symbols_by_exchange: Dict[str, int]
    symbols_by_type: Dict[str, int]


class MongoStorage(StorageProtocol):
    """MongoDB storage implementation."""
    
    def __init__(self, settings: Settings):
        """Initialize MongoDB storage."""
        self.client = AsyncIOMotorClient(settings.storage.mongodb_uri)
        self.db: AsyncIOMotorDatabase = self.client[settings.storage.mongodb_db]
        self.storage = self
        self.settings = settings
        self._setup_indexes()
        
    def _setup_indexes(self) -> None:
        """Set up MongoDB indexes."""
        index_configs: List[IndexConfig] = [
            {
                "keys": [("symbol", 1), ("date", 1)],
                "unique": True
            },
            {
                "keys": [("exchange", 1)],
                "unique": False
            }
        ]
        
        if hasattr(self, 'db') and self.db is not None:
            indexes = [
                IndexModel(
                    [(key, ASCENDING) for key, _ in config["keys"]],
                    unique=config["unique"]
                )
                for config in index_configs
            ]
            # Create indexes will be handled in init_collections

    async def init_collections(self) -> None:
        """Initialize collections with schemas and indexes."""
        collections = cast(Mapping[str, CollectionConfig], COLLECTIONS)
        
        for collection_name, collection_config in collections.items():
            # Create collection if it doesn't exist
            if collection_name not in await self.db.list_collection_names():
                await self.db.create_collection(
                    collection_name,
                    validator=collection_config["validator"]
                )
            
            # Create indexes
            collection = self.db[collection_name]
            if "indexes" in collection_config:
                indexes = [
                    IndexModel(
                        [(key, ASCENDING) for key, _ in idx["keys"]],
                        unique=idx.get("unique", False)
                    )
                    for idx in collection_config["indexes"]
                ]
                await collection.create_indexes(indexes)
                
        # Initialize MongoDB collections with proper indexes
        try:
            # Index constituents
            await self.db.index_constituents.create_indexes([
                IndexModel([("index", ASCENDING), ("symbol", ASCENDING)], unique=True),
                IndexModel([("last_updated", ASCENDING)])
            ])
            
            # Index exchange symbols
            await self.db.exchange_symbols.create_indexes([
                IndexModel([("exchange", ASCENDING), ("symbol", ASCENDING)], unique=True),
                IndexModel([("last_updated", ASCENDING)])
            ])
            
            # Index company profiles
            await self.db.company_profiles.create_indexes([
                IndexModel([("symbol", ASCENDING)], unique=True),
                IndexModel([("last_updated", ASCENDING)])
            ])
            
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

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
        asset_type = (
            exchange_info.asset_type.value 
            if exchange_info and exchange_info.asset_type 
            else AssetType.STOCK.value
        )
        
        # Get trading days between first and last date
        trading_days = ExchangeCalendar.get_trading_days(
            df["date"].min().to_pydatetime(),
            df["date"].max().to_pydatetime(),
            exchange
        )
        
        # Convert dates to YYYY-MM-DD format for comparison
        actual_dates = set(df["date"].dt.strftime("%Y-%m-%d"))
        expected_dates = set(pd.to_datetime(trading_days).strftime("%Y-%m-%d"))
        
        # Find gaps and convert to datetime
        gap_dates = sorted(list(expected_dates - actual_dates))
        
        # Convert gap dates to datetime objects
        gap_dates = [pd.Timestamp(d).to_pydatetime() for d in gap_dates]
        
        first_row = df.loc[df["date"].idxmin()]
        first_price = float(first_row["close"])  # type: ignore
        
        metadata = SymbolMetadata(
            symbol=symbol,
            first_date=df["date"].min().to_pydatetime(),
            first_price=first_price,
            last_date=df["date"].max().to_pydatetime(),
            data_points=len(df),
            intervals=[interval],
            status=DataStatus.COMPLETE if not gap_dates else DataStatus.PARTIAL,
            exchanges=[exchange],
            asset_type=asset_type,
            gap_dates=gap_dates if gap_dates else None,
            has_gaps=bool(gap_dates)
        )
        
        if gap_dates and len(gap_dates) > 10:
            logger.warning(
                f"{symbol} has {len(gap_dates)} gaps between "
                f"{metadata.first_date:%Y-%m-%d} and {metadata.last_date:%Y-%m-%d}"
            )
            
        # Update metadata
        await self.db.symbol_metadata.update_one(
            {"symbol": symbol},
            {"$set": metadata.dict()},
            upsert=True
        )

    @logger.catch
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
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Get historical price data from MongoDB."""
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

    async def store_company_profile(self, profile: Dict) -> None:
        """Store company profile data."""
        if not profile.get('symbol'):
            logger.warning("Profile missing symbol field")
            return
            
        try:
            await self.db.company_profiles.update_one(
                {"symbol": profile["symbol"]},
                {"$set": {
                    **profile,
                    "last_updated": datetime.now(UTC)  # Use timezone-aware UTC time
                }},
                upsert=True
            )
            logger.debug(f"Updated profile for {profile['symbol']}")
        except Exception as e:
            logger.error(f"Error storing profile: {e}")

    async def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """Get company profile from MongoDB.
        
        Parameters
        ----------
        symbol : str
            Company symbol
            
        Returns
        -------
        Optional[Dict]
            Company profile data or None if not found
        """
        try:
            profile = await self.db.company_profiles.find_one(
                {"symbol": symbol},
                {"_id": 0}
            )
            return profile
        except Exception as e:
            logger.error(f"Error getting profile for {symbol}: {e}")
            return None

    async def store_index_constituents(self, index: str, constituents: pd.DataFrame) -> None:
        """Store index constituents in MongoDB."""
        if constituents.empty:
            logger.warning(f"Empty constituents data for {index}")
            return
        
        try:
            # Convert DataFrame to records
            records = constituents.to_dict("records")
            
            # Add metadata
            for record in records:
                record.update({
                    "index": index,
                    "last_updated": datetime.now(UTC)
                })
            
            # Bulk upsert
            ops = [
                UpdateOne(
                    filter={"index": index, "symbol": r["symbol"]},
                    update={"$set": r},
                    upsert=True
                )
                for r in records
            ]
            
            result = await self.db.index_constituents.bulk_write(ops)
            logger.info(
                f"Stored {result.upserted_count} new and "
                f"updated {result.modified_count} existing constituents "
                f"for {index}"
            )
        except Exception as e:
            logger.error(f"Error storing constituents for {index}: {e}")

    async def store_exchange_symbols(self, exchange: str, symbols: List[ExchangeInfo]) -> None:
        """Store exchange symbols in MongoDB."""
        if not symbols:
            logger.warning(f"Empty symbols list for {exchange}")
            return
        
        try:
            # Convert to records
            records = [
                {
                    **symbol.dict(),
                    "exchange": exchange,
                    "last_updated": datetime.now(UTC)
                }
                for symbol in symbols
            ]
            
            # Bulk upsert
            ops = [
                UpdateOne(
                    filter={"exchange": exchange, "symbol": r["symbol"]},
                    update={"$set": r},
                    upsert=True
                )
                for r in records
            ]
            
            result = await self.db.exchange_symbols.bulk_write(ops)
            logger.info(
                f"Stored {result.upserted_count} new and "
                f"updated {result.modified_count} existing symbols "
                f"for {exchange}"
            )
        except Exception as e:
            logger.error(f"Error storing symbols for {exchange}: {e}")

    async def get_symbol_metadata(self, symbol: str) -> Optional[SymbolMetadata]:
        """Get metadata for a symbol.
        
        Parameters
        ----------
        symbol : str
            Symbol to get metadata for
            
        Returns
        -------
        Optional[SymbolMetadata]
            Symbol metadata if found, None otherwise
        """
        try:
            data = await self.db.symbol_metadata.find_one({"symbol": symbol})
            if data:
                return SymbolMetadata(**data)
            return None
        except Exception as e:
            logger.error(f"Error getting metadata for {symbol}: {e}")
            return None
            
    async def delete_database(self) -> None:
        """
        Delete the entire database after confirmation.
        
        Raises:
            Exception: If there's an error during database deletion
        """
        try:
            await self.client.drop_database(self.settings.storage.mongodb_db)
            logger.warning(f"Database '{self.settings.storage.mongodb_db}' has been deleted")
        except Exception as e:
            logger.error(f"Failed to delete database: {str(e)}")
            raise

    async def get_data_statistics(self) -> Statistics:
        """Get statistics about stored data."""
        try:
            # Get basic statistics
            total_symbols = await self.db.symbol_metadata.count_documents({})
            total_daily_points = await self.db.historical_prices.count_documents({})
            total_5min_points = await self.db.historical_prices_5min.count_documents({})
            
            # Get date range
            date_stats = await self.db.symbol_metadata.aggregate([{
                "$group": {
                    "_id": None,
                    "earliest_date": {"$min": "$first_date"},
                    "latest_date": {"$max": "$last_date"}
                }
            }]).to_list(1)
            
            # Get symbols by exchange
            exchange_stats = await self.db.symbol_metadata.aggregate([{
                "$unwind": "$exchanges"
            }, {
                "$group": {
                    "_id": "$exchanges",
                    "count": {"$sum": 1}
                }
            }]).to_list(None)
            
            # Get symbols by asset type
            asset_stats = await self.db.symbol_metadata.aggregate([{
                "$group": {
                    "_id": "$asset_type",
                    "count": {"$sum": 1}
                }
            }]).to_list(None)
            
            return {
                "total_symbols": total_symbols,
                "total_daily_points": total_daily_points,
                "total_5min_points": total_5min_points,
                "date_range": {
                    "start": date_stats[0].get("earliest_date") if date_stats else None,
                    "end": date_stats[0].get("latest_date") if date_stats else None
                },
                "symbols_by_exchange": {
                    str(stat.get("_id", "")): stat.get("count", 0) 
                    for stat in exchange_stats
                },
                "symbols_by_type": {
                    str(stat.get("_id", "")): stat.get("count", 0) 
                    for stat in asset_stats
                }
            }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                "total_symbols": 0,
                "total_daily_points": 0,
                "total_5min_points": 0,
                "date_range": {"start": None, "end": None},
                "symbols_by_exchange": {},
                "symbols_by_type": {}
            }