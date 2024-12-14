"""MongoDB storage for FMP data using Beanie ODM."""

from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from beanie import init_beanie
from beanie.odm.enums import SortDirection
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from ..config.settings import Settings
from ..models.data_types import AssetType, ExchangeInfo
from ..utils.calendar import ExchangeCalendar
from .protocols import StorageProtocol
from .schemas import (
    CompanyProfile,
    ExchangeSymbol,
    HistoricalPrice,
    IndexConstituent,
    SymbolMetadata,
)


class MongoStorage(StorageProtocol):
    """MongoDB storage implementation using Beanie ODM."""
    
    def __init__(self, settings: Settings):
        """Initialize MongoDB storage."""
        self.settings = settings
        self.client = None
        self._storage = self
        self._initialized = False
        self._progress = None
        self._task_id = None

    async def connect(self) -> None:
        """Establish MongoDB connection."""
        if not self.client:
            # Build MongoDB URI with authentication
            uri = self.settings.storage.mongodb_uri
            if self.settings.storage.mongodb_user and self.settings.storage.mongodb_pass:
                # Split URI into parts and handle authentication properly
                parts = uri.split("://")
                if len(parts) == 2:
                    protocol, address = parts
                    # Add authentication credentials and authSource with proper format
                    uri = f"{protocol}://{self.settings.storage.mongodb_user}:{self.settings.storage.mongodb_pass}@{address}/{self.settings.storage.mongodb_db}?authSource={self.settings.storage.mongodb_db}"

            try:
                self.client = AsyncIOMotorClient(
                    uri,
                    serverSelectionTimeoutMS=30000,
                    connectTimeoutMS=30000,
                    socketTimeoutMS=30000,
                    maxPoolSize=50,
                    waitQueueTimeoutMS=30000
                )
                
                if not self.client:
                    raise ValueError("Failed to initialize MongoDB client")

                # Test connection with proper type checking
                client = self.client
                await client.admin.command('ping')
                logger.info("Successfully connected to MongoDB")

                # Initialize Beanie with all document models
                document_models = [
                    HistoricalPrice,
                    SymbolMetadata,
                    IndexConstituent,
                    ExchangeSymbol,
                    CompanyProfile
                ]
                
                database = client[self.settings.storage.mongodb_db]
                await init_beanie(
                    database=database,
                    document_models=document_models
                )
                self._initialized = True
                
            except Exception as e:
                logger.error(f"MongoDB connection error: {e}")
                raise

    async def init_collections(self) -> None:
        """Initialize collections with schemas and indexes."""
        if not self.client:
            await self.connect()
            
        if not self.client:  # Double check after connect
            raise ValueError("Failed to initialize MongoDB client")
            
        # Initialize Beanie with all document models
        document_models = [
            HistoricalPrice,
            SymbolMetadata,
            IndexConstituent,
            ExchangeSymbol,
            CompanyProfile
        ]
        
        motor_db = self.client[self.settings.storage.mongodb_db]
        await init_beanie(
            database=motor_db,
            document_models=document_models
        )

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            self.client.close()

    @property
    def storage(self) -> StorageProtocol:
        """Get storage instance."""
        return self._storage

    async def store_historical_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str = "1d"
    ) -> None:
        """Store historical price data."""
        if df.empty:
            return

        # Convert DataFrame to Beanie documents
        documents = []
        for _, row in df.iterrows():
            # Handle NaN values with defaults
            doc_data = {
                "symbol": symbol,
                "date": row["date"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "unadjusted_volume": int(row.get("unadjusted_volume", 0)) if pd.notna(row.get("unadjusted_volume")) else 0,
                "change": float(row["change"]),
                "label": row["label"],
            }
            
            # Add optional fields if they exist and are not NaN
            for field in ["adj_close", "change_percent", "vwap", "change_over_time"]:
                if field in row and pd.notna(row[field]):
                    doc_data[field] = float(row[field])
            
            documents.append(HistoricalPrice(**doc_data))
        
        # Use Beanie's bulk insert
        await HistoricalPrice.insert_many(documents)
        
    async def get_historical_data(
        self,
        symbol: str,
        start_date: Union[str, datetime, pd.Timestamp],
        end_date: Union[str, datetime, pd.Timestamp],
        interval: str = "1d"
    ) -> pd.DataFrame:
        """Get historical price data from MongoDB."""
        # Convert dates to timezone-naive datetime
        start_dt = pd.Timestamp(start_date).tz_localize(None).normalize()
        end_dt = pd.Timestamp(end_date).tz_localize(None).normalize() + pd.Timedelta(days=1)
        
        # Query data
        query = {
            "symbol": symbol,
            "date": {"$gte": start_dt, "$lt": end_dt}
        }
        
        # Fetch data using Beanie
        data = await HistoricalPrice.find(query).to_list()
        
        if not data:
            logger.debug("No data found in MongoDB")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([doc.dict() for doc in data])
        df["date"] = pd.to_datetime(df["date"])
        
        # Convert back to original timezone if needed
        original_tz = pd.Timestamp(start_date).tz
        if original_tz:
            df["date"] = df["date"].dt.tz_localize(original_tz)
            
        return df.sort_values("date")

    async def store_index_constituents(self, index: str, constituents: pd.DataFrame) -> None:
        """Store index constituents in MongoDB."""
        if constituents.empty:
            logger.warning(f"Empty constituents data for {index}")
            return
        
        # Convert DataFrame to documents with explicit string keys
        documents = [
            IndexConstituent(
                index=index,
                **{str(k): v for k, v in record.items()},  # Ensure string keys
                last_updated=datetime.now(UTC)
            )
            for record in constituents.to_dict("records")
        ]
        
        # Delete existing constituents for this index
        await IndexConstituent.find({"index": index}).delete()
        
        # Insert new constituents
        await IndexConstituent.insert_many(documents)
        logger.info(f"Stored {len(documents)} constituents for {index}")

    async def store_exchange_symbols(self, exchange: str, symbols: List[ExchangeInfo]) -> None:
        """Store exchange symbols in MongoDB."""
        if not symbols:
            logger.warning(f"Empty symbols list for {exchange}")
            return
        
        # Convert symbols to documents
        documents = []
        for symbol in symbols:
            # Create a dictionary from the symbol data, excluding the exchange field
            symbol_data = symbol.dict()
            if "exchange" in symbol_data:
                del symbol_data["exchange"]  # Remove exchange from dict to avoid duplicate
            
            # Create document with exchange parameter
            doc = ExchangeSymbol(
                exchange=exchange,
                **symbol_data,
                last_updated=datetime.now(UTC)
            )
            documents.append(doc)
        
        # Delete existing symbols for this exchange
        await ExchangeSymbol.find({"exchange": exchange}).delete()
        
        # Insert new symbols
        await ExchangeSymbol.insert_many(documents)
        logger.info(f"Stored {len(documents)} symbols for {exchange}")

    async def store_company_profile(self, profile: Dict[str, Any]) -> None:
        """Store company profile data."""
        if not profile or not profile.get('symbol'):
            logger.warning("Profile missing symbol field")
            return
        
        try:
            # Create profile document
            doc = CompanyProfile(
                symbol=profile["symbol"],
                data=profile or {},  # Ensure data is never None
                last_updated=datetime.now(UTC)
            )
            
            # Use update_one with upsert
            await CompanyProfile.find_one(
                {"symbol": profile["symbol"]}
            ).update_one(
                {"$set": doc.dict(exclude={"_id"})},
                upsert=True
            )
            logger.debug(f"Updated profile for {profile['symbol']}")
        except Exception as e:
            logger.error(f"Error storing profile for {profile.get('symbol', 'unknown')}: {e}")

    async def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get company profile from MongoDB."""
        try:
            profile = await CompanyProfile.find_one({"symbol": symbol})
            if profile and profile.data:
                return profile.data
            return None
        except Exception as e:
            logger.error(f"Error getting profile for {symbol}: {e}")
            return None

    async def get_data_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored data."""
        try:
            # Get basic counts with timeout
            total_symbols = await SymbolMetadata.find({}, max_time_ms=5000).count()
            total_daily_points = await HistoricalPrice.find({}, max_time_ms=5000).count()

            # Get most recent metadata for date range
            latest_symbol = await SymbolMetadata.find(
                {},  # First argument is the filter
                {"first_date": 1, "last_date": 1, "_id": 0},  # Second argument is projection
                max_time_ms=5000
            ).sort(("last_date", SortDirection.DESCENDING)).limit(1).to_list(1)

            # Get top 10 exchanges by symbol count
            exchange_pipeline = [
                {"$unwind": "$exchanges"},
                {"$group": {"_id": "$exchanges", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            exchange_stats = await SymbolMetadata.aggregate(exchange_pipeline).to_list(10)

            # Get asset types summary
            asset_pipeline = [
                {"$group": {"_id": "$asset_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]
            asset_stats = await SymbolMetadata.aggregate(asset_pipeline).to_list(5)

            # Extract date range from latest symbol
            date_range = {
                "start": latest_symbol[0].get("first_date") if latest_symbol else None,
                "end": latest_symbol[0].get("last_date") if latest_symbol else None
            }

            return {
                "total_symbols": total_symbols,
                "total_daily_points": total_daily_points,
                "date_range": date_range,
                "top_exchanges": {
                    ex["_id"]: ex["count"] 
                    for ex in exchange_stats
                },
                "asset_types": {
                    t["_id"]: t["count"] 
                    for t in asset_stats
                }
            }
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                "total_symbols": 0,
                "total_daily_points": 0,
                "date_range": {"start": None, "end": None},
                "top_exchanges": {},
                "asset_types": {}
            }

    async def delete_database(self) -> None:
        """Delete the database."""
        if self.client:
            await self.client.drop_database(self.settings.storage.mongodb_db)

    async def get_symbol_metadata(self, symbol: str) -> Optional[SymbolMetadata]:
        """Get metadata for a symbol."""
        try:
            metadata = await SymbolMetadata.find_one({"symbol": symbol})
            return metadata
        except Exception as e:
            logger.error(f"Error getting metadata for {symbol}: {e}")
            return None
            
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
        gap_dates = sorted(expected_dates.difference(actual_dates))
        gap_dates = [pd.Timestamp(d) for d in gap_dates]
        gap_dates = [d.to_pydatetime() for d in gap_dates]
        
        first_row = df.loc[df["date"].idxmin()]
        first_price = float(first_row["close"].item())
        
        # Create metadata document
        doc = SymbolMetadata(
            symbol=symbol,
            first_date=df["date"].min().to_pydatetime(),
            first_price=first_price,
            last_date=df["date"].max().to_pydatetime(),
            data_points=len(df),
            intervals=[interval],
            status="complete" if not gap_dates else "partial",
            exchanges=[exchange],
            asset_type=asset_type,
            gap_dates=gap_dates if gap_dates else None,
            has_gaps=bool(gap_dates)
        )
        await SymbolMetadata.save(doc)  # Use class method instead of instance method
        
        if gap_dates and len(gap_dates) > 10:
            logger.warning(
                f"{symbol} has {len(gap_dates)} gaps between "
                f"{doc.first_date:%Y-%m-%d} and {doc.last_date:%Y-%m-%d}"
            )
