"""Command line interface for FMP data."""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

import click
import pandas as pd
from loguru import logger

from ..api.fmp_client import FMPClient
from ..config.settings import load_config
from ..storage.mongo import MongoStorage


@click.group()
def cli():
    """FMP data management CLI."""
    pass


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
@click.option("--symbol", help="Stock symbol to download")
@click.option("--start-date", help="Start date (YYYY-MM-DD)", default="2004-01-01")
@click.option("--end-date", help="End date (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"))
@click.option("--interval", default="1d", help="Data interval (1d or 5min)")
def download(
    config: Optional[str],
    symbol: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    interval: str
):
    """Download historical data."""
    async def _run():
        settings = load_config(Path(config) if config else None)
        storage = MongoStorage(settings)
        
        async with FMPClient(settings) as client:
            if symbol:
                symbols = [symbol]
            else:
                # Download all symbols from all exchanges
                exchange_infos = await client.get_all_exchange_symbols()
                symbols = [info.symbol for info in exchange_infos]
                
            for sym in symbols:
                df = await client.get_historical_data(
                    sym,
                    start_date or "2023-01-01",
                    end_date or pd.Timestamp.now(),
                    interval
                )
                if not df.empty:
                    logger.info(f"Downloaded {len(df)} records for {sym}")
    
    asyncio.run(_run())


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
def update_indexes(config: Optional[str]):
    """Update index constituents."""
    async def _run():
        settings = load_config(Path(config) if config else None)
        storage = MongoStorage(settings)
        
        async with FMPClient(settings) as client:
            for index in ["sp500", "nasdaq", "dowjones"]:
                df = await client.get_index_constituents(index)
                # Store in MongoDB
                # Implementation...
    
    asyncio.run(_run())


if __name__ == "__main__":
    cli() 