"""Command line interface for FMP data client."""

import asyncio
import sys
from typing import Optional

import click
import pandas as pd
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.theme import Theme
from tqdm import tqdm

from src.api.fmp_client import FMPClient
from src.config.settings import Settings, load_settings
from src.storage.mongo import MongoStorage
from src.utils.calendar import get_latest_market_day

# Configure rich console with custom theme
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "red",
    "success": "green",
})
console = Console(theme=custom_theme)

# Configure logger
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="INFO")  # Add new handler with INFO level

@click.group()
def cli():
    """FMP Data Management CLI"""
    console.print(Panel.fit(
        "[bold cyan]FMP Data Management CLI[/bold cyan]\n"
        "[dim]A tool for downloading and managing financial market data[/dim]"
    ))

@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
def interactive(config: Optional[str]):
    """Interactive mode for data management."""
    async def _run():
        settings = load_settings()  # Remove Path argument
        storage = MongoStorage(settings)
        await storage.connect()
        
        while True:
            console.print("\n[bold cyan]Available Actions:[/bold cyan]")
            table = Table(show_header=False, box=None)
            table.add_row("[cyan]1[/cyan]", "Download historical data")
            table.add_row("[cyan]2[/cyan]", "Update index constituents")
            table.add_row("[cyan]3[/cyan]", "View data statistics")
            table.add_row("[cyan]4[/cyan]", "Run routine update")
            table.add_row("[cyan]5[/cyan]", "Set log level")
            table.add_row("[cyan]6[/cyan]", "Delete database")
            table.add_row("[cyan]7[/cyan]", "Exit")
            console.print(table)
            
            choice = click.prompt(
                "\nSelect an action",
                type=click.Choice(["1", "2", "3", "4", "5", "6", "7"]),
                show_choices=False
            )
            
            if choice == "1":
                symbol = click.prompt("Enter symbol (leave empty for all symbols)", default="")
                start_date = click.prompt("Start date (YYYY-MM-DD)", default="2004-01-01")
                default_end_date = get_latest_market_day().strftime("%Y-%m-%d")
                end_date = click.prompt(
                    "End date (YYYY-MM-DD)",
                    default=default_end_date
                )
                interval = click.prompt(
                    "Interval",
                    type=click.Choice(["1d", "5min"]),
                    default="1d"
                )
                
                async with FMPClient(settings) as client:
                    if symbol:
                        df = await client.get_historical_data(symbol, start_date, end_date, interval)
                        if not df.empty:
                            await storage.store_historical_data(df, symbol, interval)
                    else:
                        BATCH_SIZE = 175
                        MAX_CONCURRENT_BATCHES = 4
                        
                        symbols = await client.get_all_exchange_symbols()
                        batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
                        
                        async def download_symbol_batch(batch, start_date, end_date, interval, storage, client):
                            """Download historical data for a batch of symbols."""
                            try:
                                tasks = []
                                for sym in batch:
                                    tasks.append(
                                        client.get_historical_data(
                                            sym.symbol,
                                            start_date,
                                            end_date,
                                            interval
                                        )
                                    )
                                
                                # Use asyncio.gather with return_exceptions and wrap with tqdm
                                with tqdm(
                                    total=len(tasks),
                                    desc=f"Processing batch of {len(batch)} symbols",
                                    leave=False
                                ) as pbar:
                                    results = await asyncio.gather(*tasks, return_exceptions=True)
                                    pbar.update(len(tasks))
                                
                                for sym, result in zip(batch, results):
                                    try:
                                        if isinstance(result, Exception):
                                            logger.error(f"Error downloading {sym.symbol}: {result}")
                                            continue
                                            
                                        if isinstance(result, pd.DataFrame) and not result.empty:
                                            await storage.store_historical_data(result, sym.symbol, interval)
                                        else:
                                            logger.warning(f"No data for {sym.symbol}")
                                    except Exception as e:
                                        logger.error(f"Error processing {sym.symbol}: {e}")
                            except Exception as e:
                                logger.error(f"Batch processing error: {e}")

                        # Use single tqdm for overall progress
                        with tqdm(
                            total=len(symbols),
                            desc="Downloading historical data",
                            unit="symbols"
                        ) as pbar:
                            for i in range(0, len(batches), MAX_CONCURRENT_BATCHES):
                                batch_group = batches[i:i + MAX_CONCURRENT_BATCHES]
                                await asyncio.gather(*(
                                    download_symbol_batch(
                                        batch, start_date, end_date, interval, storage, client
                                    ) for batch in batch_group
                                ))
                                # Update by the total number of symbols in this batch group
                                pbar.update(sum(len(batch) for batch in batch_group))
            
            elif choice == "2":
                index = click.prompt(
                    "Select index",
                    type=click.Choice(["sp500", "nasdaq", "dowjones"]),
                    default="sp500"
                )
                async with FMPClient(settings) as client:
                    constituents = await client.get_index_constituents(index)
                    if not constituents.empty:
                        await storage.store_index_constituents(index, constituents)
                        console.print(f"[green]Updated {len(constituents)} constituents for {index}")
                    else:
                        console.print(f"[yellow]No constituents found for {index}")
                        
            elif choice == "3":
                # View data statistics
                stats = await storage.get_data_statistics()
                if stats:
                    console.print("\n[bold cyan]Data Statistics:[/bold cyan]")
                    
                    # Create statistics table
                    table = Table(show_header=True, header_style="bold magenta")
                    table.add_column("Metric", style="cyan")
                    table.add_column("Value", style="green")
                    
                    # Add rows with safe dictionary access
                    table.add_row("Total Symbols", str(stats.get("total_symbols", 0)))
                    table.add_row("Daily Data Points", str(stats.get("total_daily_points", 0)))
                    table.add_row("5-min Data Points", str(stats.get("total_5min_points", 0)))
                    
                    date_range = stats.get("date_range", {})
                    if date_range.get("start"):
                        date_str = (
                            f"{date_range.get('start'):%Y-%m-%d} to "
                            f"{date_range.get('end'):%Y-%m-%d}"
                        )
                        table.add_row("Date Range", date_str)
                    
                    # Add exchange distribution
                    exchange_stats = stats.get("symbols_by_exchange", {})
                    if exchange_stats:
                        table.add_section()
                        table.add_row("[bold]Symbols by Exchange[/bold]", "")
                        for exchange, count in exchange_stats.items():
                            table.add_row(f"  {exchange}", str(count))
                    
                    # Add asset type distribution
                    type_stats = stats.get("symbols_by_type", {})
                    if type_stats:
                        table.add_section()
                        table.add_row("[bold]Symbols by Type[/bold]", "")
                        for asset_type, count in type_stats.items():
                            table.add_row(f"  {asset_type}", str(count))
                    
                    console.print(table)
                else:
                    console.print("[yellow]No statistics available[/yellow]")
                
            elif choice == "4":
                days = click.prompt("Number of days to update", type=int, default=7)
                await run_routine_update(settings, storage, days)
                
            elif choice == "5":
                # Add log level selection
                print("\nLog Levels:")
                print("1. DEBUG")
                print("2. INFO")
                print("3. WARNING")
                print("4. ERROR")
                log_choice = input("\nSelect log level (1-4): ")
                
                levels = {
                    "1": "DEBUG",
                    "2": "INFO",
                    "3": "WARNING",
                    "4": "ERROR"
                }
                
                if log_choice in levels:
                    logger.remove()  # Remove existing handlers
                    logger.add(sys.stderr, level=levels[log_choice])
                    print(f"\nLog level set to: {levels[log_choice]}")
                else:
                    print("\nInvalid choice. Log level unchanged.")
                
                continue
            
            elif choice == "6":
                # Database deletion confirmation
                console.print("[bold red]WARNING: This will delete all data from the database![/bold red]")
                confirmation = click.prompt(
                    "Type 'DELETE' to confirm database deletion",
                    type=str,
                    default=""
                )
                
                if confirmation == "DELETE":
                    try:
                        await storage.delete_database()
                        console.print("[green]Database successfully deleted[/green]")
                    except Exception as e:
                        console.print(f"[red]Error deleting database: {str(e)}[/red]")
                else:
                    console.print("[yellow]Database deletion cancelled[/yellow]")
                
            elif choice == "7":
                if click.confirm("Are you sure you want to exit?"):
                    break
                    
    try:
        asyncio.run(_run())
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.ClickException(str(e))


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
def update_indexes(config: Optional[str]):
    """Update index constituents."""
    async def _run():
        try:
            settings = load_settings()
            
            async with FMPClient(settings) as client:
                for index in ["sp500", "nasdaq", "dowjones"]:
                    logger.info(f"Updating {index} constituents...")
                    df = await client.get_index_constituents(index)
                    if not df.empty:
                        # TODO: Implement storage logic
                        logger.info(f"Downloaded {len(df)} constituents for {index}")
                    else:
                        logger.warning(f"No constituents found for {index}")
        
        except Exception as e:
            logger.error(f"Error in update_indexes: {str(e)}", exc_info=True)
            raise click.ClickException(str(e))
    
    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")
        raise click.ClickException(str(e))


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
@click.option("--symbol", default="AAPL", help="Symbol to test with")
def test(config: Optional[str], symbol: str):
    """Run system tests."""
    async def _run():
        try:
            settings = load_settings()
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                refresh_per_second=1
            ) as progress:
                task = progress.add_task("[cyan]Running tests...", total=5)
                
                async with FMPClient(settings) as client:
                    # Test 1: Company Profile
                    progress.update(task, description="[cyan]Testing company profile...")
                    profile = await client.get_company_profile(symbol)
                    if profile:
                        progress.update(task, advance=1, description="[green]Profile test passed")
                    else:
                        progress.update(task, advance=1, description="[red]Profile test failed")
                    
                    # Test 2: Daily Data
                    progress.update(task, description="[cyan]Testing daily data...")
                    daily_df = await client.get_historical_data(
                        symbol,
                        start_date="2024-01-01",
                        end_date=get_latest_market_day(),
                        interval="1d"
                    )
                    if not daily_df.empty:
                        progress.update(task, advance=1, description="[green]Daily data test passed")
                    else:
                        progress.update(task, advance=1, description="[red]Daily data test failed")
                    
                    # Test 3: 5min Data
                    progress.update(task, description="[cyan]Testing 5min data...")
                    m5_df = await client.get_historical_data(
                        symbol,
                        start_date=pd.Timestamp.now() - pd.Timedelta(days=5),
                        end_date=pd.Timestamp.now(),
                        interval="5min"
                    )
                    if not m5_df.empty:
                        progress.update(task, advance=1, description="[green]5min data test passed")
                    else:
                        progress.update(task, advance=1, description="[red]5min data test failed")
                    
                    # Test 4: Exchange Symbols
                    progress.update(task, description="[cyan]Testing exchange symbols...")
                    symbols = await client.get_all_exchange_symbols()
                    if symbols:
                        progress.update(task, advance=1, description="[green]Exchange symbols test passed")
                    else:
                        progress.update(task, advance=1, description="[red]Exchange symbols test failed")
                    
                    # Test 5: Index Constituents
                    progress.update(task, description="[cyan]Testing index constituents...")
                    constituents = await client.get_index_constituents("sp500")
                    if not constituents.empty:
                        progress.update(task, advance=1, description="[green]Index constituents test passed")
                    else:
                        progress.update(task, advance=1, description="[red]Index constituents test failed")
                    
                    # Print summary
                    click.echo("\nTest Summary:")
                    click.echo(f"Profile data: {'✓' if profile else '✗'}")
                    click.echo(f"Daily data: {'✓' if not daily_df.empty else '✗'} ({len(daily_df)} rows)")
                    click.echo(f"5min data: {'✓' if not m5_df.empty else '✗'} ({len(m5_df)} rows)")
                    click.echo(f"Exchange symbols: {'✓' if symbols else '✗'} ({len(symbols)} symbols)")
                    click.echo(f"Index constituents: {'✓' if not constituents.empty else '✗'} ({len(constituents)} constituents)")
                    
        except Exception as e:
            logger.error(f"Test failed: {str(e)}", exc_info=True)
            raise click.ClickException(str(e))
            
    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"Command failed: {str(e)}")
        raise click.ClickException(str(e))


async def run_routine_update(settings: Settings, storage: MongoStorage, days: int) -> None:
    """Run routine updates for all data."""
    BATCH_SIZE = 175  # 700/4 for optimal rate limit usage
    MAX_CONCURRENT_BATCHES = 4  # Number of batches to process concurrently
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        overall = progress.add_task("[cyan]Running routine update...", total=4)
        
        async with FMPClient(settings) as client:
            # 1. Update exchange symbols
            progress.update(overall, description="[cyan]Updating exchange symbols...")
            exchanges = ["NYSE", "NASDAQ", "AMEX"]
            for exchange in exchanges:
                symbols = await client.get_exchange_symbols(exchange)
                await storage.store_exchange_symbols(exchange, symbols)
            progress.advance(overall)
            
            # 2. Update index constituents
            progress.update(overall, description="[cyan]Updating index constituents...")
            indexes = ["sp500", "nasdaq", "dowjones"]
            for index in indexes:
                constituents = await client.get_index_constituents(index)
                await storage.store_index_constituents(index, constituents)
            progress.advance(overall)
            
            # 3. Update company profiles in batches
            progress.update(overall, description="[cyan]Updating company profiles...")
            symbols = await client.get_all_exchange_symbols()
            profile_task = progress.add_task(
                "[cyan]Updating profiles",
                total=len(symbols)
            )
            
            async def update_profile_batch(batch):
                """Update profiles for a batch of symbols."""
                tasks = []
                for symbol in batch:
                    tasks.append(client.get_company_profile(symbol.symbol))
                profiles = await asyncio.gather(*tasks, return_exceptions=True)
                
                for symbol, result in zip(batch, profiles):
                    try:
                        if isinstance(result, Exception):
                            logger.error(f"Error getting profile for {symbol.symbol}: {result}")
                            progress.advance(profile_task)
                            continue
                        
                        if isinstance(result, dict):
                            await storage.store_company_profile(result)
                        progress.advance(profile_task)
                    except Exception as e:
                        logger.error(f"Error storing profile for {symbol.symbol}: {e}")
                        progress.advance(profile_task)
            
            # Process profile batches concurrently
            batches = [
                symbols[i:i + BATCH_SIZE]
                for i in range(0, len(symbols), BATCH_SIZE)
            ]
            
            # Process batches in groups of MAX_CONCURRENT_BATCHES
            for i in range(0, len(batches), MAX_CONCURRENT_BATCHES):
                batch_group = batches[i:i + MAX_CONCURRENT_BATCHES]
                await asyncio.gather(*(
                    update_profile_batch(batch) for batch in batch_group
                ))
            
            progress.advance(overall)
            
            # 4. Update historical data in batches
            progress.update(overall, description="[cyan]Updating historical data...")
            end_date = get_latest_market_day()
            start_date = pd.Timestamp(end_date) - pd.Timedelta(days=days)
            
            data_task = progress.add_task(
                "[cyan]Updating historical data",
                total=len(symbols)
            )
            
            async def update_data_batch(batch):
                """Update historical data for a batch of symbols."""
                tasks = []
                for symbol in batch:
                    tasks.append(
                        client.get_historical_data(
                            symbol.symbol,
                            start_date,
                            end_date,
                            interval="1d"
                        )
                    )
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for symbol, result in zip(batch, results):
                    try:
                        if isinstance(result, Exception):
                            logger.error(f"Error getting data for {symbol.symbol}: {result}")
                            progress.advance(data_task)
                            continue
                        
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            await storage.store_historical_data(result, symbol.symbol, "1d")
                        progress.advance(data_task)
                    except Exception as e:
                        logger.error(f"Error storing data for {symbol.symbol}: {e}")
                        progress.advance(data_task)
            
            # Process historical data batches concurrently
            batches = [
                symbols[i:i + BATCH_SIZE]
                for i in range(0, len(symbols), BATCH_SIZE)
            ]
            
            # Process batches in groups of MAX_CONCURRENT_BATCHES
            for i in range(0, len(batches), MAX_CONCURRENT_BATCHES):
                batch_group = batches[i:i + MAX_CONCURRENT_BATCHES]
                await asyncio.gather(*(
                    update_data_batch(batch) for batch in batch_group
                ))
            
            progress.advance(overall)
            
            console.print("[green]Routine update completed!")


@cli.command()
@click.option("--config", type=click.Path(exists=True), help="Path to config file")
@click.option("--days", default=7, help="Number of days of historical data to update")
def routine_update(config: Optional[str], days: int):
    """Run routine updates for all data."""
    async def _run():
        settings = load_settings()  # Remove Path argument
        storage = MongoStorage(settings)
        await storage.connect()  # Initialize MongoDB connection
        await run_routine_update(settings, storage, days)
        
    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"Routine update failed: {str(e)}")
        raise click.ClickException(str(e))


if __name__ == "__main__":
    try:
        # If no command is provided, run interactive mode
        import sys
        if len(sys.argv) == 1:
            sys.argv.append("interactive")
        cli()
    except Exception as e:
        console.print(f"[red]Fatal error: {str(e)}[/red]")
        sys.exit(1) 