"""Tests for CLI functionality."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from click.testing import CliRunner
from pytest_mock import MockerFixture

from cli.main import cli, run_routine_update
from src.config.settings import APIConfig, Settings, StorageConfig
from src.models.data_types import AssetType, ExchangeInfo


@pytest.fixture
def mock_settings():
    """Mock settings fixture."""
    return Settings(
        api=APIConfig(
            key="test_key",
            base_url="https://test.com"
        ),
        storage=StorageConfig(
            mongodb_uri="mongodb://localhost:27017",
            mongodb_db="test_db",
            redis_url="redis://localhost:6379",
            redis_password=""
        )
    )


@pytest.fixture
def mock_storage():
    """Mock storage fixture."""
    storage = AsyncMock()
    storage.store_historical_data = AsyncMock()
    storage.store_company_profile = AsyncMock()
    storage.store_index_constituents = AsyncMock()
    storage.store_exchange_symbols = AsyncMock()
    return storage


@pytest.fixture
def mock_client(mocker: MockerFixture):
    """Create mock FMP client."""
    client = mocker.Mock()
    client.get_historical_data.return_value = pd.DataFrame({
        "date": ["2024-01-01"],
        "close": [100]
    })
    client.get_exchange_symbols.return_value = [
        ExchangeInfo(
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            price=150.0
        )
    ]
    client.get_all_exchange_symbols.return_value = [
        ExchangeInfo(
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            asset_type=AssetType.STOCK,
            price=150.0
        )
    ]
    return client


@pytest.mark.asyncio
async def test_routine_update(mock_settings, mock_storage, mock_client):
    """Test routine update functionality."""
    with patch("cli.main.FMPClient", return_value=mock_client):
        await run_routine_update(mock_settings, mock_storage, days=7)
        
    # Verify all expected calls were made
    mock_client.get_exchange_symbols.assert_called()
    mock_client.get_index_constituents.assert_called()
    mock_client.get_company_profile.assert_called()
    mock_client.get_historical_data.assert_called()
    
    mock_storage.store_exchange_symbols.assert_called()
    mock_storage.store_index_constituents.assert_called()
    mock_storage.store_company_profile.assert_called()
    mock_storage.store_historical_data.assert_called()


@pytest.mark.asyncio
async def test_cli_interactive(mock_settings, mock_storage, mock_client):
    """Test interactive CLI mode."""
    runner = CliRunner()
    
    with patch("cli.main.load_config", return_value=mock_settings), \
         patch("cli.main.MongoStorage", return_value=mock_storage), \
         patch("cli.main.FMPClient", return_value=mock_client), \
         patch("asyncio.run") as mock_run:
        
        # Test interactive mode with download
        result = runner.invoke(cli, ["interactive"], input="1\nAAPL\n2024-01-01\n2024-01-10\n1d\n5\ny\n")
        assert result.exit_code == 0
        mock_run.assert_called()
        
        # Test interactive mode with index update
        result = runner.invoke(cli, ["interactive"], input="2\nsp500\n5\ny\n")
        assert result.exit_code == 0
        mock_run.assert_called()
        
        # Test interactive mode with routine update
        result = runner.invoke(cli, ["interactive"], input="4\n7\n5\ny\n")
        assert result.exit_code == 0
        mock_run.assert_called()


@pytest.mark.asyncio
async def test_cli_routine_update(mock_settings, mock_storage, mock_client):
    """Test routine update CLI command."""
    runner = CliRunner()
    
    with patch("cli.main.load_config", return_value=mock_settings), \
         patch("cli.main.MongoStorage", return_value=mock_storage), \
         patch("cli.main.FMPClient", return_value=mock_client), \
         patch("asyncio.run") as mock_run:
        
        result = runner.invoke(cli, ["routine-update", "--days", "7"])
        assert result.exit_code == 0
        mock_run.assert_called()


@pytest.mark.asyncio
async def test_cli_test_command(mock_settings, mock_client):
    """Test the test CLI command."""
    runner = CliRunner()
    
    with patch("cli.main.load_config", return_value=mock_settings), \
         patch("cli.main.FMPClient", return_value=mock_client), \
         patch("asyncio.run") as mock_run:
        
        result = runner.invoke(cli, ["test", "--symbol", "AAPL"])
        assert result.exit_code == 0
        mock_run.assert_called()

def test_cli_no_command():
    """Test CLI with no command (should default to interactive)."""
    runner = CliRunner()
    
    with patch("sys.argv", ["fmp"]):
        result = runner.invoke(cli)
        assert "FMP Data Management CLI" in result.output


@pytest.mark.asyncio
async def test_update_indexes(mock_settings, mock_storage, mock_client):
    """Test update indexes command."""
    runner = CliRunner()
    
    with patch("cli.main.load_config", return_value=mock_settings), \
         patch("cli.main.MongoStorage", return_value=mock_storage), \
         patch("cli.main.FMPClient", return_value=mock_client), \
         patch("asyncio.run") as mock_run:
        
        result = runner.invoke(cli, ["update-indexes"])
        assert result.exit_code == 0
        mock_run.assert_called()