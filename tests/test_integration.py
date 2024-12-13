"""Integration tests for FMP API."""

import os
from typing import Any, Generator

import pytest
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

@pytest.fixture
def api_key() -> Generator[str, None, None]:
    """Get API key from environment."""
    key = os.getenv("FMP_API_KEY")
    if not key:
        pytest.skip("FMP_API_KEY not set")
    yield key


def test_api_key_debug(api_key: str, caplog: Any) -> None:
    """Test API key debug logging."""
    if not api_key:
        pytest.skip("API key not available")
        
    # Safe way to log API key info
    key_length = len(api_key)
    if key_length >= 8:
        masked_key = f"{api_key[:4]}...{api_key[-4:]}"
    else:
        masked_key = "****"
        
    caplog.set_level("DEBUG")
    print(f"\nAPI Debug Info:")
    print(f"API Key (masked): {masked_key}")