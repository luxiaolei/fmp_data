"""Tests for date utilities."""

import pandas as pd

from src.utils.date_utils import split_5min_date_range, split_daily_date_range


def test_split_daily_date_range() -> None:
    """Test splitting date range for daily data."""
    start = pd.Timestamp("2024-01-01")
    end = pd.Timestamp("2024-12-31")
    
    # Test with default chunk size
    chunks = split_daily_date_range(start, end)
    assert len(chunks) == 1  # Since range is less than 365 days
    assert chunks[0][0] == start
    assert chunks[0][1] == end
    
    # Test with smaller chunk size
    chunks = split_daily_date_range(start, end, chunk_size=30)
    assert len(chunks) > 1
    assert chunks[0][0] == start
    assert chunks[-1][1] == end
    
    # Test date swapping
    chunks = split_daily_date_range(end, start)
    assert chunks[0][0] == start
    assert chunks[0][1] == end


def test_split_5min_date_range() -> None:
    """Test splitting date range for 5-minute data."""
    start = pd.Timestamp("2024-01-01")
    end = pd.Timestamp("2024-01-15")
    
    # Test with default chunk size
    chunks = split_5min_date_range(start, end)
    assert len(chunks) > 1  # Should split into ~2 chunks with 7-day default
    assert chunks[0][0] == start
    assert chunks[-1][1] == end
    
    # Test with smaller chunk size
    chunks = split_5min_date_range(start, end, chunk_size=3)
    assert len(chunks) > 3
    assert chunks[0][0] == start
    assert chunks[-1][1] == end 