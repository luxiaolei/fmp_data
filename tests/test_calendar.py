"""Tests for exchange calendar functionality."""

from datetime import datetime, timedelta, timezone
from typing import Dict

import pytest

from src.utils.calendar import ExchangeCalendar


@pytest.fixture
def test_dates() -> Dict[str, datetime]:
    """Create test dates fixture."""
    tz = timezone.utc
    now = datetime.now(tz)
    
    # Find next weekday
    while now.weekday() >= 5:  # Skip weekend
        now += timedelta(days=1)
    
    return {
        "start": now,
        "end": now + timedelta(days=1),
        "holiday": datetime(2024, 1, 1, tzinfo=tz),  # New Year's Day
        "weekend": now + timedelta(days=(5 - now.weekday() + 2))  # Next Sunday
    }


def test_get_trading_days(test_dates: Dict[str, datetime]) -> None:
    """Test getting trading days."""
    # Test one week period
    start = test_dates["start"].replace(hour=0, minute=0)
    end = (start + timedelta(days=7)).replace(hour=23, minute=59)
    
    days = ExchangeCalendar.get_trading_days(start, end)
    
    # Should exclude weekends and holidays
    assert all(d.weekday() < 5 for d in days)  # No weekends
    assert test_dates["holiday"].date() not in [d.date() for d in days]
    # Check timezone consistency
    assert all(d.tzinfo == start.tzinfo for d in days)


def test_get_trading_minutes(test_dates: Dict[str, datetime]) -> None:
    """Test getting trading minutes."""
    start = test_dates["start"]
    end = test_dates["end"]
    
    # Test different intervals
    intervals = ["5min", "15min", "1h"]
    for interval in intervals:
        minutes = ExchangeCalendar.get_trading_minutes(start, end, interval)
        assert len(minutes) > 0
        assert all(isinstance(m, datetime) for m in minutes)