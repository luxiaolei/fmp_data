"""Tests for calendar utilities."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pytest

from src.utils.calendar import ExchangeCalendar


@pytest.fixture
def nyse_calendar():
    """Get NYSE calendar instance."""
    return ExchangeCalendar.get_calendar("NYSE")


@pytest.fixture
def test_dates():
    """Get test dates."""
    # Use a known trading day - use ZoneInfo instead of pytz
    ny_tz = ZoneInfo("America/New_York")
    
    # Create timezone-naive dates and then localize them
    base_date = datetime(2023, 1, 3)  # Tuesday
    return {
        "start": datetime(2023, 1, 3, 9, 30).replace(tzinfo=ny_tz),  # Market open
        "end": datetime(2023, 1, 3, 16, 0).replace(tzinfo=ny_tz),    # Market close
        "weekend": datetime(2023, 1, 1).replace(tzinfo=ny_tz),       # Sunday
        "holiday": datetime(2023, 1, 2).replace(tzinfo=ny_tz),       # New Year's Day
    }


def test_calendar_cache():
    """Test calendar caching mechanism."""
    # First call should create new calendar
    cal1 = ExchangeCalendar.get_calendar("NYSE")
    assert "NYSE" in ExchangeCalendar._calendars
    
    # Second call should use cached calendar
    cal2 = ExchangeCalendar.get_calendar("NYSE")
    assert cal1 is cal2


def test_calendar_exchange_mapping():
    """Test exchange code mapping."""
    # Test all supported exchanges
    exchanges = ["NYSE", "NASDAQ", "AMEX", "LSE", "TSX"]
    for exchange in exchanges:
        cal = ExchangeCalendar.get_calendar(exchange)
        assert isinstance(cal, xcals.ExchangeCalendar)


def test_invalid_exchange_fallback():
    """Test fallback to NYSE for invalid exchange."""
    # Should log error and fallback to NYSE
    cal = ExchangeCalendar.get_calendar("INVALID")
    assert isinstance(cal, xcals.ExchangeCalendar)
    assert cal.name == "XNYS"  # NYSE calendar code


def test_get_trading_days(test_dates):
    """Test getting trading days."""
    # Test one week period
    start = test_dates["start"].replace(hour=0, minute=0)
    end = (start + timedelta(days=7)).replace(hour=23, minute=59)
    
    days = ExchangeCalendar.get_trading_days(start, end)
    
    assert len(days) > 0
    assert all(isinstance(d, datetime) for d in days)
    # Should exclude weekends and holidays
    assert all(d.weekday() < 5 for d in days)  # No weekends
    assert test_dates["holiday"].date() not in [d.date() for d in days]
    # Check timezone
    assert all(d.tzinfo == start.tzinfo for d in days)


def test_get_trading_minutes(test_dates):
    """Test getting trading minutes."""
    start = test_dates["start"]
    end = test_dates["end"]
    
    # Test different intervals
    intervals = ["5min", "15min", "1h"]
    for interval in intervals:
        minutes = ExchangeCalendar.get_trading_minutes(
            start, end, interval=interval
        )
        
        assert len(minutes) > 0
        assert all(isinstance(m, datetime) for m in minutes)
        # Check timezone
        assert all(m.tzinfo == start.tzinfo for m in minutes)
        
        # Check interval spacing
        if len(minutes) > 1:
            diff = minutes[1] - minutes[0]
            expected_diff = pd.Timedelta(interval)
            assert diff >= expected_diff


def test_trading_minutes_outside_market_hours(test_dates):
    """Test handling of non-market hours."""
    start = test_dates["start"] - timedelta(hours=2)  # Before market open
    end = test_dates["end"] + timedelta(hours=2)      # After market close
    
    minutes = ExchangeCalendar.get_trading_minutes(start, end)
    
    # Should only include market hours
    for m in minutes:
        hour = m.hour
        assert 9 <= hour <= 16  # NYSE market hours
        if hour == 9:
            assert m.minute >= 30  # Market opens at 9:30
        elif hour == 16:
            print(m.minute)
            assert m.minute == 0, m.minute  # Market closes at 16:05


def test_trading_minutes_non_trading_days(test_dates):
    """Test handling of non-trading days."""
    # Test weekend
    weekend_start = test_dates["weekend"]
    weekend_end = weekend_start + timedelta(days=1)
    
    weekend_minutes = ExchangeCalendar.get_trading_minutes(
        weekend_start,
        weekend_end
    )
    assert len(weekend_minutes) == 0
    
    # Test holiday
    holiday_start = test_dates["holiday"]
    holiday_end = holiday_start + timedelta(days=1)
    
    holiday_minutes = ExchangeCalendar.get_trading_minutes(
        holiday_start,
        holiday_end
    )
    assert len(holiday_minutes) == 0


def test_exchange_case_insensitivity():
    """Test case-insensitive exchange handling."""
    variations = ["NYSE", "nyse", "Nyse", "nYsE"]
    calendars = [ExchangeCalendar.get_calendar(v) for v in variations]
    
    # All should return the same calendar
    assert all(cal.name == "XNYS" for cal in calendars)
    assert all(cal is calendars[0] for cal in calendars[1:])