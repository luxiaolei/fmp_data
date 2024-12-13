"""Exchange calendar utilities."""

from datetime import datetime, time
from typing import List, Optional
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
from loguru import logger
from pandas.tseries.offsets import BDay


class ExchangeCalendar:
    """Exchange calendar manager."""
    
    _calendars = {}  # Cache for calendars
    _default_exchange = "NYSE"
    _exchange_mapping = {
        "NYSE": "XNYS",
        "NASDAQ": "XNAS",
        "AMEX": "XASE",
        "LSE": "XLON",
        "TSX": "XTSE"
    }

    @classmethod
    def get_calendar(cls, exchange: str) -> xcals.ExchangeCalendar:
        """Get calendar for exchange."""
        exchange = exchange.upper()
        if exchange not in cls._calendars:
            try:
                calendar_code = cls._exchange_mapping.get(exchange, "XNYS")
                cls._calendars[exchange] = xcals.get_calendar(calendar_code)
            except Exception as e:
                logger.warning(f"Error getting calendar for {exchange}: {e}")
                cls._calendars[exchange] = xcals.get_calendar("XNYS")
                
        return cls._calendars[exchange]

    @classmethod
    def get_trading_days(
        cls,
        start_date: datetime,
        end_date: datetime,
        exchange: str = "NYSE"
    ) -> List[datetime]:
        """Get trading days between dates."""
        calendar = cls.get_calendar(exchange)
        
        # Convert dates to timezone-naive for comparison with calendar
        start_dt = pd.Timestamp(start_date).tz_localize(None).normalize()  # Ensure midnight
        end_dt = pd.Timestamp(end_date).tz_localize(None).normalize()  # Ensure midnight
        calendar_start = calendar.first_session.tz_localize(None)
        
        # Get the first valid trading day if start_date is before calendar start
        if start_dt < calendar_start:
            start_dt = calendar_start
            
        # Get only trading days (excluding weekends and holidays)
        schedule = calendar.sessions_in_range(start_dt, end_dt)
        
        # First localize to UTC, then convert to original timezone
        original_tz = start_date.tzinfo
        if original_tz:
            # Use calendar's timezone before converting to target timezone
            return [
                ts.tz_localize(calendar.tz).tz_convert(original_tz).to_pydatetime()
                for ts in schedule
            ]
        return [ts.tz_localize(calendar.tz).to_pydatetime() for ts in schedule]

    @classmethod
    def get_trading_minutes(
        cls,
        start_date: datetime,
        end_date: datetime,
        exchange: str = "NYSE",
        interval: str = "5min"
    ) -> List[datetime]:
        """Get trading minutes between dates."""
        calendar = cls.get_calendar(exchange)
        
        # Convert dates to timezone-naive for comparison
        start_dt = pd.Timestamp(start_date).tz_localize(None)
        end_dt = pd.Timestamp(end_date).tz_localize(None)
        calendar_start = calendar.first_session.tz_localize(None)
        
        # Adjust start date if before calendar start
        if start_dt < calendar_start:
            start_dt = calendar_start
            
        # Get all minutes and then resample to desired interval
        minutes = calendar.minutes_in_range(start_dt, end_dt)
        if not minutes.empty:
            # Convert to DataFrame for resampling
            df = pd.DataFrame(index=minutes)
            resampled = df.resample(interval).first()
            
            # Convert back to timezone-aware using original timezone
            original_tz = start_date.tzinfo
            if original_tz:
                # Use tz_convert instead of tz_localize for already tz-aware timestamps
                return [ts.tz_convert(original_tz).to_pydatetime() for ts in resampled.index]
            return [ts.to_pydatetime() for ts in resampled.index]
        return []

    @classmethod
    def get_holidays(cls, exchange: str = "NYSE") -> List[datetime]:
        """Get list of holidays for the given exchange."""
        calendar = cls.get_calendar(exchange)
        start_date = pd.Timestamp('2000-01-01')
        end_date = pd.Timestamp.now()
        
        # Get all holidays from the exchange calendar
        trading_days = calendar.sessions_in_range(start_date, end_date)
        all_days = pd.date_range(start=start_date, end=end_date, freq='B')
        
        # Find holidays by getting business days that aren't valid trading days
        holidays = all_days[~all_days.isin(trading_days)]
        
        # Convert to datetime objects in calendar's timezone
        return [
            h.tz_localize(calendar.tz).to_pydatetime() 
            for h in holidays
        ]


def get_ny_time() -> datetime:
    """Get current time in New York timezone.
    
    Returns
    -------
    datetime
        Current time in New York
    """
    return datetime.now(ZoneInfo("America/New_York"))


def is_market_closed(dt: Optional[datetime] = None) -> bool:
    """Check if market is closed at given time.
    
    Parameters
    ----------
    dt : Optional[datetime], optional
        Datetime to check, by default None (current NY time)
        
    Returns
    -------
    bool
        True if market is closed
    """
    dt = dt or get_ny_time()
    
    # Convert to NY time if timezone aware
    if dt.tzinfo is not None:
        dt = dt.astimezone(ZoneInfo("America/New_York"))
    
    # Market hours: 9:30 AM - 4:00 PM ET
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    # Check if weekend
    if dt.weekday() > 4:  # 5 = Saturday, 6 = Sunday
        return True
        
    # Check if before open or after close
    current_time = dt.time()
    if current_time < market_open or current_time >= market_close:
        return True
        
    return False


def get_latest_market_day(dt: Optional[datetime] = None) -> datetime:
    """Get latest completed market day.
    
    Parameters
    ----------
    dt : Optional[datetime], optional
        Reference datetime, by default None (current NY time)
        
    Returns
    -------
    datetime
        Latest completed market day
    """
    dt = dt or get_ny_time()
    
    # Convert to NY time if timezone aware
    if dt.tzinfo is not None:
        dt = dt.astimezone(ZoneInfo("America/New_York"))
    
    # If market is closed for current day, move back one business day
    if is_market_closed(dt):
        dt = (pd.Timestamp(dt) - BDay(1)).to_pydatetime()
    
    # Return date part only
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
