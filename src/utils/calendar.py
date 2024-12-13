"""Exchange calendar utilities."""

from datetime import datetime
from typing import List, cast
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
from loguru import logger
from pandas import DatetimeIndex


class ExchangeCalendar:
    """Exchange calendar handler."""
    
    # Calendar cache
    _calendars: dict[str, xcals.ExchangeCalendar] = {}
    
    @classmethod
    def get_calendar(cls, exchange: str) -> xcals.ExchangeCalendar:
        """Get exchange calendar."""
        exchange = exchange.upper()
        if exchange not in cls._calendars:
            # Map common exchange names to exchange_calendars codes
            calendar_map = {
                "NYSE": "XNYS",
                "NASDAQ": "XNAS",
                "AMEX": "XASE",
                "LSE": "XLON",
                "TSX": "XTSE"
            }
            calendar_code = calendar_map.get(exchange, exchange)
            try:
                cls._calendars[exchange] = xcals.get_calendar(calendar_code)
            except Exception as e:
                logger.error(f"Error getting calendar for {exchange}: {e}")
                # Default to NYSE calendar
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
        # Convert to timezone naive dates for calendar
        naive_start = start_date.replace(tzinfo=None)
        naive_end = end_date.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        
        # Get schedule DataFrame and extract dates
        schedule = calendar.sessions_in_range(naive_start, naive_end)
        # Return timezone-aware dates
        tz = start_date.tzinfo
        dates = cast(DatetimeIndex, schedule).to_pydatetime()
        return [d.replace(tzinfo=tz) for d in dates]
    
    @classmethod
    def get_trading_minutes(
        cls,
        start_date: datetime,
        end_date: datetime,
        exchange: str = "NYSE",
        interval: str = "5min"
    ) -> List[datetime]:
        """Get trading minutes between dates.
        
        All times are handled in US/Eastern timezone internally since that's what
        the exchange uses, but results are returned in the input timezone.
        """
        calendar = cls.get_calendar(exchange)
        logger.debug(f"Getting trading minutes for {exchange} from {start_date} to {end_date}")
        
        # Convert interval string to pandas frequency
        freq = pd.Timedelta(interval)
        
        # Store original timezone for later
        original_tz = start_date.tzinfo
        
        # Convert input dates to NY timezone for internal processing
        ny_tz = ZoneInfo("America/New_York")
        ny_start = pd.Timestamp(start_date).tz_convert(ny_tz)
        ny_end = pd.Timestamp(end_date).tz_convert(ny_tz)
        
        # Get the dates at midnight for sessions_in_range
        session_start = ny_start.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        session_end = (ny_end + pd.Timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        
        logger.debug(f"Using session range: {session_start} to {session_end}")
        
        # Get trading sessions for the date range
        sessions = calendar.sessions_in_range(session_start, session_end)
        logger.debug(f"Found {len(sessions)} trading sessions")
        
        if len(sessions) == 0:
            return []
            
        # Get market open/close times for these sessions
        opens = calendar.opens.loc[sessions]
        closes = calendar.closes.loc[sessions]
        
        # Create minute range for each session
        all_minutes = []
        # Convert to naive NY time for comparison
        original_start = ny_start.tz_localize(None)
        original_end = ny_end.tz_localize(None)
        
        for session_open, session_close in zip(opens, closes):
            # Convert session times to naive NY time
            session_open = pd.Timestamp(session_open).tz_convert(ny_tz).tz_localize(None)
            session_close = pd.Timestamp(session_close).tz_convert(ny_tz).tz_localize(None)
            logger.debug(f"Processing session: open={session_open}, close={session_close}")
            
            # Generate full range of minutes for the session
            session_minutes = pd.date_range(
                start=max(session_open, original_start),
                end=min(session_close, original_end),
                freq=freq,
                inclusive='left'  # Don't include the closing time
            )
            logger.debug(f"Generated {len(session_minutes)} minutes for session")
            
            # Filter minutes based on market hours
            valid_minutes = []
            for minute in session_minutes:
                hour = minute.hour
                minute_val = minute.minute
                
                # Only include minutes during market hours (in NY time)
                if hour < 9 or hour > 16:
                    continue
                if hour == 9 and minute_val < 30:
                    continue
                if hour == 16 and minute_val > 0:
                    continue
                    
                valid_minutes.append(minute)
            
            logger.debug(f"After filtering: {len(valid_minutes)} valid minutes")
            if valid_minutes:
                logger.debug(f"First minute: {valid_minutes[0]}, Last minute: {valid_minutes[-1]}")
            
            all_minutes.extend(valid_minutes)
            
        if not all_minutes:
            return []
            
        # Convert back to timezone-aware NY time first
        ny_minutes = [d.replace(tzinfo=ny_tz) for d in all_minutes]
        # Then convert to original timezone
        result = [d.astimezone(original_tz) for d in ny_minutes]
        
        logger.debug(f"Returning {len(result)} minutes")
        if len(result) > 0:
            logger.debug(f"First: {result[0]}, Last: {result[-1]}")
        return result
