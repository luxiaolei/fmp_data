"""Utilities for handling date ranges and time-related operations."""

from typing import List, Tuple

import pandas as pd


def split_daily_date_range(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    chunk_size: int = 365
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a date range into chunks for daily data."""
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
        
    chunks = []
    current = start_dt
    
    while current < end_dt:
        chunk_end = min(current + pd.Timedelta(days=chunk_size), end_dt)
        chunks.append((current, chunk_end))
        current = chunk_end + pd.Timedelta(days=1)
        
    return chunks


def split_5min_date_range(
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    chunk_size: int = 7
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a date range into chunks for 5-minute data."""
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
        
    chunks = []
    current = start_dt
    
    while current < end_dt:
        chunk_end = min(current + pd.Timedelta(days=chunk_size), end_dt)
        chunks.append((current, chunk_end))
        current = chunk_end + pd.Timedelta(days=1)
        
    return chunks 