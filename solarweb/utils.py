"""Utility functions for solar data processing."""

from datetime import datetime, timezone

def timestamp_newer_than(timestamp, other_timestamp):
    """Check if a timestamp is newer than the other timestamp.
    
    If the other timestamp is None, then assume the timestamp is new.
    
    Args:
        timestamp: Timestamp to check
        other_timestamp: Timestamp to compare against
        
    Returns:
        bool: True if timestamp is newer than other_timestamp
    """
    return (
        other_timestamp is None
        or timestamp > other_timestamp
    )


def timestamp_newer_than_or_equal_to_today(timestamp):
    """Check if a timestamp is newer than or equal to today.
    
    Args:
        timestamp: Timestamp to check
        
    Returns:
        bool: True if timestamp is today or newer
    """
    today = datetime.now(timezone.utc)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    return timestamp >= today


def is_new_timestamp(ts_datetime, last_dailydata_timestamp):
    """Check if the timestamp is newer than the last daily data timestamp
    and not newer than yesterday.
    
    This is to avoid processing data that is not yet complete (e.g. the current day).
    
    Args:
        ts_datetime: Timestamp to check
        last_dailydata_timestamp: Last processed timestamp
        
    Returns:
        bool: True if timestamp is new and not from today
    """
    return (
        timestamp_newer_than(ts_datetime, last_dailydata_timestamp)
        and not timestamp_newer_than_or_equal_to_today(ts_datetime)
    ) 