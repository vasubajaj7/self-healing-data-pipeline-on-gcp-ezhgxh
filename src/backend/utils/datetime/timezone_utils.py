"""
Utility module providing timezone handling functionality for the self-healing data pipeline.

This module provides functions for timezone conversion, localization, and timezone-aware 
datetime operations to ensure consistent timezone handling across the application.
"""

import datetime
import pytz  # version 2023.3
import typing
import logging

from ... import constants
from ..config.config_loader import load_config_hierarchy

# Configure logging
logger = logging.getLogger(__name__)

# Default timezone to use if no configuration is available
DEFAULT_TIMEZONE = "UTC"

# Common timezone names for convenience
COMMON_TIMEZONES = [
    "UTC", "US/Eastern", "US/Central", "US/Mountain", "US/Pacific", 
    "Europe/London", "Europe/Paris", "Asia/Tokyo", "Australia/Sydney"
]


def get_timezone(timezone_str: str) -> pytz.timezone:
    """Gets a timezone object from a timezone string.
    
    Args:
        timezone_str: String representing a timezone (e.g., 'UTC', 'US/Eastern')
        
    Returns:
        A pytz timezone object for the specified timezone
    """
    if not timezone_str:
        logger.debug(f"Empty timezone string, defaulting to {DEFAULT_TIMEZONE}")
        return pytz.timezone(DEFAULT_TIMEZONE)
    
    try:
        return pytz.timezone(timezone_str)
    except pytz.exceptions.UnknownTimeZoneError:
        logger.warning(f"Unknown timezone: {timezone_str}, defaulting to {DEFAULT_TIMEZONE}")
        return pytz.timezone(DEFAULT_TIMEZONE)


def get_default_timezone() -> pytz.timezone:
    """Gets the default timezone from configuration or falls back to UTC.
    
    Returns:
        Default timezone as a pytz timezone object
    """
    try:
        # Try to load configuration
        config = load_config_hierarchy()
        
        # Get default timezone from config if it exists
        timezone_str = config.get('datetime', {}).get('default_timezone')
        
        if timezone_str:
            logger.debug(f"Using configured default timezone: {timezone_str}")
            return get_timezone(timezone_str)
    except Exception as e:
        logger.warning(f"Error loading default timezone from configuration: {str(e)}")
    
    # Fallback to default
    logger.debug(f"Using hardcoded default timezone: {DEFAULT_TIMEZONE}")
    return pytz.timezone(DEFAULT_TIMEZONE)


def is_timezone_aware(dt: datetime.datetime) -> bool:
    """Checks if a datetime object is timezone-aware.
    
    Args:
        dt: Datetime object to check
        
    Returns:
        True if the datetime is timezone-aware, False otherwise
    """
    if dt is None:
        return False
    return dt.tzinfo is not None


def localize_datetime(dt: datetime.datetime, 
                     timezone: typing.Union[str, pytz.timezone] = None) -> datetime.datetime:
    """Localizes a naive datetime to a specific timezone.
    
    Args:
        dt: Datetime object to localize
        timezone: Timezone to localize to (string or pytz timezone object)
                 If None, uses the default timezone
    
    Returns:
        Timezone-aware datetime in the specified timezone
    """
    if dt is None:
        return None
        
    # If already timezone-aware, return as is
    if is_timezone_aware(dt):
        return dt
    
    # Convert string timezone to timezone object if needed
    if isinstance(timezone, str):
        timezone = get_timezone(timezone)
    elif timezone is None:
        timezone = get_default_timezone()
    
    # Localize using the pytz localize method (handles DST correctly)
    try:
        return timezone.localize(dt)
    except Exception as e:
        logger.error(f"Error localizing datetime {dt} to timezone {timezone}: {str(e)}")
        # Fallback to attaching timezone (less accurate with DST)
        return dt.replace(tzinfo=timezone)


def convert_to_timezone(dt: datetime.datetime, 
                       to_timezone: typing.Union[str, pytz.timezone] = None) -> datetime.datetime:
    """Converts a datetime from one timezone to another.
    
    Args:
        dt: Datetime object to convert
        to_timezone: Target timezone (string or pytz timezone object)
                    If None, uses the default timezone
    
    Returns:
        Datetime in the target timezone
    """
    if dt is None:
        return None
    
    # Ensure the datetime is timezone-aware
    if not is_timezone_aware(dt):
        # If naive, assume it's in UTC
        dt = localize_datetime(dt, pytz.UTC)
    
    # Convert string timezone to timezone object if needed
    if isinstance(to_timezone, str):
        to_timezone = get_timezone(to_timezone)
    elif to_timezone is None:
        to_timezone = get_default_timezone()
    
    # Convert to the target timezone
    return dt.astimezone(to_timezone)


def convert_to_utc(dt: datetime.datetime) -> datetime.datetime:
    """Converts a datetime to UTC timezone.
    
    Args:
        dt: Datetime object to convert
    
    Returns:
        Datetime in UTC timezone
    """
    if dt is None:
        return None
        
    return convert_to_timezone(dt, pytz.UTC)


def now_in_timezone(timezone: typing.Union[str, pytz.timezone] = None) -> datetime.datetime:
    """Gets the current datetime in a specific timezone.
    
    Args:
        timezone: Target timezone (string or pytz timezone object)
                If None, uses the default timezone
    
    Returns:
        Current datetime in the specified timezone
    """
    # Get current UTC time
    current_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    
    # Convert to target timezone
    return convert_to_timezone(current_utc, timezone)


def utcnow() -> datetime.datetime:
    """Gets the current UTC datetime with timezone information.
    
    Returns:
        Current UTC datetime (timezone-aware)
    """
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)


def get_timezone_offset(timezone: typing.Union[str, pytz.timezone], 
                       dt: datetime.datetime = None) -> datetime.timedelta:
    """Gets the offset from UTC for a specific timezone at a given datetime.
    
    Args:
        timezone: Timezone to get offset for (string or pytz timezone object)
        dt: Datetime for which to calculate the offset (default: current time)
    
    Returns:
        Timezone offset as a timedelta
    """
    # Convert string timezone to timezone object if needed
    if isinstance(timezone, str):
        timezone = get_timezone(timezone)
    
    # Use current time if dt is not provided
    if dt is None:
        dt = utcnow()
    
    # Ensure the datetime is localized to the target timezone
    if not is_timezone_aware(dt):
        dt = localize_datetime(dt, timezone)
    else:
        dt = convert_to_timezone(dt, timezone)
    
    # The offset is the utcoffset of the datetime
    return dt.utcoffset()


def get_timezone_abbreviation(timezone: typing.Union[str, pytz.timezone], 
                             dt: datetime.datetime = None) -> str:
    """Gets the timezone abbreviation (e.g., EST, PDT) for a timezone at a specific datetime.
    
    Args:
        timezone: Timezone to get abbreviation for (string or pytz timezone object)
        dt: Datetime for which to get the abbreviation (default: current time)
    
    Returns:
        Timezone abbreviation string
    """
    # Convert string timezone to timezone object if needed
    if isinstance(timezone, str):
        timezone = get_timezone(timezone)
    
    # Use current time if dt is not provided
    if dt is None:
        dt = utcnow()
    
    # Ensure the datetime is localized to the target timezone
    if not is_timezone_aware(dt):
        dt = localize_datetime(dt, timezone)
    else:
        dt = convert_to_timezone(dt, timezone)
    
    # Get the timezone abbreviation
    return dt.tzname()


def format_timezone_offset(
    offset_or_timezone: typing.Union[datetime.timedelta, datetime.datetime, str, pytz.timezone],
    dt: datetime.datetime = None
) -> str:
    """Formats a timezone offset as a string (e.g., +05:00, -08:00).
    
    Args:
        offset_or_timezone: Offset (timedelta) or timezone/datetime to format offset for
        dt: Datetime for which to calculate the offset (default: current time)
    
    Returns:
        Formatted timezone offset string
    """
    # Initialize offset
    offset = None
    
    # Handle different input types
    if isinstance(offset_or_timezone, datetime.timedelta):
        offset = offset_or_timezone
    elif isinstance(offset_or_timezone, datetime.datetime) and is_timezone_aware(offset_or_timezone):
        offset = offset_or_timezone.utcoffset()
    elif isinstance(offset_or_timezone, (str, pytz.timezone)):
        offset = get_timezone_offset(offset_or_timezone, dt)
    else:
        raise ValueError("offset_or_timezone must be a timedelta, timezone-aware datetime, timezone string, or timezone object")
    
    # Convert to total seconds and then to hours and minutes
    total_seconds = int(offset.total_seconds())
    hours, remainder = divmod(abs(total_seconds), 3600)
    minutes = remainder // 60
    
    # Format as +/-HH:MM
    sign = '+' if total_seconds >= 0 else '-'
    return f"{sign}{hours:02d}:{minutes:02d}"


def is_dst(dt: datetime.datetime, timezone: typing.Union[str, pytz.timezone] = None) -> bool:
    """Checks if a datetime is in Daylight Saving Time for a specific timezone.
    
    Args:
        dt: Datetime to check
        timezone: Timezone to check DST for (string or pytz timezone object)
                If None, uses the default timezone
    
    Returns:
        True if the datetime is in DST, False otherwise
    """
    if dt is None:
        return None
    
    # Convert string timezone to timezone object if needed
    if isinstance(timezone, str):
        timezone = get_timezone(timezone)
    elif timezone is None:
        timezone = get_default_timezone()
    
    # Ensure the datetime is localized to the target timezone
    if not is_timezone_aware(dt):
        dt = localize_datetime(dt, timezone)
    else:
        dt = convert_to_timezone(dt, timezone)
    
    # Check if DST is in effect
    return dt.dst().total_seconds() != 0


def get_timezone_name(timezone: typing.Union[str, pytz.timezone]) -> str:
    """Gets the full name of a timezone.
    
    Args:
        timezone: Timezone to get name for (string or pytz timezone object)
    
    Returns:
        Full timezone name
    """
    # Convert string timezone to timezone object if needed
    if isinstance(timezone, str):
        timezone = get_timezone(timezone)
    
    return timezone.zone


def list_common_timezones() -> list:
    """Lists common timezone names.
    
    Returns:
        List of common timezone names
    """
    # Return either from pytz or our predefined list
    return pytz.common_timezones if hasattr(pytz, 'common_timezones') else COMMON_TIMEZONES


def parse_timezone(timezone_str: str) -> pytz.timezone:
    """Attempts to parse a timezone from a string.
    
    Args:
        timezone_str: String that may represent a timezone
    
    Returns:
        Parsed timezone or UTC if parsing fails
    """
    if not timezone_str:
        logger.debug("Empty timezone string in parse_timezone, returning UTC")
        return pytz.UTC
    
    # Check for common timezone names directly
    try:
        return pytz.timezone(timezone_str)
    except pytz.exceptions.UnknownTimeZoneError:
        pass
    
    # Handle special cases like 'Z' for UTC
    if timezone_str.upper() == 'Z':
        return pytz.UTC
    
    # Try to parse offset-based timezone like '+05:00' or '-0800'
    try:
        if timezone_str.startswith(('+', '-')):
            # Extract hours and minutes from the offset
            if ':' in timezone_str:
                # Format like '+05:00'
                hours = int(timezone_str[1:3])
                minutes = int(timezone_str[4:6])
            else:
                # Format like '+0500'
                hours = int(timezone_str[1:3])
                minutes = int(timezone_str[3:5]) if len(timezone_str) >= 5 else 0
            
            # Apply sign
            total_minutes = (hours * 60 + minutes) * (-1 if timezone_str.startswith('-') else 1)
            
            # Find the timezone with this offset
            for tz_name in pytz.all_timezones:
                tz = pytz.timezone(tz_name)
                offset = tz.utcoffset(datetime.datetime.utcnow())
                if offset.total_seconds() / 60 == total_minutes:
                    return tz
    except (ValueError, IndexError) as e:
        logger.debug(f"Failed to parse timezone offset from {timezone_str}: {str(e)}")
    
    # Default to UTC if parsing fails
    logger.warning(f"Could not parse timezone from string: {timezone_str}, defaulting to UTC")
    return pytz.UTC