"""
Entry point for the datetime utility module that provides comprehensive date, time, timezone, 
and scheduling functionality for the self-healing data pipeline.

This module consolidates and exposes functionality from date_parser, timezone_utils, and 
scheduling_utils submodules.

Key capabilities:
- Consistent date/time parsing and formatting across data sources
- Timezone-aware operations to handle global data processing
- Scheduling utilities for orchestration and maintenance windows
- Time-based validation for data quality checks
- Retry timing calculations for self-healing mechanisms

This module supports multiple requirements including:
- Data ingestion from various sources with consistent time handling
- Data quality validation of temporal fields
- Pipeline orchestration with scheduling capabilities
- Self-healing mechanisms with timing utilities for retries
"""

# Import submodules
from .date_parser import (
    # Date parsing and formatting
    parse_date, parse_time, parse_datetime,
    format_date, format_time, format_datetime,
    convert_timezone, is_valid_date, is_valid_datetime,
    detect_date_format,
    
    # Current date/time
    get_current_date, get_current_time, get_current_datetime,
    
    # Date manipulation
    add_days, add_months, add_years,
    date_diff, datetime_diff,
    
    # Date analysis
    is_weekend, is_leap_year,
    get_month_name, get_month_abbr,
    get_day_name, get_day_abbr,
    get_quarter, get_week_number,
    get_days_in_month,
    get_first_day_of_month, get_last_day_of_month,
    get_first_day_of_quarter, get_last_day_of_quarter,
    get_first_day_of_year, get_last_day_of_year,
    
    # Date truncation
    truncate_to_day, truncate_to_hour, truncate_to_minute,
    
    # Date comparison
    is_future_date, is_past_date, is_same_day,
    
    # Date ranges
    date_range, DateRange, DatetimeRange
)

from .timezone_utils import (
    # Timezone utilities
    get_timezone, get_default_timezone,
    is_timezone_aware, localize_datetime,
    convert_to_timezone, convert_to_utc,
    now_in_timezone, utcnow,
    get_timezone_offset, get_timezone_abbreviation,
    format_timezone_offset, is_dst,
    get_timezone_name, list_common_timezones,
    parse_timezone
)

from .scheduling_utils import (
    # Cron expression handling
    is_valid_cron_expression, parse_cron_expression,
    get_next_execution_time, get_previous_execution_time,
    get_execution_schedule, cron_to_human_readable,
    human_readable_to_cron,
    
    # Interval handling
    is_valid_interval, parse_interval,
    interval_to_cron, cron_to_interval,
    get_interval_seconds, add_interval, subtract_interval,
    
    # Execution windows
    is_within_execution_window, get_execution_window,
    is_within_maintenance_window, get_next_maintenance_window,
    calculate_optimal_retry_time, get_execution_frequency,
    estimate_next_execution_window, is_schedule_active,
    
    # Schedule classes
    CronSchedule, IntervalSchedule, MaintenanceWindow
)

# Default constants
DEFAULT_TIMEZONE = "UTC"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_TIME_FORMAT = "%H:%M:%S"
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"