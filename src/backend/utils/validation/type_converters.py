"""
Type conversion utility for safely converting values between different data types.

This module provides robust type conversion functions that can be used for data validation,
transformation, and sanitization operations. Each converter implements appropriate error
handling and logging to ensure consistent data handling throughout the pipeline.
"""

import typing
import datetime
import decimal
import json
import ast

from ..logging.logger import debug, warning
from ..errors.error_types import DataContentError
from ...constants import AlertSeverity
from ..datetime.date_parser import parse_date, parse_datetime

# Constants for boolean string conversion
TRUE_VALUES = ['true', 't', 'yes', 'y', '1', 'on']
FALSE_VALUES = ['false', 'f', 'no', 'n', '0', 'off']

# Get logger for this module
logger = logger.get_logger(__name__)

def to_str(value: typing.Any, raise_exception: bool = False) -> typing.Optional[str]:
    """
    Converts a value to string with error handling.
    
    Args:
        value: The value to convert
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted string or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, str):
        return value
    
    try:
        return str(value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to string: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to string: {e}", 
                                   "string_conversion", 
                                   {"value": repr(value)}, 
                                   AlertSeverity.MEDIUM, 
                                   self_healable=False)
        return None

def to_int(value: typing.Any, raise_exception: bool = False) -> typing.Optional[int]:
    """
    Converts a value to integer with error handling.
    
    Args:
        value: The value to convert
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted integer or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    
    try:
        if isinstance(value, float):
            return int(value)
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
            return int(float(value))
        
        if isinstance(value, bool):
            return 1 if value else 0
        
        return int(value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to integer: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to integer: {e}",
                                 "integer_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_float(value: typing.Any, raise_exception: bool = False) -> typing.Optional[float]:
    """
    Converts a value to float with error handling.
    
    Args:
        value: The value to convert
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted float or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, float):
        return value
    
    try:
        if isinstance(value, int) and not isinstance(value, bool):
            return float(value)
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
            return float(value)
        
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        
        return float(value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to float: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to float: {e}",
                                 "float_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_decimal(value: typing.Any, raise_exception: bool = False) -> typing.Optional[decimal.Decimal]:
    """
    Converts a value to decimal with error handling.
    
    Args:
        value: The value to convert
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted Decimal or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, decimal.Decimal):
        return value
    
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return decimal.Decimal(str(value))
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
            return decimal.Decimal(value)
        
        if isinstance(value, bool):
            return decimal.Decimal('1') if value else decimal.Decimal('0')
        
        return decimal.Decimal(str(value))
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to Decimal: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to Decimal: {e}",
                                 "decimal_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_bool(value: typing.Any, raise_exception: bool = False) -> typing.Optional[bool]:
    """
    Converts a value to boolean with error handling.
    
    Args:
        value: The value to convert
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted boolean or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, bool):
        return value
    
    try:
        if isinstance(value, (int, float)):
            return bool(value)
        
        if isinstance(value, str):
            value = value.strip().lower()
            if value == '':
                return None
            if value in TRUE_VALUES:
                return True
            if value in FALSE_VALUES:
                return False
            
            # If not in predefined values, raise exception
            raise ValueError(f"String '{value}' is not a recognized boolean value")
        
        # For other types, use Python's built-in bool conversion
        return bool(value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to boolean: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to boolean: {e}",
                                 "boolean_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_date(value: typing.Any, format_str: typing.Optional[str] = None, 
            raise_exception: bool = False) -> typing.Optional[datetime.date]:
    """
    Converts a value to date with error handling.
    
    Args:
        value: The value to convert
        format_str: Optional format string for date parsing
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted date or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    
    try:
        if isinstance(value, datetime.datetime):
            return value.date()
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
            return parse_date(value, format_str)
        
        raise ValueError(f"Cannot convert {type(value)} to date")
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to date: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to date: {e}",
                                 "date_conversion",
                                 {"value": repr(value), "format": format_str},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_datetime(value: typing.Any, format_str: typing.Optional[str] = None, 
                timezone: typing.Optional[str] = None, 
                raise_exception: bool = False) -> typing.Optional[datetime.datetime]:
    """
    Converts a value to datetime with error handling.
    
    Args:
        value: The value to convert
        format_str: Optional format string for datetime parsing
        timezone: Optional timezone for the datetime
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted datetime or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, datetime.datetime):
        # TODO: Handle timezone conversion if needed
        return value
    
    try:
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            return datetime.datetime.combine(value, datetime.time.min)
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
            return parse_datetime(value, format_str, timezone)
        
        raise ValueError(f"Cannot convert {type(value)} to datetime")
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to datetime: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to datetime: {e}",
                                 "datetime_conversion",
                                 {"value": repr(value), "format": format_str, "timezone": timezone},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_time(value: typing.Any, format_str: typing.Optional[str] = None, 
            raise_exception: bool = False) -> typing.Optional[datetime.time]:
    """
    Converts a value to time with error handling.
    
    Args:
        value: The value to convert
        format_str: Optional format string for time parsing
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted time or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    if isinstance(value, datetime.time):
        return value
    
    try:
        if isinstance(value, datetime.datetime):
            return value.time()
        
        if isinstance(value, str):
            value = value.strip()
            if value == '':
                return None
                
            # Try using format string if provided
            if format_str:
                dt = datetime.datetime.strptime(value, format_str)
                return dt.time()
                
            # Try standard formats
            try:
                dt = datetime.datetime.strptime(value, "%H:%M:%S")
                return dt.time()
            except ValueError:
                try:
                    dt = datetime.datetime.strptime(value, "%H:%M")
                    return dt.time()
                except ValueError:
                    dt = parse_datetime(value)
                    return dt.time()
        
        raise ValueError(f"Cannot convert {type(value)} to time")
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to time: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to time: {e}",
                                 "time_conversion",
                                 {"value": repr(value), "format": format_str},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_list(value: typing.Any, item_converter: typing.Optional[typing.Callable] = None, 
            delimiter: str = ',', raise_exception: bool = False) -> typing.Optional[list]:
    """
    Converts a value to list with error handling.
    
    Args:
        value: The value to convert
        item_converter: Optional function to convert each item in the list
        delimiter: Delimiter string for splitting string values
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted list or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    try:
        result = None
        
        if isinstance(value, list):
            result = value.copy() if item_converter is None else [item_converter(item) for item in value]
        elif isinstance(value, (tuple, set)):
            result = list(value) if item_converter is None else [item_converter(item) for item in value]
        elif isinstance(value, str):
            value = value.strip()
            if value == '':
                return []
                
            # Try to parse as JSON if it looks like a JSON array
            if value.startswith('[') and value.endswith(']'):
                try:
                    parsed_list = json.loads(value)
                    if isinstance(parsed_list, list):
                        result = parsed_list if item_converter is None else [item_converter(item) for item in parsed_list]
                    else:
                        # If JSON parsing succeeded but result is not a list
                        result = [parsed_list] if item_converter is None else [item_converter(parsed_list)]
                except json.JSONDecodeError:
                    # If JSON parsing fails, fall back to splitting
                    items = [item.strip() for item in value.split(delimiter)]
                    result = items if item_converter is None else [item_converter(item) for item in items]
            else:
                # Regular string splitting
                items = [item.strip() for item in value.split(delimiter)]
                result = items if item_converter is None else [item_converter(item) for item in items]
        elif isinstance(value, dict):
            result = list(value.keys()) if item_converter is None else [item_converter(key) for key in value.keys()]
        else:
            # For any other type, create a single-item list
            result = [value] if item_converter is None else [item_converter(value)]
            
        return result
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to list: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to list: {e}",
                                 "list_conversion",
                                 {"value": repr(value), "delimiter": delimiter},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_dict(value: typing.Any, key_converter: typing.Optional[typing.Callable] = None,
            value_converter: typing.Optional[typing.Callable] = None, 
            raise_exception: bool = False) -> typing.Optional[dict]:
    """
    Converts a value to dictionary with error handling.
    
    Args:
        value: The value to convert
        key_converter: Optional function to convert dictionary keys
        value_converter: Optional function to convert dictionary values
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted dictionary or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    try:
        result = None
        
        if isinstance(value, dict):
            if key_converter is None and value_converter is None:
                result = value.copy()
            else:
                result = {}
                for k, v in value.items():
                    new_key = key_converter(k) if key_converter is not None else k
                    new_value = value_converter(v) if value_converter is not None else v
                    result[new_key] = new_value
        elif isinstance(value, str):
            value = value.strip()
            if value == '':
                return {}
                
            # Try to parse as JSON
            try:
                parsed_dict = json.loads(value)
                if not isinstance(parsed_dict, dict):
                    raise ValueError(f"JSON value is not a dictionary: {parsed_dict}")
                
                if key_converter is None and value_converter is None:
                    result = parsed_dict
                else:
                    result = {}
                    for k, v in parsed_dict.items():
                        new_key = key_converter(k) if key_converter is not None else k
                        new_value = value_converter(v) if value_converter is not None else v
                        result[new_key] = new_value
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string: {e}")
        elif isinstance(value, list):
            # Try to convert list of key-value pairs to dict
            result = {}
            for item in value:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    k, v = item
                    new_key = key_converter(k) if key_converter is not None else k
                    new_value = value_converter(v) if value_converter is not None else v
                    result[new_key] = new_value
                else:
                    raise ValueError(f"List item is not a key-value pair: {item}")
        else:
            raise ValueError(f"Cannot convert {type(value)} to dict")
            
        return result
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to dict: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to dict: {e}",
                                 "dict_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_set(value: typing.Any, item_converter: typing.Optional[typing.Callable] = None, 
           delimiter: str = ',', raise_exception: bool = False) -> typing.Optional[set]:
    """
    Converts a value to set with error handling.
    
    Args:
        value: The value to convert
        item_converter: Optional function to convert each item in the set
        delimiter: Delimiter string for splitting string values
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted set or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    try:
        if isinstance(value, set):
            return value if item_converter is None else {item_converter(item) for item in value}
        
        # First convert to list, then to set
        list_value = to_list(value, item_converter, delimiter)
        if list_value is None:
            return None
            
        return set(list_value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to set: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to set: {e}",
                                 "set_conversion",
                                 {"value": repr(value), "delimiter": delimiter},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_tuple(value: typing.Any, item_converter: typing.Optional[typing.Callable] = None, 
             delimiter: str = ',', raise_exception: bool = False) -> typing.Optional[tuple]:
    """
    Converts a value to tuple with error handling.
    
    Args:
        value: The value to convert
        item_converter: Optional function to convert each item in the tuple
        delimiter: Delimiter string for splitting string values
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted tuple or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return None
    
    try:
        if isinstance(value, tuple):
            return value if item_converter is None else tuple(item_converter(item) for item in value)
        
        # First convert to list, then to tuple
        list_value = to_list(value, item_converter, delimiter)
        if list_value is None:
            return None
            
        return tuple(list_value)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to tuple: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to tuple: {e}",
                                 "tuple_conversion",
                                 {"value": repr(value), "delimiter": delimiter},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def to_json(value: typing.Any, pretty: bool = False, 
            raise_exception: bool = False) -> typing.Optional[str]:
    """
    Converts a value to JSON string with error handling.
    
    Args:
        value: The value to convert
        pretty: Whether to format the JSON with indentation
        raise_exception: Whether to raise an exception on error
        
    Returns:
        JSON string or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if value is None:
        return 'null'
    
    try:
        # If it's already a string that looks like JSON, validate it
        if isinstance(value, str) and (
            (value.startswith('{') and value.endswith('}')) or 
            (value.startswith('[') and value.endswith(']')) or
            value in ('null', 'true', 'false') or
            (value.startswith('"') and value.endswith('"'))
        ):
            # Validate that it's valid JSON
            json.loads(value)
            return value
        
        # Convert to JSON string
        indent = 2 if pretty else None
        return json.dumps(value, indent=indent, default=str)
    except Exception as e:
        logger.warning(f"Failed to convert {type(value)} to JSON: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to convert {type(value)} to JSON: {e}",
                                 "json_conversion",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def from_json(value: typing.Any, raise_exception: bool = False) -> typing.Any:
    """
    Converts a JSON string to Python object with error handling.
    
    Args:
        value: The JSON string to parse
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Parsed Python object or None if parsing fails and raise_exception is False
        
    Raises:
        DataContentError: If parsing fails and raise_exception is True
    """
    if value is None:
        return None
    
    # If it's not a string, try to convert it to string first
    if not isinstance(value, str):
        value = to_str(value)
        if value is None:
            if raise_exception:
                raise DataContentError(f"Cannot convert {type(value)} to JSON-parseable string",
                                     "json_parsing",
                                     {"value": repr(value)},
                                     AlertSeverity.MEDIUM,
                                     self_healable=False)
            return None
    
    try:
        value = value.strip()
        if not value:
            return None
            
        return json.loads(value)
    except Exception as e:
        logger.warning(f"Failed to parse JSON: {e}")
        if raise_exception:
            raise DataContentError(f"Failed to parse JSON: {e}",
                                 "json_parsing",
                                 {"value": repr(value)},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return None

def convert_value(value: typing.Any, target_type: str, options: typing.Optional[dict] = None, 
                 raise_exception: bool = False) -> typing.Any:
    """
    Generic conversion function that converts a value to a specified type.
    
    Args:
        value: The value to convert
        target_type: The target type to convert to (string identifier)
        options: Optional parameters for the conversion
        raise_exception: Whether to raise an exception on error
        
    Returns:
        Converted value or None if conversion fails and raise_exception is False
        
    Raises:
        DataContentError: If conversion fails and raise_exception is True
    """
    if options is None:
        options = {}
    
    target_type = target_type.lower()
    
    # Map common type names to conversion functions
    converter_map = {
        'str': to_str,
        'string': to_str,
        'int': to_int,
        'integer': to_int,
        'float': to_float,
        'decimal': to_decimal,
        'bool': to_bool,
        'boolean': to_bool,
        'date': to_date,
        'datetime': to_datetime,
        'time': to_time,
        'list': to_list,
        'array': to_list,
        'dict': to_dict,
        'dictionary': to_dict,
        'map': to_dict,
        'set': to_set,
        'tuple': to_tuple,
        'json': to_json,
        'object': from_json
    }
    
    # Get converter function for target type
    converter = converter_map.get(target_type)
    if not converter:
        logger.warning(f"No converter found for target type: {target_type}")
        if raise_exception:
            raise DataContentError(f"No converter found for target type: {target_type}",
                                 "type_conversion",
                                 {"value": repr(value), "target_type": target_type},
                                 AlertSeverity.MEDIUM,
                                 self_healable=False)
        return value
    
    # Apply converter with options
    return converter(value, **options, raise_exception=raise_exception)

class TypeConverter:
    """
    Class that provides a configurable way to convert data between types.
    
    This class allows registration of custom converter functions and provides methods
    to convert values and dictionaries based on type mappings.
    """
    
    def __init__(self, config: typing.Optional[dict] = None):
        """
        Initialize the TypeConverter with configuration.
        
        Args:
            config: Optional configuration dictionary
        """
        self._converters = {}
        self._config = {
            'raise_exception': False,
            'strict_conversion': True
        }
        
        # Override defaults with provided config
        if config:
            self._config.update(config)
        
        # Register built-in converters
        self._register_default_converters()
    
    def register_converter(self, target_type: str, converter_func: typing.Callable) -> None:
        """
        Register a converter function for a specific target type.
        
        Args:
            target_type: The target type identifier string
            converter_func: The converter function to register
        """
        self._converters[target_type.lower()] = converter_func
    
    def convert(self, value: typing.Any, target_type: str, 
                options: typing.Optional[dict] = None, 
                raise_exception: bool = None) -> typing.Any:
        """
        Convert a value to the specified target type.
        
        Args:
            value: The value to convert
            target_type: The target type to convert to
            options: Optional parameters for the conversion
            raise_exception: Whether to raise an exception on error (overrides config)
            
        Returns:
            Converted value or None if conversion fails and raise_exception is False
        """
        if options is None:
            options = {}
            
        # Determine whether to raise exception
        if raise_exception is None:
            raise_exception = self._config.get('raise_exception', False)
        
        # Get converter function for target type
        converter = self.get_converter(target_type)
        if not converter:
            logger.warning(f"No converter registered for target type: {target_type}")
            return value
        
        # Apply converter
        return converter(value, **options, raise_exception=raise_exception)
    
    def convert_dict(self, data: dict, type_mapping: dict, 
                    options: typing.Optional[dict] = None, 
                    raise_exception: bool = None) -> dict:
        """
        Convert values in a dictionary based on type mapping.
        
        Args:
            data: Source dictionary to convert
            type_mapping: Mapping of field names to target types
            options: Optional parameters for conversion keyed by field name
            raise_exception: Whether to raise exceptions on errors (overrides config)
            
        Returns:
            Dictionary with converted values
        """
        if data is None:
            return {}
            
        if options is None:
            options = {}
            
        # Determine whether to raise exception
        if raise_exception is None:
            raise_exception = self._config.get('raise_exception', False)
        
        result = {}
        for key, value in data.items():
            # Skip fields not in type mapping if strict conversion
            if key not in type_mapping and self._config.get('strict_conversion', True):
                result[key] = value
                continue
                
            # Get target type for this field
            target_type = type_mapping.get(key)
            if not target_type:
                result[key] = value
                continue
                
            # Get field-specific options if available
            field_options = options.get(key, {})
            
            # Convert the value
            converted_value = self.convert(value, target_type, field_options, raise_exception)
            result[key] = converted_value
        
        return result
    
    def get_converter(self, target_type: str) -> typing.Optional[typing.Callable]:
        """
        Get the converter function for a specific target type.
        
        Args:
            target_type: The target type identifier string
            
        Returns:
            Converter function or None if not found
        """
        return self._converters.get(target_type.lower())
    
    def _register_default_converters(self) -> None:
        """
        Register default converters for common target types.
        """
        # Register built-in converters
        self.register_converter('str', to_str)
        self.register_converter('string', to_str)
        self.register_converter('int', to_int)
        self.register_converter('integer', to_int)
        self.register_converter('float', to_float)
        self.register_converter('decimal', to_decimal)
        self.register_converter('bool', to_bool)
        self.register_converter('boolean', to_bool)
        self.register_converter('date', to_date)
        self.register_converter('datetime', to_datetime)
        self.register_converter('time', to_time)
        self.register_converter('list', to_list)
        self.register_converter('array', to_list)
        self.register_converter('dict', to_dict)
        self.register_converter('dictionary', to_dict)
        self.register_converter('map', to_dict)
        self.register_converter('set', to_set)
        self.register_converter('tuple', to_tuple)
        self.register_converter('json', to_json)
        self.register_converter('object', from_json)