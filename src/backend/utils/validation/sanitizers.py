"""
Provides data sanitization utilities for cleaning and normalizing data before validation or processing.

These sanitizers ensure data consistency by removing unwanted characters, normalizing formats,
and applying transformations to make data suitable for validation and further processing
in the self-healing pipeline.
"""

import re
import typing
import unicodedata
import string
import html
import pandas

from ..logging.logger import debug, warning
from ..errors.error_types import DataFormatError
from ...constants import AlertSeverity
from .type_converters import to_str, to_int, to_float

# Regular expression patterns
WHITESPACE_PATTERN = re.compile(r'\s+')
MULTIPLE_UNDERSCORE_PATTERN = re.compile(r'_+')
SPECIAL_CHARS_PATTERN = re.compile(r'[^\w\s]')
NUMERIC_PATTERN = re.compile(r'[^0-9.]')
ALPHA_PATTERN = re.compile(r'[^a-zA-Z]')
ALPHANUMERIC_PATTERN = re.compile(r'[^a-zA-Z0-9]')
EMAIL_CHARS_PATTERN = re.compile(r'[^a-zA-Z0-9.@_+-]')
HTML_TAGS_PATTERN = re.compile(r'<[^>]*>')

# Logger for this module
logger = get_logger(__name__)

def sanitize_string(value: typing.Any, 
                    strip_whitespace: bool = True, 
                    normalize_whitespace: bool = True,
                    remove_special_chars: bool = False,
                    special_chars_replacement: typing.Optional[str] = None,
                    lowercase: bool = False,
                    uppercase: bool = False,
                    default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a string by removing or replacing unwanted characters.
    
    Args:
        value: The value to sanitize
        strip_whitespace: Whether to strip leading and trailing whitespace
        normalize_whitespace: Whether to replace multiple whitespaces with a single space
        remove_special_chars: Whether to remove special characters
        special_chars_replacement: Character to replace special chars with (None = remove)
        lowercase: Whether to convert the string to lowercase
        uppercase: Whether to convert the string to uppercase
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized string or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for sanitization")
            return default_value
        
        # Apply sanitization steps
        if strip_whitespace:
            result = result.strip()
            
        if normalize_whitespace:
            result = WHITESPACE_PATTERN.sub(' ', result)
            
        if remove_special_chars:
            if special_chars_replacement is not None:
                result = SPECIAL_CHARS_PATTERN.sub(special_chars_replacement, result)
            else:
                result = SPECIAL_CHARS_PATTERN.sub('', result)
                
        if lowercase:
            result = result.lower()
            
        if uppercase:
            result = result.upper()
            
        return result
    except Exception as e:
        logger.warning(f"String sanitization failed: {e}")
        return default_value

def sanitize_numeric(value: typing.Any,
                     allow_decimal: bool = True,
                     allow_negative: bool = True,
                     default_value: typing.Optional[typing.Union[int, float]] = None,
                     return_as_string: bool = False) -> typing.Optional[typing.Union[str, int, float]]:
    """
    Sanitizes a numeric string by removing non-numeric characters.
    
    Args:
        value: The value to sanitize
        allow_decimal: Whether to allow decimal points
        allow_negative: Whether to allow negative sign
        default_value: Value to return if sanitization fails
        return_as_string: Whether to return the sanitized value as a string
        
    Returns:
        Sanitized numeric value or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for numeric sanitization")
            return default_value
            
        # Remove all non-numeric characters
        if allow_decimal:
            # Keep only digits and decimal points
            result = ''.join(c for c in result if c.isdigit() or c == '.')
            
            # Ensure there's only one decimal point
            decimal_parts = result.split('.')
            if len(decimal_parts) > 2:
                result = decimal_parts[0] + '.' + ''.join(decimal_parts[1:])
        else:
            # Keep only digits
            result = ''.join(c for c in result if c.isdigit())

        # Handle negative sign
        if allow_negative and isinstance(value, (int, float)) and value < 0:
            result = '-' + result
        elif allow_negative and isinstance(value, str) and value.strip().startswith('-'):
            result = '-' + result

        # Convert to requested type
        if not result:
            return default_value
            
        if return_as_string:
            return result
        
        if allow_decimal:
            try:
                return float(result)
            except ValueError:
                return default_value
        else:
            try:
                return int(result)
            except ValueError:
                return default_value
                
    except Exception as e:
        logger.warning(f"Numeric sanitization failed: {e}")
        return default_value

def sanitize_alphanumeric(value: typing.Any,
                         allow_spaces: bool = False,
                         allow_underscores: bool = True,
                         replacement_char: typing.Optional[str] = None,
                         default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a string by removing all non-alphanumeric characters.
    
    Args:
        value: The value to sanitize
        allow_spaces: Whether to allow spaces
        allow_underscores: Whether to allow underscores
        replacement_char: Character to replace non-alphanumeric chars with (None = remove)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized alphanumeric string or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for alphanumeric sanitization")
            return default_value
            
        # Create a pattern based on allowed characters
        pattern = r'[^a-zA-Z0-9'
        if allow_spaces:
            pattern += r'\s'
        if allow_underscores:
            pattern += r'_'
        pattern += r']'
        
        # Remove or replace non-matching characters
        if replacement_char is not None:
            result = re.sub(pattern, replacement_char, result)
        else:
            result = re.sub(pattern, '', result)
            
        return result
    except Exception as e:
        logger.warning(f"Alphanumeric sanitization failed: {e}")
        return default_value
        
def sanitize_email(value: typing.Any,
                  lowercase: bool = True,
                  default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes an email address by removing invalid characters.
    
    Args:
        value: The value to sanitize
        lowercase: Whether to convert the email to lowercase
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized email address or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for email sanitization")
            return default_value
            
        # Remove characters not valid in email addresses
        result = EMAIL_CHARS_PATTERN.sub('', result)
        
        # Convert to lowercase if requested
        if lowercase:
            result = result.lower()
            
        return result
    except Exception as e:
        logger.warning(f"Email sanitization failed: {e}")
        return default_value
        
def sanitize_phone(value: typing.Any,
                  keep_plus_prefix: bool = True,
                  format_pattern: typing.Optional[str] = None,
                  default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a phone number by removing non-digit characters.
    
    Args:
        value: The value to sanitize
        keep_plus_prefix: Whether to preserve the + prefix for international numbers
        format_pattern: Optional format pattern to apply (using {} as placeholder)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized phone number or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for phone sanitization")
            return default_value
            
        # Check if the number starts with a plus and needs to be preserved
        has_plus_prefix = result.strip().startswith('+') and keep_plus_prefix
        
        # Remove all non-digit characters
        digits = ''.join(c for c in result if c.isdigit())
        
        # Add the plus prefix back if needed
        if has_plus_prefix:
            digits = '+' + digits
            
        # Apply formatting if provided
        if format_pattern and digits:
            try:
                # Simple formatting with placeholder
                result = format_pattern.replace('{}', digits)
            except Exception as format_err:
                logger.warning(f"Failed to apply phone format: {format_err}")
                result = digits
        else:
            result = digits
            
        return result
    except Exception as e:
        logger.warning(f"Phone sanitization failed: {e}")
        return default_value
        
def sanitize_html(value: typing.Any,
                 remove_tags: bool = True,
                 escape_html: bool = False,
                 default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes HTML content by removing tags or escaping special characters.
    
    Args:
        value: The value to sanitize
        remove_tags: Whether to remove HTML tags
        escape_html: Whether to escape HTML special characters
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized HTML content or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for HTML sanitization")
            return default_value
            
        # Remove HTML tags if requested
        if remove_tags:
            result = HTML_TAGS_PATTERN.sub('', result)
            
        # Escape HTML if requested
        if escape_html:
            result = html.escape(result)
            
        return result
    except Exception as e:
        logger.warning(f"HTML sanitization failed: {e}")
        return default_value

def sanitize_filename(value: typing.Any,
                     replacement_char: typing.Optional[str] = '_',
                     default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a filename by removing invalid characters.
    
    Args:
        value: The value to sanitize
        replacement_char: Character to replace invalid chars with (None = remove)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized filename or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for filename sanitization")
            return default_value
            
        # Remove or replace invalid filename characters
        # Invalid chars: / \ : * ? " < > |
        invalid_chars = r'[/\\:*?"<>|]'
        if replacement_char is not None:
            result = re.sub(invalid_chars, replacement_char, result)
        else:
            result = re.sub(invalid_chars, '', result)
            
        # Ensure filename doesn't start or end with spaces or dots
        result = result.strip().strip('.')
        
        # Replace multiple replacement chars with a single one
        if replacement_char is not None:
            result = re.sub(f'{re.escape(replacement_char)}+', replacement_char, result)
            
        return result
    except Exception as e:
        logger.warning(f"Filename sanitization failed: {e}")
        return default_value
        
def sanitize_sql_identifier(value: typing.Any,
                           lowercase: bool = True,
                           replacement_char: typing.Optional[str] = '_',
                           default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a SQL identifier (table or column name) by removing invalid characters.
    
    Args:
        value: The value to sanitize
        lowercase: Whether to convert to lowercase
        replacement_char: Character to replace invalid chars with (None = remove)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized SQL identifier or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for SQL identifier sanitization")
            return default_value
            
        # Remove or replace invalid SQL identifier characters
        # Allow only alphanumeric and underscore, and ensure it starts with a letter or underscore
        if replacement_char is not None:
            result = re.sub(r'[^\w]', replacement_char, result)
        else:
            result = re.sub(r'[^\w]', '', result)
            
        # Ensure identifier starts with a letter or underscore
        if result and not (result[0].isalpha() or result[0] == '_'):
            result = '_' + result
            
        # Convert to lowercase if requested
        if lowercase:
            result = result.lower()
            
        # Replace multiple replacement chars with a single one
        if replacement_char is not None:
            result = re.sub(f'{re.escape(replacement_char)}+', replacement_char, result)
            
        return result
    except Exception as e:
        logger.warning(f"SQL identifier sanitization failed: {e}")
        return default_value

def sanitize_url(value: typing.Any,
                ensure_protocol: bool = True,
                default_protocol: typing.Optional[str] = 'https://',
                default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a URL by encoding special characters and ensuring valid format.
    
    Args:
        value: The value to sanitize
        ensure_protocol: Whether to ensure the URL has a protocol prefix
        default_protocol: Protocol to add if missing (when ensure_protocol is True)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized URL or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for URL sanitization")
            return default_value
            
        # Trim whitespace
        result = result.strip()
        
        # Add protocol if missing and ensure_protocol is True
        if ensure_protocol and default_protocol and not re.match(r'^[a-z]+://', result):
            result = default_protocol + result
            
        # Use urllib to properly encode the URL
        from urllib.parse import urlparse, urlunparse, quote

        try:
            # Parse URL into components
            parsed = urlparse(result)
            
            # Encode each component properly
            path = quote(parsed.path)
            
            # Reassemble the URL with encoded components
            result = urlunparse((
                parsed.scheme,
                parsed.netloc,
                path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
        except Exception as url_err:
            logger.warning(f"URL encoding failed: {url_err}")
            # Return the original result if encoding fails
            
        return result
    except Exception as e:
        logger.warning(f"URL sanitization failed: {e}")
        return default_value
        
def sanitize_json_key(value: typing.Any,
                     lowercase: bool = False,
                     replacement_char: typing.Optional[str] = '_',
                     default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes a JSON key by removing invalid characters.
    
    Args:
        value: The value to sanitize
        lowercase: Whether to convert to lowercase
        replacement_char: Character to replace invalid chars with (None = remove)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized JSON key or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for JSON key sanitization")
            return default_value
            
        # Remove or replace invalid JSON key characters
        # Allow alphanumeric and underscore, avoid spaces
        if replacement_char is not None:
            result = re.sub(r'[^\w]', replacement_char, result)
        else:
            result = re.sub(r'[^\w]', '', result)
            
        # Convert to lowercase if requested
        if lowercase:
            result = result.lower()
            
        # Replace multiple replacement chars with a single one
        if replacement_char is not None:
            result = re.sub(f'{re.escape(replacement_char)}+', replacement_char, result)
            
        return result
    except Exception as e:
        logger.warning(f"JSON key sanitization failed: {e}")
        return default_value
        
def sanitize_whitespace(value: typing.Any,
                       strip: bool = True,
                       normalize: bool = True,
                       replacement_char: typing.Optional[str] = ' ',
                       default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Sanitizes whitespace in a string by normalizing or removing it.
    
    Args:
        value: The value to sanitize
        strip: Whether to strip leading and trailing whitespace
        normalize: Whether to replace multiple whitespace with replacement_char
        replacement_char: Character to replace whitespace with (default is single space)
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized string or default_value if sanitization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for whitespace sanitization")
            return default_value
            
        # Strip leading and trailing whitespace if requested
        if strip:
            result = result.strip()
            
        # Normalize internal whitespace if requested
        if normalize:
            if replacement_char is not None:
                result = WHITESPACE_PATTERN.sub(replacement_char, result)
            else:
                result = WHITESPACE_PATTERN.sub('', result)
                
        return result
    except Exception as e:
        logger.warning(f"Whitespace sanitization failed: {e}")
        return default_value

def sanitize_list(value: typing.Any,
                 sanitizer_func: typing.Callable,
                 sanitizer_args: typing.Optional[dict] = None,
                 default_value: typing.Optional[list] = None) -> typing.Optional[list]:
    """
    Sanitizes each element in a list using a provided sanitizer function.
    
    Args:
        value: The list or iterable to sanitize
        sanitizer_func: Function to apply to each element
        sanitizer_args: Arguments to pass to the sanitizer function
        default_value: Value to return if sanitization fails
        
    Returns:
        List with sanitized elements or default_value if sanitization fails
    """
    try:
        # Handle None input
        if value is None:
            return default_value
            
        # Convert to list if not already one
        if not isinstance(value, list):
            try:
                value = list(value)
            except Exception as e:
                logger.warning(f"Failed to convert {type(value)} to list: {e}")
                return default_value
                
        # Initialize sanitizer_args if None
        if sanitizer_args is None:
            sanitizer_args = {}
            
        # Apply sanitizer to each element
        result = [sanitizer_func(item, **sanitizer_args) for item in value]
        
        return result
    except Exception as e:
        logger.warning(f"List sanitization failed: {e}")
        return default_value
        
def sanitize_dict(value: typing.Any,
                 key_sanitizer: typing.Optional[typing.Callable] = None,
                 value_sanitizer: typing.Optional[typing.Callable] = None,
                 key_sanitizer_args: typing.Optional[dict] = None,
                 value_sanitizer_args: typing.Optional[dict] = None,
                 default_value: typing.Optional[dict] = None) -> typing.Optional[dict]:
    """
    Sanitizes keys and/or values in a dictionary using provided sanitizer functions.
    
    Args:
        value: The dictionary to sanitize
        key_sanitizer: Function to sanitize keys (None = no key sanitization)
        value_sanitizer: Function to sanitize values (None = no value sanitization)
        key_sanitizer_args: Arguments to pass to the key sanitizer function
        value_sanitizer_args: Arguments to pass to the value sanitizer function
        default_value: Value to return if sanitization fails
        
    Returns:
        Dictionary with sanitized keys/values or default_value if sanitization fails
    """
    try:
        # Handle None input
        if value is None:
            return default_value
            
        # Convert to dict if not already one
        if not isinstance(value, dict):
            try:
                value = dict(value)
            except Exception as e:
                logger.warning(f"Failed to convert {type(value)} to dict: {e}")
                return default_value
                
        # Initialize sanitizer_args if None
        if key_sanitizer_args is None:
            key_sanitizer_args = {}
        if value_sanitizer_args is None:
            value_sanitizer_args = {}
            
        # Create new dictionary with sanitized data
        result = {}
        for k, v in value.items():
            # Sanitize key if key_sanitizer provided
            if key_sanitizer is not None:
                sanitized_key = key_sanitizer(k, **key_sanitizer_args)
            else:
                sanitized_key = k
                
            # Sanitize value if value_sanitizer provided
            if value_sanitizer is not None:
                sanitized_value = value_sanitizer(v, **value_sanitizer_args)
            else:
                sanitized_value = v
                
            # Add sanitized key-value pair to result
            result[sanitized_key] = sanitized_value
            
        return result
    except Exception as e:
        logger.warning(f"Dict sanitization failed: {e}")
        return default_value
        
def sanitize_dataframe(df: typing.Any,
                      column_sanitizers: typing.Dict[str, typing.Callable],
                      sanitizer_args: typing.Optional[typing.Dict[str, dict]] = None,
                      inplace: bool = False,
                      default_value: typing.Optional[pandas.DataFrame] = None) -> typing.Optional[pandas.DataFrame]:
    """
    Sanitizes columns in a pandas DataFrame using provided sanitizer functions.
    
    Args:
        df: The DataFrame to sanitize
        column_sanitizers: Dict mapping column names to sanitizer functions
        sanitizer_args: Dict mapping column names to sanitizer function arguments
        inplace: Whether to modify the DataFrame in place
        default_value: Value to return if sanitization fails
        
    Returns:
        DataFrame with sanitized columns or default_value if sanitization fails
    """
    try:
        # Handle None input
        if df is None:
            return default_value
            
        # Convert to DataFrame if not already one
        if not isinstance(df, pandas.DataFrame):
            try:
                df = pandas.DataFrame(df)
            except Exception as e:
                logger.warning(f"Failed to convert {type(df)} to DataFrame: {e}")
                return default_value
                
        # Initialize sanitizer_args if None
        if sanitizer_args is None:
            sanitizer_args = {}
            
        # Create a copy if not inplace
        if not inplace:
            df = df.copy()
            
        # Apply sanitizers to each column
        for column, sanitizer in column_sanitizers.items():
            if column in df.columns:
                # Get sanitizer args for this column
                args = sanitizer_args.get(column, {})
                
                # Apply sanitizer to the column
                df[column] = df[column].apply(lambda x: sanitizer(x, **args))
                
        return df
    except Exception as e:
        logger.warning(f"DataFrame sanitization failed: {e}")
        return default_value

def normalize_unicode(value: typing.Any,
                     form: str = 'NFKC',
                     default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Normalizes Unicode characters in a string.
    
    Args:
        value: The value to normalize
        form: Unicode normalization form (NFC, NFD, NFKC, NFKD)
        default_value: Value to return if normalization fails
        
    Returns:
        String with normalized Unicode characters or default_value if normalization fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for Unicode normalization")
            return default_value
            
        # Apply Unicode normalization
        return unicodedata.normalize(form, result)
    except Exception as e:
        logger.warning(f"Unicode normalization failed: {e}")
        return default_value
        
def remove_accents(value: typing.Any,
                  default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Removes accents from characters in a string.
    
    Args:
        value: The value to process
        default_value: Value to return if processing fails
        
    Returns:
        String with accents removed or default_value if processing fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for accent removal")
            return default_value
            
        # Normalize to NFKD form to separate base characters from accents
        result = unicodedata.normalize('NFKD', result)
        
        # Filter out non-ASCII characters (accents are represented as non-ASCII chars in NFKD)
        result = ''.join([c for c in result if not unicodedata.combining(c)])
        
        return result
    except Exception as e:
        logger.warning(f"Accent removal failed: {e}")
        return default_value

def truncate_string(value: typing.Any,
                   max_length: int,
                   suffix: str = '...',
                   default_value: typing.Optional[str] = None) -> typing.Optional[str]:
    """
    Truncates a string to a specified maximum length.
    
    Args:
        value: The string to truncate
        max_length: Maximum length of the resulting string
        suffix: String to append when truncation occurs
        default_value: Value to return if truncation fails
        
    Returns:
        Truncated string or default_value if truncation fails
    """
    try:
        # Convert to string
        result = to_str(value)
        if result is None:
            logger.warning(f"Failed to convert {type(value)} to string for truncation")
            return default_value
            
        # If string is shorter than max_length, return as is
        if len(result) <= max_length:
            return result
            
        # Truncate and add suffix
        return result[:max_length - len(suffix)] + suffix
    except Exception as e:
        logger.warning(f"String truncation failed: {e}")
        return default_value

def sanitize(value: typing.Any,
            data_type: str,
            options: typing.Optional[dict] = None,
            default_value: typing.Any = None) -> typing.Any:
    """
    Generic sanitization function that applies a sanitizer based on data type.
    
    Args:
        value: The value to sanitize
        data_type: Type of data to sanitize (string, numeric, email, etc.)
        options: Options for the selected sanitizer
        default_value: Value to return if sanitization fails
        
    Returns:
        Sanitized value or default_value if sanitization fails
    """
    try:
        # Initialize options dict if None
        if options is None:
            options = {}
            
        # Select appropriate sanitization function based on data_type
        sanitizer_map = {
            'string': sanitize_string,
            'numeric': sanitize_numeric,
            'alphanumeric': sanitize_alphanumeric,
            'email': sanitize_email,
            'phone': sanitize_phone,
            'html': sanitize_html,
            'filename': sanitize_filename,
            'sql_identifier': sanitize_sql_identifier,
            'url': sanitize_url,
            'json_key': sanitize_json_key,
            'whitespace': sanitize_whitespace,
            'unicode': normalize_unicode,
            'accents': remove_accents,
            'truncate': truncate_string
        }
        
        # Get sanitizer function for data_type
        sanitizer = sanitizer_map.get(data_type.lower())
        if not sanitizer:
            logger.warning(f"No sanitizer found for data type: {data_type}")
            return value
            
        # Apply sanitizer with options
        return sanitizer(value, **options)
    except Exception as e:
        logger.warning(f"Sanitization failed for data type {data_type}: {e}")
        return default_value

class DataSanitizer:
    """
    Class that provides a configurable way to sanitize data based on data types.
    """
    
    def __init__(self, config: typing.Optional[dict] = None):
        """
        Initialize the DataSanitizer with configuration.
        
        Args:
            config: Optional configuration dictionary
        """
        # Initialize sanitizers dict and config
        self._sanitizers = {}
        self._config = {
            # Default configuration options
            'default_sanitizers': True
        }
        
        # Override defaults with provided config
        if config:
            self._config.update(config)
            
        # Register default sanitizers if enabled
        if self._config.get('default_sanitizers', True):
            self._register_default_sanitizers()
            
    def register_sanitizer(self, data_type: str, sanitizer_func: typing.Callable) -> None:
        """
        Register a sanitizer function for a specific data type.
        
        Args:
            data_type: Type of data the sanitizer handles
            sanitizer_func: The sanitizer function to register
        """
        self._sanitizers[data_type.lower()] = sanitizer_func
        
    def sanitize(self, value: typing.Any, data_type: str, 
                options: typing.Optional[dict] = None, 
                default_value: typing.Any = None) -> typing.Any:
        """
        Sanitize a value based on its data type.
        
        Args:
            value: The value to sanitize
            data_type: Type of data to sanitize
            options: Options for the sanitizer
            default_value: Value to return if sanitization fails
            
        Returns:
            Sanitized value or default_value if sanitization fails
        """
        try:
            # Initialize options dict if None
            if options is None:
                options = {}
                
            # Get sanitizer for data_type
            sanitizer = self._sanitizers.get(data_type.lower())
            if not sanitizer:
                logger.warning(f"No sanitizer registered for data type: {data_type}")
                return value
                
            # Apply sanitizer
            return sanitizer(value, **options, default_value=default_value)
        except Exception as e:
            logger.warning(f"Sanitization failed for data type {data_type}: {e}")
            return default_value
            
    def sanitize_dict(self, data: dict, type_mapping: dict, 
                     options: typing.Optional[dict] = None) -> dict:
        """
        Sanitize values in a dictionary based on data type mapping.
        
        Args:
            data: Dictionary with values to sanitize
            type_mapping: Mapping of field names to data types
            options: Options for sanitizers, keyed by field name
            
        Returns:
            Dictionary with sanitized values
        """
        # Handle None input
        if data is None:
            return {}
            
        # Initialize options dict if None
        if options is None:
            options = {}
            
        # Create new dictionary with sanitized values
        result = {}
        for key, value in data.items():
            # Get data_type from type_mapping
            data_type = type_mapping.get(key)
            if not data_type:
                # If no data_type defined, keep original value
                result[key] = value
                continue
                
            # Get field-specific options if available
            field_options = options.get(key, {})
            
            # Sanitize value
            result[key] = self.sanitize(value, data_type, field_options)
            
        return result
        
    def get_sanitizer(self, data_type: str) -> typing.Optional[typing.Callable]:
        """
        Get the sanitizer function for a specific data type.
        
        Args:
            data_type: Type of data
            
        Returns:
            Sanitizer function or None if not found
        """
        return self._sanitizers.get(data_type.lower())
        
    def _register_default_sanitizers(self) -> None:
        """
        Register default sanitizers for common data types.
        """
        self.register_sanitizer('string', sanitize_string)
        self.register_sanitizer('numeric', sanitize_numeric)
        self.register_sanitizer('alphanumeric', sanitize_alphanumeric)
        self.register_sanitizer('email', sanitize_email)
        self.register_sanitizer('phone', sanitize_phone)
        self.register_sanitizer('html', sanitize_html)
        self.register_sanitizer('filename', sanitize_filename)
        self.register_sanitizer('sql_identifier', sanitize_sql_identifier)
        self.register_sanitizer('url', sanitize_url)
        self.register_sanitizer('json_key', sanitize_json_key)
        self.register_sanitizer('whitespace', sanitize_whitespace)
        self.register_sanitizer('unicode', normalize_unicode)
        self.register_sanitizer('accents', remove_accents)
        self.register_sanitizer('truncate', truncate_string)

class SanitizationRule:
    """
    Class representing a sanitization rule with configurable options.
    """
    
    def __init__(self, name: str, field_name: str, data_type: str, 
                sanitizer_func: typing.Callable, options: typing.Optional[dict] = None):
        """
        Initialize a sanitization rule.
        
        Args:
            name: Rule name for identification
            field_name: Name of the field this rule applies to
            data_type: Type of data this rule handles
            sanitizer_func: Function to use for sanitization
            options: Options to pass to the sanitizer function
        """
        self.name = name
        self.field_name = field_name
        self.data_type = data_type
        self.sanitizer_func = sanitizer_func
        self.options = options or {}
        
    def apply(self, value: typing.Any) -> typing.Any:
        """
        Apply the sanitization rule to a value.
        
        Args:
            value: The value to sanitize
            
        Returns:
            Sanitized value
        """
        return self.sanitizer_func(value, **self.options)
        
    def to_dict(self) -> dict:
        """
        Convert the sanitization rule to a dictionary representation.
        
        Returns:
            Dictionary representation of the rule
        """
        return {
            'name': self.name,
            'field_name': self.field_name,
            'data_type': self.data_type,
            'options': self.options
        }
        
    @classmethod
    def from_dict(cls, rule_dict: dict, sanitizer_map: typing.Dict[str, typing.Callable]) -> 'SanitizationRule':
        """
        Create a sanitization rule from a dictionary representation.
        
        Args:
            rule_dict: Dictionary representation of the rule
            sanitizer_map: Mapping of data types to sanitizer functions
            
        Returns:
            Sanitization rule instance
        """
        # Get sanitizer function from sanitizer_map
        data_type = rule_dict.get('data_type')
        sanitizer_func = sanitizer_map.get(data_type)
        if not sanitizer_func:
            raise ValueError(f"No sanitizer function found for data type: {data_type}")
            
        # Create and return SanitizationRule instance
        return cls(
            name=rule_dict.get('name'),
            field_name=rule_dict.get('field_name'),
            data_type=data_type,
            sanitizer_func=sanitizer_func,
            options=rule_dict.get('options', {})
        )

class SanitizationRuleSet:
    """
    Collection of sanitization rules that can be applied together.
    """
    
    def __init__(self, name: str, rules: typing.Optional[typing.List[SanitizationRule]] = None):
        """
        Initialize a sanitization rule set.
        
        Args:
            name: Rule set name for identification
            rules: List of sanitization rules
        """
        self.name = name
        self.rules = rules or []
        
    def add_rule(self, rule: SanitizationRule) -> None:
        """
        Add a sanitization rule to the rule set.
        
        Args:
            rule: Sanitization rule to add
        """
        self.rules.append(rule)
        
    def apply(self, data: typing.Any) -> typing.Any:
        """
        Apply all sanitization rules to a value or dictionary.
        
        Args:
            data: The data to sanitize (scalar value or dictionary)
            
        Returns:
            Sanitized data
        """
        # If data is a dictionary, sanitize fields according to rules
        if isinstance(data, dict):
            result = data.copy()
            for rule in self.rules:
                if rule.field_name in result:
                    result[rule.field_name] = rule.apply(result[rule.field_name])
            return result
        else:
            # For non-dict data, apply all rules sequentially
            result = data
            for rule in self.rules:
                result = rule.apply(result)
            return result
            
    def to_dict(self) -> dict:
        """
        Convert the sanitization rule set to a dictionary representation.
        
        Returns:
            Dictionary representation of the rule set
        """
        return {
            'name': self.name,
            'rules': [rule.to_dict() for rule in self.rules]
        }
        
    @classmethod
    def from_dict(cls, ruleset_dict: dict, sanitizer_map: typing.Dict[str, typing.Callable]) -> 'SanitizationRuleSet':
        """
        Create a sanitization rule set from a dictionary representation.
        
        Args:
            ruleset_dict: Dictionary representation of the rule set
            sanitizer_map: Mapping of data types to sanitizer functions
            
        Returns:
            Sanitization rule set instance
        """
        # Create the rule set
        ruleset = cls(name=ruleset_dict.get('name', 'unnamed'))
        
        # Add rules from the dictionary
        for rule_dict in ruleset_dict.get('rules', []):
            rule = SanitizationRule.from_dict(rule_dict, sanitizer_map)
            ruleset.add_rule(rule)
            
        return ruleset