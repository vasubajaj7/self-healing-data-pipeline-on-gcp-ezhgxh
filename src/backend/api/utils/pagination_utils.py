"""
Utility functions for handling pagination in API responses for the self-healing data pipeline.

This module provides helper functions to create pagination metadata, calculate
pagination parameters, and generate pagination links for list endpoints.
"""

from typing import Dict, Optional, Tuple, Any
import math
import urllib.parse

from fastapi import Request  # fastapi ^0.95.0

from ..models.response_models import PaginationMetadata
from ..models.request_models import PaginationParams

# Default pagination parameters
DEFAULT_PAGE = '1'
DEFAULT_PAGE_SIZE = '20'
MAX_PAGE_SIZE = '100'


def create_pagination_metadata(
    page: int,
    page_size: int,
    total_items: int,
    request: Request,
    sort_by: Optional[str] = None,
    descending: Optional[bool] = None
) -> PaginationMetadata:
    """
    Creates pagination metadata for list responses.

    Args:
        page: Current page number.
        page_size: Number of items per page.
        total_items: Total number of items.
        request: FastAPI request object for link generation.
        sort_by: Field used for sorting (optional).
        descending: Whether sorting is in descending order (optional).

    Returns:
        PaginationMetadata: Pagination metadata for the response.
    """
    # Calculate total pages
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 1

    # If items exist but count is not sufficient for the requested page, adjust page
    if total_items > 0 and page > total_pages:
        page = total_pages

    # Generate next and previous page links
    next_page, previous_page = get_pagination_links(
        request, page, page_size, total_pages, sort_by, descending
    )

    # Create and return the pagination metadata
    return PaginationMetadata(
        page=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=total_pages,
        next_page=next_page,
        previous_page=previous_page
    )


def get_pagination_links(
    request: Request,
    page: int,
    page_size: int,
    total_pages: int,
    sort_by: Optional[str] = None,
    descending: Optional[bool] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Generates next and previous page links for pagination.

    Args:
        request: FastAPI request object.
        page: Current page number.
        page_size: Number of items per page.
        total_pages: Total number of pages.
        sort_by: Field used for sorting (optional).
        descending: Whether sorting is in descending order (optional).

    Returns:
        Tuple[Optional[str], Optional[str]]: Tuple of (next_page, previous_page) links.
    """
    # Parse the current request URL
    base_url = str(request.url)
    parsed_url = urllib.parse.urlparse(base_url)
    query_params = dict(urllib.parse.parse_qsl(parsed_url.query))
    
    # Initialize links as None
    next_page = None
    previous_page = None

    # Generate next page link
    if page < total_pages:
        next_page_params = query_params.copy()
        next_page_params['page'] = str(page + 1)
        next_page_params['page_size'] = str(page_size)
        
        if sort_by is not None:
            next_page_params['sort_by'] = sort_by
        
        if descending is not None:
            next_page_params['descending'] = str(descending).lower()
            
        next_page_query = urllib.parse.urlencode(next_page_params)
        next_page_url = urllib.parse.urlunparse(
            (parsed_url.scheme, parsed_url.netloc, parsed_url.path, 
             parsed_url.params, next_page_query, parsed_url.fragment)
        )
        next_page = next_page_url

    # Generate previous page link
    if page > 1:
        prev_page_params = query_params.copy()
        prev_page_params['page'] = str(page - 1)
        prev_page_params['page_size'] = str(page_size)
        
        if sort_by is not None:
            prev_page_params['sort_by'] = sort_by
        
        if descending is not None:
            prev_page_params['descending'] = str(descending).lower()
            
        prev_page_query = urllib.parse.urlencode(prev_page_params)
        prev_page_url = urllib.parse.urlunparse(
            (parsed_url.scheme, parsed_url.netloc, parsed_url.path, 
             parsed_url.params, prev_page_query, parsed_url.fragment)
        )
        previous_page = prev_page_url

    return next_page, previous_page


def calculate_pagination(pagination: PaginationParams, total_items: int) -> Dict[str, int]:
    """
    Calculates pagination parameters from request.

    Args:
        pagination: PaginationParams object from request.
        total_items: Total number of items.

    Returns:
        Dict[str, int]: Dictionary with pagination parameters.
    """
    # Extract page and page_size
    page = pagination.page
    page_size = pagination.page_size
    
    # Ensure page is at least 1
    page = max(1, page)
    
    # Ensure page_size is between 1 and MAX_PAGE_SIZE
    page_size = max(1, min(int(MAX_PAGE_SIZE), page_size))
    
    # Calculate offset and limit for database queries
    offset = (page - 1) * page_size
    limit = page_size
    
    # Calculate total pages
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 1
    
    # Return calculated pagination parameters
    return {
        'page': page,
        'page_size': page_size,
        'offset': offset,
        'limit': limit,
        'total_pages': total_pages
    }


def apply_pagination(query: Any, offset: int, limit: int, sort_by: Optional[str] = None, descending: Optional[bool] = None) -> Any:
    """
    Applies pagination parameters to a database query.

    Args:
        query: Database query object (e.g., SQLAlchemy query).
        offset: Number of records to skip.
        limit: Maximum number of records to return.
        sort_by: Field to sort by (optional).
        descending: Whether to sort in descending order (optional).

    Returns:
        Any: Query with pagination applied.
    """
    # Apply sorting if specified
    if sort_by:
        try:
            if descending:
                query = query.order_by(f"{sort_by} DESC")
            else:
                query = query.order_by(f"{sort_by} ASC")
        except Exception:
            # If sorting fails, continue without sorting
            pass

    # Apply pagination
    try:
        query = query.offset(offset).limit(limit)
    except Exception:
        # If pagination fails, return the query as is
        pass
    
    return query


def get_pagination_params(request: Request) -> Dict[str, Any]:
    """
    Extracts and validates pagination parameters from request.

    Args:
        request: FastAPI request object.

    Returns:
        Dict[str, Any]: Dictionary with pagination parameters.
    """
    # Extract query parameters
    page_str = request.query_params.get('page', DEFAULT_PAGE)
    page_size_str = request.query_params.get('page_size', DEFAULT_PAGE_SIZE)
    sort_by = request.query_params.get('sort_by')
    descending_str = request.query_params.get('descending', 'false')
    
    # Convert and validate page
    try:
        page = int(page_str)
        if page < 1:
            page = int(DEFAULT_PAGE)
    except ValueError:
        page = int(DEFAULT_PAGE)
    
    # Convert and validate page_size
    try:
        page_size = int(page_size_str)
        if page_size < 1 or page_size > int(MAX_PAGE_SIZE):
            page_size = int(DEFAULT_PAGE_SIZE)
    except ValueError:
        page_size = int(DEFAULT_PAGE_SIZE)
    
    # Parse descending parameter
    descending = descending_str.lower() in ('true', 't', 'yes', 'y', '1')
    
    # Return validated pagination parameters
    return {
        'page': page,
        'page_size': page_size,
        'sort_by': sort_by,
        'descending': descending
    }