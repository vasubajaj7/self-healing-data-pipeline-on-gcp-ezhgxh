import { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { useLocalStorage } from './useLocalStorage';
import { PaginationParams } from '../types/api';
import { PAGINATION } from '../utils/constants';

/**
 * Extended interface for pagination parameters including total pages
 */
interface UsePaginationParams extends PaginationParams {
  totalPages: number;
}

/**
 * Return type for the usePagination hook
 */
interface UsePaginationReturn {
  pagination: UsePaginationParams;
  setPage: (page: number) => void;
  setPageSize: (pageSize: number) => void;
  nextPage: () => void;
  prevPage: () => void;
  firstPage: () => void;
  lastPage: () => void;
  sort: (column: string) => void;
}

/**
 * A hook that provides pagination state and controls for data tables and lists
 * 
 * This hook manages pagination state with localStorage persistence, allowing users
 * to maintain their pagination preferences between sessions. It provides methods for
 * page navigation and sorting, and integrates with paginated API endpoints.
 * 
 * @param storageKey - Key for storing pagination preferences in localStorage
 * @param totalItems - Total number of items to paginate
 * @param initialParams - Optional initial pagination parameters
 * @returns An object containing pagination state and control functions
 * 
 * @example
 * // Basic usage
 * const { 
 *   pagination, 
 *   setPage, 
 *   nextPage, 
 *   prevPage 
 * } = usePagination('pipeline_list', totalCount);
 * 
 * @example
 * // With initial parameters
 * const { 
 *   pagination, 
 *   setPageSize, 
 *   sort 
 * } = usePagination('quality_rules', totalRules, { 
 *   pageSize: 50,
 *   sortBy: 'ruleName' 
 * });
 */
export function usePagination(
  storageKey: string,
  totalItems: number,
  initialParams?: Partial<PaginationParams>
): UsePaginationReturn {
  // Initialize pagination from localStorage or defaults
  const [storedPagination, setStoredPagination] = useLocalStorage<PaginationParams>(
    `pagination_${storageKey}`,
    {
      page: initialParams?.page || PAGINATION.DEFAULT_PAGE,
      pageSize: initialParams?.pageSize || PAGINATION.DEFAULT_PAGE_SIZE,
      sortBy: initialParams?.sortBy || undefined,
      descending: initialParams?.descending || false
    }
  );

  // Create pagination state with calculated total pages
  const [pagination, setPagination] = useState<UsePaginationParams>(() => {
    const totalPages = Math.max(1, Math.ceil(totalItems / storedPagination.pageSize));
    
    return {
      ...storedPagination,
      totalPages
    };
  });

  // Update pagination when totalItems changes
  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(totalItems / pagination.pageSize));
    
    // If current page is greater than total pages, adjust to last page
    const page = Math.min(pagination.page, totalPages);
    
    setPagination(prev => ({
      ...prev,
      page,
      totalPages
    }));
  }, [totalItems, pagination.pageSize]);

  // Function to change the current page
  const setPage = useCallback((page: number) => {
    const validPage = Math.min(Math.max(1, page), pagination.totalPages);
    setPagination(prev => ({
      ...prev,
      page: validPage
    }));
    setStoredPagination(prev => ({
      ...prev,
      page: validPage
    }));
  }, [pagination.totalPages, setStoredPagination]);

  // Function to change the page size
  const setPageSize = useCallback((pageSize: number) => {
    const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
    
    // Calculate the first item of the current page
    const firstItemIndex = (pagination.page - 1) * pagination.pageSize;
    
    // Calculate what page that item would be on with the new page size
    const newPage = Math.floor(firstItemIndex / pageSize) + 1;
    
    // Ensure the new page is valid
    const page = Math.min(Math.max(1, newPage), totalPages);
    
    setPagination(prev => ({
      ...prev,
      pageSize,
      page,
      totalPages
    }));
    
    setStoredPagination(prev => ({
      ...prev,
      pageSize,
      page
    }));
  }, [totalItems, pagination.page, pagination.pageSize, setStoredPagination]);

  // Function to go to the next page
  const nextPage = useCallback(() => {
    if (pagination.page < pagination.totalPages) {
      setPage(pagination.page + 1);
    }
  }, [pagination.page, pagination.totalPages, setPage]);

  // Function to go to the previous page
  const prevPage = useCallback(() => {
    if (pagination.page > 1) {
      setPage(pagination.page - 1);
    }
  }, [pagination.page, setPage]);

  // Function to go to the first page
  const firstPage = useCallback(() => {
    setPage(1);
  }, [setPage]);

  // Function to go to the last page
  const lastPage = useCallback(() => {
    setPage(pagination.totalPages);
  }, [pagination.totalPages, setPage]);

  // Function to sort by a column
  const sort = useCallback((column: string) => {
    setPagination(prev => {
      const descending = prev.sortBy === column ? !prev.descending : false;
      return {
        ...prev,
        sortBy: column,
        descending
      };
    });
    
    setStoredPagination(prev => {
      const descending = prev.sortBy === column ? !prev.descending : false;
      return {
        ...prev,
        sortBy: column,
        descending
      };
    });
  }, [setStoredPagination]);

  return {
    pagination,
    setPage,
    setPageSize,
    nextPage,
    prevPage,
    firstPage,
    lastPage,
    sort
  };
}