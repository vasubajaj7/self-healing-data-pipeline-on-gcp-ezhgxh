/**
 * Global TypeScript type declarations for the self-healing data pipeline web application.
 * This file defines common types, interfaces, and enums that are used across multiple components of the application.
 */

// Type Aliases
export type ID = string;
export type Timestamp = string;
export type ISO8601Date = string;
export type JSONValue = string | number | boolean | null | JSONObject | JSONValue[];
export type JSONObject = { [key: string]: JSONValue };
export type Nullable<T> = T | null;
export type Optional<T> = T | undefined;
export type AsyncState<T> = {
  data: T | null;
  loading: boolean;
  error: ApiError | null;
};
export type SortDirection = 'asc' | 'desc';
export type FilterOperator = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'startsWith' | 'endsWith';

// Interfaces
export interface Pagination {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
}

export interface DateRange {
  startDate: ISO8601Date;
  endDate: ISO8601Date;
}

export interface ApiError {
  statusCode: number;
  message: string;
  errorCode: string;
  details?: Record<string, any>;
}

// Enums
export enum PipelineStatus {
  HEALTHY = 'HEALTHY',
  WARNING = 'WARNING',
  ERROR = 'ERROR',
  INACTIVE = 'INACTIVE'
}

export enum AlertSeverity {
  CRITICAL = 'CRITICAL',
  HIGH = 'HIGH',
  MEDIUM = 'MEDIUM',
  LOW = 'LOW'
}

export enum QualityStatus {
  PASSED = 'PASSED',
  FAILED = 'FAILED',
  WARNING = 'WARNING'
}

export enum HealingStatus {
  PENDING = 'PENDING',
  IN_PROGRESS = 'IN_PROGRESS',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  APPROVAL_REQUIRED = 'APPROVAL_REQUIRED'
}

export enum ThemeMode {
  LIGHT = 'LIGHT',
  DARK = 'DARK',
  SYSTEM = 'SYSTEM'
}