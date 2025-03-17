/**
 * TypeScript type definitions for the performance optimization features of the self-healing data pipeline.
 * This file defines interfaces, types, and enums related to BigQuery query optimization, schema optimization,
 * and resource optimization.
 */

import {
  ID,
  Timestamp,
  ISO8601Date,
  JSONObject,
  Nullable,
  Optional
} from './global';

import {
  PaginationParams,
  DateRangeParams
} from './api';

/**
 * Enum for different types of optimization
 */
export enum OptimizationType {
  QUERY = 'QUERY',
  SCHEMA = 'SCHEMA',
  RESOURCE = 'RESOURCE'
}

/**
 * Enum for the status of optimization recommendations
 */
export enum OptimizationStatus {
  RECOMMENDED = 'RECOMMENDED',
  APPLIED = 'APPLIED',
  REJECTED = 'REJECTED',
  PENDING = 'PENDING'
}

/**
 * Enum for the impact level of optimization recommendations
 */
export enum ImpactLevel {
  HIGH = 'HIGH',
  MEDIUM = 'MEDIUM',
  LOW = 'LOW'
}

/**
 * Enum for specific types of query optimizations
 */
export enum QueryOptimizationType {
  PREDICATE_PUSHDOWN = 'PREDICATE_PUSHDOWN',
  JOIN_REORDERING = 'JOIN_REORDERING',
  MATERIALIZED_VIEW = 'MATERIALIZED_VIEW',
  QUERY_REWRITE = 'QUERY_REWRITE',
  COLUMN_PRUNING = 'COLUMN_PRUNING',
  QUERY_CACHING = 'QUERY_CACHING'
}

/**
 * Enum for specific types of schema optimizations
 */
export enum SchemaOptimizationType {
  PARTITIONING = 'PARTITIONING',
  CLUSTERING = 'CLUSTERING',
  COLUMN_REORDERING = 'COLUMN_REORDERING',
  DATA_TYPE_OPTIMIZATION = 'DATA_TYPE_OPTIMIZATION',
  DENORMALIZATION = 'DENORMALIZATION'
}

/**
 * Enum for specific types of resource optimizations
 */
export enum ResourceOptimizationType {
  SLOT_ALLOCATION = 'SLOT_ALLOCATION',
  RESERVATION_SIZING = 'RESERVATION_SIZING',
  STORAGE_OPTIMIZATION = 'STORAGE_OPTIMIZATION',
  COST_CONTROL = 'COST_CONTROL',
  WORKLOAD_SCHEDULING = 'WORKLOAD_SCHEDULING'
}

/**
 * Interface for query performance metrics
 */
export interface QueryPerformanceMetrics {
  queryId: ID;
  queryText: string;
  executionTime: number;
  processingTime: number;
  bytesProcessed: number;
  slotMilliseconds: number;
  estimatedCost: number;
  executionDateTime: Timestamp;
  user: string;
  project: string;
  hasOptimizationRecommendations: boolean;
}

/**
 * Interface for detailed query information
 */
export interface QueryDetails {
  queryId: ID;
  queryText: string;
  executionPlan: JSONObject;
  executionStats: JSONObject;
  executionTime: number;
  bytesProcessed: number;
  slotMilliseconds: number;
  estimatedCost: number;
  executionDateTime: Timestamp;
  user: string;
  project: string;
  referencedTables: Array<{ datasetId: string; tableId: string }>;
  optimizationRecommendations: Optional<QueryOptimizationRecommendation[]>;
}

/**
 * Interface for query optimization recommendations
 */
export interface QueryOptimizationRecommendation {
  recommendationId: ID;
  queryId: Optional<ID>;
  optimizationType: QueryOptimizationType;
  status: OptimizationStatus;
  description: string;
  impactLevel: ImpactLevel;
  originalQuery: string;
  optimizedQuery: string;
  estimatedImprovementPercentage: number;
  estimatedCostReduction: number;
  createdAt: Timestamp;
  implementedAt: Optional<Timestamp>;
  rejectedAt: Optional<Timestamp>;
  rejectionReason: Optional<string>;
  details: JSONObject;
}

/**
 * Interface for BigQuery table details
 */
export interface TableDetails {
  datasetId: string;
  tableId: string;
  description: Optional<string>;
  creationTime: Timestamp;
  lastModifiedTime: Timestamp;
  sizeBytes: number;
  numRows: number;
  schema: JSONObject;
  partitioning: Optional<JSONObject>;
  clustering: Optional<JSONObject>;
  queryFrequency: number;
  averageQueryCost: number;
  hasOptimizationRecommendations: boolean;
}

/**
 * Interface for schema optimization recommendations
 */
export interface SchemaOptimizationRecommendation {
  recommendationId: ID;
  datasetId: string;
  tableId: string;
  optimizationType: SchemaOptimizationType;
  status: OptimizationStatus;
  description: string;
  impactLevel: ImpactLevel;
  currentSchema: JSONObject;
  recommendedSchema: JSONObject;
  estimatedImprovementPercentage: number;
  estimatedCostReduction: number;
  implementationScript: string;
  createdAt: Timestamp;
  implementedAt: Optional<Timestamp>;
  rejectedAt: Optional<Timestamp>;
  rejectionReason: Optional<string>;
  details: JSONObject;
}

/**
 * Interface for resource utilization metrics
 */
export interface ResourceUtilizationMetrics {
  metricId: ID;
  resourceType: string;
  resourceName: string;
  utilizationPercentage: number;
  timestamp: Timestamp;
  period: string;
  cost: number;
  details: JSONObject;
}

/**
 * Interface for resource optimization recommendations
 */
export interface ResourceOptimizationRecommendation {
  recommendationId: ID;
  resourceType: string;
  resourceName: string;
  optimizationType: ResourceOptimizationType;
  status: OptimizationStatus;
  description: string;
  impactLevel: ImpactLevel;
  currentConfiguration: JSONObject;
  recommendedConfiguration: JSONObject;
  estimatedCostReduction: number;
  implementationSteps: string;
  createdAt: Timestamp;
  implementedAt: Optional<Timestamp>;
  rejectedAt: Optional<Timestamp>;
  rejectionReason: Optional<string>;
  details: JSONObject;
}

/**
 * Interface for cost analysis data
 */
export interface CostAnalysisData {
  timePeriod: string;
  startDate: ISO8601Date;
  endDate: ISO8601Date;
  totalCost: number;
  previousPeriodCost: number;
  costChangePercentage: number;
  costBreakdown: Record<string, number>;
  costTrend: Array<{ date: ISO8601Date; cost: number }>;
  potentialSavings: number;
  savingsOpportunities: Array<{ category: string; amount: number; description: string }>;
}

/**
 * Interface for optimization summary statistics
 */
export interface OptimizationSummary {
  total_recommendations: number;
  applied_recommendations: number;
  pending_recommendations: number;
  rejected_recommendations: number;
  total_savings: number;
  potential_savings: number;
  performance_improvement: number;
  recommendations_by_type: Record<OptimizationType, number>;
  recommendations_by_impact: Record<ImpactLevel, number>;
  savings_by_type: Record<OptimizationType, number>;
  last_updated: Timestamp;
}

/**
 * Interface for query optimization request parameters
 */
export interface QueryOptimizationParams {
  queryId: Optional<ID>;
  optimizationType: Optional<QueryOptimizationType>;
  status: Optional<OptimizationStatus>;
  impactLevel: Optional<ImpactLevel>;
  minImprovementPercentage: Optional<number>;
  startDate: Optional<ISO8601Date>;
  endDate: Optional<ISO8601Date>;
}

/**
 * Interface for schema optimization request parameters
 */
export interface SchemaOptimizationParams {
  datasetId: Optional<string>;
  tableId: Optional<string>;
  optimizationType: Optional<SchemaOptimizationType>;
  status: Optional<OptimizationStatus>;
  impactLevel: Optional<ImpactLevel>;
  minImprovementPercentage: Optional<number>;
  startDate: Optional<ISO8601Date>;
  endDate: Optional<ISO8601Date>;
}

/**
 * Interface for resource optimization request parameters
 */
export interface ResourceOptimizationParams {
  resourceType: Optional<string>;
  resourceName: Optional<string>;
  optimizationType: Optional<ResourceOptimizationType>;
  status: Optional<OptimizationStatus>;
  impactLevel: Optional<ImpactLevel>;
  minCostReduction: Optional<number>;
  startDate: Optional<ISO8601Date>;
  endDate: Optional<ISO8601Date>;
}

/**
 * Interface for applying an optimization recommendation
 */
export interface ApplyOptimizationRequest {
  recommendationId: ID;
  optimizationType: OptimizationType;
  comments: Optional<string>;
  customParameters: Optional<JSONObject>;
}

/**
 * Interface for rejecting an optimization recommendation
 */
export interface RejectOptimizationRequest {
  recommendationId: ID;
  optimizationType: OptimizationType;
  rejectionReason: string;
  additionalComments: Optional<string>;
}