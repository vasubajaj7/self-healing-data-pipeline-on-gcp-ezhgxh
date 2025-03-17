import React, { useEffect, useMemo } from 'react';
import {
  Box,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Typography,
  Divider,
  Chip,
  Tooltip
} from '@mui/material';
import {
  ErrorOutline,
  Warning,
  ArrowForward
} from '@mui/icons-material';
import Card from '../common/Card';
import { useQuality } from '../../contexts/QualityContext';
import { QualityRule } from '../../types/api';
import { QualityRuleType } from '../../types/quality';
import { colors } from '../../theme/colors';

// Interface for the component props
interface FailingRulesCardProps {
  className?: string;
  title?: string;
  dataset?: string;
  table?: string;
  maxItems?: number;
  minHeight?: number | string;
  onRuleSelect?: (rule: QualityRule) => void;
}

// Internal interface for failing rule data with additional metadata
interface FailingRule {
  rule: QualityRule;
  failureCount: number;
  lastFailedAt: string;
  affectedRows: number;
}

/**
 * Formats the rule type enum value into a readable label
 */
const formatRuleType = (ruleType: string): string => {
  return ruleType
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (l) => l.toUpperCase());
};

/**
 * Gets the color for a specific severity level
 */
const getSeverityColor = (severity: string): string => {
  switch (severity.toUpperCase()) {
    case 'CRITICAL':
    case 'HIGH':
      return colors.error.main;
    case 'MEDIUM':
      return colors.warning.main;
    default:
      return colors.info.main;
  }
};

/**
 * A card component that displays a list of failing validation rules in the data quality dashboard.
 * It helps users quickly identify problematic rules that are causing quality issues.
 */
const FailingRulesCard: React.FC<FailingRulesCardProps> = ({
  className,
  title = 'Top Failing Rules',
  dataset,
  table,
  maxItems = 5,
  minHeight,
  onRuleSelect
}) => {
  // Get quality data from context
  const { 
    rules, 
    issues, 
    statistics,
    loading, 
    fetchRules,
    fetchIssues,
    fetchStatistics
  } = useQuality();

  // Fetch data when dataset or table changes
  useEffect(() => {
    if (dataset || table) {
      fetchRules();
      fetchIssues();
      fetchStatistics();
    }
  }, [dataset, table, fetchRules, fetchIssues, fetchStatistics]);

  // Process rules to get failing rules with metadata
  const failingRules = useMemo(() => {
    if (!rules || !Array.isArray(rules) || rules.length === 0) return [];
    
    // Create maps to track rule failure metrics
    const ruleCounts = new Map<string, number>();
    const ruleAffectedRows = new Map<string, number>();
    const ruleLastFailed = new Map<string, string>();
    
    // Try to build failure data from issues array
    if (issues && Array.isArray(issues)) {
      issues.forEach(issue => {
        if (issue.ruleId) {
          // Count issues by rule
          ruleCounts.set(
            issue.ruleId, 
            (ruleCounts.get(issue.ruleId) || 0) + 1
          );
          
          // Track affected rows
          if (issue.affectedRows) {
            ruleAffectedRows.set(
              issue.ruleId,
              (ruleAffectedRows.get(issue.ruleId) || 0) + issue.affectedRows
            );
          }
          
          // Track most recent failure
          const currentLastFailed = ruleLastFailed.get(issue.ruleId) || '';
          if (!currentLastFailed || issue.detectedAt > currentLastFailed) {
            ruleLastFailed.set(issue.ruleId, issue.detectedAt);
          }
        }
      });
    }
    
    // Try to populate from statistics if available and issues were not found
    if (statistics && ruleCounts.size === 0) {
      if (statistics.issuesByRuleId) {
        Object.entries(statistics.issuesByRuleId).forEach(([ruleId, count]) => {
          ruleCounts.set(ruleId, count);
        });
      }
      
      if (statistics.affectedRowsByRuleId) {
        Object.entries(statistics.affectedRowsByRuleId).forEach(([ruleId, count]) => {
          ruleAffectedRows.set(ruleId, count);
        });
      }
    }
    
    // Filter and map rules that have issues
    return rules
      .filter(rule => ruleCounts.has(rule.ruleId) && ruleCounts.get(rule.ruleId)! > 0)
      .map(rule => ({
        rule,
        failureCount: ruleCounts.get(rule.ruleId) || 0,
        affectedRows: ruleAffectedRows.get(rule.ruleId) || 0,
        lastFailedAt: ruleLastFailed.get(rule.ruleId) || ''
      }))
      // Sort by severity first, then by issue count
      .sort((a, b) => {
        // Define severity order
        const severityOrder: Record<string, number> = {
          'CRITICAL': 0,
          'HIGH': 1,
          'MEDIUM': 2,
          'LOW': 3
        };
        
        // Compare by severity
        const severityDiff = 
          (severityOrder[a.rule.severity.toUpperCase()] || 99) - 
          (severityOrder[b.rule.severity.toUpperCase()] || 99);
        
        // If severity is the same, compare by failure count
        if (severityDiff === 0) {
          return b.failureCount - a.failureCount;
        }
        
        return severityDiff;
      });
  }, [rules, issues, statistics]);

  // Limit the number of rules to display
  const displayRules = useMemo(() => {
    return failingRules.slice(0, maxItems);
  }, [failingRules, maxItems]);

  return (
    <Card
      title={title}
      loading={loading}
      className={className}
      minHeight={minHeight}
    >
      {!loading && displayRules.length === 0 ? (
        <Box sx={{ py: 2, textAlign: 'center' }}>
          <Typography variant="body1" color="text.secondary">
            No failing rules found.
          </Typography>
        </Box>
      ) : (
        <List sx={{ width: '100%', p: 0 }}>
          {displayRules.map((item) => (
            <React.Fragment key={item.rule.ruleId}>
              <ListItem
                alignItems="flex-start"
                sx={{
                  cursor: onRuleSelect ? 'pointer' : 'default',
                  '&:hover': onRuleSelect ? {
                    backgroundColor: 'rgba(0, 0, 0, 0.04)'
                  } : {}
                }}
                onClick={() => onRuleSelect && onRuleSelect(item.rule)}
                role={onRuleSelect ? "button" : undefined}
                aria-label={onRuleSelect ? `View details for ${item.rule.ruleName}` : undefined}
              >
                <ListItemIcon sx={{ minWidth: 36, mt: 0.5 }}>
                  <Tooltip title={item.rule.severity}>
                    {['CRITICAL', 'HIGH'].includes(item.rule.severity.toUpperCase()) ? (
                      <ErrorOutline sx={{ color: colors.error.main }} />
                    ) : (
                      <Warning sx={{ color: colors.warning.main }} />
                    )}
                  </Tooltip>
                </ListItemIcon>
                <ListItemText
                  primary={
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <Typography variant="subtitle2" component="span">
                        {item.rule.ruleName}
                      </Typography>
                      <Chip
                        size="small"
                        label={`${item.failureCount} failures`}
                        sx={{
                          bgcolor: 'rgba(211, 47, 47, 0.1)',
                          color: colors.error.main,
                          fontWeight: 500,
                          fontSize: '0.7rem'
                        }}
                      />
                    </Box>
                  }
                  secondary={
                    <Box component="span">
                      <Typography 
                        variant="body2" 
                        component="span" 
                        color="text.primary"
                        sx={{ display: 'inline' }}
                      >
                        {formatRuleType(item.rule.ruleType)}
                      </Typography>
                      {item.affectedRows > 0 && (
                        <Typography 
                          variant="body2" 
                          component="span" 
                          color="text.secondary"
                          sx={{ display: 'inline' }}
                        >
                          {` - ${item.affectedRows} rows affected`}
                        </Typography>
                      )}
                    </Box>
                  }
                />
              </ListItem>
              {displayRules.indexOf(item) < displayRules.length - 1 && <Divider component="li" />}
            </React.Fragment>
          ))}
          {failingRules.length > maxItems && (
            <>
              <Divider />
              <ListItem
                sx={{
                  justifyContent: 'center',
                  color: 'primary.main',
                  cursor: 'pointer',
                  '&:hover': { backgroundColor: 'rgba(25, 118, 210, 0.04)' }
                }}
                onClick={() => onRuleSelect && failingRules.length > maxItems && onRuleSelect(failingRules[0].rule)}
                role="button"
                aria-label="View all failing rules"
              >
                <Typography variant="body2" color="primary" sx={{ display: 'flex', alignItems: 'center' }}>
                  View all failing rules
                  <ArrowForward fontSize="small" sx={{ ml: 1 }} />
                </Typography>
              </ListItem>
            </>
          )}
        </List>
      )}
    </Card>
  );
};

export default FailingRulesCard;