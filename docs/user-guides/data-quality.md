---
id: data-quality
title: Data Quality
---

Data quality is a critical aspect of any data pipeline. Without reliable data quality, downstream analytics and business decisions can be compromised. The self-healing data pipeline includes comprehensive data quality validation capabilities that automatically detect and, in many cases, correct data quality issues.

This guide will help you understand, configure, and monitor data quality in the self-healing pipeline. You'll learn how to define validation rules, interpret quality metrics, and leverage the self-healing capabilities to maintain high-quality data with minimal manual intervention.

## Introduction

### Purpose of This Guide

This guide will help you:

- Understand the data quality framework and its components
- Configure data quality validation rules for your datasets
- Monitor data quality metrics and understand quality scores
- Interpret validation results and address quality issues
- Leverage self-healing capabilities for automated issue resolution
- Implement best practices for data quality management

Whether you're a data engineer responsible for pipeline configuration, a data analyst concerned with data reliability, or a data steward overseeing data governance, this guide provides the information you need to ensure your data meets quality standards.

### Key Benefits of Data Quality Validation

The data quality validation framework provides several key benefits:

- **Early Issue Detection**: Identify data problems before they impact downstream systems
- **Comprehensive Validation**: Validate data across multiple dimensions and rule types
- **Automated Correction**: Leverage [self-healing](./self-healing.md) to automatically resolve common issues
- **Quality Metrics**: Track data quality trends and improvements over time
- **Confidence in Data**: Ensure business decisions are based on reliable information
- **Reduced Manual Effort**: Minimize the need for manual data cleansing and validation

By implementing robust data quality validation, you can significantly improve the reliability and trustworthiness of your data assets.

### Data Quality Framework Overview

The data quality framework in the self-healing pipeline is built on several key components:

1. **Validation Engine**: Core component that orchestrates validation execution
2. **Validation Rules**: Configurable rules that define quality expectations
3. **Quality Dimensions**: Categories for organizing and weighting quality aspects
4. **Validation Validators**: Specialized components for different validation types
5. **Quality Scoring**: System for quantifying overall data quality
6. **Self-Healing Integration**: Connection to automated correction capabilities

These components work together to provide a comprehensive data quality solution that not only detects issues but also helps resolve them automatically when possible.

## Understanding Data Quality Dimensions

Data quality is multifaceted and can be evaluated across several dimensions. The self-healing pipeline uses a dimensional approach to provide a comprehensive view of data quality.

### Quality Dimension Framework

The system evaluates data quality across six key dimensions:

1. **Completeness**: Are all required data elements present?
   - Measures the presence of values in required fields
   - Identifies missing or null values
   - Ensures all expected records are present

2. **Accuracy**: Does the data conform to expected patterns and values?
   - Validates data against known reference values
   - Checks for values within acceptable ranges
   - Verifies correct formats and patterns

3. **Consistency**: Is the data consistent across related elements?
   - Ensures consistent values across related fields
   - Validates cross-field dependencies
   - Checks for logical consistency in related data

4. **Validity**: Does the data conform to defined business rules?
   - Validates against business-specific rules
   - Ensures data meets domain-specific requirements
   - Checks for valid combinations of values

5. **Timeliness**: Is the data current and available when needed?
   - Verifies data is processed within expected timeframes
   - Checks for data freshness against requirements
   - Monitors processing delays and latency

6. **Uniqueness**: Are unique constraints maintained?
   - Ensures primary keys are unique
   - Checks for duplicate records
   - Validates uniqueness constraints

Each dimension contributes to the overall quality score, with configurable weights to reflect the relative importance of each dimension for your specific data assets.

### Dimension Weighting

The quality scoring system allows for custom weighting of dimensions to reflect their importance to your business:

```json
{
  "dimension_weights": {
    "COMPLETENESS": 0.25,
    "ACCURACY": 0.25,
    "CONSISTENCY": 0.2,
    "VALIDITY": 0.15,
    "TIMELINESS": 0.1,
    "UNIQUENESS": 0.05
  }
}
```

In this example configuration:
- Completeness and Accuracy are given the highest weight (25% each)
- Consistency follows at 20%
- Validity at 15%
- Timeliness at 10%
- Uniqueness at 5%

You can adjust these weights to match your specific data quality priorities. For example, if timeliness is critical for your use case, you might increase its weight relative to other dimensions.

### Quality Score Calculation

The system calculates quality scores using several methods:

1. **Simple Scoring**: Basic pass/fail ratio of validation rules
   ```
   Quality Score = (Passed Rules / Total Rules) * 100%
   ```

2. **Weighted Scoring**: Dimension-based weighting
   ```
   Quality Score = Σ (Dimension Score * Dimension Weight)
   ```
   Where Dimension Score is the pass rate for rules in that dimension

3. **Impact Scoring**: Business impact-based weighting
   ```
   Quality Score = Σ (Rule Result * Rule Impact Factor) / Σ (Impact Factors)
   ```
   Where Rule Result is 1 for pass, 0 for fail

4. **Adaptive Scoring**: Dynamically selects the most appropriate model

The default scoring model is Weighted Scoring, which balances simplicity with the ability to prioritize different quality dimensions.

## Data Quality Validation Types

The data quality framework supports multiple types of validation to address different aspects of data quality. Each validation type focuses on specific characteristics of the data.

### Schema Validation

Schema validation ensures that the structure of your data meets expectations:

- **Column Existence**: Verifies that all required columns are present
  ```json
  {
    "rule_type": "SCHEMA",
    "subtype": "column_existence",
    "columns": ["customer_id", "order_date", "product_id", "quantity", "price"],
    "dimension": "COMPLETENESS",
    "severity": "CRITICAL"
  }
  ```

- **Column Types**: Validates that columns have the expected data types
  ```json
  {
    "rule_type": "SCHEMA",
    "subtype": "column_type",
    "column_types": {
      "customer_id": "STRING",
      "order_date": "DATE",
      "product_id": "STRING",
      "quantity": "INTEGER",
      "price": "FLOAT"
    },
    "dimension": "VALIDITY",
    "severity": "HIGH"
  }
  ```

- **Schema Consistency**: Checks that the entire schema matches an expected structure
  ```json
  {
    "rule_type": "SCHEMA",
    "subtype": "schema_consistency",
    "expected_schema": {
      "columns": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "quantity", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "price", "type": "FLOAT", "mode": "REQUIRED"}
      ]
    },
    "dimension": "CONSISTENCY",
    "severity": "CRITICAL"
  }
  ```

- **Primary Key**: Validates that specified columns form a unique primary key
  ```json
  {
    "rule_type": "SCHEMA",
    "subtype": "primary_key",
    "key_columns": ["order_id"],
    "dimension": "UNIQUENESS",
    "severity": "CRITICAL"
  }
  ```

Schema validation is typically the first line of defense in data quality, as structural issues can prevent other validations from executing properly.

### Content Validation

Content validation focuses on the actual data values within your dataset:

- **Not Null**: Validates that specified columns do not contain null values
  ```json
  {
    "rule_type": "CONTENT",
    "subtype": "not_null",
    "columns": ["customer_id", "order_date", "product_id"],
    "dimension": "COMPLETENESS",
    "severity": "HIGH"
  }
  ```

- **Value Range**: Checks that values fall within expected ranges
  ```json
  {
    "rule_type": "CONTENT",
    "subtype": "value_range",
    "column": "quantity",
    "min_value": 1,
    "max_value": 1000,
    "dimension": "ACCURACY",
    "severity": "MEDIUM"
  }
  ```

- **Pattern Matching**: Validates that values match a regular expression pattern
  ```json
  {
    "rule_type": "CONTENT",
    "subtype": "pattern",
    "column": "email",
    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
    "dimension": "VALIDITY",
    "severity": "MEDIUM"
  }
  ```

- **Categorical Values**: Ensures values belong to a set of allowed values
  ```json
  {
    "rule_type": "CONTENT",
    "subtype": "categorical",
    "column": "status",
    "allowed_values": ["pending", "processing", "shipped", "delivered", "cancelled"],
    "dimension": "VALIDITY",
    "severity": "HIGH"
  }
  ```

- **Uniqueness**: Checks that values in specified columns are unique
  ```json
  {
    "rule_type": "CONTENT",
    "subtype": "uniqueness",
    "columns": ["transaction_id"],
    "dimension": "UNIQUENESS",
    "severity": "HIGH"
  }
  ```

Content validation ensures that the data values themselves meet quality expectations, even if the structure is correct.

### Relationship Validation

Relationship validation examines how data relates to other datasets:

- **Referential Integrity**: Validates that values exist in a reference table
  ```json
  {
    "rule_type": "RELATIONSHIP",
    "subtype": "referential_integrity",
    "column": "product_id",
    "ref_dataset_id": "product_catalog",
    "ref_table_id": "products",
    "ref_column": "product_id",
    "dimension": "CONSISTENCY",
    "severity": "HIGH"
  }
  ```

- **Cardinality**: Checks the cardinality relationship between tables
  ```json
  {
    "rule_type": "RELATIONSHIP",
    "subtype": "cardinality",
    "column": "customer_id",
    "ref_dataset_id": "customers",
    "ref_table_id": "customer_profile",
    "ref_column": "customer_id",
    "relationship_type": "many-to-one",
    "dimension": "CONSISTENCY",
    "severity": "MEDIUM"
  }
  ```

- **Hierarchical Relationship**: Validates hierarchical relationships within a dataset
  ```json
  {
    "rule_type": "RELATIONSHIP",
    "subtype": "hierarchical",
    "id_column": "department_id",
    "parent_column": "parent_department_id",
    "dimension": "CONSISTENCY",
    "severity": "MEDIUM"
  }
  ```

Relationship validation ensures data consistency across multiple tables and datasets, which is essential for maintaining data integrity in a data warehouse environment.

### Statistical Validation

Statistical validation applies statistical methods to identify anomalous data points.

- **Outlier Detection**: Identifies values that are statistical outliers
  ```json
  {
    "rule_type": "STATISTICAL",
    "subtype": "outliers",
    "column": "order_amount",
    "threshold": 3.0,
    "method": "z_score",
    "dimension": "ACCURACY",
    "severity": "MEDIUM"
  }
  ```

- **Distribution Validation**: Checks if data follows an expected distribution
  ```json
  {
    "rule_type": "STATISTICAL",
    "subtype": "distribution",
    "column": "transaction_amount",
    "distribution": "normal",
    "parameters": {
      "p_value": 0.05
    },
    "dimension": "ACCURACY",
    "severity": "LOW"
  }
  ```

- **Correlation Analysis**: Validates correlation between columns
  ```json
  {
    "rule_type": "STATISTICAL",
    "subtype": "correlation",
    "column1": "price",
    "column2": "discount",
    "min_correlation": -0.8,
    "max_correlation": -0.2,
    "dimension": "CONSISTENCY",
    "severity": "LOW"
  }
  ```

- **Trend Analysis**: Validates time series trends
  ```json
  {
    "rule_type": "STATISTICAL",
    "subtype": "trend",
    "time_column": "date",
    "value_column": "daily_sales",
    "trend_type": "increasing",
    "parameters": {
      "min_slope": 0.01
    },
    "dimension": "ACCURACY",
    "severity": "LOW"
  }
  ```

Statistical validation helps identify subtle data issues that aren't simple rule violations.

## Configuring Data Quality Rules

Data quality rules define the expectations for your data. This section explains how to create, manage, and organize rules effectively.

### Rule Structure

Each data quality rule has a standard structure with the following components:

```json
{
  "rule_id": "unique_rule_identifier",  // Optional, generated if not provided
  "rule_type": "CONTENT",              // SCHEMA, CONTENT, RELATIONSHIP, STATISTICAL
  "subtype": "not_null",               // Specific validation type within the rule type
  "name": "Required Fields Check",     // Human-readable name
  "description": "Validates that required fields are not null", // Description
  "dimension": "COMPLETENESS",         // Quality dimension this rule addresses
  "severity": "HIGH",                 // CRITICAL, HIGH, MEDIUM, LOW
  "enabled": true,                    // Whether the rule is active
  "parameters": {                     // Rule-specific parameters
    // Varies based on rule_type and subtype
  },
  "metadata": {                       // Optional metadata
    "owner": "data_quality_team",
    "created_date": "2023-06-15",
    "tags": ["core", "customer_data"]
  }
}
```

The specific parameters vary based on the rule type and subtype, as shown in the previous section on validation types.

### Creating Rules in the UI

To create data quality rules using the user interface:

1. Navigate to the **Data Quality** section in the main menu
2. Select the **Rules** tab
3. Click **[+ Create Rule]** to open the rule creation form
4. Fill in the rule details:
   - Select the rule type and subtype
   - Provide a name and description
   - Select the quality dimension
   - Set the severity level
   - Configure the rule-specific parameters
5. Click **[Save Rule]** to create the rule

The UI provides a user-friendly way to create rules with form validation and parameter suggestions based on the selected rule type.

### Bulk Rule Management

For managing multiple rules efficiently, the system supports bulk operations:

**Importing Rules**
1. Navigate to the **Data Quality** section
2. Select the **Rules** tab
3. Click **[Import Rules]**
4. Upload a JSON file containing rule definitions
5. Review the rules to be imported
6. Click **[Confirm Import]**

**Exporting Rules**
1. Navigate to the **Data Quality** section
2. Select the **Rules** tab
3. Select the rules you want to export (or all rules)
4. Click **[Export Rules]**
5. Choose the export format (JSON or YAML)
6. Save the exported file

**Rule Templates**

The system also provides rule templates for common validation scenarios:
1. Navigate to the **Data Quality** section
2. Select the **Rules** tab
3. Click **[Rule Templates]**
4. Select a template category
5. Choose a specific template
6. Customize the template parameters
7. Save as a new rule

Templates are available for common validation scenarios like email validation, date format checking, numeric range validation, and more.

### Rule Organization and Management

Organizing rules effectively is important for maintainability:

**Rule Tagging**
Use the metadata.tags field to categorize rules:
```json
"metadata": {
  "tags": ["customer_data", "financial", "regulatory"]
}
```

**Rule Grouping**
Rules can be organized into logical groups in the UI:
1. Navigate to the **Data Quality** section
2. Select the **Rule Groups** tab
3. Create groups based on data domain, business function, or other criteria
4. Assign rules to appropriate groups

**Rule Versioning**
The system maintains a history of rule changes:
1. Navigate to the **Data Quality** section
2. Select the **Rules** tab
3. Click on a specific rule
4. Select the **History** tab to view changes
5. Use **[Restore Version]** to revert to a previous version if needed

**Rule Dependencies**
Some rules may depend on others. For example, a content validation rule might only be relevant if a schema validation rule passes. You can define these dependencies in the rule configuration:
```json
"dependencies": [
  "schema_validation_rule_id"
]
```
Rules with dependencies will only be executed if all dependency rules pass.

### Rule Severity Levels

Rule severity determines how validation failures impact the overall quality score and trigger [self-healing](./self-healing.md) actions:

- **CRITICAL**: Severe issues that make the data unusable. Failed critical rules will cause the entire validation to fail and may block downstream processing. Self-healing is always attempted for critical issues.

- **HIGH**: Important issues that significantly impact data quality. Failed high-severity rules have a major impact on the quality score and are prioritized for self-healing.

- **MEDIUM**: Moderate issues that affect data quality but may not prevent usage. Failed medium-severity rules have a moderate impact on the quality score and may be addressed by self-healing if resources are available.

- **LOW**: Minor issues or informational checks. Failed low-severity rules have minimal impact on the quality score and are typically not addressed by automatic self-healing.

When configuring rules, assign severity levels based on the business impact of potential failures. Reserve CRITICAL for truly blocking issues that should prevent data from being used.

## Executing Data Quality Validation

Data quality validation can be executed in various ways depending on your workflow and requirements.

### Validation in Data Pipelines

The most common way to execute data quality validation is as part of a data pipeline:

1. **Automated Pipeline Integration**:
   - Data quality validation is automatically integrated into data pipelines created through the system
   - Validation occurs after data is extracted and before it is transformed and loaded
   - Results determine whether the pipeline continues or requires intervention

2. **Cloud Composer DAGs**:\
   - Validation is implemented as tasks in Cloud Composer (Apache Airflow) DAGs
   - The `quality_validation_dag.py` contains the core validation logic
   - Task dependencies ensure validation occurs at the right point in the workflow

3. **Validation Configuration**:\
   - Each pipeline can have specific validation settings:\
     - Quality threshold for pass/fail determination\
     - Rule selection and filtering\
     - Self-healing behavior for failed validations\
     - Notification preferences for quality issues

4. **Execution Modes**:\
   - **Standard Mode**: Validation runs with all configured rules\
   - **Fast Mode**: Only critical rules are executed for quick validation\
   - **Deep Mode**: Additional statistical and pattern analysis is performed

The validation results are stored and can be viewed in the Data Quality dashboard.

### On-Demand Validation

You can also run validation on demand outside of regular pipeline execution:

1. **UI-Triggered Validation**:\
   - Navigate to the **Data Quality** section\
   - Select the **Datasets** tab\
   - Choose the dataset you want to validate\
   - Click **[Validate Now]**\
   - Select the validation rules to apply\
   - Click **[Run Validation]**

2. **API-Triggered Validation**:\
   - Use the REST API to trigger validation programmatically\
   - Example API call:\
   ```\
   POST /api/v1/quality/validate\
   {\
     "dataset_id": "my_project.my_dataset.my_table",\
     "rules": ["rule_id_1", "rule_id_2"],  // Optional, all applicable rules if omitted\
     "execution_mode": "STANDARD",\
     "notification": true\
   }\
   ```

3. **Scheduled Validation**:\
   - Set up scheduled validation jobs independent of data pipelines\
   - Navigate to the **Data Quality** section\
   - Select the **Schedules** tab\
   - Click **[+ Create Schedule]**\
   - Configure the schedule details and rule selection\
   - Save the schedule

On-demand validation is useful for ad-hoc quality checks, investigating issues, or validating data that wasn't processed through the standard pipelines.

### Validation Execution Modes

The validation engine supports different execution modes to balance performance and thoroughness:

1. **In-Memory Mode**:\
   - Used for small to medium-sized datasets\
   - Data is loaded into memory for validation\
   - Fastest execution but limited by available memory\
   - Suitable for datasets under 1GB

2. **BigQuery Mode**:\
   - Used for large datasets stored in BigQuery\
   - Validation is performed using BigQuery SQL\
   - Scales to very large datasets\
   - More efficient for complex validations on large data

3. **Hybrid Mode**:\
   - Combines in-memory and BigQuery approaches\
   - Simple validations run in BigQuery\
   - Complex validations use sampling and in-memory processing\
   - Balances performance and thoroughness

The system automatically selects the appropriate mode based on dataset size and validation complexity, but you can override this selection in the configuration if needed.

### Validation Performance Optimization

For large datasets or complex validation rules, consider these performance optimization strategies:

1. **Rule Prioritization**:\
   - Configure rule execution order to run critical rules first\
   - Use dependencies to skip unnecessary validations\
   - Group rules by execution mode for efficiency

2. **Sampling**:\
   - For statistical validations, use sampling to reduce data volume\
   - Configure sampling rate based on dataset size and required accuracy\
   - Example configuration:\
   ```json\
   "validation_config": {\
     "sampling": {\
       "enabled": true,\
       "method": "random",\
       "rate": 0.1,  // 10% sample\
       "min_records": 1000,\
       "max_records": 100000\
     }\
   }\
   ```

3. **Parallel Execution**:\
   - Enable parallel rule execution where possible\
   - Configure the maximum concurrency based on available resources\
   - Example configuration:\
   ```json\
   "validation_config": {\
     "parallel_execution": {\
       "enabled": true,\
       "max_concurrent_rules": 5\
     }\
   }\