---
id: getting-started
title: Getting Started
---

Welcome to the Self-Healing Data Pipeline for BigQuery! This guide will help you get started with our end-to-end solution designed to minimize manual intervention through intelligent monitoring and autonomous correction.

The self-healing data pipeline addresses common challenges faced by data teams:

- Frequent pipeline failures requiring manual intervention
- Time-consuming error resolution processes
- Data quality issues affecting downstream analytics
- Reactive rather than proactive management
- High operational overhead for maintenance

By combining Google Cloud services with AI-driven automation, our system significantly reduces manual effort, improves data reliability, minimizes pipeline downtime, and enables faster, more reliable business insights.

## Purpose of This Guide

This getting started guide will help you:

- Understand the key components and capabilities of the self-healing data pipeline
- Navigate the user interface and access essential features
- Configure your first data pipeline with self-healing capabilities
- Monitor pipeline health and understand alerts
- Respond to issues that require manual intervention
- Find additional resources for deeper learning

Whether you're a data engineer, analyst, or business stakeholder, this guide will provide the foundation you need to effectively use the system.

## Key Benefits

The self-healing data pipeline provides several key benefits:

- **Reduced Manual Intervention**: AI-driven self-healing resolves up to 80% of common issues automatically
- **Improved Data Reliability**: Comprehensive quality validation and correction ensures reliable data
- **Minimized Downtime**: Predictive failure detection prevents many issues before they occur
- **Lower Operational Costs**: Automated resolution reduces the need for manual troubleshooting
- **Faster Time to Insight**: More reliable pipelines deliver data to business users faster
- **Comprehensive Visibility**: End-to-end monitoring provides clear insights into pipeline health

These benefits combine to create a more reliable, efficient data pipeline that requires less maintenance and delivers higher quality data.

## System Overview

The self-healing data pipeline is an end-to-end solution built on Google Cloud Platform that combines data ingestion, quality validation, processing, and monitoring with AI-driven self-healing capabilities.

### Architecture at a Glance

The system follows a modular architecture with five core components:

1. **Data Ingestion Layer**: Extracts data from various sources (GCS, Cloud SQL, APIs, etc.) and prepares it for processing

2. **Data Quality Layer**: Validates data against defined expectations and detects quality issues

3. **Self-Healing Layer**: Automatically detects, diagnoses, and resolves issues with minimal human intervention

4. **Processing & Storage Layer**: Transforms and loads data into BigQuery for analytics

5. **Monitoring & Alerting Layer**: Provides comprehensive visibility into pipeline health and performance

These components work together to create a resilient, self-maintaining data pipeline. For a more detailed technical overview, see the [Architecture Overview](../architecture/overview.md) documentation.

### Key Technologies

The self-healing data pipeline leverages several key technologies:

- **Google Cloud Storage**: For data lake storage and staging
- **BigQuery**: As the core data warehouse
- **Cloud Composer**: For workflow orchestration (based on Apache Airflow)
- **Vertex AI**: For machine learning and AI capabilities
- **Cloud Functions**: For serverless event-driven processing
- **Cloud Monitoring**: For comprehensive observability
- **Great Expectations**: For data validation

These technologies are integrated into a cohesive system that provides end-to-end data pipeline capabilities with self-healing features.

### Data Flow

The typical data flow through the pipeline follows these steps:

1. **Data Extraction**: Data is extracted from source systems using appropriate connectors

2. **Data Staging**: Extracted data is staged in Cloud Storage in a standardized format

3. **Quality Validation**: The data is validated against defined expectations

4. **Issue Detection & Healing**: Quality issues are automatically detected and corrected when possible

5. **Transformation & Loading**: Validated data is transformed and loaded into BigQuery

6. **Monitoring & Alerting**: Throughout the process, the system monitors performance and health

This flow is orchestrated by Cloud Composer (Apache Airflow), which manages the dependencies between steps and ensures reliable execution.

### Self-Healing Capabilities

The self-healing capabilities are what make this pipeline unique:

- **Automated Issue Detection**: The system automatically identifies problems through data quality validation, pipeline monitoring, and anomaly detection

- **AI-Driven Diagnosis**: Machine learning models analyze issues to determine root causes

- **Autonomous Resolution**: The system applies appropriate fixes based on the diagnosis, with confidence-based automation

- **Learning Feedback Loop**: Outcomes are recorded to continuously improve healing capabilities

These capabilities enable the pipeline to autonomously recover from many common issues, reducing the need for manual intervention and improving overall reliability.

## Getting Started

This section will guide you through the initial steps to start using the self-healing data pipeline.

### System Requirements

Before you begin, ensure you have:

- **Access Credentials**: Valid Google Cloud Platform account with appropriate permissions
- **Web Browser**: Chrome, Firefox, or Edge (latest versions recommended)
- **Network Access**: Connectivity to your organization's Google Cloud environment
- **Required Permissions**: Depending on your role, you'll need specific permissions to access different features

If you're missing any of these requirements, contact your system administrator for assistance.

### Accessing the System

To access the self-healing data pipeline:

1. Navigate to the system URL provided by your administrator
2. Sign in using your Google Cloud credentials
3. If this is your first time, you may be prompted to complete a brief onboarding process
4. Once signed in, you'll be directed to the main dashboard

If you encounter any issues accessing the system, check with your administrator to ensure you have the necessary permissions and that your account is properly configured.

### User Interface Overview

The user interface is organized into several main sections:

- **Dashboard**: Provides an overview of system health, recent executions, and key metrics

- **Pipeline Management**: For configuring and managing data pipelines

- **Data Quality**: For monitoring and managing data quality rules and results

- **Self-Healing**: For viewing and configuring self-healing activities

- **Monitoring**: For detailed monitoring of system health and performance

- **Configuration**: For system-wide settings and configurations

- **Administration**: For user management and system administration (if you have admin privileges)

The navigation menu on the left allows you to move between these sections. Each section has its own set of features and capabilities that we'll explore in more detail.

### User Roles and Permissions

The system supports several user roles, each with different permissions:

- **Viewer**: Can view dashboards, pipeline status, and monitoring information

- **Analyst**: Can view all information and create/edit data quality rules

- **Data Engineer**: Can configure and manage pipelines, quality rules, and view self-healing activities

- **Administrator**: Has full access to all system features, including user management

Your role determines which features you can access and what actions you can perform. If you need additional permissions, contact your system administrator.

## Creating Your First Pipeline

This section will guide you through creating your first data pipeline with self-healing capabilities.

### Pipeline Planning

Before creating a pipeline, gather the following information:

1. **Source Data Details**:
   - Data source type (GCS, Cloud SQL, API, etc.)
   - Connection parameters and credentials
   - Data format and schema information
   - Extraction parameters (incremental fields, filters, etc.)

2. **Target Details**:
   - BigQuery dataset and table names
   - Schema definition or mapping
   - Partitioning and clustering strategy

3. **Quality Requirements**:
   - Critical data fields that must be validated
   - Acceptable quality thresholds
   - Business rules for data validation

4. **Scheduling Requirements**:
   - Frequency of execution
   - Dependencies on other processes
   - Time windows for execution

Having this information ready will streamline the pipeline creation process.

### Creating a Pipeline

To create a new pipeline:

1. Navigate to **Pipeline Management** in the main menu
2. Click the **[+ Create Pipeline]** button
3. In the creation wizard, provide the following information:
   - **Basic Information**: Name, description, and owner
   - **Source Configuration**: Data source details and connection parameters
   - **Target Configuration**: BigQuery dataset, table, and schema information
   - **Quality Rules**: Data validation rules and thresholds
   - **Scheduling**: Execution schedule and dependencies
   - **Self-Healing Settings**: Configuration for automated healing capabilities

4. Review the configuration summary
5. Click **[Create Pipeline]** to finalize

The system will create the necessary Cloud Composer DAGs, BigQuery resources, and monitoring configurations based on your inputs.

### Configuring Data Sources

The system supports various data sources, each with specific configuration requirements:

**Google Cloud Storage (GCS)**:
- Bucket name and file path pattern
- File format (CSV, JSON, Avro, Parquet)
- Format-specific options (delimiters, headers, etc.)
- Schema definition or auto-detection settings

**Cloud SQL**:
- Instance connection details
- Database, schema, and table information
- Incremental extraction field (if applicable)
- Query or table extraction mode

**External APIs**:
- Endpoint URL and authentication details
- Request parameters and pagination settings
- Response parsing configuration
- Rate limiting and retry settings

For detailed instructions on configuring specific data sources, refer to the [Data Ingestion Guide](./data-ingestion.md).

### Defining Quality Rules

Quality rules define the expectations for your data and enable the system to detect issues:

1. In the pipeline creation wizard or edit screen, navigate to the **Quality Rules** section
2. Click **[+ Add Rule]** to create a new rule
3. Configure the rule:
   - **Rule Type**: Schema validation, null check, value range, pattern match, etc.
   - **Field Selection**: The field(s) the rule applies to
   - **Condition**: The specific condition to validate
   - **Severity**: How critical this rule is (Critical, High, Medium, Low)
   - **Self-Healing Action**: What action to take if the rule fails (when possible)

4. Add additional rules as needed
5. Set the overall quality threshold for the pipeline

Well-defined quality rules are essential for effective self-healing, as they enable the system to detect and correct issues automatically.

### Configuring Self-Healing

To configure self-healing for your pipeline:

1. In the pipeline creation wizard or edit screen, navigate to the **Self-Healing** section
2. Configure the following settings:
   - **Self-Healing Mode**: Automatic, Semi-Automatic, or Manual
   - **Confidence Threshold**: Minimum confidence score for automatic actions
   - **Approval Required**: When human approval is required for healing actions
   - **Max Retry Attempts**: Maximum number of healing attempts
   - **Notification Settings**: Who should be notified of healing activities

3. For specific issue types, you can configure custom healing rules:
   - **Data Format Issues**: How to handle format inconsistencies
   - **Missing Values**: Strategies for handling null or missing data
   - **Schema Issues**: How to respond to schema changes
   - **Pipeline Failures**: Retry strategies and parameter adjustments

These settings control how aggressively the system will attempt to heal issues automatically versus alert.

### Testing Your Pipeline

Before scheduling your pipeline for regular execution, it's important to test it:

1. From the pipeline details page, click **[Run Now]** to execute the pipeline immediately
2. Monitor the execution in the **Pipeline Execution** view
3. Once complete, verify the results:
   - Check that data was correctly extracted and loaded
   - Review quality validation results
   - Verify any transformations were applied correctly
   - Check for any issues or warnings

4. If issues are found, adjust the configuration and test again

Thoroughly testing your pipeline ensures it will operate correctly when scheduled for regular execution.

## Monitoring Pipeline Health

Monitoring is essential for ensuring your pipelines are operating correctly and efficiently.

### Dashboard Overview

The main dashboard provides a high-level overview of system health:

1. **Pipeline Health Summary**: Status of all pipelines (Healthy, Warning, Error)
2. **Recent Executions**: Status and timing of recent pipeline runs
3. **Data Quality Metrics**: Overall quality scores and trend
4. **Self-Healing Activity**: Recent self-healing actions and success rate
5. **Resource Utilization**: Key resource usage metrics
6. **Active Alerts**: Any current issues requiring attention

This dashboard gives you a quick assessment of overall system health and highlights any areas that need attention.

### Pipeline Monitoring

To monitor a specific pipeline:

1. Navigate to **Pipeline Management** in the main menu
2. Select the pipeline you want to monitor
3. The pipeline details page shows:
   - Current status and next scheduled run
   - Execution history and success rate
   - Quality metrics and validation results
   - Self-healing activities related to this pipeline
   - Resource utilization during execution

4. Click on a specific execution to see detailed logs, metrics, and any issues

Regular monitoring of your pipelines helps you identify and address issues before they impact your business operations.

### Understanding Alerts

The system generates alerts when it detects issues that may require attention:

- **Critical Alerts (P1)**: Severe issues with significant business impact, requiring immediate attention
- **High Alerts (P2)**: Important issues that should be addressed promptly
- **Medium Alerts (P3)**: Issues that should be addressed but aren't immediately critical
- **Low Alerts (P4)**: Minor issues or informational alerts

Alerts can be related to:
- Pipeline failures or performance issues
- Data quality problems
- Resource constraints
- Self-healing activities that require approval or failed

Alerts are delivered through configured notification channels (Teams, email, etc.) based on severity and can be viewed in the Monitoring section of the application.

### Responding to Alerts

When you receive an alert, follow these general steps:

1. **Review the Alert Details**: Understand the nature and severity of the issue

2. **Check Self-Healing Status**: Determine if self-healing has attempted to resolve the issue
   - If self-healing was successful, verify the resolution
   - If self-healing is in progress, monitor its progress
   - If self-healing failed or wasn't attempted, proceed with manual investigation

3. **Investigate if Necessary**: Use the provided information to diagnose the issue

4. **Take Appropriate Action**: Apply the necessary fix or approve a suggested healing action

5. **Verify Resolution**: Confirm that the issue is resolved

6. **Document the Issue**: Add notes about the cause and resolution

Prompt response to alerts ensures issues are addressed before they impact downstream systems and business operations.

### Performance Analysis

To analyze pipeline performance:

1. Navigate to the **Monitoring** section
2. Select the **Performance** tab
3. Choose the pipeline and time period you want to analyze
4. Review key performance metrics:
   - Execution time and trends
   - Data volume processed
   - Resource utilization
   - Quality metrics
   - Cost efficiency

5. Look for patterns such as:
   - Gradual performance degradation
   - Correlation with data volume changes
   - Resource constraints
   - Quality issues affecting performance

Regular performance analysis helps you identify optimization opportunities and ensure efficient operation of your pipelines.

## Core Concepts: Self-Healing Intelligence

The AI-driven self-healing capabilities are a key differentiator of this pipeline. Understanding how these capabilities work will help you effectively configure and leverage them for your data workflows.

### How Self-Healing Works

The self-healing process follows these key steps:

1. **Issue Detection**: The system identifies problems through data quality validation, pipeline monitoring, and anomaly detection

2. **Classification**: The AI engine classifies the issue into categories and subcategories

3. **Root Cause Analysis**: Advanced algorithms determine the underlying cause

4. **Resolution Selection**: The system selects the most appropriate healing strategy

5. **Confidence Scoring**: A confidence score is calculated for the diagnosis and proposed fix

6. **Execution Decision**: Based on confidence score, issue severity, and configuration settings, the system decides whether to:
   - Apply the fix automatically
   - Request approval before applying the fix
   - Provide a recommendation for manual resolution

7. **Healing Action**: The selected fix is applied

8. **Verification**: The system verifies that the issue has been resolved

9. **Learning**: The outcome is recorded to improve future healing capabilities

This process happens automatically in the background as your pipelines run, with minimal need for human intervention in most cases.

### Types of Self-Healing Actions

The system can apply various types of healing actions depending on the issue:

- **Data Corrections**: Fixing format issues, handling missing values, correcting pattern violations

- **Pipeline Adjustments**: Modifying parameters, changing execution paths, optimizing resource allocation

- **Retry Strategies**: Intelligent retry with backoff, parameter adjustments, and alternative approaches

- **Resource Management**: Scaling resources, redistributing workloads, optimizing query execution

- **Recovery Procedures**: Automated recovery from various failure states, transaction management

These healing actions are applied based on the specific issue detected and the confidence in the proposed resolution.

### Monitoring Self-Healing Activities

To monitor self-healing activities:

1. Navigate to the **Self-Healing** section in the main menu
2. The dashboard shows:
   - Recent healing actions and their outcomes
   - Success rate metrics and trends
   - Distribution of issue types and resolutions
   - Actions requiring approval

3. Click on a specific healing action to see details:
   - Issue description and context
   - Diagnosis and confidence score
   - Resolution action applied
   - Before and after states
   - Outcome and verification results

Regular monitoring of self-healing activities helps you understand how the system is automatically resolving issues and where it might need adjustment.

### Approving Healing Actions

Some healing actions may require your approval before being applied:

1. You'll receive a notification through configured channels (Teams, email, etc.)
2. The notification will include details about the issue and proposed healing action
3. To review and approve/reject:
   - Click the link in the notification, or
   - Navigate to Self-Healing > Approvals in the main menu

4. Review the issue details, diagnosis, and proposed action
5. Click **[Approve]** to allow the action or **[Reject]** to deny it
6. Optionally, provide a comment explaining your decision

Approval decisions should be made promptly to minimize pipeline delays. If an action isn't approved or rejected within the configured timeout period, it will be automatically canceled or escalated according to your configuration.

### Adjusting Self-Healing Settings

If you find that the self-healing system is too aggressive or not aggressive enough, you can adjust its settings:

1. Navigate to the **Configuration** section in the main menu
2. Select the **Self-Healing** tab
3. Adjust the global settings:
   - **Self-Healing Mode**: Automatic, Semi-Automatic, or Manual
   - **Confidence Threshold**: Minimum confidence score for automatic actions
   - **Approval Required**: When human approval is required
   - **Max Retry Attempts**: Maximum number of healing attempts

4. For specific issue types, adjust custom healing rules as needed
5. Save your changes

Regularly reviewing and adjusting these settings based on observed performance helps optimize the balance between automation and human oversight.

## Core Concepts: Data Quality Management

Ensuring data quality is a critical aspect of reliable data pipelines. The system provides comprehensive tools for defining, monitoring, and managing data quality.

### Understanding Quality Dimensions

The system evaluates data quality across several dimensions:

- **Completeness**: Are all required data elements present?
- **Accuracy**: Does the data conform to expected patterns and values?
- **Consistency**: Is the data consistent across related elements?
- **Timeliness**: Is the data current and available when needed?
- **Validity**: Does the data conform to defined business rules?
- **Integrity**: Are relationships between data elements maintained?

These dimensions are measured through specific validation rules and combined into an overall quality score for each dataset.

### Creating Quality Rules

Quality rules define the expectations for your data:

1. Navigate to the **Data Quality** section in the main menu
2. Select the **Rules** tab
3. Click **[+ Create Rule]** to define a new rule
4. Configure the rule:
   - **Rule Type**: Schema validation, null check, value range, pattern match, etc.
   - **Dataset and Table**: The data the rule applies to
   - **Field Selection**: The specific field(s) to validate
   - **Condition**: The specific condition to check
   - **Severity**: How critical this rule is (Critical, High, Medium, Low)
   - **Self-Healing Action**: What action to take if possible when the rule fails

5. Save the rule

You can create rules individually or import them in bulk using templates. Rules can be reused across multiple pipelines that process similar data.

### Monitoring Data Quality

To monitor data quality:

1. Navigate to the **Data Quality** section in the main menu
2. The dashboard shows:
   - Overall quality scores by dataset
   - Quality trend charts
   - Recent validation results
   - Top failing rules
   - Self-healing metrics for quality issues

3. Click on a specific dataset to see detailed quality metrics:
   - Quality scores by dimension
   - Validation results by rule
   - Sample records that failed validation
   - Quality trends over time

Regular monitoring of data quality helps you identify and address issues before they impact downstream analytics and business decisions.

### Quality Issue Resolution

When quality issues are detected, they can be resolved in several ways:

1. **Automated Self-Healing**: The system automatically applies corrections based on configured rules and confidence levels

2. **Guided Resolution**: For issues that can't be automatically resolved, the system provides recommendations

3. **Manual Correction**: For complex issues, you may need to manually correct the data or adjust the source system

4. **Rule Adjustment**: In some cases, the quality rule itself may need adjustment if it doesn't match business requirements

The appropriate resolution approach depends on the nature of the issue, its impact, and the confidence in automated corrections.

### Quality Reporting

The system provides several quality reports to help you understand and communicate data quality:

1. **Quality Scorecards**: Summary reports showing quality scores across datasets

2. **Validation Detail Reports**: Detailed results of quality validations

3. **Trend Reports**: Charts showing quality trends over time

4. **Issue Reports**: Details of specific quality issues and their resolution

5. **Self-Healing Reports**: Metrics on automated quality issue resolution

These reports can be accessed from the Data Quality section and can be exported or scheduled for regular delivery to stakeholders.

## Advanced Features

Once you're comfortable with the basic functionality, explore these advanced features to get the most out of the system.

### Custom Dashboards

You can create custom dashboards to focus on specific aspects of the system:

1. Navigate to **Monitoring > Dashboards** in the main menu
2. Click **[+ Create Dashboard]**
3. Configure the dashboard properties:
   - Name and description
   - Default time range
   - Refresh interval
   - Access permissions

4. Add widgets to your dashboard:
   - Click **[+ Add Widget]**
   - Select the widget type (chart, gauge, table, etc.)
   - Configure the data source and visualization options
   - Position and size the widget on the dashboard

5. Save your dashboard

Custom dashboards allow you to create focused views for specific monitoring needs, such as a dashboard for a particular pipeline or data domain.

### Notification Preferences

You can customize how you receive notifications:

1. Navigate to **User Settings > Notifications** in the user menu
2. Configure your notification channels:
   - **Microsoft Teams**: Connect your Teams account and select channels
   - **Email**: Verify your email address and set delivery preferences
   - **SMS**: Add and verify your mobile number (for critical alerts only)

3. Set alert preferences for each severity level:
   - Select which channels to use for each severity level
   - Set quiet hours when notifications should be suppressed
   - Configure digest settings for non-critical alerts

4. Save your preferences

Customized notification preferences ensure you receive alerts through your preferred channels without being overwhelmed by notifications.

### Pipeline Templates

For consistent pipeline configuration, you can use templates:

1. Navigate to **Pipeline Management > Templates** in the main menu
2. To create a new template:
   - Click **[+ Create Template]**
   - Configure the template with standard settings
   - Define which aspects are fixed vs. customizable
   - Save the template

3. To use a template:
   - Start creating a new pipeline
   - Select **[Use Template]** instead of starting from scratch
   - Choose the appropriate template
   - Customize the variable aspects as needed
   - Complete the pipeline creation process

Templates streamline the creation of multiple similar pipelines and ensure consistent configuration across your data estate.

### API Integration

The system provides APIs for integration with other tools and systems:

1. Navigate to **Administration > API Management** in the main menu
2. Generate an API key for your integration
3. Review the API documentation for available endpoints
4. Use the API to:
   - Trigger pipeline executions
   - Retrieve execution status and results
   - Access quality metrics and validation results
   - Manage pipeline configurations programmatically

API integration enables you to incorporate the pipeline into broader workflows and custom applications.

### Advanced Self-Healing Configuration

For specialized self-healing needs, you can create custom healing rules:

1. Navigate to the **Configuration** section in the main menu
2. Select the **Self-Healing** tab
3. Click **[+ Create Custom Rule]**
4. Configure the rule:
   - **Issue Pattern**: Define the pattern to match (error messages, metrics, etc.)
   - **Conditions**: Specific conditions that must be met
   - **Actions**: The healing actions to take
   - **Confidence Adjustment**: Fine-tune confidence scoring for this rule
   - **Approval Requirements**: Specify when approval is needed

5. Save the rule

Custom healing rules allow you to address specific issues in your environment that may not be covered by the standard healing capabilities.

## Troubleshooting

This section provides guidance for troubleshooting common issues you might encounter when using the system.

### Common Pipeline Issues

**Pipeline Execution Failures**
- Check the execution logs for specific error messages
- Verify source system availability and connectivity
- Check for data format or schema changes
- Verify permissions and credentials
- Look for resource constraints (memory, CPU, etc.)

**Slow Pipeline Performance**
- Check data volume trends for unexpected growth
- Look for bottlenecks in specific tasks
- Verify resource allocation is appropriate
- Check for concurrent pipelines causing contention
- Review query performance in BigQuery

**Scheduling Issues**
- Verify the schedule configuration in Cloud Composer
- Check for dependencies on other pipelines
- Look for maintenance windows or conflicting schedules
- Verify timezone settings

The execution logs and monitoring dashboards provide valuable information for diagnosing these issues.

### Data Quality Troubleshooting

**High Failure Rates**
- Review the specific rules that are failing
- Check for changes in source data format or content
- Verify that quality rules match current business requirements
- Look for patterns in failing records

**False Positives**
- Review rule definitions for overly strict conditions
- Check if business rules have changed
- Verify that reference data is current
- Consider adjusting rule severity or thresholds

**Quality Score Fluctuations**
- Look for correlation with data volume changes
- Check for seasonal patterns in the data
- Verify if source system changes occurred
- Review recent rule changes

The Data Quality dashboard and validation details provide insights for diagnosing quality issues.

### Self-Healing Issues

**Healing Actions Not Applied**
- Check the self-healing mode configuration
- Verify that confidence scores meet the threshold
- Look for pending approvals that might be blocking actions
- Check if the issue type is enabled for healing

**Incorrect Healing Actions**
- Review the pattern matching logic
- Check if the context has changed since rules were created
- Verify that the healing action is appropriate for the current environment
- Consider adjusting confidence thresholds

**Low Confidence Scores**
- Check if the issue is a new pattern the system hasn't seen before
- Verify that the AI models are up to date
- Consider providing more training data for similar issues
- Review the complexity of the issue pattern

The Self-Healing dashboard and activity logs provide details for diagnosing healing issues.

### Access and Permission Issues

**Unable to Access Features**
- Verify your user role and permissions
- Check if the feature requires specific permissions
- Ensure you're accessing the correct environment
- Contact your administrator if permissions need adjustment

**Unable to Modify Configuration**
- Check if you have edit permissions for the specific resource
- Verify if the resource is locked or in use
- Check for any approval workflows that might be required
- Look for configuration conflicts

**Authentication Failures**
- Verify your credentials are correct
- Check for account lockouts or password expiration
- Ensure your account has the necessary service access
- Check for SSO or MFA issues

Contact your system administrator if you continue to experience access issues.

### Getting Help

If you encounter issues not covered in this guide:

1. **Check Documentation**: Refer to the comprehensive documentation in the `/docs` directory

2. **Internal Support**: Contact your organization's data engineering team or system administrator

3. **User Community**: Check the internal user community forum for similar issues and solutions

4. **Issue Tracking**: Submit issues through your organization's ticketing system

5. **Office Hours**: Attend scheduled office hours for personalized assistance

When seeking help, provide detailed information about the issue, including any error messages, the steps you were taking, and relevant screenshots to help others assist you more effectively.

## Next Steps

Now that you're familiar with the basics of the self-healing data pipeline, here are some next steps to deepen your knowledge and expertise.

### Further Learning

Explore these resources to learn more about the system:

- **User Guides**: Detailed guides for specific aspects of the system
  - [Data Ingestion Guide](./data-ingestion.md)

- **Architecture Documentation**: In-depth technical details
  - [System Architecture Overview](../architecture/overview.md)

- **Tutorials and Examples**: Step-by-step guides for common scenarios
  - Setting up multi-source pipelines
  - Implementing complex data quality rules
  - Creating custom healing rules
  - Building advanced monitoring dashboards

### Best Practices

Follow these best practices to get the most out of the system:

- **Start Simple**: Begin with straightforward pipelines and gradually add complexity
- **Test Thoroughly**: Validate pipelines in development before promoting to production
- **Document Configurations**: Maintain documentation of your pipeline configurations and decisions
- **Regular Monitoring**: Check dashboards regularly, not just when alerts occur
- **Incremental Automation**: Gradually increase self-healing automation as confidence grows
- **Feedback Loop**: Provide feedback on healing actions to improve the system
- **Knowledge Sharing**: Share insights and learnings with your team
- **Stay Updated**: Keep up with new features and capabilities through release notes

### Getting Involved

There are several ways to get more involved with the system:

- **User Community**: Join the internal user community to share experiences and ask questions
- **Feature Requests**: Submit ideas for new features or improvements
- **Beta Testing**: Volunteer to test new features before general release
- **Knowledge Sharing**: Present your use cases and learnings at internal meetups
- **Customization**: Develop custom components or extensions for specialized needs

Your involvement helps improve the system and ensures it continues to meet your organization's evolving needs.

## Conclusion

The self-healing data pipeline represents a significant advancement in data pipeline management, combining cloud-native services with AI-driven automation to create a system that requires minimal manual intervention while delivering reliable, high-quality data.

By leveraging the capabilities described in this guide, you can:

- Streamline data ingestion from multiple sources
- Ensure consistent data quality through automated validation
- Reduce operational overhead with AI-driven self-healing
- Gain comprehensive visibility into pipeline health and performance
- Deliver more reliable data to support business decisions

As you become more familiar with the system, you'll discover additional ways to optimize your data pipelines and leverage the self-healing capabilities to address your specific requirements.

Remember that the system is designed to learn and improve over time, so your feedback and engagement are valuable contributions to its ongoing evolution.

We hope this getting started guide has provided a helpful introduction to the self-healing data pipeline. For more detailed information on specific components, refer to the specialized guides and documentation referenced throughout this document.

## Glossary

**BigQuery**: Google's fully managed, serverless data warehouse for analytics

**Cloud Composer**: Google's managed Apache Airflow service for workflow orchestration

**DAG (Directed Acyclic Graph)**: A workflow definition in Apache Airflow

**Data Quality**: The measure of how well data meets expectations for accuracy, completeness, and consistency

**Data Validation**: The process of checking data against defined rules and expectations

**Great Expectations**: An open-source data validation framework

**Pipeline**: A sequence of data processing steps from source to destination

**Self-Healing**: The ability of a system to detect and recover from issues automatically

**Vertex AI**: Google's unified AI platform for building and deploying machine learning models