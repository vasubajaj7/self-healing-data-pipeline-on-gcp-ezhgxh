# Coding Standards

This document outlines the coding standards, style guidelines, and best practices for the Self-Healing Data Pipeline project. Following these standards ensures code quality, maintainability, and consistency across all components of the system.

## General Principles

- **Readability**: Code should be easy to read and understand
- **Simplicity**: Prefer simple solutions over complex ones
- **Consistency**: Follow established patterns and conventions
- **Maintainability**: Write code that is easy to maintain and extend
- **Testability**: Design code to be easily testable
- **Documentation**: Document code thoroughly and clearly
- **Performance**: Consider performance implications of code choices

## Project Structure

### Directory Organization

The project follows a modular structure organized by functionality:

```
src/
├── backend/               # Backend Python services
│   ├── api/               # API endpoints and controllers
│   ├── ingestion/         # Data ingestion components
│   ├── quality/           # Data quality validation
│   ├── self_healing/      # Self-healing AI components
│   ├── monitoring/        # Monitoring and alerting
│   ├── optimization/      # Performance optimization
│   ├── utils/             # Shared utilities
│   ├── db/                # Database models and repositories
│   ├── airflow/           # Airflow DAGs and plugins
│   └── terraform/         # Infrastructure as code
├── web/                   # Frontend React application
│   ├── src/               # Source code
│   │   ├── components/    # React components
│   │   ├── hooks/         # Custom React hooks
│   │   ├── services/      # API services
│   │   ├── utils/         # Utility functions
│   │   └── contexts/      # React contexts
└── test/                  # Test suites
    ├── unit/              # Unit tests
    ├── integration/       # Integration tests
    ├── e2e/               # End-to-end tests
    └── performance/       # Performance tests
```

### File Naming Conventions

- **Python Files**: Use snake_case for file names (e.g., `data_processor.py`)
- **TypeScript/JavaScript Files**: Use camelCase for utility files (e.g., `apiClient.ts`)
- **React Components**: Use PascalCase for component files (e.g., `DataQualityCard.tsx`)
- **Test Files**: Append `.test` to the file name (e.g., `data_processor.test.py`, `DataQualityCard.test.tsx`)
- **Configuration Files**: Use lowercase with appropriate extensions (e.g., `.env`, `docker-compose.yml`)

## Python Standards

### Style Guide

We follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) with some modifications:

- **Line Length**: Maximum 100 characters
- **Indentation**: 4 spaces (no tabs)
- **Quotes**: Single quotes for strings, double quotes for docstrings
- **Imports**: Group imports in the following order:
  1. Standard library imports
  2. Related third-party imports
  3. Local application/library specific imports
- **Whitespace**: Follow PEP 8 recommendations for whitespace

### Code Formatting

We use the following tools for code formatting and linting:

- **Black**: For automatic code formatting
- **isort**: For sorting imports
- **flake8**: For linting
- **mypy**: For static type checking

Configuration for these tools is in `pyproject.toml`.

### Type Hints

Use type hints for all function definitions:

```python
def process_data(input_data: Dict[str, Any], options: Optional[List[str]] = None) -> DataFrame:
    """Process the input data with the given options.
    
    Args:
        input_data: The data to process
        options: Optional processing options
        
    Returns:
        Processed data as a DataFrame
    """
    # Implementation
```

### Docstrings

Use Google-style docstrings for all modules, classes, and functions:

```python
def validate_schema(data: DataFrame, schema: Dict[str, str]) -> Tuple[bool, List[str]]:
    """Validate that the data conforms to the expected schema.
    
    Args:
        data: The DataFrame to validate
        schema: Dictionary mapping column names to expected data types
        
    Returns:
        A tuple containing:
            - Boolean indicating if validation passed
            - List of validation error messages
    
    Raises:
        ValueError: If schema is empty or invalid
    """
    # Implementation
```

### Error Handling

- Use specific exception types rather than generic exceptions
- Handle exceptions at the appropriate level
- Log exceptions with context information
- Use custom exception classes for domain-specific errors

```python
try:
    result = process_data(input_data)
except (ConnectionError, TimeoutError) as e:
    logger.error(f"Network error during data processing: {e}")
    raise DataProcessingError(f"Failed to process data: {e}") from e
except ValueError as e:
    logger.warning(f"Invalid data format: {e}")
    return default_result
```

### Classes and Functions

- Follow the Single Responsibility Principle
- Keep functions focused and concise (preferably under 50 lines)
- Use descriptive names for functions and variables
- Prefer composition over inheritance
- Use dataclasses or Pydantic models for data containers

### Testing

- Write unit tests for all functions and classes
- Use pytest as the testing framework
- Aim for at least 85% code coverage
- Use fixtures for test setup
- Mock external dependencies

See [Testing Guide](./testing.md) for more details.

## TypeScript/JavaScript Standards

### Style Guide

We follow the TypeScript ESLint recommended rules with some customizations:

- **Line Length**: Maximum 100 characters
- **Indentation**: 2 spaces (no tabs)
- **Quotes**: Single quotes for strings
- **Semicolons**: Required
- **Trailing Commas**: Required for multiline (ES5 compatible)

### Code Formatting

We use the following tools for code formatting and linting:

- **ESLint**: For linting and static analysis
- **Prettier**: For code formatting
- **TypeScript**: For static type checking

Configuration for these tools is in `.eslintrc.ts` and `.prettierrc`.

### TypeScript Usage

- Use TypeScript for all new JavaScript code
- Define interfaces for all data structures
- Use type annotations for function parameters and return types
- Avoid using `any` type when possible
- Use generics for reusable components and functions

```typescript
interface DataValidationResult {
  isValid: boolean;
  errors: string[];
}

function validateData<T>(data: T, schema: Schema): DataValidationResult {
  // Implementation
}
```

### React Components

- Use functional components with hooks
- Define prop types using TypeScript interfaces
- Use destructuring for props
- Keep components focused and reusable
- Follow the presentational/container component pattern

```tsx
interface DataCardProps {
  title: string;
  data: DataPoint[];
  onRefresh?: () => void;
}

const DataCard: React.FC<DataCardProps> = ({ title, data, onRefresh }) => {
  // Implementation
};
```

### React Hooks

- Follow the Rules of Hooks
- Create custom hooks for reusable logic
- Use appropriate dependency arrays for useEffect
- Prefer multiple simple hooks over complex ones

```tsx
function useDataFetching<T>(url: string): {
  data: T | null;
  loading: boolean;
  error: Error | null;
  refetch: () => void;
} {
  // Implementation
}
```

### State Management

- Use React Context for global state
- Use useState for component-local state
- Consider using useReducer for complex state logic
- Keep state normalized and minimal

### Testing

- Write unit tests for all components and hooks
- Use Jest and React Testing Library
- Test component behavior, not implementation details
- Mock API calls and external dependencies
- Aim for at least 80% code coverage

See [Testing Guide](./testing.md) for more details.

## SQL Standards

### Style Guide

- **Keywords**: Use UPPERCASE for SQL keywords
- **Identifiers**: Use snake_case for table and column names
- **Indentation**: 2 spaces for each level
- **Line Breaks**: Place each major clause on a new line
- **Aliasing**: Always use meaningful table aliases

### Query Structure

```sql
SELECT
  p.pipeline_id,
  p.pipeline_name,
  COUNT(e.execution_id) AS execution_count,
  AVG(e.duration_seconds) AS avg_duration
FROM
  pipeline_definitions p
LEFT JOIN
  pipeline_executions e ON p.pipeline_id = e.pipeline_id
WHERE
  p.is_active = TRUE
  AND e.start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY
  p.pipeline_id,
  p.pipeline_name
HAVING
  execution_count > 0
ORDER BY
  avg_duration DESC
LIMIT 10
```

### BigQuery Optimization

- Use partitioning and clustering appropriately
- Filter on partitioned columns when possible
- Minimize data scanned by selecting only needed columns
- Use appropriate data types for columns
- Consider cost implications of queries
- Use parameterized queries to prevent SQL injection

### Query Comments

Add comments to complex queries explaining the purpose and any non-obvious logic:

```sql
-- Find pipelines with increasing error rates over the past week
-- compared to the previous week
WITH current_week AS (
  -- Current week error counts
  SELECT
    pipeline_id,
    COUNT(*) AS error_count
  FROM
    pipeline_executions
  WHERE
    status = 'FAILED'
    AND execution_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY
    pipeline_id
),
previous_week AS (
  -- Previous week error counts
  SELECT
    pipeline_id,
    COUNT(*) AS error_count
  FROM
    pipeline_executions
  WHERE
    status = 'FAILED'
    AND execution_time BETWEEN 
      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY) AND
      TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY
    pipeline_id
)

SELECT
  p.pipeline_name,
  cw.error_count AS current_errors,
  pw.error_count AS previous_errors,
  (cw.error_count - pw.error_count) AS error_increase
FROM
  current_week cw
JOIN
  previous_week pw ON cw.pipeline_id = pw.pipeline_id
JOIN
  pipeline_definitions p ON cw.pipeline_id = p.pipeline_id
WHERE
  cw.error_count > pw.error_count
ORDER BY
  error_increase DESC
```

## Infrastructure as Code Standards

### Terraform Standards

- Use Terraform modules for reusable components
- Follow a consistent naming convention for resources
- Use variables for all configurable values
- Document all variables, outputs, and modules
- Use remote state with appropriate locking
- Format Terraform files using `terraform fmt`

```hcl
module "bigquery_dataset" {
  source = "../modules/bigquery"

  dataset_id   = "${var.project_prefix}_analytics"
  friendly_name = "Analytics Dataset"
  description   = "Dataset for analytical data processing"
  location      = var.region

  access = [
    {
      role          = "OWNER"
      special_group = "projectOwners"
    },
    {
      role           = "READER"
      group_by_email = var.analyst_group_email
    }
  ]

  labels = {
    environment = var.environment
    department  = "data-engineering"
    application = "self-healing-pipeline"
  }
}
```

### Kubernetes/Kustomize Standards

- Use Kustomize for environment-specific configurations
- Organize resources by component
- Use namespaces to isolate resources
- Follow a consistent labeling scheme
- Document all custom resources

```yaml
# Base deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend-api
  labels:
    app: backend-api
    component: api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: backend-api
  template:
    metadata:
      labels:
        app: backend-api
    spec:
      containers:
      - name: api
        image: gcr.io/project-id/backend-api:latest
        ports:
        - containerPort: 8000
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
        env:
        - name: LOG_LEVEL
          value: INFO
```

### Docker Standards

- Use multi-stage builds to minimize image size
- Specify exact versions for base images
- Use non-root users for running applications
- Include only necessary files in the image
- Document all environment variables
- Optimize layer caching

```dockerfile
# Build stage
FROM python:3.9-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

# Final stage
FROM python:3.9-slim

WORKDIR /app

# Create non-root user
RUN useradd -m appuser

# Copy wheels from builder stage
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install dependencies
RUN pip install --no-cache /wheels/*

# Copy application code
COPY src/ /app/src/

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO

CMD ["python", "-m", "src.main"]
```

## Documentation Standards

### Code Documentation

- Document all modules, classes, and functions
- Use docstrings for Python code
- Use JSDoc for JavaScript/TypeScript code
- Keep documentation up-to-date with code changes
- Document complex algorithms and business logic

### README Files

- Include a README.md in each major directory
- Describe the purpose and contents of the directory
- Provide usage examples where appropriate
- List dependencies and requirements

### Markdown Standards

- Use ATX-style headers (`#` for headers)
- Use fenced code blocks with language specification
- Use bullet points for lists
- Include a table of contents for long documents
- Use relative links for internal references

### API Documentation

- Document all API endpoints
- Specify request and response formats
- Document error responses
- Include authentication requirements
- Provide usage examples

```markdown
## Get Pipeline Status

Returns the current status of a specific pipeline.

**URL**: `/api/v1/pipelines/{pipeline_id}/status`

**Method**: `GET`

**Auth required**: Yes (Bearer Token)

**Permissions required**: `pipeline:read`

**URL Parameters**:
- `pipeline_id`: The ID of the pipeline

**Query Parameters**:
- `include_tasks` (optional): Set to `true` to include task details

**Success Response**:
- **Code**: 200 OK
- **Content**:
```json
{
  "pipeline_id": "analytics_daily",
  "status": "RUNNING",
  "start_time": "2023-06-15T08:30:00Z",
  "duration": 1250,
  "progress": 65,
  "tasks": [
    {
      "task_id": "extract_data",
      "status": "COMPLETED",
      "duration": 450
    },
    {
      "task_id": "transform_data",
      "status": "RUNNING",
      "duration": 800
    }
  ]
}
```

**Error Responses**:
- **Code**: 404 Not Found
- **Content**: `{"error": "Pipeline not found"}`

OR

- **Code**: 403 Forbidden
- **Content**: `{"error": "Insufficient permissions"}`
```

## Git Standards

### Branch Naming

- Use descriptive branch names that reflect the purpose of the changes
- Follow the pattern: `<type>/<description>`
- Types include: `feature`, `bugfix`, `hotfix`, `docs`, `refactor`, `test`

Examples:
- `feature/add-data-quality-dashboard`
- `bugfix/fix-null-handling-in-transform`
- `docs/update-api-documentation`

### Commit Messages

Follow the Conventional Commits format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Where:
- `type`: The type of change (feat, fix, docs, style, refactor, perf, test, chore)
- `scope`: The scope of the change (optional)
- `subject`: A short description of the change
- `body`: A more detailed description (optional)
- `footer`: Information about breaking changes or issue references (optional)

Examples:
```
feat(quality): add schema drift detection

Implement automatic detection of schema changes between source and target.
This helps identify potential data quality issues early in the pipeline.

Closes #123
```

```
fix(ingestion): handle null values in CSV files

Previously, null values in CSV files would cause the pipeline to fail.
Now they are properly handled and converted to NULL in BigQuery.
```

### Pull Requests

- Create a pull request for each feature or fix
- Use the provided pull request template
- Reference related issues in the description
- Ensure all CI checks pass before requesting review
- Request review from appropriate team members
- Address all review comments

### Contributing Process

- Fork the repository if you're an external contributor
- Create a branch for your changes
- Make changes following the coding standards
- Write tests for your changes
- Update documentation as needed
- Submit a pull request with a clear description
- Respond to code review feedback promptly
- Update your PR as needed based on feedback

## Security Standards

### Authentication and Authorization

- Use OAuth 2.0 or similar for authentication
- Implement role-based access control
- Use principle of least privilege for all accounts
- Regularly rotate credentials and keys
- Store secrets in Secret Manager, never in code

### Data Protection

- Encrypt sensitive data at rest and in transit
- Use HTTPS for all API endpoints
- Implement proper input validation
- Sanitize all user inputs to prevent injection attacks
- Follow data minimization principles

### Code Security

- Run security scanning tools as part of CI/CD
- Address all high and critical vulnerabilities promptly
- Keep dependencies up-to-date
- Follow secure coding practices
- Conduct regular security reviews

### Infrastructure Security

- Use VPC Service Controls where appropriate
- Implement network segmentation
- Enable audit logging for all resources
- Use IAM for access control
- Regularly review and update security configurations

## Performance Standards

### General Performance Guidelines

- Consider performance implications of code changes
- Profile code to identify bottlenecks
- Optimize critical paths
- Use appropriate data structures and algorithms
- Implement caching where beneficial

### BigQuery Performance

- Optimize query patterns for cost and performance
- Use appropriate partitioning and clustering
- Minimize data scanned by queries
- Use materialized views for common query patterns
- Monitor and optimize slot usage

### API Performance

- Implement pagination for large result sets
- Use appropriate caching headers
- Optimize database queries
- Consider rate limiting for high-traffic endpoints
- Monitor and optimize response times

### Frontend Performance

- Minimize bundle size
- Implement code splitting
- Optimize rendering performance
- Use memoization for expensive calculations
- Follow React performance best practices

## Code Review Standards

### Code Review Process

1. **Author**: Create a pull request with a clear description
2. **Reviewers**: Review the code within 2 business days
3. **Author**: Address all comments and request re-review if needed
4. **Reviewers**: Approve the pull request or request additional changes
5. **Author**: Merge the pull request once approved

### Review Checklist

- Does the code follow the project's coding standards?
- Is the code well-structured and maintainable?
- Are there appropriate tests with good coverage?
- Is the documentation complete and accurate?
- Are there any security concerns?
- Are there any performance issues?
- Is error handling implemented appropriately?
- Are there any edge cases not covered?

### Review Etiquette

- Be respectful and constructive in comments
- Focus on the code, not the person
- Explain the reasoning behind suggestions
- Acknowledge good solutions and approaches
- Use questions to understand decisions rather than making assumptions
- Respond to review comments promptly

## Automated Enforcement

### Linting and Formatting

Automated tools enforce many of these standards:

- **Python**: Black, isort, flake8, mypy
- **TypeScript/JavaScript**: ESLint, Prettier
- **Terraform**: terraform fmt, tflint
- **Docker**: hadolint

### Pre-commit Hooks

Use pre-commit hooks to enforce standards before committing:

```bash
pip install pre-commit
pre-commit install
```

The pre-commit configuration is in `.pre-commit-config.yaml`.

### CI/CD Checks

The CI/CD pipeline enforces standards through automated checks:

- Linting and formatting checks
- Unit and integration tests
- Security scanning
- Code coverage requirements
- Documentation building

These checks should run automatically on every pull request and merge to ensure code quality is maintained throughout the development process.

## Conclusion

Following these coding standards ensures that our codebase remains maintainable, reliable, and secure. These standards will evolve over time as we identify improvements and adapt to new technologies and practices.

If you have suggestions for improving these standards, please open a pull request with your proposed changes.

## References

- [PEP 8 -- Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [TypeScript Coding Guidelines](https://github.com/microsoft/TypeScript/wiki/Coding-guidelines)
- [React Hooks Documentation](https://reactjs.org/docs/hooks-intro.html)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Google SQL Style Guide](https://google.github.io/styleguide/sqlguide.html)
- [Terraform Best Practices](https://www.terraform-best-practices.com/)
- [OWASP Secure Coding Practices](https://owasp.org/www-project-secure-coding-practices-quick-reference-guide/)