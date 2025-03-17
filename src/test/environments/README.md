# Test Environments

## Overview

This directory contains configurations and scripts for setting up test environments for the self-healing data pipeline. Two types of environments are supported: a local Docker-based environment for development and unit testing, and a GCP-based environment for integration and performance testing.

## Environment Types

### Local Environment

The local environment uses Docker Compose to create a containerized setup that simulates the full pipeline architecture. It includes emulators for GCP services (BigQuery, Cloud Storage, Pub/Sub), a PostgreSQL database, Redis, Apache Airflow, and mock API endpoints.

```bash
# Starting the local environment
cd local
docker-compose up -d
```

```bash
# Stopping the local environment
cd local
docker-compose down
```

### GCP Environment

The GCP environment uses Terraform to provision real GCP resources in an isolated project. This environment is suitable for integration testing, performance testing, and validating the pipeline against actual GCP services.

```bash
# Setting up the GCP environment
../scripts/setup_test_env.sh --project-id=your-project-id --region=us-central1
```

```bash
# Tearing down the GCP environment
../scripts/teardown_test_env.sh
```

## Directory Structure

```
./
├── README.md                   # This file
├── local/                      # Local Docker-based environment
│   ├── docker-compose.yml      # Docker Compose configuration
│   ├── config.yaml             # Environment configuration
│   ├── init_test_db.sql        # Database initialization script
│   └── mock-api-config.json    # Mock API configuration
├── gcp/                        # GCP-based environment
│   ├── setup_test_env.py       # Environment setup script
│   ├── teardown_test_env.py    # Environment teardown script
│   ├── config.yaml             # Default configuration
│   └── terraform/              # Terraform configurations
│       ├── variables.tf        # Input variables
│       ├── provider.tf         # Provider configuration
│       ├── backend.tf          # State backend configuration
│       ├── test_environment.tf # Core infrastructure
│       ├── test_bigquery.tf    # BigQuery resources
│       ├── test_storage.tf     # Storage resources
│       ├── test_composer.tf    # Cloud Composer resources
│       └── outputs.tf          # Output definitions
```

## Local Environment

### Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ installed
- Git repository cloned locally

### Configuration

The local environment is configured through the `local/config.yaml` file. This file contains settings for all components including emulated GCP services, database connections, test data paths, and service configurations.

### Usage

1. Navigate to the `local` directory
2. Start the environment: `docker-compose up -d`
3. Access services:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Mock API: http://localhost:1080
4. Run tests against the local environment:
   ```bash
   cd ../../..
   python -m pytest src/test/unit -v
   ```
5. Stop the environment: `docker-compose down`

For data persistence between restarts, Docker volumes are used. To completely reset the environment, use: `docker-compose down -v`

### Troubleshooting

- **Service not starting**: Check logs with `docker-compose logs [service_name]`
- **Connection issues**: Ensure all services are healthy with `docker-compose ps`
- **Database initialization**: If database fails to initialize, check `docker-compose logs db`

## GCP Environment

### Prerequisites

- Google Cloud SDK installed and configured
- Terraform 1.0+ installed
- Python 3.9+ installed
- Appropriate GCP permissions (Project Creator or existing project with Owner/Editor role)
- Git repository cloned locally

### Configuration

The GCP environment is configured through the `gcp/config.yaml` file and command-line parameters. The configuration includes project settings, resource naming, service configurations, and test data generation options.

### Setup Process

The setup process is handled by the `setup_test_env.py` script, which:

1. Generates Terraform variables based on configuration
2. Provisions GCP resources using Terraform
3. Generates and uploads test data
4. Saves environment information for later use

A convenience wrapper script is provided at `../scripts/setup_test_env.sh`.

### Setup Options

```
Usage: setup_test_env.sh [OPTIONS]

Options:
  -h, --help                 Show this help message and exit
  -c, --config PATH          Path to configuration file (default: gcp/config.yaml)
  -p, --project-id ID        Google Cloud project ID
  -r, --region REGION        Google Cloud region (default: us-central1)
  -e, --env-id ID            Test environment ID (default: auto-generated)
  -v, --volume SIZE          Test data volume (small, medium, large) (default: small)
  -a, --auto-destroy         Automatically destroy environment after TTL
  -t, --ttl HOURS            Time to live in hours for auto-destroy (default: 24)
  -o, --output PATH          Path to save environment information (default: gcp/output.json)
  -l, --log-level LEVEL      Logging level (default: INFO)
  -s, --skip-terraform       Skip Terraform execution (use existing environment)
  -d, --skip-data            Skip test data generation
```

### Teardown Process

The teardown process is handled by the `teardown_test_env.py` script, which:

1. Loads environment information from the output file
2. Destroys all provisioned resources using Terraform
3. Cleans up any data resources that might not be handled by Terraform

A convenience wrapper script is provided at `../scripts/teardown_test_env.sh`.

### Teardown Options

```
Usage: teardown_test_env.sh [OPTIONS]

Options:
  -h, --help                 Show this help message and exit
  -i, --info-file PATH       Path to environment information file (default: gcp/output.json)
  -f, --force                Force cleanup even if environment info is missing
  -l, --log-level LEVEL      Logging level (default: INFO)
```

### Auto-Destruction

The GCP environment can be configured to automatically destroy itself after a specified time-to-live (TTL) period. This is useful for CI/CD pipelines and preventing resource leakage.

To enable auto-destruction, use the `--auto-destroy` flag and optionally specify a TTL with `--ttl HOURS` (default: 24 hours).

### Running Tests

After setting up the GCP environment, you can run tests against it:

```bash
cd ../../..
python -m pytest src/test/integration -v
python -m pytest src/test/performance -v
```

The environment information file (`output.json`) contains all the details needed by the tests to connect to the GCP resources.

### Troubleshooting

- **Terraform errors**: Check the Terraform logs in the console output
- **Permission issues**: Ensure your GCP account has the necessary permissions
- **Resource limits**: Check if you've hit GCP quotas or limits
- **Cleanup failures**: Use the `--force` flag with teardown script if normal cleanup fails

## CI/CD Integration

Both test environments can be integrated into CI/CD pipelines:

- **Local Environment**: Suitable for unit tests and quick validation in PR checks
- **GCP Environment**: Suitable for integration and performance tests in merge/release pipelines

Example GitHub Actions workflow snippets are provided below:

```yaml
# GitHub Actions workflow for local environment
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up local test environment
        run: |
          cd src/test/environments/local
          docker-compose up -d
      - name: Run unit tests
        run: |
          python -m pytest src/test/unit -v
      - name: Tear down local test environment
        run: |
          cd src/test/environments/local
          docker-compose down -v
```

```yaml
# GitHub Actions workflow for GCP environment
jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up GCP test environment
        run: |
          src/test/scripts/setup_test_env.sh \
            --project-id=${{ secrets.GCP_PROJECT_ID }} \
            --region=us-central1 \
            --auto-destroy \
            --ttl=3
      - name: Run integration tests
        run: |
          python -m pytest src/test/integration -v
      - name: Tear down GCP test environment
        if: always()
        run: |
          src/test/scripts/teardown_test_env.sh
```

## Best Practices

1. **Resource Cleanup**: Always tear down GCP environments after use to avoid unnecessary costs
2. **Environment Isolation**: Use unique environment IDs to prevent conflicts between concurrent test runs
3. **Data Sizing**: Use appropriate test data volume for your needs (small for quick tests, large for performance testing)
4. **Configuration Management**: Version control your environment configurations
5. **Security**: Never commit sensitive credentials to the repository
6. **Monitoring**: Monitor resource usage during test runs to identify optimization opportunities
7. **Documentation**: Document any custom modifications to the test environments

## Contributing

When adding new features to the test environments:

1. Update the appropriate configuration files
2. Test changes in both local and GCP environments
3. Update this README with any new instructions
4. Add appropriate test cases to validate the environment setup