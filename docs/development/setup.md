# Development Environment Setup

This document provides comprehensive instructions for setting up your development environment for the Self-Healing Data Pipeline project. Follow these steps to get your local and cloud environments configured properly.

## Prerequisites

Before you begin, ensure you have the following prerequisites installed and configured:

### Required Software

- **Python 3.9+**: Required for all development work
- **Node.js 16+**: Required for web UI development
- **Docker**: For containerized development and testing
- **Git**: For version control
- **Terraform 1.0+**: For infrastructure as code
- **Google Cloud SDK**: For interacting with Google Cloud services

### Google Cloud Account

- A Google Cloud account with billing enabled
- Appropriate permissions to create and manage resources
- The following APIs enabled in your project:
  - Cloud Composer API
  - BigQuery API
  - Cloud Storage API
  - Cloud Functions API
  - Vertex AI API
  - Cloud Build API
  - Secret Manager API
  - Cloud Monitoring API
  - Cloud Logging API

## Local Development Setup

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/self-healing-pipeline.git
cd self-healing-pipeline
```

### 2. Set Up Python Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

# Install backend dependencies
cd src/backend
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Set Up Web UI Development Environment

```bash
cd src/web
npm install
```

### 4. Configure Environment Variables

Copy the example environment files and update them with your specific configuration:

```bash
# Backend environment variables
cp src/backend/.env.example src/backend/.env

# Web environment variables
cp src/web/.env.example src/web/.env
```

Update the `.env` files with your specific configuration values, including:
- Google Cloud project ID
- Service account credentials
- API endpoints
- Development-specific settings

## Google Cloud Environment Setup

### 1. Configure Google Cloud SDK

```bash
# Initialize gcloud CLI and authenticate
gcloud init
gcloud auth login
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID
```

### 2. Create Service Accounts

Create service accounts with appropriate permissions for development:

```bash
# Create a development service account
gcloud iam service-accounts create dev-pipeline-sa \
    --display-name="Development Pipeline Service Account"

# Assign necessary roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:dev-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/composer.worker"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:dev-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:dev-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# Create and download a key file
gcloud iam service-accounts keys create dev-key.json \
    --iam-account=dev-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 3. Set Up Infrastructure Using Terraform

```bash
cd src/backend/terraform

# Initialize Terraform
terraform init

# Apply the development environment configuration
terraform apply -var-file=env/dev.tfvars
```

This will create the necessary GCP resources for development, including:
- Cloud Storage buckets
- BigQuery datasets
- Cloud Composer environment (if specified)
- Networking components
- IAM permissions

## Running the Application Locally

### 1. Start the Backend Services

```bash
cd src/backend
python app.py
```

This will start the backend API server on http://localhost:5000.

### 2. Start the Web UI Development Server

```bash
cd src/web
npm run dev
```

This will start the web UI development server on http://localhost:3000.

### 3. Using Docker Compose (Alternative)

Alternatively, you can use Docker Compose to run all services:

```bash
# From the project root
docker-compose up
```

This will start all services defined in the `docker-compose.yml` file.

## Setting Up Cloud Composer

If you need to work with Cloud Composer (Apache Airflow) locally:

### 1. Install the Composer CLI

```bash
pip install google-cloud-composer
```

### 2. Initialize the Composer Environment

```bash
cd src/backend/scripts
./init_composer.sh YOUR_PROJECT_ID YOUR_COMPOSER_ENV_NAME YOUR_REGION
```

This script will:
- Upload DAGs to your Composer environment
- Configure connections and variables
- Set up required plugins

### 3. Access the Airflow UI

```bash
gcloud composer environments describe YOUR_COMPOSER_ENV_NAME \
    --location=YOUR_REGION \
    --format="get(config.airflowUri)"
```

Open the provided URL in your browser to access the Airflow UI.

## Setting Up Great Expectations

To configure Great Expectations for data validation:

```bash
# Initialize Great Expectations in your project
cd src/backend
great_expectations init

# Configure BigQuery datasource
great_expectations datasource new
```

Follow the interactive prompts to configure your BigQuery datasource. This will create the necessary configuration files in the `great_expectations` directory.

## Setting Up Vertex AI

For AI/ML model development and deployment:

### 1. Install Vertex AI SDK

```bash
pip install google-cloud-aiplatform
```

### 2. Configure Authentication

Ensure your service account has the necessary Vertex AI permissions:

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:dev-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/aiplatform.user"
```

### 3. Initialize Vertex AI Resources

Run the setup script to create necessary Vertex AI resources:

```bash
cd src/backend/scripts
./setup_vertex_ai.sh YOUR_PROJECT_ID YOUR_REGION
```

## Running Tests

### Backend Tests

```bash
cd src/backend
pytest
```

### Web UI Tests

```bash
cd src/web
npm test
```

### Integration Tests

```bash
cd src/test
pytest -xvs integration/
```

### End-to-End Tests

```bash
# Using Cypress
cd src/test/e2e
npm run cypress:open

# Using Playwright
cd src/test/e2e
npm run playwright:test
```

## Troubleshooting

### Common Issues

#### Google Cloud Authentication

If you encounter authentication issues:

```bash
# Re-authenticate with gcloud
gcloud auth login
gcloud auth application-default login
```

#### Terraform Errors

If Terraform fails to create resources:

1. Check that all required APIs are enabled
2. Verify your service account has sufficient permissions
3. Check for quota limitations in your GCP project

#### Docker Issues

If Docker containers fail to start:

1. Check Docker logs: `docker-compose logs`
2. Ensure ports are not already in use
3. Verify environment variables are correctly set

#### Cloud Composer Issues

If DAGs are not appearing in Airflow:

1. Check the DAG parsing logs in the Airflow UI
2. Verify that DAGs are correctly uploaded to the GCS bucket
3. Check for Python syntax errors in your DAG files

## Next Steps

After setting up your development environment, refer to the following documentation:

- [Coding Standards](./coding-standards.md) for code style guidelines
- [Testing](./testing.md) for detailed testing procedures
- [CI/CD](./ci-cd.md) for continuous integration and deployment
- [Contributing](./contributing.md) for contribution guidelines

For component-specific documentation, see:

- [Data Ingestion Architecture](../architecture/data-ingestion.md)
- [Data Quality Framework](../architecture/data-quality.md)
- [Self-Healing System](../architecture/self-healing.md)
- [Monitoring System](../architecture/monitoring.md)