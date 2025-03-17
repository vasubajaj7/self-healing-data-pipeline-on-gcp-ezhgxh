# Deployment Guide

## Introduction

This document provides comprehensive instructions for deploying the self-healing data pipeline system across different environments. It covers infrastructure provisioning, application deployment, configuration management, and verification procedures.

The deployment process follows a GitOps approach, with infrastructure and application configurations stored in version control and deployed through automated pipelines. This ensures consistency, reproducibility, and auditability of all deployments.

## Prerequisites

Before beginning the deployment process, ensure the following prerequisites are met:

### Required Tools

- Google Cloud SDK (latest version)
- Terraform (v1.0.0+)
- kubectl (v1.20.0+)
- Helm (v3.0.0+)
- Git

### Required Permissions

- Google Cloud IAM permissions:
  - Project Owner or Editor role for initial setup
  - Compute Admin
  - Kubernetes Engine Admin
  - Service Account User
  - Storage Admin
  - BigQuery Admin
  - Composer Admin

### Required Resources

- Google Cloud Project with billing enabled
- GitHub repository access with write permissions
- Access to container registry (Artifact Registry or Container Registry)

## Deployment Architecture

The self-healing data pipeline system is deployed using a multi-tier architecture:

1. **Infrastructure Layer**: GCP resources provisioned via Terraform
   - VPC, subnets, and networking
   - GKE clusters
   - BigQuery datasets
   - Cloud Storage buckets
   - Cloud Composer environments
   - IAM roles and service accounts

2. **Application Layer**: Deployed on GKE via Kubernetes manifests
   - Backend services
   - Web UI
   - Monitoring components

3. **Configuration Layer**: Environment-specific configurations
   - Secrets and credentials
   - Environment variables
   - Feature flags

![Deployment Architecture](../images/deployment-architecture.png)

## Infrastructure Deployment

### Preparing for Infrastructure Deployment

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/self-healing-pipeline.git
   cd self-healing-pipeline
   ```

2. Set up Google Cloud authentication:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

3. Create a service account for Terraform (if not using Cloud Build):
   ```bash
   gcloud iam service-accounts create terraform-deployer
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:terraform-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/owner"
   gcloud iam service-accounts keys create terraform-key.json \
     --iam-account=terraform-deployer@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

### Deploying Infrastructure with Terraform

1. Navigate to the Terraform directory:
   ```bash
   cd infrastructure/terraform
   ```

2. Initialize Terraform:
   ```bash
   terraform init
   ```

3. Create a terraform.tfvars file for your environment (or use existing env files):
   ```bash
   # For development
   cp env/dev.tfvars terraform.tfvars
   # Edit terraform.tfvars with your specific values
   ```

4. Plan the deployment:
   ```bash
   terraform plan -var-file=terraform.tfvars -out=tfplan
   ```

5. Apply the Terraform plan:
   ```bash
   terraform apply tfplan
   ```

6. Verify infrastructure deployment:
   ```bash
   terraform output
   # Note the outputs for use in subsequent steps
   ```

### Infrastructure Modules Deployment Order

For manual deployments, follow this order to ensure proper dependency management:

1. VPC and networking (`infrastructure/terraform/vpc`)
2. IAM roles and service accounts (`infrastructure/terraform/iam`)
3. GKE clusters (`infrastructure/terraform/gke`)
4. Storage resources (BigQuery, Cloud Storage)
5. Cloud Composer environment
6. Monitoring resources

## Application Deployment

### Kubernetes Deployment

1. Configure kubectl to connect to your GKE cluster:
   ```bash
   gcloud container clusters get-credentials CLUSTER_NAME --zone ZONE --project YOUR_PROJECT_ID
   ```

2. Deploy using Kustomize:
   ```bash
   # For development environment
   kubectl apply -k infrastructure/k8s/overlays/dev
   
   # For staging environment
   kubectl apply -k infrastructure/k8s/overlays/staging
   
   # For production environment
   kubectl apply -k infrastructure/k8s/overlays/prod
   ```

3. Alternatively, deploy using Helm:
   ```bash
   # For development environment
   helm upgrade --install self-healing-pipeline ./infrastructure/helm/self-healing-pipeline \
     -f ./infrastructure/helm/self-healing-pipeline/values-dev.yaml \
     --namespace self-healing-pipeline --create-namespace
   ```

### Backend Services Deployment

1. Deploy the backend services:
   ```bash
   kubectl apply -f infrastructure/k8s/components/backend/deployment.yaml
   kubectl apply -f infrastructure/k8s/components/backend/service.yaml
   kubectl apply -f infrastructure/k8s/components/backend/hpa.yaml
   ```

2. Verify backend deployment:
   ```bash
   kubectl get deployments -n self-healing-pipeline
   kubectl get pods -n self-healing-pipeline
   kubectl get services -n self-healing-pipeline
   ```

### Web UI Deployment

1. Deploy the web UI components:
   ```bash
   kubectl apply -f infrastructure/k8s/components/web/deployment.yaml
   kubectl apply -f infrastructure/k8s/components/web/service.yaml
   kubectl apply -f infrastructure/k8s/components/web/hpa.yaml
   ```

2. Verify web UI deployment:
   ```bash
   kubectl get deployments -n self-healing-pipeline
   kubectl get pods -n self-healing-pipeline
   kubectl get services -n self-healing-pipeline
   ```

### Cloud Composer DAGs Deployment

1. Deploy Airflow DAGs to Cloud Composer:
   ```bash
   gcloud composer environments storage dags import \
     --environment COMPOSER_ENV_NAME \
     --location LOCATION \
     --source src/backend/airflow/dags
   ```

2. Deploy Airflow plugins to Cloud Composer:
   ```bash
   gcloud composer environments storage plugins import \
     --environment COMPOSER_ENV_NAME \
     --location LOCATION \
     --source src/backend/airflow/plugins
   ```

## Environment-Specific Configurations

### Development Environment

The development environment is designed for feature development and testing. It uses:

- Smaller GKE cluster (1-3 nodes)
- Reduced resource requests and limits
- Debug logging enabled
- Non-production data sources

Configuration files:
- `infrastructure/terraform/env/dev.tfvars`
- `infrastructure/k8s/overlays/dev/*`
- `infrastructure/helm/self-healing-pipeline/values-dev.yaml`

### Staging Environment

The staging environment mirrors production for pre-release testing. It uses:

- Production-like GKE cluster (3-5 nodes)
- Production-equivalent resource allocations
- Standard logging level
- Anonymized production data or production-like test data

Configuration files:
- `infrastructure/terraform/env/staging.tfvars`
- `infrastructure/k8s/overlays/staging/*`
- `infrastructure/helm/self-healing-pipeline/values-staging.yaml`

### Production Environment

The production environment is designed for reliability and performance. It uses:

- Highly available GKE cluster (5+ nodes, multi-zone)
- Optimized resource allocations
- Minimal logging (errors and critical information only)
- Production data with appropriate security controls

Configuration files:
- `infrastructure/terraform/env/prod.tfvars`
- `infrastructure/k8s/overlays/prod/*`
- `infrastructure/helm/self-healing-pipeline/values-prod.yaml`

## Deployment Verification

### Infrastructure Verification

1. Verify GKE cluster status:
   ```bash
   gcloud container clusters describe CLUSTER_NAME --zone ZONE
   ```

2. Verify BigQuery datasets:
   ```bash
   bq ls
   ```

3. Verify Cloud Storage buckets:
   ```bash
   gsutil ls
   ```

4. Verify Cloud Composer environment:
   ```bash
   gcloud composer environments describe COMPOSER_ENV_NAME --location LOCATION
   ```

### Application Verification

1. Verify all pods are running:
   ```bash
   kubectl get pods -n self-healing-pipeline
   ```

2. Verify services are exposed correctly:
   ```bash
   kubectl get services -n self-healing-pipeline
   ```

3. Verify ingress configuration:
   ```bash
   kubectl get ingress -n self-healing-pipeline
   ```

4. Check application logs for errors:
   ```bash
   kubectl logs -l app=backend -n self-healing-pipeline
   kubectl logs -l app=web -n self-healing-pipeline
   ```

### Functional Verification

1. Access the web UI at the exposed URL
2. Verify authentication is working
3. Check dashboard displays correctly
4. Verify data pipeline status is visible
5. Run a test pipeline to confirm end-to-end functionality

## Rollback Procedures

### Infrastructure Rollback

To rollback infrastructure changes:

1. Identify the previous working Terraform state or commit
2. Apply the previous Terraform configuration:
   ```bash
   git checkout <previous-commit>
   cd infrastructure/terraform
   terraform init
   terraform plan -var-file=terraform.tfvars -out=tfplan
   terraform apply tfplan
   ```

### Kubernetes Application Rollback

1. Rollback to a previous deployment:
   ```bash
   kubectl rollout undo deployment/backend -n self-healing-pipeline
   kubectl rollout undo deployment/web -n self-healing-pipeline
   ```

2. Alternatively, rollback to a specific revision:
   ```bash
   kubectl rollout history deployment/backend -n self-healing-pipeline
   kubectl rollout undo deployment/backend --to-revision=<revision-number> -n self-healing-pipeline
   ```

3. Verify the rollback was successful:
   ```bash
   kubectl get pods -n self-healing-pipeline
   kubectl rollout status deployment/backend -n self-healing-pipeline
   ```

### Using the Rollback Script

For convenience, a rollback script is provided:

```bash
cd infrastructure/scripts
./rollback.sh <environment> <component> <version>
```

Example:
```bash
./rollback.sh prod backend v1.2.3
```

## CI/CD Integration

The repository includes GitHub Actions workflows for continuous integration and deployment.

### GitHub Actions Workflows

- `.github/workflows/ci.yml`: Runs tests and builds artifacts
- `.github/workflows/cd.yml`: Deploys to target environments
- `.github/workflows/pr-validation.yml`: Validates pull requests

### Automated Deployment Process

1. Merge to main branch triggers the CI workflow
2. Successful CI builds trigger the CD workflow for development environment
3. Manual approval required for staging and production deployments
4. Deployment status and logs available in GitHub Actions UI

### Setting Up CI/CD Secrets

The following secrets need to be configured in GitHub:

- `GCP_PROJECT_ID`: Google Cloud project ID
- `GCP_SA_KEY`: Base64-encoded service account key with deployment permissions
- `DOCKER_USERNAME` and `DOCKER_PASSWORD`: Container registry credentials
- Environment-specific secrets as needed

## Security Considerations

### Secure Deployment Practices

1. **Least Privilege Principle**: Use service accounts with minimal required permissions
2. **Secret Management**: Store secrets in Secret Manager, not in code or environment variables
3. **Network Security**: Use private GKE clusters and VPC Service Controls where possible
4. **Image Security**: Scan container images for vulnerabilities before deployment
5. **Secure CI/CD**: Protect CI/CD credentials and use trusted runners

### Sensitive Data Handling

1. Customer-managed encryption keys (CMEK) for sensitive data
2. Data classification and appropriate controls
3. Audit logging for all deployment activities
4. Regular security reviews of deployment procedures

## Troubleshooting

### Common Deployment Issues

#### Infrastructure Deployment Failures

- **Issue**: Terraform fails with permission errors
  - **Solution**: Verify service account has required IAM roles

- **Issue**: Resource quota exceeded
  - **Solution**: Request quota increase or optimize resource usage

- **Issue**: Terraform state lock
  - **Solution**: Check for abandoned operations and use `terraform force-unlock`

#### Kubernetes Deployment Failures

- **Issue**: Pods stuck in Pending state
  - **Solution**: Check for resource constraints or PVC issues

- **Issue**: ImagePullBackOff errors
  - **Solution**: Verify image exists and credentials are correct

- **Issue**: CrashLoopBackOff errors
  - **Solution**: Check container logs for application errors

#### Application Configuration Issues

- **Issue**: Application fails to connect to services
  - **Solution**: Verify service endpoints and credentials

- **Issue**: Environment-specific configuration missing
  - **Solution**: Check ConfigMaps and Secrets are properly created

### Getting Help

If you encounter issues not covered in this guide:

1. Check the project's internal documentation
2. Review recent changes in the repository
3. Contact the DevOps team via Slack (#devops-support)
4. Create an issue in the project repository with detailed information

## Appendix

### Deployment Checklist

- [ ] Prerequisites verified
- [ ] Infrastructure deployed
- [ ] Application components deployed
- [ ] Environment-specific configurations applied
- [ ] Deployment verification completed
- [ ] Monitoring and alerting configured
- [ ] Documentation updated

### Reference Documentation

- [Google Kubernetes Engine Documentation](https://cloud.google.com/kubernetes-engine/docs)
- [Terraform Google Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)