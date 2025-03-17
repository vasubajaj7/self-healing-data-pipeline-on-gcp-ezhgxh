# Self-Healing Data Pipeline Infrastructure

This directory contains the infrastructure components for the self-healing data pipeline project. The infrastructure is defined as code using Terraform, Kubernetes manifests, and Helm charts to ensure consistency, reproducibility, and version control across all environments.

## Architecture Overview

The infrastructure architecture is built on Google Cloud Platform (GCP) and follows a multi-tier approach with separation of concerns between different components. The architecture is designed to be scalable, resilient, and secure.

### Key Components

- **VPC Network**: Secure network infrastructure with private subnets, Cloud NAT, and firewall rules
- **GKE Cluster**: Managed Kubernetes cluster with node pools optimized for different workloads
- **Cloud Composer**: Managed Apache Airflow for pipeline orchestration
- **BigQuery**: Data warehouse for analytics and storage
- **Cloud Storage**: Object storage for data lake and staging
- **Vertex AI**: Machine learning platform for self-healing capabilities
- **Cloud Monitoring**: Observability and alerting

### Environment Separation

The infrastructure supports multiple environments (development, staging, production) with appropriate isolation and configuration for each environment. Environment-specific configurations are managed through Terraform variables, Kubernetes overlays, and Helm value files.

### Security Considerations

- Private GKE clusters with authorized networks
- VPC Service Controls for data exfiltration prevention
- Workload Identity for secure service authentication
- Binary Authorization for container image verification
- Encryption at rest and in transit
- Least privilege IAM roles

## Directory Structure

```
infrastructure/
├── k8s/                  # Kubernetes manifests
│   ├── base/             # Base Kubernetes configurations
│   ├── components/       # Component-specific configurations
│   └── overlays/         # Environment-specific overlays
├── helm/                 # Helm charts
│   └── self-healing-pipeline/  # Main application chart
├── terraform/            # Terraform configurations
│   ├── gke/              # GKE cluster configuration
│   ├── vpc/              # Network infrastructure
│   └── iam/              # Identity and access management
├── scripts/              # Deployment and maintenance scripts
└── diagrams/             # Architecture diagrams
```

## Kubernetes (k8s)

The `k8s` directory contains Kubernetes manifests organized using Kustomize for environment-specific configurations.

### Base Configuration

The `base` directory contains the core Kubernetes resources that are common across all environments:

- `namespace.yaml`: Defines the application namespace
- `service-accounts.yaml`: Service accounts for different components
- `rbac.yaml`: Role-based access control configurations
- `configmaps.yaml`: Common configuration data
- `secrets.yaml`: Template for secret resources (actual values managed separately)
- `kustomization.yaml`: Base kustomization file

### Components

The `components` directory contains configurations for specific application components:

- `backend/`: Backend service deployments, services, and HPA
- `web/`: Frontend web application deployments, services, and HPA

### Overlays

The `overlays` directory contains environment-specific configurations that extend the base:

- `dev/`: Development environment configurations
- `staging/`: Staging environment configurations
- `prod/`: Production environment configurations

Each overlay includes environment-specific customizations such as replica counts, resource limits, and configuration values.

## Helm Charts

The `helm` directory contains Helm charts for deploying the application and its dependencies.

### Self-Healing Pipeline Chart

The main application chart in `self-healing-pipeline/` includes:

- `Chart.yaml`: Chart metadata and dependencies
- `values.yaml`: Default configuration values
- `values-dev.yaml`, `values-staging.yaml`, `values-prod.yaml`: Environment-specific values
- `templates/`: Kubernetes resource templates

The chart includes dependencies for monitoring (Prometheus, Grafana), certificate management (cert-manager), and ingress (ingress-nginx).

### Chart Dependencies

The main chart has the following dependencies:

- Prometheus: Metrics collection and monitoring
- Grafana: Visualization dashboards
- cert-manager: TLS certificate management
- ingress-nginx: Ingress controller for external access

## Terraform

The `terraform` directory contains Infrastructure as Code definitions for provisioning GCP resources.

### GKE Configuration

The `gke` directory contains Terraform configurations for the Google Kubernetes Engine cluster:

- `main.tf`: GKE cluster and node pool definitions
- `variables.tf`: Input variables
- `outputs.tf`: Output values
- `provider.tf`: Provider configuration

The GKE configuration includes:
- Regional cluster for high availability
- Multiple node pools optimized for different workloads (general purpose, memory-optimized, compute-optimized)
- Workload Identity for secure authentication
- Binary Authorization for container verification
- Backup and monitoring configurations

### VPC Configuration

The `vpc` directory contains Terraform configurations for the network infrastructure:

- `main.tf`: VPC network, subnets, and related resources
- `variables.tf`: Input variables
- `outputs.tf`: Output values
- `provider.tf`: Provider configuration

The VPC configuration includes:
- Custom VPC network with private subnets
- Secondary IP ranges for GKE pods and services
- Cloud NAT for outbound internet access
- Firewall rules for secure communication
- Private service access for Google managed services
- VPC Access Connector for serverless services

### IAM Configuration

The `iam` directory contains Terraform configurations for identity and access management:

- `main.tf`: Service accounts and IAM role bindings
- `variables.tf`: Input variables
- `outputs.tf`: Output values
- `provider.tf`: Provider configuration

The IAM configuration follows the principle of least privilege, with specific service accounts and custom roles for different components of the pipeline.

## Deployment Scripts

The `scripts` directory contains shell scripts for deploying and managing the infrastructure.

### bootstrap.sh

Initializes the GCP project and creates foundational resources required before Terraform can be applied.

### deploy.sh

Main deployment script that orchestrates the deployment of infrastructure and application components to different environments (dev, staging, prod). The script handles:

- Terraform deployment for infrastructure
- Kubernetes resource application
- Helm chart installation
- Post-deployment validation

Usage:
```bash
./deploy.sh --project-id=PROJECT_ID [options]
```

Options:
- `--project-id PROJECT_ID`: GCP project ID (required)
- `--environment, -e ENV`: Target environment (dev, staging, prod)
- `--region, -r REGION`: GCP region
- `--skip-terraform`: Skip Terraform deployment
- `--skip-k8s`: Skip Kubernetes deployment
- `--dry-run`: Perform a dry run without making changes

### rollback.sh

Performs rollback operations in case of deployment failures.

### monitoring_setup.sh

Sets up monitoring dashboards, alerts, and notification channels.

### db-migration.sh

Handles database schema migrations and data migrations.

## Architecture Diagrams

The `diagrams` directory contains visual representations of the infrastructure architecture:

### Available Diagrams

- `system-architecture.png`: Overall system architecture
- `network-diagram.png`: Network topology and connectivity
- `data-flow.png`: Data flow through the pipeline components
- `security-architecture.png`: Security controls and boundaries
- `infrastructure.drawio`: Source diagram file (draw.io format)

## Getting Started

Follow these steps to deploy the infrastructure:

### Prerequisites

1. Google Cloud SDK installed and configured
2. Terraform (v1.0.0+) installed
3. kubectl installed
4. Helm (v3.0.0+) installed
5. Access to the GCP project with appropriate permissions

### Initial Setup

1. Clone the repository
2. Navigate to the infrastructure directory
3. Run the bootstrap script to initialize the project:
   ```bash
   ./scripts/bootstrap.sh --project-id=YOUR_PROJECT_ID
   ```

### Deployment

Deploy to the development environment:
```bash
./scripts/deploy.sh --project-id=YOUR_PROJECT_ID --environment=dev
```

Deploy to staging or production:
```bash
./scripts/deploy.sh --project-id=YOUR_PROJECT_ID --environment=staging
./scripts/deploy.sh --project-id=YOUR_PROJECT_ID --environment=prod
```

### Validation

After deployment, validate the infrastructure:

1. Check GKE cluster status:
   ```bash
   gcloud container clusters list
   ```

2. Verify Kubernetes deployments:
   ```bash
   kubectl get pods -n self-healing-pipeline-dev
   ```

3. Access the application UI (development environment):
   ```
   https://pipeline-dev.YOUR_PROJECT_ID.example.com
   ```

## Environment Configuration

The infrastructure supports multiple environments with different configurations:

### Development (dev)

- Minimal resource allocation
- Debug logging enabled
- Less stringent security controls
- Fast deployment cycle

### Staging

- Production-like configuration
- Realistic data volumes
- Full security controls
- Used for integration testing and UAT

### Production (prod)

- Maximum reliability and security
- Optimized resource allocation
- Strict change management
- High availability configuration

### Configuration Files

Environment-specific configurations are defined in:

- Terraform: `terraform/*/env/*.tfvars`
- Kubernetes: `k8s/overlays/*/`
- Helm: `helm/self-healing-pipeline/values-*.yaml`

## Maintenance and Operations

Guidelines for maintaining and operating the infrastructure:

### Scaling

- GKE node pools will automatically scale based on workload
- Adjust min/max node counts in Terraform variables for different environments
- Monitor resource utilization and adjust limits as needed

### Updates and Upgrades

- GKE cluster: Use the maintenance window defined in Terraform
- Application components: Deploy new versions using the deployment script
- Dependencies: Update Helm chart versions in Chart.yaml

### Monitoring

- Access monitoring dashboards in Google Cloud Console
- Configure alerts using the monitoring_setup.sh script
- Review logs in Cloud Logging

### Backup and Recovery

- GKE: Automatic backup configured through Terraform
- Application data: Regular backups to Cloud Storage
- Disaster recovery: Follow procedures in the operations documentation

## Security Best Practices

Security recommendations for the infrastructure:

### Access Control

- Use Workload Identity for service authentication
- Apply least privilege principle for IAM roles
- Regularly review and rotate service account keys
- Enable VPC Service Controls for sensitive data

### Network Security

- Keep GKE clusters private
- Use authorized networks for master access
- Implement network policies for pod-to-pod communication
- Enable Private Google Access for all subnets

### Container Security

- Enable Binary Authorization for container verification
- Use Container Analysis for vulnerability scanning
- Implement secure base images with minimal attack surface
- Apply security context constraints in Kubernetes

### Data Protection

- Enable encryption at rest for all storage
- Use Customer-Managed Encryption Keys (CMEK) for sensitive data
- Implement column-level security in BigQuery
- Apply data loss prevention policies

## Troubleshooting

Common issues and their solutions:

### Deployment Failures

- Check deployment logs: `./scripts/deploy.sh --project-id=YOUR_PROJECT_ID --environment=dev`
- Verify Terraform state: `cd terraform/gke && terraform state list`
- Check Kubernetes events: `kubectl get events -n self-healing-pipeline-dev`

### Connectivity Issues

- Verify VPC and subnet configuration
- Check firewall rules for necessary traffic
- Ensure Private Google Access is enabled for API access
- Validate service account permissions

### Performance Problems

- Review resource allocation in node pools
- Check for resource contention in Kubernetes
- Analyze BigQuery query performance
- Monitor network throughput and latency

### Security Alerts

- Investigate audit logs for unauthorized access
- Check for policy violations in Security Command Center
- Review IAM permission changes
- Scan containers for vulnerabilities

## Contributing

Guidelines for contributing to the infrastructure code:

### Development Workflow

1. Create a feature branch from main
2. Make changes to infrastructure code
3. Test changes in a development environment
4. Submit a pull request for review
5. Address review comments
6. Merge to main after approval

### Testing Changes

- Use `--dry-run` flag with deployment scripts
- Test in isolated development environments
- Validate changes with `terraform plan`
- Use `kubectl apply --dry-run=client` for Kubernetes changes

### Documentation

- Update README.md files with any infrastructure changes
- Document new components or configurations
- Update architecture diagrams as needed
- Provide examples for new features

## References

Additional resources and documentation:

### Internal Documentation

- [System Architecture Overview](../docs/architecture/overview.md)
- [Deployment Guide](../docs/operations/deployment.md)
- [Disaster Recovery Procedures](../docs/architecture/disaster-recovery.md)
- [Security Architecture](../docs/architecture/security.md)

### External Documentation

- [Google Kubernetes Engine Documentation](https://cloud.google.com/kubernetes-engine/docs)
- [Terraform Google Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Helm Documentation](https://helm.sh/docs/)