# Self-Healing Data Pipeline - Makefile
# Central automation script for development, testing, deployment, and maintenance tasks

# Declare PHONY targets (targets that don't represent files)
.PHONY: help setup dev lint format test test-unit test-integration test-e2e test-performance build docker-build docker-push deploy-dev deploy-staging deploy-prod terraform-init terraform-plan terraform-apply clean docs monitoring-setup backup

# Set default target to help
.DEFAULT_GOAL := help

#------------------------------------------------------------------------------
# GENERAL COMMANDS
#------------------------------------------------------------------------------

# Display help information about available make commands
help:
	@echo "Self-Healing Data Pipeline - Available Commands"
	@echo "==============================================="
	@echo ""
	@echo "Setup and Environment:"
	@echo "  setup                  - Initialize development environment with dependencies"
	@echo "  dev                    - Start development environment with hot-reloading"
	@echo "  clean                  - Clean up generated files and build artifacts"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint                   - Run linting checks on all code"
	@echo "  format                 - Format code according to project standards"
	@echo ""
	@echo "Testing:"
	@echo "  test                   - Run all tests"
	@echo "  test-unit              - Run unit tests only"
	@echo "  test-integration       - Run integration tests only"
	@echo "  test-e2e               - Run end-to-end tests"
	@echo "  test-performance       - Run performance tests"
	@echo ""
	@echo "Build and Deployment:"
	@echo "  build                  - Build all components for production"
	@echo "  docker-build           - Build Docker images for all components"
	@echo "  docker-push            - Push Docker images to container registry"
	@echo "  deploy-dev             - Deploy to development environment"
	@echo "  deploy-staging         - Deploy to staging environment"
	@echo "  deploy-prod            - Deploy to production environment"
	@echo ""
	@echo "Infrastructure:"
	@echo "  terraform-init         - Initialize Terraform for infrastructure management"
	@echo "  terraform-plan         - Generate and show Terraform execution plan"
	@echo "  terraform-apply        - Apply Terraform changes to infrastructure"
	@echo "  monitoring-setup       - Set up monitoring and alerting infrastructure"
	@echo ""
	@echo "Documentation and Maintenance:"
	@echo "  docs                   - Generate project documentation"
	@echo "  backup                 - Create backups of critical data"

#------------------------------------------------------------------------------
# SETUP AND DEVELOPMENT
#------------------------------------------------------------------------------

# Initialize the development environment with all dependencies
setup:
	@echo "Setting up development environment..."
	pip install -r requirements.txt
	npm install
	@echo "Initializing configuration files..."
	cp .env.example .env
	@echo "Setting up pre-commit hooks..."
	pre-commit install
	@echo "Creating local directories..."
	mkdir -p data/temp
	mkdir -p logs
	@echo "Setup complete!"

# Start the development environment with hot-reloading
dev:
	@echo "Starting development environment..."
	export FLASK_ENV=development && \
	export FLASK_APP=app.py && \
	python -m flask run --host=0.0.0.0 --port=5000 & \
	cd frontend && npm run dev

#------------------------------------------------------------------------------
# CODE QUALITY
#------------------------------------------------------------------------------

# Run linting checks on all code
lint:
	@echo "Running linters..."
	flake8 .
	pylint --recursive=y .
	cd frontend && npx eslint .
	cd infrastructure && terraform validate

# Format code according to project standards
format:
	@echo "Formatting code..."
	black .
	isort .
	cd frontend && npx prettier --write "**/*.{js,jsx,ts,tsx,json,css,md}"
	cd infrastructure && terraform fmt -recursive

#------------------------------------------------------------------------------
# TESTING
#------------------------------------------------------------------------------

# Run all tests
test: test-unit test-integration
	@echo "Generating test coverage report..."
	coverage combine
	coverage report
	coverage html

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	pytest tests/unit --cov=app --cov-report=term --cov-report=xml:coverage/unit.xml
	cd frontend && npm test -- --coverage

# Run integration tests only
test-integration:
	@echo "Running integration tests..."
	@echo "Setting up test environment..."
	docker-compose -f docker-compose.test.yml up -d
	sleep 5  # Wait for services to be ready
	pytest tests/integration --cov=app --cov-report=term --cov-report=xml:coverage/integration.xml
	@echo "Tearing down test environment..."
	docker-compose -f docker-compose.test.yml down

# Run end-to-end tests
test-e2e:
	@echo "Running end-to-end tests..."
	docker-compose -f docker-compose.test.yml up -d
	sleep 10  # Wait for services to be fully ready
	cd frontend && npx cypress run
	docker-compose -f docker-compose.test.yml down

# Run performance tests
test-performance:
	@echo "Running performance tests..."
	@echo "Setting up performance test environment..."
	docker-compose -f docker-compose.perf.yml up -d
	sleep 10  # Wait for services to be fully ready
	k6 run performance/load_test.js
	@echo "Generating performance report..."
	python scripts/generate_performance_report.py
	docker-compose -f docker-compose.perf.yml down

#------------------------------------------------------------------------------
# BUILD AND DEPLOYMENT
#------------------------------------------------------------------------------

# Build all components for production
build:
	@echo "Building for production..."
	@echo "Building backend components..."
	pip install -e .
	@echo "Building frontend components..."
	cd frontend && npm run build
	@echo "Preparing deployment artifacts..."
	mkdir -p dist
	cp -r frontend/build dist/frontend
	cp -r app dist/app
	@echo "Build complete!"

# Build Docker images for all components
docker-build:
	@echo "Building Docker images..."
	docker build -t self-healing-pipeline-backend:latest -f Dockerfile.backend .
	docker build -t self-healing-pipeline-frontend:latest -f Dockerfile.frontend ./frontend
	@echo "Tagging images with version..."
	VERSION=$$(cat VERSION) && \
	docker tag self-healing-pipeline-backend:latest self-healing-pipeline-backend:$$VERSION && \
	docker tag self-healing-pipeline-frontend:latest self-healing-pipeline-frontend:$$VERSION
	@echo "Docker build complete!"

# Push Docker images to container registry
docker-push:
	@echo "Pushing Docker images to container registry..."
	@echo "Authenticating with container registry..."
	gcloud auth configure-docker gcr.io
	VERSION=$$(cat VERSION) && \
	docker tag self-healing-pipeline-backend:$$VERSION gcr.io/project-id/self-healing-pipeline-backend:$$VERSION && \
	docker tag self-healing-pipeline-frontend:$$VERSION gcr.io/project-id/self-healing-pipeline-frontend:$$VERSION && \
	docker push gcr.io/project-id/self-healing-pipeline-backend:$$VERSION && \
	docker push gcr.io/project-id/self-healing-pipeline-frontend:$$VERSION
	@echo "Docker push complete!"

# Deploy to development environment
deploy-dev: build docker-build docker-push
	@echo "Deploying to development environment..."
	cd infrastructure && \
	terraform workspace select dev && \
	terraform apply -var-file=environments/dev/terraform.tfvars -auto-approve
	@echo "Running post-deployment verification..."
	python scripts/verify_deployment.py --env=dev
	@echo "Deployment to development complete!"

# Deploy to staging environment
deploy-staging: build docker-build docker-push
	@echo "Deploying to staging environment..."
	cd infrastructure && \
	terraform workspace select staging && \
	terraform apply -var-file=environments/staging/terraform.tfvars -auto-approve
	@echo "Running post-deployment verification..."
	python scripts/verify_deployment.py --env=staging
	@echo "Deployment to staging complete!"

# Deploy to production environment
deploy-prod: build docker-build docker-push
	@echo "Deploying to production environment..."
	cd infrastructure && \
	terraform workspace select prod && \
	terraform apply -var-file=environments/prod/terraform.tfvars -auto-approve
	@echo "Running post-deployment verification..."
	python scripts/verify_deployment.py --env=prod
	@echo "Deployment to production complete!"

#------------------------------------------------------------------------------
# INFRASTRUCTURE
#------------------------------------------------------------------------------

# Initialize Terraform for infrastructure management
terraform-init:
	@echo "Initializing Terraform..."
	cd infrastructure && terraform init \
		-backend-config=environments/backend.hcl
	@echo "Terraform initialization complete!"

# Generate and show Terraform execution plan
terraform-plan:
	@echo "Generating Terraform plan..."
	cd infrastructure && terraform plan \
		-var-file=environments/$(ENV)/terraform.tfvars \
		-out=$(ENV).tfplan
	@echo "Terraform plan generated!"

# Apply Terraform changes to infrastructure
terraform-apply:
	@echo "Applying Terraform changes..."
	cd infrastructure && terraform apply \
		-auto-approve $(ENV).tfplan
	@echo "Terraform changes applied!"

# Set up monitoring and alerting infrastructure
monitoring-setup:
	@echo "Setting up monitoring and alerting infrastructure..."
	cd infrastructure/monitoring && \
	terraform apply -auto-approve
	@echo "Applying dashboard and alert configurations..."
	python scripts/setup_monitoring.py
	@echo "Configuring notification channels..."
	python scripts/setup_alerts.py
	@echo "Monitoring setup complete!"

#------------------------------------------------------------------------------
# MAINTENANCE AND UTILITIES
#------------------------------------------------------------------------------

# Clean up generated files and build artifacts
clean:
	@echo "Cleaning up generated files and build artifacts..."
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	rm -rf .coverage coverage.xml coverage/
	cd frontend && rm -rf node_modules dist build .cache
	@echo "Clean complete!"

# Generate project documentation
docs:
	@echo "Generating project documentation..."
	@echo "Generating API documentation..."
	sphinx-apidoc -o docs/source app
	@echo "Generating code documentation..."
	cd docs && make html
	@echo "Documentation generated in docs/build/html/"

# Create backups of critical data
backup:
	@echo "Creating backups of critical data..."
	@echo "Triggering database backup procedures..."
	python scripts/backup_databases.py
	@echo "Exporting configuration data..."
	python scripts/export_configurations.py
	@echo "Storing backups in designated location..."
	gsutil -m cp -r backups/ gs://self-healing-pipeline-backups/$(shell date +%Y-%m-%d)/
	@echo "Backup complete!"