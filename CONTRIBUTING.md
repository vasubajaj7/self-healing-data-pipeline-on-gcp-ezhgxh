# Contributing to the Self-Healing Data Pipeline

Thank you for your interest in contributing to the Self-Healing Data Pipeline project! This document provides guidelines and instructions for contributing to the project. By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Table of Contents
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Requirements](#testing-requirements)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Security Vulnerabilities](#security-vulnerabilities)
- [Community](#community)

## Getting Started

### Project Overview

The Self-Healing Data Pipeline is an end-to-end solution for BigQuery using Google Cloud services and AI-driven automation that minimizes manual intervention through intelligent monitoring and autonomous correction. For a comprehensive overview of the project, please refer to the project documentation.

### Ways to Contribute

There are many ways to contribute to the project:

- Reporting bugs and issues
- Suggesting new features or enhancements
- Improving documentation
- Writing code and submitting pull requests
- Reviewing pull requests
- Participating in discussions
- Helping with testing

### First-Time Contributors

If you're new to the project, we recommend starting with issues labeled `good-first-issue` or `help-wanted`. These issues are specifically curated for new contributors and provide a good entry point to the project.

## Development Environment

### Prerequisites

Before you begin, ensure you have the following prerequisites:

- Python 3.9+
- Node.js 16+
- Docker
- Git
- Google Cloud SDK
- Terraform 1.0+
- A Google Cloud account with billing enabled

### Setup Instructions

Detailed instructions for setting up your development environment can be found in [docs/development/setup.md](docs/development/setup.md). This guide includes:

1. Cloning the repository
2. Setting up Python and Node.js environments
3. Configuring Google Cloud credentials
4. Setting up local development tools
5. Running the application locally

### Development Tools

We recommend the following tools for development:

- **IDE**: Visual Studio Code with Python and TypeScript extensions
- **API Testing**: Postman or Insomnia
- **Database Tools**: BigQuery Web UI or a compatible SQL client
- **Git Client**: Command line or GitHub Desktop

## Development Workflow

We follow a GitHub Flow workflow:

1. **Fork the Repository** (if you're an external contributor)
   ```bash
   # Clone your fork
   git clone https://github.com/your-username/self-healing-pipeline.git
   cd self-healing-pipeline
   
   # Add the upstream repository as a remote
   git remote add upstream https://github.com/original-org/self-healing-pipeline.git
   ```

2. **Create a Branch**
   ```bash
   # Ensure you're on the main branch and up-to-date
   git checkout main
   git pull upstream main
   
   # Create a new branch for your feature or bugfix
   git checkout -b feature/your-feature-name
   ```

3. **Make Changes**
   - Write your code following the coding standards
   - Add or update tests as necessary
   - Update documentation to reflect your changes

4. **Commit Your Changes**
   ```bash
   # Stage your changes
   git add .
   
   # Commit with a descriptive message following conventional commits
   git commit -m "feat(component): add new feature"
   ```

5. **Push to Your Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Submit a Pull Request**
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your branch and provide a description of your changes
   - Reference any related issues

7. **Code Review**
   - Address any feedback from reviewers
   - Make additional commits as needed
   - Update your pull request

8. **Merge**
   - Once approved, your pull request will be merged
   - Delete your branch after merging

## Coding Standards

We maintain strict coding standards to ensure code quality, maintainability, and consistency. Please follow these standards when contributing code.

### General Principles

- Write clean, readable, and maintainable code
- Follow the principle of least surprise
- Keep functions and methods focused and concise
- Use meaningful names for variables, functions, and classes
- Write code that is testable and well-tested

### Language-Specific Standards

- **Python**: Follow PEP 8 with our project-specific modifications
- **TypeScript/JavaScript**: Follow our ESLint and Prettier configurations
- **SQL**: Follow our SQL style guide for BigQuery queries
- **Terraform**: Follow HashiCorp's Terraform style conventions

Detailed coding standards can be found in [docs/development/coding-standards.md](docs/development/coding-standards.md).

### Automated Formatting

We use the following tools to enforce coding standards:

- **Python**: Black, isort, flake8, mypy
- **TypeScript/JavaScript**: ESLint, Prettier
- **Terraform**: terraform fmt, tflint

We recommend setting up pre-commit hooks to automatically format your code before committing:

```bash
pip install pre-commit
pre-commit install
```

## Testing Requirements

All code contributions must include appropriate tests. We have several types of tests in the project:

### Unit Tests

- Required for all new code
- Should test individual functions and classes in isolation
- Should cover both normal operation and error cases
- Aim for at least 85% code coverage for backend code
- Aim for at least 80% code coverage for frontend code

### Integration Tests

- Required for features that interact with external systems
- Should test the integration between components
- Should use mocks for external dependencies when appropriate

### End-to-End Tests

- Required for major features
- Should test complete user workflows
- Should run against a test environment

### Running Tests

```bash
# Backend unit tests
cd src/backend
pytest

# Frontend unit tests
cd src/web
npm test

# Integration tests
cd src/test
pytest -xvs integration/

# End-to-end tests
cd src/test/e2e
npm run cypress:open
```

### Continuous Integration

All pull requests are automatically tested by our CI system. The following checks must pass before a pull request can be merged:

- Linting and formatting checks
- Unit tests
- Integration tests
- Code coverage requirements
- Security scanning

## Documentation

Good documentation is essential for the project. Please follow these guidelines for documentation:

### Code Documentation

- Document all modules, classes, and functions
- Use docstrings for Python code
- Use JSDoc for JavaScript/TypeScript code
- Explain complex algorithms and business logic

### User Documentation

- Update user guides when adding or changing features
- Provide examples for new functionality
- Ensure documentation is clear and accessible

### README Files

- Each major directory should have a README.md file
- READMEs should explain the purpose and contents of the directory
- Include usage examples where appropriate

### Documentation Format

- Use Markdown for all documentation
- Follow our Markdown style guidelines
- Use relative links for internal references
- Include diagrams where they help explain concepts (Mermaid diagrams preferred)

## Pull Request Process

### Creating a Pull Request

1. Ensure your code follows our coding standards
2. Ensure all tests pass locally
3. Update documentation as necessary
4. Create a pull request using the [pull request template](.github/PULL_REQUEST_TEMPLATE.md)
5. Fill out all relevant sections of the template
6. Reference any related issues

### Pull Request Requirements

All pull requests must meet the following requirements:

- Pass all CI checks
- Include appropriate tests
- Include updated documentation
- Follow coding standards
- Be reviewed by at least one maintainer
- Address all review comments

### Code Review Process

1. Maintainers will review your pull request within 2-3 business days
2. Reviewers will provide feedback and suggestions
3. Address all feedback and make necessary changes
4. Request re-review after addressing feedback
5. Once approved, a maintainer will merge your pull request

### After Merge

- Delete your branch after it's merged
- Monitor the CI/CD pipeline to ensure your changes deploy successfully
- Help address any issues that arise from your changes

## Issue Reporting

### Bug Reports

If you find a bug, please report it using the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md). Include as much detail as possible:

- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Screenshots or logs if applicable
- Environment information

### Feature Requests

If you have an idea for a new feature, please submit it using the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md). Include:

- A clear description of the feature
- The problem it solves
- Potential implementation approaches
- Business value or benefits

### Issue Labels

We use the following labels to categorize issues:

- `bug`: Something isn't working as expected
- `enhancement`: New feature or improvement
- `documentation`: Documentation improvements
- `good-first-issue`: Good for newcomers
- `help-wanted`: Extra attention is needed
- `question`: Further information is requested
- `wontfix`: This will not be worked on

## Security Vulnerabilities

If you discover a security vulnerability, please do NOT open an issue. Security vulnerabilities should be reported privately to maintain security for all users.

Please follow the guidelines in our [Security Policy](SECURITY.md) for reporting security vulnerabilities.

## Community

### Communication Channels

- **GitHub Discussions**: For general questions and discussions
- **Issue Tracker**: For bug reports and feature requests
- **Slack Channel**: For real-time communication (invitation available upon request)

### Code of Conduct

We expect all contributors to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before participating in the project.

### Recognition

All contributors will be recognized in our contributors list. We value and appreciate all contributions, regardless of size.

### Getting Help

If you need help with contributing, you can:

- Ask in GitHub Discussions
- Reach out on Slack
- Comment on the relevant issue
- Contact the maintainers directly

We're here to help you make successful contributions to the project!

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project. See the LICENSE file for details.