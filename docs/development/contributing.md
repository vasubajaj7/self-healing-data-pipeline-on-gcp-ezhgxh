# Contributing to the Self-Healing Data Pipeline

This document provides guidelines for contributing to the Self-Healing Data Pipeline project. We welcome contributions from all team members and external collaborators. By following these guidelines, you'll help maintain the quality and consistency of the codebase.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Branching Strategy](#branching-strategy)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Code Review Guidelines](#code-review-guidelines)
- [Testing Requirements](#testing-requirements)
- [Documentation](#documentation)
- [Issue Tracking](#issue-tracking)
- [Release Process](#release-process)
- [Community Contributions](#community-contributions)

## Code of Conduct

All contributors are expected to adhere to the project's [Code of Conduct](../../CODE_OF_CONDUCT.md). Please read it before participating in the project.

## Getting Started

### Prerequisites

Before you begin contributing, ensure you have:

1. Read the [Setup Guide](./setup.md) and configured your development environment
2. Familiarized yourself with the [Coding Standards](./coding-standards.md)
3. Understood the [Testing Guide](./testing.md) requirements
4. Reviewed the CI/CD pipeline documentation to understand the deployment process

### First-Time Contributors

If you're contributing for the first time:

1. Fork the repository (if you're an external contributor)
2. Clone the repository locally
3. Set up the development environment following the [Setup Guide](./setup.md)
4. Create a new branch for your work
5. Make your changes following the guidelines in this document
6. Submit a pull request

## Development Workflow

The development workflow follows these general steps:

1. **Issue Assignment**: Pick an issue to work on from the issue tracker or create a new one
2. **Branch Creation**: Create a feature/bugfix branch from the appropriate base branch
3. **Development**: Make changes following the coding standards
4. **Testing**: Write and run tests to verify your changes
5. **Documentation**: Update documentation to reflect your changes
6. **Pull Request**: Submit a pull request for review
7. **Code Review**: Address feedback from reviewers
8. **Merge**: Once approved, your changes will be merged

### Local Development

For local development:

```bash
# Clone the repository
git clone https://github.com/your-org/self-healing-pipeline.git
cd self-healing-pipeline

# Set up the development environment
# Follow instructions in setup.md

# Create a new branch
git checkout -b feature/your-feature-name

# Make your changes
# ...

# Run tests locally
src/test/scripts/run_unit_tests.sh

# Commit your changes
git add .
git commit -m "feat(component): add new feature"

# Push your branch
git push origin feature/your-feature-name
```

## Branching Strategy

We follow a trunk-based development approach with feature branches:

### Branch Naming

Branches should be named according to the following pattern:

```
<type>/<description>
```

Where `<type>` is one of:
- `feature`: For new features
- `bugfix`: For bug fixes
- `hotfix`: For critical fixes to production
- `docs`: For documentation changes only
- `refactor`: For code refactoring without changing functionality
- `test`: For adding or modifying tests
- `chore`: For maintenance tasks

And `<description>` is a brief, hyphenated description of the change.

Examples:
- `feature/add-data-quality-dashboard`
- `bugfix/fix-null-handling-in-transform`
- `docs/update-api-documentation`
- `refactor/optimize-query-performance`

### Main Branches

- `main`: Production-ready code
- `develop`: Integration branch for features

### Feature Development

1. Create feature branches from `develop`
2. Develop and test your feature
3. Submit a pull request to merge back into `develop`

### Hotfixes

1. Create hotfix branches from `main`
2. Fix the critical issue
3. Submit pull requests to both `main` and `develop`

## Commit Guidelines

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages. This leads to more readable messages that are easy to follow when looking through the project history.

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Where:
- `<type>`: The type of change (see below)
- `<scope>`: The scope of the change (optional)
- `<subject>`: A short description of the change
- `<body>`: A more detailed description (optional)
- `<footer>`: Information about breaking changes or issue references (optional)

### Types

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation changes
- `style`: Changes that don't affect code functionality (formatting, etc.)
- `refactor`: Code changes that neither fix bugs nor add features
- `perf`: Performance improvements
- `test`: Adding or correcting tests
- `chore`: Changes to the build process, tools, etc.

### Examples

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

### Pre-commit Hooks

We use pre-commit hooks to ensure commit message format and code quality. Install them with:

```bash
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```

## Pull Request Process

Pull requests are the primary method for contributing code to the project.

### Creating a Pull Request

1. Push your branch to the remote repository
2. Go to the repository on GitHub and click "New Pull Request"
3. Select your branch and the target branch (usually `develop`)
4. Fill out the pull request template with all required information
5. Assign reviewers and relevant labels

### Pull Request Template

The pull request template includes sections for:
- Description of changes
- Related issues
- Type of change
- Changes made (added, changed, removed)
- Testing performed
- Screenshots (if applicable)
- Checklist of requirements

Ensure you complete all sections of the template to facilitate the review process.

### Review Process

1. Automated checks will run on your pull request
2. Reviewers will provide feedback on your code
3. Address all feedback by making additional commits to your branch
4. Once all reviews are approved and checks pass, your PR can be merged

### Merge Requirements

Before a pull request can be merged, it must meet these requirements:

- Pass all automated CI checks
- Receive approval from at least one reviewer
- Meet code coverage requirements (85% for backend, 80% for frontend)
- Have no unresolved comments
- Pass all security scans
- Be up-to-date with the target branch

## Code Review Guidelines

Code reviews are a critical part of our development process. They help maintain code quality, share knowledge, and catch issues early.

### For Authors

- Keep pull requests focused and reasonably sized
- Provide context and explain your approach in the PR description
- Be responsive to feedback and questions
- Address all comments before requesting re-review
- Be open to suggestions and alternative approaches

### For Reviewers

- Review code within 2 business days
- Be respectful and constructive in comments
- Focus on the code, not the person
- Consider both the technical implementation and user experience
- Verify that the code meets project standards
- Check for edge cases and potential issues
- Ensure appropriate test coverage
- Approve only when all concerns have been addressed

### Review Checklist

- Does the code follow the project's coding standards?
- Is the code well-structured and maintainable?
- Are there appropriate tests with good coverage?
- Is the documentation complete and accurate?
- Are there any security concerns?
- Are there any performance issues?
- Is error handling implemented appropriately?
- Are there any edge cases not covered?

## Testing Requirements

All code contributions must include appropriate tests. Refer to the [Testing Guide](./testing.md) for detailed information on our testing approach.

### Testing Expectations

- **Unit Tests**: Required for all new code and modified functionality
- **Integration Tests**: Required for components that interact with other parts of the system
- **End-to-End Tests**: Required for user-facing features
- **Performance Tests**: Required for performance-critical components

### Coverage Requirements

- Backend code: Minimum 85% test coverage
- Frontend code: Minimum 80% test coverage
- Critical components: Minimum 90% test coverage

### Running Tests Locally

```bash
# Run backend unit tests
cd src/backend
pytest

# Run frontend unit tests
cd src/web
npm test

# Run integration tests
cd src/test
pytest -xvs integration/

# Run end-to-end tests
cd src/test/e2e
npm run cypress:open
```

### Test Quality

Tests should be:
- Focused on behavior, not implementation details
- Independent and idempotent
- Fast and reliable
- Easy to understand and maintain

## Documentation

Documentation is a crucial part of the project. All code contributions should include appropriate documentation updates.

### Types of Documentation

- **Code Documentation**: Docstrings, comments, and type hints
- **API Documentation**: Endpoint descriptions, request/response formats
- **User Guides**: How-to guides for using the system
- **Architecture Documentation**: System design and component interactions
- **Development Documentation**: Guidelines for developers

### Documentation Standards

- Use clear, concise language
- Include examples where appropriate
- Keep documentation up-to-date with code changes
- Follow the project's documentation style guide
- Use proper Markdown formatting

### README Files

Each major directory should include a README.md file that explains:
- The purpose of the directory
- Key components and their functions
- How to use or contribute to the components
- Any special considerations or requirements

## Issue Tracking

We use GitHub Issues to track bugs, features, and other tasks.

### Creating Issues

When creating a new issue:

1. Check if a similar issue already exists
2. Use a clear, descriptive title
3. Provide detailed information about the issue
4. Include steps to reproduce for bugs
5. Add appropriate labels
6. Assign to the relevant milestone if applicable

### Issue Templates

Use the provided issue templates for:
- Bug reports
- Feature requests
- Documentation improvements
- Performance issues

### Issue Labels

Common labels include:
- `bug`: Something isn't working as expected
- `feature`: New functionality
- `enhancement`: Improvements to existing functionality
- `documentation`: Documentation-related issues
- `good first issue`: Good for newcomers
- `help wanted`: Extra attention is needed
- `priority: high/medium/low`: Indicates priority level

### Working on Issues

1. Assign yourself to an issue before starting work
2. Reference the issue number in your branch name if possible
3. Mention the issue number in your commit messages
4. Close issues automatically from pull requests using keywords (e.g., "Fixes #123")

## Release Process

Our release process follows these general steps:

1. **Feature Freeze**: All features for the release are merged to `develop`
2. **Release Branch**: Create a `release/vX.Y.Z` branch from `develop`
3. **Stabilization**: Fix bugs and prepare release notes
4. **Testing**: Perform final testing on the release branch
5. **Merge to Main**: Merge the release branch to `main` and tag the release
6. **Deploy**: Deploy the release to production
7. **Sync Back**: Merge `main` back to `develop`

### Versioning

We follow [Semantic Versioning](https://semver.org/) (SemVer) for releases:

- **Major version (X)**: Incompatible API changes
- **Minor version (Y)**: Backwards-compatible new functionality
- **Patch version (Z)**: Backwards-compatible bug fixes

### Release Notes

Release notes should include:
- Summary of the release
- New features
- Bug fixes
- Breaking changes
- Deprecations
- Upgrade instructions if needed

## Community Contributions

We welcome contributions from the community. External contributors should follow the same guidelines as team members, with a few additional considerations.

### For External Contributors

1. **Fork the Repository**: Create your own fork of the repository
2. **Create a Branch**: Make your changes in a branch on your fork
3. **Submit a Pull Request**: Submit a PR from your fork to the main repository
4. **Sign CLA**: If required, sign the Contributor License Agreement

### Communication Channels

- GitHub Issues: For bug reports and feature requests
- GitHub Discussions: For general questions and discussions
- Project Chat: For real-time communication (if available)

### Recognition

All contributors will be recognized in the project's contributors list. Significant contributions may be highlighted in release notes.

## Additional Resources

- [Project Documentation](../README.md)
- [Coding Standards](./coding-standards.md)
- [Setup Guide](./setup.md)
- [Testing Guide](./testing.md)
- [Architecture Overview](../architecture/overview.md)

## Continuous Integration and Deployment

Our project uses automated CI/CD pipelines to ensure code quality and streamline deployment.

### CI Process

When you submit a pull request, the CI system will automatically:

1. Run linting and code style checks
2. Execute unit and integration tests
3. Calculate test coverage
4. Perform security scans
5. Build the application

### CD Process

After merging to the main branch, the CD pipeline will:

1. Build the application for the target environment
2. Run final validation tests
3. Deploy to the appropriate environment (dev, staging, or production)
4. Perform post-deployment verification

### Pipeline Status

You can check the status of CI/CD pipelines in the GitHub Actions tab of the repository. Green checks indicate passing builds, while red X marks indicate failures that need attention.