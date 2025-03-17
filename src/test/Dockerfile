# =================================================================
# Test Environment Dockerfile for Self-Healing Data Pipeline Project
# =================================================================
#
# This Dockerfile creates a containerized test environment with all 
# necessary dependencies and tools for running various types of tests:
# - Unit tests
# - Integration tests
# - Performance tests
# - End-to-end tests
# - Security tests
# - Data quality tests
#
# The build uses a multi-stage approach to create an optimized final image
# while maintaining all required testing capabilities.

# Stage 1: Builder stage for installing dependencies and preparing the test environment
FROM python:3.9-slim AS builder

# Set environment variables for Python optimization
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Set working directory
WORKDIR /app

# Install system dependencies required for builds and GCP SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    gnupg \
    lsb-release \
    wget \
    ca-certificates \
    apt-transport-https \
    software-properties-common \
    # Install Google Cloud SDK - version 428.0.0
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update && apt-get install -y \
    google-cloud-sdk \
    google-cloud-sdk-gke-gcloud-auth-plugin \
    google-cloud-sdk-datastore-emulator \
    google-cloud-sdk-pubsub-emulator \
    google-cloud-sdk-bigtable-emulator \
    google-cloud-sdk-firestore-emulator \
    # Install browser dependencies for Playwright - version 1.33.0
    && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    # Clean up to reduce image size
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN mkdir -p /ms-playwright && \
    playwright install --with-deps chromium firefox webkit && \
    cp -R /root/.cache/ms-playwright/* /ms-playwright/

# Stage 2: Final stage with optimized test environment
FROM python:3.9-slim

# Add labels for better documentation
LABEL maintainer="Data Engineering Team" \
      description="Test environment for self-healing data pipeline project" \
      version="1.0"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    TEST_ENV=test \
    GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/service-account.json

# Set working directory
WORKDIR /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    lsb-release \
    ca-certificates \
    apt-transport-https \
    # Install Google Cloud SDK - version 428.0.0
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update && apt-get install -y \
    google-cloud-sdk \
    google-cloud-sdk-gke-gcloud-auth-plugin \
    google-cloud-sdk-datastore-emulator \
    google-cloud-sdk-pubsub-emulator \
    google-cloud-sdk-firestore-emulator \
    # Install browser dependencies for Playwright - version 1.33.0
    && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    # Clean up to reduce image size
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy installed Playwright browsers from builder stage
COPY --from=builder /ms-playwright /ms-playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Copy test configuration
COPY pytest.ini .

# Create non-root user for security
RUN groupadd -r testuser && useradd -r -g testuser testuser

# Create required directories and set appropriate permissions
RUN mkdir -p /app/reports /app/logs /app/secrets && \
    chown -R testuser:testuser /app && \
    chmod -R 755 /ms-playwright

# Switch to non-root user
USER testuser

# Volume configuration for data persistence
VOLUME ["/app", "/app/reports", "/app/logs", "/app/secrets"]

# Set entrypoint and default command for test execution
ENTRYPOINT ["pytest"]
CMD ["--verbose"]