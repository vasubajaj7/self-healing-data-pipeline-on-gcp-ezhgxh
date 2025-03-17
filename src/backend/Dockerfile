# Builder stage
FROM python:3.9-slim AS builder

# Set working directory
WORKDIR /app

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements files
COPY requirements*.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    if [ -f requirements-dev.txt ]; then pip install --no-cache-dir -r requirements-dev.txt; fi && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Final stage
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/service-account.json
ENV ENV=production

# Install Google Cloud SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    gnupg \
    curl \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    google-cloud-sdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install runtime dependencies
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

# Copy source code
COPY --from=builder /app/src /app/src

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create directory structure and set permissions
RUN mkdir -p /app/secrets /app/configs && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose API service port
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["gunicorn"]
CMD ["--bind", "0.0.0.0:8080", "--workers", "4", "--timeout", "120", "src.backend.api.app:app"]