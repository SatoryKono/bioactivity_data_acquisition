# Multi-stage Dockerfile for bioactivity-data-acquisition
# Stage 1: Base image with Python and system dependencies
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd --gid 1000 bioactivity && \
    useradd --uid 1000 --gid bioactivity --shell /bin/bash --create-home bioactivity

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY pyproject.toml requirements.txt ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install .[dev]

# Stage 2: Development image
FROM base as development

# Install additional development tools
RUN pip install --no-deps \
    ipython \
    jupyter \
    pre-commit

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/
COPY configs/ ./configs/
COPY docs/ ./docs/
COPY .pre-commit-config.yaml ./
COPY mkdocs.yml ./

# Copy scripts and configs
COPY scripts/ ./scripts/
COPY .github/ ./.github/

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Default command for development
CMD ["bash"]

# Stage 3: Production image
FROM base as production

# Copy only necessary files for production
COPY src/ ./src/
COPY configs/ ./configs/

# Install production dependencies only
RUN pip uninstall -y pytest pytest-cov mypy ruff black pre-commit && \
    pip install --no-deps .

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from library.cli import app; print('Health check passed')" || exit 1

# Default command for production
ENTRYPOINT ["python", "-m", "library.cli"]

# Stage 4: CI image
FROM base as ci

# Install CI-specific dependencies including security tools
RUN pip install --no-deps \
    pytest-xdist \
    pytest-benchmark \
    pytest-mock \
    safety \
    bandit

# Copy all source code and tests
COPY . .

# Copy security configuration files
COPY .bandit .bandit
COPY .banditignore .banditignore
COPY .safety_policy.yaml .safety_policy.yaml

# Set ownership to non-root user
RUN chown -R bioactivity:bioactivity /app

# Switch to non-root user
USER bioactivity

# Default command for CI
CMD ["pytest", "--cov=library", "--cov-report=xml", "--cov-report=term-missing"]
