# Use AWS Lambda Python runtime with explicit platform for ECS
FROM --platform=linux/x86_64 public.ecr.aws/lambda/python:3.11

# Set environment variables
ENV PYTHONBUFFERED=1 \
    POETRY_HOME="/opt/poetry" \
    PATH="/opt/poetry/bin:$PATH" \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH="/app"

WORKDIR /app

# Install poetry with error handling
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    poetry --version

# Debug: List contents of build context
RUN ls -la

# Copy dependency files and observability wheel
COPY pyproject.toml /app/
COPY wheels/ /app/wheels/

# Install dependencies (no dev dependencies)
RUN poetry install --only main --no-interaction --no-cache --no-root || exit 1

# Copy source code
COPY src/ /app/

# Create necessary directories with proper permissions
RUN mkdir -p /app/data && \
    chmod 755 /app/data

# Lambda handler will be configured through environment variables
ENV HANDLER_MODULE="" \
    HANDLER_CLASS=""

# Health check for ECS
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health')"

ENTRYPOINT ["python", "-m", "runner"]
