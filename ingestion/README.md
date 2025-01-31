# Data Vanguards Platform - Ingestion Service

This service handles data ingestion from various sources into the Data Vanguards Platform.

## Directory Structure

```
ingestion/
├── base/               # Base classes and shared functionality
├── handlers/           # Individual ingestion handlers
│   ├── liveheats/     # LiveHeats data ingestion
│   └── graphql/       # GraphQL data ingestion
├── utils/             # Utility functions and clients
│   ├── aws/          # AWS-specific utilities
│   └── clients/      # API clients
├── docker/            # Docker configuration
├── tests/             # Test suite
└── docker-compose.yml # Local development setup
```

## Development Setup

1. Install dependencies:
```bash
poetry install
```

2. Set up environment variables:
```bash
cp ../.platform_config/dv_enterprise/.env.example ../.platform_config/dv_enterprise/.env
```

3. Start services:
```bash
# Start all services
docker compose up

# Start specific handler
docker compose up liveheats
```

## Adding a New Handler

1. Create a new directory under `handlers/`
2. Implement the handler class extending `BaseHandler`
3. Add handler configuration to `handlers_mapping.yaml`
4. Add Docker service configuration if needed

## Testing

```bash
# Run tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=.

# Run specific test file
poetry run pytest tests/handlers/test_liveheats.py
```

## Deployment

The service is deployed as individual ECS tasks, one for each handler. Configuration is managed through AWS Secrets Manager and environment variables.

## Monitoring

- Metrics are collected and can be viewed in CloudWatch
- Distributed tracing is available through Jaeger (http://localhost:16686 in development)
- Health checks are exposed on each handler's HTTP port
