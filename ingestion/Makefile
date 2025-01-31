.PHONY: install test test-unit test-integration test-liveheats test-graphql build clean format lint check test-graphql-handler build-deps test-graphql-unit

# Default target platform
TARGET_PLATFORM ?= dev_platform
DATA_DIR ?= output_test_data

install:
	poetry install

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".eggs" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -delete
	find . -type f -name ".coverage" -delete
	docker rmi ingestion-test --force
	rm -rf $(DATA_DIR)

test-unit:
	@echo "Running unit tests..."
	poetry run pytest tests/unit

test-integration:
	@echo "Running integration tests with TARGET_PLATFORM=$(TARGET_PLATFORM)"
	TARGET_PLATFORM=$(TARGET_PLATFORM) poetry run pytest tests/integration

test-liveheats:
	@echo "Running LiveHeats integration test for platform: $(TARGET_PLATFORM)"
	@if [ ! -f "../.platform_config/$(TARGET_PLATFORM)/.env.liveheats" ]; then \
		echo "Error: LiveHeats environment file not found at ../.platform_config/$(TARGET_PLATFORM)/.env.liveheats"; \
		echo "Please copy .env.liveheats.example to .env.liveheats and fill in the values"; \
		exit 1; \
	fi
	@echo "Loading environment from ../.platform_config/$(TARGET_PLATFORM)/.env.liveheats"
	@set -a && . "../.platform_config/$(TARGET_PLATFORM)/.env.liveheats" && set +a && \
	HANDLER_NAME=LiveHeatsOrganisations \
	HANDLER_TYPE=graphql \
	TARGET_PLATFORM=$(TARGET_PLATFORM) \
	poetry run pytest tests/integration/liveheats/test_liveheats_orgs_integration.py -v

test-graphql:
	@echo "Creating data directory..."
	@mkdir -p $(DATA_DIR)
	@echo "Building ingestion container..."
	@make build
	@echo "Running GraphQL handler test..."
	docker run --rm \
		-v $(PWD)/$(DATA_DIR):/app/data \
		-e HANDLER_MODULE=ingestion.handlers.graphql.graphql_handler \
		-e HANDLER_CLASS=GraphQLHandler \
		-e TARGET_PLATFORM=dev_platform \
		-e AWS_REGION=ap-southeast-2 \
		-e GRAPHQL_URL=$${GRAPHQL_URL} \
		-e AUTH_URL=$${AUTH_URL} \
		-e CLIENT_ID=$${CLIENT_ID} \
		-e CLIENT_SECRET=$${CLIENT_SECRET} \
		-e SCOPE=$${SCOPE} \
		-e QUERY='query getOrganisationAthletes($$id: ID!, $$page: Int!, $$per: Int!) { organisationAthletes(id: $$id, page: $$page, per: $$per) { athletes { id name dob properties users { phone } memberships { id expired createdAt expiryDate organisation { id name } } } totalCount } }' \
		-e VARIABLES='{"id": 78, "page": 1, "per": 100}' \
		-e SINK_CONFIG='{"type": "local", "base_path": "/app/data", "key_prefix": "athletes", "format": "json"}' \
		ingestion-test

test-graphql-unit:
	@echo "Running GraphQL-specific unit tests..."
	@( \
		set -a && \
		source .platform_config/$(TARGET_PLATFORM)/.env && \
		cd ingestion && poetry run pytest tests/unit/handlers/graphql/test_graphql_handler.py$(if $(TEST)"::$(TEST)") -v  \
	)
	poetry run pytest \
		tests/unit/handlers/graphql/test_graphql_handler.py \
		tests/unit/utils/clients/test_graphql_client.py \
		tests/unit/utils/clients/test_clickhouse_client.py \
		-v --cov=ingestion.handlers.graphql \
		--cov=ingestion.utils.graphql_client \
		--cov=ingestion.utils.clickhouse_client \
		--cov-report=term-missing

test-graphql-handler: test-graphql-unit
	@echo "Running GraphQL handler integration tests..."
	@if [ ! -f "../.platform_config/$(TARGET_PLATFORM)/.env.liveheats" ]; then \
		echo "Error: LiveHeats environment file not found at ../.platform_config/$(TARGET_PLATFORM)/.env.liveheats"; \
		exit 1; \
	fi
	@echo "Starting test environment..."
	cd tests/integration/handlers && docker-compose -f docker-compose.test.yml up --build -d
	@echo "Running tests..."
	poetry run pytest tests/integration/handlers/test_graphql_handler_integration.py -v
	@echo "Cleaning up..."
	cd tests/integration/handlers && docker-compose -f docker-compose.test.yml down

build:
	docker build --no-cache -t ingestion-test .

build-deps:
	@echo "Building dependencies..."
	cd ../observability && poetry build
	@mkdir -p wheels/ingestion/wheels
	@cp ../observability/dist/*.whl wheels/ingestion/wheels/
	poetry lock --no-update
	poetry install

test: test-unit test-graphql-handler

format:
	poetry run black .
	poetry run isort .

lint:
	poetry run pylint src tests
	poetry run mypy src tests

check: format lint test
