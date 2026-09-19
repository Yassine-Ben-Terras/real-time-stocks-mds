.DEFAULT_GOAL := help
SHELL := /bin/bash
DBT := dbt --project-dir dbt_stocks --profiles-dir dbt_stocks

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- setup
.PHONY: install
install: ## Install dev dependencies and git hooks
	pip install -r requirements/dev.txt
	pre-commit install

.PHONY: env
env: ## Create .env from the template if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env -- fill it in")

.PHONY: keypair
keypair: ## Generate the Snowflake key pair for Snowpipe Streaming
	./scripts/generate_snowflake_keypair.sh

# ------------------------------------------------------------------ run
.PHONY: up
up: env ## Start the whole stack
	docker compose up -d --build
	@echo "Kafka UI      http://localhost:8081"
	@echo "Kafka Connect http://localhost:8083"
	@echo "MinIO console http://localhost:9001"
	@echo "Airflow       http://localhost:8080"

.PHONY: connectors
connectors: ## Register the Snowflake and MinIO sink connectors
	./scripts/register_connectors.sh

.PHONY: down
down: ## Stop the stack
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete all volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail producer and connect logs
	docker compose logs -f producer connect

.PHONY: status
status: ## Show connector status
	@curl -s localhost:8083/connectors?expand=status | python3 -m json.tool

# ------------------------------------------------------------------ dbt
.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	$(DBT) deps

.PHONY: dbt-build
dbt-build: ## Build and test every model
	$(DBT) build

.PHONY: dbt-test
dbt-test: ## Run tests only
	$(DBT) test

.PHONY: dbt-fresh
dbt-fresh: ## Check source freshness
	$(DBT) source freshness

.PHONY: dbt-docs
dbt-docs: ## Generate and serve docs
	$(DBT) docs generate && $(DBT) docs serve

# ---------------------------------------------------------------- checks
.PHONY: lint
lint: ## Run every linter
	ruff check .
	ruff format --check .
	sqlfluff lint dbt_stocks/models dbt_stocks/tests --dialect snowflake
	yamllint -c .yamllint.yml .

.PHONY: fmt
fmt: ## Auto-format python and sql
	ruff check --fix .
	ruff format .
	sqlfluff fix dbt_stocks/models dbt_stocks/tests --dialect snowflake

.PHONY: test
test: ## Run unit tests with coverage
	pytest tests/unit --cov=producer --cov-report=term-missing

.PHONY: ci
ci: lint test ## Everything CI runs, locally
