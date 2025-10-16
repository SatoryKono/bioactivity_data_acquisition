# Makefile for bioactivity-data-acquisition

.PHONY: help build dev prod ci test clean logs shell

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Docker targets
build: ## Build all Docker images
	docker-compose build

build-dev: ## Build development image
	docker-compose build bioactivity-etl

build-prod: ## Build production image
	docker build --target production -t bioactivity-etl:prod .

build-ci: ## Build CI image
	docker build --target ci -t bioactivity-etl:ci .

# Development targets
dev: ## Start development environment
	docker-compose up -d jaeger prometheus grafana postgres redis
	docker-compose run --rm bioactivity-etl

dev-build: ## Start development environment with rebuild
	docker-compose up -d jaeger prometheus grafana postgres redis
	docker-compose build bioactivity-etl
	docker-compose run --rm bioactivity-etl

# Production targets
prod: ## Start production environment
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

prod-build: ## Build and start production environment
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml build
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# CI targets
ci: ## Run CI pipeline in Docker
	docker-compose run --rm bioactivity-etl-ci

ci-test: ## Run tests in CI container
	docker run --rm -v $(PWD):/app -w /app bioactivity-etl:ci pytest -v

ci-lint: ## Run linting in CI container
	docker run --rm -v $(PWD):/app -w /app bioactivity-etl:ci ruff check .
	docker run --rm -v $(PWD):/app -w /app bioactivity-etl:ci black --check .
	docker run --rm -v $(PWD):/app -w /app bioactivity-etl:ci mypy src

# Testing targets
test: ## Run tests locally
	pytest -v

test-integration: ## Run integration tests
	pytest -v -m integration

test-coverage: ## Run tests with coverage
	pytest --cov=library --cov-report=term-missing --cov-report=html

test-watch: ## Run tests in watch mode
	pytest-watch

# Quality targets
lint: ## Run all linting tools
	ruff check .
	black --check .
	mypy src

format: ## Format code
	black .
	ruff --fix .

pre-commit: ## Run pre-commit hooks
	pre-commit run --all-files

# Utility targets
shell: ## Open shell in development container
	docker-compose run --rm bioactivity-etl bash

shell-prod: ## Open shell in production container
	docker run --rm -it bioactivity-etl:prod bash

logs: ## Show logs from all services
	docker-compose logs -f

logs-etl: ## Show logs from ETL service only
	docker-compose logs -f bioactivity-etl

clean: ## Clean up Docker resources
	docker-compose down -v
	docker system prune -f

clean-all: ## Clean up all Docker resources including images
	docker-compose down -v --rmi all
	docker system prune -af

# Pipeline execution targets
run-pipeline: ## Run ETL pipeline in development container
	docker-compose run --rm bioactivity-etl pipeline

run-pipeline-prod: ## Run ETL pipeline in production container
	docker run --rm -v $(PWD)/data:/app/data bioactivity-etl:prod pipeline

run-documents: ## Run document processing pipeline
	docker-compose run --rm bioactivity-etl get-document-data

# Monitoring targets
monitor: ## Start monitoring stack (Jaeger, Prometheus, Grafana)
	docker-compose up -d jaeger prometheus grafana

monitor-stop: ## Stop monitoring stack
	docker-compose stop jaeger prometheus grafana

# Data targets
data-setup: ## Set up data directories
	mkdir -p data/input data/output data/logs

data-clean: ## Clean output data
	rm -rf data/output/* data/logs/*

# Documentation targets
docs-serve: ## Serve documentation locally
	mkdocs serve

docs-build: ## Build documentation
	mkdocs build

docs-deploy: ## Deploy documentation to GitHub Pages
	mkdocs gh-deploy

# Installation targets
install: ## Install package in development mode
	pip install -e .[dev]

install-prod: ## Install package for production
	pip install .

# Version targets
version: ## Show current version
	python -c "from library.cli import app; print('bioactivity-data-acquisition 0.1.0')"

version-check: ## Check version consistency
	@echo "Checking version consistency..."
	@grep -q "version = \"0.1.0\"" pyproject.toml || (echo "Version mismatch in pyproject.toml" && exit 1)
	@grep -q "bioactivity-data-acquisition 0.1.0" src/library/cli/__init__.py || (echo "Version mismatch in CLI" && exit 1)
	@echo "Version consistency check passed"

# Security targets
security-check: ## Run security checks
	safety check
	bandit -r src/

# Dependencies targets
deps-update: ## Update dependencies
	pip-compile pyproject.toml
	pip-compile pyproject.toml --dev

deps-sync: ## Sync dependencies
	pip-sync requirements.txt requirements-dev.txt
