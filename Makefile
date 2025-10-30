.PHONY: help test test-unit test-integration test-cov lint format type-check install-dev clean

help:
	@echo "Available commands:"
	@echo "  make install-dev   - Install development dependencies"
	@echo "  make test          - Run all tests"
	@echo "  make test-unit     - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make test-cov      - Run tests with coverage report"
	@echo "  make lint          - Run linter"
	@echo "  make format        - Format code"
	@echo "  make type-check    - Run type checker"
	@echo "  make clean         - Clean cache files"

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-cov:
	pytest tests/ --cov=src/bioetl --cov-report=term-missing --cov-report=html

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

type-check:
	mypy src/

clean:
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".pytest_cache" -delete
	find . -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
