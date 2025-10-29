# BioETL

Unified ETL framework for bioactivity data extraction from ChEMBL and external sources with full determinism and reproducibility.

## Installation

```bash
# Install in development mode
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

## Quick Start

```bash
# Load configuration
python -c "from bioetl.config import load_config; print(load_config('configs/profiles/dev.yaml'))"

# Test logger
python -c "from bioetl.core.logger import UnifiedLogger; UnifiedLogger.setup('development', 'test'); UnifiedLogger.get('test').info('Hello World')"
```

## Structure

```text
src/bioetl/
  ├── core/          # Logger, API client, output writer
  ├── config/        # Configuration system
  ├── normalizers/   # Data normalizers
  ├── schemas/       # Pandera schemas
  ├── pipelines/     # Pipeline implementations
  └── cli/           # CLI interface

configs/
  ├── base.yaml      # Base configuration
  ├── profiles/      # dev.yaml, prod.yaml, test.yaml
  └── pipelines/     # Pipeline-specific configs

tests/
  ├── unit/          # Unit tests
  ├── integration/   # Integration tests
  ├── golden/        # Golden tests
  └── fixtures/      # Test fixtures
```

## Development

```bash
# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## License

MIT
