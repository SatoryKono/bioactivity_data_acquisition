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
# Run linting (same as CI)
ruff check src/bioetl src/library tests
ruff format --check src/bioetl src/library tests
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src/bioetl --cov-report=html

# Run specific test suite
pytest tests/unit/ -v              # Unit tests only
pytest tests/integration/ -v       # Integration tests only

# Lint
ruff check src/ tests/

# Type check
mypy --config-file=pyproject.toml src/bioetl src/library

# Execute the full test matrix
pytest tests/unit tests/integration tests/schemas

# Run every hook locally
pre-commit run --all-files
```

Подробнее о запуске тестов см. [docs/TESTING.md](docs/TESTING.md).

### Extract-stage conventions

* Всегда используйте `PipelineBase.read_input_table` для чтения исходных CSV.
  Хелпер логирует путь, применяет `limit`/`sample` и возвращает как датафрейм,
  так и разрешённый путь. Это гарантирует единообразное поведение при
  отсутствии файла и упрощает написание новых пайплайнов.

## Commands

Для быстрого запуска pipeline'ов используйте команды из [docs/COMMANDS.md](docs/COMMANDS.md).

## License

MIT
