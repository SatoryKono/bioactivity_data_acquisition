# BioETL

Unified ETL framework for bioactivity data extraction from ChEMBL and
external sources with full determinism and reproducibility.

## Installation

```bash

# Install in development mode

pip install -e ".[dev]"

# Install pre-commit hooks

pre-commit install

```

### Install test dependencies only

```bash
pip install -r requirements.txt
```

> **Note:** The test suite relies on [Faker](https://faker.readthedocs.io/en/master/)
> for deterministic fixture data. Installing the development extras or the
> pinned requirements file above ensures the dependency is available before
> running `pytest`.

## Quick Start

```bash

# Load configuration

python -c "from bioetl.config import load_config; print(load_config('configs/profiles/dev.yaml'))"

# Test logger

python -c "from bioetl.core.logger import UnifiedLogger; \
UnifiedLogger.setup('development', 'test'); \
UnifiedLogger.get('test').info('Hello World')"

```text

## Environment variables

Некоторые пайплайны требуют секретов из переменных окружения. Для доступа к IUPHAR
необходимо установить API-ключ до запуска конфигурации или CLI:

```bash

export IUPHAR_API_KEY="your-iuphar-token"

```text

Если переменная не задана, загрузчик конфигурации завершится ошибкой, предотвращая
случайную отправку плейсхолдеров `${IUPHAR_API_KEY}` в HTTP-заголовки.

## CLI Usage

Команды Typer регистрируются автоматически на основе `scripts.PIPELINE_COMMAND_REGISTRY`,
поэтому консольный интерфейс всегда отражает актуальные пайплайны.

```bash

# Просмотреть доступные команды и флаги

python -m bioetl.cli.main --help

# Список зарегистрированных пайплайнов

python -m bioetl.cli.main list

# Пример запуска пайплайна в режиме dry-run

python -m bioetl.cli.main activity \
  --config configs/pipelines/activity.yaml \
  --dry-run \
  --verbose

```text

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

```text

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
make test-unit                     # Unit tests only (directory scoped)
make test-integration              # Integration tests only (directory scoped)
pytest tests/unit/ -v              # Direct pytest invocation for unit tests
pytest tests/integration/ -v       # Direct pytest invocation for integration tests

# Lint

ruff check src/ tests/

# Type check

mypy --config-file=pyproject.toml src/bioetl src/library

# Execute the full test matrix

pytest tests/unit tests/integration tests/schemas

# Run every hook locally

pre-commit run --all-files

```text

Подробнее о запуске тестов см. [docs/TESTING.md](docs/TESTING.md).

### Continuous Integration setup

CI workflows rely on the same configuration loader that powers the CLI. To
customise logging or other runtime options in GitHub Actions, use
double-underscore separated environment variables with the `BIOETL__` prefix.
For example, the default workflow sets `BIOETL__LOGGING__LEVEL=DEBUG` and
`BIOETL__LOGGING__FILE__ENABLED=true` before running `pytest`. The loader also
accepts the legacy `BIOACTIVITY__` prefix to keep existing secrets working
while pipelines migrate to the new namespace.

### Extract-stage conventions

* Всегда используйте `PipelineBase.read_input_table` для чтения исходных CSV.
  Хелпер логирует путь, применяет `limit`/`sample` и возвращает как датафрейм,
  так и разрешённый путь. Это гарантирует единообразное поведение при
  отсутствии файла и упрощает написание новых пайплайнов.

### Extended output mode

Флаг `--extended` в CLI добавляет к стандартному набору артефактов
(`dataset.csv`, `qc/<name>_quality_report.csv`, `meta.yaml`) следующие файлы:

* `qc/<name>_correlation_report.csv` — матрица парных корреляций для всех
  числовых признаков в длинном формате.

* `qc/<name>_summary_statistics.csv` — сводка описательных статистик по
  каждому столбцу (count, mean, top, freq и т. д.).

* `qc/<name>_dataset_metrics.csv` — агрегированные QC-метрики на уровне всего
  датасета (количество строк/столбцов, дубликаты, пустые значения, размер в
  памяти и т. п.).

Все новые файлы сохраняются в каталоге `qc/` рядом с основным датасетом и
учитываются в `meta.yaml` для контроля целостности.

## Commands

Для быстрого запуска pipeline'ов используйте команды из [docs/COMMANDS.md](docs/COMMANDS.md).

## License

MIT

