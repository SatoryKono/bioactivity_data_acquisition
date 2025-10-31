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

```

## Environment variables

Все обязательные переменные перечислены в файле [`.env.example`](.env.example) с
комментариями по формату значений. Скопируйте его и заполните реальные данные:

```bash
cp .env.example .env
${SHELL:-bash} -lc 'set -a; source .env; set +a'

```

Команда `set -a` экспортирует все переменные из `.env` в текущую сессию. В
CI/CD можно использовать аналогичный подход (например, `python -m dotenv load`
или встроенные менеджеры секретов) до вызова CLI.

<!-- markdownlint-disable MD013 -->
| Переменная | Назначение | Обязательность |
| --- | --- | --- |
| `PUBMED_TOOL` | Значение параметра `tool` для обращения к NCBI E-utilities. | Да — требуется документному пайплайну. |
| `PUBMED_EMAIL` | Контактный e-mail для PubMed, который указывается в запросах. | Да — требуется документному пайплайну. |
| `PUBMED_API_KEY` | Повышает PubMed rate limit с 3 до 10 запросов/сек. | Необязательная, но рекомендована для production. |
| `CROSSREF_MAILTO` | E-mail для polite pool Crossref API. | Да — требуется документному пайплайну. |
| `SEMANTIC_SCHOLAR_API_KEY` | Токен Semantic Scholar Graph API. | Необязательная локально, обязательна для production. |
| `IUPHAR_API_KEY` | Ключ доступа к Guide to Pharmacology API. | Да — требуется target-пайплайну. |
<!-- markdownlint-enable MD013 -->

Если `IUPHAR_API_KEY` не задан, загрузчик конфигурации завершится ошибкой,
предотвращая случайную отправку плейсхолдеров `${IUPHAR_API_KEY}` в HTTP-заголовки.

## CLI Usage

Команды Typer регистрируются автоматически на основе `scripts.PIPELINE_COMMAND_REGISTRY`,
поэтому консольный интерфейс всегда отражает актуальные пайплайны. Полный перечень команд,
дефолтных путей и унифицированных флагов поддерживается в [refactoring/FAQ.md](refactoring/FAQ.md#cli-commands).

```bash

# Просмотреть доступные команды и флаги

python -m bioetl.cli.main --help

# Список зарегистрированных пайплайнов

python -m bioetl.cli.main list

# Пример запуска пайплайна в режиме dry-run

python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --dry-run \
  --verbose

```

## Structure

```text
src/bioetl/
  ├── core/          # Logger, API client, output writer
  ├── config/        # Configuration system
  ├── configs/
  │   ├── includes/  # Shared YAML fragments for pipelines
  │   └── pipelines/ # Per-source configs (<source>.yaml)
  ├── normalizers/   # Data normalizers
  ├── schemas/       # Pandera schemas
  ├── pipelines/     # Pipeline implementations
  ├── sources/
  │   └── <source>/
  │       ├── client/       # HTTP clients with retry/backoff
  │       ├── request/      # Request builders and API etiquette
  │       ├── pagination/   # Pagination strategies (page, cursor, ...)
  │       ├── parser/       # Response parsing without side effects
  │       ├── normalizer/   # Converts payloads to UnifiedSchema
  │       ├── schema/       # Pandera schemas (optional)
  │       ├── merge/        # Merge policies (optional)
  │       ├── output/       # Deterministic writers, hashes, meta.yaml
  │       └── pipeline.py   # PipelineBase orchestration entrypoint
  └── cli/           # CLI interface

configs/
  ├── base.yaml      # Base configuration
  ├── profiles/      # dev.yaml, prod.yaml, test.yaml
  └── sources/       # Per-source configs (pipeline.yaml, schema.yaml, includes/)

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

```

Подробнее о запуске тестов см. [docs/TESTING.md](docs/TESTING.md).

### Continuous Integration setup

CI workflows rely on the same configuration loader that powers the CLI. To
customise logging or other runtime options in GitHub Actions, use
double-underscore separated environment variables with the `BIOETL__` prefix.
For example, the default workflow sets `BIOETL__LOGGING__LEVEL=DEBUG` and
`BIOETL__LOGGING__FILE__ENABLED=true` before running `pytest`. The loader also
accepts the legacy `BIOACTIVITY__` prefix to keep existing secrets working
while pipelines migrate to the new namespace.

Column layout drift is caught automatically in CI by running
`python -m scripts.validate_columns compare-all`. The helper now raises an
assertion if a CSV filename does not start with its registered entity prefix or
if the materialised columns diverge from the Pandera schema, keeping the I/O
contracts in sync with `bioetl.schemas.*`.

### Extract-stage conventions

\1- Всегда используйте `PipelineBase.read_input_table` для чтения исходных CSV.

  Хелпер логирует путь, применяет `limit`/`sample` и возвращает как датафрейм,
  так и разрешённый путь. Это гарантирует единообразное поведение при
  отсутствии файла и упрощает написание новых пайплайнов.

### Transform-stage conventions

- `transform()` обязан принимать и возвращать `pd.DataFrame`, сохраняя табличный
  контракт между стадиями и позволяя повторно применять схемы валидации без
  дополнительных адаптеров.【F:src/bioetl/pipelines/base.py†L785-L802】

### Export-stage conventions

- `export()` использует `UnifiedOutputWriter` для детерминированной фиксации
  данных и побочных артефактов QC, поэтому на вход всегда подаётся
  `pd.DataFrame`, уже прошедший валидацию.【F:src/bioetl/pipelines/base.py†L804-L880】

### Extended output mode

Флаг `--extended` в CLI добавляет к стандартному набору артефактов
(`dataset.csv`, `qc/<name>_quality_report.csv`, `meta.yaml`) следующие файлы:

\1- `qc/<name>_correlation_report.csv` — матрица парных корреляций для всех

  числовых признаков в длинном формате.

\1- `qc/<name>_summary_statistics.csv` — сводка описательных статистик по

  каждому столбцу (count, mean, top, freq и т. д.).

\1- `qc/<name>_dataset_metrics.csv` — агрегированные QC-метрики на уровне всего

  датасета (количество строк/столбцов, дубликаты, пустые значения, размер в
  памяти и т. п.).

Все новые файлы сохраняются в каталоге `qc/` рядом с основным датасетом и
учитываются в `meta.yaml` для контроля целостности.

## Commands

Для быстрого запуска pipeline'ов используйте команды из [docs/COMMANDS.md](docs/COMMANDS.md).

## License

MIT
