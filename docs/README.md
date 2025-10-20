# Bioactivity Data Acquisition

A modular ETL pipeline for downloading bioactivity data via HTTP APIs,
validating it with
[Pandera](https://pandera.readthedocs.io/), transforming it into
a normalized
representation, and
exporting deterministic CSV outputs together with QC reports.

## Features

- **Config-driven** execution using YAML files parsed with Pydantic.

- **Resilient HTTP clients** powered by `requests` and `backoff`.

- **Data validation** with Pandera schemas for both raw and normalized data.

- **Deterministic outputs** including QC metrics and correlation matrices.

- **Typer-based CLI** for orchestrating the pipeline.

- **Comprehensive QA** with pytest (coverage ≥ 90%), mypy `--strict`, ruff, and black.

## Installation

Create and activate a virtual environment (Python ≥ 3.10), then install the
project with the
development dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[dev]
```

If you only need the runtime dependencies, drop the `[dev]` extra.

## Configuration

The canonical configuration lives in
[`configs/config.yaml`](configs/config.yaml). It is documented in
detail in [`docs/CONFIG.md`](docs/CONFIG.md) together with the precedence rules:

1. Defaults defined in `bioactivity.config`.
2. Values from the YAML file passed via `--config`.
3. Environment variables prefixed with `BIOACTIVITY__` (e.g. `BIOACTIVITY__RUNTIME__LOG_LEVEL=DEBUG`).
4. CLI overrides provided with `--set section.key=value`.

Secrets such as API tokens are injected via placeholders in headers (e.g.
`Authorization: "Bearer {CHEMBL_API_TOKEN}"`) and are resolved exclusively from
environment
variables. The canonical configuration covers ChEMBL and Crossref sources,
output destinations,
deterministic behaviour, and QC thresholds.

Consult [`reports/config_audit.csv`](reports/config_audit.csv) for an inventory
of available keys.

## Command Line Interface

Run the pipeline from the CLI using Typer:

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml
```

Override individual configuration values at runtime:

```bash
BIOACTIVITY__LOGGING__LEVEL=DEBUG \
  bioactivity-data-acquisition pipeline \
  --config configs/config.yaml \
  --set runtime.workers=8 \
  --set sources.chembl.pagination.max_pages=1
```

## Testing and Quality Gates

Run the full validation suite (coverage threshold 90% is enforced by pytest):

```bash
pytest
```

Type-check the codebase with mypy in strict mode:

```bash
mypy src
```

Lint and format using ruff and black:

```bash
ruff check .
black .
```

## Pre-commit Hooks

Install pre-commit hooks to run Ruff, Black, mypy, and pytest automatically:

```bash
pre-commit install
pre-commit run --all-files
```

## Continuous Integration

The GitHub Actions workflow (`.github/workflows/ci.yaml`) runs comprehensive quality checks on each push and pull request targeting `main` or `develop`:

- **Linting**: Ruff for code style and formatting
- **Type checking**: mypy in strict mode
- **Security**: Bandit and Safety vulnerability scans
- **Testing**: pytest with 90% coverage threshold
- **Documentation**: MkDocs build and deployment

### CI Artifacts

All generated reports and outputs are uploaded as GitHub Actions artifacts instead of being committed to the repository:

- **Test outputs**: Generated test files and results
- **Coverage reports**: XML and HTML coverage reports
- **Security reports**: Bandit and Safety JSON reports
- **Benchmark results**: Performance test results
- **Logs**: Test execution logs

Access artifacts via the Actions tab in GitHub. See [docs/ci.md](docs/ci.md) for detailed information about artifact locations and access.

## API Limits Monitoring

Для проверки лимитов и доступности API используйте следующие скрипты:

### Быстрая проверка

```bash
# Проверка всех API
python src/library/scripts/api_health_check.py --save

# Проверка конкретного API
python src/library/scripts/quick_api_check.py crossref
```

### Детальная проверка

```bash
# Полная проверка с отчетом
python src/library/scripts/check_api_limits.py

# Детальная информация о лимитах
python src/library/scripts/check_specific_limits.py
```

### Мониторинг в реальном времени

```bash
# Мониторинг Crossref API каждые 30 секунд
python src/library/scripts/monitor_api.py crossref

# Мониторинг с настройками
python src/library/scripts/monitor_api.py pubmed -i 60 -d 3600 # каждую минуту в течение часа
```

Подробная документация: [docs/API_LIMITS_CHECK.md](docs/API_LIMITS_CHECK.md)

## Навигация по документации

### ⚙️ Пайплайны
- [Documents](pipelines/documents.md)
- [Testitem](pipelines/testitem.md)
- [Assay](pipelines/assay.md)
- [Activity](pipelines/activity.md)
- [Target](pipelines/target.md)

### 📚 Разделы
- [Configuration → Sources](configuration/sources.md)
- [Extraction Contract](extraction-contract.md)
- [Normalization](normalization.md) · [Enrichment](enrichment.md)
- [Validation](validation.md) · [QC](qc.md)
- [Persistence & Determinism](persistence.md)
- [CLI usage](cli.md) · [Logging & Metrics](logging.md)
- [Tests Policy](tests.md) · [Output Artifacts](output-artifacts.md)
- [Non-Goals](non-goals.md)

## Outputs

The load stage produces three artefacts:

- **Bioactivities CSV** – sorted deterministic dataset.

- **QC report** – summary metrics (row counts, duplicates, missing values).

- **Correlation matrix** – numeric correlations saved as CSV.

## Git LFS для больших файлов

Репозиторий настроен для использования Git Large File Storage (LFS) для отслеживания больших бинарных файлов (>500KB).

Подробная документация: [docs/GIT_LFS_WORKFLOW.md](docs/GIT_LFS_WORKFLOW.md)

### Быстрый старт

```bash
# Проверить статус LFS
git lfs ls-files

# Добавить новый большой файл (автоматически отслеживается)
git add large_dataset.parquet
git commit -m "Add large dataset"
```

## Архивная документация

Исторические отчеты и документы о реализации улучшений сохранены в отдельной ветке `archive/internal-reports` для сохранения истории проекта. Эти документы не являются частью активной документации.