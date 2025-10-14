# Bioactivity Data Acquisition

A modular ETL pipeline for downloading bioactivity data via HTTP APIs, validating it with
[Pandera](https://pandera.readthedocs.io/), transforming it into a normalized representation, and
exporting deterministic CSV outputs together with QC reports.

## Features

- **Config-driven** execution using YAML files parsed with Pydantic.
- **Resilient HTTP clients** powered by `requests` and `backoff`.
- **Data validation** with Pandera schemas for both raw and normalized data.
- **Deterministic outputs** including QC metrics and correlation matrices.
- **Typer-based CLI** for orchestrating the pipeline.
- **Comprehensive QA** with pytest (coverage ≥ 90 %), mypy `--strict`, ruff, and black.

## Installation

Create and activate a virtual environment (Python ≥ 3.10), then install the project with the
development dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[dev]
```

If you only need the runtime dependencies, drop the `[dev]` extra.

## Configuration

Pipeline behaviour is controlled via YAML configuration files. Use `configs/example.yaml` as a
starting point:

```yaml
clients:
  - name: chembl
    url: "https://example.com/api"
    pagination_param: "page"
    page_size_param: "page_size"
    page_size: 100
output:
  data_path: "data/output/bioactivity.csv"
  qc_report_path: "data/output/qc_report.csv"
  correlation_path: "data/output/correlation.csv"
retries:
  max_tries: 5
  backoff_multiplier: 1.0
logging:
  level: INFO
validation:
  strict: true
```

- **clients** – API sources with optional pagination settings.
- **output** – destinations for normalized data, QC metrics, and correlation matrices.
- **retries** – `backoff` configuration for HTTP resilience.
- **logging** – structlog log level.
- **validation** – toggle strict schema validation.

Store secrets such as API tokens in environment variables (consider using `.env` files with
`python-dotenv`).

## Running the CLI

Use the Typer app to execute the pipeline:

```bash
bioactivity-etl pipeline --config configs/example.yaml
```

Or, if you prefer running the module directly:

```bash
python -m library.cli pipeline --config configs/example.yaml
```

The command fetches data from all configured sources, applies normalization, and writes CSV outputs
and QC artefacts to the paths defined in the configuration file.

## Testing and Quality Gates

Run the full validation suite (coverage threshold 90 % is enforced by pytest):

```bash
pytest
```

Type-check the codebase with mypy in strict mode:

```bash
mypy --strict library
```

Lint and format using ruff and black:

```bash
ruff check .
black --check .
```

## Pre-commit Hooks

Install the hooks locally to guard commits:

```bash
pre-commit install
pre-commit run --all-files
```

## Continuous Integration

GitHub Actions workflow `.github/workflows/ci.yaml` runs linting (ruff, black), strict mypy, and the
pytest suite on every push and pull request targeting `main`.

