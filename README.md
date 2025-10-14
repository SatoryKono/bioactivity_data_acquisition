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
- **Comprehensive QA** with pytest (coverage ≥ 90 %), mypy `--strict`, ruff, and black.

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

Pipeline behaviour is controlled via YAML configuration files. Use `configs/config.example.yaml` as a
starting point:

```yaml
sources:
  - name: chembl
    base_url: "https://example.com"
    activities_endpoint: "/activities"
    page_size: 200
output:
  output_path: "data/output/bioactivities.csv"
  qc_report_path: "data/output/qc_report.csv"
  correlation_path: "data/output/correlation.csv"
retries:
  max_tries: 5
log_level: INFO
strict_validation: true
```

- **sources** – API sources with endpoint configuration and pagination settings.
- **output** – destinations for normalized data, QC metrics, and correlation matrices.
- **retries** – retry configuration for HTTP resilience.
- **log_level** – structlog log level.
- **strict_validation** – toggle strict schema validation.

Authentication tokens can be injected through an optional `.env` file placed alongside the
configuration. Declare keys matching `<SOURCE_NAME>_AUTH_TOKEN` (e.g. `CHEMBL_AUTH_TOKEN`).

## Command Line Interface

Run the pipeline from the CLI using Typer:

```bash
bioactivity-data-acquisition pipeline --config configs/config.example.yaml
```

Provide `--env-file` if secret tokens live in a `.env` file:

```bash
bioactivity-data-acquisition pipeline --config configs/config.example.yaml --env-file .env
```

## Testing and Quality Gates

Run the full validation suite (coverage threshold 90 % is enforced by pytest):

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

The GitHub Actions workflow (`.github/workflows/ci.yaml`) runs Ruff, Black, mypy, and pytest
on each push and pull request targeting `main` or `work`.

## Outputs

The load stage produces three artefacts:

- **Bioactivities CSV** – sorted deterministic dataset.
- **QC report** – summary metrics (row counts, duplicates, missing values).
- **Correlation matrix** – numeric correlations saved as CSV.
