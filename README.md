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

The canonical configuration lives in [`configs/config.yaml`](configs/config.yaml). It is documented in
detail in [`docs/CONFIG.md`](docs/CONFIG.md) together with the precedence rules:

1. Defaults defined in `bioactivity.config`.
2. Values from the YAML file passed via `--config`.
3. Environment variables prefixed with `BIOACTIVITY__` (e.g. `BIOACTIVITY__RUNTIME__LOG_LEVEL=DEBUG`).
4. CLI overrides provided with `--set section.key=value`.

Secrets such as API tokens are injected via placeholders in headers (e.g.
`Authorization: "Bearer {CHEMBL_API_TOKEN}"`) and are resolved exclusively from environment
variables. The canonical configuration covers ChEMBL and Crossref sources, output destinations,
deterministic behaviour, and QC thresholds.

Consult [`reports/config_audit.csv`](reports/config_audit.csv) for an inventory of available keys.

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
