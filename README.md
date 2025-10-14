# Bioactivity Data Acquisition

Bioactivity data acquisition is a modular ETL pipeline for downloading ChEMBL bioactivity
records, normalising them with Pandas/Pandera, and exporting deterministic CSV artefacts.
The project targets Python 3.10+ and emphasises reliability, testability, and observability.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[dev]
```

## Configuration

Pipeline behaviour is configured via YAML. Use `configs/config.example.yaml` as a starting
point. The structure is:

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

## Development Workflow

### Formatting and Linting

```bash
ruff check .
black .
```

### Static Typing

```bash
mypy src
```

### Tests

Pytest is configured with coverage ≥90 %:

```bash
pytest
```

### Pre-commit Hooks

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
