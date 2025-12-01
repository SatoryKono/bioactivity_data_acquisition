# Bioactivity Publications ETL

This repository contains a modular ETL pipeline for collecting bioactivity-related publications
from public data sources such as ChEMBL, PubMed, Semantic Scholar, Crossref, and OpenAlex. The
pipeline is built around resilient HTTP clients, Pandas-based transformations, and deterministic
CSV exports to support downstream analytics and reproducible research.

## Requirements

- Python 3.10 or newer
- Recommended: a virtual environment managed by `venv`, `conda`, or `uv`
- Access credentials for APIs that require authentication (stored in `.env` files)

Install the project and development dependencies with:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pre-commit install
```

## Configuration

Runtime parameters are provided by a YAML configuration file. Copy the example below into
`config/pipeline.example.yaml` (or another path) and adjust as needed:

```yaml
logging:
  level: INFO
  output: stdout
  json: true
etl:
  batch_size: 100
  strict_validation: true
  output_dir: data/output
sources:
  chembl:
    base_url: "https://www.ebi.ac.uk/chembl/api/data/"
    api_key: "${CHEMBL_API_KEY}"
    rate_limit_per_minute: 30
  pubmed:
    base_url: "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    api_key: "${PUBMED_API_KEY}"
  semscholar:
    base_url: "https://api.semanticscholar.org/graph/v1/"
  crossref:
    base_url: "https://api.crossref.org/"
  openalex:
    base_url: "https://api.openalex.org/"
```

Secrets such as API keys are read from environment variables. Create a `.env` file containing the
relevant values and load it before running the pipeline.

## Example Input File

A minimal CSV containing the topics or identifiers to fetch can look like:

```csv
query,type
"G protein-coupled receptor",keyword
"CHEMBL25",chembl_id
```

Place the file at `data/input/queries.csv` (the directory is ignored by Git) or pass the path via
CLI options.

## CLI Usage

The main entry point is a Typer application exposed as `fetch-publications`.

```bash
fetch-publications run --config config/pipeline.yaml --input data/input/queries.csv --output data/output/publications.csv
```

### Commands

- `run`: Execute the end-to-end ETL pipeline (extract, transform, load) using the provided
  configuration file.
- `extract`: Fetch publications without persisting results. Useful for dry runs and debugging.

Use `--help` on any command for detailed options.

## Testing and Quality Gates

```bash
pytest
ruff check .
ruff format .
mypy .
```

CI is expected to run the same set of checks. Coverage reporting is configured via the
`pyproject.toml` file.

## Project Layout

```
project/
├── pyproject.toml
├── README.md
├── scripts/
│   └── fetch_publications.py
├── library/
│   ├── clients/
│   │   ├── chembl.py
│   │   ├── pubmed.py
│   │   ├── semscholar.py
│   │   ├── crossref.py
│   │   └── openalex.py
│   ├── io/
│   │   ├── normalize.py
│   │   └── read_write.py
│   ├── utils/
│   │   ├── errors.py
│   │   ├── joins.py
│   │   ├── logging.py
│   │   └── rate_limit.py
│   └── validation/
│       ├── input_schema.py
│       └── output_schema.py
└── tests/
```

Each module currently contains scaffolding to help kick-start implementation.
