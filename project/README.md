# Project Publications ETL

Command-line pipeline that fetches publication metadata from several biomedical sources (ChEMBL, PubMed, Semantic Scholar, Crossref, OpenAlex), normalises and validates the combined dataset, and writes deterministic CSV outputs.

## Quick start

```bash
pip install -e .[dev]
fetch-publications --input ./data/input/documents.csv --output ./data/output/publications.csv
```
