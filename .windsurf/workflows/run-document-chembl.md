---
description: Run document_chembl pipeline via /run-document-chembl
---

# /run-document-chembl Workflow

Use together with
[.windsurf/commands/run-document-chembl.md](../commands/run-document-chembl.md)
to run the ChEMBL document pipeline.

## Inputs

- `--output-dir PATH` (required)
- `--config PATH` (optional)
- `--mode NAME` (optional)
- `--dry-run` (optional)
- `--verbose` (optional)
- `--limit N` (optional)
- `--sample N` (optional)
- `--extended` (optional)
- `--set KEY=VALUE` (optional, repeatable)
- `--input-file PATH` (optional)
- `--golden PATH` (optional)

## Steps

1. Choose an `--output-dir` for document artifacts.
2. Optionally prepare a custom `--config` or use the default path from
   the command doc.
3. Select `--mode` if you need a specific enrichment profile
   (for example `chembl` or `all`).
4. If needed, set `--limit` or `--sample` for smoke runs and
   `--extended` for extra QC.
5. Run `/run-document-chembl --output-dir <output-dir> [other options]`.
6. Verify that data files, `meta.yaml` and QC reports are written under
   `data/output/document/`.

## Outputs

- Document table in `data/output/document/` (CSV or Parquet).
- `meta.yaml` with pipeline metadata.
- QC reports such as `quality_report_table.csv` and
  `correlation_report_table.csv`.
- Structured JSON logs in `logs/` or the configured logs directory.

## Exit Criteria

- Command exits with code 0.
- Expected dataset, `meta.yaml` and QC artifacts are present and
  validated.
