---
description: Run testitem_chembl pipeline via /run-testitem-chembl
---

# /run-testitem-chembl Workflow

Use together with
[.windsurf/commands/run-testitem-chembl.md](../commands/run-testitem-chembl.md)
to run the ChEMBL testitem pipeline.

## Inputs

- `--output-dir PATH` (required)
- `--config PATH` (optional)
- `--dry-run` (optional)
- `--verbose` (optional)
- `--limit N` (optional)
- `--sample N` (optional)
- `--extended` (optional)
- `--set KEY=VALUE` (optional, repeatable)
- `--input-file PATH` (optional)
- `--golden PATH` (optional)

## Steps

1. Choose an `--output-dir` for testitem artifacts.
2. Optionally prepare a custom `--config` or use the default path from
   the command doc.
3. If needed, set `--limit` or `--sample` for smoke runs and
   `--extended` for extra QC.
4. Run `/run-testitem-chembl --output-dir <output-dir> [other options]`.
5. Verify that data files, `meta.yaml` and QC reports are written under
   `data/output/testitem/`.

## Outputs

- Test item table in `data/output/testitem/` (CSV or Parquet).
- `meta.yaml` with pipeline metadata.
- QC reports such as `quality_report_table.csv` and
  `correlation_report_table.csv`.
- Structured JSON logs in `logs/` or the configured logs directory.

## Exit Criteria

- Command exits with code 0.
- Expected dataset, `meta.yaml` and QC artifacts are present and
  validated.
