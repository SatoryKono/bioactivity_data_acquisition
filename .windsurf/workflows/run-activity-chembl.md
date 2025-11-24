---
description: Run activity_chembl pipeline via /run-activity-chembl
---

# /run-activity-chembl Workflow

Use together with
[.windsurf/commands/run-activity-chembl.md](../commands/run-activity-chembl.md)
to run the ChEMBL activity pipeline.

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

1. Choose an `--output-dir` for pipeline artifacts.
2. Optionally prepare a custom `--config` or use the default path from
   the command doc.
3. If needed, set `--limit` or `--sample` for smoke runs and
   `--extended` for extra QC.
4. Run `/run-activity-chembl --output-dir <output-dir> [other options]`.
5. Verify that data files, `meta.yaml` and QC reports are written under
   `data/output/activity/`.

## Outputs

- Activity table in `data/output/activity/` (CSV or Parquet).
- `meta.yaml` with pipeline metadata.
- QC reports such as `quality_report_table.csv` and
  `correlation_report_table.csv`.
- Structured JSON logs in `logs/` or the configured logs directory.

## Exit Criteria

- Command exits with code 0.
- Expected dataset, `meta.yaml` and QC artifacts are present and
  validated.
