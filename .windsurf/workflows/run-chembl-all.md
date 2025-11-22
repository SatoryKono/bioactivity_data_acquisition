---
description: Run all ChEMBL pipelines via /run-chembl-all
---

# /run-chembl-all Workflow

Use together with
[.windsurf/commands/run-chembl-all.md](../commands/run-chembl-all.md)
to orchestrate all ChEMBL pipelines.

## Inputs

- `--output-root PATH` (required)
- `--configs-dir PATH` (optional)
- `--limit N` (optional)
- `--extended` (optional)
- `--golden PATH` (optional)

## Steps

1. Choose an `--output-root` directory for all pipeline artifacts.
2. Optionally point `--configs-dir` to a non-default configs root.
3. If needed, set `--limit` and `--extended` flags and provide a
   `--golden` dataset for determinism checks.
4. Run `/run-chembl-all --output-root <output-root> [other options]`.
5. Inspect the aggregated summary and QC outputs in
   `reports/chembl_all/`.

## Outputs

- `reports/chembl_all/summary.md` with aggregated run information.
- `reports/chembl_all/qc.json` with combined QC metrics.

## Exit Criteria

- Command exits with code 0.
- All underlying pipelines either succeed or report clear failure
  reasons.
