# CLI Utilities Inventory

This inventory covers every executable helper script that ships with the repository.  All
utilities now live in the top-level `scripts/` directory so they can be invoked uniformly
with `python scripts/<name>.py`.  Unless stated otherwise the commands materialise their
artifacts under `audit_results/`.

| Command | Path | Purpose | Key artifacts | Example invocation |
| --- | --- | --- | --- | --- |
| `build_vocab_store` | `scripts/build_vocab_store.py` | Aggregate individual YAML dictionaries into a single ChEMBL vocabulary store using the Typer CLI. | `configs/chembl_dictionaries.yaml` (or a custom path supplied via `--output`). | `python scripts/build_vocab_store.py --src configs/dictionaries --output audit_results/chembl_vocab.yaml` |
| `catalog_code_symbols` | `scripts/catalog_code_symbols.py` | Walk the code base to capture `PipelineBase` method signatures and Typer CLI registrations. | `audit_results/code_signatures.json`, `audit_results/cli_commands.txt`. | `python scripts/catalog_code_symbols.py` |
| `create_matrix_doc_code` | `scripts/create_matrix_doc_code.py` | Build the documentation ↔ code traceability matrix for ETL contracts. | `audit_results/matrix-doc-code.csv`, `audit_results/matrix-doc-code.json`. | `python scripts/create_matrix_doc_code.py` |
| `determinism_check` | `scripts/determinism_check.py` | Run `activity_chembl` and `assay_chembl` pipelines twice in `--dry-run` mode and compare structured logs for determinism. | `audit_results/DETERMINISM_CHECK_REPORT.md`. | `python scripts/determinism_check.py` |
| `doctest_cli` | `scripts/doctest_cli.py` | Extract CLI examples from README and pipeline docs, execute them safely (`--dry-run`), and capture pass/fail status. | `audit_results/CLI_DOCTEST_REPORT.md`. | `python scripts/doctest_cli.py` |
| `inventory_docs` | `scripts/inventory_docs.py` | Produce a hash-based inventory of Markdown documentation. | `audit_results/docs_inventory.txt`, `audit_results/docs_hashes.txt`. | `python scripts/inventory_docs.py` |
| `link_check` | `scripts/link_check.py` | Run the Lychee link checker with the repository `.lychee.toml` configuration. | `audit_results/link-check-report.md`. | `python scripts/link_check.py` |
| `run_test_report` | `scripts/run_test_report.py` | Orchestrate `pytest` with coverage, calculate metadata hashes, and atomically publish a timestamped report folder. | Timestamped directory inside `TEST_REPORTS_ROOT` (see `bioetl.core.test_report_artifacts`). | `python scripts/run_test_report.py --output-root audit_results/test-reports` |
| `schema_guard` | `scripts/schema_guard.py` | Validate pipeline configuration files against `bioetl.config.loader` and check critical determinism fields. | `audit_results/SCHEMA_GUARD_REPORT.md`. | `python scripts/schema_guard.py` |
| `semantic_diff` | `scripts/semantic_diff.py` | Compare documented API surfaces with the runtime implementation to spot divergences. | `audit_results/semantic-diff-report.json`. | `python scripts/semantic_diff.py` |
| `vocab_audit` | `scripts/vocab_audit.py` | Fetch live ChEMBL vocabularies and reconcile them with local dictionary snapshots. | `audit_results/vocab_audit.csv`, `audit_results/vocab_audit_meta.yaml`. | `python scripts/vocab_audit.py --src configs/dictionaries --output audit_results/vocab_audit.csv` |

All helper CLIs rely on the editable installation of the project (`pip install -e .[dev]`).
Run them from the repository root so relative paths resolve correctly.
