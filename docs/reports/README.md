# Reports Directory Status

The CSV files in this directory are **historical QA artifacts** that captured
clone-detection and configuration-duplication snapshots during an earlier phase
of the project. They are kept for traceability only and are no longer
maintained or regenerated as part of the active ETL pipeline.

- `ast_clones.csv`
- `token_clones.csv`
- `semantic_clones.csv`
- `config_duplicates.csv`

When auditing the codebase, treat these files as archival references rather
than up-to-date sources of truth. Any remediation work based on them should be
validated against the current code and configuration state first.
