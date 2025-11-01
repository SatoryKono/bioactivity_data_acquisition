# Runtime Asset Layout

Runtime outputs produced by QA tools, integration tests, and exploratory runs
are intentionally excluded from version control. They now live under the
`var/` hierarchy to keep the repository history focused on source code.

## Directory structure

- `var/artifacts/` – generated graphs and JSON manifests, including the module
  dependency map and Mermaid import graph built by `tools.qa.analyze_codebase`.
- `var/reports/` – historical QA exports such as clone-detection CSV files and
  configuration-duplicate listings. These are regenerated on demand when the QA
  tooling is executed locally.
- `var/logs/` – structured log files emitted by the CLI and background workers
  during debugging sessions.
- `var/tmp/` – ephemeral scratch space for ad-hoc datasets (for example,
  `temp_head.csv` when inspecting ETL outputs).
- `var/diagnostics/` – machine-generated diagnostics like coverage reports and
  pytest summaries.

Each subdirectory contains a `.gitkeep` placeholder so that the layout is
visible even when the runtime data has been cleaned. Generated files remain
ignored by `.gitignore`, ensuring that accidental commits cannot introduce large
or environment-specific binaries.
