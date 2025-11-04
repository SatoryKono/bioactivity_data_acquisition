# `/data/output/` Layout

This reference describes how BioETL pipelines organize their deterministic run
artifacts under `/data/output/`. The layout stitches together the catalog of
ChEMBL pipelines with the filesystem guarantees enforced by the shared
`PipelineBase` orchestrator so that operators know where to find datasets, QC
summaries, logs, manifests, and `meta.yaml` metadata for every run.

## 1. Top-Level Map

Every pipeline writes into its own deterministic folder whose name is prefixed
with an underscore (for example `_activity`, `_documents`). The directory is
created automatically when a `PipelineBase` instance boots, mirroring the sample
runs documented in the pipeline catalog. Inside each folder, a single filename
stem captures the pipeline code, optional mode, and a deterministic run tag (for
example a date), and all artifacts share that stem. 【F:src/bioetl/pipelines/base.py†L41-L127】

A representative snapshot of `/data/output/` therefore looks like this:

```text
/data/output/
├── _activity/
│   ├── activity_all_20251028.csv
│   ├── activity_all_20251028_quality_report.csv
│   ├── activity_all_20251028_correlation_report.csv
│   ├── activity_all_20251028_qc.csv
│   ├── activity_all_20251028_meta.yaml
│   └── activity_all_20251028_run_manifest.json
├── _assay/
│   ├── assay_all_20251028.csv
│   ├── assay_all_20251028_quality_report.csv
│   ├── assay_all_20251028_meta.yaml
│   └── assay_all_20251028_run_manifest.json
├── _documents/
│   ├── documents_all_20251021.csv
│   ├── documents_all_20251021_quality_report.csv
│   ├── documents_all_20251021_correlation_report.csv
│   ├── documents_all_20251021_qc.csv
│   ├── documents_all_20251021_meta.yaml
│   └── documents_all_20251021_run_manifest.json
├── _target/
│   ├── target_all_20251028.csv
│   ├── target_all_20251028_quality_report.csv
│   ├── target_all_20251028_meta.yaml
│   └── target_all_20251028_run_manifest.json
└── _testitem/
    ├── testitem_all_20251028.csv
    ├── testitem_all_20251028_quality_report.csv
    ├── testitem_all_20251028_meta.yaml
    └── testitem_all_20251028_run_manifest.json
```

The concrete filenames follow the conventions laid out in the pipeline cards:
CSV or Parquet datasets, optional correlation reports, and required
`meta.yaml` files for every pipeline. 【F:docs/pipelines/10-pipelines-catalog.md†L48-L210】

## 2. Artifact Roles and Relationships

| Artifact | Purpose | Notes |
| --- | --- | --- |
| `<stem>.csv` / `<stem>.parquet` | Primary dataset exported by the pipeline. | Documented per pipeline in the catalog coverage matrix. 【F:docs/pipelines/10-pipelines-catalog.md†L7-L210】 |
| `<stem>_quality_report.csv` | Deterministic QC metrics (row counts, duplicates, missingness). | Always generated in the standard output mode. 【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L1037-L1044】 |
| `<stem>_correlation_report.csv` | Optional correlation matrix for numeric fields. | Only present when correlation post-processing is enabled. 【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L1039-L1046】 |
| `<stem>_qc.csv` | Aggregated QC summary derived from validation hooks. | Declared as part of the shared artifact plan so downstream QA can diff deterministic runs. 【F:src/bioetl/pipelines/base.py†L85-L127】 |
| `<stem>_meta.yaml` | Canonical metadata record containing configuration fingerprints, schema versions, row counts, hash details, and lineage. | Captures the full structure defined in the determinism policy. 【F:docs/determinism/00-determinism-policy.md†L73-L119】 |
| `<stem>_run_manifest.json` | Run manifest enumerating generated files and checksums (extended mode). | Added by the extended artifact mode described in the activity pipeline specification. 【F:docs/pipelines/activity-chembl/09-activity-chembl-extraction.md†L1047-L1051】 |
| Logs (`/data/logs/<pipeline>/<stem>.log`) | Structured log output tied to the same stem for traceability. | Created alongside filesystem artifacts by the orchestrator. 【F:src/bioetl/pipelines/base.py†L68-L113】 |

The `meta.yaml` artifact links everything together: it lists every output file,
their hashes, the schema version, and the configuration fingerprint. This file
is the canonical lineage document and must accompany any dataset sharing. 【F:docs/determinism/00-determinism-policy.md†L73-L119】

Run manifests complement `meta.yaml` by providing a machine-readable list of
artifacts that can be ingested by downstream automation such as golden tests and
release tooling. 【F:docs/pipelines/06-activity-data-extraction.md†L1047-L1051】

## 3. Deterministic Run Example

The deterministic naming scheme makes it easy to inspect a run. For example, an
extended run of the activity pipeline on 2025-10-28 uses the stem
`activity_all_20251028`:

- Dataset and QC artifacts: `activity_all_20251028.csv`,
  `activity_all_20251028_quality_report.csv`, optional
  `activity_all_20251028_correlation_report.csv`, and the aggregated
  `activity_all_20251028_qc.csv`. 【F:docs/pipelines/06-activity-data-extraction.md†L1037-L1046】【F:src/bioetl/pipelines/base.py†L85-L127】
- Metadata: `activity_all_20251028_meta.yaml` records the configuration hash,
  schema version, row counts, hashes, QC summary, and lineage fields shown in the
  determinism policy example. 【F:docs/determinism/00-determinism-policy.md†L73-L119】
- Run manifest: `activity_all_20251028_run_manifest.json` enumerates the same
  files for artifact audits. 【F:docs/pipelines/06-activity-data-extraction.md†L1047-L1051】
- Logs: `/data/logs/activity/activity_all_20251028.log` houses the structured
  console/file log for the run, sharing the same stem. 【F:src/bioetl/pipelines/base.py†L68-L113】

The same structure applies to `document`, `target`, `assay`, and `testitem`
pipelines, with stems such as `documents_all_20251021` or `target_all_20251028`
per the catalog. 【F:docs/pipelines/10-pipelines-catalog.md†L48-L210】

## 4. Retention and Cleanup

`PipelineBase` tracks every run via the `*_meta.yaml` files and applies a
retention policy when new runs are registered. Only the most recent `N` run
stems (default 5) are preserved; older datasets, QC outputs, manifests, metadata,
and logs are pruned together so the directory never drifts out of sync. 【F:src/bioetl/pipelines/base.py†L129-L166】

Operators can raise or lower the retention count per pipeline when instantiating
the orchestrator, giving fine-grained control over how many deterministic runs
stay on disk.
