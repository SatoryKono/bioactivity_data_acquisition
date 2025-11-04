# PipelineBase Orchestrator Specification

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document provides a comprehensive technical specification for the `PipelineBase` abstract class, which serves as the core orchestrator for all ETL pipelines within the `bioetl` framework.

[ref: repo:README.md@refactoring_001]

## 1. Overview and Goals

The `PipelineBase` class standardizes the ETL process by defining a fixed, four-stage lifecycle: `extract` → `transform` → `validate` → `write`, orchestrated by the `run()` method. Its primary purpose is to abstract away the boilerplate of pipeline orchestration, allowing developers to focus on source-specific business logic.

**Boundaries of Responsibility:**

- **`PipelineBase` (Framework)** is responsible for:
  - Orchestrating the sequence of stages.
  - Injecting configuration (`PipelineConfig`).
  - Managing the logging context and stage timers.
  - Handling exceptions at a high level.
  - Performing the final validation step against a provided schema.
  - Atomically writing the output dataset and all associated metadata artifacts.
  - Enforcing determinism through sorting and hashing.

- **Concrete Implementation (Developer)** is responsible for:
  - Implementing the logic to connect to and extract data from a specific source (e.g., API clients, database connections).
  - Implementing the business logic to parse, clean, and transform the raw data.
  - Providing the Pandera schema for the final data structure.

## 2. Public API (Signatures and Types)

The public API is defined by the `PipelineBase` abstract class and a set of dataclasses for structured results. The implementation is based on Python's `abc` module.

[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]

### Result Types

These dataclasses define the expected structure of the results returned by the `write` and `run` stages.

```python
from dataclasses import dataclass, field
from pathlib import Path

@dataclass(frozen=True)
class WriteResult:
    """Materialised artifacts produced by the write stage."""
    dataset: Path
    metadata: Path
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None
    extras: dict[str, Path] = field(default_factory=dict)

@dataclass(frozen=True)
class RunResult:
    """Final result of a pipeline execution."""
    run_id: str
    write_result: WriteResult
    run_directory: Path
    manifest: Path
    log_file: Path
    stage_durations_ms: dict[str, float] = field(default_factory=dict)
```

### `PipelineBase` Abstract Class

All pipelines **must** inherit from this class.

```python
from abc import ABC, abstractmethod
import pandas as pd
from bioetl.config import PipelineConfig

class PipelineBase(ABC):
    """Shared orchestration helpers for ETL pipelines."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        ...

    @abstractmethod
    def extract(self, *args: object, **kwargs: object) -> object:
        """Subclasses fetch raw data and return domain-specific payloads."""
        raise NotImplementedError

    @abstractmethod
    def transform(self, payload: object) -> object:
        """Subclasses transform raw payloads into normalized tabular data."""
        raise NotImplementedError

    def validate(self, payload: object) -> pd.DataFrame:
        """Validate payload against the configured Pandera schema."""
        ...

    def write(self, payload: object, artifacts: "RunArtifacts") -> "WriteResult":
        """Default deterministic write implementation used by pipelines."""
        ...

    def run(
        self,
        *args: object,
        mode: str | None = None,
        include_correlation: bool = False,
        include_qc_metrics: bool = True,
        extras: dict[str, Path] | None = None,
        **kwargs: object,
    ) -> "RunResult":
        """Execute the pipeline lifecycle and return collected artifacts."""
        ...
```

## 3. Lifecycle and `run()` Pseudocode

The `run()` method executes the ETL stages in a fixed sequence. It provides robust error handling, logging, and timing for each stage.

### Full `run()` Pseudocode

This pseudocode is a realistic representation of the orchestration logic within `PipelineBase.run()`.

```python
# This pseudocode resides within the PipelineBase class.
def run(self, *args, mode=None, **kwargs) -> "RunResult":
    """
    Orchestrates the full pipeline lifecycle: extract → transform → validate → write.
    """
    log = UnifiedLogger.get(__name__)
    UnifiedLogger.bind(
        run_id=self.run_id,
        pipeline=self.pipeline_code,
        # ... other context fields
    )

    stage_durations_ms = {}
    self._stage_durations_ms = stage_durations_ms

    log.info("pipeline_started", mode=mode)

    try:
        # --- EXTRACT STAGE ---
        with UnifiedLogger.stage("extract"):
            log.info("extract_started")
            extract_start = time.perf_counter()

            # `extract` can return any object (e.g., a list of dicts, a custom object).
            extracted_payload = self.extract(*args, **kwargs)

            stage_durations_ms["extract"] = (time.perf_counter() - extract_start) * 1000.0
            log.info("extract_completed", duration_ms=stage_durations_ms["extract"], rows=self._safe_len(extracted_payload))

        # --- TRANSFORM STAGE ---
        with UnifiedLogger.stage("transform"):
            log.info("transform_started")
            transform_start = time.perf_counter()

            # `transform` receives the payload from `extract` and can also return any object.
            transformed_payload = self.transform(extracted_payload)

            stage_durations_ms["transform"] = (time.perf_counter() - transform_start) * 1000.0
            log.info("transform_completed", duration_ms=stage_durations_ms["transform"], rows=self._safe_len(transformed_payload))

        # --- VALIDATE STAGE ---
        with UnifiedLogger.stage("validate"):
            log.info("validate_started")
            validate_start = time.perf_counter()

            # `validate` receives the payload from `transform` and MUST return a DataFrame.
            # It enforces the DataFrame contract before the write stage.
            validated_df = self.validate(transformed_payload)

            stage_durations_ms["validate"] = (time.perf_counter() - validate_start) * 1000.0
            log.info("validate_completed", duration_ms=stage_durations_ms["validate"], rows=len(validated_df))

        # --- WRITE STAGE ---
        with UnifiedLogger.stage("write"):
            # Plan all output file paths before writing.
            artifacts = self.plan_run_artifacts(run_tag=self.run_id, mode=mode)

            log.info("write_started")
            write_start = time.perf_counter()

            # The `write` method receives the validated DataFrame and the planned artifact paths.
            write_result = self.write(validated_df, artifacts)

            stage_durations_ms["write"] = (time.perf_counter() - write_start) * 1000.0
            log.info("write_completed", duration_ms=stage_durations_ms["write"])

        # Apply retention policy to clean up old runs.
        self.apply_retention_policy()
        log.info("pipeline_completed", stage_durations_ms=stage_durations_ms)

        # Construct and return the final RunResult.
        return RunResult(
            run_id=self.run_id,
            write_result=write_result,
            run_directory=artifacts.run_directory,
            manifest=artifacts.manifest,
            log_file=artifacts.log_file,
            stage_durations_ms=stage_durations_ms,
        )

    except Exception as e:
        log.error("pipeline_failed", error=str(e), exc_info=True)
        raise

    finally:
        # --- CLEANUP STAGE ---
        with UnifiedLogger.stage("cleanup"):
            log.info("cleanup_started")
            self._cleanup_registered_clients()
            self.close_resources()
            log.info("cleanup_completed")
```

### Stage Contracts and Guarantees

The real implementation in `src/bioetl/pipelines/base.py` formalises the public
contract of each lifecycle stage. The table below summarises what the
orchestrator guarantees, what implementers must provide, and the determinism or
idempotency expectations that are enforced at runtime.

| Stage | Framework responsibilities | Implementer responsibilities | Key Contract Points |
| --- | --- | --- | --- |
| `extract` | Sets logging context, records timing, handles exceptions. | Implement a side-effect-free extractor that returns a payload (`object`). | The returned payload can be any Python object (e.g., list of dicts, custom data structure), not necessarily a DataFrame. |
| `transform` | Sets logging context, records timing, passes the payload from `extract`. | Implement transformations as pure functions. | The transform stage receives the exact payload from `extract` and can also return any Python `object`. |
| `validate` | Enforces the DataFrame contract, runs Pandera validation, records QC metrics. | Do not override. The schema is specified in the pipeline configuration. | This is the first stage that **requires** a `pandas.DataFrame`. It will raise a `TypeError` if the incoming payload is not a DataFrame. |
| `write` | Applies determinism rules (sorting, hashing), writes all artifacts atomically. | Optional overrides should call `super().write()` to inherit base functionality. | Receives a validated DataFrame and a `RunArtifacts` object containing all planned output paths. |
| `cleanup` | Ensures all registered resources (e.g., API clients) are closed, even on failure. | Implement `close_resources()` to release any custom resources. | This stage is always executed, making it a reliable place for cleanup logic. |

### Retry and Backoff Expectations

`PipelineBase` centralises HTTP/client creation through helper methods that
inject the project-wide retry and backoff policies. Pipelines should:

1. Call `init_chembl_client` to obtain a `ChemblClientContext`, which wraps the
   shared `UnifiedAPIClient` so that the default retry/backoff, throttling, and
   observability policies are applied consistently.
2. Register any instantiated clients via `register_client` so that the
   orchestrator can dispose of them safely during cleanup (even on failure).
3. Prefer `read_input_table` for disk inputs—`limit`/`sample` runtime options are
   honoured automatically, missing files become structured warnings, and the
   resolved path is logged for traceability.

Custom retry loops should compose with the defaults (for example by decorating
client calls with additional `backoff.on_exception` policies) rather than
replacing them. This keeps failure semantics and observability consistent across
pipelines.【F:docs/pipelines/00-pipeline-base.md†L430-L470】

## 4. Configuration and DI

The pipeline receives its configuration via Dependency Injection (DI) in the constructor. A `PipelineConfig` object, loaded and validated from a YAML file, is passed in during initialization.

[ref: repo:src/bioetl/configs/models.py@refactoring_001]

- **Priority and Overlays**: The configuration system supports profiles via the `extends` key, allowing a pipeline-specific YAML file (e.g., `activity.yaml`) to inherit from and override values in base files (e.g., `base.yaml`, `determinism.yaml`).
- **Strictness**: The Pydantic models are configured with `extra="forbid"`. This is a critical feature that causes the configuration loading to fail if the YAML file contains any keys that are not explicitly defined in the models, preventing typos and "silent" configuration errors.

## 5. Logging and Telemetry

The framework provides a unified, structured logging system based on
`structlog`. `PipelineBase.run()` enriches every record with mandatory context
fields (`run_id`, `stage`, `actor`, `source`) and captures wall-clock durations
for extract/transform/validate/write stages in milliseconds. Additional
artifacts such as the QC summary and metadata files embed the same timings via
`stage_durations_ms`.【F:docs/pipelines/00-pipeline-base.md†L472-L522】

### Stage Event Catalogue

The table below enumerates the core log events emitted by the orchestrator. All
events inherit the mandatory fields described above, and many attach additional
payload fields as shown.

| Stage | Event | Additional fields |
| --- | --- | --- |
| Bootstrap | `pipeline_initialized` | `pipeline`, `run_id` |
| Bootstrap | `pipeline_started` | `pipeline` |
| Extract | `reading_input` | `path`, optional `limit` |
| Extract | `input_file_not_found` | `path` |
| Extract | `input_limit_active` | `limit`, `rows` |
| Extract | `extraction_completed` | `rows`, `duration_ms` |
| Transform | `transformation_completed` | `rows`, `duration_ms` |
| Transform | `enrichment_stage_*` | `stage`, plus `reason`, `rows`, or `error` |
| Validate | `schema_validation_error` | `dataset`, `column`, `check`, `count`, `severity` |
| Validate | `schema_validation_failed` | `dataset`, `errors`, `error` |
| Validate | `validation_completed` | `rows`, `duration_ms` |
| Export | `exporting_data` | `path`, `rows` |
| Export | `pipeline_completed` | `artifacts`, `load_duration_ms` |
| Cleanup | `pipeline_resource_cleanup_failed` | `error` |
| Any | `pipeline_failed` | `error`, `exc_info` |

These events, combined with the logger's redaction processors, form the minimum
telemetry contract. Pipelines may emit additional structured logs (for example,
per-API-call retries) provided they do not remove the baseline context.

### 5.1. Logging Structure

All pipeline logs use structured JSON format via `structlog` with mandatory context fields automatically injected by `PipelineBase.run()`. Every log record includes the following mandatory fields:

- **`run_id`**: Unique identifier for the pipeline run (UUID format)
- **`stage`**: Current pipeline stage (`bootstrap`, `extract`, `transform`, `validate`, `write`, `cleanup`)
- **`actor`**: Pipeline name (e.g., `activity_chembl`, `target_uniprot`)
- **`source`**: Data source identifier (e.g., `chembl`, `uniprot`, `pubmed`)
- **`timestamp`**: ISO-8601 UTC timestamp

#### Stage-Specific Events

All pipelines follow a standard event pattern for each stage:

**Extract Stage:**

- `extraction_started`: Batch extraction begins
- `extraction_completed`: Batch extraction completes with `rows` and `duration_ms`
- `extraction_failed`: Extraction error with details

**Transform Stage:**

- `transformation_started`: Transformation begins
- `transformation_completed`: Transformation completes with `rows` and `duration_ms`

**Validate Stage:**

- `validation_started`: Schema validation begins
- `validation_completed`: Validation passes with `rows` and `duration_ms`
- `validation_failed`: Validation errors with details

**Write Stage:**

- `export_started`: File writing begins with `path` and `rows`
- `export_completed`: All artifacts written successfully with `artifacts` path

**Error Handling:**

- `pipeline_failed`: Any stage failure with `error` and `exc_info` fields

Pipeline-specific documentation should reference this section for general logging structure and only document pipeline-specific actor values or additional events that differ from this standard pattern.

### Extension Hooks

Implementers can extend `PipelineBase` behaviour without reimplementing the
orchestrator by using the following hook surface:

- **Abstract methods** – `extract`, `transform`, and `close_resources` must be implemented by every concrete pipeline. `validate` should only be overridden to customise schema loading.
- **Stage utilities** – `read_input_table`, `execute_enrichment_stages`, `run_schema_validation`, and `finalize_with_standard_metadata` encapsulate shared orchestration logic and should be preferred over bespoke implementations.
- **QC helpers** – `set_stage_summary`, `add_qc_summary_section(s)`, `set_qc_metrics`, `record_validation_issue`, `refresh_validation_issue_summary` keep validation and enrichment telemetry consistent.
- **Metadata helpers** – `set_export_metadata_from_dataframe`, `set_export_metadata`, and `add_additional_table` feed the writer with the required context for deterministic artefact generation.
- **Client lifecycle** – `init_chembl_client`, `register_client`, and `reset_stage_context` provide safe resource management and per-stage state.

All hooks are idempotent when invoked with identical inputs and configuration,
supporting reproducible pipeline runs.【F:docs/pipelines/00-pipeline-base.md†L524-L586】

## 6. Determinism and Artifacts

The framework guarantees that a pipeline run with the same configuration will produce a bit-for-bit identical output.

- **Stable Sorting**: Before writing, the final DataFrame is sorted by the columns specified in the `determinism.sort.by` key in the configuration.
- **Integrity Hashes**: The `write` stage calculates two critical hashes that are stored in `meta.yaml`:
  - `hash_business_key`: A hash of the columns forming the unique business identifier.
  - `hash_row`: A hash of all columns specified, ensuring row-level integrity.
- **`meta.yaml`**: This artifact is the run's "birth certificate," recording the `run_id`, configuration hash, source versions, `row_count`, all hashes, and stage timings.
- **Invariant**: A repeated run with an identical configuration against an identical source state **must** produce identical `meta.yaml` hashes and an identical primary dataset file hash.

### 6.1. I/O and Artifacts

All pipelines generate a standard set of output artifacts with consistent formatting and atomic write guarantees.

#### Output Files

Every pipeline generates three core artifacts:

1. **Primary Dataset** (`{entity}_{date}.csv`): Main dataset with all records
2. **Quality Report** (`{entity}_{date}_quality_report.csv`): QC metrics and statistics
3. **Metadata** (`{entity}_{date}_meta.yaml`): Metadata and lineage information

The entity name and date format are pipeline-specific, but the naming pattern is consistent across all pipelines.

#### CSV Format

All CSV files follow these specifications:

- **Encoding**: UTF-8
- **Separator**: Comma (`,`)
- **Header Row**: First row contains column names matching Pandera schema order
- **Row Ordering**: Stable sorting by columns specified in `determinism.sort.by` configuration
- **Line Endings**: Unix-style (`\n`)

#### Metadata Format

The `meta.yaml` file is a YAML-formatted document with deterministic key ordering. It includes:

- **Pipeline Information**: `pipeline_version`, `pipeline_name`, `entity`
- **Run Information**: `run_id`, `git_commit`, `config_hash`, `generated_at_utc`
- **Data Statistics**: `row_count`, `schema_version`
- **Integrity Hashes**: `hash_business_key`, `hash_row`, `blake2_checksum`
- **Performance Metrics**: `stage_durations_ms` (extract, transform, validate, write)
- **Source Versions**: Source-specific version information (e.g., ChEMBL release version)

#### Atomic Writing

All file writes use atomic operations to prevent partial writes and ensure data integrity:

1. **Write to temporary file**: Data is written to a temporary file with `.tmp` suffix
2. **Flush and sync**: Data is flushed to disk and synced using `os.fsync()`
3. **Atomic rename**: Temporary file is atomically renamed to final filename using `os.replace()`

This ensures that either the complete file is present or no file exists, preventing corruption from interrupted writes.

Pipeline-specific documentation should reference this section for general I/O format and atomic writing guarantees, and only document pipeline-specific file names and sort keys.

## 7. Validation Contracts

Data validation is a non-negotiable stage of the pipeline, enforced by Pandera schemas.

- **Mandatory Checks**: Every output schema **must** enforce:
  - **Fixed Column Order**: `class Config: ordered = True`.
  - **Strict Data Types**: All columns must have a specific type (e.g., `Series[Int64]`). Coercion is allowed (`pa.Field(coerce=True)`).
  - **Business Key Uniqueness**: The primary business key column must be marked with `unique=True`.
- **Schema Evolution**: Schemas must be versioned. Any backward-incompatible changes require a major version bump. The schema version is recorded in the `meta.yaml` file.

## 8. CLI Integration

Pipelines are exposed as commands in the Typer-based CLI via a static registry.

[ref: repo:src/bioetl/cli/registry.py@refactoring_001]

- **Registration**: To add a new pipeline command, it must be explicitly imported and added to the `PIPELINE_REGISTRY` in `src/bioetl/cli/registry.py`. The framework does **not** use automatic discovery.
- **Required Flags**: All pipeline commands accept a standard set of flags:
  - `--config <path>`: Path to the pipeline's YAML configuration.
  - The output directory is now specified inside the configuration file (`materialization.root`) and is not a required CLI flag.
- **Exit Codes**: The CLI returns a `0` exit code on success and a non-zero exit code on failure, as defined in `src/bioetl/cli/command.py`.

**Example Invocation:**

```bash
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml
```

### 8.1. Standard CLI Flags

All pipeline commands follow a consistent CLI contract. The following flags are standardized across all pipelines:

#### Required Flags

- **`--config <path>`**: Path to the pipeline's YAML configuration file. The path may be absolute or relative to the current working directory. The configuration file must be valid YAML and conform to the pipeline's configuration schema.

#### Optional Flags

- **`--limit <number>`**: Limit the number of records to process (for testing). Useful for quick smoke tests or debugging. The limit is applied during the extract stage.

- **`--set <key>=<value>`**: Override configuration values at runtime. The key follows dot notation (e.g., `sources.chembl.batch_size=10`). Multiple overrides can be specified by repeating the flag.

#### Exit Codes

The CLI uses a standardized set of exit codes to indicate pipeline execution status:

- **`0`**: Success - Pipeline completed successfully
- **`1`**: Application Error - A general error occurred during pipeline execution (e.g., validation, extraction, transformation, or write failure).
- **`2`**: Usage Error - An error related to the command-line interface usage (e.g., invalid parameters, configuration file not found).

Pipeline-specific documentation should reference this section for general CLI flag descriptions and exit codes, and only document pipeline-specific command names or additional flags that differ from this standard contract.

## 9. Test Plan

- **Unit Tests**:
  - Verify that the `run()` orchestrator calls the stages in the correct order (`extract` -> `transform` -> `validate` -> `write`).
  - Test that an exception raised in any stage is caught, logged with `exc_info`, and correctly re-raised.
  - Test that the `RunResult` object is constructed correctly with all expected paths.
- **Golden Test**:
  - Create a test that runs a pipeline and captures the primary dataset file and the `meta.yaml` file.
  - Compare these artifacts against a pre-approved "golden" version committed to the repository. The test fails if there is any byte-level difference, ensuring determinism.
- **Integration Test**:
  - Write a test that invokes the pipeline via the CLI (`subprocess.run`).
  - Assert that a run with the `--dry-run` flag completes with exit code `0`, produces no files in the output directory, and emits valid log messages.

## 10. Minimal Example

### Minimal `PipelineBase` Subclass

```python
# file: src/bioetl/pipelines/minimal_example.py

import pandas as pd
from bioetl.pipelines.base import PipelineBase

class MinimalPipeline(PipelineBase):

    @abstractmethod
    def close_resources(self) -> None:
        """Required by ABC, even if empty."""
        pass

    def extract(self) -> pd.DataFrame:
        print("Extracting data...")
        # In a real pipeline, this would connect to a source.
        return pd.DataFrame([{"id": 1, "data": "raw"}])

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Transforming data...")
        # In a real pipeline, this would apply business logic.
        df["data"] = df["data"].str.upper()
        df["transformed"] = True
        return df
```

### Stage Mapping Table

| Stage       | Extension Points (Developer Implements) | Invariants (Framework Guarantees)                               |
|-------------|-----------------------------------------|-----------------------------------------------------------------|
| **`extract`**   | `extract()` method, API clients         | A `pd.DataFrame` is produced; stage is timed and logged.        |
| **`transform`** | `transform()` method, normalizers       | A `pd.DataFrame` is produced; stage is timed and logged.        |
| **`validate`**  | Pandera schema (`schema_out.py`)        | Fails fast on any schema violation; enforces column order.      |
| **`write`**     | (None)                                  | Output is atomic; `meta.yaml` is generated; data is sorted.     |
| **`run`**       | (None)                                  | Stages run in fixed order; exceptions are handled.              |
| **`cleanup`**   | `close_resources()` method              | Always called, even on failure.                                 |
