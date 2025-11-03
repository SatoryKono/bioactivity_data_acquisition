# Quality Control and Assurance Framework

This document provides an overview of the Quality Control (QC) and Quality Assurance (QA) methodology used in the `bioetl` framework. Its purpose is to ensure that all data processed by the pipelines is reliable, reproducible, and meets a high standard of quality.

## 1. Core Principles

The QC/QA framework is built on four pillars of data quality control, which are checked at different stages of the ETL process.

### 1.1. Structural Integrity (Schema Validation)

This is the first and most critical level of control. It ensures that the data conforms to its expected physical structure.

-   **Mechanism**: [Pandera schemas][pandera-docs] are used to define the strict structure of a dataset, including column names, data types, and order.
-   **Enforcement**: The `validate` stage of the `PipelineBase` orchestrator performs a rigorous check against the defined Pandera schema. Any violation results in an immediate pipeline failure.
-   **Invariant**: All schemas **MUST** be defined with `strict=True`, `ordered=True`, and `coerce=True` to prevent unexpected columns, enforce a canonical order, and ensure type safety.

### 1.2. Content Quality (Expectations)

This level of control validates the semantic correctness and plausibility of the data *within* the columns.

-   **Mechanism**: The framework uses a system of "Expectations," which are verifiable assertions about the data content, analogous to the concepts in [Great Expectations][ge-docs]. These include checks for uniqueness, non-nullability, value ranges, and set membership.
-   **Enforcement**: Content quality checks are performed during the `transform` and `validate` stages. Metrics are collected, and failures can be configured to either raise warnings or cause the pipeline to fail.

### 1.3. Determinism and Lineage (Reproducibility)

This level ensures that a pipeline run is repeatable and that its outputs are verifiable.

-   **Mechanism**: Every pipeline run generates a `meta.yaml` file containing a complete lineage of the process. This includes input parameters, configuration hashes, the schema version, and cryptographic hashes of the output data.
-   **Enforcement**: The `write` stage is responsible for sorting the data by a stable key and calculating `hash_row` and `hash_business_key` values. These hashes provide a cryptographic guarantee of the data's integrity and are used in [Golden Tests](02-golden-tests.md) to detect regressions.

### 1.4. Extraction and Network Reliability

This level of control monitors the health and reliability of the data extraction process itself.

-   **Mechanism**: The `UnifiedAPIClient` and other source clients are instrumented to produce structured logs and metrics for all network operations.
-   **Enforcement**: The framework tracks metrics such as HTTP error rates, retries, timeouts, and parsing errors. These are aggregated and reported at the end of a run, and can be used to trigger alerts if they exceed predefined thresholds.

## 2. Integration with the Pipeline Lifecycle

The QC checks are not a separate process but are deeply integrated into the `PipelineBase` lifecycle to ensure continuous validation.

-   **`extract`**: During this stage, the framework collects metrics on network reliability and data source health (e.g., `http_error_rate`, `retry_count_total`).
-   **`transform`**: This stage is where content quality checks are often performed. Custom validation rules and expectations can be applied as data is normalized and cleaned.
-   **`validate`**: This is the primary gate for data quality. The framework performs a mandatory, strict validation of the transformed data against its Pandera schema. No data can proceed to the `write` stage without passing this check.
-   **`write`**: In this final stage, the framework ensures determinism by sorting the data, calculating integrity hashes, and generating the final `meta.yaml` artifact.

This integrated approach guarantees that all data produced by the framework has been rigorously vetted at every stage of its lifecycle.

[pandera-docs]: https://pandera.readthedocs.io/en/stable/
[ge-docs]: https://docs.greatexpectations.io/docs/
