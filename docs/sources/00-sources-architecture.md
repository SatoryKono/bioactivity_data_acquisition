# Specification: Source Component Architecture

## 1. Overview

This document defines the normative architecture for data source components within the `bioetl` framework. Each pipeline, as declared in the `[ref: repo:README.md@refactoring_001]`, is powered by a stack of components responsible for fetching, parsing, and cleaning data before it reaches the main `PipelineBase` orchestrator.

The standard component stack follows this sequence:
**Client → Paginator → Parser → Normalizer → Pandera Schema**

These components are invoked during the `extract` and `transform` stages of a pipeline, which is executed via the CLI with commands like `python -m bioetl.cli.main activity`.

## 2. Layer Interfaces and Invariants

This section defines the project contract for each layer of the source component stack.

### 2.1. HTTP Client

-   **Interface**: `request(method, endpoint, **kwargs) -> Response`
-   **Implementation**: `bioetl.core.api_client.UnifiedAPIClient` (`[ref: repo:src/bioetl/core/api_client.py@refactoring_001]`)
-   **Invariants**:
    -   MUST use strict, configurable timeouts (`connect` and `read`).
    -   MUST implement an exponential backoff retry policy for transient errors (`5xx`, `429`, network failures).
    -   MUST respect the `Retry-After` header.
    -   MUST support centralized headers (e.g., `User-Agent`).
    -   MUST log key event details: endpoint, attempt number, and duration.

### 2.2. Paginator

-   **Project Contract**: `iterate(client, initial_request) -> Iterable[Response]`
-   **Implementation**: Varies by source. For ChEMBL, this logic is encapsulated within the source-specific client (e.g., `ActivityChEMBLClient`'s batching mechanism).
-   **Invariants**:
    -   MUST have a definitive stop condition (e.g., empty response, `next` link is null).
    -   MUST NOT yield duplicate data across pages.
    -   SHOULD respect rate limits by introducing delays between page requests if necessary.

### 2.3. Parser

-   **Interface**: `parse(response_content: dict) -> dict`
-   **Implementation**: Source-specific classes, e.g., `ActivityParser` (`[ref: repo:src/bioetl/sources/chembl/activity/parser/activity_parser.py@refactoring_001]`)
-   **Invariants**:
    -   MUST be a pure function with no side effects.
    -   MUST raise an explicit error (e.g., `KeyError`, `ValueError`) if the input structure is invalid.
    -   MUST return a flat dictionary.
    -   Is responsible for initial data extraction from the raw response.

### 2.4. Normalizer

-   **Interface**: `normalize(value: Any) -> Any` and/or `normalize_many(values: Iterable) -> Iterable`
-   **Implementation**: A combination of source-specific normalizer classes (e.g., `ActivityNormalizer`) and a central registry at `bioetl.normalizers.registry`.
-   **Invariants**:
    -   MUST be a pure function.
    -   MUST enforce canonical data types (e.g., convert all date strings to ISO 8601).
    -   MUST handle missing or null values predictably.
    -   MUST NOT alter the fundamental meaning of a business key.

### 2.5. Pandera Schema & Validation

-   **Interface**: `validate(dataframe: pd.DataFrame) -> pd.DataFrame`
-   **Implementation**: Handled by the `PipelineBase.validate` method, which dynamically loads a schema. `[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]`
-   **Invariants**:
    -   MUST enforce strict column sets (`strict=True`).
    -   MUST enforce a fixed column order (via `ordered=True` or an external utility).
    -   MUST coerce data to the correct types (`coerce=True`).
    -   MUST validate the uniqueness of the business key.

## 3. Interface and Implementation Matrix

A detailed matrix mapping each pipeline to its specific component implementations is available in a separate document:
-   **[Interface Matrix](./INTERFACE_MATRIX.md)**

## 4. Interaction with Schemas and Determinism Policy

-   **Schema Invocation**: The `PipelineBase.validate()` method is the single point of invocation for Pandera validation. It is called after the `transform()` stage, meaning the normalizers have already processed the data.
-   **Determinism**: The determinism policy (stable sort keys, row hashing) is applied in the `export()` method of `PipelineBase`, *after* the data has been successfully validated by its Pandera schema. This ensures that only clean, structured data is hashed and written to disk.

## 5. Error Handling and Retries

-   **Taxonomy**:
    -   **Client Layer**: Handles `requests.RequestException` and HTTP status codes (`4xx`/`5xx`).
    -   **Paginator/Parser Layers**: Handle `KeyError`, `ValueError`, `JSONDecodeError` for malformed data.
-   **Retries**: All HTTP-level retries are handled exclusively by the `UnifiedAPIClient` as described in the HTTP client specification. Logic in higher layers (Parser, Normalizer) SHOULD NOT implement its own retry mechanisms.
-   **Logging**: All HTTP events MUST include `endpoint`, `attempt`, `duration_ms`, and `retry_after` (if applicable) in their structured logs.

## 6. Integration with CLI and Configs

The CLI and configuration system are responsible for wiring the component stack together.
-   **CLI Commands**: Typer commands, defined via `[ref: repo:src/bioetl/cli/app.py@refactoring_001]`, instantiate a specific `PipelineBase` subclass.
-   **Configuration**: The pipeline's YAML config provides the `http_profile` that selects the correct network settings, as well as any source-specific parameters needed by the client or parser. The `extends` key is used to inherit from shared profiles like `base.yaml` and `network.yaml`.
-   **Cross-Navigation**: This document is linked from the main `[ref: repo:docs/INDEX.md@refactoring_001]` and provides context for the **CLI**, **Configs**, and **QC** documentation.

## 7. Examples

**Trace of a ChEMBL Activity Request:**

1.  **Client (`ActivityChEMBLClient`)**: The `extract` method is called with a list of activity IDs.
2.  **Paginator (`ActivityRequestBuilder`)**: The list of IDs is broken into batches (e.g., 25 IDs per batch).
3.  **HTTP Client (`UnifiedAPIClient`)**: A GET request is made to `/activity.json?activity_id__in=1,2,3...`.
    -   If the server returns `503`, the client waits and retries.
    -   If the server returns `429` with `Retry-After: 10`, the client waits 10 seconds and retries.
4.  **Parser (`ActivityParser`)**: The `parse` method is called for each item in the `activities` list from the JSON response. It extracts and flattens the raw dictionary.
5.  **Normalizer (`ActivityNormalizer` / `registry`)**: Within the `parse` method, functions like `registry.normalize("chemistry.chembl_id", ...)` are called to clean up specific fields.
6.  **Schema (`ActivitySchema`)**: After all activities are processed into a DataFrame, the `PipelineBase.validate()` method calls `ActivitySchema.validate(df)`.
7.  **Output**: The validated DataFrame is sorted by its stable sort keys (e.g., `["assay_id", "testitem_id", "activity_id"]` for the `activity` pipeline) before being written to a file.

## 8. Test Plan

-   **Contract Tests**: Each Parser MUST have unit tests that provide a reference API response and assert that the parsed output matches the expected dictionary structure.
-   **Pagination Tests**: Paginators MUST be tested against boundary conditions (e.g., a source that returns zero results, a source that returns exactly one page).
-   **Integration Tests**: A "dry run" test for each pipeline should execute the full stack up to the `validate` stage to ensure all components are wired correctly.
-   **Golden Tests**: The final output of a pipeline run MUST be compared against a "golden" file to ensure column order, data types, and row hashes remain stable.
