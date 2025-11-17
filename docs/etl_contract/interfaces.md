# Interface Reference

This document summarises the concrete interfaces that the BioETL runtime
exposes to pipeline authors. Each section links to the canonical implementation
under `src/bioetl/` and highlights the expected lifecycle hooks, inputs, and
outputs.

## Pipeline lifecycle (`PipelineBase` / `IPipeline`)

`PipelineBase` (see `src/bioetl/core/pipeline/base.py`) represents the
production-ready version of the conceptual `IPipeline` contract described in
[01-pipeline-contract](01-pipeline-contract.md). The class provides the
following hooks that are executed when `PipelineBase.run()` is invoked:

1. `bootstrap()` — optional preparation that runs before any IO.
2. `extract(**kwargs)` — source-specific logic that returns raw artifacts.
3. `transform(extracted, **kwargs)` — produces the canonical dataframe payload.
4. `validate(transformed, **kwargs)` — Pandera validation against the registered schema.
5. `write(validated, **kwargs)` — materialises deterministic datasets/QC bundles.
6. `postprocess(write_result, **kwargs)` — optional reporting/notifications.
7. `teardown()` — always executed to release resources.

Every derived pipeline class only needs to override the stages that require
custom logic. The base implementation handles timing, logging, retry policies,
QC orchestration, and manifest persistence so that downstream code stays
minimal and deterministic.

## HTTP clients (`BaseApiClient`)

`BaseApiClient` lives in `src/bioetl/base_classes.py` and defines the
lightweight contract implemented by resilient HTTP clients such as
`UnifiedAPIClient` (`src/bioetl/core/http/api_client.py`). The protocol exposes:

- `get(endpoint, *, params=None, headers=None)` — fetch a single resource.
- `batch_get(endpoints, *, params=None, headers=None, batch_size=None)` — iterate
  through multiple endpoints with built-in chunking.
- `search(endpoint, *, params=None, headers=None, page_size=None)` — stream
  paginated responses.
- `close()` — release HTTP sessions and circuit-breaker registrations.

Pipeline components should depend on this protocol instead of concrete client
implementations so that retry/backoff/ratelimit logic stays centralised.

## Parsers (`IParser`)

`IParser` (also defined in `src/bioetl/base_classes.py`) encapsulates the
parsing contract for raw payloads. A parser receives the unmodified response
(e.g. JSON, XML, or list of dictionaries) and yields an iterable of dictionaries
using the `parse(raw)` method. The iterable may be a generator or list, but it
must be repeatable within the same invocation so that instrumentation can
inspect the contents while the normaliser consumes the stream.

## Normalisers (`INormalizer`)

`INormalizer` resides next to `IParser` and exposes a single
`normalize(record)` method. It receives one parsed record at a time and must
return a mapping that aligns with the relevant schema in
`bioetl.schemas.pipeline_contracts`. Normalisers should raise explicit
exceptions for malformed inputs so that `PipelineBase.validate()` can surface
clear diagnostics.

## Dataset writers (`BaseDatasetWriter`)

`BaseDatasetWriter` is introduced in `src/bioetl/core/io.py` to wrap the low
level helpers such as `prepare_dataframe()` and `write_dataset_atomic()`. The
planned responsibilities are:

1. Accept a dataframe plus runtime artifacts (`RunArtifacts`).
2. Normalise column order and data types via `prepare_dataframe()`.
3. Delegate to `write_dataset_atomic()` to materialise deterministic CSV/Parquet outputs.
4. Return a `WriteResult` that `PipelineBase.write()` can pass down the
   lifecycle.

While legacy pipelines still call `write_dataset_atomic()` directly, new code
should depend on `BaseDatasetWriter.write()` so that future metadata, QC, or
compression logic can be added in a single place without touching every
pipeline implementation.
