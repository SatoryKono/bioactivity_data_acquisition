# 8. Event Inventory and Naming Scheme

## Overview

All log identifiers are defined in `bioetl.core.log_events.LogEvents` and now
adhere to a strict `namespace.action.suffix` structure. Only lowercase ASCII
letters, digits, underscores and dashes are permitted inside individual
segments. Helper factories `stage_event()` and `client_event()` produce
compliant identifiers for dynamic scenarios.

Legacy identifiers have been normalised by replacing snake_case segments with
dot-separated components. For example, `cli_run_start` became `cli.run.start`.
The following tables summarise the canonical identifiers for the primary event
groups.

## CLI lifecycle

| Legacy identifier        | New identifier        | Description                            |
| ------------------------ | --------------------- | -------------------------------------- |
| `cli_run_start`          | `cli.run.start`       | CLI command entered the execution.     |
| `cli_run_finish`         | `cli.run.finish`      | CLI command finished successfully.     |
| `cli_run_error`          | `cli.run.error`       | CLI command terminated with error.     |
| `cli_pipeline_started`   | `cli.pipeline.start`  | Pipeline entry resolved & initialised. |
| `cli_pipeline_completed` | `cli.pipeline.finish` | Pipeline run completed.                |
| `cli_pipeline_failed`    | `cli.pipeline.error`  | Pipeline run failed.                   |

## Pipeline stages

| Identifier                                   | Emitted when                        |
| -------------------------------------------- | ----------------------------------- |
| `stage.run.start`                            | Pipeline orchestration started.     |
| `stage.run.finish`                           | Pipeline orchestration finished.    |
| `stage.run.error`                            | Orchestration failed.               |
| `stage.extract.start` / `finish` / `error`   | Extract stage lifecycle markers.    |
| `stage.transform.start` / `finish` / `error` | Transform stage lifecycle markers.  |
| `stage.validate.start` / `finish` / `error`  | Validation stage lifecycle markers. |
| `stage.write.start` / `finish` / `error`     | Write stage lifecycle markers.      |
| `stage.cleanup.start` / `finish` / `error`   | Post-run cleanup lifecycle markers. |

Dynamic stage identifiers can be generated through
`stage_event(stage_name, suffix)` to maintain the same namespace pattern.

## Client and HTTP events

| Identifier                     | Description                                                 |
| ------------------------------ | ----------------------------------------------------------- |
| `client.request.sent`          | Outgoing HTTP request dispatched.                           |
| `client.request.retry`         | Retrying the same request with backoff strategy.            |
| `client.rate_limit.hit`        | Request throttled by rate limiter before execution.         |
| `client.circuit.open`          | Circuit breaker transitioned to OPEN.                       |
| `http.rate.limiter.wait`       | Current request delayed because of rate limiter token wait. |
| `http.request.completed`       | HTTP request completed successfully.                        |
| `http.request.retry`           | HTTP response indicated retryable condition (4xx/5xx).      |
| `http.request.failed`          | HTTP request finished with non-retryable error.             |
| `http.request.exception`       | HTTP layer raised `RequestException`.                       |
| `http.request.method_override` | Automatic method override from GET to POST triggered.       |
| `http.resolve.url`             | Request URL reconstructed with base URL + endpoint.         |

## Domain-specific events

All remaining identifiers follow the same transformation rule—snake_case
segments are split into namespace, action and suffix. For example:

- `enrichment_fetching_terms` → `enrichment.fetching.terms`
- `validation_failure_cases` → `validation.failure.cases`
- `write_artifacts_prepared` → `write.artifacts.prepared`

Corresponding Python members retain their existing names (e.g.
`LogEvents.ENRICHMENT_FETCHING_TERMS`), so the update is backwards compatible
for call sites that already relied on the enumeration.

## Helper factories

- `stage_event(stage: str, suffix: str)` constructs deterministic identifiers
  such as `stage.transform.finish`. Both arguments must match `[a-z0-9_-]+`.
- `client_event(name: str)` maps a human-readable short name (`"request"`,
  `"retry"`, `"rate_limit"`, `"circuit_open"`) to the canonical constant.

## Migration checklist

- Replace ad-hoc string literals (`"extract_started"`, `"cli_pipeline_started"`,
  …) with the corresponding `LogEvents` members.
- For dynamically assembled identifiers, use `stage_event()` or `client_event()`
  instead of manual concatenation.
- Ensure that tests asserting on log output expect the dot-separated
  identifiers.
