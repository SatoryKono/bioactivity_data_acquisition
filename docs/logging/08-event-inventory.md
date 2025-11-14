# 8. Event Inventory and Naming Scheme

## Overview

All log identifiers are defined in `bioetl.core.logging.log_events.LogEvents` and now
adhere to a strict `namespace.action.suffix` structure. Only lowercase ASCII
letters, digits, underscores and dashes are permitted inside individual segments.
Dynamic identifiers, when needed, must follow the same scheme explicitly rather
than relying on helper factories.

Legacy identifiers have been normalised by replacing snake_case segments with
dot-separated components. For example, `cli_run_start` became `cli.run.start`.
The following tables summarise the canonical identifiers for the primary event
groups.

## CLI lifecycle

| Legacy identifier | New identifier  | Description                        |
| ----------------- | --------------- | ---------------------------------- |
| `cli_run_start`   | `cli.run.start` | CLI command entered the execution. |
| `cli_run_finish`  | `cli.run.finish`| CLI command finished successfully. |
| `cli_run_error`   | `cli.run.error` | CLI command terminated with error. |

## Pipeline stages

| Identifier                             | Emitted when                        |
| -------------------------------------- | ----------------------------------- |
| `stage.run.start`                      | Pipeline orchestration started.     |
| `stage.run.finish`                     | Pipeline orchestration finished.    |
| `stage.run.error`                      | Orchestration failed.               |
| `stage.extract.start` / `finish`       | Extract stage lifecycle markers.    |
| `stage.transform.start` / `finish`     | Transform stage lifecycle markers.  |
| `stage.validate.start` / `finish`      | Validation stage lifecycle markers. |
| `stage.write.start` / `finish`         | Write stage lifecycle markers.      |
| `stage.cleanup.start` / `finish`       | Post-run cleanup lifecycle markers. |
| `stage.cleanup.error`                  | Post-run cleanup failure.           |

## Client and HTTP events

| Identifier                     | Description                                                 |
| ------------------------------ | ----------------------------------------------------------- |
| `client.circuit.open`          | Circuit breaker transitioned to OPEN.                       |
| `client.cleanup.failed`        | Client cleanup hook failed to complete.                     |
| `client.factory.build`         | Client factory instantiated a provider-specific client.     |
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

## Migration checklist

- Replace ad-hoc string literals (`"extract_started"`, `"cli_pipeline_started"`,
  …) with the corresponding `LogEvents` members.
- When a dynamic identifier is necessary, build the dotted string explicitly
  (for example `f"stage.{stage}.{suffix}"`) and ensure it matches the canonical
  pattern.
- Ensure that tests asserting on log output expect the dot-separated
  identifiers.
