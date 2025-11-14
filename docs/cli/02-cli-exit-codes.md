# CLI Exit Codes

> **Note**: The Typer CLI shipped in `bioetl.cli.cli_app` is available today.
> All paths referencing `src/bioetl/` correspond to the editable source tree
> that backs the installed package.

This document provides a reference for the exit codes returned by the `bioetl`
CLI and maps them to specific error scenarios. Understanding these codes is
essential for scripting, automation, and CI/CD integration.

For a general overview of the CLI, see
`[ref: repo:docs/cli/00-cli-overview.md@refactoring_001]`.

## 1. Exit Code Summary Table

| Exit Code | Condition            | Meaning                                                                                   | Primary surface                                                                        |
| --------- | -------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `0`       | Success              | The pipeline completed all stages successfully.                                           | Process exits normally after printing the execution summary.                           |
| `1`       | Application error    | A runtime failure occurred inside the pipeline, column validator, or supporting services. | `typer.Exit(code=1)` raised with a structured error message and `pipeline_failed` log. |
| `2`       | Usage error          | Typer rejected the CLI invocation before the pipeline started.                            | Typer prints a usage banner and exits with code `2`.                                   |
| `3`       | External API failure | Unified API client surfaced a network/HTTP error after exhausting retries.                | `typer.Exit(code=3)` raised; message includes `External API failure` and details.      |

## 2. Exception → Exit Code Matrix

The CLI wraps every pipeline command in
`[ref: repo:src/bioetl/cli/cli_command.py@refactoring_001]`. The command raises
`typer.Exit(code=1)` for any unhandled internal exception and
`typer.Exit(code=3)` for upstream HTTP/API failures so that automation receives
deterministic failure codes. Only `typer.BadParameter` exceptions are re-raised,
allowing Typer to exit with `code=2` and display the usage banner. Unit tests in
`[ref: repo:tests/bioetl/cli/test_cli.py@refactoring_001]` assert that simulated
external API failures exit with code `3`.

| Exit Code | Exception(s) (примеры)                                                                            | Raised by                                                                                |
| --------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `1`       | `FileNotFoundError`, `PermissionError`, `ValueError`                                              | Внутри выполнения пайплайна (см. `typer.Exit(code=1)` в `src/bioetl/cli/cli_command.py`) |
| `2`       | Ошибки валидации Typer/аргументов (`typer.BadParameter`)                                          | До старта пайплайна (`Typer` завершает процесс с `Exit(code=2)`)                         |
| `3`       | `ConnectionError`, `TimeoutError`, `requests.exceptions.Timeout`, `requests.exceptions.HTTPError` | Внешний HTTP/API клиент (`typer.Exit(code=3)` в обёртке пайплайна)                       |

| Exit Code | Exception(s)                                                                                            | Raised by                                                                                      | Typical root cause                                                                                             |
| --------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `0`       | _n/a_                                                                                                   | Normal completion                                                                              | Pipeline stages (`extract`, `transform`, `load`) all returned without raising.                                 |
| `1`       | `FileNotFoundError`, `PermissionError`, `ValueError`                                                    | Config loader `[ref: repo:src/bioetl/config/loader.py@refactoring_001]`                        | Missing YAML config, circular `extends/!include` reference, or writing to an unwritable directory.             |
| `1`       | `pydantic.ValidationError`                                                                              | Config model validation `[ref: repo:src/bioetl/config/models.py@refactoring_001]`              | YAML parsed successfully but violated the typed configuration contract (e.g., wrong field types).              |
| `1`       | `pandera.errors.SchemaErrors`, `ValueError`                                                             | Pipeline validation `[ref: repo:src/bioetl/pipelines/base.py@refactoring_001]`                 | Dataset failed schema validation or produced unexpected column ordering.                                       |
| `3`       | `requests.exceptions.Timeout`, `requests.exceptions.ConnectionError`, `requests.exceptions.ReadTimeout` | Unified HTTP client `[ref: repo:src/bioetl/core/api_client.py@refactoring_001]`                | Upstream API timed out or the network connection dropped after all retries; CLI raises `External API failure`. |
| `3`       | `requests.exceptions.HTTPError` (HTTP 4xx/5xx)                                                          | Unified HTTP client `[ref: repo:src/bioetl/core/api_client.py@refactoring_001]`                | Upstream API returned an error status after retries; CLI converts to `External API failure`.                   |
| `3`       | `CircuitBreakerOpenError`, `RateLimitExceeded`, `PartialFailure`                                        | Circuit breaker & fallback manager `[ref: repo:src/bioetl/core/api_client.py@refactoring_001]` | Repeated downstream failures tripped safeguards; run terminates with code `3`.                                 |
| `1`       | `typer.Exit(1)`                                                                                         | Column validator `[ref: repo:src/bioetl/cli/cli_command.py@refactoring_001]`                   | Column comparison detected missing/extra columns and aborted the run after printing the validation report.     |
| `2`       | `typer.BadParameter`                                                                                    | CLI option validators `[ref: repo:src/bioetl/cli/cli_command.py@refactoring_001]`              | Mutually exclusive `--sample/--limit`, out-of-range sample size, or unsupported `--mode`.                      |

Инструменты из `bioetl.cli.tools.*` используют тот же контракт через
`bioetl.cli.tools.emit_tool_error`. Внешние HTTP/ChEMBL ошибки завершаются кодом
`3`, остальные — кодом `1`, при этом stderr сообщение стандартизовано и
дублируется структурным логом `LogEvents.CLI_RUN_ERROR`.

### HTTP-specific behaviour

The resilient HTTP layer retries transient errors before surfacing them as
`requests.exceptions.HTTPError`. Once the exception propagates, the CLI converts
it into exit code `3` with the message prefix `External API failure`. The table
below highlights the most common status codes:

| Status code             | Exception propagated            | Exit code | Notes                                                                                                                                               |
| ----------------------- | ------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `429 Too Many Requests` | `requests.exceptions.HTTPError` | `3`       | The client honours `Retry-After` headers and sleeps before retrying. Continued 429 responses terminate the run with `External API failure` message. |
| `5xx` (server errors)   | `requests.exceptions.HTTPError` | `3`       | Treated as retryable by default; after the retry budget is exhausted the final 5xx leads to exit code `3`.                                          |
| `4xx` (client errors)   | `requests.exceptions.HTTPError` | `3`       | Non-429 4xx responses are considered permanent; the client logs `client_error_giving_up` and raises `External API failure`.                         |
| `Timeout / ReadTimeout` | `requests.exceptions.Timeout`   | `3`       | Propagated when all retry attempts fail or when recursive batch splitting still times out (document pipeline).                                      |

## 3. Example output by exit code

### Exit code `0`: successful run

```text
$ python -m bioetl.cli.cli_app activity_chembl --config configs/pipelines/activity/activity_chembl.yaml
=== Pipeline Execution Summary ===
Dataset: data/output/activity_20250115.csv
Quality report: data/output/activity_20250115_qc.csv
QC summary: data/output/activity_20250115_summary.json
$ echo $?
0
```

### Exit code `1`: runtime failure surfaced by the CLI

Example: column validation detected missing columns.

```text
$ python -m bioetl.cli.cli_app document_chembl --config configs/pipelines/document/document_chembl.yaml --validate-columns
...
Критические несоответствия в колонках обнаружены!
[ERROR] Pipeline failed: Column validation requested exit
$ echo $?
1
```

### Exit code `3`: External API failure propagated by the CLI

Example: upstream API returned HTTP 503 after exhausting retries.

```shell
$ python -m bioetl.cli.cli_app activity_chembl --config configs/pipelines/activity/activity_chembl.yaml
[2025-01-15T09:12:44Z] WARNING bioetl.core.http.api_client retry_after_header wait_seconds=2.0 retry_after_raw="2"
[2025-01-15T09:12:47Z] ERROR   cli.activity external_api_failure error="503 Server Error: Service Unavailable for url: https://www.ebi.ac.uk/chembl/api/data/activity.json"
[ERROR] External API failure: 503 Server Error: Service Unavailable for url: https://www.ebi.ac.uk/chembl/api/data/activity.json
$ echo $?
3

```

### Exit code `2`: usage error rejected by Typer

```shell
$ python -m bioetl.cli.cli_app activity_chembl --sample 0
Usage: python -m bioetl.cli.cli_app activity_chembl [OPTIONS]
Try 'python -m bioetl.cli.cli_app activity_chembl --help' for help.

Error: Invalid value for '--sample': --sample must be >= 1
$ echo $?
2
```

## 4. Behavior in CI/CD environments

When integrating the CLI into automated environments, you **MUST** check the
exit code after every execution.

- A script **SHOULD** treat any non-zero exit code as a failure.
- `if ! python -m bioetl.cli.cli_app activity_chembl ...; then echo "Pipeline failed!"; exit 1; fi`
- **Exit code `1`** indicates a problem with the pipeline's execution,
  configuration, or the data itself. The logs from the run are essential for
  diagnosing the root cause.
- **Exit code `2`** indicates a problem with how the CI script is _invoking_ the
  command. This is a script-level bug and should be fixed in the CI
  configuration.
- **Exit code `3`** indicates the CLI exhausted its retry budget against an
  external API. Retry the job after the upstream dependency stabilises or
  investigate connectivity.
