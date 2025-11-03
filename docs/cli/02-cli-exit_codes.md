# CLI Exit Codes

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document provides a reference for the exit codes returned by the `bioetl` CLI and maps them to specific error scenarios. Understanding these codes is essential for scripting, automation, and CI/CD integration.

For a general overview of the CLI, see `[ref: repo:docs/cli/00-cli-overview.md@refactoring_001]`.

## 1. Exit Code Summary Table

| Exit Code | Condition | Meaning |
|---|---|---|
| `0` | Success | The pipeline completed all stages successfully. |
| `1` | Application Error | A critical error occurred during the pipeline's execution, and the process was halted. |
| `2` | Usage Error | The command was invoked with invalid parameters or options. |

## 2. Exception and Scenario Mapping

The exit code is determined by the type of exception that occurs during the application's lifecycle, based on the error handling in `[ref: repo:src/bioetl/cli/command.py@refactoring_001]`.

### Exit Code `0`: Success

This code is returned only when the entire `pipeline.run()` method completes without raising any exceptions.

-   **Scenario**: A standard, successful pipeline run, including a `--dry-run`.

### Exit Code `1`: Application Error

This is the most common failure code and is triggered by a general `try...except Exception` block that wraps the main pipeline execution. Any unhandled exception from the pipeline or its components will result in this exit code.

-   **Scenario: Configuration Errors**
    -   `FileNotFoundError`: The specified `--config` file does not exist.
    -   `pydantic.ValidationError`: The configuration file is invalid (e.g., missing required keys, incorrect data types).
-   **Scenario: Data Validation Errors**
    -   `pandera.errors.SchemaError`: The data failed validation against the Pandera schema during the `validate` stage.
    -   `ValueError`: Raised if the final column order does not match the schema's expected order.
-   **Scenario: Extraction and HTTP Client Errors**
    -   `requests.exceptions.HTTPError`: The API returned a critical error status code (e.g., 500 Internal Server Error) that was not resolved by retries.
    -   `requests.exceptions.Timeout`: The request to the data source timed out.
    -   `requests.exceptions.ConnectionError`: A network-level error occurred (e.g., DNS failure).
-   **Scenario: Filesystem Errors**
    -   `PermissionError`: The application does not have the necessary permissions to write to the `--output-dir`.

### Exit Code `2`: Usage Error

This exit code is typically managed by the `typer` library and indicates that the user has provided invalid command-line arguments.

-   **Scenario: Invalid Options**
    -   Providing a non-existent option (e.g., `--non-existent-flag`).
    -   Providing an invalid value for an option (e.g., `--sample -5`).
-   **Scenario: Missing Required Options**
    -   Failing to provide a required option, such as `--output-dir` or `--config`.

## 3. Behavior in CI/CD Environments

When integrating the CLI into automated environments, you **MUST** check the exit code after every execution.

-   A script **SHOULD** treat any non-zero exit code as a failure.
-   `if ! python -m bioetl.cli.main activity ...; then echo "Pipeline failed!"; exit 1; fi`
-   **Exit Code `1`** indicates a problem with the pipeline's execution, configuration, or the data itself. The logs from the run are essential for diagnosing the root cause.
-   **Exit Code `2`** indicates a problem with how the CI script is *invoking* the command. This is a script-level bug and should be fixed in the CI configuration.
