# 3. Output Formats and Determinism

## Overview

The `bioetl` logging system is designed to produce output that is both human-readable during development and machine-parsable in production. This is achieved by using different `structlog` renderers based on the `LoggerConfig` settings. The system also includes built-in log rotation to manage file sizes.

A key feature of the system is its guarantee of **deterministic logs** in the file-based JSON format, which is essential for auditing and automated testing.

## Environment-Specific Formats

The logging system supports two primary output formats, controlled by the `console_format` and `file_format` options in the `LoggerConfig`.

### Console Format (`console_format`)

This setting controls the format of logs written to standard output.

1.  **`"text"` (Default)**: This format uses `structlog.dev.KeyValueRenderer`. It is designed for local development and produces colorful, easy-to-read `key=value` pairs. It is not suitable for production as it is not strictly machine-parsable.

    **Example Text Output:**
    ```
    2025-11-02T10:30:00.123Z [info     ] Extraction complete.         run_id=r-abc-123 stage=extract rows=5000
    ```

2.  **`"json"`**: This format uses `structlog.processors.JSONRenderer`. It produces a single, minified JSON object per log record. This is the recommended format for containerized environments (e.g., Docker, Kubernetes) where logs are collected and forwarded by an external agent.

    **Example JSON Output (formatted for readability):**
    ```json
    {
      "generated_at": "2025-11-02T10:30:00.123Z",
      "level": "info",
      "message": "Extraction complete.",
      "run_id": "r-abc-123",
      "stage": "extract",
      "rows": 5000
    }
    ```

### File Format (`file_format`)

This setting controls the format of logs written to a file, as configured by `file_path`.

1.  **`"json"` (Only option)**: To ensure logs are always machine-parsable and suitable for production analysis, the file output format is fixed to JSON.

## Log Rotation

To prevent log files from growing indefinitely, the system uses Python's standard `logging.handlers.RotatingFileHandler`. This behavior is controlled by two parameters in the `LoggerConfig`:

-   **`max_bytes`**: The maximum size (in bytes) that a log file can reach before it is "rolled over." When the current log file exceeds this size, it is renamed (e.g., from `app.log` to `app.log.1`), and a new `app.log` is started.
-   **`backup_count`**: The number of historical log files to keep. In the example above, if `backup_count` is 10, the system will keep `app.log.1`, `app.log.2`, ..., `app.log.10`. When the next rollover occurs, `app.log.10` is deleted.

## Deterministic Logs

For auditing and testing purposes, it is critical that identical events produce bit-for-bit identical log records. The logging system guarantees this for its JSON output by configuring the `JSONRenderer` with specific parameters:

-   **`sort_keys=True`**: This is the most important setting for determinism. It ensures that the keys in the final JSON object are always written in alphabetical order. Without this, the same key-value pairs could be written in a different order on different runs, resulting in a different log line.
-   **`ensure_ascii=False`**: This setting ensures that Unicode characters (such as Cyrillic) are written directly to the log file, rather than being escaped (e.g., `\u041f`). This improves readability and provides a more canonical representation of the data.

**Example of Determinism:**

Given the same event, the JSON output is **guaranteed** to be:
```json
{"actor":"scheduler","endpoint":"/activity.json","generated_at":"...","level":"info","message":"Fetching","params":{"limit":100},"run_id":"r-20251102-001","source":"chembl","stage":"extract"}
```
And **never**:
```json
{"level":"info","message":"Fetching","run_id":"r-20251102-001","stage":"extract","actor":"scheduler","source":"chembl","endpoint":"/activity.json","params":{"limit":100},"generated_at":"..."}
```

This deterministic output allows for powerful testing techniques, such as "golden tests," where the output of a test run can be directly compared against a pre-approved "golden" log file.
