# 7. Testing and Migration Guide

## Testing

Testing code that involves logging requires a different approach than testing
pure business logic. Instead of checking a function's return value, you need to
assert that the correct log messages were emitted with the correct structured
data.

### Unit Testing with `structlog-sentry-logger`

For unit tests, the recommended approach is to use a library like
`structlog-sentry-logger` or `pytest-structlog` which provides fixtures for
capturing and inspecting log records.

**Example using `pytest-structlog`:**

```python
# file: tests/bioetl/core/test_my_module.py
import pytest
from my_app.my_module import process_data


def test_process_data_with_invalid_record(log_output):
    """
    Tests that an invalid record logs a warning with the correct context.
    """
    data = {"id": 1, "status": "valid"}
    invalid_data = {"id": 2, "status": "invalid"}

    process_data([data, invalid_data])

    # log_output is a list of all log records emitted during the test
    assert len(log_output) == 1
    log_record = log_output[0]

    assert log_record["level"] == "warning"
    assert log_record["event"] == "Invalid record status detected."
    assert log_record["record_id"] == 2
```

### Golden Testing for Determinism

Because the JSON output of the logger is deterministic (thanks to
`sort_keys=True`), you can use "golden testing" to validate the entire log
output of a pipeline run.

The process is:

1. Run a pipeline and capture its JSON log output to a file (e.g.,
   `test_run.golden.log`).
1. Manually inspect and approve this "golden" file, committing it to your test
   assets.
1. In your integration test, run the same pipeline and capture its log output.
1. The test then performs a simple diff between the new output and the golden
   file.

If the diff is empty, the test passes, proving that the logging output has not
changed unexpectedly. If there is a diff, the test fails, alerting you to a
potential regression.

## Migration Guide

Migrating an existing codebase to use the `UnifiedLogger` is a straightforward
process.

### Migrating from standard `logging`

If your code currently uses Python's standard `logging` library:

**Old Code:**

```python
import logging

log = logging.getLogger(__name__)


def my_function():
    log.info("Processing item %s", item_id)
```

**New Code:**

1. Ensure `UnifiedLogger.configure()` is called at your application's entry
   point.
1. Replace the logger acquisition and logging calls.

```python
from bioetl.core.logging import UnifiedLogger

log = UnifiedLogger.get(__name__)  # <-- Change this line


def my_function():
    # Change to structured logging
    log.info("Processing item.", item_id=item_id)
```

The key is to move from string formatting (`%s`) to passing data as key-value
pairs. This unlocks the full power of structured logging.

### Migrating from a basic `structlog` setup

If you are already using `structlog` but without the `UnifiedLogger` wrapper:

1. **Centralize Configuration**: Remove any scattered `structlog.configure()`
   calls. Replace them with a single
   `UnifiedLogger.configure(LoggerConfig(...))` call at the application entry
   point.
1. **Switch to `UnifiedLogger.get()`**: Replace `structlog.get_logger()` with
   `UnifiedLogger.get()`.
1. **Use `set_run_context()`**: If you were manually binding context
   (`structlog.contextvars.bind_contextvars`), replace this with a single call
   to `set_run_context()` at the start of your pipeline run. This ensures that
   the mandatory context fields (`run_id`, `stage`, etc.) are consistently
   applied.

By following this guide, you can effectively test your application's logging
behavior and smoothly migrate existing code to the new, standardized
`UnifiedLogger` system.
