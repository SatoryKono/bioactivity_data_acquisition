import json
from collections.abc import Generator
from typing import Any

import pytest

from bioetl.core.log_events import LogEvents
from bioetl.core.logger import LogConfig, LogFormat, UnifiedLogger


@pytest.fixture(autouse=True)
def reset_logger_context() -> Generator[None, None, None]:
    UnifiedLogger.reset()
    yield
    UnifiedLogger.reset()


def _bind_mandatory(log: Any) -> Any:
    return log.bind(
        run_id="run-1",
        pipeline="chembl",
        stage="run",
        dataset="chembl",
        component="cli",
        trace_id="trace-1",
        span_id="span-1",
    )


def test_unified_logger_json_output(capfd: Any) -> None:
    UnifiedLogger.configure(LogConfig(format=LogFormat.JSON, level="INFO"))
    logger = _bind_mandatory(UnifiedLogger.get(__name__))

    logger.info(LogEvents.CLI_RUN_START, duration_ms=100)

    _, err = capfd.readouterr()
    payload = json.loads(err.strip())

    assert payload["message"] == LogEvents.CLI_RUN_START.value
    assert payload["duration_ms"] == 100
    assert "timestamp" in payload
    assert "missing_context" not in payload


def test_unified_logger_missing_context_report(capfd: Any) -> None:
    UnifiedLogger.configure(LogConfig(format=LogFormat.JSON, level="INFO"))
    logger = UnifiedLogger.get(__name__)
    logger.info(LogEvents.CLI_RUN_START, pipeline="chembl")

    _, err = capfd.readouterr()
    payload = json.loads(err.strip())

    assert sorted(payload["missing_context"]) == [
        "component",
        "dataset",
        "run_id",
        "span_id",
        "stage",
        "trace_id",
    ]


def test_unified_logger_key_value_output(capfd: Any) -> None:
    UnifiedLogger.configure(LogConfig(format=LogFormat.KEY_VALUE, level="INFO"))
    logger = _bind_mandatory(UnifiedLogger.get(__name__))

    logger.info(LogEvents.CLI_RUN_FINISH, rows=10)

    _, err = capfd.readouterr()
    line = err.strip()
    assert line.startswith("timestamp=")
    assert "level=" in line and "info" in line
    assert "pipeline='chembl'" in line
    assert "stage='run'" in line
    assert "cli.run.finish" in line
    assert "rows=10" in line
