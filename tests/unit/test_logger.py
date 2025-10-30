"""Tests for UnifiedLogger."""

from __future__ import annotations

import importlib.util
import io
import logging
import sys
import types
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
from structlog.testing import capture_logs

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOGGER_PATH = PROJECT_ROOT / "src" / "bioetl" / "core" / "logger.py"

spec = importlib.util.spec_from_file_location("bioetl.core.logger", LOGGER_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Failed to load bioetl.core.logger module for testing")

logger_module = importlib.util.module_from_spec(spec)

sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
core_module = sys.modules.setdefault("bioetl.core", types.ModuleType("bioetl.core"))
sys.modules["bioetl"].core = core_module
sys.modules["bioetl.core.logger"] = logger_module
core_module.logger = logger_module
spec.loader.exec_module(logger_module)

UnifiedLogger = logger_module.UnifiedLogger
RedactSecretsFilter = logger_module.RedactSecretsFilter
SafeFormattingFilter = logger_module.SafeFormattingFilter
LoggerConfig = logger_module.LoggerConfig


def _apply_core_processors(log, method_name: str, event_dict: dict[str, object]) -> dict[str, object]:
    """Replicate the core processor chain needed for assertions."""

    processors = [
        logger_module.structlog.contextvars.merge_contextvars,
        logger_module.add_utc_timestamp,
        logger_module.add_context,
        logger_module.security_processor,
    ]

    processed = event_dict
    for processor in processors:
        processed = processor(log, method_name, processed)
    return processed


@pytest.fixture(autouse=True)
def reset_logging_state():
    """Ensure logging and structlog are clean between tests."""

    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.NOTSET)

    # Reset structlog configuration and local context storage
    import structlog

    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    logger_module._log_context.set(None)  # type: ignore[attr-defined]

    yield

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.NOTSET)

    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    logger_module._log_context.set(None)  # type: ignore[attr-defined]


@pytest.fixture
def fixed_timestamp(monkeypatch):
    """Patch datetime.now used by the logger to return a deterministic value."""

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            base = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
            if tz is None:
                return base.replace(tzinfo=None)
            return base.astimezone(tz)

    monkeypatch.setattr(logger_module, "datetime", _FixedDateTime)
    return datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def test_secret_redaction():
    """Test that secrets are redacted in logs."""
    logger = logging.getLogger("test")
    buffer = io.StringIO()
    handler = logging.StreamHandler(buffer)
    handler.addFilter(RedactSecretsFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Test secret redaction
    logger.info("api_key=secret123")
    handler.flush()
    output = buffer.getvalue()
    assert "***REDACTED***" in output
    assert "secret123" not in output


def test_structlog_event_includes_context_and_timestamp(fixed_timestamp):
    """Test that logs include UTC timestamps."""
    run_id = "test-123"
    UnifiedLogger.setup(mode="development", run_id=run_id)
    UnifiedLogger.set_context(run_id=run_id, stage="extract", actor="etl", source="chembl")
    log = UnifiedLogger.get("test")

    with capture_logs() as logs:
        log.info("test_event")

    assert logs, "Expected to capture at least one log entry"
    event = _apply_core_processors(log, "info", logs[0])
    assert event["event"] == "test_event"
    assert event["run_id"] == run_id
    assert event["stage"] == "extract"
    assert event["actor"] == "etl"
    assert event["source"] == "chembl"
    assert event["timestamp"] == fixed_timestamp.isoformat()


@pytest.mark.parametrize(
    ("mode", "log_method"),
    [
        ("development", "info"),
        ("production", "info"),
        ("testing", "warning"),
    ],
)
def test_processors_inject_context_in_all_modes(fixed_timestamp, mode, log_method):
    """Ensure processors inject mandatory keys regardless of mode."""

    run_id = f"{mode}-run"
    UnifiedLogger.setup(mode=mode, run_id=run_id)
    UnifiedLogger.set_context(run_id=run_id, stage="load", actor="pipeline", source="chembl")
    log = UnifiedLogger.get("test")

    with capture_logs() as logs:
        getattr(log, log_method)("contextual_event")

    assert logs
    event = _apply_core_processors(log, log_method, logs[0])
    assert event["run_id"] == run_id
    assert event["stage"] == "load"
    assert event["actor"] == "pipeline"
    assert event["source"] == "chembl"
    assert event["timestamp"] == fixed_timestamp.isoformat()


def test_safe_formatting_filter():
    """Test SafeFormattingFilter protection."""
    logger = logging.getLogger("test.filter")
    handler = logging.StreamHandler()
    handler.addFilter(SafeFormattingFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Should not crash on normal messages
    logger.info("test message")


def test_security_processor_in_structlog(fixed_timestamp):
    """Test security processor in structlog pipeline."""
    UnifiedLogger.setup(mode="production", run_id="security-test")
    UnifiedLogger.set_context(run_id="security-test", stage="extract", actor="etl", source="chembl")
    log = UnifiedLogger.get("test")

    with capture_logs() as logs:
        log.info("api_request", api_key="secret123", url="https://api.example.com")

    assert logs
    event = _apply_core_processors(log, "info", logs[0])
    assert event["api_key"] == "***REDACTED***"
    assert event["timestamp"] == fixed_timestamp.isoformat()


def test_get_run_id():
    """Test getting current run ID."""
    UnifiedLogger.setup(mode="development", run_id="run-id-789")
    assert UnifiedLogger.get_run_id() == "run-id-789"


def test_file_handler_creates_log_and_redacts(tmp_path):
    """Ensure file handler writes logs and redacts secrets."""

    log_file = tmp_path / "logs" / "app.log"
    config = LoggerConfig(
        level="INFO",
        file_enabled=True,
        file_path=log_file,
        file_format="text",
        max_bytes=1024,
        backup_count=1,
    )

    logger_module.setup_logger(mode="development", run_id="file-test", config=config)

    std_logger = logging.getLogger("file-handler-test")
    std_logger.info("token=supersecret")

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if hasattr(handler, "flush"):
            handler.flush()

    assert log_file.exists()
    contents = log_file.read_text(encoding="utf-8")
    assert "***REDACTED***" in contents
    assert "supersecret" not in contents


def _get_rotating_file_handlers() -> list[RotatingFileHandler]:
    """Return all rotating file handlers attached to the root logger."""

    root_logger = logging.getLogger()
    return [
        handler
        for handler in root_logger.handlers
        if isinstance(handler, RotatingFileHandler)
    ]


def test_setup_logger_adds_single_file_handler(tmp_path):
    """Initial setup attaches a single file handler for the configured path."""

    log_file = tmp_path / "logs" / "app.log"
    config = LoggerConfig(
        level="INFO",
        file_enabled=True,
        file_path=log_file,
        file_format="text",
        max_bytes=1024,
        backup_count=1,
    )

    logger_module.setup_logger(mode="development", run_id="file-test", config=config)

    handlers = _get_rotating_file_handlers()
    assert len(handlers) == 1
    assert Path(handlers[0].baseFilename) == log_file.resolve()


def test_setup_logger_reinitialization_is_idempotent(tmp_path):
    """Repeated setup does not accumulate duplicate file handlers."""

    log_file = tmp_path / "logs" / "app.log"
    config = LoggerConfig(
        level="INFO",
        file_enabled=True,
        file_path=log_file,
        file_format="text",
        max_bytes=1024,
        backup_count=1,
    )

    logger_module.setup_logger(mode="development", run_id="file-test", config=config)
    first_handlers = _get_rotating_file_handlers()
    assert len(first_handlers) == 1

    logger_module.setup_logger(mode="development", run_id="file-test", config=config)
    second_handlers = _get_rotating_file_handlers()
    assert len(second_handlers) == 1
    assert Path(second_handlers[0].baseFilename) == log_file.resolve()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

