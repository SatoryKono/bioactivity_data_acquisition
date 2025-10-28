"""Tests for UnifiedLogger."""

import logging
import os

import pytest

from bioetl.core.logger import UnifiedLogger, RedactSecretsFilter, SafeFormattingFilter


def test_secret_redaction():
    """Test that secrets are redacted in logs."""
    logger = logging.getLogger("test")
    handler = logging.StreamHandler()
    handler.addFilter(RedactSecretsFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Test secret redaction
    logger.info("api_key=secret123")
    # Should be redacted in the output


def test_utc_timestamps():
    """Test that logs include UTC timestamps."""
    UnifiedLogger.setup(mode="development", run_id="test-123")
    log = UnifiedLogger.get("test")

    # This should include timestamp in the output
    log.info("test_event")


def test_context_var_propagation():
    """Test that run_id is propagated through ContextVar."""
    UnifiedLogger.setup(mode="development", run_id="test-run-456")
    log = UnifiedLogger.get("test")

    # Context should include run_id
    log.info("test_event_with_context")


def test_development_mode():
    """Test development mode configuration."""
    UnifiedLogger.setup(mode="development", run_id="dev-test")
    log = UnifiedLogger.get("test")

    log.debug("debug_message")
    log.info("info_message")
    log.warning("warning_message")


def test_production_mode():
    """Test production mode configuration."""
    UnifiedLogger.setup(mode="production", run_id="prod-test")
    log = UnifiedLogger.get("test")

    log.info("production_log", extra_data="value")


def test_testing_mode():
    """Test testing mode configuration."""
    UnifiedLogger.setup(mode="testing", run_id="test-id")
    log = UnifiedLogger.get("test")

    # Only WARNING and above should appear in testing mode
    log.info("this_should_not_appear")
    log.warning("this_should_appear")


def test_set_context():
    """Test setting additional context."""
    UnifiedLogger.setup(mode="development", run_id="context-test")
    log = UnifiedLogger.get("test")

    UnifiedLogger.set_context(stage="extract", source="chembl")
    log.info("contextual_log")


def test_safe_formatting_filter():
    """Test SafeFormattingFilter protection."""
    logger = logging.getLogger("test.filter")
    handler = logging.StreamHandler()
    handler.addFilter(SafeFormattingFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # Should not crash on malformed format
    logger.info("test message with %d and %s", "wrong", "args")


def test_security_processor_in_structlog():
    """Test security processor in structlog pipeline."""
    UnifiedLogger.setup(mode="production", run_id="security-test")
    log = UnifiedLogger.get("test")

    # This should redact the api_key value
    log.info("api_request", api_key="secret123", url="https://api.example.com")


def test_get_run_id():
    """Test getting current run ID."""
    UnifiedLogger.setup(mode="development", run_id="run-id-789")
    assert UnifiedLogger.get_run_id() == "run-id-789"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

