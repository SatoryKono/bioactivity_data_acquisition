"""Regression tests for the schema registry behaviour."""

from __future__ import annotations

import logging
import re
import sys
import types

import pytest


if "cachetools" not in sys.modules:
    cachetools_stub = types.ModuleType("cachetools")

    class TTLCache(dict):  # pragma: no cover - simple stub for import
        """Minimal TTLCache stub used for registry tests."""

        def __init__(self, maxsize: int, ttl: int) -> None:  # noqa: D401 - simple stub
            super().__init__()

    cachetools_stub.TTLCache = TTLCache  # type: ignore[attr-defined]
    sys.modules["cachetools"] = cachetools_stub

from bioetl.schemas.assay import AssaySchema
from bioetl.schemas.registry import SchemaRegistry


def test_register_logs_positive_column_count(caplog: pytest.LogCaptureFixture) -> None:
    """Registry should log a positive column count when registering AssaySchema."""

    entity = "assay_schema_test_entity"

    SchemaRegistry._registry.pop(entity, None)

    with caplog.at_level(logging.INFO):
        SchemaRegistry.register(entity, "1.0.0", AssaySchema)

    log_messages = [
        record.getMessage()
        for record in caplog.records
        if "schema_registered" in record.getMessage()
    ]

    assert log_messages, "schema_registered log entry was not captured"

    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    last_message = ansi_escape.sub("", log_messages[-1])

    match = re.search(r"columns=(\d+)", last_message)
    assert match is not None, f"columns count missing from log: {last_message}"
    assert int(match.group(1)) > 0, "registered schema should have a positive column count"
