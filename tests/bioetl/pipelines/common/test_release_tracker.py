"""Unit tests for ChemblReleaseMixin behaviour."""

from __future__ import annotations

from datetime import timezone
from typing import Any

import pytest

from bioetl.pipelines.common.release_tracker import ChemblReleaseMixin


class _DummyLogger:
    """Простейший логгер для проверки событий handshake."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **kwargs: Any) -> None:
        self.records.append(("info", event, kwargs))

    def debug(self, event: str, **kwargs: Any) -> None:
        self.records.append(("debug", event, kwargs))


class _DummyTracker(ChemblReleaseMixin):
    """Helper реализация mixin для тестов."""

    def __init__(self) -> None:
        super().__init__()


@pytest.mark.unit
def test_release_defaults_to_none() -> None:
    tracker = _DummyTracker()

    assert tracker.chembl_release is None


@pytest.mark.unit
def test_release_updates_and_resets() -> None:
    tracker = _DummyTracker()

    tracker._update_release("CHEMBL_36")  # noqa: SLF001
    assert tracker.chembl_release == "CHEMBL_36"

    tracker._update_release("")  # noqa: SLF001
    assert tracker.chembl_release is None

    tracker._update_release(None)  # noqa: SLF001
    assert tracker.chembl_release is None


@pytest.mark.unit
def test_handshake_updates_release_from_payload() -> None:
    logger = _DummyLogger()
    tracker = _DummyTracker()

    class _Client:
        call_count = 0

        def handshake(self, *, endpoint: str, enabled: bool) -> dict[str, Any]:
            self.call_count += 1
            assert endpoint == "/status"
            assert enabled is True
            return {"chembl_release": " CHEMBL_37 "}

    client = _Client()

    result = tracker.perform_chembl_handshake(
        client,
        log=logger,  # type: ignore[arg-type]
        event="test.handshake",
        endpoint="/status",
        enabled=True,
    )

    assert tracker.chembl_release == "CHEMBL_37"
    assert result.release == "CHEMBL_37"
    assert result.payload == {"chembl_release": " CHEMBL_37 "}
    assert result.requested_at_utc.tzinfo is timezone.utc
    assert client.call_count == 1
    assert ("info", "test.handshake", {"chembl_release": "CHEMBL_37", "handshake_endpoint": "/status", "handshake_enabled": True}) in logger.records


@pytest.mark.unit
def test_handshake_missing_method_uses_fallback_release() -> None:
    logger = _DummyLogger()
    tracker = _DummyTracker()

    class _Client:
        chembl_release = " CHEMBL_36 "

    client = _Client()

    result = tracker.perform_chembl_handshake(
        client,
        log=logger,  # type: ignore[arg-type]
        event="test.handshake",
        endpoint="/status",
        enabled=True,
    )

    assert tracker.chembl_release == "CHEMBL_36"
    assert result.release == "CHEMBL_36"
    assert result.payload == {}
    assert ("debug", "test.handshake.skipped", {"reason": "handshake_method_missing", "handshake_endpoint": "/status"}) in logger.records


@pytest.mark.unit
def test_handshake_disabled_skips_execution() -> None:
    logger = _DummyLogger()
    tracker = _DummyTracker()

    class _Client:
        call_count = 0

        def handshake(self, *, endpoint: str, enabled: bool) -> dict[str, Any]:
            self.call_count += 1
            return {"chembl_release": "CHEMBL_38"}

    client = _Client()

    result = tracker.perform_chembl_handshake(
        client,
        log=logger,  # type: ignore[arg-type]
        event="test.handshake",
        endpoint="/status",
        enabled=False,
    )

    assert client.call_count == 0
    assert tracker.chembl_release is None
    assert result.payload == {}
    assert result.release is None
    assert ("info", "test.handshake.skipped", {"handshake_enabled": False, "handshake_endpoint": "/status"}) in logger.records


@pytest.mark.unit
def test_handshake_supports_legacy_signature() -> None:
    logger = _DummyLogger()
    tracker = _DummyTracker()

    class _LegacyClient:
        def handshake(self, *, endpoint: str) -> dict[str, Any]:
            return {"chembl_db_version": "CHEMBL_39"}

    client = _LegacyClient()

    tracker.perform_chembl_handshake(
        client,
        log=logger,  # type: ignore[arg-type]
        event="test.handshake",
        endpoint="/status",
        enabled=True,
    )

    assert tracker.chembl_release == "CHEMBL_39"
