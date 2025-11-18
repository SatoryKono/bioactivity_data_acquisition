"""Unit tests covering mixin-level helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bioetl.pipelines.mixins import LoggingMixin, PaginatedExtractorMixin


class _LoggingProbe(LoggingMixin):
    def __init__(self) -> None:
        self._stage_durations_ms: dict[str, float] = {}
        self._logger = MagicMock()

    def _make_pipeline_logger(self, **_: object) -> MagicMock:  # type: ignore[override]
        return self._logger


class _PaginationProbe(PaginatedExtractorMixin, LoggingMixin):
    def __init__(self) -> None:
        self._stage_durations_ms: dict[str, float] = {}
        self.observed: list[tuple[int, dict[str, object]]] = []

    def _make_pipeline_logger(self, **_: object) -> MagicMock:  # type: ignore[override]
        logger = MagicMock()
        logger.info = MagicMock()
        return logger

    def on_page(self, index: int, meta: dict[str, object]) -> None:
        self.observed.append((index, dict(meta)))


def test_stage_logger_records_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _LoggingProbe()
    counter = iter([1.0, 1.25])

    def fake_perf_counter() -> float:
        return next(counter)

    monkeypatch.setattr("bioetl.pipelines.mixins.time.perf_counter", fake_perf_counter)

    with probe.stage_logger("extract", rows=3) as logger:
        logger.info("custom_event")

    assert "extract" in probe._stage_durations_ms
    assert probe._stage_durations_ms["extract"] == pytest.approx(250.0)
    probe._logger.info.assert_any_call("stage_started", rows=3)
    probe._logger.info.assert_any_call("stage_completed", duration_ms=pytest.approx(250.0), rows=3)


def test_iterate_pages_invokes_on_page() -> None:
    probe = _PaginationProbe()
    client = MagicMock()

    first_response = MagicMock()
    first_response.json.return_value = {
        "results": [{"identifier": "a"}],
        "page_meta": {"next": "/next"},
    }

    second_response = MagicMock()
    second_response.json.return_value = {
        "results": [{"identifier": "b"}],
        "page_meta": {"next": None},
    }

    client.get.side_effect = [first_response, second_response]

    pages = list(
        probe.iterate_pages(
            client,
            "/items",
            params={"foo": "bar"},
            page_size=1,
            items_key="results",
        )
    )

    assert len(pages) == 2
    assert pages[0][1][0]["identifier"] == "a"
    assert probe.observed[0] == (0, {"next": "/next"})
    assert probe.observed[-1][0] == 1
