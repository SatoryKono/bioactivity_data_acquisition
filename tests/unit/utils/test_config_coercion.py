"""Tests for configuration coercion helpers."""

from __future__ import annotations

from typing import Any

import pytest

from bioetl.utils.config import coerce_float_config, coerce_int_config


class _LogCollector:
    """Collect log messages emitted by the coercion helpers."""

    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, event: str, **kwargs: Any) -> None:  # pragma: no cover - trivial
        self.records.append((event, kwargs))


def test_coerce_int_returns_default_for_none_without_logging() -> None:
    log = _LogCollector()

    result = coerce_int_config(
        None,
        5,
        field="cache_ttl",
        log=log,
        invalid_event="invalid",
    )

    assert result == 5
    assert log.records == []


@pytest.mark.parametrize("value", ["abc", object()])
def test_coerce_int_invalid_value_logs_and_returns_default(value: Any) -> None:
    log = _LogCollector()

    result = coerce_int_config(
        value,
        10,
        field="cache_maxsize",
        log=log,
        log_context={"source": "pubchem"},
        invalid_event="config_invalid_int",
    )

    assert result == 10
    assert log.records == [
        (
            "config_invalid_int",
            {"field": "cache_maxsize", "value": value, "default": 10, "source": "pubchem"},
        )
    ]


def test_coerce_int_enforces_minimum_inclusive() -> None:
    log = _LogCollector()

    result = coerce_int_config(
        0,
        3,
        field="workers",
        minimum=1,
        log=log,
        invalid_event="config_invalid_int",
        out_of_range_event="config_out_of_range",
    )

    assert result == 3
    assert log.records == [
        (
            "config_out_of_range",
            {
                "field": "workers",
                "value": 0,
                "minimum": 1,
                "exclusive_minimum": False,
                "default": 3,
            },
        )
    ]


def test_coerce_float_enforces_exclusive_minimum() -> None:
    log = _LogCollector()

    result = coerce_float_config(
        0.0,
        2.5,
        field="rate_limit_period",
        minimum=0.0,
        exclusive_minimum=True,
        log=log,
        invalid_event="config_invalid_float",
        out_of_range_event="config_out_of_range",
    )

    assert result == 2.5
    assert log.records == [
        (
            "config_out_of_range",
            {
                "field": "rate_limit_period",
                "value": 0.0,
                "minimum": 0.0,
                "exclusive_minimum": True,
                "default": 2.5,
            },
        )
    ]


def test_coerce_float_invalid_value_logs() -> None:
    log = _LogCollector()

    result = coerce_float_config(
        "not-a-number",
        1.0,
        field="timeout",
        log=log,
        log_context={"source": "document"},
        invalid_event="config_invalid_float",
    )

    assert pytest.approx(result) == 1.0
    assert log.records == [
        (
            "config_invalid_float",
            {
                "field": "timeout",
                "value": "not-a-number",
                "default": 1.0,
                "source": "document",
            },
        )
    ]
