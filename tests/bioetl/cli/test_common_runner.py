"""Тесты для общего раннера CLI."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer

from bioetl.cli.common import run
from bioetl.core.logger import UnifiedLogger


class DummyLogger:
    """Простейший логгер для фиксации событий в тестах."""

    def __init__(self, events: list[tuple[str, str, dict[str, object]]]) -> None:
        self._events = events

    def info(self, event: str, **payload: object) -> None:
        self._events.append(("info", event, payload))

    def warning(self, event: str, **payload: object) -> None:
        self._events.append(("warning", event, payload))

    def error(self, event: str, **payload: object) -> None:
        self._events.append(("error", event, payload))


@pytest.fixture()
def logger_spy(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Подменяет UnifiedLogger и возвращает объект с журналом вызовов."""

    events: list[tuple[str, str, dict[str, object]]] = []
    configure_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_configure(*args: object, **kwargs: object) -> None:
        configure_calls.append((args, kwargs))

    monkeypatch.setattr(UnifiedLogger, "configure", fake_configure)
    monkeypatch.setattr(UnifiedLogger, "bind", lambda **_: None)
    monkeypatch.setattr(UnifiedLogger, "reset", lambda: None)
    monkeypatch.setattr(UnifiedLogger, "get", lambda _: DummyLogger(events))

    return SimpleNamespace(events=events, configure_calls=configure_calls)


def _event_names(events: list[tuple[str, str, dict[str, object]]]) -> list[str]:
    return [event for _, event, _ in events]


def test_run_success_returns_zero(logger_spy: SimpleNamespace) -> None:
    exit_code = run(lambda: None)

    assert exit_code == 0
    assert "cli_runner_completed" in _event_names(logger_spy.events)
    assert len(logger_spy.configure_calls) == 1


def test_run_success_propagates_custom_code(logger_spy: SimpleNamespace) -> None:
    exit_code = run(lambda: 5)

    assert exit_code == 5
    assert "cli_runner_completed" in _event_names(logger_spy.events)


def test_run_uses_typer_exit_code(logger_spy: SimpleNamespace) -> None:
    def command() -> int | None:
        raise typer.Exit(code=4)

    exit_code = run(command)

    assert exit_code == 4
    assert "cli_runner_typer_exit" in _event_names(logger_spy.events)


def test_run_maps_keyboard_interrupt(logger_spy: SimpleNamespace) -> None:
    def command() -> int | None:
        raise KeyboardInterrupt()

    exit_code = run(command)

    assert exit_code == 130
    assert "cli_runner_interrupted" in _event_names(logger_spy.events)


def test_run_maps_value_error_to_usage_exit(logger_spy: SimpleNamespace) -> None:
    def command() -> int | None:
        raise ValueError("boom")

    exit_code = run(command)

    assert exit_code == 2
    assert "cli_runner_usage_error" in _event_names(logger_spy.events)


def test_run_maps_runtime_error(logger_spy: SimpleNamespace) -> None:
    def command() -> int | None:
        raise RuntimeError("boom")

    exit_code = run(command)

    assert exit_code == 1
    assert "cli_runner_runtime_error" in _event_names(logger_spy.events)


def test_run_passes_through_system_exit(logger_spy: SimpleNamespace) -> None:
    def command() -> int | None:
        raise SystemExit(99)

    exit_code = run(command)

    assert exit_code == 99
    assert "cli_runner_system_exit" in _event_names(logger_spy.events)


def test_run_without_setup_logging_does_not_configure_logger(
    logger_spy: SimpleNamespace,
) -> None:
    exit_code = run(lambda: None, setup_logging=False)

    assert exit_code == 0
    assert logger_spy.configure_calls == []

