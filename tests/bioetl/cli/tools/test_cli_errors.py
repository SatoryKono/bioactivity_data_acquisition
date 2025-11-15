from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer

from bioetl.cli.tool_specs import TOOL_COMMAND_SPECS
from bioetl.core.logging import LogEvents
from bioetl.core.runtime import cli_errors


class _StubLogger:
    def __init__(self) -> None:
        self.records: list[tuple[LogEvents | str, dict[str, object]]] = []

    def error(self, event: LogEvents | str, /, **context: object) -> None:
        self.records.append((event, context))


def test_emit_tool_error_logs_and_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        cli_errors,
        "typer",
        SimpleNamespace(
            echo=lambda message, err=False: captured.append((message, err)),
        ),
    )
    logger = _StubLogger()

    with pytest.raises(typer.Exit) as exit_info:
        cli_errors.emit_tool_error(
            template=cli_errors.CLI_ERROR_INTERNAL,
            message="test failure",
            context={"foo": "bar"},
            logger=logger,
            exit_code=2,
        )

    assert exit_info.value.code == 2
    assert logger.records
    event, context = logger.records[0]
    assert event == LogEvents.CLI_RUN_ERROR
    assert context["error_message"] == "test failure"
    assert context["foo"] == "bar"
    assert captured == [
        ("[bioetl-cli] ERROR E001: test failure", True),
    ]


def test_emit_tool_error_propagates_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli_errors,
        "typer",
        SimpleNamespace(echo=lambda *args, **kwargs: None),
    )
    logger = _StubLogger()
    root_exc = RuntimeError("boom")

    with pytest.raises(typer.Exit) as exit_info:
        cli_errors.emit_tool_error(
            template=cli_errors.CLI_ERROR_INTERNAL,
            message="root failure",
            logger=logger,
            cause=root_exc,
        )

    assert exit_info.value.code == 1
    assert exit_info.value.__cause__ is root_exc


def test_qc_boundary_tool_is_declared() -> None:
    assert any(spec.code == "qc_boundary_check" for spec in TOOL_COMMAND_SPECS)


