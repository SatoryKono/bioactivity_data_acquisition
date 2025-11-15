from __future__ import annotations

from typing import Any

import pytest
import typer

from bioetl.core.logging import UnifiedLogger
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL


class _RecordingCommand(CliCommandBase):
    """Test double that records received arguments."""

    def __init__(self) -> None:
        super().__init__(logger=UnifiedLogger.get(__name__))
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def handle(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append((args, kwargs))


class _ExitCommand(CliCommandBase):
    """Command that terminates with typer.Exit."""

    def handle(self, *args: Any, **kwargs: Any) -> None:
        raise typer.Exit(code=5)


class _FailingCommand(CliCommandBase):
    """Command that raises an unexpected exception."""

    def handle(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("boom")


@pytest.mark.unit
def test_cli_command_base_invokes_handle() -> None:
    command = _RecordingCommand()

    command.invoke("foo", bar=1)

    assert command.calls == [(("foo",), {"bar": 1})]


@pytest.mark.unit
def test_cli_command_base_propagates_typer_exit() -> None:
    command = _ExitCommand()

    with pytest.raises(typer.Exit) as excinfo:
        command.invoke()

    exit_code = getattr(excinfo.value, "code", getattr(excinfo.value, "exit_code", None))
    assert exit_code == 5


@pytest.mark.unit
def test_cli_command_base_emits_error_on_unexpected_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, Any] = {}

    def fake_emit_error(
        *,
        template: Any,
        message: str,
        logger: Any | None = None,
        event: Any | None = None,
        context: Any | None = None,
        **kwargs: Any,
    ) -> None:
        recorded.update(
            {"template": template, "message": message, "logger": logger, "event": event, "context": context}
        )

    monkeypatch.setattr(CliCommandBase, "emit_error", staticmethod(fake_emit_error))
    command = _FailingCommand()

    with pytest.raises(typer.Exit) as excinfo:
        command.invoke()

    exit_code = getattr(excinfo.value, "code", getattr(excinfo.value, "exit_code", None))
    assert exit_code == CliCommandBase.exit_code_error
    assert recorded["template"] == CLI_ERROR_INTERNAL
    assert "Unhandled CLI exception" in recorded["message"]

