from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, cast

import pytest
from typer.testing import CliRunner

from bioetl.cli import cli_app
from bioetl.cli.cli_app import _load_tool_entrypoint, create_app
from bioetl.cli.cli_registry import ToolCommandConfig


class LogSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, dict[str, Any]]] = []

    def error(self, event: Any, **context: Any) -> None:
        self.calls.append((event, context))


def _make_config(name: str, description: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, description=description, pipeline_class=object)


def test_create_app_list_command_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    # Capture log calls
    log_spy = LogSpy()
    monkeypatch.setattr(cli_app, "_log", log_spy)

    # Stub create_pipeline_command to exercise both success and error branches
    def fake_factory(*, pipeline_class: Any, command_config: SimpleNamespace) -> Callable[[], None]:
        if command_config.name == "broken":
            raise RuntimeError("boom")

        def command() -> None:
            return None

        return command

    monkeypatch.setattr(cli_app, "create_pipeline_command", fake_factory)

    # Prepare tool modules
    tool_module_ok = ModuleType("tests.cli_app.tool_ok")

    def tool_ok() -> None:
        return None

    setattr(tool_module_ok, "tool", tool_ok)

    tool_module_bad = ModuleType("tests.cli_app.tool_bad")
    setattr(tool_module_bad, "entry", "not-callable")

    monkeypatch.setitem(sys.modules, tool_module_ok.__name__, tool_module_ok)
    monkeypatch.setitem(sys.modules, tool_module_bad.__name__, tool_module_bad)

    registry = {
        "ok": lambda: _make_config("ok", "OK pipeline"),
        "broken": lambda: _make_config("broken", "Broken pipeline"),
        "unimplemented": lambda: (_ for _ in ()).throw(NotImplementedError("todo")),
    }
    tools = {
        "tool-ok": ToolCommandConfig(
            name="tool-ok",
            description="Tool OK",
            module=tool_module_ok.__name__,
            attribute="tool",
        ),
        "tool-bad": ToolCommandConfig(
            name="tool-bad",
            description="Tool BAD",
            module=tool_module_bad.__name__,
            attribute="entry",
        ),
    }

    app = create_app(command_registry=registry, tool_commands=tools)

    result = CliRunner().invoke(app, ["list"])
    assert result.exit_code == 0

    output = cast(str, getattr(result, "output"))
    assert "ok" in output
    assert "not implemented" in output
    assert "WARN: Command 'broken' not loaded" in output
    assert "Registered utility commands" in output
    assert "Tool 'tool-bad' not loaded" in output

    # Errors were logged for broken command and failing tool.
    assert log_spy.calls
    logged_commands = {context["command"] for _, context in log_spy.calls}
    assert logged_commands == {"broken", "tool-bad"}


def test_load_tool_entrypoint_non_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("tests.cli_app.non_callable")
    setattr(module, "attr", "nope")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    config = ToolCommandConfig(
        name="tool",
        description="desc",
        module=module.__name__,
        attribute="attr",
    )

    with pytest.raises(TypeError):
        _load_tool_entrypoint(config)


def test_load_typer_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_module("bioetl.cli.tools._typer")
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda name: (_ for _ in ()).throw(ModuleNotFoundError(name)),
    )
    with pytest.raises(RuntimeError):
        module._load_typer()

