from __future__ import annotations

from typing import Any, Callable

import typer
from typer.testing import CliRunner

from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_app,
    create_simple_tool_app,
    run_app,
)


def test_create_app_registers_command() -> None:
    app = create_app(name="test-app", help_text="Test application")

    @app.command("hello")  # type: ignore[misc]
    def _hello() -> None:
        typer.echo("hello")

    result = CliRunner().invoke(app, ["hello"])
    assert result.exit_code == 0
    assert "hello" in result.stdout


def test_create_simple_tool_app_binds_main() -> None:
    captured: dict[str, Any] = {}

    def _main(name: str = typer.Option("world", "--name")) -> None:
        captured["name"] = name
        typer.echo(f"hi {name}")

    app = create_simple_tool_app(
        name="demo-tool",
        help_text="Demo tool",
        main_fn=_main,
    )

    result = CliRunner().invoke(app, ["--name", "bioetl"])
    assert result.exit_code == 0
    assert captured["name"] == "bioetl"
    assert "hi bioetl" in result.stdout


def test_run_app_invokes_entrypoint() -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    class _DummyApp:
        def __call__(self, *args: Any, **kwargs: Any) -> None:
            calls.append((args, kwargs))

        def command(self, *args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                return func

            return _decorator

    dummy_app: TyperApp = _DummyApp()

    run_app(dummy_app)

    assert calls == [((), {})]


