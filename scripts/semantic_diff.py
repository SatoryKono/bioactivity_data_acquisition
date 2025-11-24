"""CLI command ``bioetl-semantic-diff``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from bioetl.devtools import cli_semantic_diff as cli_semantic_diff_impl
from interfaces.cli.common import create_app, run_app

_LOGIC_EXPORTS = getattr(cli_semantic_diff_impl, "__all__", [])
globals().update({symbol: getattr(cli_semantic_diff_impl, symbol) for symbol in _LOGIC_EXPORTS})
run_semantic_diff = cli_semantic_diff_impl.run_semantic_diff
__all__ = [*_LOGIC_EXPORTS, "run_semantic_diff", "app", "cli_main", "run"]  # pyright: ignore[reportUnsupportedDunderAll]


def cli_main() -> None:
    """Run the semantic diff workflow."""

    report_path: Path

    try:
        report_path = run_semantic_diff()
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Semantic diff failed: {exc}", err=True)
        raise

    typer.echo(f"Semantic diff report written to: {report_path.resolve()}")
    raise typer.Exit(code=0)


app = create_app(name="bioetl-semantic-diff")
app.callback(invoke_without_command=True)(cli_main)


def run() -> int:
    """Запустить CLI и вернуть exit code."""

    return run_app(app)


if __name__ == "__main__":
    raise SystemExit(run())
