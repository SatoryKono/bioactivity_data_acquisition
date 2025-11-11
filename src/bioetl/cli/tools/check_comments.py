"""CLI-команда `bioetl-check-comments`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.check_comments import run_comment_check as run_comment_check_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "run_comment_check"]

run_comment_check = run_comment_check_sync

app: TyperApp = create_app(
    name="bioetl-check-comments",
    help_text="Проверь качество комментариев и TODO в коде",
)


@app.command()
def main(
    root: Path | None = typer.Option(
        None,
        "--root",
        help="Каталог проекта для проверки (по умолчанию корень репозитория).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
) -> None:
    """Запускает проверку комментариев."""

    try:
        run_comment_check(root=root.resolve() if root else None)
    except NotImplementedError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.YELLOW)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo("Проверка комментариев завершена без ошибок")
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    app()


if __name__ == "__main__":
    run()

