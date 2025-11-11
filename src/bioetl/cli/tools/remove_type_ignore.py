"""CLI-команда `bioetl-remove-type-ignore`."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, cast

from bioetl.cli.tools._typer import TyperApp, create_app
from bioetl.tools.remove_type_ignore import remove_type_ignore as remove_type_ignore_sync

typer = cast(Any, importlib.import_module("typer"))

__all__ = ["app", "main", "run", "remove_type_ignore"]

remove_type_ignore = remove_type_ignore_sync

app: TyperApp = create_app(
    name="bioetl-remove-type-ignore",
    help_text="Удаляй директивы type ignore из исходников",
)


@app.command()
def main(
    root: Path | None = typer.Option(
        None,
        "--root",
        help="Каталог для обработки (по умолчанию корень репозитория).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        writable=True,
    ),
) -> None:
    """Удаляет директивы `type: ignore`."""

    try:
        resolved_root = root.resolve() if root is not None else None
        removed = remove_type_ignore(root=resolved_root)
    except Exception as exc:  # noqa: BLE001
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Удалено директив `type: ignore`: {removed}")
    raise typer.Exit(code=0)


def run() -> None:
    """Запускает Typer-приложение."""

    app()


if __name__ == "__main__":
    run()

