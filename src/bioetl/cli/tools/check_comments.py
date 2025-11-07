"""CLI-заглушка для проверки комментариев."""

from __future__ import annotations

from pathlib import Path

import typer

from bioetl.cli.tools import create_app
from bioetl.tools.check_comments import run_comment_check

app = create_app(
    name="bioetl-check-comments",
    help_text="Проверка комментариев и TODO (пока в разработке)",
)


@app.command()
def main(
    root: Path = typer.Option(Path("."), help="Корень репозитория для проверки"),
) -> None:
    """Попытаться выполнить проверку комментариев."""

    try:
        run_comment_check(root=root.resolve())
    except NotImplementedError as exc:  # pragma: no cover - статус ожидаемо неуспешный
        typer.secho(str(exc), fg=typer.colors.YELLOW)
        raise typer.Exit(code=1) from exc


def run() -> None:
    app()

