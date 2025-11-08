"""CLI для проверки таблицы нарушений именований."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import typer

from bioetl.cli.tools import create_app, runner_factory

DEFAULT_TABLE_PATH = Path("docs/styleguide/VIOLATIONS_TABLE.md")

app = create_app(
    name="bioetl-validate-naming-violations",
    help_text="Проверяет, что таблица нарушений именований пуста.",
)


def _iter_markdown_rows(content: str) -> Iterable[list[str]]:
    """Извлекает строки таблицы Markdown без заголовков и разделителей."""

    header_tokens = {
        "category",
        "identifier",
        "path",
        "detected_at",
        "status",
        "notes",
    }

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line.startswith("|") or set(line) <= {"|", " ", "-"}:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        normalized_cells = {cell.lower() for cell in cells if cell}
        if normalized_cells and normalized_cells.issubset(header_tokens):
            continue
        if all(cell.replace("-", "") == "" for cell in cells):
            continue
        yield cells


@app.command()
def main(table: Path = typer.Option(DEFAULT_TABLE_PATH, exists=True, readable=True)) -> None:
    """Проверяет отсутствие данных в таблице нарушений именований."""

    rows = list(_iter_markdown_rows(table.read_text(encoding="utf-8")))
    if rows:
        formatted_rows = "\n".join(" | ".join(row) for row in rows)
        typer.secho(
            (
                "Обнаружены незакрытые нарушения именований.\n"
                "Удалите строки из docs/styleguide/VIOLATIONS_TABLE.md или обновите план."
                f"\n\n{formatted_rows}"
            ),
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    typer.secho("Таблица нарушений именований пуста.", fg=typer.colors.GREEN)


run = runner_factory(app)

