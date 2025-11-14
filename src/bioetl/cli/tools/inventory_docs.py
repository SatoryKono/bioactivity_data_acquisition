"""CLI command ``bioetl-inventory-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.cli.tools import emit_tool_error, exit_with_code
from bioetl.cli.tools.typer_helpers import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL
from bioetl.tools.inventory_docs import InventoryResult, collect_markdown_files
from bioetl.tools.inventory_docs import (
    write_inventory as write_inventory_sync,
)

typer: Any = get_typer()

__all__ = [
    "app",
    "main",
    "run",
    "write_inventory",
    "collect_markdown_files",
    "InventoryResult",
]

write_inventory = write_inventory_sync

def main(
    inventory_path: Path = typer.Option(
        Path("artifacts/docs_inventory.txt"),
        "--inventory",
        help="Destination for the Markdown document inventory (text file).",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
    hashes_path: Path = typer.Option(
        Path("artifacts/docs_hashes.txt"),
        "--hashes",
        help="Destination for the Markdown document SHA256 hashes.",
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Run the documentation inventory routine."""

    inventory_path_resolved = inventory_path.resolve()
    hashes_path_resolved = hashes_path.resolve()
    try:
        result = write_inventory(
            inventory_path=inventory_path_resolved,
            hashes_path=hashes_path_resolved,
        )
    except Exception as exc:  # noqa: BLE001
        emit_tool_error(
            template=CLI_ERROR_INTERNAL,
            message=f"Documentation inventory failed: {exc}",
            context={
                "command": "bioetl-inventory-docs",
                "inventory_path": str(inventory_path_resolved),
                "hashes_path": str(hashes_path_resolved),
                "exception_type": exc.__class__.__name__,
            },
            cause=exc,
        )

    typer.echo(
        f"Inventory completed: {len(result.files)} files, "
        f"inventory {result.inventory_path.resolve()}, "
        f"hashes {result.hashes_path.resolve()}"
    )
    exit_with_code(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-inventory-docs",
    help_text="Collect a Markdown document inventory and compute hashes",
    main_fn=main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()

