"""CLI command ``bioetl-inventory-docs``."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_simple_tool_app,
    get_typer,
    run_app,
)
from bioetl.cli.tools._logic import cli_inventory_docs as cli_inventory_docs_impl
from bioetl.core.runtime.cli_base import CliCommandBase
from bioetl.core.runtime.cli_errors import CLI_ERROR_INTERNAL

_LOGIC_EXPORTS = getattr(cli_inventory_docs_impl, "__all__", [])
globals().update({symbol: getattr(cli_inventory_docs_impl, symbol) for symbol in _LOGIC_EXPORTS})
InventoryResult = getattr(cli_inventory_docs_impl, "InventoryResult")
write_inventory = getattr(cli_inventory_docs_impl, "write_inventory")
__all__ = [
    * _LOGIC_EXPORTS,
    "InventoryResult",
    "write_inventory",
    "app",
    "cli_main",
    "run",
]  # pyright: ignore[reportUnsupportedDunderAll]

if TYPE_CHECKING:
    from bioetl.cli.tools._logic.cli_inventory_docs import InventoryResult as InventoryResultType
else:
    InventoryResultType = Any

typer: Any = get_typer()


def cli_main(
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
    result: InventoryResultType
    try:
        result = write_inventory(
            inventory_path=inventory_path_resolved,
            hashes_path=hashes_path_resolved,
        )
    except typer.Exit:
        raise
    except Exception as exc:  # noqa: BLE001
        CliCommandBase.emit_error(
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
    CliCommandBase.exit(0)


app: TyperApp = create_simple_tool_app(
    name="bioetl-inventory-docs",
    help_text="Collect a Markdown document inventory and compute hashes",
    main_fn=cli_main,
)


def run() -> None:
    """Execute the Typer application."""

    run_app(app)


if __name__ == "__main__":
    run()