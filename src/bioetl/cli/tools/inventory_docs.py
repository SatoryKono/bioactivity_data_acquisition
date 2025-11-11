"""Shim для совместимости с `bioetl-inventory-docs`."""

from __future__ import annotations

from tools.inventory_docs import (
    InventoryResult,
    app,
    collect_markdown_files,
    main,
    run,
    write_inventory,
)

__all__ = [
    "app",
    "main",
    "run",
    "write_inventory",
    "collect_markdown_files",
    "InventoryResult",
]


if __name__ == "__main__":
    run()

