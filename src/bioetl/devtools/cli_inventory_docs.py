"""Inventory utilities for project markdown documentation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.tools import get_project_root, hash_file

__all__ = [
    "InventoryResult",
    "collect_markdown_files",
    "write_inventory",
]


@dataclass(frozen=True)
class InventoryResult:
    """Inventory output containing tracked files and artifact paths."""

    files: tuple[Path, ...]
    inventory_path: Path
    hashes_path: Path


def _iter_markdown_files(root: Path) -> Iterable[Path]:
    """Yield markdown documents under ``root`` in stable order."""
    for path in sorted(root.rglob("*.md")):
        if path.is_file():
            yield path


def collect_markdown_files(docs_root: Path | None = None) -> tuple[Path, ...]:
    """Return an ordered tuple of markdown files under ``docs_root``."""

    project_root = get_project_root()
    base = docs_root if docs_root is not None else project_root / "docs"
    resolved = base.resolve()
    return tuple(_iter_markdown_files(resolved))


def write_inventory(
    inventory_path: Path,
    hashes_path: Path,
    files: tuple[Path, ...] | None = None,
) -> InventoryResult:
    """Write markdown inventory and checksum files atomically."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    project_root = get_project_root()
    docs_files = files if files is not None else collect_markdown_files()

    log.info(LogEvents.INVENTORY_COLLECTED,
        docs_root=str(project_root / "docs"),
        files=len(docs_files),
    )

    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    hashes_path.parent.mkdir(parents=True, exist_ok=True)

    inventory_tmp = inventory_path.with_suffix(inventory_path.suffix + ".tmp")
    hashes_tmp = hashes_path.with_suffix(hashes_path.suffix + ".tmp")

    with inventory_tmp.open("w", encoding="utf-8") as inventory_handle:
        for path in docs_files:
            relative = path.relative_to(project_root)
            inventory_handle.write(f"{relative.as_posix()}\n")

    with hashes_tmp.open("w", encoding="utf-8") as hashes_handle:
        for path in docs_files:
            relative = path.relative_to(project_root)
            hashes_handle.write(f"{hash_file(path)}  {relative.as_posix()}\n")

    inventory_tmp.replace(inventory_path)
    hashes_tmp.replace(hashes_path)

    log.info(LogEvents.INVENTORY_WRITTEN,
        inventory=str(inventory_path),
        hashes=str(hashes_path),
    )

    return InventoryResult(files=docs_files, inventory_path=inventory_path, hashes_path=hashes_path)
