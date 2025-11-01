"""Validate module naming conventions for high-level packages.

This script enforces the `<source>_<object>.py` pattern inside selected
directories so that public entry points stay uniform across the
codebase.  Modules that intentionally deviate (for example compatibility
proxies or package initialisers) are tracked via explicit allow-lists.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable


BASE_DIRECTORIES: tuple[tuple[Path, frozenset[str]], ...] = (
    (Path("src/bioetl/clients"), frozenset({"__init__.py"})),
    (
        Path("src/bioetl/adapters"),
        frozenset(
            {
                "__init__.py",
                "_normalizer_helpers.py",
                "base.py",
                "crossref.py",
                "openalex.py",
                "pubmed.py",
            }
        ),
    ),
    (
        Path("src/bioetl/pipelines"),
        frozenset(
            {
                "__init__.py",
                "base.py",
                "registry.py",
                "activity.py",
                "assay.py",
                "document.py",
                "target.py",
                "testitem.py",
            }
        ),
    ),
    (
        Path("src/bioetl/transform/normalizers"),
        frozenset({"__init__.py"}),
    ),
    (Path("src/bioetl/cli/commands"), frozenset({"__init__.py"})),
)


def iter_python_files(directory: Path) -> Iterable[Path]:
    """Yield all first-level Python files inside ``directory``."""

    if not directory.exists():
        return
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.suffix == ".py":
            yield path


def is_valid_module(filename: str) -> bool:
    """Return ``True`` when ``filename`` matches `<source>_<object>.py`."""

    stem = filename[:-3]
    if "_" not in stem:
        return False
    source, _, obj = stem.partition("_")
    if not source or not obj:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789")
    return set(source) <= allowed and set(obj) <= allowed


def check_module_names() -> int:
    """Validate module naming conventions and return an exit status."""

    invalid: list[str] = []

    for directory, exceptions in BASE_DIRECTORIES:
        for file_path in iter_python_files(directory):
            if file_path.name in exceptions:
                continue
            if file_path.name.startswith("__"):
                # Package dunder modules are exempt.
                continue
            if not is_valid_module(file_path.name):
                invalid.append(str(file_path))

    if invalid:
        joined = "\n  - ".join(invalid)
        print(
            "Module naming violations detected. Expected <source>_<object>.py pattern:\n"
            f"  - {joined}",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Reserved for forwards compatibility; currently ignored.",
    )
    parser.parse_args()
    return check_module_names()


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
