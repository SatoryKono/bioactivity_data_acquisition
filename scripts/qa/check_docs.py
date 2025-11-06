#!/usr/bin/env python3
"""Validate that key documentation files are present and non-empty."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Tuple

REQUIRED_DOCS: Tuple[Tuple[Path, str], ...] = (
    (Path("README.md"), "Repository README"),
    (Path("docs/INDEX.md"), "Documentation index"),
)


def has_heading(text: str) -> bool:
    """Return True if the text contains at least one markdown heading."""
    return any(line.lstrip().startswith("#") for line in text.splitlines())


def validate_document(path: Path, label: str) -> Iterable[str]:
    """Yield validation errors for the provided document."""
    if not path.exists():
        yield f"{label} missing: {path}"
        return

    if not path.is_file():
        yield f"{label} is not a regular file: {path}"
        return

    content = path.read_text(encoding="utf-8").strip()
    if not content:
        yield f"{label} is empty: {path}"

    if not has_heading(content):
        yield f"{label} does not contain a markdown heading: {path}"


def main() -> int:
    errors = [error for doc in REQUIRED_DOCS for error in validate_document(*doc)]

    if errors:
        print("❌ Documentation checks failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("✅ Documentation checks passed for required files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
