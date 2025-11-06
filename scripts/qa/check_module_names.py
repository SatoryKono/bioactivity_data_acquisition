#!/usr/bin/env python3
"""Ensure that Python module filenames follow snake_case conventions."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

MODULE_ROOT = Path("src") / "bioetl"
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


def iter_python_modules(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        yield path


def validate_module_name(path: Path) -> str | None:
    stem = path.stem
    if not SNAKE_CASE.match(stem):
        return f"Invalid module name '{stem}' at {path.as_posix()}"
    return None


def main() -> int:
    if not MODULE_ROOT.exists():
        print(f"⚠️ Module root not found: {MODULE_ROOT}")
        return 0

    errors = [
        error
        for module_path in iter_python_modules(MODULE_ROOT)
        if (error := validate_module_name(module_path)) is not None
    ]

    if errors:
        print("❌ Module naming violations detected:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("✅ Module naming checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
