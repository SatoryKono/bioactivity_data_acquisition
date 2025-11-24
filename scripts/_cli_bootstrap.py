"""Shared helpers for standalone CLI scripts.

Importing this module ensures that ``src/`` is added to ``sys.path`` so that
scripts executed via ``python scripts/foo.py`` can import ``bioetl`` packages
without requiring ``pip install -e .`` beforehand.
"""

from __future__ import annotations

import sys
from pathlib import Path


def configure_path() -> None:
    """Add the project ``src`` directory to ``sys.path`` if missing."""

    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"

    if src_dir.exists():
        src_str = str(src_dir)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)


__all__ = ["configure_path"]
