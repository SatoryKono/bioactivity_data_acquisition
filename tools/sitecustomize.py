"""Ensure src/ is on sys.path for CLI entrypoints."""

from __future__ import annotations

import sys
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parent / "src"
if SRC_PATH.exists():
    path_str = str(SRC_PATH)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
