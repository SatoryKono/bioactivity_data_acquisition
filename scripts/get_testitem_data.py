"""CLI entry point for the testitem pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    SRC_PATH = Path(__file__).resolve().parents[1] / "src"
    if str(SRC_PATH) not in sys.path:
        sys.path.insert(0, str(SRC_PATH))

from library.cli.pipeline_app import create_pipeline_app

app = create_pipeline_app("testitem")


if __name__ == "__main__":
    app()
