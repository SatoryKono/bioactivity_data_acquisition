"""Main CLI entry point for BioETL pipelines.

This module provides backward compatibility by importing the app from app.py.
New code should use app.py directly.
"""

from __future__ import annotations

from bioetl.cli.app import app, run

__all__ = ["app", "run"]


if __name__ == "__main__":
    run()
