"""Shim для совместимости с `bioetl-semantic-diff`."""

from __future__ import annotations

from tools.semantic_diff import app, main, run, run_semantic_diff

__all__ = ["app", "main", "run", "run_semantic_diff"]


if __name__ == "__main__":
    run()

