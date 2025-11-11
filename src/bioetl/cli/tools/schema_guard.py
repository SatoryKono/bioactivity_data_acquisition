"""Shim для совместимости с `bioetl-schema-guard`."""

from __future__ import annotations

from tools.schema_guard import app, main, run, run_schema_guard

__all__ = ["app", "main", "run", "run_schema_guard"]


if __name__ == "__main__":
    run()
