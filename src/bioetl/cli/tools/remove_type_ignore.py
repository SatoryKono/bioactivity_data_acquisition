"""Shim для совместимости с `bioetl-remove-type-ignore`."""

from __future__ import annotations

from tools.remove_type_ignore import app, main, remove_type_ignore, run

__all__ = ["app", "main", "run", "remove_type_ignore"]


if __name__ == "__main__":
    run()

