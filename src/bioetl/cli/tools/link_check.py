"""Shim для совместимости с `bioetl-link-check`."""

from __future__ import annotations

from tools.link_check import app, main, run, run_link_check

__all__ = ["app", "main", "run", "run_link_check"]


if __name__ == "__main__":
    run()

