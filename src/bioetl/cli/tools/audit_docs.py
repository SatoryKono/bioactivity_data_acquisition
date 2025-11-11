"""Shim для совместимости с `bioetl-audit-docs`."""

from __future__ import annotations

from tools.audit_docs import app, main, run, run_audit

__all__ = ["app", "main", "run", "run_audit"]


if __name__ == "__main__":
    run()


