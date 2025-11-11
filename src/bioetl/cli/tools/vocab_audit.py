"""Shim для совместимости с `bioetl-vocab-audit`."""

from __future__ import annotations

from tools.vocab_audit import (
    DEFAULT_META,
    DEFAULT_OUTPUT,
    VocabAuditResult,
    app,
    audit_vocabularies,
    main,
    run,
)

__all__ = [
    "app",
    "main",
    "run",
    "audit_vocabularies",
    "DEFAULT_OUTPUT",
    "DEFAULT_META",
    "VocabAuditResult",
]


if __name__ == "__main__":
    run()

