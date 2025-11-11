"""Shim для совместимости с `bioetl-build-vocab-store`."""

from __future__ import annotations

from tools.build_vocab_store import app, build_vocab_store, main, run

__all__ = ["app", "main", "run", "build_vocab_store"]


if __name__ == "__main__":
    run()

