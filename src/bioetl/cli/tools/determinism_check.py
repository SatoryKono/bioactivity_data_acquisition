"""Shim для совместимости с `bioetl-determinism-check`."""

from __future__ import annotations

from tools.determinism_check import (
    DeterminismRunResult,
    app,
    main,
    run,
    run_determinism_check,
)

__all__ = ["app", "main", "run", "run_determinism_check", "DeterminismRunResult"]


if __name__ == "__main__":
    run()

