"""Shim для совместимости с `bioetl-check-output-artifacts`."""

from __future__ import annotations

from tools.check_output_artifacts import (
    MAX_BYTES,
    app,
    check_output_artifacts,
    main,
    run,
)

__all__ = ["app", "main", "run", "check_output_artifacts", "MAX_BYTES"]


if __name__ == "__main__":
    run()

