"""Shim для совместимости с `bioetl-create-matrix-doc-code`."""

from __future__ import annotations

from tools.create_matrix_doc_code import (
    DocCodeMatrix,
    app,
    build_matrix,
    main,
    run,
    write_matrix,
)

__all__ = ["app", "main", "run", "write_matrix", "build_matrix", "DocCodeMatrix"]


if __name__ == "__main__":
    run()

