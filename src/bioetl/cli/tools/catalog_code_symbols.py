"""Shim для совместимости с `bioetl-catalog-code-symbols`."""

from __future__ import annotations

from tools.catalog_code_symbols import (
    CodeCatalog,
    app,
    catalog_code_symbols,
    main,
    run,
)

__all__ = ["app", "main", "run", "catalog_code_symbols", "CodeCatalog"]


if __name__ == "__main__":
    run()

