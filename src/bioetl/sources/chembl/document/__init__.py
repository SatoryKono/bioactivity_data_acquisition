"""Document pipeline namespace for ChEMBL source."""

from __future__ import annotations

from typing import Any

__all__ = ["DocumentPipeline"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_document as _chembl_document

    try:
        return getattr(_chembl_document, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
