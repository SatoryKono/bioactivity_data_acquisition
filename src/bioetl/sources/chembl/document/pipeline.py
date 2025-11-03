"""Compatibility wrapper for the relocated ChEMBL document pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["DocumentPipeline"]


if TYPE_CHECKING:  # pragma: no cover
    from bioetl.pipelines.chembl_document import DocumentPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_document as _chembl_document

    try:
        return getattr(_chembl_document, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
