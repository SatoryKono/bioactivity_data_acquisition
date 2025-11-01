"""Assay pipeline namespace for ChEMBL source."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["AssayPipeline"]


if TYPE_CHECKING:  # pragma: no cover - import-time cycle guard for typing
    from bioetl.pipelines.chembl_assay import AssayPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_assay as _chembl_assay

    try:
        return getattr(_chembl_assay, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
