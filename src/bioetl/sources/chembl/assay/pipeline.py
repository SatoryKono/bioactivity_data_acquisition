"""Compatibility wrapper for the relocated ChEMBL assay pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["AssayPipeline"]


if TYPE_CHECKING:  # pragma: no cover - import-time cycle guard for typing
    from bioetl.pipelines.chembl.chembl_assay import AssayPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    if name == "AssayPipeline":
        from bioetl.pipelines.chembl.chembl_assay import AssayPipeline

        return AssayPipeline
    raise AttributeError(name)
