"""Compatibility wrapper for the relocated ChEMBL target pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["TargetPipeline"]


if TYPE_CHECKING:  # pragma: no cover
    from bioetl.pipelines.chembl_target import TargetPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_target as _chembl_target

    try:
        return getattr(_chembl_target, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
