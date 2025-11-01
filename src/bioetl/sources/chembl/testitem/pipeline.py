"""Compatibility wrapper for the relocated ChEMBL test item pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["TestItemPipeline"]


if TYPE_CHECKING:  # pragma: no cover
    from bioetl.pipelines.chembl_testitem import TestItemPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_testitem as _chembl_testitem

    try:
        return getattr(_chembl_testitem, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
