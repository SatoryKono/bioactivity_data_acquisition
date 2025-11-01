"""Test item pipeline namespace for ChEMBL source."""

from __future__ import annotations

from typing import Any

__all__ = ["TestItemPipeline"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_testitem as _chembl_testitem

    try:
        return getattr(_chembl_testitem, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
