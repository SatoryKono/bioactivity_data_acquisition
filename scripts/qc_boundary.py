"""Прокси к доменному AST-анализатору границы CLI ↔ QC."""

from __future__ import annotations

from bioetl.domain.qc.boundary_tools import (
    DEFAULT_PACKAGE,
    DEFAULT_SRC_ROOT,
    ModuleAnalysis,
    ModuleRecord,
    QC_MODULE_PREFIX,
    Violation,
    collect_qc_boundary_violations,
)

__all__ = [
    "QC_MODULE_PREFIX",
    "DEFAULT_PACKAGE",
    "DEFAULT_SRC_ROOT",
    "ModuleRecord",
    "ModuleAnalysis",
    "Violation",
    "collect_qc_boundary_violations",
]
