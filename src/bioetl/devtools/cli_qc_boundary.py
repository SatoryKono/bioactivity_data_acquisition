"""Прокси к пайплайновому анализу границы CLI ↔ QC."""

from __future__ import annotations

from application.pipelines.specs.qc import (
    DEFAULT_PACKAGE,
    DEFAULT_SOURCE_ROOT,
    QC_MODULE_PREFIX,
    QCBoundaryReport,
    QCBoundaryViolation,
    collect_cli_qc_boundary_report,
)

__all__ = [
    "QC_MODULE_PREFIX",
    "DEFAULT_PACKAGE",
    "DEFAULT_SOURCE_ROOT",
    "QCBoundaryViolation",
    "QCBoundaryReport",
    "collect_cli_qc_boundary_report",
]
