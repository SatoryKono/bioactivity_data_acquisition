"""Пайплайновые интерфейсы для QC-сценариев."""

from __future__ import annotations

from .boundary_check import (
    DEFAULT_PACKAGE,
    DEFAULT_SOURCE_ROOT,
    QC_MODULE_PREFIX,
    QCBoundaryReport,
    QCBoundaryViolation,
    collect_cli_qc_boundary_report,
)

__all__ = [
    "DEFAULT_PACKAGE",
    "DEFAULT_SOURCE_ROOT",
    "QC_MODULE_PREFIX",
    "QCBoundaryReport",
    "QCBoundaryViolation",
    "collect_cli_qc_boundary_report",
]


