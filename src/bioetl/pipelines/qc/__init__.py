"""Compatibility shim exposing QC helpers under the legacy namespace."""

from __future__ import annotations

import sys
from importlib import import_module

from bioetl.qc.boundary_check import (
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

# Ensure legacy submodule path resolves.
sys.modules[f"{__name__}.boundary_check"] = import_module("bioetl.qc.boundary_check")

