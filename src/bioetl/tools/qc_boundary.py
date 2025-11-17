"""Deprecated shim for QC boundary AST analysis helpers."""

from __future__ import annotations

import warnings

from bioetl.domain.qc.boundary_tools import *  # noqa: F401,F403
from bioetl.domain.qc.boundary_tools import __all__ as _domain_all

__all__ = tuple(_domain_all)

warnings.warn(
    "'bioetl.tools.qc_boundary' is deprecated; use 'bioetl.domain.qc.boundary_tools' instead.",
    DeprecationWarning,
    stacklevel=2,
)
