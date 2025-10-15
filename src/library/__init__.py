"""Deprecated compatibility package."""
from __future__ import annotations

from warnings import warn

from bioactivity import *  # noqa: F401,F403
from bioactivity import __all__ as _bioactivity_all

warn("deprecated", DeprecationWarning, stacklevel=2)

__all__ = list(_bioactivity_all)
