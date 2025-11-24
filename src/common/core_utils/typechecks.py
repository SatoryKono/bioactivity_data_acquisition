"""Common runtime type-checking helpers used across the project.

This module is kept as a thin compatibility wrapper. The implementations of
``is_list`` and ``is_dict`` now live in :mod:`infrastructure.infra.typechecks`.
"""

from __future__ import annotations

from infrastructure.infra.typechecks import is_dict, is_list

__all__ = ["is_list", "is_dict"]
