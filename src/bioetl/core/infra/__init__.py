"""Infrastructure helpers for the BioETL core package.

This package hosts low-level, domain-agnostic utilities such as iterables and
typechecks that are reused across pipelines.
"""

from __future__ import annotations

from .iterables import is_non_string_iterable
from .typechecks import is_dict, is_list

__all__ = [
    "is_non_string_iterable",
    "is_list",
    "is_dict",
]
