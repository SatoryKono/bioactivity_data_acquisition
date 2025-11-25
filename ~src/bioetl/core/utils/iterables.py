"""Iterable helper utilities.

This module is kept as a thin compatibility wrapper. The implementation of
``is_non_string_iterable`` now lives in :mod:`bioetl.core.infra.iterables`.
"""

from __future__ import annotations

from bioetl.core.infra.iterables import is_non_string_iterable

__all__ = ["is_non_string_iterable"]
