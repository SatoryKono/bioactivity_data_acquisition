"""EOL shim for pagination helpers.

This module will be removed; import helpers from
``bioetl.core.http.pagination_helpers`` instead.
"""

from __future__ import annotations

import warnings

from bioetl.core.http import pagination_helpers as _pagination_helpers

DEPRECATION_MESSAGE = (
    "bioetl.clients.utils.pagination is deprecated and will be removed; "
    "import from bioetl.core.http.pagination_helpers instead."
)

__all__ = list(_pagination_helpers.__all__)

DEFAULT_NEXT_KEY = _pagination_helpers.DEFAULT_NEXT_KEY
DEFAULT_PAGE_KEY = _pagination_helpers.DEFAULT_PAGE_KEY
DEFAULT_PAGE_PARAM = _pagination_helpers.DEFAULT_PAGE_PARAM
PaginationStrategy = _pagination_helpers.PaginationStrategy


def _warn_deprecated() -> None:
    warnings.warn(DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=3)


def normalize_payload(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.normalize_payload(*args, **kwargs)


def iter_pages(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.iter_pages(*args, **kwargs)


def iter_ids(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.iter_ids(*args, **kwargs)


def iterate_records(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.iterate_records(*args, **kwargs)


def iterate_entity_records(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.iterate_entity_records(*args, **kwargs)


def list_entities(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.list_entities(*args, **kwargs)


def warn_fetch_all(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.warn_fetch_all(*args, **kwargs)


def fetch_all_entities(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    return _pagination_helpers.fetch_all_entities(*args, **kwargs)
