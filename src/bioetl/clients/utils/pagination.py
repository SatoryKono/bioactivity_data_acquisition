from __future__ import annotations

import warnings

from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403


def _warn_deprecated() -> None:
    warnings.warn(
        "bioetl.clients.utils.pagination is deprecated; import from "
        "bioetl.core.http.pagination_helpers instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def normalize_payload(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import normalize_payload as _normalize_payload

    return _normalize_payload(*args, **kwargs)


def iter_pages(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import iter_pages as _iter_pages

    return _iter_pages(*args, **kwargs)


def iter_ids(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import iter_ids as _iter_ids

    return _iter_ids(*args, **kwargs)


def iterate_records(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import iterate_records as _iterate_records

    return _iterate_records(*args, **kwargs)


def list_entities(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import list_entities as _list_entities

    return _list_entities(*args, **kwargs)


def warn_fetch_all(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import warn_fetch_all as _warn_fetch_all

    return _warn_fetch_all(*args, **kwargs)


from bioetl.core.http.pagination_helpers import __all__  # noqa: F401  E402
