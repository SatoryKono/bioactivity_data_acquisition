from __future__ import annotations

import warnings

from bioetl.core.http.pagination_helpers import *  # noqa: F401,F403


def _warn_deprecated() -> None:
    warnings.warn(
        "bioetl.core.http.entity_helpers is deprecated; import from "
        "bioetl.core.http.pagination_helpers instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def iterate_entity_records(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import iterate_entity_records as _iterate_entity_records

    return _iterate_entity_records(*args, **kwargs)


def list_entities(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import list_entities as _list_entities

    return _list_entities(*args, **kwargs)


def fetch_all_entities(*args, **kwargs):  # type: ignore[override]
    _warn_deprecated()
    from bioetl.core.http.pagination_helpers import fetch_all_entities as _fetch_all_entities

    return _fetch_all_entities(*args, **kwargs)


from bioetl.core.http.pagination_helpers import __all__  # noqa: F401  E402
