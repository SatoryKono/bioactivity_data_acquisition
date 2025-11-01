"""Compatibility helpers for importing Pandera typing aliases.

This module provides a stable import location for ``Series`` and ``DataFrame``
so we can support multiple Pandera versions. Pandera <0.17 exposed the aliases
from ``pandera.typing.pandas`` while newer releases re-export them from the
package root. Some type-checkers report ``AttributeError`` when the re-export is
missing, which breaks ``from pandera.typing import Series`` style imports.

By centralising the import logic here we gracefully handle both code paths and
keep downstream schema modules clean and type-checker friendly.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, TypeAlias, cast


def _resolve_alias(name: str) -> type[Any]:
    """Return the ``pandera.typing`` alias named ``name`` across versions."""

    typing_module = import_module("pandera.typing")
    alias = getattr(typing_module, name, None)
    if alias is not None:
        return cast("type[Any]", alias)

    pandas_typing = import_module("pandera.typing.pandas")
    return cast("type[Any]", getattr(pandas_typing, name))


if TYPE_CHECKING:
    # Explicit type aliases for static type checkers
    from pandera.typing import DataFrame as _DataFrameAlias
    from pandera.typing import Series as _SeriesAlias

    Series: TypeAlias = _SeriesAlias[Any]
    DataFrame: TypeAlias = _DataFrameAlias[Any]
else:
    # Runtime resolution for compatibility across Pandera versions
    Series = _resolve_alias("Series")
    DataFrame = _resolve_alias("DataFrame")

__all__ = ["Series", "DataFrame"]
