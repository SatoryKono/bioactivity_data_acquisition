"""Compatibility helpers for importing Pandera typing aliases.

This module provides a stable import location for ``Series`` so we can support
multiple Pandera versions. Pandera <0.17 exposed the alias from
``pandera.typing.pandas`` while newer releases re-export it from the package
root. Some type-checkers report ``AttributeError`` when the re-export is
missing, which breaks ``from pandera.typing import Series`` imports.

By centralising the import logic here we gracefully handle both code paths and
keep downstream schema modules clean and type-checker friendly.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, cast


def _resolve_series() -> type[Any]:
    """Return the ``pandera.typing.Series`` alias across Pandera versions."""

    typing_module = import_module("pandera.typing")
    series = getattr(typing_module, "Series", None)
    if series is not None:
        return cast("type[Any]", series)

    pandas_typing = import_module("pandera.typing.pandas")
    return cast("type[Any]", pandas_typing.Series)


if TYPE_CHECKING:  # pragma: no cover - assists IDEs and static analysers.
    from pandera.typing import Series as _SeriesType

Series = cast("type[_SeriesType[Any]]", _resolve_series())

__all__ = ["Series"]
