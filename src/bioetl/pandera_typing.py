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

from typing import TYPE_CHECKING


try:  # pragma: no cover - exercised implicitly by import behaviour.
    from pandera.typing import Series as _Series
except (ImportError, AttributeError):  # pragma: no cover
    # Fallback for older Pandera releases (<0.17) where the alias lives inside
    # the pandas backend module.
    from pandera.typing.pandas import Series as _Series  # type: ignore[attr-defined]


Series = _Series

__all__ = ["Series"]


if TYPE_CHECKING:  # pragma: no cover - assists IDEs and static analysers.
    # Re-export ``Series`` for tools that inspect TYPE_CHECKING blocks.
    from pandera.typing import Series as _SeriesCheck  # noqa: F401
