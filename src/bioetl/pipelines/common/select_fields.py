"""Utilities for normalising select_fields payloads.

This module centralises the logic for composing the ``select_fields``
parameter used by multiple ChEMBL pipelines. Historically each pipeline
contained bespoke helper functions to append mandatory fields or fall back
to sensible defaults. Providing a shared helper keeps the behaviour
consistent and makes it easier to reason about API request composition.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Final

__all__ = ["normalize_select_fields", "SelectFieldsMixin"]


def _dedupe_preserve_order(values: Iterable[str]) -> tuple[str, ...]:
    """Return a tuple with duplicates removed while preserving order."""

    return tuple(dict.fromkeys(values))


def normalize_select_fields(
    select_fields: Sequence[str] | None,
    *,
    required: Iterable[str] = (),
    default: Sequence[str] | None = None,
    preserve_none: bool = False,
) -> tuple[str, ...] | None:
    """Normalise ``select_fields`` respecting defaults and mandatory fields.

    Parameters
    ----------
    select_fields:
        Existing sequence of fields provided by configuration. ``None``
        indicates that the caller should not send the ``only`` query
        parameter unless a default is supplied.
    required:
        Iterable of field names that must always be present in the final
        payload. Ordering is preserved with respect to ``select_fields`` and
        ``default``.
    default:
        Default field sequence to use when ``select_fields`` is ``None``.
        Pipelines that must explicitly request a canonical set of fields can
        pass their API defaults here.
    preserve_none:
        When ``True`` and both ``select_fields`` and ``default`` are ``None``
        the helper returns ``None`` instead of a tuple. This mirrors the
        historical behaviour of pipelines where omitting the ``only``
        parameter allows the API to decide which fields to return.

    Returns
    -------
    tuple[str, ...] | None
        Normalised field list or ``None`` when ``preserve_none`` is enabled
        and no other values are available.
    """

    required_fields: Final[tuple[str, ...]] = _dedupe_preserve_order(required)

    if select_fields:
        merged = (*select_fields, *required_fields)
        return _dedupe_preserve_order(merged)

    if default:
        merged = (*default, *required_fields)
        return _dedupe_preserve_order(merged)

    if preserve_none:
        return None

    if required_fields:
        return required_fields

    return tuple()


class SelectFieldsMixin:
    """Mixin providing a convenience wrapper around ``normalize_select_fields``."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)

    def normalize_select_fields(
        self,
        select_fields: Sequence[str] | None,
        *,
        required: Iterable[str] = (),
        default: Sequence[str] | None = None,
        preserve_none: bool = False,
    ) -> tuple[str, ...] | None:
        """Instance-friendly wrapper returning normalised ``select_fields``."""

        return normalize_select_fields(
            select_fields,
            required=required,
            default=default,
            preserve_none=preserve_none,
        )

