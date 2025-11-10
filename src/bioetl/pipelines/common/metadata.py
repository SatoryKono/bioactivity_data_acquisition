"""Shared helpers for pipeline metadata normalisation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

__all__ = ["normalise_metadata_value"]


def _normalise_timestamp(value: datetime) -> str:
    """Return an ISO-8601 UTC string for ``value``.

    The framework expects all temporal metadata to be serialised in the
    ``YYYY-MM-DDTHH:MM:SS.sssZ`` format.  Downstream tests verify the `Z`
    suffix, so timestamps must be normalised to UTC explicitly.
    """

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def normalise_metadata_value(candidate: Any) -> Any:
    """Normalise metadata payload values for deterministic serialisation.

    This helper recursively walks mappings and sequences to ensure the
    resulting structure is stable when written to ``meta.yaml``:

    * mapping keys are coerced to ``str`` and sorted alphabetically;
    * sequences are normalised element-wise while preserving their order;
    * datetimes are converted to UTC ISO-8601 strings with a trailing ``Z``.

    Parameters
    ----------
    candidate:
        Arbitrary metadata value that requires normalisation.

    Returns
    -------
    Any
        The normalised value that is safe to serialise.
    """

    if isinstance(candidate, Mapping):
        mapping_items = []
        for key, value in candidate.items():
            key_as_str = str(key)
            normalised_value = normalise_metadata_value(value)
            mapping_items.append((key_as_str, normalised_value))
        mapping_items.sort(key=lambda item: item[0])
        return {key: value for key, value in mapping_items}
    if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
        return [normalise_metadata_value(item) for item in candidate]
    if isinstance(candidate, datetime):
        return _normalise_timestamp(candidate)
    return candidate

