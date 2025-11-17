"""Unified helpers for computing deterministic hashes."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

_DEFAULT_SEPARATOR = "\u001f"

_pd: Any | None
try:  # Optional dependency (avoids importing pandas when not installed)
    import pandas as _pd
except ImportError:  # pragma: no cover - pandas is always available in runtime env
    _pd = None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if _pd is not None:
        # pandas exposes dedicated singleton objects for NA values
        if value is getattr(_pd, "NA", object()):
            return True
        if hasattr(_pd, "isna"):
            try:
                result = _pd.isna(value)
            except TypeError:
                pass
            else:
                if isinstance(result, bool):
                    if result:
                        return True
                elif hasattr(result, "all"):
                    if bool(result.all()):
                        return True
                else:
                    if bool(result):
                        return True
    return False


def _normalise_component(value: Any) -> str:
    """Convert arbitrary values to a canonical string representation."""

    if _is_missing(value):
        return ""

    if isinstance(value, str):
        return value.strip().lower()

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip().lower()

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Mapping):
        items: list[str] = []
        for key in sorted(value.keys(), key=lambda candidate: str(candidate)):
            component = value[key]
            key_part = str(key).strip().lower()
            value_part = _normalise_component(component)
            items.append(f"{key_part}={value_part}")
        return _DEFAULT_SEPARATOR.join(items)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        normalised_items = [_normalise_component(item) for item in value]
        return _DEFAULT_SEPARATOR.join(sorted(normalised_items))

    return str(value).strip().lower()


def compute_hash(
    components: Iterable[Any],
    *,
    algorithm: str = "sha256",
    separator: str = _DEFAULT_SEPARATOR,
) -> str:
    """Return digest for the provided components using canonical normalisation."""

    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:  # pragma: no cover - defensive guard
        msg = f"Unsupported hash algorithm: {algorithm}"
        raise ValueError(msg) from exc

    normalised = [_normalise_component(part) for part in components]
    payload = separator.join(normalised)
    digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


def hash_from_mapping(
    data: Mapping[str, Any],
    fields: Sequence[str],
    *,
    algorithm: str = "sha256",
    separator: str = _DEFAULT_SEPARATOR,
) -> str:
    """Compute hash for subset of mapping fields (pd.Series compatible)."""

    missing = [field for field in fields if field not in data]
    if missing:
        msg = f"Fields required for hashing are missing: {missing}"
        raise KeyError(msg)

    values = (data[field] for field in fields)
    return compute_hash(values, algorithm=algorithm, separator=separator)


__all__ = [
    "compute_hash",
    "hash_from_mapping",
]
