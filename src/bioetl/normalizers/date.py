"""Date and timestamp normalisation utilities."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Iterable

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.helpers import _is_na

logger = UnifiedLogger.get(__name__)


def _iter_parse_candidates(value: str) -> Iterable[datetime]:
    """Yield parsed datetime values using a suite of known formats."""

    try:
        yield datetime.fromisoformat(value)
    except ValueError:
        pass

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y%m%d",
        "%d.%m.%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
    )

    for fmt in formats:
        try:
            yield datetime.strptime(value, fmt)
        except ValueError:
            continue


class DateNormalizer(BaseNormalizer):
    """Normalise date-like values to canonical ISO-8601 strings."""

    def normalize(self, value: Any, **_: Any) -> str | None:
        if _is_na(value):
            return None

        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            if value.tzinfo is not None:
                value = value.tz_convert(UTC)
            return value.date().isoformat()

        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(UTC)
            return value.date().isoformat()

        if isinstance(value, date):
            return value.isoformat()

        text = str(value).strip()
        if not text:
            return None

        for candidate in _iter_parse_candidates(text):
            try:
                return candidate.date().isoformat()
            except (ValueError, OverflowError):
                continue

        logger.debug("date_normalization_failed", value=text)
        return None

    def validate(self, value: Any) -> bool:
        if _is_na(value):
            return True
        try:
            return self.normalize(value) is not None
        except Exception:  # pragma: no cover - defensive guard
            return False


__all__ = ["DateNormalizer"]
