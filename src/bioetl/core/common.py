"""Common utilities shared across BioETL core modules."""

from __future__ import annotations

from typing import Any

__all__ = ["ChemblReleaseMixin"]


class ChemblReleaseMixin:
    """Mixin providing normalised storage for ChEMBL release metadata."""

    _chembl_release: str | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._chembl_release = None
        super().__init__(*args, **kwargs)

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release identifier."""

        return self._chembl_release

    def _set_chembl_release(self, value: str | None) -> None:
        """Store a ChEMBL release identifier after normalisation."""

        if value is None:
            self._chembl_release = None
            return

        if not isinstance(value, str):
            value = str(value)

        normalized = value.strip()
        self._chembl_release = normalized or None
