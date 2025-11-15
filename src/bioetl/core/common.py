"""Common utilities shared across BioETL core modules."""

from __future__ import annotations

from typing import Any

from bioetl.pipelines.chembl.common.mixins import ChemblOptionalStringValueMixin

__all__ = ["ChemblReleaseMixin"]


class ChemblReleaseMixin(ChemblOptionalStringValueMixin):
    """Mixin providing normalised storage for ChEMBL release metadata."""

    _chembl_release: str | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._chembl_release = None
        super().__init__(*args, **kwargs)

    @property
    def chembl_release(self) -> str | None:
        """Return the cached ChEMBL release identifier."""

        return self._get_optional_string_value(
            "_chembl_release", field_name="chembl_release"
        )

    def _set_chembl_release(self, value: str | None) -> None:
        """Store a ChEMBL release identifier after normalisation."""

        self._set_optional_string_value(
            "_chembl_release", value, field_name="chembl_release"
        )
