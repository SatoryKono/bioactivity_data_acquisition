"""Utility mixins shared across ChEMBL pipeline implementations."""

from __future__ import annotations

from typing import Any

__all__ = ["ChemblOptionalStringValueMixin"]


class ChemblOptionalStringValueMixin:
    """Provide normalised storage helpers for optional string metadata."""

    @staticmethod
    def _normalise_optional_string(value: Any) -> str | None:
        """Coerce arbitrary values to a normalised optional string."""

        if value is None:
            return None

        if not isinstance(value, str):
            value = str(value)

        normalised = value.strip()
        return normalised or None

    def _get_optional_string_value(
        self,
        attr_name: str,
        *,
        field_name: str,
    ) -> str | None:
        """Return a cached optional string, validating the stored value."""

        current_value = getattr(self, attr_name, None)
        if current_value is None:
            return None

        if not isinstance(current_value, str):
            msg = (
                f"Attribute '{field_name}' must be stored as a string or None; "
                f"got {type(current_value).__name__}."
            )
            raise TypeError(msg)

        normalised = self._normalise_optional_string(current_value)
        if normalised != current_value:
            setattr(self, attr_name, normalised)
        return normalised

    def _set_optional_string_value(
        self,
        attr_name: str,
        value: Any,
        *,
        field_name: str,
    ) -> None:
        """Normalise and store an optional string value."""

        normalised = self._normalise_optional_string(value)
        setattr(self, attr_name, normalised)
