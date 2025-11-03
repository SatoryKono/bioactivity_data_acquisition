"""Normalisation helpers for ChEMBL activity payloads."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from bioetl.normalizers import registry
from bioetl.sources.chembl.activity.constants import (
    ACTIVITY_TYPE_ALIASES,
    QUDT_UNITS_TO_STANDARD,
    UO_UNITS_TO_STANDARD,
)

__all__ = ["ActivityNormalizer"]


class ActivityNormalizer:
    """Provide reusable helpers for normalising activity attributes."""

    @staticmethod
    def normalize_int_scalar(value: Any) -> int | None:
        """Safely coerce ``value`` to an ``int`` or ``None``."""

        try:
            text = str(value).strip()
            if not text:
                return None
            return int(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _canonicalize_activity_token(value: str) -> str:
        """Return a lower-cased token with spaces, dashes and underscores removed."""

        collapsed = re.sub(r"[\s_-]+", "", value)
        return collapsed.lower()

    def normalize_activity_type(self, value: Any) -> str | None:
        """Normalise activity type labels to the canonical casing used by ChEMBL."""

        text = registry.normalize("chemistry.string", value)
        if not text:
            return None

        token = self._canonicalize_activity_token(text)
        if not token:
            return None

        # Handle prefixed measures like pIC50 explicitly by removing the prefix
        # and mapping the suffix through the alias table.
        if token.startswith("p") and token[1:] in ACTIVITY_TYPE_ALIASES:
            suffix = ACTIVITY_TYPE_ALIASES[token[1:]]
            if suffix.startswith("p"):
                return suffix
            return f"p{suffix}"

        alias = ACTIVITY_TYPE_ALIASES.get(token)
        if alias:
            return alias

        # Fallback: return the original text with collapsed spacing while
        # preserving the original casing for clarity.
        collapsed_original = re.sub(r"[\s_-]+", " ", text).strip()
        if collapsed_original:
            return collapsed_original
        return text

    def normalize_units(self, *values: Any, default: str | None = None) -> str | None:
        """Return the first successfully normalised unit from ``values``."""

        for candidate in values:
            # Prefer textual units provided directly by the API response.
            normalised = registry.normalize("chemistry.units", candidate)
            if normalised:
                if isinstance(normalised, str):
                    stripped = normalised.strip()
                    if stripped in UO_UNITS_TO_STANDARD:
                        return UO_UNITS_TO_STANDARD[stripped]
                    lookup = QUDT_UNITS_TO_STANDARD.get(stripped.lower())
                    if lookup:
                        return lookup
                return normalised

            if isinstance(candidate, str):
                key = candidate.strip()
                if key in UO_UNITS_TO_STANDARD:
                    return UO_UNITS_TO_STANDARD[key]
                lookup = QUDT_UNITS_TO_STANDARD.get(key.lower())
                if lookup:
                    return lookup

        return default

    def derive_compound_key(
        self,
        molecule_id: str | None,
        standard_type: str | None,
        target_id: str | None,
    ) -> str | None:
        """Return a composite compound identifier when all parts are known."""

        if molecule_id and standard_type and target_id:
            return "|".join([molecule_id, standard_type, target_id])
        return None

    def derive_is_citation(
        self,
        document_id: str | None,
        properties: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Infer citation status from linked document identifiers or properties."""

        if document_id:
            return True

        for prop in properties:
            label = str(prop.get("name") or prop.get("type") or "").lower()
            if label.replace(" ", "_") == "is_citation":
                return bool(
                    registry.normalize("boolean", prop.get("value"), default=False) or False
                )
        return False

    def derive_exact_data_citation(
        self,
        comment: str | None,
        properties: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Return whether the payload explicitly references exact data citation."""

        text = (comment or "").lower()
        if "exact" in text and "citation" in text:
            return True

        for prop in properties:
            label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
            if label == "exact_data_citation":
                return bool(
                    registry.normalize("boolean", prop.get("value"), default=False) or False
                )
        return False

    def derive_rounded_data_citation(
        self,
        comment: str | None,
        properties: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Return whether the payload references rounded data citations."""

        text = (comment or "").lower()
        if "rounded" in text and "citation" in text:
            return True

        for prop in properties:
            label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
            if label == "rounded_data_citation":
                return bool(
                    registry.normalize("boolean", prop.get("value"), default=False) or False
                )
        return False

    def derive_high_citation_rate(
        self, properties: Sequence[Mapping[str, Any]]
    ) -> bool:
        """Determine whether the activity exhibits a high citation rate."""

        for prop in properties:
            label = str(prop.get("name") or prop.get("type") or "").lower()
            if "citation" not in label:
                continue

            numeric = None
            for candidate in ("value", "num_value", "property_value", "count"):
                if candidate in prop:
                    numeric = registry.normalize("numeric", prop.get(candidate))
                    if numeric is not None:
                        break

            if numeric is not None and numeric >= 50:
                return True

            if label.replace(" ", "_") == "high_citation_rate":
                return bool(
                    registry.normalize("boolean", prop.get("value"), default=False) or False
                )

        return False

    @staticmethod
    def derive_is_censored(relation: str | None) -> bool | None:
        """Infer censorship flag from a relation symbol."""

        if relation is None:
            return None
        return relation != "="
