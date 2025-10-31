"""Pure parsing helpers for PubChem API responses."""

from __future__ import annotations

from typing import Any, Iterable

__all__ = ["PubChemParser"]


class PubChemParser:
    """Parse JSON payloads returned by PubChem."""

    @staticmethod
    def parse_cid_response(response: dict[str, Any] | None) -> int | None:
        """Extract the first CID from an identifier lookup response."""

        if not response:
            return None
        identifier_list = response.get("IdentifierList")
        if not isinstance(identifier_list, dict):
            return None
        cids = identifier_list.get("CID")
        if isinstance(cids, list) and cids:
            try:
                return int(cids[0])
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def extract_cids_from_identifier_list(data: dict[str, Any] | None) -> list[int]:
        """Return all CIDs embedded in an identifier list payload."""

        if not data:
            return []
        identifier_list = data.get("IdentifierList")
        if not isinstance(identifier_list, dict):
            return []
        cids = identifier_list.get("CID")
        if not isinstance(cids, list):
            return []
        result: list[int] = []
        for value in cids:
            try:
                result.append(int(value))
            except (TypeError, ValueError):
                continue
        return result

    @staticmethod
    def parse_properties_response(response: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Extract the property records returned by PubChem."""

        if not response:
            return []
        property_table = response.get("PropertyTable")
        if not isinstance(property_table, dict):
            return []
        properties = property_table.get("Properties")
        if not isinstance(properties, list):
            return []
        filtered: list[dict[str, Any]] = []
        for entry in properties:
            if isinstance(entry, dict):
                filtered.append(entry)
        return filtered

    @staticmethod
    def extract_properties_from_table(data: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Alias kept for backwards compatibility within the parser module."""

        return PubChemParser.parse_properties_response(data)

    @staticmethod
    def iter_property_records(response: dict[str, Any] | None) -> Iterable[dict[str, Any]]:
        """Yield individual property entries from a response."""

        for record in PubChemParser.parse_properties_response(response):
            yield record
