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
    def parse_information_response(
        response: dict[str, Any] | None,
        field: str,
    ) -> dict[int, list[Any]]:
        """Parse responses that use the ``InformationList`` wrapper."""

        if not response:
            return {}
        information_list = response.get("InformationList")
        if not isinstance(information_list, dict):
            return {}
        information = information_list.get("Information")
        if not isinstance(information, list):
            return {}

        results: dict[int, list[Any]] = {}
        for entry in information:
            if not isinstance(entry, dict):
                continue
            cid = entry.get("CID")
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            values = entry.get(field)
            if values is None:
                continue
            if isinstance(values, list):
                cleaned = [value for value in values if value is not None]
            else:
                cleaned = [values]
            if cleaned:
                results[cid_int] = cleaned
        return results

    @staticmethod
    def parse_synonyms_response(response: dict[str, Any] | None) -> dict[int, list[Any]]:
        """Extract synonym lists keyed by CID."""

        return PubChemParser.parse_information_response(response, "Synonym")

    @staticmethod
    def parse_registry_ids_response(response: dict[str, Any] | None) -> dict[int, list[Any]]:
        """Extract RegistryID cross references keyed by CID."""

        return PubChemParser.parse_information_response(response, "RegistryID")

    @staticmethod
    def parse_rn_response(response: dict[str, Any] | None) -> dict[int, list[Any]]:
        """Extract RN cross references keyed by CID."""

        return PubChemParser.parse_information_response(response, "RN")

    @staticmethod
    def extract_properties_from_table(data: dict[str, Any] | None) -> list[dict[str, Any]]:
        """Alias kept for backwards compatibility within the parser module."""

        return PubChemParser.parse_properties_response(data)

    @staticmethod
    def iter_property_records(response: dict[str, Any] | None) -> Iterable[dict[str, Any]]:
        """Yield individual property entries from a response."""

        for record in PubChemParser.parse_properties_response(response):
            yield record
