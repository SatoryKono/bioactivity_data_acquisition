"""Helpers for constructing PubChem REST API requests."""

from __future__ import annotations

from typing import Final, Iterable, Sequence

__all__ = ["PubChemRequestBuilder"]


class PubChemRequestBuilder:
    """Build URLs and query parameters for PubChem PUG-REST calls."""

    _CID_LOOKUP_TEMPLATE: Final[str] = "/compound/inchikey/{inchikey}/cids/JSON"
    _PROPERTIES_TEMPLATE: Final[str] = "/compound/cid/{cid_list}/property/{properties}/JSON"
    _DEFAULT_PROPERTIES: Final[tuple[str, ...]] = (
        "MolecularFormula",
        "MolecularWeight",
        "SMILES",
        "ConnectivitySMILES",
        "InChI",
        "InChIKey",
        "IUPACName",
        "RegistryID",
        "RN",
        "Synonym",
    )

    @classmethod
    def build_cid_lookup_url(cls, inchikey: str) -> str:
        """Return the endpoint for resolving a CID by InChIKey."""

        sanitized = inchikey.strip().upper()
        return cls._CID_LOOKUP_TEMPLATE.format(inchikey=sanitized)

    @classmethod
    def build_properties_url(
        cls,
        cids: Sequence[int | str],
        properties: Iterable[str] | None = None,
    ) -> str:
        """Return the endpoint for retrieving compound properties."""

        cid_tokens = [str(cid) for cid in cids if str(cid)]
        cid_list = ",".join(cid_tokens)
        property_tokens = list(properties or cls._DEFAULT_PROPERTIES)
        property_list = ",".join(property_tokens)
        return cls._PROPERTIES_TEMPLATE.format(cid_list=cid_list, properties=property_list)

    @classmethod
    def get_default_properties(cls) -> tuple[str, ...]:
        """Expose the canonical property list used for enrichment."""

        return cls._DEFAULT_PROPERTIES
