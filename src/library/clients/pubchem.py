"""HTTP client for PubChem API endpoints."""

from __future__ import annotations

import logging
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class PubChemClient(BaseApiClient):
    """HTTP client for PubChem API."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_compound_properties(self, cid: str) -> dict[str, Any]:
        """Fetch compound properties by PubChem CID."""
        try:
            payload = self._request("GET", f"compound/cid/{cid}/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey/JSON")
            return self._parse_compound_properties(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem properties for CID {cid}: {e}")
            return {}

    def fetch_compound_xrefs(self, cid: str) -> dict[str, Any]:
        """Fetch compound cross-references by PubChem CID."""
        try:
            payload = self._request("GET", f"compound/cid/{cid}/xrefs/RegistryID,RN/JSON")
            return self._parse_compound_xrefs(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem xrefs for CID {cid}: {e}")
            return {}

    def fetch_compound_synonyms(self, cid: str) -> dict[str, Any]:
        """Fetch compound synonyms by PubChem CID."""
        try:
            payload = self._request("GET", f"compound/cid/{cid}/synonyms/JSON")
            return self._parse_compound_synonyms(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem synonyms for CID {cid}: {e}")
            return {}

    def fetch_compound_by_name(self, name: str) -> dict[str, Any]:
        """Fetch compound CIDs by name."""
        try:
            payload = self._request("GET", f"compound/name/{name}/cids/JSON")
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem CIDs for name {name}: {e}")
            return {}

    def fetch_compound_by_inchikey(self, inchikey: str) -> dict[str, Any]:
        """Fetch compound CIDs by InChIKey."""
        try:
            payload = self._request("GET", f"compound/inchikey/{inchikey}/cids/JSON")
            logger.debug(f"PubChem InChIKey request payload: {payload}")
            if payload is None:
                logger.warning(f"PubChem request returned None for InChIKey {inchikey}")
                return {}
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem CIDs for InChIKey {inchikey}: {e}")
            return {}

    def fetch_compound_by_smiles(self, smiles: str) -> dict[str, Any]:
        """Fetch compound CIDs by SMILES."""
        try:
            # URL encode SMILES for the request
            import urllib.parse

            encoded_smiles = urllib.parse.quote(smiles, safe="")
            payload = self._request("GET", f"compound/smiles/{encoded_smiles}/cids/JSON")
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch PubChem CIDs for SMILES {smiles[:50]}...: {e}")
            return {}

    def _parse_compound_properties(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound properties from PubChem response."""
        properties = payload.get("PropertyTable", {}).get("Properties", [])
        if not properties:
            return {}

        props = properties[0]
        # Note: PubChem API doesn't return separate CanonicalSMILES and IsomericSMILES
        # It returns SMILES and ConnectivitySMILES, which are often the same
        smiles = props.get("SMILES")
        connectivity_smiles = props.get("ConnectivitySMILES")

        return {
            "pubchem_molecular_formula": props.get("MolecularFormula"),
            "pubchem_molecular_weight": props.get("MolecularWeight"),
            "pubchem_canonical_smiles": smiles,  # Use SMILES as canonical
            "pubchem_isomeric_smiles": connectivity_smiles if connectivity_smiles != smiles else smiles,  # Use ConnectivitySMILES if different
            "pubchem_inchi": props.get("InChI"),
            "pubchem_inchi_key": props.get("InChIKey"),
        }

    def _parse_compound_xrefs(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound cross-references from PubChem response."""
        xrefs = payload.get("InformationList", {}).get("Information", [])
        if not xrefs:
            return {}

        xref_data = xrefs[0]
        return {"pubchem_registry_id": xref_data.get("RegistryID"), "pubchem_rn": xref_data.get("RN")}

    def _parse_compound_synonyms(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound synonyms from PubChem response."""
        synonyms = payload.get("InformationList", {}).get("Information", [])
        if not synonyms:
            return {}

        synonym_list = synonyms[0].get("Synonym", [])
        return {"pubchem_synonyms": synonym_list}

    def _parse_compound_cids(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound CIDs from PubChem response."""
        if not payload:
            return {"pubchem_cids": []}
        cids = payload.get("IdentifierList", {}).get("CID", [])
        return {"pubchem_cids": cids}
