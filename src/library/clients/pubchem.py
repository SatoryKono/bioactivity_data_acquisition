"""HTTP client for PubChem API endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import quote

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class PubChemClient(BaseApiClient):
    """HTTP client for PubChem API."""

    def __init__(self, config: APIClientConfig, *, cache_dir: str | None = None, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        # Optional simple file cache directory for GET requests
        self._cache_dir: Path | None = Path(cache_dir) if cache_dir else None
        if self._cache_dir is not None:
            try:
                self._cache_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to create PubChem cache dir %s: %s", self._cache_dir, exc)
                self._cache_dir = None

    # --- simple GET JSON cache helpers ----------------------------------------------------
    def _cache_key(self, path: str) -> str:
        key_source = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        return hashlib.sha256(key_source.encode("utf-8")).hexdigest()

    def _cache_read(self, path: str) -> dict[str, Any] | None:
        if self._cache_dir is None:
            return None
        file_path = self._cache_dir / f"{self._cache_key(path)}.json"
        if not file_path.exists():
            return None
        try:
            with open(file_path) as f:
                return json.load(f)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to read PubChem cache: %s", exc)
            return None

    def _cache_write(self, path: str, payload: dict[str, Any]) -> None:
        if self._cache_dir is None:
            return
        file_path = self._cache_dir / f"{self._cache_key(path)}.json"
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to write PubChem cache: %s", exc)

    def _get_with_cache(self, path: str) -> dict[str, Any]:
        cached = self._cache_read(path)
        if cached is not None:
            return cached
        payload = self._request("GET", path)
        if isinstance(payload, dict):
            self._cache_write(path, payload)
        return payload

    def fetch_compound_properties(self, cid: str) -> dict[str, Any]:
        """Fetch compound properties by PubChem CID."""
        try:
            payload = self._get_with_cache(
                f"compound/cid/{cid}/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey/JSON"
            )
            return self._parse_compound_properties(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem properties for CID %s: %s", cid, e)
            return {}

    def fetch_compounds_properties_batch(
        self,
        cids: list[str],
        batch_size: int = 100
    ) -> dict[str, dict[str, Any]]:
        """Fetch properties for multiple CIDs.
        
        PubChem supports comma-separated CID lists:
        compound/cid/1,2,3/property/.../JSON
        """
        if not cids:
            return {}
        
        results = {}
        
        # Process in batches to avoid URL length limits
        for i in range(0, len(cids), batch_size):
            batch = cids[i:i + batch_size]
            batch_str = ",".join(batch)
            
            try:
                payload = self._get_with_cache(
                    f"compound/cid/{batch_str}/property/MolecularFormula,MolecularWeight,CanonicalSMILES,IsomericSMILES,InChI,InChIKey/JSON"
                )
                
                # Parse batch results
                if "PropertyTable" in payload and "Properties" in payload["PropertyTable"]:
                    properties_list = payload["PropertyTable"]["Properties"]
                    for prop_data in properties_list:
                        cid = str(prop_data.get("CID", ""))
                        if cid:
                            parsed_props = self._parse_compound_properties({"PropertyTable": {"Properties": [prop_data]}})
                            results[cid] = parsed_props
                
                # Add empty records for missing CIDs
                for cid in batch:
                    if cid not in results:
                        results[cid] = {}
                        
            except Exception as e:
                logger.warning("Failed to fetch PubChem properties batch %s: %s", batch, e)
                # Add empty records for failed batch
                for cid in batch:
                    results[cid] = {}
        
        return results

    def fetch_compound_xrefs(self, cid: str) -> dict[str, Any]:
        """Fetch compound cross-references by PubChem CID."""
        try:
            payload = self._get_with_cache(f"compound/cid/{cid}/xrefs/RegistryID,RN/JSON")
            return self._parse_compound_xrefs(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem xrefs for CID %s: %s", cid, e)
            return {}

    def fetch_compound_synonyms(self, cid: str) -> dict[str, Any]:
        """Fetch compound synonyms by PubChem CID."""
        try:
            payload = self._get_with_cache(f"compound/cid/{cid}/synonyms/JSON")
            return self._parse_compound_synonyms(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem synonyms for CID %s: %s", cid, e)
            return {}

    def fetch_compound_by_name(self, name: str) -> dict[str, Any]:
        """Fetch compound CIDs by name."""
        try:
            encoded = quote(str(name), safe="")
            payload = self._get_with_cache(f"compound/name/{encoded}/cids/JSON")
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem CIDs for name %s: %s", name, e)
            return {}

    def fetch_cids_by_inchikey(self, inchikey: str) -> dict[str, Any]:
        """Fetch PubChem CIDs by InChIKey."""
        try:
            key = str(inchikey).strip()
            payload = self._get_with_cache(f"compound/inchikey/{key}/cids/JSON")
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem CIDs for InChIKey %s: %s", inchikey, e)
            return {}

    def fetch_cids_by_smiles(self, smiles: str) -> dict[str, Any]:
        """Fetch PubChem CIDs by SMILES (URL-encoded)."""
        try:
            encoded = quote(str(smiles), safe="")
            payload = self._get_with_cache(f"compound/smiles/{encoded}/cids/JSON")
            return self._parse_compound_cids(payload)
        except Exception as e:
            logger.warning("Failed to fetch PubChem CIDs for SMILES: %s", e)
            return {}

    def fetch_record_smiles(self, cid: str) -> dict[str, Any]:
        """Fetch SMILES from the generic record endpoint as a fallback.

        Parses PC_Compounds.props looking for SMILES/Isomeric/Canonical entries.
        """
        try:
            payload = self._get_with_cache(f"compound/cid/{cid}/JSON")
        except Exception as e:
            logger.warning("Failed to fetch PubChem record for CID %s: %s", cid, e)
            return {}

        try:
            compounds = payload.get("PC_Compounds", [])
            if not compounds:
                return {}
            props = compounds[0].get("props", [])
            canon: str | None = None
            iso: str | None = None
            for p in props:
                urn = p.get("urn", {}) if isinstance(p, dict) else {}
                label = str(urn.get("label") or "").lower()
                name = str(urn.get("name") or "").lower()
                if label != "smiles":
                    continue
                val = p.get("value", {})
                # PubChem PC schema stores string values under 'sval'
                s_val = val.get("sval") if isinstance(val, dict) else None
                if not s_val:
                    continue
                if name in {"isomeric", "isomeric smiles", "isomeric_smiles"}:
                    iso = str(s_val)
                elif name in {"canonical", "canonical smiles", "canonical_smiles", ""}:
                    canon = str(s_val)
            result: dict[str, Any] = {}
            if canon:
                result["pubchem_canonical_smiles"] = canon
            if iso:
                result["pubchem_isomeric_smiles"] = iso
            return result
        except Exception as e:  # pragma: no cover
            logger.debug("Failed to parse record SMILES for CID %s: %s", cid, e)
            return {}

    def _parse_compound_properties(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound properties from PubChem response."""
        properties = payload.get("PropertyTable", {}).get("Properties", [])
        if not properties:
            return {}
        
        props = properties[0]
        return {
            "pubchem_molecular_formula": props.get("MolecularFormula"),
            "pubchem_molecular_weight": props.get("MolecularWeight"),
            "pubchem_canonical_smiles": props.get("CanonicalSMILES"),
            "pubchem_isomeric_smiles": props.get("IsomericSMILES"),
            "pubchem_inchi": props.get("InChI"),
            "pubchem_inchi_key": props.get("InChIKey")
        }

    def _parse_compound_xrefs(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound cross-references from PubChem response."""
        xrefs = payload.get("InformationList", {}).get("Information", [])
        if not xrefs:
            return {}
        
        xref_data = xrefs[0]
        return {
            "pubchem_registry_id": xref_data.get("RegistryID"),
            "pubchem_rn": xref_data.get("RN")
        }

    def _parse_compound_synonyms(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound synonyms from PubChem response."""
        synonyms = payload.get("InformationList", {}).get("Information", [])
        if not synonyms:
            return {}
        
        synonym_list = synonyms[0].get("Synonym", [])
        return {
            "pubchem_synonyms": synonym_list
        }

    def _parse_compound_cids(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound CIDs from PubChem response."""
        cids = payload.get("IdentifierList", {}).get("CID", [])
        return {
            "pubchem_cids": cids
        }
