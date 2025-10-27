"""Shared HTTP utilities for ChEMBL API access."""

from __future__ import annotations

import json
import logging
import socket
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import monotonic, sleep
from typing import Any, cast

import requests
from requests import Session

from library.clients.base import BaseApiClient
from library.common.cache import CacheConfig, CacheStrategy, UnifiedCache
from library.common.rate_limiter import RateLimiter
from library.config import APIClientConfig, RetrySettings

try:  # pragma: no cover - urllib3 is always available with requests
    from urllib3.exceptions import ReadTimeoutError as _Urllib3ReadTimeoutError
except Exception:  # pragma: no cover - defensive fallback
    _Urllib3ReadTimeoutError = None

try:  # pragma: no cover - urllib3 is always available with requests
    from urllib3.exceptions import NameResolutionError as _Urllib3NameResolutionError
except Exception:  # pragma: no cover - defensive fallback
    _Urllib3NameResolutionError = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


class TestitemChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL molecule endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL version and release information."""
        try:
            payload = self._request("GET", "status.json").json()
            return {
                "version": payload.get("chembl_db_version", "unknown"),
                "release_date": payload.get("chembl_release_date"),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL version: {e}")
            return {
                "version": "unknown",
                "release_date": None,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }

    def fetch(self, url: str, cfg: Any, timeout: float | None = None) -> dict[str, Any]:
        """Fetch data from ChEMBL API endpoint.
        
        This method provides compatibility with legacy code that expects a fetch() method.
        It extracts the endpoint from the full URL and uses the internal _request() method.
        
        Args:
            url: Full URL or endpoint path
            cfg: Configuration object (unused, kept for compatibility)
            timeout: Request timeout in seconds
            
        Returns:
            Dictionary containing the API response data
            
        Raises:
            Exception: If the request fails
        """
        try:
            # Extract endpoint from full URL if needed
            if url.startswith("http"):
                # Remove base URL to get endpoint
                endpoint = url.replace(self.config.base_url, "").lstrip("/")
            else:
                endpoint = url.lstrip("/")
            
            # Make request using internal method
            response = self._request("GET", endpoint, timeout=timeout)
            
            # Return JSON data if response is a Response object, otherwise return as-is
            if hasattr(response, 'json'):
                return response.json()
            elif isinstance(response, dict):
                return response
            else:
                # Try to parse as JSON if it's a string
                return json.loads(response) if isinstance(response, str) else response
                
        except Exception as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise

    def fetch_molecule(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule data by ChEMBL ID using proper API endpoint."""
        try:
            payload = self._request("GET", f"molecule.json?molecule_chembl_id={molecule_chembl_id}&format=json&limit=1").json()
            parsed = self._parse_molecule(payload)
            if parsed:
                return parsed
            else:
                return self._create_empty_molecule_record(molecule_chembl_id, "No data found")
        except Exception as e:
            logger.warning(f"Failed to fetch molecule {molecule_chembl_id}: {e}")
            return self._create_empty_molecule_record(molecule_chembl_id, str(e))

    def fetch_molecules_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple molecules using proper ChEMBL API."""
        try:
            # ChEMBL API URL length limit requires smaller batches
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_molecules_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"molecule.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            molecules = payload.get("molecules", [])
            
            for molecule in molecules:
                parsed = self._parse_molecule({"molecules": [molecule]})
                if parsed:
                    results[parsed["molecule_chembl_id"]] = parsed
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = self._create_empty_molecule_record(chembl_id, "Not found in API response")
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch molecules batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_molecule(chembl_id)
            return results

    def fetch_molecules_batch_streaming(
        self, 
        molecule_ids: list[str], 
        batch_size: int = 25,
        fields: tuple[str, ...] | None = None
    ) -> Iterator[tuple[list[str], dict[str, dict[str, Any]]]]:
        """Fetch molecule data in streaming batches.
        
        Args:
            molecule_ids: List of ChEMBL molecule IDs to fetch
            batch_size: Number of molecules per batch (max 25 for ChEMBL API)
            fields: Optional tuple of field names to request from API.
                    If None, all available fields are requested.
                    
        Yields:
            Tuple of (batch_ids, batch_results) where:
            - batch_ids: List of molecule IDs in this batch
            - batch_results: Dict mapping molecule_id to molecule data
        """
        if not molecule_ids:
            return
            
        # Limit batch size to ChEMBL API maximum
        effective_batch_size = min(batch_size, 25)
        
        for i in range(0, len(molecule_ids), effective_batch_size):
            batch_ids = molecule_ids[i:i + effective_batch_size]
            batch_results = {}
            
            try:
                # Build query parameters
                params = {
                    "format": "json",
                    "limit": len(batch_ids)
                }
                
                # Add fields parameter if specified
                if fields:
                    params["fields"] = ",".join(fields)
                
                # Build URL with molecule IDs filter
                ids_str = ",".join(batch_ids)
                endpoint = f"molecule.json?molecule_chembl_id__in={ids_str}"
                
                # Build query string for additional params
                from urllib.parse import urlencode
                if params:
                    query_string = urlencode(params)
                    endpoint += f"&{query_string}"
                
                # Make request
                response = self._request("GET", endpoint)
                payload = response.json()
                
                # Parse molecules from response
                molecules = payload.get("molecules", [])
                
                # Convert to dict keyed by molecule_chembl_id
                for molecule in molecules:
                    chembl_id = molecule.get("molecule_chembl_id")
                    if chembl_id:
                        batch_results[chembl_id] = molecule
                
                # Add empty records for molecules not found
                for chembl_id in batch_ids:
                    if chembl_id not in batch_results:
                        batch_results[chembl_id] = {}
                
                logger.debug(f"Fetched {len(batch_results)} molecules from batch of {len(batch_ids)}")
                
            except Exception as e:
                logger.error(f"Failed to fetch batch of molecules: {e}")
                # Add empty records for failed batch
                for chembl_id in batch_ids:
                    batch_results[chembl_id] = {}
            
            yield batch_ids, batch_results

    def _parse_molecule(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse molecule data from ChEMBL API response."""
        molecule = payload.get("molecules", [{}])[0] if "molecules" in payload else payload
        
        result = {
            "molecule_chembl_id": molecule.get("molecule_chembl_id"),
            "molregno": molecule.get("molregno"),
            "pref_name": molecule.get("pref_name"),
            "max_phase": molecule.get("max_phase"),
            "therapeutic_flag": molecule.get("therapeutic_flag"),
            "dosed_ingredient": molecule.get("dosed_ingredient"),
            "structure_type": molecule.get("structure_type"),
            "molecule_type": molecule.get("molecule_type"),
            "first_approval": molecule.get("first_approval"),
            "oral": molecule.get("oral"),
            "parenteral": molecule.get("parenteral"),
            "topical": molecule.get("topical"),
            "black_box_warning": molecule.get("black_box_warning"),
            "natural_product": molecule.get("natural_product"),
            "first_in_class": molecule.get("first_in_class"),
            "chirality": molecule.get("chirality"),
            "prodrug": molecule.get("prodrug"),
            "inorganic_flag": molecule.get("inorganic_flag"),
            "usan_year": molecule.get("usan_year"),
            "availability_type": molecule.get("availability_type"),
            "usan_stem": molecule.get("usan_stem"),
            "polymer_flag": molecule.get("polymer_flag"),
            "usan_substem": molecule.get("usan_substem"),
            "usan_stem_definition": molecule.get("usan_stem_definition"),
            "indication_class": molecule.get("indication_class"),
            "withdrawn_flag": molecule.get("withdrawn_flag"),
            "withdrawn_year": molecule.get("withdrawn_year"),
            "withdrawn_country": molecule.get("withdrawn_country"),
            "withdrawn_reason": molecule.get("withdrawn_reason"),
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat()
        }
        
        # Extract molecule properties if available
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]
            result.update({
                "mw_freebase": props.get("mw_freebase"),
                "alogp": props.get("alogp"),
                "hba": props.get("hba"),
                "hbd": props.get("hbd"),
                "psa": props.get("psa"),
                "rtb": props.get("rtb"),
                "ro3_pass": props.get("ro3_pass"),
                "num_ro5_violations": props.get("num_ro5_violations"),
                "acd_most_apka": props.get("acd_most_apka"),
                "acd_most_bpka": props.get("acd_most_bpka"),
                "acd_logp": props.get("acd_logp"),
                "acd_logd": props.get("acd_logd"),
                "molecular_species": props.get("molecular_species"),
                "full_mwt": props.get("full_mwt"),
                "aromatic_rings": props.get("aromatic_rings"),
                "heavy_atoms": props.get("heavy_atoms"),
                "qed_weighted": props.get("qed_weighted"),
                "mw_monoisotopic": props.get("mw_monoisotopic"),
                "full_molformula": props.get("full_molformula"),
                "hba_lipinski": props.get("hba_lipinski"),
                "hbd_lipinski": props.get("hbd_lipinski"),
                "num_lipinski_ro5_violations": props.get("num_lipinski_ro5_violations")
            })
        
        # Extract molecule structures if available
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            struct = molecule["molecule_structures"]
            result.update({
                "inchi": struct.get("inchi"),
                "inchikey": struct.get("inchikey"),
                "canonical_smiles": struct.get("canonical_smiles"),
                "standard_inchi": struct.get("standard_inchi"),
                "standard_inchikey": struct.get("standard_inchikey"),
                "standard_smiles": struct.get("standard_smiles"),
                "molecular_formula": struct.get("molecular_formula")
            })
        
        # Extract molecule synonyms if available
        if "molecule_synonyms" in molecule and molecule["molecule_synonyms"]:
            synonyms = molecule["molecule_synonyms"]
            synonym_list = [syn.get("molecule_synonym") for syn in synonyms if syn.get("molecule_synonym")]
            result["synonyms"] = synonym_list
        
        # Extract cross references if available
        if "cross_references" in molecule and molecule["cross_references"]:
            xrefs = molecule["cross_references"]
            xref_list = []
            for xref in xrefs:
                xref_data = {
                    "xref_name": xref.get("xref_name"),
                    "xref_id": xref.get("xref_id"),
                    "xref_src": xref.get("xref_src")
                }
                xref_list.append(xref_data)
            result["xref_sources"] = xref_list
        
        # Extract nested structures as JSON strings
        if "atc_classifications" in molecule and molecule["atc_classifications"]:
            result["atc_classifications"] = json.dumps(molecule["atc_classifications"])

        if "biotherapeutic" in molecule and molecule["biotherapeutic"]:
            result["biotherapeutic"] = json.dumps(molecule["biotherapeutic"])

        if "cross_references" in molecule and molecule["cross_references"]:
            result["cross_references"] = json.dumps(molecule["cross_references"])

        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            result["molecule_hierarchy"] = json.dumps(molecule["molecule_hierarchy"])

        # Extract flags and additional fields
        result["chemical_probe"] = molecule.get("chemical_probe")
        result["orphan"] = molecule.get("orphan")
        result["veterinary"] = molecule.get("veterinary")
        result["helm_notation"] = molecule.get("helm_notation")
        result["chirality_chembl"] = molecule.get("chirality")
        result["molecule_type_chembl"] = molecule.get("molecule_type")
        
        return result

    def _create_empty_molecule_record(self, molecule_chembl_id: str, error_msg: str) -> dict[str, Any]:
        """Create empty molecule record with error information."""
        return {
            "molecule_chembl_id": molecule_chembl_id,
            "molregno": None,
            "pref_name": None,
            "max_phase": None,
            "therapeutic_flag": None,
            "dosed_ingredient": None,
            "structure_type": None,
            "molecule_type": None,
            "first_approval": None,
            "oral": None,
            "parenteral": None,
            "topical": None,
            "black_box_warning": None,
            "natural_product": None,
            "first_in_class": None,
            "chirality": None,
            "prodrug": None,
            "inorganic_flag": None,
            "usan_year": None,
            "availability_type": None,
            "usan_stem": None,
            "polymer_flag": None,
            "usan_substem": None,
            "usan_stem_definition": None,
            "indication_class": None,
            "withdrawn_flag": None,
            "withdrawn_year": None,
            "withdrawn_country": None,
            "withdrawn_reason": None,
            "mw_freebase": None,
            "alogp": None,
            "hba": None,
            "hbd": None,
            "psa": None,
            "rtb": None,
            "ro3_pass": None,
            "num_ro5_violations": None,
            "acd_most_apka": None,
            "acd_most_bpka": None,
            "acd_logp": None,
            "acd_logd": None,
            "molecular_species": None,
            "full_mwt": None,
            "aromatic_rings": None,
            "heavy_atoms": None,
            "qed_weighted": None,
            "mw_monoisotopic": None,
            "full_molformula": None,
            "hba_lipinski": None,
            "hbd_lipinski": None,
            "num_lipinski_ro5_violations": None,
            "inchi": None,
            "inchikey": None,
            "canonical_smiles": None,
            "standard_inchi": None,
            "standard_inchikey": None,
            "standard_smiles": None,
            "molecular_formula": None,
            "synonyms": None,
            "xref_sources": None,
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "error": error_msg
        }

    def fetch_molecule_form(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule form data."""
        try:
            payload = self._request("GET", f"molecule_form?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_molecule_form(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch molecule form for {molecule_chembl_id}: {e}")
            return {}

    def fetch_mechanism(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch mechanism data."""
        try:
            payload = self._request("GET", f"mechanism?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_mechanism(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch mechanism for {molecule_chembl_id}: {e}")
            return {}

    def fetch_mechanism_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch mechanism data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_mechanism_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"mechanism.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            mechanisms = payload.get("mechanisms", [])
            
            for mechanism in mechanisms:
                chembl_id = mechanism.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_mechanism({"mechanisms": [mechanism]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch mechanism batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_mechanism(chembl_id)
            return results

    def fetch_atc_classification(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch ATC classification data."""
        try:
            payload = self._request("GET", f"atc_classification?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_atc_classification(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch ATC classification for {molecule_chembl_id}: {e}")
            return {}

    def fetch_atc_classification_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch ATC classification data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_atc_classification_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"atc_classification.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            classifications = payload.get("atc_classifications", [])
            
            for classification in classifications:
                chembl_id = classification.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_atc_classification({"atc_classifications": [classification]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch ATC classification batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_atc_classification(chembl_id)
            return results

    def fetch_drug(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug data."""
        try:
            payload = self._request("GET", f"drug?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_drug(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch drug for {molecule_chembl_id}: {e}")
            return {}

    def fetch_drug_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_drug_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"drug.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            drugs = payload.get("drugs", [])
            
            for drug in drugs:
                chembl_id = drug.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_drug({"drugs": [drug]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch drug batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_drug(chembl_id)
            return results

    def fetch_drug_warning(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug warnings."""
        try:
            payload = self._request("GET", f"drug_warning?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_drug_warning(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch drug warning for {molecule_chembl_id}: {e}")
            return {}

    def fetch_drug_warning_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug warnings for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_drug_warning_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"drug_warning.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            warnings = payload.get("drug_warnings", [])
            
            for warning in warnings:
                chembl_id = warning.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_drug_warning({"drug_warnings": [warning]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch drug warning batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_drug_warning(chembl_id)
            return results

    def fetch_xref_source(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch cross-reference sources."""
        try:
            payload = self._request("GET", f"xref_source?molecule_chembl_id={molecule_chembl_id}&format=json").json()
            return self._parse_xref_source(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch xref source for {molecule_chembl_id}: {e}")
            return {}

    def _parse_molecule_form(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse molecule form data."""
        forms = payload.get("molecule_forms", [])
        if not forms:
            return {}
        
        # Take the first form as primary
        form = forms[0]
        return {
            "molecule_form_chembl_id": form.get("molecule_form_chembl_id"),
            "parent_chembl_id": form.get("parent_chembl_id"),
            "parent_molregno": form.get("parent_molregno")
        }

    def _parse_mechanism(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse mechanism data."""
        mechanisms = payload.get("mechanisms", [])
        if not mechanisms:
            return {}
        
        # Take the first mechanism
        mechanism = mechanisms[0]
        return {
            "mechanism_of_action": mechanism.get("mechanism_of_action"),
            "direct_interaction": mechanism.get("direct_interaction"),
            "molecular_mechanism": mechanism.get("molecular_mechanism"),
            "mechanism_comment": mechanism.get("mechanism_comment"),
            "target_chembl_id": mechanism.get("target_chembl_id")
        }

    def _parse_atc_classification(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse ATC classification data."""
        classifications = payload.get("atc_classifications", [])
        if not classifications:
            return {}
        
        # Take the first classification
        atc = classifications[0]
        return {
            "level1": atc.get("level1"),
            "level1_description": atc.get("level1_description"),
            "level2": atc.get("level2"),
            "level2_description": atc.get("level2_description"),
            "level3": atc.get("level3"),
            "level3_description": atc.get("level3_description"),
            "level4": atc.get("level4"),
            "level4_description": atc.get("level4_description"),
            "level5": atc.get("level5"),
            "level5_description": atc.get("level5_description")
        }

    def _parse_drug(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse drug data."""
        drugs = payload.get("drugs", [])
        if not drugs:
            return {}
        
        # Take the first drug
        drug = drugs[0]
        return {
            "drug_type": drug.get("drug_type"),
            "max_phase": drug.get("max_phase"),
            "indication_class": drug.get("indication_class"),
            "withdrawn_flag": drug.get("withdrawn_flag"),
            "withdrawn_year": drug.get("withdrawn_year"),
            "withdrawn_country": drug.get("withdrawn_country"),
            "withdrawn_reason": drug.get("withdrawn_reason")
        }

    def _parse_drug_warning(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse drug warnings."""
        warnings = payload.get("drug_warnings", [])
        if not warnings:
            return {}
        
        # Take the first warning
        warning = warnings[0]
        return {
            "warning_type": warning.get("warning_type"),
            "warning_class": warning.get("warning_class"),
            "warning_description": warning.get("warning_description"),
            "warning_country": warning.get("warning_country"),
            "warning_year": warning.get("warning_year")
        }

    def _parse_xref_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse cross-reference sources."""
        sources = payload.get("xref_sources", [])
        if not sources:
            return {}
        
        # Take the first source
        source = sources[0]
        return {
            "xref_name": source.get("xref_name"),
            "xref_description": source.get("xref_description"),
            "xref_url": source.get("xref_url")
        }

    def resolve_molregno_to_chembl_id(self, molregno: int) -> str | None:
        """Resolve molregno to molecule_chembl_id via ChEMBL API."""
        try:
            payload = self._request("GET", f"molecule?molregno={molregno}&format=json").json()
            molecules = payload.get("molecules", [])
            if molecules:
                return molecules[0].get("molecule_chembl_id")
            return None
        except Exception as e:
            logger.warning(f"Failed to resolve molregno {molregno} to molecule_chembl_id: {e}")
            return None

def _strip_json_suffix(url: str) -> str | None:
    """Return URL without .json suffix if present."""
    if url.endswith(".json"):
        return url[:-5]
    return None


def _is_name_resolution_error(exc: Exception) -> bool:
    """Return True if exc is a name resolution error."""
    if _Urllib3NameResolutionError is not None and isinstance(exc, _Urllib3NameResolutionError):
        return True
    if isinstance(exc, (socket.gaierror, socket.herror)):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "Name or service not known" in str(exc)
    return False


def _should_switch_to_fallback(exc: Exception) -> bool:
    """Return True if we should try fallback URL."""
    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        if response is not None:
            return response.status_code in {404, 406}
    return False


def _backoff_delay(
    attempt: int,
    cfg: RetrySettings,
    header_delay: float | None = None,
    jitter: Callable[[float], float] | None = None,
) -> float:
    """Calculate backoff delay for attempt."""
    base_delay = cfg.backoff_multiplier ** (attempt - 1)
    if header_delay is not None:
        base_delay = max(base_delay, header_delay)
    if jitter is not None:
        base_delay = jitter(base_delay)
    return min(base_delay, cfg.max_delay)


def _find_chembl_id_by_molregno(molregno: int) -> str | None:
    """Find ChEMBL ID by molregno.
    
    Returns the first matching ChEMBL ID or None if not found.
    """
    try:
        str(int(molregno))
    except Exception:
        return None
    
    # Note: This function needs a client instance to make requests
    # It should be a method of a client class, not a standalone function
    return None

    def fetch_molecules_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple molecules using proper ChEMBL API."""
        try:
            # ChEMBL API URL length limit requires smaller batches
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_molecules_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"molecule.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            molecules = payload.get("molecules", [])
            
            for molecule in molecules:
                parsed = self._parse_molecule({"molecules": [molecule]})
                if parsed:
                    results[parsed["molecule_chembl_id"]] = parsed
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = self._create_empty_molecule_record(chembl_id, "Not found in API response")
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch molecules batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_molecule(chembl_id)
            return results

def _log_retry_warning(
    event: str,
    *,
    url: str,
    attempt: int,
    delay: float,
    elapsed: float,
    status: int | None = None,
    exc_info: bool = False,
) -> None:
    """Log retry warning with structured data."""
    extra = {
        "url": url,
        "attempt": attempt,
        "delay": delay,
        "elapsed": elapsed,
        "status": status,
    }
    if exc_info:
        logger.warning(event, extra=extra, exc_info=True)
    else:
        logger.warning(event, extra=extra)


def _log_retry_delay(
    url: str,
    attempt: int,
    status: int | None,
    delay: float,
    header_delay: float | None = None,
) -> None:
    """Log retry delay information."""
    extra = {
        "url": url,
        "attempt": attempt,
        "status": status,
        "delay": delay,
        "header_delay": header_delay,
    }
    logger.debug("retry_delay", extra=extra)


@dataclass
class ChemblClient:
    """HTTP client for the ChEMBL API with a TTL cache.

    Parameters
    ----------
    api:
        Global API settings providing the ``User-Agent`` header.
    retry:
        Retry configuration applied to all requests.
    chembl:
        Optional ChEMBL-specific configuration controlling cache TTL and size.
    session:
        Optional pre-configured :class:`requests.Session` instance; primarily
        intended for tests.
    global_limiter:
        Optional system-wide :class:`RateLimiter` enforcing ``Config.rate``
        across all HTTP clients.
    jitter:
        Optional callable producing jitter values for retry backoff. When not
        provided the jitter is derived from ``retry`` using
        :meth:`library.config.RetryCfg.build_jitter`.
    """

    cache: UnifiedCache[dict[str, Any]] = field(init=False)
    _cache_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _session_local: threading.local = field(init=False)
    _sessions: set[Session] = field(init=False)
    _sessions_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _session_factory: Callable[[], Session] = field(init=False)
    _global_limiter: RateLimiter | None = field(default=None, init=False)

    def __init__(
        self,
        api: APIClientConfig | None = None,
        retry: RetrySettings | None = None,
        chembl: APIClientConfig | None = None,
        *,
        session: Session | None = None,
        global_limiter: RateLimiter | None = None,
        jitter: Callable[[float], float] | None = None,
    ) -> None:
        # api = api or APIClientConfig()  # Cannot create without required fields
        retry = retry or RetrySettings()
        self._jitter = jitter if jitter is not None else None
        if session is not None:

            def _session_from_argument(provided: Session = session) -> Session:
                return provided

            self._session_factory = _session_from_argument
        else:
            api_cfg_default: APIClientConfig = api
            retry_cfg_default: RetrySettings = retry

            def _build_session(
                api_cfg: APIClientConfig = api_cfg_default,
                retry_cfg: RetrySettings = retry_cfg_default,
            ) -> Session:
                session = Session()
                session.headers.update({"User-Agent": api_cfg.user_agent})
                return session

            self._session_factory = _build_session
        ttl = chembl.cache_ttl if chembl is not None else 3600
        maxsize = chembl.cache_size if chembl is not None else 1000

        # Create unified cache with memory strategy
        cache_config = CacheConfig(strategy=CacheStrategy.MEMORY, ttl=ttl, max_size=maxsize, key_prefix="chembl")
        self.cache = UnifiedCache(cache_config)
        self._session_local = threading.local()
        self._sessions = set()
        self._global_limiter = global_limiter

    def _get_session(self) -> Session:
        """Return a thread-local session."""
        if not hasattr(self._session_local, "session"):
            session = self._session_factory()
            self._session_local.session = session
            with self._sessions_lock:
                self._sessions.add(session)
        return self._session_local.session

    def fetch(
        self,
        url: str,
        cfg: APIClientConfig,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Fetch JSON data from ``url`` with retry logic and caching.

        Parameters
        ----------
        url:
            Target URL to fetch.
        cfg:
            API configuration providing timeouts and retry settings.
        timeout:
            Optional read timeout override.

        Returns
        -------
        dict[str, Any]
            Parsed JSON response.

        Raises
        ------
        requests.RequestException
            If the HTTP request fails.
        ValueError
            If the response body is not valid JSON or cannot be decoded.
        """

        read_timeout = timeout if timeout is not None else cfg.timeout_read
        cache_key = url
        with self._cache_lock:
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug(
                    "cache_hit",
                    extra={
                        "url": url,
                        "rps": cfg.rps,
                        "status": "hit",
                        "timeout": read_timeout,
                    },
                )
                return cast(dict[str, Any], cached)
            logger.debug(
                "cache_miss",
                extra={
                    "url": url,
                    "rps": cfg.rps,
                    "status": "miss",
                    "timeout": read_timeout,
                },
            )

    def fetch_molecule_form(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule form data."""
        try:
            payload = self._request("GET", f"molecule_form?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_molecule_form(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch molecule form for {molecule_chembl_id}: {e}")
            return {}

    def fetch_molecule_properties(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule properties."""
        try:
            payload = self._request("GET", f"molecule_properties?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_molecule_properties(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch molecule properties for {molecule_chembl_id}: {e}")
            return {}

    def fetch_molecule_properties_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch molecule properties for multiple molecules in batch."""
        try:
            # ChEMBL supports batch requests for properties
            if len(molecule_chembl_ids) > 50:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds ChEMBL limit of 50, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 50):
                    chunk = molecule_chembl_ids[i:i+50]
                    chunk_results = self.fetch_molecule_properties_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Prepare batch request payload
            batch_payload = {"molecule_chembl_ids": molecule_chembl_ids}
            
            # Make batch request
            payload = self._request("POST", "molecule_properties/batch", json=batch_payload)
            
            # Parse results
            results = {}
            properties = payload.get("molecule_properties", [])
            
            for prop in properties:
                chembl_id = prop.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_molecule_properties({"molecule_properties": [prop]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch molecule properties batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_molecule_properties(chembl_id)
            return results

    def fetch_mechanism(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch mechanism data."""
        try:
            payload = self._request("GET", f"mechanism?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_mechanism(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch mechanism for {molecule_chembl_id}: {e}")
            return {}

    def fetch_mechanism_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch mechanism data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_mechanism_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"mechanism.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            mechanisms = payload.get("mechanisms", [])
            
            for mechanism in mechanisms:
                chembl_id = mechanism.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_mechanism({"mechanisms": [mechanism]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch mechanism batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_mechanism(chembl_id)
            return results

    def fetch_atc_classification(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch ATC classification data."""
        try:
            payload = self._request("GET", f"atc_classification?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_atc_classification(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch ATC classification for {molecule_chembl_id}: {e}")
            return {}

    def fetch_atc_classification_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch ATC classification data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_atc_classification_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"atc_classification.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            classifications = payload.get("atc_classifications", [])
            
            for classification in classifications:
                chembl_id = classification.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_atc_classification({"atc_classifications": [classification]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch ATC classification batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_atc_classification(chembl_id)
            return results

    def fetch_compound_synonym(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch compound synonyms."""
        try:
            payload = self._request("GET", f"compound_synonym?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_compound_synonym(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch compound synonym for {molecule_chembl_id}: {e}")
            return {}

    def fetch_drug(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug data."""
        try:
            payload = self._request("GET", f"drug?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_drug(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch drug data for {molecule_chembl_id}: {e}")
            return {}

    def fetch_drug_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_drug_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"drug.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            drugs = payload.get("drugs", [])
            
            for drug in drugs:
                chembl_id = drug.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_drug({"drugs": [drug]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch drug batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_drug(chembl_id)
            return results

    def fetch_drug_warning(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug warnings."""
        try:
            payload = self._request("GET", f"drug_warning?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_drug_warning(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch drug warning for {molecule_chembl_id}: {e}")
            return {}

    def fetch_drug_warning_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug warnings for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds URL limit of 25, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 25):
                    chunk = molecule_chembl_ids[i:i+25]
                    chunk_results = self.fetch_drug_warning_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(molecule_chembl_ids)
            url = f"drug_warning.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url).json()
            
            # Parse results
            results = {}
            warnings = payload.get("drug_warnings", [])
            
            for warning in warnings:
                chembl_id = warning.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_drug_warning({"drug_warnings": [warning]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = {}
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch drug warning batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_drug_warning(chembl_id)
            return results

    def fetch_xref_source(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch cross-reference sources."""
        try:
            payload = self._request("GET", f"xref_source?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_xref_source(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch xref source for {molecule_chembl_id}: {e}")
            return {}

    def _parse_molecule(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse molecule data from ChEMBL API response."""
        molecule = payload.get("molecules", [{}])[0] if "molecules" in payload else payload
        
        result = {
            "molecule_chembl_id": molecule.get("molecule_chembl_id"),
            "molregno": molecule.get("molregno"),
            "pref_name": molecule.get("pref_name"),
            "max_phase": molecule.get("max_phase"),
            "therapeutic_flag": molecule.get("therapeutic_flag"),
            "dosed_ingredient": molecule.get("dosed_ingredient"),
            "structure_type": molecule.get("structure_type"),
            "molecule_type": molecule.get("molecule_type"),
            "first_approval": molecule.get("first_approval"),
            "oral": molecule.get("oral"),
            "parenteral": molecule.get("parenteral"),
            "topical": molecule.get("topical"),
            "black_box_warning": molecule.get("black_box_warning"),
            "natural_product": molecule.get("natural_product"),
            "first_in_class": molecule.get("first_in_class"),
            "chirality": molecule.get("chirality"),
            "prodrug": molecule.get("prodrug"),
            "inorganic_flag": molecule.get("inorganic_flag"),
            "usan_year": molecule.get("usan_year"),
            "availability_type": molecule.get("availability_type"),
            "usan_stem": molecule.get("usan_stem"),
            "polymer_flag": molecule.get("polymer_flag"),
            "usan_substem": molecule.get("usan_substem"),
            "usan_stem_definition": molecule.get("usan_stem_definition"),
            "indication_class": molecule.get("indication_class"),
            "withdrawn_flag": molecule.get("withdrawn_flag"),
            "withdrawn_year": molecule.get("withdrawn_year"),
            "withdrawn_country": molecule.get("withdrawn_country"),
            "withdrawn_reason": molecule.get("withdrawn_reason"),
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat()
        }
        
        # Extract molecule properties if available
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]
            result.update({
                "mw_freebase": props.get("mw_freebase"),
                "alogp": props.get("alogp"),
                "hba": props.get("hba"),
                "hbd": props.get("hbd"),
                "psa": props.get("psa"),
                "rtb": props.get("rtb"),
                "ro3_pass": props.get("ro3_pass"),
                "num_ro5_violations": props.get("num_ro5_violations"),
                "acd_most_apka": props.get("acd_most_apka"),
                "acd_most_bpka": props.get("acd_most_bpka"),
                "acd_logp": props.get("acd_logp"),
                "acd_logd": props.get("acd_logd"),
                "molecular_species": props.get("molecular_species"),
                "full_mwt": props.get("full_mwt"),
                "aromatic_rings": props.get("aromatic_rings"),
                "heavy_atoms": props.get("heavy_atoms"),
                "qed_weighted": props.get("qed_weighted"),
                "mw_monoisotopic": props.get("mw_monoisotopic"),
                "full_molformula": props.get("full_molformula"),
                "hba_lipinski": props.get("hba_lipinski"),
                "hbd_lipinski": props.get("hbd_lipinski"),
                "num_lipinski_ro5_violations": props.get("num_lipinski_ro5_violations")
            })
        
        # Extract molecule structures if available
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            struct = molecule["molecule_structures"]
            result.update({
                "inchi": struct.get("inchi"),
                "inchikey": struct.get("inchikey"),
                "canonical_smiles": struct.get("canonical_smiles"),
                "standard_inchi": struct.get("standard_inchi"),
                "standard_inchikey": struct.get("standard_inchikey"),
                "standard_smiles": struct.get("standard_smiles"),
                "molecular_formula": struct.get("molecular_formula")
            })
        
        # Extract molecule synonyms if available
        if "molecule_synonyms" in molecule and molecule["molecule_synonyms"]:
            synonyms = molecule["molecule_synonyms"]
            synonym_list = [syn.get("molecule_synonym") for syn in synonyms if syn.get("molecule_synonym")]
            result["synonyms"] = synonym_list
        
        # Extract cross references if available
        if "cross_references" in molecule and molecule["cross_references"]:
            xrefs = molecule["cross_references"]
            xref_list = []
            for xref in xrefs:
                xref_data = {
                    "xref_name": xref.get("xref_name"),
                    "xref_id": xref.get("xref_id"),
                    "xref_src": xref.get("xref_src")
                }
                xref_list.append(xref_data)
            result["xref_sources"] = xref_list
        
        return result

    def _parse_molecule_form(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse molecule form data."""
        forms = payload.get("molecule_forms", [])
        if not forms:
            return {}
        
        # Take the first form as primary
        form = forms[0]
        return {
            "molecule_form_chembl_id": form.get("molecule_form_chembl_id"),
            "parent_chembl_id": form.get("parent_chembl_id"),
            "parent_molregno": form.get("parent_molregno")
        }

    def _parse_molecule_properties(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse molecule properties data."""
        properties = payload.get("molecule_properties", [])
        if not properties:
            return {}
        
        # Take the first set of properties
        props = properties[0]
        return {
            "mw_freebase": props.get("mw_freebase"),
            "alogp": props.get("alogp"),
            "hba": props.get("hba"),
            "hbd": props.get("hbd"),
            "psa": props.get("psa"),
            "rtb": props.get("rtb"),
            "ro3_pass": props.get("ro3_pass"),
            "num_ro5_violations": props.get("num_ro5_violations"),
            "acd_most_apka": props.get("acd_most_apka"),
            "acd_most_bpka": props.get("acd_most_bpka"),
            "acd_logp": props.get("acd_logp"),
            "acd_logd": props.get("acd_logd"),
            "molecular_species": props.get("molecular_species"),
            "full_mwt": props.get("full_mwt"),
            "aromatic_rings": props.get("aromatic_rings"),
            "heavy_atoms": props.get("heavy_atoms"),
            "qed_weighted": props.get("qed_weighted"),
            "mw_monoisotopic": props.get("mw_monoisotopic"),
            "full_molformula": props.get("full_molformula"),
            "hba_lipinski": props.get("hba_lipinski"),
            "hbd_lipinski": props.get("hbd_lipinski"),
            "num_lipinski_ro5_violations": props.get("num_lipinski_ro5_violations")
        }

    def _parse_mechanism(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse mechanism data."""
        mechanisms = payload.get("mechanisms", [])
        if not mechanisms:
            return {}
        
        # Take the first mechanism
        mechanism = mechanisms[0]
        return {
            "mechanism_of_action": mechanism.get("mechanism_of_action"),
            "direct_interaction": mechanism.get("direct_interaction"),
            "molecular_mechanism": mechanism.get("molecular_mechanism"),
            "mechanism_comment": mechanism.get("mechanism_comment"),
            "target_chembl_id": mechanism.get("target_chembl_id")
        }

    def _parse_atc_classification(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse ATC classification data."""
        classifications = payload.get("atc_classifications", [])
        if not classifications:
            return {}
        
        # Take the first classification
        atc = classifications[0]
        return {
            "level1": atc.get("level1"),
            "level1_description": atc.get("level1_description"),
            "level2": atc.get("level2"),
            "level2_description": atc.get("level2_description"),
            "level3": atc.get("level3"),
            "level3_description": atc.get("level3_description"),
            "level4": atc.get("level4"),
            "level4_description": atc.get("level4_description"),
            "level5": atc.get("level5"),
            "level5_description": atc.get("level5_description")
        }

    def _parse_compound_synonym(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse compound synonyms."""
        synonyms = payload.get("compound_synonyms", [])
        if not synonyms:
            return {}
        
        # Collect all synonyms
        synonym_list = [syn.get("synonyms") for syn in synonyms if syn.get("synonyms")]
        return {
            "synonyms": synonym_list
        }

    def _parse_drug(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse drug data."""
        drugs = payload.get("drugs", [])
        if not drugs:
            return {}
        
        # Take the first drug
        drug = drugs[0]
        return {
            "drug_chembl_id": drug.get("drug_chembl_id"),
            "drug_name": drug.get("drug_name"),
            "drug_type": drug.get("drug_type"),
            "drug_substance_flag": drug.get("drug_substance_flag"),
            "drug_indication_flag": drug.get("drug_indication_flag"),
            "drug_antibacterial_flag": drug.get("drug_antibacterial_flag"),
            "drug_antiviral_flag": drug.get("drug_antiviral_flag"),
            "drug_antifungal_flag": drug.get("drug_antifungal_flag"),
            "drug_antiparasitic_flag": drug.get("drug_antiparasitic_flag"),
            "drug_antineoplastic_flag": drug.get("drug_antineoplastic_flag"),
            "drug_immunosuppressant_flag": drug.get("drug_immunosuppressant_flag"),
            "drug_antiinflammatory_flag": drug.get("drug_antiinflammatory_flag")
        }

    def _parse_drug_warning(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse drug warnings."""
        warnings = payload.get("drug_warnings", [])
        if not warnings:
            return {}
        
        # Collect all warnings
        warning_list = []
        for warning in warnings:
            warning_data = {
                "warning_type": warning.get("warning_type"),
                "warning_class": warning.get("warning_class"),
                "warning_description": warning.get("warning_description"),
                "warning_country": warning.get("warning_country"),
                "warning_year": warning.get("warning_year")
            }
            warning_list.append(warning_data)
        
        return {
            "drug_warnings": warning_list
        }

    def _parse_xref_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse cross-reference sources."""
        xrefs = payload.get("xref_sources", [])
        if not xrefs:
            return {}
        
        # Collect all cross-references
        xref_list = []
        for xref in xrefs:
            xref_data = {
                "xref_name": xref.get("xref_name"),
                "xref_id": xref.get("xref_id"),
                "xref_src": xref.get("xref_src")
            }
            xref_list.append(xref_data)
        
        return {
            "xref_sources": xref_list
        }

    def _create_empty_molecule_record(self, molecule_chembl_id: str, error_msg: str) -> dict[str, Any]:
        """Create empty molecule record with error information."""
        return {
            "molecule_chembl_id": molecule_chembl_id,
            "molregno": None,
            "pref_name": None,
            "max_phase": None,
            "therapeutic_flag": None,
            "dosed_ingredient": None,
            "structure_type": None,
            "molecule_type": None,
            "first_approval": None,
            "oral": None,
            "parenteral": None,
            "topical": None,
            "black_box_warning": None,
            "natural_product": None,
            "first_in_class": None,
            "chirality": None,
            "prodrug": None,
            "inorganic_flag": None,
            "usan_year": None,
            "availability_type": None,
            "usan_stem": None,
            "polymer_flag": None,
            "usan_substem": None,
            "usan_stem_definition": None,
            "indication_class": None,
            "withdrawn_flag": None,
            "withdrawn_year": None,
            "withdrawn_country": None,
            "withdrawn_reason": None,
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "error": error_msg
        }


class ChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL document endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL version and release information."""
        try:
            payload = self._request("GET", "status.json").json()
            return {
                "version": payload.get("chembl_db_version", "unknown"),
                "release_date": payload.get("chembl_release_date"),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL version: {e}")
            return {
                "version": "unknown",
                "release_date": None,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            }

    def fetch_by_doc_id(self, document_chembl_id: str) -> dict[str, Any]:
        """Fetch document data by ChEMBL document ID."""
        logger.info(f"Fetching document {document_chembl_id} from ChEMBL...")
        try:
            payload = self._request("GET", f"document/{document_chembl_id}")
            logger.info(f"Received payload keys: {list(payload.keys())}")
            
            # Fetch source metadata if src_id is available
            source_fields = {}
            if "src_id" in payload and payload["src_id"]:
                source_fields = self.fetch_source_metadata(payload["src_id"])
            
            # Extract relevant document fields
            data = {
                # Legacy fields
                "document_chembl_id": document_chembl_id,
                "document_pubmed_id": payload.get("pubmed_id"),
                "document_classification": payload.get("doc_type"),
                "referenses_on_previous_experiments": payload.get("refs"),
                "original_experimental_document": payload.get("original_experimental_document"),
                "chembl_title": payload.get("title"),
                "chembl_abstract": payload.get("abstract"),
                "chembl_authors": payload.get("authors"),
                "chembl_journal": payload.get("journal"),
                "chembl_year": payload.get("year"),
                "chembl_volume": payload.get("volume"),
                "chembl_issue": payload.get("issue"),
                "chembl_first_page": payload.get("first_page"),
                "chembl_last_page": payload.get("last_page"),
                "chembl_doi": payload.get("doi"),
                "chembl_patent_id": payload.get("patent_id"),
                "chembl_ridx": payload.get("ridx"),
                "chembl_teaser": payload.get("teaser"),
                
                # NEW: CHEMBL.DOCS.* fields according to specification
                "CHEMBL.DOCS.document_chembl_id": payload.get("document_chembl_id", document_chembl_id),
                "CHEMBL.DOCS.doc_type": payload.get("doc_type"),
                "CHEMBL.DOCS.title": payload.get("title"),
                "CHEMBL.DOCS.journal": payload.get("journal"),
                "CHEMBL.DOCS.year": payload.get("year"),
                "CHEMBL.DOCS.volume": payload.get("volume"),
                "CHEMBL.DOCS.issue": payload.get("issue"),
                "CHEMBL.DOCS.first_page": payload.get("first_page"),
                "CHEMBL.DOCS.last_page": payload.get("last_page"),
                "CHEMBL.DOCS.doi": payload.get("doi"),
                "CHEMBL.DOCS.pubmed_id": payload.get("pubmed_id"),
                "CHEMBL.DOCS.abstract": payload.get("abstract"),
                "CHEMBL.DOCS.chembl_release": payload.get("src_id"),  # JSON reference to release
                
                # NEW: CHEMBL.SOURCE.* fields
                **source_fields,  # adds CHEMBL.SOURCE.* fields
                
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat()
            }
            
            non_none_count = sum(1 for v in data.values() if v is not None)
            logger.info(f"Mapped fields: {list(data.keys())}, non-None values: {non_none_count}")
            return data
        except Exception as e:
            logger.warning(f"Failed to fetch document {document_chembl_id}: {e}")
            return {
                "document_chembl_id": document_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "error": str(e)
            }

    def fetch_documents_batch(
        self, 
        document_chembl_ids: list[str],
        batch_size: int = 50
    ) -> dict[str, dict[str, Any]]:
        """Fetch multiple documents in batch.
        
        ChEMBL doesn't have a true batch endpoint for documents, so we use
        parallel requests with controlled concurrency.
        """
        if not document_chembl_ids:
            return {}
        
        results = {}
        
        # Process in chunks to avoid overwhelming the API
        for i in range(0, len(document_chembl_ids), batch_size):
            chunk = document_chembl_ids[i:i + batch_size]
            
            # For each document in the chunk, make individual requests
            # This is still more efficient than the original sequential approach
            for doc_id in chunk:
                try:
                    result = self.fetch_by_doc_id(doc_id)
                    results[doc_id] = result
                except Exception as e:
                    logger.warning(f"Failed to fetch document {doc_id} in batch: {e}")
                    results[doc_id] = {
                        "document_chembl_id": doc_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "error": str(e)
                    }
        
        return results

    def fetch_source_metadata(self, src_id: int) -> dict[str, Any]:
        """Fetch source metadata from ChEMBL."""
        try:
            payload = self._request("GET", f"source/{src_id}")
            return {
                "CHEMBL.SOURCE.src_id": payload.get("src_id"),
                "CHEMBL.SOURCE.src_description": payload.get("src_description"),
                "CHEMBL.SOURCE.src_short_name": payload.get("src_short_name"),
                "CHEMBL.SOURCE.src_url": payload.get("src_url"),
                "CHEMBL.SOURCE.data": json.dumps(payload) if payload else None
            }
        except Exception as e:
            logger.warning(f"Failed to fetch source {src_id}: {e}")
            return {
                "CHEMBL.SOURCE.src_id": None,
                "CHEMBL.SOURCE.src_description": None,
                "CHEMBL.SOURCE.src_short_name": None,
                "CHEMBL.SOURCE.src_url": None,
                "CHEMBL.SOURCE.data": None,
            }

    def __enter__(self) -> ChemblClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close all sessions."""
        with self._sessions_lock:
            for session in self._sessions:
                session.close()
            self._sessions.clear()

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        """Make HTTP request using base client functionality."""
        try:
            # Use the base client's request method
            response = self.session.get(f"{self.base_url}/{url}", timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
        
        request_url = url
        used_fallback = False
        abort_attempts = False
        total_attempts = cfg.retries.retries + 1
        last_exc = None
        last_exc_cause = None
        
        for attempt in range(1, total_attempts + 1):
            if abort_attempts:
                break
                
            if self._global_limiter is not None:
                self._global_limiter.acquire()
            limiter.acquire()
            
            if used_fallback:
                event = "request_fallback"
            else:
                event = "request_start" if attempt == 1 else "request_retry"
                
            logger.debug(
                event,
                extra={
                    "url": request_url,
                    "attempt": attempt,
                    "rps": cfg.rps,
                    "timeout": read_timeout,
                },
            )
            
            try:
                start_time = monotonic()
                session = self._get_session()
                with session.get(request_url, timeout=(30.0, read_timeout)) as response:
                    response.raise_for_status()
                    try:
                        data = cast(dict[str, Any], response.json())
                    except ValueError as exc:
                        elapsed = monotonic() - start_time
                        logger.exception(
                            "json_error",
                            extra={"url": request_url, "elapsed": elapsed},
                        )
                        raise ValueError(f"invalid JSON in response from {request_url}") from exc
                        
                    response_elapsed = getattr(response, "elapsed", None)
                    response_elapsed_seconds: float | None
                    if response_elapsed is not None and hasattr(response_elapsed, "total_seconds"):
                        response_elapsed_seconds = float(response_elapsed.total_seconds())
                    else:
                        response_elapsed_seconds = None
                        
                    duration = monotonic() - start_time
                    logger.debug(
                        "request_ok",
                        extra={
                            "url": request_url,
                            "status": getattr(response, "status_code", None),
                            "rps": cfg.rps,
                            "elapsed": duration,
                            "response_elapsed": response_elapsed_seconds,
                            "timeout": read_timeout,
                        },
                    )
                    
                    with self._cache_lock:
                        cached = self.cache.get(cache_key)
                        if cached is not None:
                            return cast(dict[str, Any], cached)
                        self.cache.set(cache_key, data)
                        logger.debug("cache_set", extra={"url": url, "rps": cfg.rps})
                        
                    if used_fallback:
                        logger.debug(
                            "request_fallback_ok",
                            extra={
                                "original_url": url,
                                "fallback_url": request_url,
                                "attempt": attempt,
                                "rps": cfg.rps,
                                "elapsed": duration,
                                "response_elapsed": response_elapsed_seconds,
                                "timeout": read_timeout,
                            },
                        )
                    return data
                    
            except ValueError as exc:
                elapsed = monotonic() - start_time
                last_exc = exc
                last_exc_cause = None
                if attempt >= total_attempts:
                    logger.exception(
                        "request_fail",
                        extra={
                            "url": request_url,
                            "status": None,
                            "rps": cfg.rps,
                            "elapsed": elapsed,
                            "attempt": attempt,
                            "timeout": read_timeout,
                            "retry": attempt,
                            "backoff": 0.0,
                        },
                    )
                    break
                delay = _backoff_delay(attempt, cfg.retries, header_delay=None, jitter=self._jitter)
                _log_retry_warning(
                    "request_retry_json_error",
                    url=request_url,
                    attempt=attempt,
                    delay=delay,
                    elapsed=elapsed,
                    status=None,
                )
                _log_retry_delay(request_url, attempt, None, delay)
                sleep(delay)
                break
                
            except requests.exceptions.HTTPError as exc:
                elapsed = monotonic() - start_time
                response = getattr(exc, "response", None)
                status = getattr(response, "status_code", None)
                header_delay = None
                if response is not None:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            header_delay = float(retry_after)
                        except ValueError:
                            try:
                                retry_date = parsedate_to_datetime(retry_after)
                                if retry_date is not None:
                                    header_delay = retry_date.replace(tzinfo=timezone.utc).timestamp() - datetime.now(timezone.utc).timestamp()
                            except ValueError:
                                pass
                last_exc = exc
                last_exc_cause = None
                if attempt >= total_attempts:
                    logger.exception(
                        "request_fail",
                        extra={
                            "url": request_url,
                            "status": status,
                            "rps": cfg.rps,
                            "elapsed": elapsed,
                            "attempt": attempt,
                            "timeout": read_timeout,
                            "retry": attempt,
                            "backoff": 0.0,
                        },
                    )
                    break
                delay = _backoff_delay(attempt, cfg.retries, header_delay=header_delay, jitter=self._jitter)
                _log_retry_warning(
                    "request_retry_http_error",
                    url=request_url,
                    attempt=attempt,
                    delay=delay,
                    elapsed=elapsed,
                    status=status,
                    exc_info=True,
                )
                _log_retry_delay(request_url, attempt, status, delay, header_delay)
                sleep(delay)
                break
                
            except requests.RequestException as exc:
                elapsed = monotonic() - start_time
                normalized_exc = exc
                last_exc = normalized_exc
                last_exc_cause = exc if normalized_exc is not exc else None
                response = getattr(exc, "response", None)
                status = getattr(response, "status_code", None)
                name_resolution_error = _is_name_resolution_error(exc)
                
                if name_resolution_error:
                    logger.exception(
                        "request_name_resolution_error",
                        extra={
                            "url": request_url,
                            "attempt": attempt,
                            "rps": cfg.rps,
                            "timeout": read_timeout,
                            "retry": attempt,
                            "backoff": 0.0,
                            "status": status,
                            "elapsed": elapsed,
                        },
                    )
                    abort_attempts = True
                    break
                    
                if attempt >= total_attempts:
                    logger.exception(
                        "request_fail",
                        extra={
                            "url": request_url,
                            "status": None,
                            "rps": cfg.rps,
                            "elapsed": elapsed,
                            "attempt": attempt,
                            "timeout": read_timeout,
                            "retry": attempt,
                            "backoff": 0.0,
                        },
                    )
                    break
                delay = _backoff_delay(attempt, cfg.retries, header_delay=None, jitter=self._jitter)
                _log_retry_warning(
                    "request_retry_exception",
                    url=request_url,
                    attempt=attempt,
                    delay=delay,
                    elapsed=elapsed,
                    status=status,
                    exc_info=True,
                )
                _log_retry_delay(request_url, attempt, status, delay)
                sleep(delay)
                break

        if last_exc is not None:
            if last_exc_cause is not None and last_exc is not last_exc_cause:
                raise last_exc from last_exc_cause
            raise last_exc
            
        # This should never be reached due to the loop structure
        raise RuntimeError(f"Request loop exited unexpectedly for {url}")

    def fetch_all_activities(self, activity_ids: list[str]) -> Iterator[dict[str, Any]]:
        """Fetch activity data from ChEMBL API for given activity IDs.

        Parameters
        ----------
        activity_ids : list[str]
            List of ChEMBL activity IDs to fetch.

        Yields
        ------
        dict[str, Any]
            Activity data from ChEMBL API.
        """
        if not activity_ids:
            return

        # ChEMBL API base URL for activities
        base_url = "https://www.ebi.ac.uk/chembl/api/data/activity"

        # Process activity IDs in batches to avoid URL length limits
        batch_size = 100  # ChEMBL API can handle up to 100 IDs per request

        for i in range(0, len(activity_ids), batch_size):
            batch_ids = activity_ids[i : i + batch_size]

            # Build URL with activity IDs as query parameters
            # Format: /activity?activity_id__in=ID1,ID2,ID3
            activity_ids_str = ",".join(batch_ids)
            url = f"{base_url}?activity_id__in={activity_ids_str}&format=json&limit=1000"

            try:
                response_data = self._request("GET", url)

                # Extract activities from response
                if isinstance(response_data, dict) and "activities" in response_data:
                    activities = response_data["activities"]
                    if isinstance(activities, list):
                        yield from activities
                    else:
                        logger.warning(f"Unexpected activities format in response: {type(activities)}")
                else:
                    logger.warning(f"Unexpected response format: {type(response_data)}")

            except Exception as e:
                logger.error(f"Failed to fetch activities for batch {i // batch_size + 1}: {e}")
                # Continue with next batch instead of failing completely
                continue

    def clear_cache(self) -> None:
        """Remove all entries from the in-memory cache."""

        with self._cache_lock:
            self.cache.clear()


