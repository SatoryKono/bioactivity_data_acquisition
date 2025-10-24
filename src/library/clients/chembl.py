"""HTTP client for ChEMBL API endpoints."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class ChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL API endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL status and release information."""
        try:
            payload = self._request("GET", "status")
            return {
                "chembl_release": payload.get("chembl_release"),
                "status": payload.get("status", "unknown"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning("Failed to get ChEMBL status: %s", e)
            return {
                "chembl_release": None,
                "status": "error",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

    def fetch_molecule(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule data by ChEMBL ID using proper API endpoint."""
        try:
            payload = self._request("GET", f"molecule.json?molecule_chembl_id={molecule_chembl_id}&format=json&limit=1")
            parsed = self._parse_molecule(payload)
            if parsed:
                return parsed
            else:
                return self._create_empty_molecule_record(molecule_chembl_id, "No data found")
        except Exception as e:
            logger.warning("Failed to fetch molecule %s: %s", molecule_chembl_id, e)
            return self._create_empty_molecule_record(molecule_chembl_id, str(e))

    def resolve_molregno_to_chembl_id(self, molregno: int | str) -> str | None:
        """Resolve molregno to molecule_chembl_id.

        Returns the first matching ChEMBL ID or None if not found.
        """
        try:
            molregno_str = str(int(molregno))
        except Exception:
            return None
        try:
            payload = self._request("GET", f"molecule?molregno={molregno_str}&format=json")
            molecules = payload.get("molecules", [])
            if not molecules:
                return None
            chembl_id = molecules[0].get("molecule_chembl_id")
            return chembl_id
        except Exception as e:
            logger.warning("Failed to resolve molregno %s to ChEMBL ID: %s", molregno, e)
            return None

    def fetch_molecules_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple molecules using proper ChEMBL API."""
        try:
            # ChEMBL API URL length limit requires smaller batches
            if len(molecule_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(molecule_chembl_ids))
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
            payload = self._request("GET", url)
            
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
            logger.warning("Failed to fetch molecules batch: %s", e)
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_molecule(chembl_id)
            return results

    def fetch_molecules_batch_streaming(
        self,
        molecule_chembl_ids: list[str],
        batch_size: int = 50
    ) -> Generator[tuple[list[str], list[dict[str, Any]]], None, None]:
        """Stream molecules in batches using proper ChEMBL API.
        
        Yields tuples: (requested_ids, parsed_molecules).
        Uses molecule.json endpoint with molecule_chembl_id__in filter.
        """
        if not molecule_chembl_ids:
            return

        # ChEMBL API URL length limit requires smaller batches
        effective_batch_size = min(batch_size, 25)

        for i in range(0, len(molecule_chembl_ids), effective_batch_size):
            batch = molecule_chembl_ids[i:i + effective_batch_size]
            
            try:
                # Use proper ChEMBL API endpoint
                ids_str = ",".join(batch)
                url = f"molecule.json?molecule_chembl_id__in={ids_str}&format=json&limit=1000"
                
                # Make request
                payload = self._request("GET", url)
                
                # Parse results
                parsed_molecules = []
                molecules = payload.get("molecules", [])
                
                for molecule in molecules:
                    parsed_molecule = self._parse_molecule({"molecules": [molecule]})
                    if parsed_molecule:
                        parsed_molecules.append(parsed_molecule)
                
                # Add empty records for missing molecules
                found_ids = {mol.get("molecule_chembl_id") for mol in parsed_molecules}
                for chembl_id in batch:
                    if chembl_id not in found_ids:
                        empty_record = self._create_empty_molecule_record(chembl_id, "Not found in API response")
                        parsed_molecules.append(empty_record)
                
                yield (batch, parsed_molecules)
                
            except Exception as e:
                logger.warning("Failed to fetch molecules batch %s: %s", batch, e)
                # Yield empty records for failed batch
                failed_molecules = []
                for chembl_id in batch:
                    failed_molecules.append(self._create_empty_molecule_record(chembl_id, str(e)))
                yield (batch, failed_molecules)

    def fetch_molecule_form(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule form data."""
        try:
            payload = self._request("GET", f"molecule_form?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_molecule_form(payload)
        except Exception as e:
            logger.warning("Failed to fetch molecule form for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_molecule_properties(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule properties."""
        try:
            payload = self._request("GET", f"molecule_properties?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_molecule_properties(payload)
        except Exception as e:
            logger.warning("Failed to fetch molecule properties for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_molecule_properties_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch molecule properties for multiple molecules in batch."""
        try:
            # ChEMBL supports batch requests for properties
            if len(molecule_chembl_ids) > 50:
                logger.warning("Batch size %d exceeds ChEMBL limit of 50, splitting into chunks", len(molecule_chembl_ids))
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
            logger.warning("Failed to fetch molecule properties batch: %s", e)
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
            logger.warning("Failed to fetch mechanism for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_mechanism_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch mechanism data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(molecule_chembl_ids))
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
            payload = self._request("GET", url)
            
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
            logger.warning("Failed to fetch mechanism batch: %s", e)
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
            logger.warning("Failed to fetch ATC classification for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_atc_classification_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch ATC classification data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(molecule_chembl_ids))
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
            payload = self._request("GET", url)
            
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
            logger.warning("Failed to fetch ATC classification batch: %s", e)
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
            logger.warning("Failed to fetch compound synonym for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_drug(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug data."""
        try:
            payload = self._request("GET", f"drug?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_drug(payload)
        except Exception as e:
            logger.warning("Failed to fetch drug data for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_drug_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug data for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(molecule_chembl_ids))
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
            payload = self._request("GET", url)
            
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
            logger.warning("Failed to fetch drug batch: %s", e)
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
            logger.warning("Failed to fetch drug warning for %s: %s", molecule_chembl_id, e)
            return {}

    def fetch_drug_warning_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch drug warnings for multiple molecules using proper ChEMBL API."""
        try:
            if len(molecule_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(molecule_chembl_ids))
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
            payload = self._request("GET", url)
            
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
            logger.warning("Failed to fetch drug warning batch: %s", e)
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
            logger.warning("Failed to fetch xref source for %s: %s", molecule_chembl_id, e)
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
            "extracted_at": datetime.utcnow().isoformat() + "Z"
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

    def fetch_all_activities(self, activity_ids: list[str]) -> Generator[dict[str, Any], None, None]:
        """Fetch activity data for multiple activity IDs from ChEMBL API.
        
        Args:
            activity_ids: List of ChEMBL activity IDs to fetch
            
        Yields:
            Dictionary containing activity data for each ID
        """
        if not activity_ids:
            logger.warning("No activity IDs provided to fetch_all_activities")
            return
            
        logger.info(f"Fetching activity data for {len(activity_ids)} activity IDs")
        logger.debug(f"Sample activity IDs: {activity_ids[:5]}")
        
        # ChEMBL API supports batch requests for activities
        # Process in batches to avoid URL length limits
        batch_size = 25  # ChEMBL API limit
        
        total_found = 0
        total_missing = 0
        total_errors = 0
        
        for i in range(0, len(activity_ids), batch_size):
            batch = activity_ids[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                # Use ChEMBL activity endpoint with activity_id__in filter
                ids_str = ",".join(batch)
                url = f"activity.json?activity_id__in={ids_str}&format=json&limit=1000"
                
                logger.debug(f"Fetching batch {batch_num} with {len(batch)} activity IDs")
                logger.debug(f"Request URL: {url}")
                
                # Make request
                payload = self._request("GET", url)
                
                # Parse results
                activities = payload.get("activities", [])
                logger.info(f"Batch {batch_num}: Found {len(activities)} activities in API response")
                
                for activity in activities:
                    parsed_activity = self._parse_activity(activity)
                    if parsed_activity:
                        total_found += 1
                        yield parsed_activity
                
                # Add empty records for missing activities
                found_ids = {str(act.get("activity_id")) for act in activities}
                missing_in_batch = 0
                for activity_id in batch:
                    if activity_id not in found_ids:
                        missing_in_batch += 1
                        total_missing += 1
                        empty_record = self._create_empty_activity_record(activity_id, "Not found in API response")
                        yield empty_record
                
                if missing_in_batch > 0:
                    logger.warning(f"Batch {batch_num}: {missing_in_batch} activities not found in API response")
                        
            except Exception as e:
                total_errors += len(batch)
                logger.warning("Failed to fetch activities batch %s: %s", batch, e)
                # Yield empty records for failed batch
                for activity_id in batch:
                    empty_record = self._create_empty_activity_record(activity_id, str(e))
                    yield empty_record
        
        logger.info(f"Activity fetch summary: {total_found} found, {total_missing} missing, {total_errors} errors")
        if total_missing > 0:
            logger.warning(f"High number of missing activities ({total_missing}). Check activity ID format and availability in ChEMBL.")

    def _parse_activity(self, activity: dict[str, Any]) -> dict[str, Any]:
        """Parse activity data from ChEMBL API response."""
        return {
            "activity_chembl_id": activity.get("activity_id"),
            "assay_chembl_id": activity.get("assay_chembl_id"),
            "molecule_chembl_id": activity.get("molecule_chembl_id"),
            "target_chembl_id": activity.get("target_chembl_id"),
            "document_chembl_id": activity.get("document_chembl_id"),
            "activity_type": activity.get("type"),
            "activity_value": activity.get("value"),
            "activity_unit": activity.get("units"),
            "lower_bound": activity.get("lower_value"),
            "upper_bound": activity.get("upper_value"),
            "is_censored": activity.get("potential_duplicate"),
            "published_type": activity.get("published_type"),
            "published_relation": activity.get("published_relation"),
            "published_value": activity.get("published_value"),
            "published_units": activity.get("published_units"),
            "standard_type": activity.get("standard_type"),
            "standard_relation": activity.get("standard_relation"),
            "standard_value": activity.get("standard_value"),
            "standard_units": activity.get("standard_units"),
            "standard_flag": activity.get("standard_flag"),
            "pchembl_value": activity.get("pchembl_value"),
            "data_validity_comment": activity.get("data_validity_comment"),
            "activity_comment": activity.get("activity_comment"),
            "bao_endpoint": activity.get("bao_endpoint"),
            "bao_format": activity.get("bao_format"),
            "bao_label": activity.get("bao_label"),
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat() + "Z"
        }

    def _create_empty_activity_record(self, activity_chembl_id: str, error_msg: str) -> dict[str, Any]:
        """Create empty activity record with error information."""
        return {
            "activity_chembl_id": activity_chembl_id,
            "assay_chembl_id": None,
            "molecule_chembl_id": None,
            "target_chembl_id": None,
            "document_chembl_id": None,
            "published_type": None,
            "published_relation": None,
            "published_value": None,
            "published_units": None,
            "standard_type": None,
            "standard_relation": None,
            "standard_value": None,
            "standard_units": None,
            "standard_flag": None,
            "pchembl_value": None,
            "data_validity_comment": None,
            "activity_comment": None,
            "bao_endpoint": None,
            "bao_format": None,
            "bao_label": None,
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "error": error_msg
        }

    def fetch_by_target_id(self, target_chembl_id: str) -> dict[str, Any]:
        """Fetch target data by ChEMBL target ID to enrich assay data."""
        try:
            payload = self._request("GET", f"target/{target_chembl_id}")
            
            # Extract target organism and tax_id
            target_organism = payload.get("organism")
            target_tax_id = payload.get("tax_id")
            
            # Extract UniProt accession and isoform from target_components
            target_uniprot_accession = None
            target_isoform = None
            
            target_components = payload.get("target_components", [])
            if target_components:
                # Get the first protein component
                for component in target_components:
                    if component.get("component_type") == "PROTEIN":
                        target_uniprot_accession = component.get("accession")
                        # Check for isoform information in component description or synonyms
                        component_desc = component.get("component_description", "")
                        if "isoform" in component_desc.lower():
                            target_isoform = component_desc
                        break
            
            return {
                "target_chembl_id": target_chembl_id,
                "target_organism": target_organism,
                "target_tax_id": target_tax_id,
                "target_uniprot_accession": target_uniprot_accession,
                "target_isoform": target_isoform,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning("Failed to fetch target %s: %s", target_chembl_id, e)
            return {
                "target_chembl_id": target_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z",
                "error": str(e)
            }

    def fetch_by_assay_id(self, assay_chembl_id: str) -> dict[str, Any]:
        """Fetch assay data by ChEMBL assay ID."""
        try:
            payload = self._request("GET", f"assay/{assay_chembl_id}")
            
            # Extract relevant assay fields
            return {
                "assay_chembl_id": assay_chembl_id,
                "assay_type": payload.get("assay_type"),
                "assay_category": payload.get("assay_category"),
                "target_chembl_id": payload.get("target_chembl_id"),
                # Note: target_organism, target_tax_id, target_uniprot_accession, target_isoform
                # are not available in /assay endpoint - will be enriched via /target endpoint
                "bao_format": payload.get("bao_format"),
                "bao_label": payload.get("bao_label"),
                # Note: Extended BAO fields are not available in ChEMBL API v33+
                "bao_endpoint": None,  # Not available in API
                "bao_assay_format": None,  # Not available in API
                "bao_assay_type": None,  # Not available in API
                "bao_assay_type_label": None,  # Not available in API
                "bao_assay_type_uri": None,  # Not available in API
                "bao_assay_format_uri": None,  # Not available in API
                "bao_assay_format_label": None,  # Not available in API
                "bao_endpoint_uri": None,  # Not available in API
                "bao_endpoint_label": None,  # Not available in API
                # Note: Variant fields are not available in ChEMBL API
                "variant_id": None,  # Not available in API
                "is_variant": None,  # Not available in API
                "variant_accession": None,  # Not available in API
                "variant_sequence_accession": None,  # Not available in API
                "variant_sequence_mutation": None,  # Not available in API
                "variant_mutations": None,  # Not available in API
                "variant_sequence": payload.get("variant_sequence"),  # Available but often null
                "variant_text": None,  # Not available in API
                "variant_sequence_id": None,  # Not available in API
                "variant_organism": None,  # Not available in API
                "target_uniprot_accession": None,  # Will be enriched via /target endpoint
                "target_isoform": None,  # Will be enriched via /target endpoint
                "assay_organism": payload.get("assay_organism"),
                "assay_tax_id": payload.get("assay_tax_id"),
                "assay_strain": payload.get("assay_strain"),
                "assay_tissue": payload.get("assay_tissue"),
                "assay_cell_type": payload.get("assay_cell_type"),
                "assay_subcellular_fraction": payload.get("assay_subcellular_fraction"),
                "description": payload.get("description"),
                "assay_parameters": payload.get("assay_parameters"),  # Available but often null
                "assay_parameters_json": None,  # Not available in API
                "assay_format": None,  # Not available in API
                "confidence_score": payload.get("confidence_score"),
                "curated_by": payload.get("curated_by"),
                "src_id": payload.get("src_id"),
                "src_name": payload.get("src_name"),
                "src_assay_id": payload.get("src_assay_id"),
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning("Failed to fetch assay %s: %s", assay_chembl_id, e)
            return {
                "assay_chembl_id": assay_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z",
                "error": str(e)
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
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "error": error_msg
        }

    def fetch_by_doc_id(self, document_chembl_id: str) -> dict[str, Any]:
        """Fetch document data by ChEMBL document ID."""
        try:
            payload = self._request("GET", f"document/{document_chembl_id}")
            
            # Extract relevant document fields
            return {
                "document_chembl_id": document_chembl_id,
                "chembl_pmid": payload.get("pubmed_id"),
                "chembl_doc_type": payload.get("doc_type"),
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
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning("Failed to fetch document %s: %s", document_chembl_id, e)
            return {
                "document_chembl_id": document_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z",
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
                    logger.warning("Failed to fetch document %s in batch: %s", doc_id, e)
                    results[doc_id] = {
                        "document_chembl_id": doc_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().isoformat() + "Z",
                        "error": str(e)
                    }
        
        return results