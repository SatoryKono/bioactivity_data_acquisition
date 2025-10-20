"""HTTP client for ChEMBL API endpoints."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig

logger = logging.getLogger(__name__)


class TestitemChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL molecule endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def get_chembl_status(self) -> dict[str, Any]:
        """Get ChEMBL status and release information."""
        try:
            payload = self._request("GET", "status")
            return {
                "chembl_release": payload.get("chembl_release", "unknown"),
                "status": payload.get("status", "unknown"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning(f"Failed to get ChEMBL status: {e}")
            return {
                "chembl_release": "unknown",
                "status": "error",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

    def fetch_molecule(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule data by ChEMBL ID."""
        try:
            payload = self._request("GET", f"molecule/{molecule_chembl_id}?format=json")
            return self._parse_molecule(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch molecule {molecule_chembl_id}: {e}")
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
            logger.warning(f"Failed to resolve molregno {molregno} to ChEMBL ID: {e}")
            return None

    def fetch_molecules_batch(self, molecule_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple molecules in a single batch request."""
        try:
            # ChEMBL supports batch requests for up to 50 molecules
            if len(molecule_chembl_ids) > 50:
                logger.warning(f"Batch size {len(molecule_chembl_ids)} exceeds ChEMBL limit of 50, splitting into chunks")
                results = {}
                for i in range(0, len(molecule_chembl_ids), 50):
                    chunk = molecule_chembl_ids[i:i+50]
                    chunk_results = self.fetch_molecules_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Prepare batch request payload
            batch_payload = {"chembl_ids": molecule_chembl_ids}
            
            # Make batch request
            payload = self._request("POST", "molecule/batch", json=batch_payload)
            
            # Parse results
            results = {}
            molecules = payload.get("molecules", [])
            
            for molecule in molecules:
                chembl_id = molecule.get("molecule_chembl_id")
                if chembl_id:
                    results[chembl_id] = self._parse_molecule({"molecules": [molecule]})
            
            # Add empty records for missing molecules
            for chembl_id in molecule_chembl_ids:
                if chembl_id not in results:
                    results[chembl_id] = self._create_empty_molecule_record(chembl_id, "Not found in batch response")
            
            return results
            
        except Exception as e:
            logger.warning(f"Failed to fetch molecules batch: {e}")
            # Fallback to individual requests
            results = {}
            for chembl_id in molecule_chembl_ids:
                results[chembl_id] = self.fetch_molecule(chembl_id)
            return results

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

    def fetch_atc_classification(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch ATC classification data."""
        try:
            payload = self._request("GET", f"atc_classification?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_atc_classification(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch ATC classification for {molecule_chembl_id}: {e}")
            return {}

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

    def fetch_drug_warning(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug warnings."""
        try:
            payload = self._request("GET", f"drug_warning?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_drug_warning(payload)
        except Exception as e:
            logger.warning(f"Failed to fetch drug warning for {molecule_chembl_id}: {e}")
            return {}

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


class ChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL document endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

    def fetch_by_doc_id(self, document_chembl_id: str) -> dict[str, Any]:
        """Fetch document data by ChEMBL document ID."""
        try:
            payload = self._request("GET", f"document/{document_chembl_id}")
            
            # Extract relevant document fields
            return {
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
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.warning(f"Failed to fetch document {document_chembl_id}: {e}")
            return {
                "document_chembl_id": document_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z",
                "error": str(e)
            }