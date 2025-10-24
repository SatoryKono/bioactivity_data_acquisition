"""HTTP client for ChEMBL API endpoints."""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

from library.clients.base import BaseApiClient
from library.config import APIClientConfig
from library.common.cache_manager import CacheManager
from library.common.fallback_data import get_fallback_assay_data

logger = logging.getLogger(__name__)


def _safe_log_error(log_func, message_template: str, *args) -> None:
    """Безопасное логирование без использования % форматирования."""
    try:
        # Форматируем сообщение вручную, заменяя %s на аргументы
        message_parts = message_template.split('%s')
        if len(message_parts) - 1 != len(args):
            # Если количество %s не совпадает с количеством аргументов, просто выводим как есть
            log_func(message_template)
            return
        
        # Собираем сообщение
        result = []
        for i, part in enumerate(message_parts[:-1]):
            result.append(part)
            result.append(str(args[i]))
        result.append(message_parts[-1])
        
        # Логируем без форматирования
        log_func(''.join(result))
    except Exception:  # noqa: S110
        # Если что-то пошло не так с логированием, просто пропускаем
        pass


class ChEMBLClient(BaseApiClient):
    """HTTP client for ChEMBL API endpoints."""

    def __init__(self, config: APIClientConfig, **kwargs: Any) -> None:
        # Extract our custom parameters before passing to parent
        cache_dir = kwargs.pop('cache_dir', 'data/cache/chembl')
        cache_ttl = kwargs.pop('cache_ttl', 3600)  # 1 hour default
        use_fallback = kwargs.pop('use_fallback', True)
        fallback_on_errors = kwargs.pop('fallback_on_errors', True)
        
        super().__init__(config, **kwargs)
        
        # Initialize cache manager
        self.cache = CacheManager(cache_dir=cache_dir, default_ttl=cache_ttl)
        
        # Fallback settings
        self.use_fallback = use_fallback
        self.fallback_on_errors = fallback_on_errors

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
            _safe_log_error(logger.warning, "Failed to fetch molecule %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to resolve molregno %s to ChEMBL ID: %s", molregno, str(e))
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
                _safe_log_error(logger.warning, "Failed to fetch molecules batch %s: %s", batch, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch molecule form for %s: %s", molecule_chembl_id, str(e))
            return {}

    def fetch_molecule_properties(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch molecule properties."""
        try:
            payload = self._request("GET", f"molecule_properties?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_molecule_properties(payload)
        except Exception as e:
            _safe_log_error(logger.warning, "Failed to fetch molecule properties for %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch mechanism for %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch ATC classification for %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch compound synonym for %s: %s", molecule_chembl_id, str(e))
            return {}

    def fetch_drug(self, molecule_chembl_id: str) -> dict[str, Any]:
        """Fetch drug data."""
        try:
            payload = self._request("GET", f"drug?molecule_chembl_id={molecule_chembl_id}&format=json")
            return self._parse_drug(payload)
        except Exception as e:
            _safe_log_error(logger.warning, "Failed to fetch drug data for %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch drug warning for %s: %s", molecule_chembl_id, str(e))
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
            _safe_log_error(logger.warning, "Failed to fetch xref source for %s: %s", molecule_chembl_id, str(e))
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
                _safe_log_error(logger.warning, "Failed to fetch activities batch %s: %s", batch, str(e))
                # Yield empty records for failed batch
                for activity_id in batch:
                    empty_record = self._create_empty_activity_record(activity_id, str(e))
                    yield empty_record
        
        logger.info(f"Activity fetch summary: {total_found} found, {total_missing} missing, {total_errors} errors")
        if total_missing > 0:
            logger.warning(f"High number of missing activities ({total_missing}). Check activity ID format and availability in ChEMBL.")

# Old methods removed - using new comprehensive methods below

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
            _safe_log_error(logger.warning, "Failed to fetch target %s: %s", target_chembl_id, str(e))
            return {
                "target_chembl_id": target_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z",
                "error": str(e)
            }

    def fetch_by_assay_id(self, assay_chembl_id: str) -> dict[str, Any]:
        """Fetch assay data by ChEMBL assay ID."""
        # Check cache first
        endpoint = "assay.json"
        params = {"assay_chembl_id": assay_chembl_id, "format": "json", "limit": 1}
        
        cached_data = self.cache.get("assays", endpoint, params)
        if cached_data:
            logger.debug(f"Using cached data for assay {assay_chembl_id}")
            return cached_data
        
        try:
            # Use correct ChEMBL API endpoint format
            payload = self._request("GET", f"assay.json?assay_chembl_id={assay_chembl_id}&format=json&limit=1")
            
            # Extract assay data from response
            assays = payload.get("assays", [])
            if not assays:
                return self._create_empty_assay_record(assay_chembl_id, "No data found in API response")
            
            assay_data = assays[0]  # Get first (and should be only) assay
            
            # Helper function to convert to JSON string if needed
            def to_json_string(value):
                if value is None:
                    return None
                if isinstance(value, (dict, list)):
                    import json
                    return json.dumps(value)
                return str(value) if value else None
            
            # Extract relevant assay fields
            result = {
                "assay_chembl_id": assay_chembl_id,
                "assay_type": assay_data.get("assay_type"),
                "assay_category": assay_data.get("assay_category"),
                "assay_cell_type": assay_data.get("assay_cell_type"),
                "assay_classifications": to_json_string(assay_data.get("assay_classifications")),
                "assay_group": assay_data.get("assay_group"),
                "assay_organism": assay_data.get("assay_organism"),
                "assay_parameters": assay_data.get("assay_parameters"),  # Keep as object for expansion
                "assay_parameters_json": to_json_string(assay_data.get("assay_parameters")),  # Keep for compatibility
                "assay_strain": assay_data.get("assay_strain"),
                "assay_subcellular_fraction": assay_data.get("assay_subcellular_fraction"),
                "assay_tax_id": assay_data.get("assay_tax_id"),
                "assay_test_type": assay_data.get("assay_test_type"),
                "assay_tissue": assay_data.get("assay_tissue"),
                "assay_type_description": assay_data.get("assay_type_description"),
                "bao_format": assay_data.get("bao_format"),
                "bao_label": assay_data.get("bao_label"),
                "bao_endpoint": assay_data.get("bao_endpoint"),  # New field
                "cell_chembl_id": assay_data.get("cell_chembl_id"),
                "confidence_description": assay_data.get("confidence_description"),
                "confidence_score": assay_data.get("confidence_score"),
                "assay_description": assay_data.get("description"),  # Renamed from description
                "document_chembl_id": assay_data.get("document_chembl_id"),
                "relationship_description": assay_data.get("relationship_description"),
                "relationship_type": assay_data.get("relationship_type"),
                "src_assay_id": assay_data.get("src_assay_id"),
                "src_id": assay_data.get("src_id"),
                "target_chembl_id": assay_data.get("target_chembl_id"),
                "tissue_chembl_id": assay_data.get("tissue_chembl_id"),
                "variant_sequence": assay_data.get("variant_sequence"),  # Keep as object for expansion
                "variant_sequence_json": to_json_string(assay_data.get("variant_sequence")),  # Keep for compatibility
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
            
            # Cache successful result
            self.cache.set("assays", endpoint, params, result)
            return result
        except Exception as e:
            _safe_log_error(logger.warning, "Failed to fetch assay %s: %s", assay_chembl_id, str(e))
            
            # Use fallback data if enabled
            if self.use_fallback and self.fallback_on_errors:
                logger.info(f"Using fallback data for assay {assay_chembl_id}")
                fallback_data = get_fallback_assay_data(assay_chembl_id)
                # Cache fallback data with shorter TTL
                self.cache.set("assays", endpoint, params, fallback_data, ttl=300)  # 5 minutes
                return fallback_data
            
            return self._create_empty_assay_record(assay_chembl_id, str(e))

    def _create_empty_assay_record(self, assay_chembl_id: str, error_msg: str) -> dict[str, Any]:
        """Create empty assay record with error information."""
        return {
            "assay_chembl_id": assay_chembl_id,
            "assay_type": None,
            "assay_category": None,
            "assay_cell_type": None,
            "assay_classifications": None,
            "assay_group": None,
            "assay_organism": None,
            "assay_parameters": None,
            "assay_parameters_json": None,
            "assay_strain": None,
            "assay_subcellular_fraction": None,
            "assay_tax_id": None,
            "assay_test_type": None,
            "assay_tissue": None,
            "assay_type_description": None,
            "bao_format": None,
            "bao_label": None,
            "bao_endpoint": None,
            "cell_chembl_id": None,
            "confidence_description": None,
            "confidence_score": None,
            "assay_description": None,
            "document_chembl_id": None,
            "relationship_description": None,
            "relationship_type": None,
            "src_assay_id": None,
            "src_id": None,
            "target_chembl_id": None,
            "tissue_chembl_id": None,
            "variant_sequence": None,
            "variant_sequence_json": None,
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "error": error_msg
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

    def fetch_assay_class(self, assay_class_id: int) -> dict[str, Any]:
        """Fetch assay class data from /assay_class/{id}."""
        try:
            payload = self._request("GET", f"assay_class/{assay_class_id}")
            return {
                "assay_class_id": assay_class_id,
                "assay_class_bao_id": payload.get("bao_id"),
                "assay_class_type": payload.get("class_type"),
                "assay_class_l1": payload.get("l1"),
                "assay_class_l2": payload.get("l2"),
                "assay_class_l3": payload.get("l3"),
                "assay_class_description": payload.get("description"),
            }
        except Exception as e:
            _safe_log_error(logger.warning, "Failed to fetch assay_class %s: %s", assay_class_id, str(e))
            return {
                "assay_class_id": assay_class_id,
                "error": str(e)
            }

    def fetch_assays_batch(self, assay_chembl_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch multiple assays using ChEMBL API batch endpoint."""
        try:
            # ChEMBL API URL length limit requires smaller batches
            if len(assay_chembl_ids) > 25:
                logger.warning("Batch size %d exceeds URL limit of 25, splitting into chunks", len(assay_chembl_ids))
                results = {}
                for i in range(0, len(assay_chembl_ids), 25):
                    chunk = assay_chembl_ids[i:i+25]
                    chunk_results = self.fetch_assays_batch(chunk)
                    results.update(chunk_results)
                return results
            
            # Check cache first for batch request
            endpoint = "assay.json"
            params = {"assay_chembl_id__in": ",".join(assay_chembl_ids), "format": "json", "limit": 1000}
            
            cached_data = self.cache.get("assays", endpoint, params)
            if cached_data:
                logger.debug(f"Using cached batch data for {len(assay_chembl_ids)} assays")
                return cached_data
            
            # Use proper ChEMBL API endpoint
            ids_str = ",".join(assay_chembl_ids)
            url = f"assay.json?assay_chembl_id__in={ids_str}&format=json&limit=1000"
            
            # Make request
            payload = self._request("GET", url)
            
            # Parse results
            results = {}
            assays = payload.get("assays", [])
            
            for assay in assays:
                parsed = self._parse_assay(assay)
                if parsed:
                    results[parsed["assay_chembl_id"]] = parsed
            
            # Add empty records for missing assays
            for assay_id in assay_chembl_ids:
                if assay_id not in results:
                    results[assay_id] = {
                        "assay_chembl_id": assay_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().isoformat() + "Z",
                        "error": "Not found in ChEMBL"
                    }
            
            # Cache successful batch results
            self.cache.set("assays", endpoint, params, results)
            
            logger.info(f"Fetched {len(results)} assays from ChEMBL batch endpoint")
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch assays batch: {e}")
            
            # Use fallback data if enabled
            if self.use_fallback and self.fallback_on_errors:
                logger.info(f"Using fallback data for batch of {len(assay_chembl_ids)} assays")
                results = {}
                for assay_id in assay_chembl_ids:
                    fallback_data = get_fallback_assay_data(assay_id)
                    results[assay_id] = fallback_data
                
                # Cache fallback batch data with shorter TTL
                self.cache.set("assays", endpoint, params, results, ttl=300)  # 5 minutes
                return results
            
            # Fallback to individual requests
            logger.info("Falling back to individual assay requests")
            results = {}
            for assay_id in assay_chembl_ids:
                try:
                    assay_data = self.fetch_by_assay_id(assay_id)
                    results[assay_id] = assay_data
                except Exception as individual_error:
                    logger.warning(f"Failed to fetch assay {assay_id}: {individual_error}")
                    results[assay_id] = {
                        "assay_chembl_id": assay_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().isoformat() + "Z",
                        "error": str(individual_error)
                    }
            return results

    def _parse_assay(self, assay_data: dict[str, Any]) -> dict[str, Any]:
        """Parse single assay data from ChEMBL API response."""
        try:
            # Helper function to convert to JSON string if needed
            def to_json_string(value):
                if value is None:
                    return None
                if isinstance(value, (dict, list)):
                    import json
                    return json.dumps(value)
                return str(value) if value else None
            
            return {
                "assay_chembl_id": assay_data.get("assay_chembl_id"),
                "assay_type": assay_data.get("assay_type"),
                "assay_category": assay_data.get("assay_category"),
                "assay_cell_type": assay_data.get("assay_cell_type"),
                "assay_classifications": to_json_string(assay_data.get("assay_classifications")),
                "assay_group": assay_data.get("assay_group"),
                "assay_organism": assay_data.get("assay_organism"),
                "assay_parameters": assay_data.get("assay_parameters"),  # Keep as object for expansion
                "assay_parameters_json": to_json_string(assay_data.get("assay_parameters")),  # Keep for compatibility
                "assay_strain": assay_data.get("assay_strain"),
                "assay_subcellular_fraction": assay_data.get("assay_subcellular_fraction"),
                "assay_tax_id": assay_data.get("assay_tax_id"),
                "assay_test_type": assay_data.get("assay_test_type"),
                "assay_tissue": assay_data.get("assay_tissue"),
                "assay_type_description": assay_data.get("assay_type_description"),
                "bao_format": assay_data.get("bao_format"),
                "bao_label": assay_data.get("bao_label"),
                "bao_endpoint": assay_data.get("bao_endpoint"),  # New field
                "cell_chembl_id": assay_data.get("cell_chembl_id"),
                "confidence_description": assay_data.get("confidence_description"),
                "confidence_score": assay_data.get("confidence_score"),
                "assay_description": assay_data.get("description"),  # Renamed from description
                "document_chembl_id": assay_data.get("document_chembl_id"),
                "relationship_description": assay_data.get("relationship_description"),
                "relationship_type": assay_data.get("relationship_type"),
                "src_assay_id": assay_data.get("src_assay_id"),
                "src_id": assay_data.get("src_id"),
                "target_chembl_id": assay_data.get("target_chembl_id"),
                "tissue_chembl_id": assay_data.get("tissue_chembl_id"),
                "variant_sequence": assay_data.get("variant_sequence"),  # Keep as object for expansion
                "variant_sequence_json": to_json_string(assay_data.get("variant_sequence")),  # Keep for compatibility
                "source_system": "ChEMBL",
                "extracted_at": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            logger.error(f"Failed to parse assay data: {e}")
            return {}

    def fetch_assay_classifications_batch(self, assay_class_ids: list[int]) -> dict[int, dict[str, Any]]:
        """Batch fetch assay classes (fallback to individual if no batch endpoint)."""
        results = {}
        
        # ChEMBL doesn't have batch endpoint for assay_class, so use individual requests
        for class_id in assay_class_ids:
            try:
                class_data = self.fetch_assay_class(class_id)
                results[class_id] = class_data
            except Exception as e:
                _safe_log_error(logger.warning, "Failed to fetch assay_class %s in batch: %s", class_id, e)
                results[class_id] = {"assay_class_id": class_id, "error": str(e)}
        
        return results

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
            _safe_log_error(logger.warning, "Failed to fetch document %s: %s", document_chembl_id, str(e))
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
                    _safe_log_error(logger.warning, "Failed to fetch document %s in batch: %s", doc_id, str(e))
                    results[doc_id] = {
                        "document_chembl_id": doc_id,
                        "source_system": "ChEMBL",
                        "extracted_at": datetime.utcnow().isoformat() + "Z",
                        "error": str(e)
                    }
        
        return results

    def fetch_activity(self, activity_id: str | int) -> dict[str, Any]:
        """Fetch single activity data by activity ID."""
        try:
            # ChEMBL API accepts both numeric and CHEMBL format IDs
            activity_id_str = str(activity_id)
            payload = self._request("GET", f"activity/{activity_id_str}?format=json")
            return self._parse_activity(payload)
        except Exception as e:
            logger.warning("Failed to fetch activity %s: %s", activity_id, str(e))
            return self._create_empty_activity_record(activity_id, str(e))

    def fetch_activities_batch(self, activity_ids: list[str | int]) -> dict[str, dict[str, Any]]:
        """Fetch multiple activities in batch using ChEMBL API."""
        if not activity_ids:
            return {}
        
        results = {}
        
        # ChEMBL API has URL length limits, so we need to batch requests
        batch_size = 25  # Conservative limit for URL length
        
        for i in range(0, len(activity_ids), batch_size):
            batch = activity_ids[i:i + batch_size]
            
            try:
                # Convert to strings and join with commas
                ids_str = ",".join(str(aid) for aid in batch)
                url = f"activity?activity_id__in={ids_str}&format=json&limit=1000"
                
                payload = self._request("GET", url)
                activities = payload.get("activities", [])
                
                for activity in activities:
                    parsed = self._parse_activity({"activities": [activity]})
                    if parsed:
                        results[parsed["activity_id"]] = parsed
                
                # Add empty records for missing activities
                found_ids = {str(activity.get("activity_id")) for activity in activities}
                for activity_id in batch:
                    if str(activity_id) not in found_ids:
                        results[str(activity_id)] = self._create_empty_activity_record(activity_id, "Not found in API response")
                        
            except Exception as e:
                logger.warning("Failed to fetch activities batch %s: %s", batch, str(e))
                # Add empty records for failed batch
                for activity_id in batch:
                    results[str(activity_id)] = self._create_empty_activity_record(activity_id, str(e))
        
        return results

    def _parse_activity(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Parse activity data from ChEMBL API response."""
        activities = payload.get("activities", [])
        if not activities:
            return {}
        
        activity = activities[0]  # Take first activity
        
        # Extract main ACTIVITIES fields
        result = {
            # Primary key
            "activity_id": activity.get("activity_id"),
            
            # Foreign keys
            "assay_id": activity.get("assay_id"),
            "doc_id": activity.get("doc_id"),
            "record_id": activity.get("record_id"),
            "molregno": activity.get("molregno"),
            
            # Activity data
            "type": activity.get("type"),
            "relation": activity.get("relation"),
            "value": activity.get("value"),
            "units": activity.get("units"),
            "text_value": activity.get("text_value"),
            "upper_value": activity.get("upper_value"),
            
            # Standardized data
            "standard_type": activity.get("standard_type"),
            "standard_relation": activity.get("standard_relation"),
            "standard_value": activity.get("standard_value"),
            "standard_units": activity.get("standard_units"),
            "standard_flag": activity.get("standard_flag"),
            "standard_text_value": activity.get("standard_text_value"),
            "standard_upper_value": activity.get("standard_upper_value"),
            
            # Comments and metadata
            "activity_comment": activity.get("activity_comment"),
            "data_validity_comment": activity.get("data_validity_comment"),
            "potential_duplicate": activity.get("potential_duplicate"),
            "pchembl_value": activity.get("pchembl_value"),
            
            # Ontology fields
            "bao_endpoint": activity.get("bao_endpoint"),
            "uo_units": activity.get("uo_units"),
            "qudt_units": activity.get("qudt_units"),
            "src_id": activity.get("src_id"),
            "action_type": activity.get("action_type"),
            
            # System fields
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat() + "Z"
        }
        
        # Extract ACTIVITY_PROPERTIES (array of objects)
        activity_properties = activity.get("activity_properties", [])
        if activity_properties:
            # For now, take the first property or create a summary
            # In a full implementation, you might want to handle multiple properties
            prop = activity_properties[0] if activity_properties else {}
            result.update({
                "activity_prop_type": prop.get("type"),
                "activity_prop_relation": prop.get("relation"),
                "activity_prop_value": prop.get("value"),
                "activity_prop_units": prop.get("units"),
                "activity_prop_text_value": prop.get("text_value"),
                "activity_prop_standard_type": prop.get("standard_type"),
                "activity_prop_standard_relation": prop.get("standard_relation"),
                "activity_prop_standard_value": prop.get("standard_value"),
                "activity_prop_standard_units": prop.get("standard_units"),
                "activity_prop_standard_text_value": prop.get("standard_text_value"),
                "activity_prop_comments": prop.get("comments"),
                "activity_prop_result_flag": prop.get("result_flag")
            })
        
        # Extract LIGAND_EFF (ligand efficiency object)
        ligand_efficiency = activity.get("ligand_efficiency", {})
        if ligand_efficiency:
            result.update({
                "bei": ligand_efficiency.get("bei"),
                "sei": ligand_efficiency.get("sei"),
                "le": ligand_efficiency.get("le"),
                "lle": ligand_efficiency.get("lle")
            })
        
        return result

    def _create_empty_activity_record(self, activity_id: str | int, error_msg: str) -> dict[str, Any]:
        """Create empty activity record with error information."""
        return {
            "activity_id": str(activity_id),
            "assay_id": None,
            "doc_id": None,
            "record_id": None,
            "molregno": None,
            "type": None,
            "relation": None,
            "value": None,
            "units": None,
            "text_value": None,
            "upper_value": None,
            "standard_type": None,
            "standard_relation": None,
            "standard_value": None,
            "standard_units": None,
            "standard_flag": None,
            "standard_text_value": None,
            "standard_upper_value": None,
            "activity_comment": None,
            "data_validity_comment": None,
            "potential_duplicate": None,
            "pchembl_value": None,
            "bao_endpoint": None,
            "uo_units": None,
            "qudt_units": None,
            "src_id": None,
            "action_type": None,
            "activity_prop_type": None,
            "activity_prop_relation": None,
            "activity_prop_value": None,
            "activity_prop_units": None,
            "activity_prop_text_value": None,
            "activity_prop_standard_type": None,
            "activity_prop_standard_relation": None,
            "activity_prop_standard_value": None,
            "activity_prop_standard_units": None,
            "activity_prop_standard_text_value": None,
            "activity_prop_comments": None,
            "activity_prop_result_flag": None,
            "bei": None,
            "sei": None,
            "le": None,
            "lle": None,
            "source_system": "ChEMBL",
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "error": error_msg
        }