"""Refactored testitem ETL pipeline using PipelineBase."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.common.pipeline_base import PipelineBase
from library.testitem.config import TestitemConfig
from library.testitem.normalize import TestitemNormalizer
from library.testitem.quality import TestitemQualityFilter
from library.testitem.validate import TestitemValidator

logger = logging.getLogger(__name__)


class TestitemPipeline(PipelineBase[TestitemConfig]):
    """Testitem ETL pipeline using unified PipelineBase."""
    
    def __init__(self, config: TestitemConfig) -> None:
        """Initialize testitem pipeline with configuration."""
        super().__init__(config)
        self.validator = TestitemValidator(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.normalizer = TestitemNormalizer(config.model_dump() if hasattr(config, 'model_dump') else {})
        self.quality_filter = TestitemQualityFilter(config.model_dump() if hasattr(config, 'model_dump') else {})
    
    def _setup_clients(self) -> None:
        """Initialize HTTP clients for testitem sources."""
        self.clients = {}
        
        # ChEMBL client
        if "chembl" in self.config.sources and self.config.sources["chembl"].enabled:
            self.clients["chembl"] = self._create_chembl_client()
        
        # PubChem client
        if "pubchem" in self.config.sources and self.config.sources["pubchem"].enabled:
            self.clients["pubchem"] = self._create_pubchem_client()
    
    def _create_chembl_client(self) -> ChEMBLClient:
        """Create ChEMBL client."""
        from library.config import RateLimitSettings
        
        source_config = self.config.sources["chembl"]
        timeout = source_config.http.timeout_sec or self.config.http.global_.timeout_sec
        timeout = max(timeout, 60.0)  # At least 60 seconds for ChEMBL
        
        headers = self._get_headers("chembl")
        headers.update(self.config.http.global_.headers)
        headers.update(source_config.http.headers)
        
        processed_headers = self._process_headers(headers)
        
        from library.config import APIClientConfig
        
        # Get retry settings
        retries = source_config.http.retries or self.config.http.global_.retries
        
        # Get rate limit settings if available
        rate_limit = None
        if hasattr(source_config, 'rate_limit') and source_config.rate_limit:
            rate_limit = RateLimitSettings(
                max_calls=source_config.rate_limit.max_calls,
                period=source_config.rate_limit.period,
            )
        
        client_config = APIClientConfig(
            name="chembl",
            base_url=source_config.http.base_url,
            timeout=timeout,
            retries=retries,
            rate_limit=rate_limit,
            headers=processed_headers,
        )
        
        return ChEMBLClient(client_config)
    
    def _create_pubchem_client(self) -> Any:
        """Create PubChem client."""
        import requests
        
        # Create a simple requests session for PubChem
        session = requests.Session()
        
        # Set headers
        headers = self._get_headers("pubchem")
        headers.update(self.config.http.global_.headers)
        session.headers.update(headers)
        
        return session
    
    def _get_headers(self, source: str) -> dict[str, str]:
        """Get default headers for a source."""
        return {
            "Accept": "application/json",
            "User-Agent": "bioactivity-data-acquisition/0.1.0",
        }
    
    def _process_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Process headers with secret placeholders."""
        import os
        processed = {}
        for key, value in headers.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                processed[key] = os.getenv(env_var, value)
            else:
                processed[key] = value
        return processed
    
    def _is_valid_value(self, value) -> bool:
        """Check if a value is valid (not null, not empty array, not empty string)."""
        import numpy as np
        
        # Check for None
        if value is None:
            return False
        
        # Check for numpy arrays first (before pd.isna)
        if isinstance(value, np.ndarray):
            return value.size > 0
        
        # Check for pandas NA (but not for numpy arrays)
        try:
            # Skip pd.isna for numpy arrays as they're already handled above
            if not isinstance(value, np.ndarray) and pd.isna(value):
                return False
        except (ValueError, TypeError):
            # If pd.isna fails, continue with other checks
            pass
        
        # Check for empty strings
        if isinstance(value, str) and value.strip() == "":
            return False
        
        # Check for empty lists
        if isinstance(value, list) and len(value) == 0:
            return False
        
        return True
    
    def extract(self, input_data: pd.DataFrame) -> pd.DataFrame:
        """Extract testitem data from multiple sources."""
        logger.info(f"Extracting testitem data for {len(input_data)} testitems")
        
        # Apply limit if specified
        if getattr(self.config.runtime, 'limit', None) is not None:
            input_data = input_data.head(self.config.runtime.limit)
        
        # Check for duplicates
        duplicates = input_data["molecule_chembl_id"].duplicated()
        if duplicates.any():
            raise ValueError("Duplicate molecule_chembl_id values detected")
        
        # Extract data from each enabled source
        extracted_data = input_data.copy()
        
        # ChEMBL extraction
        if "chembl" in self.clients:
            try:
                logger.info("Extracting data from ChEMBL")
                chembl_data = self._extract_from_chembl(extracted_data)
                extracted_data = self._merge_chembl_data(extracted_data, chembl_data)
            except Exception as e:
                logger.error("Failed to extract from ChEMBL: %s", e)
                if not getattr(self.config.runtime, 'allow_incomplete_sources', False):
                    raise
        
        # PubChem extraction
        if "pubchem" in self.clients:
            try:
                logger.info("Extracting data from PubChem")
                pubchem_data = self._extract_from_pubchem(extracted_data)
                extracted_data = self._merge_pubchem_data(extracted_data, pubchem_data)
            except Exception as e:
                logger.error("Failed to extract from PubChem: %s", e)
                if not getattr(self.config.runtime, 'allow_incomplete_sources', False):
                    raise
        
        # Validate input data first
        self.validator.validate_input(input_data)
        
        # Return the extracted and enriched data
        logger.info(f"Extracted data for {len(extracted_data)} testitems")
        return extracted_data
    
    def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from ChEMBL."""
        if data.empty:
            return pd.DataFrame()
        
        # Get ChEMBL client
        client = self.clients.get("chembl")
        if not client:
            logger.warning("ChEMBL client not available")
            return pd.DataFrame()
        
        # Extract molecule IDs
        molecule_ids = data["molecule_chembl_id"].dropna().unique().tolist()
        if not molecule_ids:
            logger.warning("No valid molecule_chembl_id found for ChEMBL extraction")
            return pd.DataFrame()
        
        logger.info(f"Extracting ChEMBL data for {len(molecule_ids)} molecules")
        
        # ChEMBL fields to extract
        chembl_fields = [
            "molecule_chembl_id", "molregno", "pref_name", "max_phase", 
            "therapeutic_flag", "structure_type", "alogp", "hba", "hbd", 
            "psa", "rtb", "ro3_pass", "qed_weighted", "oral", "parenteral", 
            "topical", "withdrawn_flag", "parent_chembl_id", "molecule_type",
            "first_approval", "black_box_warning", "natural_product", 
            "first_in_class", "chirality", "prodrug", "inorganic_flag", 
            "polymer_flag", "usan_year", "availability_type", "usan_stem",
            "usan_substem", "usan_stem_definition", "indication_class",
            "withdrawn_year", "withdrawn_country", "withdrawn_reason",
            "mechanism_of_action", "direct_interaction", "molecular_mechanism",
            "drug_chembl_id", "drug_name", "drug_type", "drug_substance_flag",
            "drug_indication_flag", "drug_antibacterial_flag", "drug_antiviral_flag",
            "drug_antifungal_flag", "drug_antiparasitic_flag", "drug_antineoplastic_flag",
            "drug_immunosuppressant_flag", "drug_antiinflammatory_flag"
        ]
        
        # Process in batches
        batch_size = 25  # ChEMBL API limit
        all_records = []
        
        for i in range(0, len(molecule_ids), batch_size):
            batch_ids = molecule_ids[i:i + batch_size]
            try:
                batch_data = self._fetch_chembl_batch(batch_ids, chembl_fields, client)
                if not batch_data.empty:
                    all_records.append(batch_data)
            except Exception as e:
                logger.error(f"Failed to fetch ChEMBL batch {i//batch_size + 1}: {e}")
                continue
        
        if not all_records:
            logger.warning("No ChEMBL data extracted")
            return pd.DataFrame()
        
        # Combine all records
        result = pd.concat(all_records, ignore_index=True)
        result["retrieved_at"] = pd.Timestamp.now().isoformat()
        
        logger.info(f"ChEMBL extraction completed: {len(result)} records")
        return result
    
    def _fetch_chembl_batch(self, molecule_ids: list[str], fields: list[str], client) -> pd.DataFrame:
        """Fetch a batch of molecules from ChEMBL API."""
        from urllib.parse import urlencode
        
        import requests
        params = {
            "format": "json",
            "limit": "1000",
            "fields": ",".join(fields)
        }
        
        # Add molecule filter
        if len(molecule_ids) == 1:
            params["molecule_chembl_id"] = molecule_ids[0]
        else:
            params["molecule_chembl_id__in"] = ",".join(molecule_ids)
        
        query_string = urlencode(params)
        
        try:
            # Make request using ChEMBL client's _request method
            response = client._request("GET", f"molecule.json?{query_string}")
            if not response:
                logger.warning("No response from ChEMBL for batch: %s", molecule_ids)
                return pd.DataFrame()
            
            # response is already parsed data from ChEMBL client
            if "molecules" not in response:
                logger.warning("No molecules found in ChEMBL response for batch: %s", molecule_ids)
                return pd.DataFrame()
            
            # Convert to DataFrame and flatten nested structures
            molecules = response["molecules"]
            if not molecules:
                return pd.DataFrame()
            
            # Flatten nested structures
            flattened_molecules = []
            for molecule in molecules:
                flattened = self._flatten_molecule_data(molecule)
                flattened_molecules.append(flattened)
            
            df = pd.DataFrame(flattened_molecules)
            return df
            
        except requests.RequestException as e:
            logger.error(f"ChEMBL API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to process ChEMBL response: {e}")
            raise
    
    def _flatten_molecule_data(self, molecule: dict) -> dict:
        """Flatten nested molecule structures into top-level fields."""
        import json
        
        flattened = molecule.copy()
        
        # Extract from molecule_hierarchy
        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            hierarchy = molecule["molecule_hierarchy"]
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")
        
        # Extract from molecule_properties
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]
            
            # Физико-химические свойства
            flattened["alogp"] = props.get("alogp")
            flattened["aromatic_rings"] = props.get("aromatic_rings")
            flattened["hba"] = props.get("hba")
            flattened["hbd"] = props.get("hbd")
            flattened["hba_lipinski"] = props.get("hba_lipinski")
            flattened["hbd_lipinski"] = props.get("hbd_lipinski")
            flattened["heavy_atoms"] = props.get("heavy_atoms")
            flattened["mw_freebase"] = props.get("mw_freebase")
            flattened["full_mwt"] = props.get("full_mwt")
            flattened["mw_monoisotopic"] = props.get("mw_monoisotopic")
            flattened["full_molformula"] = props.get("full_molformula")
            flattened["molecular_species"] = props.get("molecular_species")
            flattened["num_ro5_violations"] = props.get("num_ro5_violations")
            flattened["num_lipinski_ro5_violations"] = props.get("num_lipinski_ro5_violations")
            flattened["psa"] = props.get("psa")
            flattened["qed_weighted"] = props.get("qed_weighted")
            flattened["ro3_pass"] = props.get("ro3_pass")
            flattened["rtb"] = props.get("rtb")
            flattened["acd_logd"] = props.get("acd_logd")
            flattened["acd_logp"] = props.get("acd_logp")
            flattened["acd_most_apka"] = props.get("acd_most_apka")
            flattened["acd_most_bpka"] = props.get("acd_most_bpka")
            
            # Механизм действия
            flattened["mechanism_of_action"] = props.get("mechanism_of_action")
            flattened["molecular_mechanism"] = props.get("molecular_mechanism")
            flattened["direct_interaction"] = props.get("direct_interaction")
        
        # Extract from molecule_structures
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            structures = molecule["molecule_structures"]
            flattened["canonical_smiles"] = structures.get("canonical_smiles")
            flattened["standard_inchi"] = structures.get("standard_inchi")
            # ChEMBL API may not always return standard_inchi_key
            flattened["standard_inchi_key"] = structures.get("standard_inchi_key") if structures.get("standard_inchi_key") else None
        
        # Extract from molecule_synonyms
        if "molecule_synonyms" in molecule and molecule["molecule_synonyms"]:
            synonyms = molecule["molecule_synonyms"]
            if isinstance(synonyms, list) and synonyms:
                # Конкатенируем все синонимы в одну строку для all_names
                synonym_names = []
                for syn in synonyms:
                    if isinstance(syn, dict) and "molecule_synonym" in syn:
                        synonym_names.append(syn["molecule_synonym"])
                    elif isinstance(syn, str):
                        synonym_names.append(syn)
                flattened["all_names"] = "; ".join(synonym_names) if synonym_names else None
                # Сериализуем как JSON string для molecule_synonyms
                flattened["molecule_synonyms"] = json.dumps(synonyms)
            else:
                flattened["all_names"] = None
                flattened["molecule_synonyms"] = None
        else:
            flattened["all_names"] = None
            flattened["molecule_synonyms"] = None
        
        # Extract from atc_classifications
        if "atc_classifications" in molecule and molecule["atc_classifications"]:
            atc_classifications = molecule["atc_classifications"]
            if isinstance(atc_classifications, list) and atc_classifications:
                # Сериализуем как JSON string для сохранения структуры
                flattened["atc_classifications"] = json.dumps(atc_classifications)
            else:
                flattened["atc_classifications"] = None
        
        # Extract from cross_references
        if "cross_references" in molecule and molecule["cross_references"]:
            cross_refs = molecule["cross_references"]
            if isinstance(cross_refs, list) and cross_refs:
                # Сериализуем как JSON string
                flattened["cross_references"] = json.dumps(cross_refs)
            else:
                flattened["cross_references"] = None
        
        # Extract from biotherapeutic
        if "biotherapeutic" in molecule and molecule["biotherapeutic"]:
            biotherapeutic = molecule["biotherapeutic"]
            if isinstance(biotherapeutic, dict):
                # Извлекаем основные поля биотерапевтического соединения
                flattened["biotherapeutic"] = json.dumps(biotherapeutic)
                # Можно добавить отдельные поля если нужно:
                # flattened["peptide_sequence"] = biotherapeutic.get("peptide_sequence")
                # flattened["helm_notation"] = biotherapeutic.get("helm_notation")
            else:
                flattened["biotherapeutic"] = None
        
        # Extract molecule_hierarchy as JSON
        if "molecule_hierarchy" in molecule and molecule["molecule_hierarchy"]:
            hierarchy = molecule["molecule_hierarchy"]
            flattened["molecule_hierarchy"] = json.dumps(hierarchy)
        
        # Extract molecule_properties as JSON
        if "molecule_properties" in molecule and molecule["molecule_properties"]:
            props = molecule["molecule_properties"]
            flattened["molecule_properties"] = json.dumps(props)
        
        # Extract molecule_structures as JSON
        if "molecule_structures" in molecule and molecule["molecule_structures"]:
            structures = molecule["molecule_structures"]
            flattened["molecule_structures"] = json.dumps(structures)
        
        
        # Extract boolean flags
        if "orphan" in molecule:
            flattened["orphan"] = bool(molecule.get("orphan", False))
        
        if "veterinary" in molecule:
            flattened["veterinary"] = bool(molecule.get("veterinary", False))
        
        if "chemical_probe" in molecule:
            flattened["chemical_probe"] = bool(molecule.get("chemical_probe", False))
        
        # Extract helm_notation if present
        if "helm_notation" in molecule:
            flattened["helm_notation"] = molecule.get("helm_notation")
        
        # Extract chirality_chembl
        if "chirality_chembl" in molecule:
            flattened["chirality_chembl"] = molecule.get("chirality_chembl")
        
        # Extract molecule_type_chembl
        if "molecule_type_chembl" in molecule:
            flattened["molecule_type_chembl"] = molecule.get("molecule_type_chembl")
        
        return flattened
    
    def _extract_from_pubchem(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract data from PubChem."""
        if data.empty:
            return pd.DataFrame()
        
        # Get PubChem client
        client = self.clients.get("pubchem")
        if not client:
            logger.warning("PubChem client not available")
            return pd.DataFrame()
        
        # Check if PubChem is enabled
        if not getattr(self.config.sources["pubchem"], 'enabled', True):
            logger.info("PubChem extraction disabled")
            return pd.DataFrame()
        
        # Get InChI Keys for PubChem lookup
        # Check if standard_inchi_key column exists
        if "standard_inchi_key" not in data.columns:
            logger.warning("No standard_inchi_key column found for PubChem extraction")
            return pd.DataFrame()
            
        inchi_keys = data["standard_inchi_key"].dropna().unique().tolist()
        if not inchi_keys:
            logger.warning("No InChI Keys found for PubChem extraction")
            return pd.DataFrame()
        
        logger.info(f"Extracting PubChem data for {len(inchi_keys)} InChI Keys")
        
        # PubChem fields to extract
        pubchem_fields = [
            "pubchem_cid", "pubchem_molecular_formula", "pubchem_molecular_weight",
            "pubchem_canonical_smiles", "pubchem_inchi", "pubchem_inchi_key",
            "pubchem_registry_id", "pubchem_rn"
        ]
        
        # Process in batches (PubChem rate limit: 5 requests/second)
        all_records = []
        
        for i, inchi_key in enumerate(inchi_keys):
            try:
                # Rate limiting: wait 0.2 seconds between requests (5 req/sec)
                if i > 0:
                    import time
                    time.sleep(0.2)
                
                record_data = self._fetch_pubchem_record(inchi_key, pubchem_fields, client)
                if not record_data.empty:
                    all_records.append(record_data)
                    
            except Exception as e:
                logger.error(f"Failed to fetch PubChem data for InChI Key {inchi_key}: {e}")
                continue
        
        if not all_records:
            logger.warning("No PubChem data extracted")
            return pd.DataFrame()
        
        # Combine all records
        result = pd.concat(all_records, ignore_index=True)
        
        logger.info(f"PubChem extraction completed: {len(result)} records")
        return result
    
    def _fetch_pubchem_record(self, inchi_key: str, fields: list[str], client) -> pd.DataFrame:
        """Fetch a single record from PubChem API using InChI Key."""
        import requests
        
        # Build URL for PubChem PUG-REST API
        base_url = self.config.sources["pubchem"].http.base_url
        url = f"{base_url}/compound/inchikey/{inchi_key}/property/MolecularFormula,MolecularWeight,ConnectivitySMILES,InChI,InChIKey/JSON"
        
        try:
            # Make request
            response = client.get(url, timeout=self.config.sources["pubchem"].http.timeout_sec)
            response.raise_for_status()
            
            data = response.json()
            if "PropertyTable" not in data or "Properties" not in data["PropertyTable"]:
                logger.warning("No properties found in PubChem response for InChI Key: %s", inchi_key)
                return pd.DataFrame()
            
            properties = data["PropertyTable"]["Properties"]
            if not properties:
                return pd.DataFrame()
            
            # Convert to DataFrame
            record = properties[0]  # Take first property set
            
            # Map PubChem fields to our schema
            mapped_record = {
                "inchi_key_from_mol": inchi_key,
                "pubchem_cid": record.get("CID"),
                "pubchem_molecular_formula": record.get("MolecularFormula"),
                "pubchem_molecular_weight": record.get("MolecularWeight"),
                "pubchem_canonical_smiles": record.get("ConnectivitySMILES"),
                "pubchem_inchi": record.get("InChI"),
                "pubchem_inchi_key": record.get("InChIKey"),
                "pubchem_registry_id": None,  # Not available in this API endpoint
                "pubchem_rn": None  # Not available in this API endpoint
            }
            
            df = pd.DataFrame([mapped_record])
            return df
            
        except requests.RequestException as e:
            logger.error(f"PubChem API request failed for InChI Key {inchi_key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to process PubChem response for InChI Key {inchi_key}: {e}")
            raise
    
    def _merge_chembl_data(self, base_data: pd.DataFrame, chembl_data: pd.DataFrame) -> pd.DataFrame:
        """Merge ChEMBL data into base data."""
        if chembl_data.empty:
            logger.warning("No ChEMBL data to merge")
            return base_data
        
        # Remove duplicates from ChEMBL data (keep first occurrence)
        chembl_data = chembl_data.drop_duplicates(subset=["molecule_chembl_id"], keep="first")
        
        # Create a mapping from molecule_chembl_id to ChEMBL data
        chembl_dict = chembl_data.set_index("molecule_chembl_id").to_dict("index")
        
        # Start with base data
        merged_data = base_data.copy()
        
        # For each row in base data, enrich with ChEMBL data
        for idx, row in merged_data.iterrows():
            molecule_id = row["molecule_chembl_id"]
            if molecule_id in chembl_dict:
                chembl_row = chembl_dict[molecule_id]
                
                # Update existing columns with ChEMBL data (prefer non-null ChEMBL values)
                for col, value in chembl_row.items():
                    if col in merged_data.columns:
                        # Only update if ChEMBL value is not null and not empty
                        if self._is_valid_value(value):
                            # Convert value to appropriate type if needed
                            try:
                                # Try to maintain the original column dtype
                                original_dtype = merged_data[col].dtype
                                if original_dtype == 'float64' and isinstance(value, str):
                                    # Try to convert string to float
                                    try:
                                        converted_value = float(value)
                                        merged_data.at[idx, col] = converted_value
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep original value
                                        pass
                                elif original_dtype == 'int64' and isinstance(value, str):
                                    # Try to convert string to int
                                    try:
                                        converted_value = int(float(value))  # int(float()) handles "123.0"
                                        merged_data.at[idx, col] = converted_value
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep original value
                                        pass
                                else:
                                    merged_data.at[idx, col] = value
                            except (ValueError, TypeError):
                                # If type conversion fails, keep original value
                                pass
                    else:
                        # Add new column if it doesn't exist
                        if col not in merged_data.columns:
                            merged_data[col] = None
                        try:
                            merged_data.at[idx, col] = value
                        except (ValueError, TypeError):
                            # If type conversion fails, convert to string
                            merged_data.at[idx, col] = str(value)
        
        # Debug: log what columns we have after merge
        logger.debug("Columns after ChEMBL merge: %s", str(list(merged_data.columns)))
        if "parent_chembl_id" in merged_data.columns:
            logger.debug("parent_chembl_id values: %s", str(merged_data['parent_chembl_id'].tolist()))
        else:
            logger.debug("parent_chembl_id column not found after merge")
        
        # Log statistics
        enriched_count = 0
        if "molregno" in merged_data.columns:
            enriched_count = merged_data["molregno"].notna().sum()
        elif "molecule_chembl_id" in merged_data.columns:
            # Count records that have ChEMBL data by checking if any ChEMBL field is not null
            chembl_fields = ["parent_chembl_id", "mechanism_of_action", "direct_interaction"]
            for field in chembl_fields:
                if field in merged_data.columns:
                    enriched_count = merged_data[field].notna().sum()
                    break
        
        total_count = len(merged_data)
        enrichment_rate = enriched_count / total_count if total_count > 0 else 0
        
        # Ensure numeric columns maintain their proper dtypes
        numeric_columns = [
            "molregno", "parent_molregno", "max_phase", "mw_freebase", "alogp",
            "hba", "hbd", "psa", "rtb", "ro3_pass", "num_ro5_violations",
            "acd_most_apka", "acd_most_bpka", "acd_logp", "acd_logd",
            "full_mwt", "aromatic_rings", "heavy_atoms", "qed_weighted",
            "mw_monoisotopic", "hba_lipinski", "hbd_lipinski", 
            "num_lipinski_ro5_violations", "first_approval", "usan_year",
            "withdrawn_year", "pubchem_molecular_weight", "pubchem_cid"
        ]
        
        for col in numeric_columns:
            if col in merged_data.columns:
                try:
                    merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce')
                except (ValueError, TypeError):
                    # If conversion fails, keep as is
                    pass
        
        logger.info("ChEMBL data merge completed: %d/%d records enriched (%.1f%%)", int(enriched_count), int(total_count), float(enrichment_rate * 100))
        
        return merged_data
    
    def _merge_pubchem_data(self, base_data: pd.DataFrame, pubchem_data: pd.DataFrame) -> pd.DataFrame:
        """Merge PubChem data into base data."""
        if pubchem_data.empty:
            logger.warning("No PubChem data to merge")
            return base_data
        
        # Remove duplicates from PubChem data (keep first occurrence)
        pubchem_data = pubchem_data.drop_duplicates(subset=["inchi_key_from_mol"], keep="first")
        
        # Perform left join on InChI Key
        merged_data = base_data.merge(
            pubchem_data, 
            left_on="standard_inchi_key",
            right_on="inchi_key_from_mol", 
            how="left", 
            suffixes=("", "_pubchem")
        )
        
        # Log statistics
        enriched_count = merged_data["pubchem_cid"].notna().sum()
        total_count = len(merged_data)
        enrichment_rate = enriched_count / total_count if total_count > 0 else 0
        
        logger.info("PubChem data merge completed: %d/%d records enriched (%.1f%%)", int(enriched_count), int(total_count), float(enrichment_rate * 100))
        
        return merged_data
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Normalize testitem data."""
        logger.info("Normalizing testitem data")
        
        # Apply testitem normalization
        normalized_data = self.normalizer.normalize_testitems(raw_data)
        
        logger.info(f"Normalized {len(normalized_data)} testitems")
        return normalized_data
    
    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate testitem data."""
        logger.info("Validating testitem data")
        
        # Validate normalized data
        validated_data = self.validator.validate_normalized(data)
        
        logger.info(f"Validated {len(validated_data)} testitems")
        return validated_data
    
    def filter_quality(self, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter testitems by quality."""
        logger.info("Filtering testitems by quality")
        
        # Apply quality filters
        accepted_data, rejected_data = self.quality_filter.apply_moderate_quality_filter(data)
        
        logger.info(f"Quality filtering: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _get_entity_type(self) -> str:
        """Получить тип сущности для пайплайна."""
        return "testitems"
    
    def _create_qc_validator(self) -> Any:
        """Создать QC валидатор для пайплайна."""
        from library.common.qc_profiles import QCProfile, TestitemQCValidator
        
        # Создаем базовый QC профиль для теститемов
        qc_profile = QCProfile(
            name="testitem_qc",
            description="Quality control profile for testitems",
            rules=[]
        )
        
        return TestitemQCValidator(qc_profile)
    
    def _create_postprocessor(self) -> Any:
        """Создать постпроцессор для пайплайна."""
        from library.common.postprocess_base import TestitemPostprocessor
        return TestitemPostprocessor(self.config)
    
    def _create_etl_writer(self) -> Any:
        """Создать ETL writer для пайплайна."""
        from library.common.writer_base import create_etl_writer
        return create_etl_writer(self.config, "testitems")
    
    def _build_metadata(
        self, 
        data: pd.DataFrame, 
        accepted_data: pd.DataFrame | None = None, 
        rejected_data: pd.DataFrame | None = None,
        correlation_analysis: dict[str, Any] | None = None,
        correlation_insights: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Build metadata for testitem pipeline."""
        # Use accepted_data as data if not provided
        if accepted_data is None:
            accepted_data = data
        if rejected_data is None:
            rejected_data = pd.DataFrame()
        
        # Create base metadata dictionary
        current_time = pd.Timestamp.now().isoformat()
        
        # Calculate source statistics
        source_counts = {}
        if accepted_data is not None and not accepted_data.empty:
            # ChEMBL enrichment - check for any ChEMBL data
            chembl_enriched = 0
            if "molecule_chembl_id" in accepted_data.columns:
                # Count records that have molecule_chembl_id (main indicator of ChEMBL data)
                chembl_enriched = accepted_data["molecule_chembl_id"].notna().sum()
            source_counts["chembl"] = chembl_enriched
            
            # PubChem enrichment
            pubchem_enriched = accepted_data["pubchem_cid"].notna().sum() if "pubchem_cid" in accepted_data.columns else 0
            source_counts["pubchem"] = pubchem_enriched
        
        # PubChem enrichment statistics
        pubchem_enrichment = {
            "enabled": getattr(self.config.sources["pubchem"], 'enabled', True),
            "enrichment_rate": 0.0,
            "records_with_pubchem_data": 0
        }
        
        if accepted_data is not None and not accepted_data.empty and "pubchem_cid" in accepted_data.columns:
            total_records = len(accepted_data)
            pubchem_records = accepted_data["pubchem_cid"].notna().sum()
            pubchem_enrichment["enrichment_rate"] = pubchem_records / total_records if total_records > 0 else 0.0
            pubchem_enrichment["records_with_pubchem_data"] = int(pubchem_records)
        
        metadata = {
            "pipeline": {
                "pipeline_version": "2.0.0",
                "config": self.config.model_dump() if hasattr(self.config, 'model_dump') else {},
                "source_counts": source_counts,
                "pubchem_enrichment": pubchem_enrichment
            },
            "execution": {
                "run_id": f"testitem_run_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}",
                "pipeline_name": "testitems",
                "pipeline_version": "2.0.0",
                "entity_type": "testitems",
                "sources_enabled": [name for name, source in self.config.sources.items() if source.enabled],
                "started_at": current_time,
                "completed_at": current_time,
                "extraction_timestamp": current_time,
                "config": self.config.model_dump() if hasattr(self.config, 'model_dump') else {},
            },
            "data": {
                "total_testitems": len(accepted_data) if accepted_data is not None else 0,
                "accepted_testitems": len(accepted_data) if accepted_data is not None else 0,
                "rejected_testitems": len(rejected_data) if rejected_data is not None else 0,
                "columns": list(accepted_data.columns) if accepted_data is not None and not accepted_data.empty else [],
            },
            "validation": {
                "validation_passed": True,
                "quality_filter_passed": len(rejected_data) == 0 if rejected_data is not None else True,
            }
        }
        
        # Add correlation analysis if provided
        if correlation_analysis is not None:
            metadata["correlation_analysis"] = correlation_analysis
        if correlation_insights is not None:
            metadata["correlation_insights"] = correlation_insights
        
        return metadata


# Import required modules - removed circular import
