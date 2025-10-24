"""Data extraction stage for testitem ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import ChEMBLClient
from library.clients.pubchem import PubChemClient
from library.testitem.config import TestitemConfig

logger = logging.getLogger(__name__)


def extract_molecules_batch(
    client: ChEMBLClient,
    molecule_chembl_ids: list[str],
    config: TestitemConfig
) -> list[dict[str, Any]]:
    """Extract comprehensive molecule data from ChEMBL API using batch requests."""
    
    logger.info(f"Extracting data for {len(molecule_chembl_ids)} molecules using batch requests")
    
    results = []
    
    try:
        # S02: Fetch molecule core data in batch
        logger.debug(f"S02: Fetching molecule core data for {len(molecule_chembl_ids)} molecules")
        molecule_data_batch = client.fetch_molecules_batch(molecule_chembl_ids)
        
        # Note: Properties are now included in main endpoint, no need for separate batch request
        
        # Process each molecule
        for molecule_chembl_id in molecule_chembl_ids:
            result = {
                "molecule_chembl_id": molecule_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z"
            }
            
            # Add core molecule data
            if molecule_chembl_id in molecule_data_batch:
                result.update(molecule_data_batch[molecule_chembl_id])
            
            # Properties are now included in main endpoint data
            
            # Fetch additional data individually (only for data not included in main endpoint)
            try:
                # S03: Fetch parent/child relationship data
                molecule_form_data = client.fetch_molecule_form(molecule_chembl_id)
                result.update(molecule_form_data)
                
                # Fetch mechanism data
                mechanism_data = client.fetch_mechanism(molecule_chembl_id)
                result.update(mechanism_data)
                
                # Fetch ATC classification data
                atc_data = client.fetch_atc_classification(molecule_chembl_id)
                result.update(atc_data)
                
                # Note: Synonyms, properties, structures, and cross-references are now included in main endpoint
                # No need for separate requests for these
                
                # Fetch drug data
                drug_data = client.fetch_drug(molecule_chembl_id)
                result.update(drug_data)
                
                # Fetch drug warnings
                warning_data = client.fetch_drug_warning(molecule_chembl_id)
                result.update(warning_data)
                
            except Exception as e:
                logger.warning(f"Failed to fetch additional data for {molecule_chembl_id}: {e}")
            
            results.append(result)
        
        logger.info(f"Successfully extracted data for {len(results)} molecules")
        return results
        
    except Exception as e:
        logger.error(f"Failed to extract molecules batch: {e}")
        # Fallback to individual extraction
        logger.info("Falling back to individual molecule extraction")
        results = []
        for molecule_chembl_id in molecule_chembl_ids:
            result = extract_molecule_data(client, molecule_chembl_id, config)
            results.append(result)
        return results


def extract_molecule_data(
    client: ChEMBLClient,
    molecule_chembl_id: str,
    config: TestitemConfig
) -> dict[str, Any]:
    """Extract comprehensive molecule data from ChEMBL API."""
    
    logger.info(f"Extracting data for molecule: {molecule_chembl_id}")
    
    # Initialize result with basic molecule data
    result = {
        "molecule_chembl_id": molecule_chembl_id,
        "source_system": "ChEMBL",
        "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z"
    }
    
    try:
        # S02: Fetch molecule core data
        logger.debug(f"S02: Fetching molecule core data for {molecule_chembl_id}")
        molecule_data = client.fetch_molecule(molecule_chembl_id)
        result.update(molecule_data)
        
        # S03: Fetch parent/child relationship data
        logger.debug(f"S03: Fetching parent/child data for {molecule_chembl_id}")
        molecule_form_data = client.fetch_molecule_form(molecule_chembl_id)
        result.update(molecule_form_data)
        
        # Note: Properties, structures, synonyms, and cross-references are now included in main endpoint
        # No need for separate requests for these
        
        # Fetch mechanism data
        mechanism_data = client.fetch_mechanism(molecule_chembl_id)
        result.update(mechanism_data)
        
        # Fetch ATC classification
        atc_data = client.fetch_atc_classification(molecule_chembl_id)
        result.update(atc_data)
        
        # Fetch drug data
        drug_data = client.fetch_drug(molecule_chembl_id)
        result.update(drug_data)
        
        # Fetch drug warnings
        warning_data = client.fetch_drug_warning(molecule_chembl_id)
        result.update(warning_data)
        
        logger.info(f"Successfully extracted ChEMBL data for {molecule_chembl_id}")
        
    except Exception as e:
        logger.error(f"Error extracting ChEMBL data for {molecule_chembl_id}: {e}")
        result["error"] = str(e)
    
    return result


def extract_pubchem_data(
    client: PubChemClient,
    molecule_data: dict[str, Any],
    config: TestitemConfig
) -> dict[str, Any]:
    """Extract PubChem data for molecule enrichment."""
    
    if not config.enable_pubchem:
        logger.debug("PubChem enrichment disabled")
        return molecule_data
    
    # Try to get PubChem CID from various sources (priority): xref -> InChIKey -> SMILES -> pref_name
    pubchem_cid = None

    # Already provided
    if "pubchem_cid" in molecule_data and molecule_data["pubchem_cid"]:
        pubchem_cid = str(molecule_data["pubchem_cid"]) if molecule_data["pubchem_cid"] is not None else None

    # From xref_sources (ChEMBL)
    if pubchem_cid is None and "xref_sources" in molecule_data and molecule_data["xref_sources"]:
        try:
            for xref in molecule_data.get("xref_sources", []) or []:
                name = (xref.get("xref_name") or "").lower()
                src = (xref.get("xref_src") or "").lower()
                xref_id = xref.get("xref_id")
                if xref_id and ("pubchem" in name or "pubchem" in src):
                    digits = "".join(ch for ch in str(xref_id) if ch.isdigit())
                    if digits:
                        pubchem_cid = digits
                        break
        except Exception as e:
            logger.debug(f"Failed parsing xref_sources for PubChem CID: {e}")

    # By InChIKey
    if pubchem_cid is None:
        inchikey = None
        for key_field in ("pubchem_inchi_key", "standard_inchikey", "inchikey"):
            if key_field in molecule_data and molecule_data[key_field]:
                inchikey = str(molecule_data[key_field]).strip()
                break
        if inchikey and len(inchikey) == 27:
            try:
                cid_data = client.fetch_cids_by_inchikey(inchikey)
                cids = cid_data.get("pubchem_cids", [])
                if cids:
                    pubchem_cid = str(cids[0])
            except Exception as e:
                logger.debug(f"Failed PubChem CID by InChIKey {inchikey}: {e}")

    # By SMILES (try multiple candidates)
    if pubchem_cid is None:
        candidate_smiles: list[str] = []
        for smi_field in (
            "pubchem_canonical_smiles",
            "pubchem_isomeric_smiles",
            "standard_smiles",
            "canonical_smiles",
            "smiles",
        ):
            if smi_field in molecule_data and molecule_data[smi_field]:
                val = str(molecule_data[smi_field]).strip()
                if val and val not in candidate_smiles:
                    candidate_smiles.append(val)
        for smi in candidate_smiles:
            try:
                cid_data = client.fetch_cids_by_smiles(smi)
                cids = cid_data.get("pubchem_cids", [])
                if cids:
                    pubchem_cid = str(cids[0])
                    break
            except Exception as e:
                logger.debug("Failed PubChem CID by SMILES candidate: %s", e)

    # By name
    if pubchem_cid is None and "pref_name" in molecule_data and molecule_data["pref_name"]:
        try:
            logger.debug(f"S05: Fetching PubChem CID for name: {molecule_data['pref_name']}")
            cid_data = client.fetch_compound_by_name(molecule_data["pref_name"])
            cids = cid_data.get("pubchem_cids", [])
            if cids:
                pubchem_cid = str(cids[0])
        except Exception as e:
            logger.debug(f"Failed to get PubChem CID for name {molecule_data['pref_name']}: {e}")
    
    if not pubchem_cid:
        logger.debug("No PubChem CID available for enrichment")
        return molecule_data
    
    logger.info(f"S05: Enriching with PubChem data for CID: {pubchem_cid}")
    
    try:
        # Fetch compound properties
        properties_data = client.fetch_compound_properties(pubchem_cid)
        molecule_data.update(properties_data)
        # If SMILES still missing, try record fallback
        if (
            not molecule_data.get("pubchem_canonical_smiles")
            or not molecule_data.get("pubchem_isomeric_smiles")
        ):
            record_smiles = client.fetch_record_smiles(pubchem_cid)
            if record_smiles:
                molecule_data.update(record_smiles)
        
        # Fetch cross-references
        xref_data = client.fetch_compound_xrefs(pubchem_cid)
        molecule_data.update(xref_data)
        
        # Fetch synonyms
        synonym_data = client.fetch_compound_synonyms(pubchem_cid)
        molecule_data.update(synonym_data)
        
        # Store the CID for reference
        molecule_data["pubchem_cid"] = pubchem_cid
        
        logger.info(f"Successfully enriched with PubChem data for CID: {pubchem_cid}")
        
    except Exception as e:
        logger.warning(f"Error enriching with PubChem data for CID {pubchem_cid}: {e}")
        molecule_data["pubchem_error"] = str(e)
    
    return molecule_data


def extract_pubchem_data_batch(
    client: PubChemClient,
    molecules_data: list[dict[str, Any]],
    config: TestitemConfig
) -> list[dict[str, Any]]:
    """Extract PubChem data for batch of molecules."""
    
    if not config.enable_pubchem:
        logger.debug("PubChem enrichment disabled")
        return molecules_data
    
    # Collect CIDs for batch processing
    cids_to_fetch = []
    cid_mapping = {}  # Map CID to molecule index
    
    for idx, molecule_data in enumerate(molecules_data):
        # Try to get PubChem CID from various sources
        pubchem_cid = None
        
        # Already provided
        if "pubchem_cid" in molecule_data and molecule_data["pubchem_cid"]:
            pubchem_cid = str(molecule_data["pubchem_cid"])
        
        # From xref_sources (ChEMBL)
        if pubchem_cid is None and "xref_sources" in molecule_data and molecule_data["xref_sources"]:
            try:
                for xref in molecule_data.get("xref_sources", []) or []:
                    name = (xref.get("xref_name") or "").lower()
                    src = (xref.get("xref_src") or "").lower()
                    xref_id = xref.get("xref_id")
                    if xref_id and ("pubchem" in name or "pubchem" in src):
                        digits = "".join(ch for ch in str(xref_id) if ch.isdigit())
                        if digits:
                            pubchem_cid = digits
                            break
            except Exception as e:
                logger.debug(f"Failed parsing xref_sources for PubChem CID: {e}")
        
        # By InChIKey
        if pubchem_cid is None:
            inchikey = None
            for key_field in ("pubchem_inchi_key", "standard_inchikey", "inchikey"):
                if key_field in molecule_data and molecule_data[key_field]:
                    inchikey = str(molecule_data[key_field]).strip()
                    break
            if inchikey and len(inchikey) == 27:
                try:
                    cid_data = client.fetch_cids_by_inchikey(inchikey)
                    cids = cid_data.get("pubchem_cids", [])
                    if cids:
                        pubchem_cid = str(cids[0])
                except Exception as e:
                    logger.debug(f"Failed PubChem CID by InChIKey {inchikey}: {e}")
        
        # By SMILES
        if pubchem_cid is None:
            candidate_smiles = []
            for smi_field in (
                "pubchem_canonical_smiles", "pubchem_isomeric_smiles",
                "standard_smiles", "canonical_smiles", "smiles"
            ):
                if smi_field in molecule_data and molecule_data[smi_field]:
                    val = str(molecule_data[smi_field]).strip()
                    if val and val not in candidate_smiles:
                        candidate_smiles.append(val)
            for smi in candidate_smiles:
                try:
                    cid_data = client.fetch_cids_by_smiles(smi)
                    cids = cid_data.get("pubchem_cids", [])
                    if cids:
                        pubchem_cid = str(cids[0])
                        break
                except Exception as e:
                    logger.debug("Failed PubChem CID by SMILES candidate: %s", e)
        
        # By name
        if pubchem_cid is None and "pref_name" in molecule_data and molecule_data["pref_name"]:
            try:
                cid_data = client.fetch_compound_by_name(molecule_data["pref_name"])
                cids = cid_data.get("pubchem_cids", [])
                if cids:
                    pubchem_cid = str(cids[0])
            except Exception as e:
                logger.debug(f"Failed to get PubChem CID for name {molecule_data['pref_name']}: {e}")
        
        if pubchem_cid:
            cids_to_fetch.append(pubchem_cid)
            cid_mapping[pubchem_cid] = idx
    
    if not cids_to_fetch:
        logger.debug("No PubChem CIDs available for batch enrichment")
        return molecules_data
    
    logger.info(f"Enriching {len(cids_to_fetch)} molecules with PubChem data")
    
    # Batch fetch properties
    try:
        pubchem_batch_size = getattr(config.runtime, "pubchem_batch_size", 100)
        pubchem_props = client.fetch_compounds_properties_batch(cids_to_fetch, pubchem_batch_size)
        
        # Update molecules with PubChem data
        for cid, molecule_idx in cid_mapping.items():
            if cid in pubchem_props:
                molecules_data[molecule_idx].update(pubchem_props[cid])
                molecules_data[molecule_idx]["pubchem_cid"] = cid
        
        logger.info(f"Successfully enriched {len(pubchem_props)} molecules with PubChem properties")
        
    except Exception as e:
        logger.warning(f"Failed to enrich molecules with PubChem data: {e}")
    
    return molecules_data


def extract_batch_data(
    chembl_client: ChEMBLClient,
    pubchem_client: PubChemClient,
    input_data: pd.DataFrame,
    config: TestitemConfig
) -> pd.DataFrame:
    """Extract data for a batch of molecules using streaming batch processing."""
    
    logger.info(f"Extracting data for {len(input_data)} molecules using streaming batches")
    
    # Collect all molecule_chembl_ids for batch processing
    molecule_chembl_ids = []
    row_mapping = {}  # Map molecule_chembl_id to row index
    
    for idx, row in input_data.iterrows():
        try:
            # Get molecule identifier
            molecule_chembl_id = None
            if "molecule_chembl_id" in row and pd.notna(row["molecule_chembl_id"]):
                molecule_chembl_id = str(row["molecule_chembl_id"]).strip()
            elif "molregno" in row and pd.notna(row["molregno"]):
                # Resolve molregno to molecule_chembl_id via ChEMBL
                try:
                    resolved = chembl_client.resolve_molregno_to_chembl_id(row["molregno"])  # type: ignore[attr-defined]
                except AttributeError:
                    resolved = None
                except Exception as e:
                    logger.warning(f"Failed to resolve molregno for row {idx}: {e}")
                    resolved = None
                if resolved:
                    molecule_chembl_id = str(resolved).strip()
                else:
                    logger.warning(f"Row {idx} has molregno but could not resolve molecule_chembl_id - skipping")
                    continue
            
            if not molecule_chembl_id:
                logger.warning(f"Row {idx} has no valid molecule identifier - skipping")
                continue
            
            molecule_chembl_ids.append(molecule_chembl_id)
            row_mapping[molecule_chembl_id] = idx
            
        except Exception as e:
            logger.warning(f"Error processing row {idx}: {e}")
            continue
    
    if not molecule_chembl_ids:
        logger.warning("No valid molecule identifiers found")
        return pd.DataFrame()
    
    # Use streaming batch processing
    batch_size = getattr(config.runtime, "batch_size", 50)
    total_rows = 0
    batches = []
    batch_index = 0
    
    logger.info(f"Using streaming batch processing with batch_size={batch_size}")
    
    for requested_ids, molecules_batch in chembl_client.fetch_molecules_batch_streaming(molecule_chembl_ids, batch_size):
        batch_index += 1
        
        # Enrich batch with additional ChEMBL data
        try:
            mechanism_data = chembl_client.fetch_mechanism_batch(requested_ids)
            atc_data = chembl_client.fetch_atc_classification_batch(requested_ids)
            drug_data = chembl_client.fetch_drug_batch(requested_ids)
            warning_data = chembl_client.fetch_drug_warning_batch(requested_ids)
        except Exception as e:
            logger.warning(f"Failed to fetch additional ChEMBL data for batch {batch_index}: {e}")
            mechanism_data = {}
            atc_data = {}
            drug_data = {}
            warning_data = {}
        
        # Process each molecule in the batch
        batch_results = []
        for molecule_data in molecules_batch:
            try:
                molecule_chembl_id = molecule_data.get("molecule_chembl_id")
                if not molecule_chembl_id:
                    continue
                
                # Add additional ChEMBL data
                molecule_data.update(mechanism_data.get(molecule_chembl_id, {}))
                molecule_data.update(atc_data.get(molecule_chembl_id, {}))
                molecule_data.update(drug_data.get(molecule_chembl_id, {}))
                molecule_data.update(warning_data.get(molecule_chembl_id, {}))
                
                # Get original row data
                row_idx = row_mapping.get(molecule_chembl_id)
                if row_idx is not None:
                    original_row = input_data.iloc[row_idx]
                    
                    # Add original input data
                    for col in input_data.columns:
                        if col not in molecule_data and pd.notna(original_row[col]):
                            molecule_data[col] = original_row[col]
                
                batch_results.append(molecule_data)
                
            except Exception as e:
                logger.error(f"Error processing molecule {molecule_chembl_id}: {e}")
                # Create error record
                error_data = {
                    "molecule_chembl_id": molecule_chembl_id,
                    "source_system": "ChEMBL",
                    "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z",
                    "error": str(e)
                }
                batch_results.append(error_data)
        
        # PubChem enrichment for this batch
        if config.enable_pubchem and pubchem_client:
            try:
                batch_results = extract_pubchem_data_batch(pubchem_client, batch_results, config)
            except Exception as e:
                logger.warning(f"Failed to enrich batch {batch_index} with PubChem data: {e}")
        
        # Convert to DataFrame and add to batches
        batch_df = pd.DataFrame(batch_results)
        total_rows += len(batch_df)
        batches.append(batch_df)
        
        logger.info(f"Processed batch {batch_index}: size={len(batch_df)}, total_rows={total_rows}")
        
        # Check limit
        if config.runtime.limit is not None and total_rows >= config.runtime.limit:
            logger.info(f"Reached global limit of {config.runtime.limit} molecules")
            break
    
    if not batches:
        logger.info("No molecules processed in streaming mode")
        return pd.DataFrame()
    
    result_df = pd.concat(batches, ignore_index=True)
    if config.runtime.limit is not None and len(result_df) > config.runtime.limit:
        result_df = result_df.head(config.runtime.limit)
    
    logger.info(f"Successfully processed {len(result_df)} molecules in {batch_index} batches")
    
    return result_df
