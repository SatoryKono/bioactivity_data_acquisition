"""Data extraction stage for testitem ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import TestitemChEMBLClient
from library.clients.pubchem import PubChemClient
from library.testitem.config import TestitemConfig

logger = logging.getLogger(__name__)


def extract_molecules_batch(
    client: TestitemChEMBLClient,
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
    client: TestitemChEMBLClient,
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
    
    # Try to get PubChem CID from various sources
    pubchem_cid = None
    
    # Check if CID is already provided in input
    if "pubchem_cid" in molecule_data and molecule_data["pubchem_cid"]:
        pubchem_cid = str(molecule_data["pubchem_cid"])
    # Check if we can get CID from molecule name
    elif "pref_name" in molecule_data and molecule_data["pref_name"]:
        try:
            logger.debug(f"S05: Fetching PubChem CID for name: {molecule_data['pref_name']}")
            cid_data = client.fetch_compound_by_name(molecule_data["pref_name"])
            cids = cid_data.get("pubchem_cids", [])
            if cids:
                pubchem_cid = str(cids[0])  # Take the first CID
        except Exception as e:
            logger.warning(f"Failed to get PubChem CID for name {molecule_data['pref_name']}: {e}")
    
    if not pubchem_cid:
        logger.debug("No PubChem CID available for enrichment")
        return molecule_data
    
    logger.info(f"S05: Enriching with PubChem data for CID: {pubchem_cid}")
    
    try:
        # Fetch compound properties
        properties_data = client.fetch_compound_properties(pubchem_cid)
        molecule_data.update(properties_data)
        
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


def extract_batch_data(
    chembl_client: TestitemChEMBLClient,
    pubchem_client: PubChemClient,
    input_data: pd.DataFrame,
    config: TestitemConfig
) -> pd.DataFrame:
    """Extract data for a batch of molecules using optimized batch requests."""
    
    logger.info(f"Extracting data for {len(input_data)} molecules")
    
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
                # If we only have molregno, we need to find the molecule_chembl_id
                # This would require an additional API call to resolve molregno to molecule_chembl_id
                logger.warning(f"Row {idx} has molregno but no molecule_chembl_id - skipping")
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
    
    # Use batch extraction for better performance
    logger.info(f"Using batch extraction for {len(molecule_chembl_ids)} molecules")
    batch_results = extract_molecules_batch(chembl_client, molecule_chembl_ids, config)
    
    extracted_data = []
    
    # Process batch results and add PubChem enrichment
    for molecule_data in batch_results:
        try:
            molecule_chembl_id = molecule_data.get("molecule_chembl_id")
            if not molecule_chembl_id:
                continue
            
            # Get original row data
            row_idx = row_mapping.get(molecule_chembl_id)
            if row_idx is not None:
                original_row = input_data.iloc[row_idx]
                
                # Add original input data
                for col in input_data.columns:
                    if col not in molecule_data and pd.notna(original_row[col]):
                        molecule_data[col] = original_row[col]
            
            # Extract PubChem data if enabled
            if config.enable_pubchem:
                pubchem_data = extract_pubchem_data(pubchem_client, molecule_data, config)
                molecule_data.update(pubchem_data)
            
            extracted_data.append(molecule_data)
            
        except Exception as e:
            logger.error(f"Error processing molecule {molecule_chembl_id}: {e}")
            # Create error record
            error_data = {
                "molecule_chembl_id": molecule_chembl_id,
                "source_system": "ChEMBL",
                "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z",
                "error": str(e)
            }
            extracted_data.append(error_data)
    
    logger.info(f"Successfully extracted data for {len(extracted_data)} molecules")
    
    return pd.DataFrame(extracted_data)
