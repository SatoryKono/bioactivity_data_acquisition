"""Data extraction stage for testitem ETL pipeline."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from library.clients.chembl import TestitemChEMBLClient
from library.clients.pubchem import PubChemClient
from library.testitem.config import TestitemConfig

logger = logging.getLogger(__name__)


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
        
        # S04: Fetch properties and classifications
        logger.debug(f"S04: Fetching properties and classifications for {molecule_chembl_id}")
        properties_data = client.fetch_molecule_properties(molecule_chembl_id)
        result.update(properties_data)
        
        # Fetch mechanism data
        mechanism_data = client.fetch_mechanism(molecule_chembl_id)
        result.update(mechanism_data)
        
        # Fetch ATC classification
        atc_data = client.fetch_atc_classification(molecule_chembl_id)
        result.update(atc_data)
        
        # S06: Fetch synonyms and cross-references
        logger.debug(f"S06: Fetching synonyms and xrefs for {molecule_chembl_id}")
        synonym_data = client.fetch_compound_synonym(molecule_chembl_id)
        result.update(synonym_data)
        
        xref_data = client.fetch_xref_source(molecule_chembl_id)
        result.update(xref_data)
        
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
    """Extract data for a batch of molecules."""
    
    logger.info(f"Extracting data for {len(input_data)} molecules")
    
    extracted_data = []
    
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
            
            # Extract ChEMBL data
            molecule_data = extract_molecule_data(chembl_client, molecule_chembl_id, config)
            
            # Enrich with PubChem data
            enriched_data = extract_pubchem_data(pubchem_client, molecule_data, config)
            
            # Add any additional input data
            for col in input_data.columns:
                if col not in enriched_data and pd.notna(row[col]):
                    enriched_data[col] = row[col]
            
            extracted_data.append(enriched_data)
            
        except Exception as e:
            logger.error(f"Error processing row {idx}: {e}")
            # Create error record
            error_data = {
                "molecule_chembl_id": row.get("molecule_chembl_id"),
                "molregno": row.get("molregno"),
                "source_system": "ChEMBL",
                "extracted_at": pd.Timestamp.utcnow().isoformat() + "Z",
                "error": str(e)
            }
            extracted_data.append(error_data)
    
    logger.info(f"Successfully extracted data for {len(extracted_data)} molecules")
    
    return pd.DataFrame(extracted_data)
