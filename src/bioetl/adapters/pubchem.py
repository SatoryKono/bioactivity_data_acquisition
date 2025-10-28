"""PubChem PUG-REST API adapter for molecular data enrichment."""

import json
import time
from typing import Any

from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig


class PubChemAdapter(ExternalAdapter):
    """Adapter for PubChem PUG-REST API.

    Implements "Best of Both Worlds" approach:
    - Batch properties fetch (100 CIDs per request)
    - Multi-step CID resolution strategy
    - Rate limiting (5 req/sec)
    - Graceful degradation
    """

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize PubChem adapter."""
        super().__init__(api_config, adapter_config)
        self.last_request_time = 0.0
        self.min_request_interval = 0.2  # 5 requests per second

    def fetch_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch records by identifiers.

        Args:
            ids: List of InChI Keys or other identifiers

        Returns:
            List of PubChem records
        """
        if not ids:
            return []

        # Fetch CIDs first, then properties
        cid_mapping = self._resolve_cids_batch(ids)
        if not cid_mapping:
            return []

        # Fetch properties in batch (max 100 CIDs per request)
        all_records = []
        cids = [cid for cid in cid_mapping.values() if cid]
        batch_size = self.adapter_config.batch_size or 100

        for i in range(0, len(cids), batch_size):
            batch_cids = cids[i : i + batch_size]
            try:
                records = self._fetch_properties_batch(batch_cids)
                all_records.extend(records)
            except Exception as e:
                self.logger.error("batch_properties_failed", batch=i, error=str(e))

        # Map back to original identifiers
        return self._map_records_to_ids(all_records, cid_mapping)

    def _resolve_cids_batch(self, identifiers: list[str]) -> dict[str, int]:
        """Resolve PubChem CIDs from identifiers.

        Args:
            identifiers: List of InChI Keys, SMILES, or names

        Returns:
            Mapping of identifier -> CID
        """
        cid_mapping = {}

        for identifier in identifiers:
            if not identifier:
                continue

            try:
                # Try InChIKey lookup (most reliable)
                cid = self._resolve_cid_by_inchikey(identifier)
                if cid:
                    cid_mapping[identifier] = cid
                    self.logger.debug("cid_resolved", identifier=identifier[:20], cid=cid)
            except Exception as e:
                self.logger.warning("cid_resolution_failed", identifier=identifier[:20], error=str(e))

        self.logger.info("cid_resolution_completed", total=len(identifiers), resolved=len(cid_mapping))
        return cid_mapping

    def _resolve_cid_by_inchikey(self, inchikey: str) -> int | None:
        """Resolve CID by InChIKey.

        Args:
            inchikey: Standard InChI Key

        Returns:
            PubChem CID or None
        """
        if not inchikey:
            return None

        # PubChem InChIKey lookup endpoint
        url = f"/compound/inchikey/{inchikey}/cids/JSON"

        try:
            self._rate_limit()
            response = self.api_client.request_json(url)
            
            if not response:
                return None

            # Extract CID from response
            # Response format: {"IdentifierList": {"CID": [1234567]}}
            if "IdentifierList" in response:
                identifier_list = response["IdentifierList"]
                if "CID" in identifier_list and isinstance(identifier_list["CID"], list):
                    cids = identifier_list["CID"]
                    if cids:
                        return int(cids[0])  # Return first CID

            return None
        except Exception as e:
            self.logger.warning("inchikey_lookup_failed", inchikey=inchikey[:20], error=str(e))
            return None

    def _fetch_properties_batch(self, cids: list[int]) -> list[dict[str, Any]]:
        """Fetch properties for a batch of CIDs.

        Args:
            cids: List of PubChem CIDs (max 100)

        Returns:
            List of property records
        """
        if not cids:
            return []

        # PubChem batch properties endpoint
        cids_str = ",".join(map(str, cids))
        # Note: PubChem returns SMILES (isomeric) and ConnectivitySMILES (canonical)
        # not CanonicalSMILES/IsomericSMILES
        properties = "MolecularFormula,MolecularWeight,SMILES,ConnectivitySMILES,InChI,InChIKey,IUPACName"
        url = f"/compound/cid/{cids_str}/property/{properties}/JSON"

        try:
            self._rate_limit()
            response = self.api_client.request_json(url)

            if not response:
                return []

            # Extract properties from response
            # Response format: {"PropertyTable": {"Properties": [...]}}
            if "PropertyTable" in response:
                property_table = response["PropertyTable"]
                if "Properties" in property_table:
                    return property_table["Properties"]

            return []
        except Exception as e:
            self.logger.error("batch_properties_fetch_failed", cids_count=len(cids), error=str(e))
            return []

    def _map_records_to_ids(
        self,
        records: list[dict[str, Any]],
        cid_mapping: dict[str, int]
    ) -> list[dict[str, Any]]:
        """Map PubChem records back to original identifiers.

        Args:
            records: List of PubChem property records
            cid_mapping: Mapping of identifier -> CID

        Returns:
            List of enriched records with original identifiers
        """
        # Reverse mapping: CID -> identifier
        cid_to_identifier = {cid: identifier for identifier, cid in cid_mapping.items()}

        enriched_records = []
        for record in records:
            cid = record.get("CID")
            if cid and cid in cid_to_identifier:
                # Add original identifier to record
                enriched_record = record.copy()
                enriched_record["_source_identifier"] = cid_to_identifier[cid]
                enriched_records.append(enriched_record)

        return enriched_records

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize PubChem record to standard format.

        Args:
            record: Raw PubChem property record

        Returns:
            Normalized record with pubchem_* prefix
        """
        normalized = {}

        # PubChem CID
        if "CID" in record:
            normalized["pubchem_cid"] = int(record["CID"])

        # Molecular formula
        if "MolecularFormula" in record:
            normalized["pubchem_molecular_formula"] = str(record["MolecularFormula"])

        # Molecular weight
        if "MolecularWeight" in record:
            normalized["pubchem_molecular_weight"] = float(record["MolecularWeight"])

        # Canonical SMILES (without stereochemistry)
        # PubChem returns this as "ConnectivitySMILES"
        if "ConnectivitySMILES" in record:
            normalized["pubchem_canonical_smiles"] = str(record["ConnectivitySMILES"])

        # Isomeric SMILES (with stereochemistry)
        # PubChem returns this as "SMILES"
        if "SMILES" in record:
            normalized["pubchem_isomeric_smiles"] = str(record["SMILES"])

        # InChI
        if "InChI" in record:
            normalized["pubchem_inchi"] = str(record["InChI"])

        # InChI Key
        if "InChIKey" in record:
            normalized["pubchem_inchi_key"] = str(record["InChIKey"])

        # IUPAC name
        if "IUPACName" in record:
            normalized["pubchem_iupac_name"] = str(record["IUPACName"])

        return normalized

    def _rate_limit(self) -> None:
        """Enforce rate limiting: 5 requests per second."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def enrich_with_pubchem(
        self,
        df: Any,
        inchi_key_col: str = "standard_inchi_key"
    ) -> Any:
        """Enrich dataframe with PubChem data.

        Args:
            df: Input DataFrame
            inchi_key_col: Column name containing InChI Keys

        Returns:
            Enriched DataFrame with pubchem_* columns
        """
        import pandas as pd

        if df.empty or inchi_key_col not in df.columns:
            self.logger.warning("enrichment_skipped", reason="empty_or_missing_column")
            return df

        # Get unique InChI Keys
        inchi_keys = df[inchi_key_col].dropna().unique().tolist()
        if not inchi_keys:
            self.logger.warning("enrichment_skipped", reason="no_inchi_keys")
            return df

        self.logger.info("starting_enrichment", inchi_keys_count=len(inchi_keys))

        # Fetch PubChem data
        ids = [str(key) for key in inchi_keys]
        pubchem_df = self.process(ids)

        if pubchem_df.empty:
            self.logger.warning("enrichment_failed", reason="no_pubchem_data")
            return df

        # Map back using InChI Key
        # Create mapping from InChI Key to PubChem data
        pubchem_df['inchi_key_normalized'] = pubchem_df['pubchem_inchi_key'].str.upper()
        
        # Merge with original dataframe
        df_enriched = df.merge(
            pubchem_df,
            left_on=inchi_key_col,
            right_on='inchi_key_normalized',
            how='left',
            suffixes=('', '_pubchem_duplicate')
        )

        # Remove duplicate columns
        df_enriched = df_enriched.loc[:, ~df_enriched.columns.str.endswith('_pubchem_duplicate')]
        df_enriched = df_enriched.drop(columns=['inchi_key_normalized'], errors='ignore')

        enriched_count = df_enriched['pubchem_cid'].notna().sum()
        self.logger.info(
            "enrichment_completed",
            total_rows=len(df_enriched),
            enriched_rows=enriched_count,
            enrichment_rate=enriched_count / len(df_enriched) if len(df_enriched) > 0 else 0.0
        )

        return df_enriched

