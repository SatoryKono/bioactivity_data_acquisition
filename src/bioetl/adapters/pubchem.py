"""PubChem PUG-REST API adapter for molecular data enrichment."""

import json
import time
from datetime import datetime, timezone
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

    _PUBCHEM_COLUMNS: list[str] = [
        "pubchem_cid",
        "pubchem_molecular_formula",
        "pubchem_molecular_weight",
        "pubchem_canonical_smiles",
        "pubchem_isomeric_smiles",
        "pubchem_inchi",
        "pubchem_inchi_key",
        "pubchem_iupac_name",
        "pubchem_registry_id",
        "pubchem_rn",
        "pubchem_synonyms",
        "pubchem_enriched_at",
        "pubchem_cid_source",
        "pubchem_fallback_used",
        "pubchem_enrichment_attempt",
        "pubchem_lookup_inchikey",
    ]

    def __init__(self, api_config: APIConfig, adapter_config: AdapterConfig):
        """Initialize PubChem adapter."""
        super().__init__(api_config, adapter_config)
        self.last_request_time = 0.0
        self.min_request_interval = 0.2  # 5 requests per second

    @staticmethod
    def _canonical_json(value: Any) -> str | None:
        """Serialize value to canonical JSON string."""

        if value in (None, ""):
            return None
        try:
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return None

    def fetch_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        """Fetch records by identifiers.

        Args:
            ids: List of InChI Keys or other identifiers

        Returns:
            List of PubChem records
        """
        if not ids:
            return []

        unique_ids = list(dict.fromkeys([identifier for identifier in ids if identifier]))
        if not unique_ids:
            return []

        resolution = self._resolve_cids_batch(unique_ids)

        # Fetch properties in batch (max 100 CIDs per request)
        cid_to_properties: dict[int, dict[str, Any]] = {}
        resolved_cids = [info["cid"] for info in resolution.values() if info.get("cid")]
        if resolved_cids:
            batch_size = self.adapter_config.batch_size or 100
            for i in range(0, len(resolved_cids), batch_size):
                batch_cids = resolved_cids[i : i + batch_size]
                try:
                    for record in self._fetch_properties_batch(batch_cids):
                        cid = record.get("CID")
                        if cid is not None:
                            cid_to_properties[int(cid)] = record
                except Exception as exc:  # noqa: BLE001
                    self.logger.error("batch_properties_failed", batch=i, error=str(exc))

        results: list[dict[str, Any]] = []
        for identifier in unique_ids:
            info = resolution.get(
                identifier,
                {"cid": None, "cid_source": "failed", "attempt": 1, "fallback_used": False},
            )
            cid = info.get("cid")
            base_record = cid_to_properties.get(cid, {}).copy() if cid else {}
            if cid is not None:
                base_record.setdefault("CID", cid)
            base_record["_source_identifier"] = identifier
            base_record["_cid_source"] = info.get("cid_source", "inchikey" if cid else "failed")
            base_record["_enrichment_attempt"] = info.get("attempt", 1)
            base_record["_fallback_used"] = info.get("fallback_used", False)
            results.append(base_record)

        return results

    def _resolve_cids_batch(self, identifiers: list[str]) -> dict[str, dict[str, Any]]:
        """Resolve PubChem CIDs from identifiers.

        Args:
            identifiers: List of InChI Keys, SMILES, or names

        Returns:
            Mapping of identifier -> resolution metadata
        """
        cid_mapping: dict[str, dict[str, Any]] = {}

        for identifier in identifiers:
            if not identifier:
                continue

            metadata: dict[str, Any] = {
                "cid": None,
                "cid_source": "failed",
                "attempt": 1,
                "fallback_used": False,
            }

            try:
                cid = self._resolve_cid_by_inchikey(identifier)
                if cid:
                    metadata["cid"] = cid
                    metadata["cid_source"] = "inchikey"
                    self.logger.debug("cid_resolved", identifier=identifier[:20], cid=cid)
                else:
                    self.logger.debug("cid_not_found", identifier=identifier[:20])
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("cid_resolution_failed", identifier=identifier[:20], error=str(exc))

            cid_mapping[identifier] = metadata

        resolved = sum(1 for info in cid_mapping.values() if info.get("cid"))
        self.logger.info("cid_resolution_completed", total=len(identifiers), resolved=resolved)
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

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Normalize PubChem record to standard format.

        Args:
            record: Raw PubChem property record

        Returns:
            Normalized record with pubchem_* prefix
        """
        normalized = {column: None for column in self._PUBCHEM_COLUMNS}

        # PubChem CID
        cid = record.get("CID") or record.get("_cid")
        if cid is not None:
            try:
                normalized["pubchem_cid"] = int(cid)
            except (TypeError, ValueError):
                normalized["pubchem_cid"] = None

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

        if "RegistryID" in record:
            normalized["pubchem_registry_id"] = str(record["RegistryID"])

        if "RN" in record:
            normalized["pubchem_rn"] = str(record["RN"])

        if "Synonym" in record:
            normalized["pubchem_synonyms"] = self._canonical_json(record["Synonym"])
        elif "Synonyms" in record:
            normalized["pubchem_synonyms"] = self._canonical_json(record["Synonyms"])

        normalized["pubchem_lookup_inchikey"] = record.get("_source_identifier")

        cid_source = record.get("_cid_source", "inchikey" if normalized["pubchem_cid"] else "failed")
        normalized["pubchem_cid_source"] = cid_source
        normalized["pubchem_enrichment_attempt"] = record.get("_enrichment_attempt", 1)
        normalized["pubchem_fallback_used"] = bool(record.get("_fallback_used", False))
        normalized["pubchem_enriched_at"] = datetime.now(timezone.utc).isoformat()

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

        # Map back using InChI Key (fallback to lookup identifier when PubChem omits InChIKey)
        inchi_series = pubchem_df.get('pubchem_inchi_key')
        lookup_series = pubchem_df.get('pubchem_lookup_inchikey')
        if inchi_series is None:
            inchi_series = lookup_series
        elif lookup_series is not None:
            inchi_series = inchi_series.fillna(lookup_series)

        if inchi_series is None:
            pubchem_df['inchi_key_normalized'] = None
        else:
            normalized = inchi_series.astype(str)
            normalized = normalized.where(inchi_series.notna(), None)
            normalized = normalized.str.upper()
            normalized = normalized.where(~normalized.isin(['NONE', 'NAN']), None)
            pubchem_df['inchi_key_normalized'] = normalized

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

