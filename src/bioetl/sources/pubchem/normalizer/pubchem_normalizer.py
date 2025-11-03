"""Normalization routines for PubChem enrichment results."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

import pandas as pd

from bioetl.sources.pubchem.client.pubchem_client import PubChemClient
from bioetl.utils.json import canonical_json

__all__ = ["PubChemNormalizer"]


class PubChemNormalizer:
    """Normalize PubChem payloads into the unified dataset contract."""

    _PUBCHEM_COLUMNS: tuple[str, ...] = (
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
    )

    def __init__(self, timestamp_factory: Callable[[], str] | None = None) -> None:
        self._timestamp_factory = timestamp_factory or self._default_timestamp

    @staticmethod
    def _default_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def normalize_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Normalize a single PubChem record into canonical column names."""

        normalized: dict[str, Any] = {column: None for column in self._PUBCHEM_COLUMNS}

        cid = record.get("CID") or record.get("_cid")
        try:
            normalized["pubchem_cid"] = int(cid) if cid is not None else None
        except (TypeError, ValueError):
            normalized["pubchem_cid"] = None

        normalized["pubchem_molecular_formula"] = self._coerce_string(record.get("MolecularFormula"))
        normalized["pubchem_molecular_weight"] = self._coerce_float(record.get("MolecularWeight"))
        normalized["pubchem_canonical_smiles"] = self._coerce_string(
            record.get("CanonicalSMILES")
            or record.get("ConnectivitySMILES")
            or record.get("SMILES")
        )
        normalized["pubchem_isomeric_smiles"] = self._coerce_string(
            record.get("IsomericSMILES") or record.get("SMILES")
        )
        normalized["pubchem_inchi"] = self._coerce_string(record.get("InChI"))
        normalized["pubchem_inchi_key"] = self._coerce_string(record.get("InChIKey"))
        normalized["pubchem_iupac_name"] = self._coerce_string(record.get("IUPACName"))
        normalized["pubchem_registry_id"] = self._coerce_string(self._first_value(record.get("RegistryID")))
        normalized["pubchem_rn"] = self._coerce_string(self._first_value(record.get("RN")))

        synonyms = record.get("Synonym") or record.get("Synonyms")
        if synonyms is not None:
            normalized["pubchem_synonyms"] = canonical_json(synonyms)

        normalized["pubchem_lookup_inchikey"] = self._coerce_string(record.get("_source_identifier"))
        normalized["pubchem_cid_source"] = self._coerce_string(
            record.get("_cid_source") or ("inchikey" if normalized["pubchem_cid"] else "failed")
        )
        normalized["pubchem_enrichment_attempt"] = self._coerce_int(record.get("_enrichment_attempt"))
        normalized["pubchem_fallback_used"] = bool(record.get("_fallback_used", False))
        normalized["pubchem_enriched_at"] = self._timestamp_factory()

        return normalized

    @staticmethod
    def _coerce_string(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            candidate = value.strip()
            return candidate or None
        return str(value)

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _first_value(value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return value[0] if value else None
        return value

    def normalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce DataFrame columns to their canonical dtypes."""

        working = df.copy()
        if "pubchem_cid" in working.columns:
            working["pubchem_cid"] = pd.to_numeric(working["pubchem_cid"], errors="coerce").astype("Int64")
        if "pubchem_enrichment_attempt" in working.columns:
            working["pubchem_enrichment_attempt"] = pd.to_numeric(
                working["pubchem_enrichment_attempt"], errors="coerce"
            ).astype("Int64")
        if "pubchem_molecular_weight" in working.columns:
            working["pubchem_molecular_weight"] = pd.to_numeric(
                working["pubchem_molecular_weight"], errors="coerce"
            ).astype("Float64")
        if "pubchem_fallback_used" in working.columns:
            working["pubchem_fallback_used"] = working["pubchem_fallback_used"].astype("boolean")
        return working.convert_dtypes()

    def ensure_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all PubChem columns are present on the DataFrame."""

        working = df.copy()
        for column in self._PUBCHEM_COLUMNS:
            if column not in working.columns:
                working[column] = pd.NA
        return working

    def enrich_dataframe(
        self,
        df: pd.DataFrame,
        *,
        inchi_key_col: str = "standard_inchi_key",
        client: PubChemClient,
    ) -> pd.DataFrame:
        """Enrich a DataFrame with PubChem properties via the provided client."""

        if df.empty or inchi_key_col not in df.columns:
            return self.ensure_columns(df)

        working = df.copy()
        working[inchi_key_col] = working[inchi_key_col].astype("string").str.upper().str.strip()
        unique_keys = (
            working[inchi_key_col]
            .dropna()
            .map(lambda value: value if value and value.upper() != "NONE" else None)
            .dropna()
            .unique()
            .tolist()
        )
        if not unique_keys:
            return self.ensure_columns(working)

        raw_records = client.enrich_batch(unique_keys)
        normalized_records = [self.normalize_record(record) for record in raw_records]
        pubchem_df = pd.DataFrame(normalized_records).convert_dtypes() if normalized_records else pd.DataFrame()
        if pubchem_df.empty:
            return self.ensure_columns(working)

        join_key = self._prepare_join_key(pubchem_df)
        pubchem_df = pd.concat([pubchem_df, join_key], axis=1)
        enriched = working.merge(
            pubchem_df,
            left_on=inchi_key_col,
            right_on="inchi_key_normalized",
            how="left",
            suffixes=("", "_pubchem_duplicate"),
        )
        enriched = enriched.loc[:, ~enriched.columns.str.endswith("_pubchem_duplicate")]
        enriched = enriched.drop(columns=["inchi_key_normalized"], errors="ignore")
        enriched = self.ensure_columns(enriched)
        return enriched

    @staticmethod
    def _prepare_join_key(df: pd.DataFrame) -> pd.DataFrame:
        if "pubchem_inchi_key" in df.columns:
            inchi_series = df["pubchem_inchi_key"].astype("string")
        else:
            inchi_series = pd.Series([None] * len(df), dtype="string")
        lookup_series = df.get("pubchem_lookup_inchikey")
        if lookup_series is not None:
            lookup_normalized = lookup_series.astype("string")
            inchi_series = inchi_series.fillna(lookup_normalized)
        normalized = inchi_series.str.upper()
        normalized = normalized.mask(normalized.isin(["NONE", "NAN", "<NA>"]))
        return pd.DataFrame({"inchi_key_normalized": normalized})

    def get_pubchem_columns(self) -> Sequence[str]:
        """Expose the ordered list of PubChem-specific columns."""

        return list(self._PUBCHEM_COLUMNS)
