"""TestItem Pipeline - ChEMBL molecule data extraction."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pandas as pd

from bioetl.adapters import PubChemAdapter
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TestItemSchema
from bioetl.schemas.testitem import TESTITEM_COLUMN_ORDER
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("testitem", "1.0.0", TestItemSchema)


_MOLECULE_METADATA_COLUMNS = {
    "index",
    "pipeline_version",
    "source_system",
    "chembl_release",
    "extracted_at",
    "hash_row",
    "hash_business_key",
}


def _canonical_json(value: Any) -> str | None:
    """Serialize value to canonical JSON or return ``None`` for empty payloads."""

    if value in (None, "", [], {}):
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _normalize_pref_name(name: str | None) -> str | None:
    """Create deterministic search key for preferred molecule name."""

    if not isinstance(name, str):
        return None

    normalized = re.sub(r"\s+", " ", name.strip()).lower()
    normalized = re.sub(r"[^0-9a-z]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or None


def _flatten_molecule_hierarchy(molecule: dict[str, Any]) -> dict[str, Any]:
    """Extract parent molecule identifiers and canonical JSON hierarchy."""

    hierarchy = molecule.get("molecule_hierarchy")
    if not isinstance(hierarchy, dict):
        return {
            "parent_chembl_id": None,
            "parent_molregno": None,
            "molecule_hierarchy": None,
        }

    return {
        "parent_chembl_id": hierarchy.get("parent_chembl_id"),
        "parent_molregno": hierarchy.get("parent_molregno"),
        "molecule_hierarchy": _canonical_json(hierarchy),
    }


_PROPERTY_FIELDS: tuple[str, ...] = (
    "mw_freebase",
    "alogp",
    "hba",
    "hbd",
    "psa",
    "rtb",
    "ro3_pass",
    "num_ro5_violations",
    "acd_most_apka",
    "acd_most_bpka",
    "acd_logp",
    "acd_logd",
    "molecular_species",
    "full_mwt",
    "aromatic_rings",
    "heavy_atoms",
    "qed_weighted",
    "mw_monoisotopic",
    "full_molformula",
    "hba_lipinski",
    "hbd_lipinski",
    "num_lipinski_ro5_violations",
)


def _flatten_molecule_properties(molecule: dict[str, Any]) -> dict[str, Any]:
    """Extract physico-chemical properties with canonical JSON serialization."""

    props = molecule.get("molecule_properties")
    flattened: dict[str, Any] = {field: None for field in _PROPERTY_FIELDS}

    if isinstance(props, dict):
        for field in _PROPERTY_FIELDS:
            flattened[field] = props.get(field)
        flattened["molecule_properties"] = _canonical_json(props)
    else:
        flattened["molecule_properties"] = None

    return flattened


_STRUCTURE_FIELDS = {
    "canonical_smiles": "canonical_smiles",
    "standard_inchi": "standard_inchi",
    "standard_inchi_key": "standard_inchi_key",
    "standardized_smiles": "standardized_smiles",
    "standardized_inchi": "standardized_inchi",
    "standardized_inchi_key": "standardized_inchi_key",
}


def _flatten_molecule_structures(molecule: dict[str, Any]) -> dict[str, Any]:
    """Extract structures and canonical JSON representation."""

    structures = molecule.get("molecule_structures")
    flattened = {field: None for field in _STRUCTURE_FIELDS}

    if isinstance(structures, dict):
        for field, key in _STRUCTURE_FIELDS.items():
            flattened[field] = structures.get(key)
        flattened["molecule_structures"] = _canonical_json(structures)
    else:
        flattened["molecule_structures"] = None

    return flattened


def _sorted_synonym_entries(synonyms: Iterable[Any]) -> list[Any]:
    """Return synonyms sorted deterministically by synonym text and source."""

    sortable_entries: list[tuple[tuple[str, str], Any]] = []
    for entry in synonyms:
        if isinstance(entry, dict):
            synonym = str(entry.get("molecule_synonym", ""))
            source = str(entry.get("synonyms_source", ""))
            sanitized = {key: entry.get(key) for key in sorted(entry.keys())}
            sortable_entries.append(((synonym, source), sanitized))
        else:
            sortable_entries.append(((str(entry), ""), entry))

    sortable_entries.sort(key=lambda item: item[0])
    return [entry for _, entry in sortable_entries]


def _flatten_molecule_synonyms(molecule: dict[str, Any]) -> dict[str, Any]:
    """Extract synonyms with canonical JSON string and aggregated names."""

    synonyms = molecule.get("molecule_synonyms")
    if not isinstance(synonyms, Iterable) or isinstance(synonyms, (str, bytes)):
        return {"all_names": None, "molecule_synonyms": None}

    sorted_entries = _sorted_synonym_entries(synonyms)

    synonym_names: list[str] = []
    for entry in sorted_entries:
        if isinstance(entry, dict) and "molecule_synonym" in entry:
            synonym_names.append(str(entry["molecule_synonym"]))
        elif isinstance(entry, dict) and "value" in entry:
            synonym_names.append(str(entry["value"]))

    aggregated = "; ".join(synonym_names) if synonym_names else None

    return {
        "all_names": aggregated,
        "molecule_synonyms": _canonical_json(sorted_entries),
    }


def _flatten_nested_json_fields(molecule: dict[str, Any], field_names: Iterable[str]) -> dict[str, Any]:
    """Serialize nested JSON structures for the given field names."""

    return {field: _canonical_json(molecule.get(field)) for field in field_names}


_TOP_LEVEL_FIELDS: tuple[str, ...] = (
    "molregno",
    "pref_name",
    "max_phase",
    "therapeutic_flag",
    "dosed_ingredient",
    "first_approval",
    "structure_type",
    "molecule_type",
    "oral",
    "parenteral",
    "topical",
    "black_box_warning",
    "natural_product",
    "first_in_class",
    "chirality",
    "prodrug",
    "inorganic_flag",
    "polymer_flag",
    "usan_year",
    "availability_type",
    "usan_stem",
    "usan_substem",
    "usan_stem_definition",
    "indication_class",
    "withdrawn_flag",
    "withdrawn_year",
    "withdrawn_country",
    "withdrawn_reason",
    "mechanism_of_action",
    "direct_interaction",
    "molecular_mechanism",
    "drug_chembl_id",
    "drug_name",
    "drug_type",
    "drug_substance_flag",
    "drug_indication_flag",
    "drug_antibacterial_flag",
    "drug_antiviral_flag",
    "drug_antifungal_flag",
    "drug_antiparasitic_flag",
    "drug_antineoplastic_flag",
    "drug_immunosuppressant_flag",
    "drug_antiinflammatory_flag",
)


_NESTED_JSON_FIELDS: tuple[str, ...] = (
    "atc_classifications",
    "biotherapeutic",
    "chemical_probe",
    "cross_references",
    "helm_notation",
    "orphan",
    "veterinary",
    "chirality_chembl",
    "molecule_type_chembl",
)


def _initialize_record_defaults() -> dict[str, Any]:
    """Return default record with ``None`` for all expected columns."""

    defaults: dict[str, Any] = {}
    for column in TESTITEM_COLUMN_ORDER:
        if column in _MOLECULE_METADATA_COLUMNS or column.startswith("pubchem_"):
            continue
        defaults[column] = None
    return defaults


def _normalize_molecule_record(molecule: dict[str, Any]) -> dict[str, Any]:
    """Normalize raw ChEMBL molecule into flat record."""

    record = _initialize_record_defaults()
    record["molecule_chembl_id"] = molecule.get("molecule_chembl_id")
    record["pref_name"] = molecule.get("pref_name")
    record["pref_name_key"] = _normalize_pref_name(molecule.get("pref_name"))

    for field in _TOP_LEVEL_FIELDS:
        if field in molecule:
            record[field] = molecule.get(field)

    record.update(_flatten_molecule_hierarchy(molecule))
    record.update(_flatten_molecule_properties(molecule))
    record.update(_flatten_molecule_structures(molecule))
    record.update(_flatten_molecule_synonyms(molecule))
    record.update(_flatten_nested_json_fields(molecule, _NESTED_JSON_FIELDS))

    # Ensure parent molregno type alignment if provided as string
    if record.get("parent_molregno") is not None:
        try:
            record["parent_molregno"] = int(record["parent_molregno"])
        except (TypeError, ValueError):
            record["parent_molregno"] = record.get("parent_molregno")

    return record


class TestItemPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL molecule (testitem) data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 25)
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 25

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size

        # Initialize external adapters (PubChem)
        self.external_adapters: dict[str, ExternalAdapter] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract molecule data from input file."""
        if input_file is None:
            # Default to data/input/testitem.csv
            input_file = Path("data/input/testitem.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with molecule IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            # Return empty DataFrame with schema structure
            return pd.DataFrame(columns=TESTITEM_COLUMN_ORDER)

        df = pd.read_csv(input_file)  # Read all records

        # Don't filter - keep all fields from input file
        # They will be matched against schema column_order in transform()

        logger.info("extraction_completed", rows=len(df), columns=len(df.columns))
        return df

    def _fetch_molecule_data(self, molecule_ids: list[str]) -> pd.DataFrame:
        """Fetch molecule data from ChEMBL API and normalize records."""

        records: list[dict[str, Any]] = []

        for i in range(0, len(molecule_ids), self.batch_size):
            batch_ids = molecule_ids[i : i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                url = f"{self.api_client.config.base_url}/molecule.json"
                params = {
                    "molecule_chembl_id__in": ",".join(batch_ids),
                    "limit": len(batch_ids),
                }
                response = self.api_client.request_json(url, params=params)
                molecules = response.get("molecules", [])

                returned_ids = {m.get("molecule_chembl_id") for m in molecules if isinstance(m, dict)}
                missing_ids = [mid for mid in batch_ids if mid not in returned_ids]

                if missing_ids:
                    logger.warning(
                        "incomplete_batch_response",
                        requested=len(batch_ids),
                        returned=len(molecules),
                        missing=missing_ids,
                    )

                for mol in molecules:
                    if not isinstance(mol, dict):
                        continue
                    records.append(_normalize_molecule_record(mol))

                for missing_id in missing_ids:
                    try:
                        retry_url = f"{self.api_client.config.base_url}/molecule/{missing_id}.json"
                        retry_response = self.api_client.request_json(retry_url)
                        if isinstance(retry_response, dict) and retry_response.get("molecule_chembl_id"):
                            logger.info("retry_successful", molecule_chembl_id=missing_id)
                            records.append(_normalize_molecule_record(retry_response))
                        else:
                            logger.warning("retry_no_payload", molecule_chembl_id=missing_id)
                            fallback = _initialize_record_defaults()
                            fallback["molecule_chembl_id"] = missing_id
                            records.append(fallback)
                    except Exception as retry_error:  # noqa: BLE001
                        logger.error(
                            "retry_failed",
                            molecule_chembl_id=missing_id,
                            error=str(retry_error),
                        )
                        fallback = _initialize_record_defaults()
                        fallback["molecule_chembl_id"] = missing_id
                        records.append(fallback)

                logger.info("batch_fetched", count=len(molecules))

            except Exception as error:  # noqa: BLE001
                logger.error("batch_fetch_failed", error=str(error), batch_ids=batch_ids)

        if not records:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        logger.info("api_extraction_completed", rows=len(df))
        return df

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from status endpoint.

        Returns:
            Version string (e.g., 'ChEMBL_36') or None
        """
        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)

            # Extract version info from status endpoint
            version = response.get("chembl_db_version")
            release_date = response.get("chembl_release_date")
            activities = response.get("activities")

            if version:
                logger.info(
                    "chembl_version_fetched",
                    version=version,
                    release_date=release_date,
                    activities=activities
                )
                return str(version)

            logger.warning("chembl_version_not_in_status_response")
            return None

        except Exception as e:
            logger.warning("failed_to_get_chembl_version", error=str(e))
            return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform molecule data with hash generation."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["molecule_chembl_id", "parent_chembl_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Fetch molecule data from ChEMBL API
        molecule_ids = df["molecule_chembl_id"].dropna().unique().tolist()
        molecule_data = self._fetch_molecule_data(molecule_ids)

        if molecule_data.empty:
            logger.warning("molecule_data_empty", reason="chembl_fetch")
            normalized_records = []
            for mol_id in molecule_ids:
                record = _initialize_record_defaults()
                record["molecule_chembl_id"] = mol_id
                normalized_records.append(record)
            molecule_data = pd.DataFrame(normalized_records)

        # Ensure all expected columns are present before merging back input data
        for column in TESTITEM_COLUMN_ORDER:
            if column in _MOLECULE_METADATA_COLUMNS or column.startswith("pubchem_"):
                continue
            if column not in molecule_data.columns:
                molecule_data[column] = None

        # Merge normalized data with input, preferring normalized values
        normalized_df = molecule_data.drop_duplicates("molecule_chembl_id").set_index("molecule_chembl_id")
        input_df = df.set_index("molecule_chembl_id")

        combined_df = normalized_df.copy()
        for column in input_df.columns:
            if column in combined_df.columns:
                combined_df[column] = combined_df[column].combine_first(input_df[column])
            else:
                combined_df[column] = input_df[column]

        df = combined_df.reset_index().rename(columns={"index": "molecule_chembl_id"})

        if "pref_name" in df.columns:
            normalized_keys = df["pref_name"].apply(_normalize_pref_name)
            if "pref_name_key" in df.columns:
                df["pref_name_key"] = df["pref_name_key"].combine_first(normalized_keys)
            else:
                df["pref_name_key"] = normalized_keys

        # PubChem enrichment (optional)
        if "pubchem" in self.external_adapters:
            logger.info("pubchem_enrichment_enabled")
            try:
                df = self._enrich_with_pubchem(df)
            except Exception as e:
                logger.error("pubchem_enrichment_failed", error=str(e))
                # Continue with original data - graceful degradation

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = self._chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["molecule_chembl_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values("molecule_chembl_id")  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema and add missing columns with None
        from bioetl.schemas import TestItemSchema

        if "column_order" in TestItemSchema.Config.__dict__:
            expected_cols = TestItemSchema.Config.column_order

            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def _init_external_adapters(self) -> None:
        """Initialize external API adapters."""
        sources = self.config.sources

        # PubChem adapter
        if "pubchem" in sources:
            pubchem = sources["pubchem"]
            # Handle both dict and SourceConfig object
            if isinstance(pubchem, dict):
                enabled = pubchem.get("enabled", False)
                base_url = pubchem.get("base_url", "https://pubchem.ncbi.nlm.nih.gov/rest/pug")
                rate_limit_max_calls = pubchem.get("rate_limit_max_calls", 5)
                rate_limit_period = pubchem.get("rate_limit_period", 1.0)
                batch_size = pubchem.get("batch_size", 100)
            else:
                enabled = pubchem.enabled
                base_url = pubchem.base_url or "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
                rate_limit_max_calls = getattr(pubchem, "rate_limit_max_calls", 5)
                rate_limit_period = getattr(pubchem, "rate_limit_period", 1.0)
                batch_size = pubchem.batch_size or 100

            if enabled:
                pubchem_config = APIConfig(
                    name="pubchem",
                    base_url=base_url,
                    rate_limit_max_calls=rate_limit_max_calls,
                    rate_limit_period=rate_limit_period,
                    cache_enabled=self.config.cache.enabled,
                    cache_ttl=self.config.cache.ttl,
                )
                adapter_config = AdapterConfig(
                    enabled=True,
                    batch_size=batch_size,
                    workers=1,  # PubChem doesn't use parallel workers for simplicity
                )
                self.external_adapters["pubchem"] = PubChemAdapter(pubchem_config, adapter_config)
                logger.info("pubchem_adapter_initialized", base_url=base_url, batch_size=batch_size)

        logger.info("adapters_initialized", count=len(self.external_adapters))

    def _enrich_with_pubchem(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich testitem data with PubChem properties.

        Args:
            df: DataFrame with molecule data from ChEMBL

        Returns:
            DataFrame enriched with pubchem_* fields
        """
        if "pubchem" not in self.external_adapters:
            logger.warning("pubchem_adapter_not_available")
            return df

        if df.empty:
            logger.warning("enrichment_skipped", reason="empty_dataframe")
            return df

        # Check if we have InChI Keys for enrichment
        if "standard_inchi_key" not in df.columns:
            logger.warning("enrichment_skipped", reason="missing_standard_inchi_key_column")
            return df

        pubchem_adapter = self.external_adapters["pubchem"]

        try:
            # Type cast to PubChemAdapter for specific method access
            enriched_df = cast(PubChemAdapter, pubchem_adapter).enrich_with_pubchem(df, inchi_key_col="standard_inchi_key")
            return enriched_df
        except Exception as e:
            logger.error("pubchem_enrichment_error", error=str(e))
            # Return original dataframe on error - graceful degradation
            return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate molecule data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["molecule_chembl_id"].duplicated().sum() if "molecule_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                # Remove duplicates, keeping first occurrence
                df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

