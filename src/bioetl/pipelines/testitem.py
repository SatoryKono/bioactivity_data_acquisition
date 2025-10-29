"""TestItem Pipeline - ChEMBL molecule data extraction."""

import json
import re
from pathlib import Path
from typing import Any, cast

import pandas as pd
import requests
from pandera.errors import SchemaErrors

from bioetl.adapters import PubChemAdapter
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.config import PipelineConfig
from bioetl.config.models import TargetSourceConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TestItemSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("testitem", "1.0.0", TestItemSchema)


class TestItemPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL molecule (testitem) data."""

    # ChEMBL molecule columns expected from the API according to 07a specification
    _CHEMBL_CORE_FIELDS: list[str] = [
        "molregno",
        "pref_name",
        "pref_name_key",
        "parent_chembl_id",
        "parent_molregno",
        "therapeutic_flag",
        "structure_type",
        "molecule_type",
        "molecule_type_chembl",
        "max_phase",
        "first_approval",
        "dosed_ingredient",
        "availability_type",
        "chirality",
        "chirality_chembl",
        "mechanism_of_action",
        "direct_interaction",
        "molecular_mechanism",
        "oral",
        "parenteral",
        "topical",
        "black_box_warning",
        "natural_product",
        "first_in_class",
        "prodrug",
        "inorganic_flag",
        "polymer_flag",
        "usan_year",
        "usan_stem",
        "usan_substem",
        "usan_stem_definition",
        "indication_class",
        "withdrawn_flag",
        "withdrawn_year",
        "withdrawn_country",
        "withdrawn_reason",
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
    ]

    _CHEMBL_PROPERTY_FIELDS: list[str] = [
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
        "lipinski_ro5_violations",
        "lipinski_ro5_pass",
    ]

    _CHEMBL_STRUCTURE_FIELDS: list[str] = [
        "standardized_smiles",
        "standard_inchi",
        "standard_inchi_key",
    ]

    _CHEMBL_JSON_FIELDS: list[str] = [
        "molecule_hierarchy",
        "molecule_properties",
        "molecule_structures",
        "molecule_synonyms",
        "atc_classifications",
        "cross_references",
        "biotherapeutic",
        "chemical_probe",
        "orphan",
        "veterinary",
        "helm_notation",
    ]

    _CHEMBL_TEXT_FIELDS: list[str] = [
        "all_names",
    ]

    _PUBCHEM_FIELDS: list[str] = [
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
    ]

    _FALLBACK_FIELDS: list[str] = [
        "fallback_error_code",
        "fallback_http_status",
        "fallback_retry_after_sec",
        "fallback_attempt",
        "fallback_error_message",
    ]

    @classmethod
    def _expected_columns(cls) -> list[str]:
        """Return ordered list of expected columns prior to metadata fields."""

        business_fields = [
            "molecule_chembl_id",
            *cls._CHEMBL_CORE_FIELDS,
            *cls._CHEMBL_PROPERTY_FIELDS,
            *cls._CHEMBL_STRUCTURE_FIELDS,
            *cls._CHEMBL_TEXT_FIELDS,
            *cls._CHEMBL_JSON_FIELDS,
        ]
        # Deduplicate while preserving order
        seen: set[str] = set()
        ordered: list[str] = []
        for col in business_fields:
            if col not in seen:
                ordered.append(col)
                seen.add(col)
        ordered.extend(cls._PUBCHEM_FIELDS)
        ordered.extend(cls._FALLBACK_FIELDS)
        return ordered

    @staticmethod
    def _canonical_json(value: Any) -> str | None:
        """Serialize value to canonical JSON string."""

        if value in (None, ""):
            return None
        try:
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError):
            return None

    @classmethod
    def _empty_molecule_record(cls) -> dict[str, Any]:
        """Create an empty molecule record with all expected fields."""

        record = dict.fromkeys(cls._expected_columns())
        return record

    @staticmethod
    def _normalize_pref_name(pref_name: Any) -> str | None:
        """Normalize preferred name for deterministic keys."""

        if not isinstance(pref_name, str):
            return None
        normalized = pref_name.strip().lower()
        return normalized or None

    @classmethod
    def _flatten_molecule_hierarchy(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Flatten molecule_hierarchy node with canonical JSON."""

        flattened = {
            "parent_chembl_id": None,
            "parent_molregno": None,
            "molecule_hierarchy": None,
        }

        hierarchy = molecule.get("molecule_hierarchy")
        if isinstance(hierarchy, dict) and hierarchy:
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")
            flattened["molecule_hierarchy"] = cls._canonical_json(hierarchy)

        return flattened

    @classmethod
    def _flatten_molecule_properties(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Extract 22 molecular properties with canonical JSON payload."""

        flattened = dict.fromkeys(cls._CHEMBL_PROPERTY_FIELDS)
        flattened["molecule_properties"] = None

        props = molecule.get("molecule_properties")
        if isinstance(props, dict) and props:
            mapping = {
                "mw_freebase": "mw_freebase",
                "alogp": "alogp",
                "hba": "hba",
                "hbd": "hbd",
                "psa": "psa",
                "rtb": "rtb",
                "ro3_pass": "ro3_pass",
                "num_ro5_violations": "num_ro5_violations",
                "acd_most_apka": "acd_most_apka",
                "acd_most_bpka": "acd_most_bpka",
                "acd_logp": "acd_logp",
                "acd_logd": "acd_logd",
                "molecular_species": "molecular_species",
                "full_mwt": "full_mwt",
                "aromatic_rings": "aromatic_rings",
                "heavy_atoms": "heavy_atoms",
                "qed_weighted": "qed_weighted",
                "mw_monoisotopic": "mw_monoisotopic",
                "full_molformula": "full_molformula",
                "hba_lipinski": "hba_lipinski",
                "hbd_lipinski": "hbd_lipinski",
                "num_lipinski_ro5_violations": "num_lipinski_ro5_violations",
                "lipinski_ro5_violations": "num_lipinski_ro5_violations",
                "lipinski_ro5_pass": "lipinski_ro5_pass",
            }
            for field, source in mapping.items():
                flattened[field] = props.get(source)

            if "lipinski_ro5_pass" not in props and "ro3_pass" in props:
                flattened["lipinski_ro5_pass"] = props.get("ro3_pass")

            if flattened["rtb"] is None and "num_rotatable_bonds" in props:
                flattened["rtb"] = props.get("num_rotatable_bonds")

            flattened["molecule_properties"] = cls._canonical_json(props)

        return flattened

    @classmethod
    def _flatten_molecule_structures(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Extract canonical molecular structures."""

        flattened = {
            "standardized_smiles": None,
            "standard_inchi": None,
            "standard_inchi_key": None,
            "molecule_structures": None,
        }

        structures = molecule.get("molecule_structures")
        if isinstance(structures, dict) and structures:
            flattened["standardized_smiles"] = structures.get("canonical_smiles")
            flattened["standard_inchi"] = structures.get("standard_inchi")
            flattened["standard_inchi_key"] = structures.get("standard_inchi_key")
            flattened["molecule_structures"] = cls._canonical_json(structures)

        return flattened

    @staticmethod
    def _sorted_synonym_entries(synonyms: list[Any]) -> list[Any]:
        """Sort synonym entries deterministically."""

        def synonym_key(entry: Any) -> str:
            if isinstance(entry, dict):
                value = entry.get("molecule_synonym")
            else:
                value = entry
            if not isinstance(value, str):
                return ""
            return value.strip().lower()

        return sorted(synonyms, key=synonym_key)

    @classmethod
    def _flatten_molecule_synonyms(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Extract synonyms with canonical serialization."""

        flattened = {
            "all_names": None,
            "molecule_synonyms": None,
        }

        synonyms = molecule.get("molecule_synonyms")
        if isinstance(synonyms, list) and synonyms:
            synonym_names: list[str] = []
            normalized_entries: list[Any] = []

            for entry in synonyms:
                if isinstance(entry, dict):
                    normalized_entry = entry.copy()
                    value = normalized_entry.get("molecule_synonym")
                    if isinstance(value, str):
                        trimmed_value = value.strip()
                        normalized_entry["molecule_synonym"] = trimmed_value
                        synonym_names.append(trimmed_value)
                    normalized_entries.append(normalized_entry)
                elif isinstance(entry, str):
                    trimmed = entry.strip()
                    normalized_entries.append(trimmed)
                    synonym_names.append(trimmed)

            unique_names = sorted({name.strip() for name in synonym_names if name}, key=str.lower)
            if unique_names:
                flattened["all_names"] = "; ".join(unique_names)

            if normalized_entries:
                sorted_entries = cls._sorted_synonym_entries(normalized_entries)
                flattened["molecule_synonyms"] = cls._canonical_json(sorted_entries)

        return flattened

    @classmethod
    def _flatten_nested_json(cls, molecule: dict[str, Any], field_name: str) -> str | None:
        """Serialize nested JSON structure to canonical string."""

        value = molecule.get(field_name)
        if value in (None, ""):
            return None
        return cls._canonical_json(value)


    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, TargetSourceConfig):
            base_url = chembl_source.base_url
            batch_size_config = chembl_source.batch_size if chembl_source.batch_size is not None else 25
        elif isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", default_base_url)
            batch_size_config = chembl_source.get("batch_size", 25)
        else:
            base_url = default_base_url
            batch_size_config = 25

        try:
            batch_size_value = int(batch_size_config)
        except (TypeError, ValueError) as exc:
            raise ValueError("sources.chembl.batch_size must be an integer") from exc

        if batch_size_value <= 0:
            raise ValueError("sources.chembl.batch_size must be greater than zero")

        if batch_size_value > 25:
            raise ValueError("sources.chembl.batch_size must be <= 25 due to ChEMBL API limits")

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size_value

        # Initialize external adapters (PubChem)
        self.external_adapters: dict[str, ExternalAdapter] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()
        self._molecule_cache: dict[str, dict[str, Any]] = {}

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
            return pd.DataFrame(columns=TestItemSchema.get_column_order())

        df = pd.read_csv(input_file)  # Read all records

        # Don't filter - keep all fields from input file
        # They will be matched against schema column_order in transform()

        logger.info("extraction_completed", rows=len(df), columns=len(df.columns))
        return df

    def _fetch_molecule_data(self, molecule_ids: list[str]) -> pd.DataFrame:
        """Fetch molecule data from ChEMBL API with release-scoped caching."""
        if not molecule_ids:
            logger.warning("no_molecule_ids_provided")
            return pd.DataFrame()

        cache_hits = 0
        api_success_count = 0
        fallback_count = 0
        results: list[dict[str, Any]] = []
        ids_to_fetch: list[str] = []

        for molecule_id in molecule_ids:
            cache_key = self._cache_key(molecule_id)
            cached_record = self._molecule_cache.get(cache_key)
            if cached_record is not None:
                results.append(cached_record.copy())
                cache_hits += 1
            else:
                ids_to_fetch.append(molecule_id)

        for i in range(0, len(ids_to_fetch), self.batch_size):
            batch_ids = ids_to_fetch[i : i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                params = {
                    "molecule_chembl_id__in": ",".join(batch_ids),
                    "limit": min(len(batch_ids), self.batch_size),
                }
                response = self.api_client.request_json("/molecule.json", params=params)
                molecules = response.get("molecules", [])

                returned_ids = {m.get("molecule_chembl_id") for m in molecules if m.get("molecule_chembl_id")}
                missing_ids = [mol_id for mol_id in batch_ids if mol_id not in returned_ids]

                for mol in molecules:
                    record = self._serialize_molecule_record(mol)
                    results.append(record)
                    api_success_count += 1
                    self._store_in_cache(record)

                if missing_ids:
                    logger.warning(
                        "incomplete_batch_response",
                        requested=len(batch_ids),
                        returned=len(molecules),
                        missing=missing_ids,
                    )
                    for missing_id in missing_ids:
                        record = self._fetch_single_molecule(missing_id, attempt=2)
                        if record:
                            results.append(record)
                            if record.get("fallback_attempt") is not None:
                                fallback_count += 1
                            else:
                                api_success_count += 1

                logger.info("batch_fetched", count=len(molecules))

            except Exception as exc:  # noqa: BLE001
                logger.error("batch_fetch_failed", error=str(exc), batch_ids=batch_ids)
                for missing_id in batch_ids:
                    record = self._create_fallback_record(
                        missing_id,
                        attempt=1,
                        error=exc,
                        message="Batch request failed",
                    )
                    results.append(record)
                    fallback_count += 1
                    self._store_in_cache(record)

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        logger.info(
            "molecule_fetch_summary",
            requested=len(molecule_ids),
            fetched=len(results),
            cache_hits=cache_hits,
            api_success_count=api_success_count,
            fallback_count=fallback_count,
        )

        return pd.DataFrame(results)

    def _cache_key(self, molecule_id: str) -> str:
        release = self._chembl_release or "unversioned"
        return f"{release}:{molecule_id}"

    def _store_in_cache(self, record: dict[str, Any]) -> None:
        molecule_id = record.get("molecule_chembl_id")
        if not molecule_id:
            return
        cache_key = self._cache_key(str(molecule_id))
        self._molecule_cache[cache_key] = record.copy()

    def _serialize_molecule_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = self._empty_molecule_record()

        record["molecule_chembl_id"] = payload.get("molecule_chembl_id")
        record["molregno"] = payload.get("molregno")
        pref_name = payload.get("pref_name")
        record["pref_name"] = pref_name
        record["pref_name_key"] = self._normalize_pref_name(pref_name)
        record["therapeutic_flag"] = payload.get("therapeutic_flag")
        record["structure_type"] = payload.get("structure_type")
        record["molecule_type"] = payload.get("molecule_type")
        record["molecule_type_chembl"] = payload.get("molecule_type")
        record["max_phase"] = payload.get("max_phase")
        record["first_approval"] = payload.get("first_approval")
        record["dosed_ingredient"] = payload.get("dosed_ingredient")
        record["availability_type"] = payload.get("availability_type")
        record["chirality"] = payload.get("chirality")
        record["chirality_chembl"] = payload.get("chirality")
        record["mechanism_of_action"] = payload.get("mechanism_of_action")
        record["direct_interaction"] = payload.get("direct_interaction")
        record["molecular_mechanism"] = payload.get("molecular_mechanism")
        record["oral"] = payload.get("oral")
        record["parenteral"] = payload.get("parenteral")
        record["topical"] = payload.get("topical")
        record["black_box_warning"] = payload.get("black_box_warning")
        record["natural_product"] = payload.get("natural_product")
        record["first_in_class"] = payload.get("first_in_class")
        record["prodrug"] = payload.get("prodrug")
        record["inorganic_flag"] = payload.get("inorganic_flag")
        record["polymer_flag"] = payload.get("polymer_flag")
        record["usan_year"] = payload.get("usan_year")
        record["usan_stem"] = payload.get("usan_stem")
        record["usan_substem"] = payload.get("usan_substem")
        record["usan_stem_definition"] = payload.get("usan_stem_definition")
        record["indication_class"] = payload.get("indication_class")
        record["withdrawn_flag"] = payload.get("withdrawn_flag")
        record["withdrawn_year"] = payload.get("withdrawn_year")
        record["withdrawn_country"] = payload.get("withdrawn_country")
        record["withdrawn_reason"] = payload.get("withdrawn_reason")
        record["drug_chembl_id"] = payload.get("drug_chembl_id")
        record["drug_name"] = payload.get("drug_name")
        record["drug_type"] = payload.get("drug_type")
        record["drug_substance_flag"] = payload.get("drug_substance_flag")
        record["drug_indication_flag"] = payload.get("drug_indication_flag")
        record["drug_antibacterial_flag"] = payload.get("drug_antibacterial_flag")
        record["drug_antiviral_flag"] = payload.get("drug_antiviral_flag")
        record["drug_antifungal_flag"] = payload.get("drug_antifungal_flag")
        record["drug_antiparasitic_flag"] = payload.get("drug_antiparasitic_flag")
        record["drug_antineoplastic_flag"] = payload.get("drug_antineoplastic_flag")
        record["drug_immunosuppressant_flag"] = payload.get("drug_immunosuppressant_flag")
        record["drug_antiinflammatory_flag"] = payload.get("drug_antiinflammatory_flag")

        record.update(self._flatten_molecule_hierarchy(payload))
        record.update(self._flatten_molecule_properties(payload))
        record.update(self._flatten_molecule_structures(payload))
        record.update(self._flatten_molecule_synonyms(payload))

        # Nested JSON blobs
        for field in [
            "atc_classifications",
            "cross_references",
            "biotherapeutic",
            "chemical_probe",
            "orphan",
            "veterinary",
            "helm_notation",
        ]:
            record[field] = self._flatten_nested_json(payload, field)

        # Reset fallback metadata defaults
        for field in self._FALLBACK_FIELDS:
            record[field] = None

        return record

    def _fetch_single_molecule(self, molecule_id: str, attempt: int) -> dict[str, Any]:
        try:
            response = self.api_client.request_json(f"/molecule/{molecule_id}.json")
        except requests.exceptions.HTTPError as exc:
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                error=exc,
                retry_after=self._extract_retry_after(exc),
            )
            fallback_message = record.get("fallback_error_message")
            logger.warning(
                "molecule_fallback_http_error",
                molecule_chembl_id=molecule_id,
                http_status=record.get("fallback_http_status"),
                attempt=attempt,
                message=fallback_message,
            )
            self._store_in_cache(record)
            return record
        except Exception as exc:  # noqa: BLE001
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                error=exc,
            )
            logger.error(
                "molecule_fallback_unexpected_error",
                molecule_chembl_id=molecule_id,
                attempt=attempt,
                error=str(exc),
            )
            self._store_in_cache(record)
            return record

        if not isinstance(response, dict) or "molecule_chembl_id" not in response:
            record = self._create_fallback_record(
                molecule_id,
                attempt=attempt,
                message="Missing molecule in response",
            )
            logger.warning("molecule_missing_in_response", molecule_chembl_id=molecule_id, attempt=attempt)
            self._store_in_cache(record)
            return record

        record = self._serialize_molecule_record(response)
        self._store_in_cache(record)
        return record

    def _create_fallback_record(
        self,
        molecule_id: str,
        attempt: int,
        error: Exception | None = None,
        retry_after: float | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        error_code = getattr(error, "code", None)
        if error_code is None and error is not None:
            error_code = error.__class__.__name__

        http_status: int | None = None
        if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
            http_status = error.response.status_code

        if retry_after is None and isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
            retry_after = self._extract_retry_after(error)

        fallback_record = self._empty_molecule_record()
        fallback_record["molecule_chembl_id"] = molecule_id
        fallback_record["fallback_error_code"] = error_code
        fallback_record["fallback_http_status"] = http_status
        fallback_record["fallback_retry_after_sec"] = retry_after
        fallback_record["fallback_attempt"] = attempt
        fallback_record["fallback_error_message"] = (
            message or (str(error) if error else "Missing from ChEMBL response")
        )

        return fallback_record

    @staticmethod
    def _extract_retry_after(error: requests.exceptions.HTTPError) -> float | None:
        if error.response is None:
            return None
        retry_after = error.response.headers.get("Retry-After")
        if retry_after is None:
            return None
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            return None

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
        molecule_ids = df["molecule_chembl_id"].unique().tolist()
        molecule_data = self._fetch_molecule_data(molecule_ids)

        # Merge normalized ChEMBL data with existing input records
        if not molecule_data.empty:
            if "molecule_chembl_id" not in molecule_data.columns:
                logger.warning(
                    "molecule_data_missing_id_column",
                    available_columns=list(molecule_data.columns),
                )
            else:
                normalized_df = (
                    molecule_data.drop_duplicates("molecule_chembl_id").set_index("molecule_chembl_id")
                )

                input_indexed = df.set_index("molecule_chembl_id")
                deduplicated_input = input_indexed

                if input_indexed.index.has_duplicates:
                    duplicated_ids = (
                        input_indexed.index[input_indexed.index.duplicated()].unique().tolist()
                    )
                    logger.warning(
                        "duplicate_molecule_ids_in_input",
                        count=len(duplicated_ids),
                        sample=[str(mol_id) for mol_id in duplicated_ids[:5]],
                    )
                    deduplicated_input = input_indexed[~input_indexed.index.duplicated(keep="first")]

                # Overlay normalized values while falling back to the original input when needed
                normalized_columns = [
                    column for column in normalized_df.columns if column not in {"molecule_chembl_id"}
                ]

                for column in normalized_columns:
                    normalized_series = normalized_df[column]
                    if column in deduplicated_input.columns:
                        combined_series = normalized_series.combine_first(deduplicated_input[column])
                    else:
                        combined_series = normalized_series

                    overlay = df["molecule_chembl_id"].map(combined_series)
                    if column in df.columns:
                        df[column] = overlay.combine_first(df[column])
                    else:
                        df[column] = overlay

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

        expected_cols = TestItemSchema.get_column_order()
        if expected_cols:
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
        """Validate molecule data against schema and QC policies."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        qc_metrics = self._calculate_qc_metrics(df)
        failing_metrics: list[str] = []

        for metric_name, metric in qc_metrics.items():
            log_fn = logger.error if not metric["passed"] else logger.info
            log_fn(
                "qc_metric",
                metric=metric_name,
                value=metric["value"],
                threshold=metric["threshold"],
                severity=metric["severity"],
                count=metric.get("count"),
                details=metric.get("details"),
            )

            issue: dict[str, Any] = {
                "metric": metric_name,
                "issue_type": "qc_metric",
                "severity": metric["severity"],
                "value": metric["value"],
                "threshold": metric["threshold"],
            }
            if "count" in metric:
                issue["count"] = metric["count"]
            if metric.get("details") is not None:
                issue["details"] = metric["details"]
            self.record_validation_issue(issue)

            if not metric["passed"] and self._should_fail(metric["severity"]):
                failing_metrics.append(metric_name)

        if failing_metrics:
            raise ValueError(
                "QC thresholds exceeded for metrics: " + ", ".join(sorted(failing_metrics))
            )

        duplicates_metric = qc_metrics.get("testitem.duplicate_ratio")
        if (
            duplicates_metric
            and duplicates_metric.get("count")
            and "molecule_chembl_id" in df.columns
        ):
            df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

        try:
            validated_df = TestItemSchema.validate(df, lazy=True)
        except SchemaErrors as exc:
            schema_issues = self._summarize_schema_errors(exc.failure_cases)
            for issue in schema_issues:
                self.record_validation_issue(issue)
                logger.error(
                    "schema_validation_error",
                    column=issue.get("column"),
                    check=issue.get("check"),
                    count=issue.get("count"),
                    severity=issue.get("severity"),
                )

            summary = "; ".join(
                f"{issue.get('column')}: {issue.get('check')} ({issue.get('count')} cases)"
                for issue in schema_issues
            )
            raise ValueError(f"Schema validation failed: {summary}") from exc
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("schema_validation_unexpected_error", error=str(exc))
            raise

        self._validate_identifier_formats(validated_df)
        self._check_referential_integrity(validated_df)

        logger.info(
            "validation_completed",
            rows=len(validated_df),
            issues=len(self.validation_issues),
        )
        return validated_df

    def _calculate_qc_metrics(self, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        """Compute QC metrics used to gate validation."""

        thresholds = self.config.qc.thresholds or {}
        total_rows = len(df)
        metrics: dict[str, dict[str, Any]] = {}

        duplicate_count = 0
        duplicate_ratio = 0.0
        duplicate_values: list[str] = []
        if total_rows > 0 and "molecule_chembl_id" in df.columns:
            duplicate_count = int(df["molecule_chembl_id"].duplicated().sum())
            if duplicate_count:
                duplicate_values = (
                    df.loc[
                        df["molecule_chembl_id"].duplicated(keep=False),
                        "molecule_chembl_id",
                    ]
                    .astype(str)
                    .tolist()
                )
                duplicate_ratio = duplicate_count / total_rows

        duplicate_threshold = float(thresholds.get("testitem.duplicate_ratio", 0.0))
        duplicate_severity = "error" if duplicate_ratio > duplicate_threshold else "info"
        metrics["testitem.duplicate_ratio"] = {
            "count": duplicate_count,
            "value": duplicate_ratio,
            "threshold": duplicate_threshold,
            "passed": duplicate_ratio <= duplicate_threshold,
            "severity": duplicate_severity,
            "details": {"duplicate_values": duplicate_values},
        }

        fallback_count = 0
        fallback_ratio = 0.0
        if total_rows > 0 and "fallback_error_code" in df.columns:
            fallback_count = int(df["fallback_error_code"].notna().sum())
            fallback_ratio = fallback_count / total_rows

        fallback_threshold = float(thresholds.get("testitem.fallback_ratio", 1.0))
        fallback_severity = "warning" if fallback_ratio > fallback_threshold else "info"
        metrics["testitem.fallback_ratio"] = {
            "count": fallback_count,
            "value": fallback_ratio,
            "threshold": fallback_threshold,
            "passed": fallback_ratio <= fallback_threshold,
            "severity": fallback_severity,
            "details": None if fallback_count == 0 else {"fallback_count": fallback_count},
        }

        return metrics

    def _summarize_schema_errors(self, failure_cases: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert Pandera failure cases to structured validation issues."""

        issues: list[dict[str, Any]] = []
        if failure_cases.empty:
            return issues

        for column, group in failure_cases.groupby("column", dropna=False):
            column_name = (
                str(column)
                if column is not None and not (isinstance(column, float) and pd.isna(column))
                else "<dataframe>"
            )
            checks = sorted({str(check) for check in group["check"].dropna().unique()})
            details = ", ".join(
                group["failure_case"].dropna().astype(str).unique().tolist()[:5]
            )
            issues.append(
                {
                    "issue_type": "schema",
                    "severity": "error",
                    "column": column_name,
                    "check": ", ".join(checks) if checks else "<unspecified>",
                    "count": int(group.shape[0]),
                    "details": details,
                }
            )

        return issues

    def _check_referential_integrity(self, df: pd.DataFrame) -> None:
        """Ensure parent_chembl_id values resolve to known molecules."""

        required_columns = {"molecule_chembl_id", "parent_chembl_id"}
        if df.empty or not required_columns.issubset(df.columns):
            logger.debug("referential_check_skipped", reason="columns_absent")
            return

        parent_series = (
            df["parent_chembl_id"].dropna().astype("string").str.strip().str.upper()
        )
        if parent_series.empty:
            logger.info("referential_integrity_passed", relation="testitem->parent", checked=0)
            return

        molecule_ids = (
            df["molecule_chembl_id"].astype("string").str.strip().str.upper()
        )
        known_ids = set(molecule_ids.tolist())
        missing_mask = ~parent_series.isin(known_ids)
        missing_count = int(missing_mask.sum())
        total_refs = int(parent_series.size)

        if missing_count == 0:
            logger.info(
                "referential_integrity_passed",
                relation="testitem->parent",
                checked=total_refs,
            )
            return

        missing_ratio = missing_count / total_refs if total_refs else 0.0
        threshold = float(self.config.qc.thresholds.get("testitem.parent_missing_ratio", 0.0))
        severity = "error" if missing_ratio > threshold else "warning"
        sample_parents = parent_series[missing_mask].unique().tolist()[:5]

        issue = {
            "metric": "testitem.parent_missing_ratio",
            "issue_type": "referential_integrity",
            "severity": severity,
            "value": missing_ratio,
            "count": missing_count,
            "threshold": threshold,
            "details": {"sample_parent_ids": sample_parents},
        }
        self.record_validation_issue(issue)

        should_fail = self._should_fail(severity)
        log_fn = logger.error if should_fail else logger.warning
        log_fn(
            "referential_integrity_failure",
            relation="testitem->parent",
            missing_count=missing_count,
            missing_ratio=missing_ratio,
            threshold=threshold,
            severity=severity,
        )

        if should_fail:
            raise ValueError(
                "Referential integrity violation: parent_chembl_id references missing molecules"
            )

    def _validate_identifier_formats(self, df: pd.DataFrame) -> None:
        """Enforce identifier format constraints not handled by Pandera."""

        if "molecule_chembl_id" not in df.columns:
            return

        pattern = re.compile(r"^CHEMBL\d+$")
        chembl_ids = df["molecule_chembl_id"].astype("string")
        invalid_mask = chembl_ids.isna() | ~chembl_ids.str.match(pattern)
        invalid_count = int(invalid_mask.sum())

        if invalid_count == 0:
            return

        invalid_values = chembl_ids[invalid_mask].dropna().unique().tolist()[:5]
        issue = {
            "issue_type": "schema",
            "severity": "error",
            "column": "molecule_chembl_id",
            "check": "regex:^CHEMBL\\d+$",
            "count": invalid_count,
            "details": ", ".join(invalid_values),
        }
        self.record_validation_issue(issue)

        logger.error(
            "identifier_format_error",
            column="molecule_chembl_id",
            invalid_count=invalid_count,
            sample_values=invalid_values,
        )

        raise ValueError(
            "Schema validation failed: molecule_chembl_id does not match CHEMBL pattern"
        )

