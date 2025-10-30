"""TestItem Pipeline - ChEMBL molecule data extraction."""

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import pandas as pd
import requests  # type: ignore[import-untyped]

from bioetl.adapters import PubChemAdapter
from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TestItemSchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dtypes import (
    coerce_nullable_int,
    coerce_optional_bool,
    coerce_retry_after,
)
from bioetl.utils.fallback import FallbackRecordBuilder, build_fallback_payload
from bioetl.utils.json import canonical_json
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    duplicate_summary,
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("testitem", "1.0.0", TestItemSchema)  # type: ignore[arg-type]


def _extract_boolean_columns() -> list[str]:
    annotations = getattr(TestItemSchema, "__annotations__", {})
    boolean_columns: list[str] = []
    for name, annotation in annotations.items():
        if "BooleanDtype" in str(annotation):
            boolean_columns.append(name)
    return sorted(boolean_columns)


_TESTITEM_BOOLEAN_COLUMNS = _extract_boolean_columns()


# _coerce_nullable_int_columns заменена на coerce_nullable_int из bioetl.utils.dtypes


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
        "fallback_reason",
        "fallback_error_type",
        "fallback_error_code",
        "fallback_http_status",
        "fallback_retry_after_sec",
        "fallback_attempt",
        "fallback_error_message",
        "fallback_timestamp",
    ]

    _NULLABLE_INT_COLUMNS: list[str] = [
        "molregno",
        "parent_molregno",
        "max_phase",
        "first_approval",
        "availability_type",
        "usan_year",
        "withdrawn_year",
        "hba",
        "hbd",
        "rtb",
        "num_ro5_violations",
        "aromatic_rings",
        "heavy_atoms",
        "hba_lipinski",
        "hbd_lipinski",
        "num_lipinski_ro5_violations",
        "lipinski_ro5_violations",
        "pubchem_cid",
        "pubchem_enrichment_attempt",
        "fallback_http_status",
        "fallback_attempt",
    ]

    _BOOLEAN_COLUMNS: list[str] = _TESTITEM_BOOLEAN_COLUMNS

    _INT_COLUMN_MINIMUMS: dict[str, int] = {
        "molregno": 1,
        "parent_molregno": 1,
        "pubchem_cid": 1,
    }

    @classmethod
    def _expected_columns(cls) -> list[str]:
        """Return ordered list of expected columns prior to metadata fields."""

        business_fields = [
            *cls._FALLBACK_FIELDS,
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
        return ordered

    # _canonical_json заменена на canonical_json из bioetl.utils.json

    @classmethod
    def _empty_molecule_record(cls) -> dict[str, Any]:
        """Create an empty molecule record with all expected fields."""

        record = dict.fromkeys(cls._expected_columns())
        return record

    @staticmethod
    def _normalize_pref_name(pref_name: Any) -> str | None:
        """Normalize preferred name for deterministic keys."""

        normalized = registry.normalize("chemistry.string", pref_name)
        if normalized is None:
            return None
        lowered = normalized.lower()
        return lowered or None

    @classmethod
    def _flatten_molecule_hierarchy(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Flatten molecule_hierarchy node with canonical JSON."""

        flattened: dict[str, Any] = {
            "parent_chembl_id": None,
            "parent_molregno": None,
            "molecule_hierarchy": None,
        }

        hierarchy = molecule.get("molecule_hierarchy")
        if isinstance(hierarchy, dict) and hierarchy:
            flattened["parent_chembl_id"] = hierarchy.get("parent_chembl_id")
            flattened["parent_molregno"] = hierarchy.get("parent_molregno")
            flattened["molecule_hierarchy"] = canonical_json(hierarchy)

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

            flattened["molecule_properties"] = canonical_json(props)

        return flattened

    @classmethod
    def _flatten_molecule_structures(cls, molecule: dict[str, Any]) -> dict[str, Any]:
        """Extract canonical molecular structures."""

        flattened: dict[str, Any] = {
            "standardized_smiles": None,
            "standard_inchi": None,
            "standard_inchi_key": None,
            "molecule_structures": None,
        }

        structures = molecule.get("molecule_structures")
        if isinstance(structures, dict) and structures:
            flattened["standardized_smiles"] = registry.normalize(
                "chemistry",
                structures.get("canonical_smiles"),
            )
            flattened["standard_inchi"] = registry.normalize(
                "chemistry",
                structures.get("standard_inchi"),
            )
            flattened["standard_inchi_key"] = registry.normalize(
                "chemistry.string",
                structures.get("standard_inchi_key"),
                uppercase=True,
            )
            flattened["molecule_structures"] = canonical_json(structures)

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

        flattened: dict[str, Any] = {
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
                flattened["molecule_synonyms"] = canonical_json(sorted_entries)

        return flattened

    @classmethod
    def _flatten_nested_json(cls, molecule: dict[str, Any], field_name: str) -> str | None:
        """Serialize nested JSON structure to canonical string."""

        value = molecule.get(field_name)
        if value in (None, ""):
            return None
        return canonical_json(value)

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 25

        chembl_context = self._init_chembl_client(
            defaults={
                "enabled": True,
                "base_url": default_base_url,
                "batch_size": default_batch_size,
            }
        )

        configured_batch_size = (
            chembl_context.source_config.batch_size
            if chembl_context.source_config.batch_size is not None
            else default_batch_size
        )
        try:
            batch_size_value = int(configured_batch_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("sources.chembl.batch_size must be an integer") from exc

        if batch_size_value <= 0:
            raise ValueError("sources.chembl.batch_size must be greater than zero")

        if batch_size_value > 25:
            raise ValueError("sources.chembl.batch_size must be <= 25 due to ChEMBL API limits")

        self.api_client = chembl_context.client
        self.register_client(self.api_client)
        self.batch_size = batch_size_value
        self.configured_max_url_length = chembl_context.max_url_length

        # Initialize external adapters (PubChem)
        self.external_adapters: dict[str, ExternalAdapter] = {}
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()
        self._molecule_cache: dict[str, dict[str, Any]] = {}
        self._fallback_builder = FallbackRecordBuilder(
            business_columns=tuple(self._expected_columns()),
            context={"chembl_release": self._chembl_release},
        )

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract molecule data from input file."""
        df, resolved_path = self.read_input_table(
            default_filename=Path("testitem.csv"),
            expected_columns=TestItemSchema.get_column_order(),
            input_file=input_file,
        )

        if not resolved_path.exists():
            return df

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

        batches: list[list[str]] = []
        for index in range(0, len(ids_to_fetch), self.batch_size):
            chunk = ids_to_fetch[index : index + self.batch_size]
            if not chunk:
                continue
            batches.extend(self._split_molecule_ids_by_url_length(chunk))

        for batch_index, batch_ids in enumerate(batches, start=1):
            logger.info("fetching_batch", batch=batch_index, size=len(batch_ids))

            try:
                params = {
                    "molecule_chembl_id__in": ",".join(batch_ids),
                    "limit": min(len(batch_ids), self.batch_size),
                }
                response = self.api_client.request_json("/molecule.json", params=params)
                molecules = response.get("molecules", [])

                returned_ids = {
                    m.get("molecule_chembl_id") for m in molecules if m.get("molecule_chembl_id")
                }
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
                        reason="batch_exception",
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

    def _split_molecule_ids_by_url_length(self, candidate_ids: Sequence[str]) -> list[list[str]]:
        """Recursively split molecule identifiers honoring the configured URL limit."""

        ids = [molid for molid in candidate_ids if molid]
        if not ids:
            return []

        limit = self.configured_max_url_length
        if limit is None:
            return [ids]

        limit_value = int(limit)
        url = self._build_molecule_request_url(ids)
        if not url:
            return [ids]

        if len(url) <= limit_value or len(ids) == 1:
            if len(url) > limit_value:
                logger.warning(
                    "testitem_single_id_exceeds_url_limit",
                    molecule_id=ids[0],
                    url_length=len(url),
                    max_length=limit_value,
                )
            return [ids]

        midpoint = max(1, len(ids) // 2)
        return self._split_molecule_ids_by_url_length(
            ids[:midpoint]
        ) + self._split_molecule_ids_by_url_length(ids[midpoint:])

    def _build_molecule_request_url(self, molecule_ids: Sequence[str]) -> str:
        """Construct the request URL for the given molecule identifiers."""

        base = str(self.api_client.config.base_url).rstrip("/")
        url = f"{base}/molecule.json"
        params = {
            "molecule_chembl_id__in": ",".join(molecule_ids),
            "limit": min(len(molecule_ids), self.batch_size),
        }
        query_string = urlencode(params)
        return f"{url}?{query_string}"

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
                reason="http_error",
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
                reason="unexpected_error",
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
                reason="missing_from_response",
                message="Missing molecule in response",
            )
            logger.warning(
                "molecule_missing_in_response", molecule_chembl_id=molecule_id, attempt=attempt
            )
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
        reason: str = "exception",
    ) -> dict[str, Any]:
        if retry_after is None and isinstance(error, requests.exceptions.HTTPError):
            retry_after = self._extract_retry_after(error)

        fallback_record = self._fallback_builder.record({"molecule_chembl_id": molecule_id})

        metadata = build_fallback_payload(
            entity="testitem",
            reason=reason,
            error=error,
            source="TESTITEM_FALLBACK",
            attempt=attempt,
            message=message,
            context=self._fallback_builder.context_with({"molecule_chembl_id": molecule_id}),
        )

        if retry_after is not None:
            metadata["fallback_retry_after_sec"] = retry_after

        metadata.setdefault("fallback_error_code", reason if reason else None)

        fallback_record.update(metadata)
        return fallback_record

    @staticmethod
    def _extract_retry_after(error: requests.exceptions.HTTPError) -> float | None:  # type: ignore[valid-type]
        if not hasattr(error, "response") or error.response is None:  # type: ignore[attr-defined]
            return None
        retry_after = error.response.headers.get("Retry-After")  # type: ignore[attr-defined]
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
        from bioetl.utils.chembl import SupportsRequestJson

        # Type cast to satisfy protocol compatibility
        client: SupportsRequestJson = cast(SupportsRequestJson, self.api_client)
        release = self._fetch_chembl_release_info(client)
        return release.version

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform molecule data with hash generation."""
        if df.empty:
            return df

        # Normalize identifiers
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
                normalized_df = molecule_data.drop_duplicates("molecule_chembl_id").set_index(
                    "molecule_chembl_id"
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
                    deduplicated_input = input_indexed[
                        ~input_indexed.index.duplicated(keep="first")
                    ]

                # Overlay normalized values while falling back to the original input when needed
                normalized_columns = [
                    column
                    for column in normalized_df.columns
                    if column not in {"molecule_chembl_id"}
                ]

                overlay_updates: dict[str, pd.Series] = {}

                for column in normalized_columns:
                    normalized_series = normalized_df[column]
                    if column in deduplicated_input.columns:
                        if normalized_series.empty:
                            combined_series = deduplicated_input[column]
                        else:
                            normalized_aligned = normalized_series.reindex(deduplicated_input.index)
                            combined_series = normalized_aligned.fillna(deduplicated_input[column])
                    else:
                        combined_series = normalized_series

                    overlay = df["molecule_chembl_id"].map(combined_series)
                    if column in df.columns:
                        if overlay.empty:
                            overlay_updates[column] = df[column]
                        else:
                            overlay_updates[column] = overlay.fillna(df[column])
                    else:
                        overlay_updates[column] = overlay

                if overlay_updates:
                    overlay_df = pd.DataFrame(overlay_updates, index=df.index)
                    remaining_columns = df.drop(
                        columns=[col for col in overlay_updates if col in df.columns],
                        errors="ignore",
                    )
                    df = pd.concat([remaining_columns, overlay_df], axis=1)

        canonical_column = "canonical_smiles"
        standardized_column = "standardized_smiles"

        if canonical_column in df.columns:
            canonical_series = df[canonical_column]
            normalized_canonical = canonical_series.apply(
                lambda value: registry.normalize("chemistry", value) if pd.notna(value) else None
            )

            if standardized_column in df.columns:
                missing_mask = df[standardized_column].isna()
                if missing_mask.any():
                    df.loc[missing_mask, standardized_column] = normalized_canonical[missing_mask]
            else:
                df[standardized_column] = normalized_canonical

            df = df.drop(columns=[canonical_column], errors="ignore")

        # PubChem enrichment (optional)
        if "pubchem" in self.external_adapters:
            logger.info("pubchem_enrichment_enabled")
            try:
                df = self._enrich_with_pubchem(df)
            except Exception as e:
                logger.error("pubchem_enrichment_failed", error=str(e))
                # Continue with original data - graceful degradation

        extraneous_columns = [
            "inchi_key_from_mol",
            "inchi_key_from_smiles",
            "is_radical",
            "mw_<100_or_>1000",
            "n_stereocenters",
            "nstereo",
            "salt_chembl_id",
            "standard_inchi_skeleton",
            "standard_inchi_stereo",
        ]
        df = df.drop(columns=extraneous_columns, errors="ignore")

        pipeline_version = getattr(self.config.pipeline, "version", None) or "1.0.0"
        default_source = "chembl"
        timestamp_now = pd.Timestamp.now(tz="UTC").isoformat()

        if "source_system" in df.columns:
            df["source_system"] = df["source_system"].fillna(default_source)
        else:
            df["source_system"] = default_source

        release_value: str | None = self._chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip() or None

        if release_value is None:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].where(
                    df["chembl_release"].notna(),
                    pd.NA,
                )
            else:
                df["chembl_release"] = pd.NA
        else:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].fillna(release_value)
            else:
                df["chembl_release"] = release_value

        if "extracted_at" in df.columns:
            df["extracted_at"] = df["extracted_at"].fillna(timestamp_now)
        else:
            df["extracted_at"] = timestamp_now

        df = finalize_output_dataset(
            df,
            business_key="molecule_chembl_id",
            sort_by=["molecule_chembl_id"],
            schema=TestItemSchema,
            metadata={
                "pipeline_version": pipeline_version,
                "run_id": self.run_id,
                "source_system": default_source,
                "chembl_release": release_value,
                "extracted_at": timestamp_now,
            },
        )

        coerce_optional_bool(df, columns=self._BOOLEAN_COLUMNS)

        default_minimums = dict.fromkeys(self._NULLABLE_INT_COLUMNS, 0)
        default_minimums.update(self._INT_COLUMN_MINIMUMS)
        coerce_nullable_int(
            df,
            self._NULLABLE_INT_COLUMNS,
            min_values=default_minimums,
        )

        return df

    def _init_external_adapters(self) -> None:
        """Initialize external API adapters."""
        sources = self.config.sources

        default_base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
        default_batch_size = 100
        default_rate_limit_max_calls = 5
        default_rate_limit_period = 1.0

        def _source_attr(source: Any, attr: str, default: Any) -> Any:
            if isinstance(source, dict):
                return source.get(attr, default)
            if source is None:
                return default
            return getattr(source, attr, default)

        def _coerce_int(value: Any, default: int, *, minimum: int = 1, field: str) -> int:
            if value is None:
                return default
            try:
                candidate = int(value)
            except (TypeError, ValueError):
                logger.warning("pubchem_config_invalid_int", field=field, value=value, default=default)
                return default
            if candidate < minimum:
                logger.warning(
                    "pubchem_config_out_of_range",
                    field=field,
                    value=candidate,
                    minimum=minimum,
                    default=default,
                )
                return default
            return candidate

        def _coerce_float(value: Any, default: float, *, minimum: float = 0.0, field: str) -> float:
            if value is None:
                return default
            try:
                candidate = float(value)
            except (TypeError, ValueError):
                logger.warning("pubchem_config_invalid_float", field=field, value=value, default=default)
                return default
            if candidate <= minimum:
                logger.warning(
                    "pubchem_config_out_of_range",
                    field=field,
                    value=candidate,
                    minimum=minimum,
                    default=default,
                )
                return default
            return candidate

        pubchem_source = sources.get("pubchem") if sources is not None else None

        base_url = _source_attr(pubchem_source, "base_url", default_base_url)
        rate_limit_max_calls_raw = _source_attr(
            pubchem_source, "rate_limit_max_calls", default_rate_limit_max_calls
        )
        rate_limit_period_raw = _source_attr(
            pubchem_source, "rate_limit_period", default_rate_limit_period
        )
        batch_size_raw = _source_attr(pubchem_source, "batch_size", default_batch_size)
        headers_raw = _source_attr(pubchem_source, "headers", {})
        rate_limit_jitter = bool(_source_attr(pubchem_source, "rate_limit_jitter", True))

        if pubchem_source is not None and not _source_attr(pubchem_source, "enabled", True):
            logger.warning("pubchem_adapter_force_enabled", reason="explicit_disable_ignored")

        headers: dict[str, Any]
        if isinstance(headers_raw, dict):
            headers = dict(headers_raw)
        else:
            headers = {}
            logger.warning("pubchem_config_invalid_headers", headers_type=type(headers_raw).__name__)

        http_profile = _source_attr(pubchem_source, "http", None)
        if http_profile is not None:
            http_headers = _source_attr(http_profile, "headers", None)
            if isinstance(http_headers, dict):
                headers.update({str(key): str(value) for key, value in http_headers.items()})

            http_rate_limit = _source_attr(http_profile, "rate_limit", None)
            if http_rate_limit is not None:
                rate_limit_max_calls_raw = _source_attr(
                    http_rate_limit, "max_calls", rate_limit_max_calls_raw
                )
                rate_limit_period_raw = _source_attr(
                    http_rate_limit, "period", rate_limit_period_raw
                )

        cache_maxsize = getattr(self.config.cache, "maxsize", None)
        if cache_maxsize is None:
            cache_maxsize = APIConfig.__dataclass_fields__["cache_maxsize"].default  # type: ignore[index]

        rate_limit_max_calls = _coerce_int(
            rate_limit_max_calls_raw,
            default_rate_limit_max_calls,
            minimum=1,
            field="rate_limit_max_calls",
        )
        rate_limit_period = _coerce_float(
            rate_limit_period_raw,
            default_rate_limit_period,
            minimum=0.0,
            field="rate_limit_period",
        )
        batch_size = _coerce_int(batch_size_raw, default_batch_size, minimum=1, field="batch_size")

        workers_raw = _source_attr(pubchem_source, "workers", 1)
        workers = _coerce_int(workers_raw, 1, minimum=1, field="workers")

        adapter_kwargs: dict[str, Any] = {
            "enabled": True,
            "batch_size": batch_size,
            "workers": workers,
        }
        for optional_field in ("tool", "email", "api_key", "mailto"):
            optional_value = _source_attr(pubchem_source, optional_field, None)
            if optional_value:
                adapter_kwargs[optional_field] = optional_value

        pubchem_config = APIConfig(
            name="pubchem",
            base_url=base_url,
            headers=headers,
            cache_enabled=self.config.cache.enabled,
            cache_ttl=self.config.cache.ttl,
            cache_maxsize=cache_maxsize,
            rate_limit_max_calls=rate_limit_max_calls,
            rate_limit_period=rate_limit_period,
            rate_limit_jitter=rate_limit_jitter,
        )

        adapter_config = AdapterConfig(**adapter_kwargs)
        self.external_adapters["pubchem"] = PubChemAdapter(pubchem_config, adapter_config)
        logger.info(
            "pubchem_adapter_initialized",
            base_url=base_url,
            batch_size=batch_size,
            rate_limit_max_calls=rate_limit_max_calls,
            rate_limit_period=rate_limit_period,
        )

        logger.info("adapters_initialized", count=len(self.external_adapters))

    def close_resources(self) -> None:
        """Close the PubChem adapter alongside inherited resources."""

        for name, adapter in getattr(self, "external_adapters", {}).items():
            self._close_resource(adapter, resource_name=f"external_adapter.{name}")

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

        # Pre-enrichment coverage logging and QC metric
        total_rows = int(len(df))
        inchi_present = int(df["standard_inchi_key"].notna().sum())
        inchi_coverage = float(inchi_present / total_rows) if total_rows else 0.0
        logger.info(
            "pubchem_inchikey_coverage",
            total_rows=total_rows,
            present=inchi_present,
            coverage=inchi_coverage,
        )

        qc_cfg = getattr(self.config, "qc", None)
        thresholds: dict[str, Any] = getattr(qc_cfg, "thresholds", {}) if qc_cfg is not None else {}
        min_inchikey_cov = float(thresholds.get("testitem.pubchem_min_inchikey_coverage", 0.0))
        inchikey_metric = {
            "count": inchi_present,
            "value": inchi_coverage,
            "threshold": min_inchikey_cov,
            "passed": inchi_coverage >= min_inchikey_cov,
            "severity": "warning" if inchi_coverage < min_inchikey_cov else "info",
        }
        update_summary_metrics(self.qc_summary_data, {"pubchem.inchikey_coverage": inchikey_metric})

        if inchi_present == 0:
            logger.warning(
                "pubchem_enrichment_skipped_no_inchikey",
                advice="Убедитесь, что из ChEMBL приходит molecule_structures.standard_inchi_key",
            )
            return df

        pubchem_adapter = self.external_adapters["pubchem"]

        try:
            # Type cast to PubChemAdapter for specific method access
            enriched_df = cast(PubChemAdapter, pubchem_adapter).enrich_with_pubchem(
                df, inchi_key_col="standard_inchi_key"
            )
            # Post-enrichment metrics
            if "pubchem_cid" in enriched_df.columns:
                enriched_rows = int(enriched_df["pubchem_cid"].notna().sum())
                enrichment_rate = float(enriched_rows / len(enriched_df)) if len(enriched_df) else 0.0
                logger.info(
                    "pubchem_enrichment_metrics",
                    enriched_rows=enriched_rows,
                    total_rows=int(len(enriched_df)),
                    enrichment_rate=enrichment_rate,
                )
                update_summary_metrics(
                    self.qc_summary_data,
                    {
                        "pubchem.enrichment_rate": {
                            "count": enriched_rows,
                            "value": enrichment_rate,
                            "threshold": float(thresholds.get("testitem.pubchem_min_enrichment_rate", 0.0)),
                            "passed": enrichment_rate >= float(
                                thresholds.get("testitem.pubchem_min_enrichment_rate", 0.0)
                            ),
                            "severity": "warning"
                            if enrichment_rate
                            < float(thresholds.get("testitem.pubchem_min_enrichment_rate", 0.0))
                            else "info",
                        }
                    },
                )
            return enriched_df  # type: ignore[no-any-return]
        except Exception as e:
            logger.error("pubchem_enrichment_error", error=str(e))
            # Return original dataframe on error - graceful degradation
            return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate molecule data against schema and QC policies."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            update_summary_section(self.qc_summary_data, "row_counts", {"testitems": 0})
            update_summary_section(
                self.qc_summary_data,
                "datasets",
                {"testitems": {"rows": 0}},
            )
            update_summary_section(
                self.qc_summary_data,
                "duplicates",
                {"testitems": duplicate_summary(0, 0, field="molecule_chembl_id")},
            )
            update_validation_issue_summary(self.qc_summary_data, self.validation_issues)
            return df

        initial_rows = int(len(df))

        qc_metrics = self._calculate_qc_metrics(df)
        self.qc_metrics = qc_metrics
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

        update_summary_metrics(self.qc_summary_data, qc_metrics)

        duplicates_metric = qc_metrics.get("testitem.duplicate_ratio")
        if (
            duplicates_metric
            and duplicates_metric.get("count")
            and "molecule_chembl_id" in df.columns
        ):
            df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

        coerce_retry_after(df)

        def _testitem_error_adapter(
            issues: list[dict[str, Any]],
            exc: Exception,
            should_fail: bool,
        ) -> Exception | None:
            if not should_fail:
                return None

            if not issues:
                return ValueError("Schema validation failed")

            summary = "; ".join(
                f"{issue.get('column')}: {issue.get('check')} ({issue.get('count')} cases)"
                for issue in issues
            )
            return ValueError(f"Schema validation failed: {summary}")

        def _refresh_qc_summary(validated_df: pd.DataFrame) -> None:
            row_count = int(len(validated_df))
            update_summary_section(self.qc_summary_data, "row_counts", {"testitems": row_count})
            update_summary_section(
                self.qc_summary_data,
                "datasets",
                {"testitems": {"rows": row_count}},
            )

            duplicate_count = int(qc_metrics.get("testitem.duplicate_ratio", {}).get("count", 0) or 0)
            update_summary_section(
                self.qc_summary_data,
                "duplicates",
                {
                    "testitems": duplicate_summary(
                        initial_rows,
                        duplicate_count,
                        field="molecule_chembl_id",
                        threshold=qc_metrics.get("testitem.duplicate_ratio", {}).get("threshold"),
                    )
                },
            )
            update_validation_issue_summary(self.qc_summary_data, self.validation_issues)

        validated_df = self.run_schema_validation(
            df,
            TestItemSchema,
            dataset_name="testitems",
            severity="error",
            metric_name="schema.validation",
            success_callbacks=(_refresh_qc_summary,),
            error_adapter=_testitem_error_adapter,
        )

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

    def _check_referential_integrity(self, df: pd.DataFrame) -> None:
        """Ensure parent_chembl_id values resolve to known molecules."""

        required_columns = {"molecule_chembl_id", "parent_chembl_id"}
        if df.empty or not required_columns.issubset(df.columns):
            logger.debug("referential_check_skipped", reason="columns_absent")
            return

        parent_series = df["parent_chembl_id"].apply(
            lambda raw: (registry.normalize("chemistry.chembl_id", raw) if pd.notna(raw) else None)
        )
        parent_series = parent_series.dropna()
        if parent_series.empty:
            logger.info("referential_integrity_passed", relation="testitem->parent", checked=0)
            return

        molecule_ids = df["molecule_chembl_id"].apply(
            lambda raw: (registry.normalize("chemistry.chembl_id", raw) if pd.notna(raw) else None)
        )
        known_ids = {value for value in molecule_ids.tolist() if value}
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
