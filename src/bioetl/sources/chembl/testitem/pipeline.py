"""TestItem Pipeline - ChEMBL molecule data extraction."""

import re
from collections.abc import Hashable
from pathlib import Path
from typing import Any, cast

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.input_schemas import TestItemInputSchema
from bioetl.schemas.registry import schema_registry
from bioetl.schemas.testitem import TestItemSchema
from bioetl.sources.chembl.testitem.client import TestItemChEMBLClient
from bioetl.sources.chembl.testitem.normalizer import (
    coerce_boolean_and_integer_columns,
    normalize_smiles_columns,
)
from bioetl.sources.chembl.testitem.output import build_duplicate_summary, calculate_fallback_stats
from bioetl.sources.chembl.testitem.parser import TestItemParser
from bioetl.sources.chembl.testitem.request import TestItemRequestBuilder
from bioetl.sources.pubchem.client.pubchem_client import PubChemClient
from bioetl.sources.pubchem.normalizer.pubchem_normalizer import PubChemNormalizer
from bioetl.utils.dtypes import coerce_retry_after
from bioetl.utils.fallback import FallbackRecordBuilder
from bioetl.utils.qc import (
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

schema_registry.register("testitem", "1.0.0", TestItemSchema)

__all__ = ["TestItemPipeline"]

logger = UnifiedLogger.get(__name__)

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

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 25

        chembl_context = self.init_chembl_client(
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

        # Initialize PubChem enrichment helpers
        self.pubchem_client: PubChemClient | None = None
        self._pubchem_api_client = None
        self.pubchem_normalizer = PubChemNormalizer()
        self._init_external_adapters()

        # Cache ChEMBL release version
        self._chembl_release = self._get_chembl_release()
        self._molecule_cache: dict[str, dict[str, Any]] = {}
        self._fallback_builder = FallbackRecordBuilder(
            business_columns=tuple(self._expected_columns()),
            context={"chembl_release": self._chembl_release},
        )

        self.request_builder = TestItemRequestBuilder(
            api_client=self.api_client,
            batch_size=self.batch_size,
            max_url_length=self.configured_max_url_length,
        )
        self.parser = TestItemParser(
            expected_columns=self._expected_columns(),
            property_fields=self._CHEMBL_PROPERTY_FIELDS,
            structure_fields=self._CHEMBL_STRUCTURE_FIELDS,
            json_fields=self._CHEMBL_JSON_FIELDS,
            text_fields=self._CHEMBL_TEXT_FIELDS,
            fallback_fields=self._FALLBACK_FIELDS,
        )
        self.chembl_client = TestItemChEMBLClient(
            api_client=self.api_client,
            batch_size=self.batch_size,
            chembl_release=self._chembl_release,
            molecule_cache=self._molecule_cache,
            request_builder=self.request_builder,
            parser=self.parser,
            fallback_builder=self._fallback_builder,
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

        if not df.empty:
            schema_columns = TestItemInputSchema.to_schema().columns.keys()
            for column in schema_columns:
                if column not in df.columns:
                    df[column] = pd.Series(pd.NA, index=df.index)
            df = df.convert_dtypes()
            df = TestItemInputSchema.validate(df, lazy=True)

        logger.info("extraction_completed", rows=len(df), columns=len(df.columns))
        return df

    def _fetch_molecule_data(self, molecule_ids: list[str]) -> pd.DataFrame:
        """Fetch molecule data from ChEMBL API with release-scoped caching."""
        if not molecule_ids:
            logger.warning("no_molecule_ids_provided")
            return pd.DataFrame()

        records, stats = self.chembl_client.fetch_molecules(molecule_ids)

        if not records:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        logger.info(
            "molecule_fetch_summary",
            requested=len(molecule_ids),
            fetched=len(records),
            cache_hits=stats.get("cache_hits", 0),
            api_success_count=stats.get("api_success_count", 0),
            fallback_count=stats.get("fallback_count", 0),
        )

        return pd.DataFrame(records)

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
                    duplicate_counts: dict[Hashable, int] = {}
                    duplicated_ids: list[str] = []
                    for label in input_indexed.index.tolist():
                        if not isinstance(label, Hashable):
                            logger.debug(
                                "non_hashable_index_label",
                                label_type=type(label).__name__,
                            )
                            continue
                        count = duplicate_counts.get(label, 0) + 1
                        duplicate_counts[label] = count
                        if count == 2:
                            duplicated_ids.append(str(label))

                    if duplicated_ids:
                        logger.warning(
                            "duplicate_molecule_ids_in_input",
                            count=len(duplicated_ids),
                            sample=duplicated_ids[:5],
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

        df = normalize_smiles_columns(df)

        # PubChem enrichment (optional)
        if self.pubchem_client is not None:
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

        default_source = "chembl"

        release_value: str | None = self._chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip() or None

        df = self.finalize_with_standard_metadata(
            df,
            business_key="molecule_chembl_id",
            sort_by=["molecule_chembl_id"],
            schema=TestItemSchema,
            default_source=default_source,
            chembl_release=release_value,
        )

        df = coerce_boolean_and_integer_columns(
            df,
            boolean_columns=self._BOOLEAN_COLUMNS,
            nullable_int_columns=self._NULLABLE_INT_COLUMNS,
            int_minimums=self._INT_COLUMN_MINIMUMS,
        )

        return df

    def _init_external_adapters(self) -> None:
        """Initialise the optional PubChem enrichment client."""

        client, api_client = PubChemClient.from_config(self.config)
        self.pubchem_client = client
        self._pubchem_api_client = api_client

        if api_client is not None:
            self.register_client(api_client)

        if client is None:
            logger.info("pubchem_client_unavailable")
        else:
            logger.info("pubchem_client_initialized", batch_size=client.batch_size)

    def close_resources(self) -> None:
        """Close the PubChem client alongside inherited resources."""

        if self._pubchem_api_client is not None:
            self._close_resource(self._pubchem_api_client, resource_name="api_client.pubchem")

    def _enrich_with_pubchem(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich testitem data with PubChem properties.

        Args:
            df: DataFrame with molecule data from ChEMBL

        Returns:
            DataFrame enriched with pubchem_* fields
        """
        client = self.pubchem_client
        if client is None:
            logger.warning("pubchem_client_not_available")
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
            return self.pubchem_normalizer.ensure_columns(df)

        try:
            enriched_df = self.pubchem_normalizer.enrich_dataframe(
                df,
                inchi_key_col="standard_inchi_key",
                client=client,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("pubchem_enrichment_exception", error=str(exc))
            return df

        enriched_df = self.pubchem_normalizer.ensure_columns(enriched_df)
        enriched_df = self.pubchem_normalizer.normalize_types(enriched_df)

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
                        "passed": enrichment_rate
                        >= float(thresholds.get("testitem.pubchem_min_enrichment_rate", 0.0)),
                        "severity": "warning"
                        if enrichment_rate
                        < float(thresholds.get("testitem.pubchem_min_enrichment_rate", 0.0))
                        else "info",
                    }
                },
            )

        return enriched_df

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
                {
                    "testitems": build_duplicate_summary(
                        df,
                        field="molecule_chembl_id",
                        initial_rows=0,
                        threshold=None,
                    )
                },
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

            threshold = qc_metrics.get("testitem.duplicate_ratio", {}).get("threshold")
            update_summary_section(
                self.qc_summary_data,
                "duplicates",
                {
                    "testitems": build_duplicate_summary(
                        validated_df,
                        field="molecule_chembl_id",
                        initial_rows=initial_rows,
                        threshold=threshold,
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

        fallback_count, fallback_ratio = calculate_fallback_stats(df)

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
