"""Target Pipeline - ChEMBL target data extraction with multi-stage enrichment."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import (
    EnrichmentStage,
    PipelineBase,
    enrichment_stage_registry,
)
from bioetl.schemas.chembl_target import (
    ProteinClassSchema,
    TargetComponentSchema,
    TargetSchema,
    XrefSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.sources.iuphar.pagination import PageNumberPaginator
from bioetl.sources.iuphar.service import IupharService, IupharServiceConfig

# Import UniProtSearchClient, UniProtIdMappingClient and UniProtOrthologClient directly from client.py module (not from client package)
# The client package __init__.py exports from search_client.py/idmapping_client.py/orthologs_client.py which have different APIs
# We need the version from client.py which has (client, fields, batch_size) API
try:
    import importlib.util
    import sys

    # Get the path to client.py
    uniprot_dir = Path(__file__).parent.parent.parent / "uniprot"
    client_py_file = uniprot_dir / "client.py"

    if client_py_file.exists():
        # Check if already loaded by checking both the file path and the module name
        module_key = str(client_py_file)
        module_name = "bioetl.sources.uniprot._client_py_module"
        # Check if already loaded in sys.modules by file path or module name
        if module_key in sys.modules:
            client_module = sys.modules[module_key]
        elif module_name in sys.modules:
            client_module = sys.modules[module_name]
        else:
            # Load the module directly from the file
            # Use a different name in sys.modules to avoid overwriting the package
            spec = importlib.util.spec_from_file_location(module_name, client_py_file)
            if spec and spec.loader:
                client_module = importlib.util.module_from_spec(spec)
                # Set proper module attributes for imports to work
                client_module.__name__ = module_name
                client_module.__package__ = "bioetl.sources.uniprot"
                client_module.__file__ = str(client_py_file)
                # Register in sys.modules under both file path and module name
                sys.modules[module_key] = client_module
                sys.modules[module_name] = client_module
                # Execute the module
                spec.loader.exec_module(client_module)
            else:
                raise ImportError("Could not create module spec")
        UniProtSearchClient = client_module.UniProtSearchClient
        UniProtIdMappingClient = client_module.UniProtIdMappingClient
        # In client.py, the class is called UniProtOrthologClient (without 's')
        UniProtOrthologsClient = client_module.UniProtOrthologClient
    else:
        raise FileNotFoundError(f"client.py not found at {client_py_file}")
except Exception:
    # Fallback to package import (will use search_client.py/idmapping_client.py/orthologs_client.py versions)
    from bioetl.sources.uniprot.client import (
        UniProtIdMappingClient,
        UniProtOrthologsClient,
        UniProtSearchClient,
    )
from bioetl.sources.chembl.target.client import TargetClientManager
from bioetl.sources.chembl.target.merge import build_gold_outputs
from bioetl.sources.chembl.target.normalizer import (
    MissingMappingRecorder,
    TargetEnricher,
)
from bioetl.sources.chembl.target.output import TargetOutputService
from bioetl.sources.chembl.target.request import IupharRequestBuilder
from bioetl.utils.qc import (
    prepare_missing_mappings,
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

schema_registry.register("target", "1.0.0", TargetSchema)  # type: ignore[arg-type]

__all__ = ["TargetPipeline"]

logger = UnifiedLogger.get(__name__)

class TargetPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL target data with multi-stage enrichment.

    Stages:
    1. ChEMBL extraction (primary)
    2. UniProt enrichment (optional)
    3. IUPHAR enrichment (optional)
    4. Post-processing and materialization
    """

    _UNIPROT_FIELDS = (
        "accession,primaryAccession,secondaryAccession,protein_name,gene_primary,gene_synonym,"
        "organism_name,organism_id,lineage,sequence_length,cc_subcellular_location,cc_ptm,features"
    )
    _ORTHOLOG_FIELDS = "accession,organism_name,organism_id,protein_name"
    _UNIPROT_BATCH_SIZE = 50
    _IDMAPPING_BATCH_SIZE = 200
    _IDMAPPING_POLL_INTERVAL = 2.0
    _IDMAPPING_MAX_WAIT = 120.0
    _ORTHOLOG_PRIORITY = {
        "9606": 0,  # Homo sapiens
        "10090": 1,  # Mus musculus
        "10116": 2,  # Rattus norvegicus
    }
    _IUPHAR_PAGE_SIZE = 200
    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = TargetSchema

        client_manager = TargetClientManager(
            config,
            self.register_client,
            defaults={
                "enabled": True,
                "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                "batch_size": 25,
            },
        )
        self.client_manager = client_manager
        self.source_configs = dict(client_manager.source_configs)
        self.api_clients = dict(client_manager.api_clients)

        self.chembl_client = client_manager.get_client("chembl")
        self.uniprot_client = client_manager.get_client("uniprot")
        self.uniprot_idmapping_client = client_manager.get_client("uniprot_idmapping")
        self.uniprot_orthologs_client = client_manager.get_client("uniprot_orthologs")
        self.iuphar_client = client_manager.get_client("iuphar")
        self.iuphar_paginator: PageNumberPaginator | None = None
        if self.iuphar_client is not None:
            iuphar_config = client_manager.get_source_config("iuphar")
            batch_size = getattr(iuphar_config, "batch_size", None)
            if batch_size:
                try:
                    page_size = int(batch_size)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    page_size = self._IUPHAR_PAGE_SIZE
            else:
                page_size = self._IUPHAR_PAGE_SIZE
            self.iuphar_paginator = PageNumberPaginator(self.iuphar_client, page_size)
        else:
            page_size = self._IUPHAR_PAGE_SIZE
        self.iuphar_request_builder = IupharRequestBuilder(page_size=page_size)
        self.iuphar_service = IupharService(
            config=IupharServiceConfig(
                identifier_column="target_chembl_id",
                candidate_columns=("pref_name", "target_names"),
                gene_symbol_columns=("uniprot_gene_primary", "gene_symbol"),
                fallback_source="chembl",
            ),
            record_missing_mapping=self._record_missing_mapping,
        )

        self.uniprot_search_client = UniProtSearchClient(
            client=self.uniprot_client,
            fields=self._UNIPROT_FIELDS,
            batch_size=self._UNIPROT_BATCH_SIZE,
        )
        self.uniprot_id_mapping_client = UniProtIdMappingClient(
            client=self.uniprot_idmapping_client,
            batch_size=self._IDMAPPING_BATCH_SIZE,
            poll_interval=self._IDMAPPING_POLL_INTERVAL,
            max_wait=self._IDMAPPING_MAX_WAIT,
        )
        ortholog_base_client = self.uniprot_orthologs_client or self.uniprot_client
        self.uniprot_ortholog_client = UniProtOrthologsClient(
            client=ortholog_base_client,
            fields=self._ORTHOLOG_FIELDS,
            priority_map=self._ORTHOLOG_PRIORITY,
        )
        self.missing_mapping_recorder = MissingMappingRecorder()
        self.enricher = TargetEnricher(
            uniprot_search_client=self.uniprot_search_client,
            uniprot_id_mapping_client=self.uniprot_id_mapping_client,
            uniprot_ortholog_client=self.uniprot_ortholog_client,
            iuphar_service=self.iuphar_service,
            iuphar_paginator=self.iuphar_paginator,
        )
        self.uniprot_normalizer = self.enricher.uniprot_normalizer

        # Backwards compatibility
        self.api_client = self.chembl_client

        self.batch_size = client_manager.batch_size
        self._chembl_release = self._get_chembl_release() if self.chembl_client else None

        self.gold_targets: pd.DataFrame = pd.DataFrame()
        self.gold_components: pd.DataFrame = pd.DataFrame()
        self.gold_protein_class: pd.DataFrame = pd.DataFrame()
        self.gold_xref: pd.DataFrame = pd.DataFrame()
        runtime_config = getattr(self.config, "runtime", None)
        self.output_service = TargetOutputService(
            pipeline_config=config,
            run_id=self.run_id,
            stage_context=self.stage_context,
            determinism=self.determinism,
            runtime_config=runtime_config,
        )

    def close_resources(self) -> None:
        """Close API clients constructed by the target pipeline."""

        for name, client in getattr(self, "api_clients", {}).items():
            self._close_resource(client, resource_name=f"api_client.{name}")

    def _record_missing_mapping(
        self,
        *,
        stage: str,
        target_id: Any | None,
        accession: Any | None,
        resolution: str,
        status: str,
        resolved_accession: Any | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Capture missing mapping events for QC artifacts."""

        self.missing_mapping_recorder.record(
            stage=stage,
            target_id=target_id,
            accession=accession,
            resolution=resolution,
            status=status,
            resolved_accession=resolved_accession,
            details=details,
        )

    def _get_chembl_release(self) -> str | None:
        """Fetch the ChEMBL database release identifier."""

        release = self._fetch_chembl_release_info(self.chembl_client)
        return release.version

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract target data from input file."""
        Path("target.csv")
        expected_columns = [
            "target_chembl_id",
            "pref_name",
            "target_type",
            "organism",
            "taxonomy",
            "hgnc_id",
            "uniprot_accession",
            "iuphar_type",
            "iuphar_class",
            "iuphar_subclass",
        ]

        df, resolved_path = self.read_input_table(
            default_filename=Path("target.csv"),
            expected_columns=expected_columns,
            input_file=input_file,
        )

        if not resolved_path.exists():
            return df

        logger.info("extraction_completed", rows=len(df))
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform target data."""
        if df.empty:
            return df

        df = df.copy()
        self.missing_mapping_recorder.reset()
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        self.qc_summary_data = {}

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["target_chembl_id", "hgnc_id", "uniprot_accession"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Normalize names
        if "pref_name" in df.columns:
            df["pref_name"] = df["pref_name"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        with_uniprot = bool(self.runtime_options.get("with_uniprot", True))
        with_iuphar = bool(self.runtime_options.get("with_iuphar", True))
        self.runtime_options["with_uniprot"] = with_uniprot
        self.runtime_options["with_iuphar"] = with_iuphar

        logger.info(
            "enrichment_stages_configured",
            with_uniprot=with_uniprot,
            with_iuphar=with_iuphar,
        )

        self.reset_stage_context()
        df = self.execute_enrichment_stages(df)

        uniprot_context = self.stage_context.get("uniprot", {})
        uniprot_silver = uniprot_context.get("silver")
        if not isinstance(uniprot_silver, pd.DataFrame):
            uniprot_silver = pd.DataFrame()
        component_enrichment = uniprot_context.get("component_enrichment")
        if not isinstance(component_enrichment, pd.DataFrame):
            component_enrichment = pd.DataFrame()

        iuphar_context = self.stage_context.get("iuphar", {})
        iuphar_classification = iuphar_context.get("classification")
        if not isinstance(iuphar_classification, pd.DataFrame):
            iuphar_classification = pd.DataFrame()
        iuphar_gold = iuphar_context.get("gold")
        if not isinstance(iuphar_gold, pd.DataFrame):
            iuphar_gold = pd.DataFrame()

        timestamp = pd.Timestamp.now(tz="UTC").isoformat()
        pipeline_version = self.config.pipeline.version
        source_system = "chembl"

        gold_targets, gold_components, gold_protein_class, gold_xref = build_gold_outputs(
            df,
            component_enrichment,
            iuphar_gold,
        )

        sort_config = getattr(self.config, "determinism", None)
        sort_settings = getattr(sort_config, "sort", None)
        sort_columns = list(getattr(sort_settings, "by", []) or [])
        if not sort_columns:
            sort_columns = ["target_chembl_id"]
        ascending_flags = list(getattr(sort_settings, "ascending", []) or [])
        ascending_param = ascending_flags if ascending_flags else None

        gold_targets = self.output_service.finalize_targets(
            gold_targets,
            business_key="target_chembl_id",
            sort_by=sort_columns,
            ascending=ascending_param,
            schema=TargetSchema,
            metadata={
                "pipeline_version": pipeline_version,
                "run_id": self.run_id,
                "source_system": source_system,
                "chembl_release": self._chembl_release,
                "extracted_at": timestamp,
            },
        )

        self.gold_targets = gold_targets
        self.gold_components = gold_components
        self.gold_protein_class = gold_protein_class
        self.gold_xref = gold_xref

        self.reset_additional_tables()
        if not gold_components.empty:
            self.add_additional_table(
                "target_components",
                gold_components,
                formats=("csv", "parquet"),
            )
        if not gold_protein_class.empty:
            self.add_additional_table(
                "target_protein_classifications",
                gold_protein_class,
                formats=("csv", "parquet"),
            )
        if not gold_xref.empty:
            self.add_additional_table(
                "target_xrefs",
                gold_xref,
                formats=("csv", "parquet"),
            )
        if not component_enrichment.empty:
            self.add_additional_table(
                "target_component_enrichment",
                component_enrichment,
                formats=("csv", "parquet"),
            )
        if not iuphar_classification.empty:
            self.add_additional_table(
                "target_iuphar_classification",
                iuphar_classification,
                formats=("csv", "parquet"),
            )
        if not iuphar_gold.empty:
            self.add_additional_table(
                "target_iuphar_enrichment",
                iuphar_gold,
                formats=("csv", "parquet"),
            )

        self.set_export_metadata_from_dataframe(
            gold_targets,
            pipeline_version=pipeline_version,
            source_system=source_system,
            chembl_release=self._chembl_release,
            column_order=list(gold_targets.columns),
        )

        missing_records = self.missing_mapping_recorder.records
        if missing_records:
            self.qc_missing_mappings = prepare_missing_mappings(missing_records)
        else:
            self.qc_missing_mappings = pd.DataFrame()

        self._evaluate_enrichment_thresholds()
        self._update_qc_summary(
            gold_targets,
            gold_components,
            gold_protein_class,
            gold_xref,
        )

        if self.qc_metrics:
            logger.info("qc_metrics_collected", metrics=self.qc_metrics)

        self._materialize_gold_outputs(
            gold_targets,
            gold_components,
            gold_protein_class,
            gold_xref,
        )

        return gold_targets

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate target data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self.qc_summary_data.setdefault("validation", {})["targets"] = {
                "status": "skipped",
                "rows": 0,
            }
            return df

        duplicate_count = 0
        if "target_chembl_id" in df.columns:
            duplicate_count = int(df["target_chembl_id"].duplicated().sum())
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                df = df.drop_duplicates(subset=["target_chembl_id"], keep="first")
                self.record_validation_issue(
                    {
                        "metric": "targets.duplicates_removed",
                        "issue_type": "deduplication",
                        "severity": "warning",
                        "count": duplicate_count,
                    }
                )

        validated_targets = self.run_schema_validation(
            df,
            TargetSchema,
            dataset_name="targets",
            severity="critical",
        )
        self.gold_targets = validated_targets

        self.gold_components = self.run_schema_validation(
            self.gold_components,
            TargetComponentSchema,
            dataset_name="target_components",
        )

        self.gold_protein_class = self.run_schema_validation(
            self.gold_protein_class,
            ProteinClassSchema,
            dataset_name="protein_classifications",
        )

        self.gold_xref = self.run_schema_validation(
            self.gold_xref,
            XrefSchema,
            dataset_name="target_xrefs",
        )

        self._update_qc_summary(
            self.gold_targets,
            self.gold_components,
            self.gold_protein_class,
            self.gold_xref,
        )

        logger.info(
            "validation_completed",
            rows=len(self.gold_targets),
            duplicates_removed=duplicate_count,
        )
        return self.gold_targets

    def enrich_uniprot(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich the target dataframe using UniProt entries via the shared service."""

        return self.enricher.enrich_uniprot(
            df,
            record_missing_mapping=self._record_missing_mapping,
            record_validation_issue=self.record_validation_issue,
        )

    def enrich_iuphar(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich target dataframe with IUPHAR classifications."""

        enriched_df, classification_df, gold_df, metrics, targets, families = self.enricher.enrich_iuphar(
            df,
            request_builder=self.iuphar_request_builder,
        )

        self.stage_context["raw_targets"] = targets
        self.stage_context["raw_families"] = families

        return enriched_df, classification_df, gold_df, metrics

    def _fetch_iuphar_collection(
        self,
        path: str,
        unique_key: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Delegate collection fetches via the shared enricher instance."""

        params = params or {}
        return self.enricher.fetch_collection(path, unique_key=unique_key, params=params)

    def _build_family_hierarchy(
        self, families: dict[int, dict[str, Any]]
    ) -> dict[int, dict[str, list[Any]]]:
        return self.enricher.build_family_hierarchy(families)

    def _normalize_iuphar_name(self, value: str | None) -> str:
        """Normalize identifiers for fuzzy matching."""

        return self.enricher.normalize_iuphar_name(value)

    def _candidate_names_from_row(self, row: pd.Series) -> list[str]:
        """Extract candidate names for IUPHAR matching from a row."""

        return self.enricher.candidate_names_from_row(row)

    def _fallback_classification_record(self, row: pd.Series) -> dict[str, Any]:
        """Construct a fallback classification record for unmatched targets."""

        return self.enricher.fallback_classification_record(row)

    def _select_best_classification(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """Select the best classification entry for a matched target."""

        return self.enricher.select_best_classification(records)

    def materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
    ) -> None:
        """Persist silver-level UniProt artifacts deterministically."""

        format_name = self._resolve_materialization_format()
        self.output_service.materialize_silver(
            uniprot_df,
            component_df,
            format_name=format_name,
        )

    def materialize_iuphar(
        self,
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
    ) -> None:
        """Persist IUPHAR classification artifacts."""

        format_name = self._resolve_materialization_format()
        self.output_service.materialize_iuphar(
            classification_df,
            gold_df,
            format_name=format_name,
        )

    def _materialize_gold_outputs(
        self,
        targets_df: pd.DataFrame,
        components_df: pd.DataFrame,
        protein_class_df: pd.DataFrame,
        xref_df: pd.DataFrame,
    ) -> None:
        """Persist gold-level DataFrames respecting runtime configuration."""

        format_name = self._resolve_materialization_format()
        gold_path = self.config.materialization.resolve_dataset_path("gold", "targets", format_name)
        self.output_service.materialize_gold(
            targets_df,
            components_df,
            protein_class_df,
            xref_df,
            format_name=format_name,
            output_path=gold_path,
        )

    def _resolve_materialization_format(self) -> str:
        """Determine the requested output format for materialization artefacts."""

        format_name = self.output_service.resolve_materialization_format(self.runtime_options)
        if format_name not in {"csv", "parquet"}:
            logger.warning(
                "unsupported_materialization_format",
                format=self.runtime_options.get("materialization_format")
                or self.runtime_options.get("format"),
                default="parquet",
            )
            return "parquet"
        return format_name

    def evaluate_iuphar_qc(self, coverage: float) -> None:
        """Compare coverage against QC thresholds and log warnings."""

        thresholds = self.config.qc.thresholds or {}
        config: dict[str, Any] | None = None
        if isinstance(thresholds.get("iuphar_coverage"), dict):
            config = thresholds.get("iuphar_coverage")
        elif isinstance(thresholds.get("enrichment_success.iuphar"), dict):
            config = thresholds.get("enrichment_success.iuphar")

        if not config:
            return

        min_threshold = config.get("min")
        severity = str(config.get("severity", "warning"))

        issue_payload = {
            "metric": "iuphar_coverage",
            "value": coverage,
            "threshold_min": float(min_threshold) if min_threshold is not None else None,
            "severity": "info",
            "passed": True,
            "issue_type": "qc",
        }

        if min_threshold is not None and coverage < float(min_threshold):
            logger.warning(
                "iuphar_coverage_below_threshold",
                coverage=coverage,
                threshold=min_threshold,
            )
            issue_payload.update({"severity": severity, "passed": False})

        self.record_validation_issue(issue_payload)

    def _evaluate_enrichment_thresholds(self) -> None:
        """Evaluate enrichment coverage against configured thresholds."""

        qc_config = getattr(self.config, "qc", None)
        thresholds = getattr(qc_config, "enrichments", {}) if qc_config else {}
        if not thresholds:
            self.qc_enrichment_metrics = pd.DataFrame()
            return

        records: list[dict[str, Any]] = []
        failing: list[dict[str, Any]] = []
        enrichment_summary = self.qc_summary_data.setdefault("enrichment", {})

        for name, raw_threshold in thresholds.items():
            if isinstance(raw_threshold, dict):
                min_threshold = raw_threshold.get("min")
                severity = str(raw_threshold.get("severity", "error"))
            else:
                min_threshold = raw_threshold
                severity = "error"

            try:
                min_threshold_value = float(min_threshold) if min_threshold is not None else None
            except (TypeError, ValueError):
                min_threshold_value = None

            metric_key = f"enrichment_success.{name}"
            value = self.qc_metrics.get(metric_key)

            if value is None:
                enrichment_summary[name] = {
                    "value": None,
                    "threshold_min": min_threshold_value,
                    "passed": False,
                    "status": "missing",
                    "severity": severity,
                }
                self.record_validation_issue(
                    {
                        "metric": f"enrichment.{name}",
                        "issue_type": "qc_metric",
                        "severity": "info",
                        "status": "missing",
                    }
                )
                continue

            try:
                value_float = float(value)
            except (TypeError, ValueError):
                value_float = value

            passed = True
            if min_threshold_value is not None:
                passed = value_float >= min_threshold_value

            record = {
                "metric": name,
                "value": value_float,
                "threshold_min": min_threshold_value,
                "passed": passed,
                "severity": severity,
            }
            records.append(record)

            enrichment_summary[name] = {
                "value": value_float,
                "threshold_min": min_threshold_value,
                "passed": passed,
                "severity": severity,
            }

            issue_payload = {
                "metric": f"enrichment.{name}",
                "issue_type": "qc_metric",
                "severity": severity if not passed else "info",
                "value": value_float,
                "threshold_min": min_threshold_value,
                "passed": passed,
            }
            self.record_validation_issue(issue_payload)

            if not passed:
                failing.append({"metric": name, "severity": severity, "value": value_float, "threshold": min_threshold_value})

        self.qc_enrichment_metrics = self.output_service.prepare_enrichment_metrics(records)

        blocking: list[dict[str, Any]] = []
        downgraded: list[dict[str, Any]] = []
        for failure in failing:
            severity = failure["severity"]
            if self._severity_value(severity) >= self._severity_value("error") and self._should_fail(severity):
                blocking.append(failure)
            elif self._should_fail(severity):
                downgraded.append(failure)

        if downgraded:
            downgraded_details = ", ".join(
                f"{item['metric']} (value={item['value']}, threshold={item['threshold']}, severity={item['severity']})"
                for item in downgraded
            )
            logger.warning(
                "enrichment_threshold_below_min_non_blocking",
                details=downgraded_details,
                severity_threshold=self.config.qc.severity_threshold,
            )

        if blocking:
            details = ", ".join(
                f"{item['metric']} (value={item['value']}, threshold={item['threshold']})"
                for item in blocking
            )
            logger.error("enrichment_threshold_failed", details=details)
            raise ValueError(f"Enrichment thresholds failed: {details}")

    def _update_qc_summary(
        self,
        targets_df: pd.DataFrame,
        components_df: pd.DataFrame,
        protein_class_df: pd.DataFrame,
        xref_df: pd.DataFrame,
    ) -> None:
        """Populate QC summary structure with dataset statistics."""

        dataset_counts = {
            "targets": int(len(targets_df)),
            "components": int(len(components_df)),
            "classifications": int(len(protein_class_df)),
            "xrefs": int(len(xref_df)),
        }
        update_summary_section(self.qc_summary_data, "row_counts", dataset_counts)
        update_summary_section(self.qc_summary_data, "datasets", dataset_counts)

        fallback_counts = {
            key.split(".")[1]: int(value)
            for key, value in self.qc_metrics.items()
            if key.startswith("fallback.") and key.endswith(".count")
        }
        if fallback_counts:
            update_summary_section(self.qc_summary_data, "fallback_counts", fallback_counts)

        update_summary_metrics(self.qc_summary_data, self.qc_metrics)

        update_validation_issue_summary(self.qc_summary_data, self.validation_issues)

        if not self.qc_missing_mappings.empty:
            update_summary_section(
                self.qc_summary_data,
                "missing_mappings",
                {
                    "records": int(len(self.qc_missing_mappings)),
                    "stages": sorted(
                        self.qc_missing_mappings["stage"].dropna().unique().tolist()
                    ),
                },
                merge=False,
            )

def _target_should_run_uniprot(
    pipeline: PipelineBase, df: pd.DataFrame
) -> tuple[bool, str | None]:
    """Determine if the UniProt stage should run for the given pipeline."""

    if not isinstance(pipeline, TargetPipeline):
        return False, "unsupported_pipeline"
    if df.empty:
        return False, "empty_frame"

    with_uniprot = bool(pipeline.runtime_options.get("with_uniprot", True))
    pipeline.runtime_options["with_uniprot"] = with_uniprot
    if not with_uniprot:
        return False, "disabled"

    if pipeline.uniprot_client is None:
        return False, "client_unavailable"

    if "uniprot_accession" not in df.columns:
        return False, "missing_column"

    if not df["uniprot_accession"].notna().any():
        return False, "no_accessions"

    return True, None


def _target_run_uniprot_stage(
    pipeline: PipelineBase, df: pd.DataFrame
) -> pd.DataFrame:
    """Execute UniProt enrichment for the target pipeline."""

    if not isinstance(pipeline, TargetPipeline):  # pragma: no cover - defensive
        return df

    try:
        enriched_df, silver_df, component_df, metrics = pipeline.enrich_uniprot(df)
    except Exception as exc:
        pipeline.record_validation_issue(
            {
                "metric": "enrichment.uniprot",
                "issue_type": "enrichment",
                "severity": "warning",
                "status": "failed",
                "error": str(exc),
            }
        )
        raise

    if metrics:
        pipeline.qc_metrics.update(metrics)

    if not silver_df.empty or not component_df.empty:
        pipeline.materialize_silver(silver_df, component_df)

    pipeline.stage_context["uniprot"] = {
        "silver": silver_df,
        "component_enrichment": component_df,
    }
    pipeline.set_stage_summary("uniprot", "completed", rows=int(len(enriched_df)))

    return enriched_df


def _target_should_run_iuphar(
    pipeline: PipelineBase, df: pd.DataFrame
) -> tuple[bool, str | None]:
    """Determine if the IUPHAR stage should run for the given pipeline."""

    if not isinstance(pipeline, TargetPipeline):
        return False, "unsupported_pipeline"
    if df.empty:
        return False, "empty_frame"

    with_iuphar = bool(pipeline.runtime_options.get("with_iuphar", True))
    pipeline.runtime_options["with_iuphar"] = with_iuphar
    if not with_iuphar:
        return False, "disabled"

    if pipeline.iuphar_client is None or pipeline.iuphar_paginator is None:
        return False, "client_unavailable"

    return True, None


def _target_run_iuphar_stage(
    pipeline: PipelineBase, df: pd.DataFrame
) -> pd.DataFrame:
    """Execute IUPHAR enrichment for the target pipeline."""

    if not isinstance(pipeline, TargetPipeline):  # pragma: no cover - defensive
        return df

    try:
        enriched_df, classification_df, gold_df, metrics = pipeline.enrich_iuphar(df)
    except Exception as exc:
        pipeline.record_validation_issue(
            {
                "metric": "enrichment.iuphar",
                "issue_type": "enrichment",
                "severity": "warning",
                "status": "failed",
                "error": str(exc),
            }
        )
        raise

    if metrics:
        pipeline.qc_metrics.update(metrics)
        coverage_value = metrics.get("iuphar_coverage")
        if coverage_value is not None:
            try:
                pipeline.evaluate_iuphar_qc(float(coverage_value))
            except (TypeError, ValueError):  # pragma: no cover - defensive
                pipeline.evaluate_iuphar_qc(0.0)

    if not classification_df.empty or not gold_df.empty:
        pipeline.materialize_iuphar(classification_df, gold_df)

    pipeline.stage_context["iuphar"] = {
        "classification": classification_df,
        "gold": gold_df,
    }
    pipeline.set_stage_summary("iuphar", "completed", rows=int(len(enriched_df)))

    return enriched_df


def _register_target_enrichment_stages() -> None:
    """Register enrichment stages for the target pipeline."""

    enrichment_stage_registry.register(
        TargetPipeline,
        EnrichmentStage(
            name="uniprot",
            include_if=_target_should_run_uniprot,
            handler=_target_run_uniprot_stage,
        ),
    )
    enrichment_stage_registry.register(
        TargetPipeline,
        EnrichmentStage(
            name="iuphar",
            include_if=_target_should_run_iuphar,
            handler=_target_run_iuphar_stage,
        ),
    )


_register_target_enrichment_stages()

