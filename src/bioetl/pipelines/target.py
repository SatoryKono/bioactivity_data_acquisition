"""Target Pipeline - ChEMBL target data extraction with multi-stage enrichment."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.config.models import TargetSourceConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.logger import UnifiedLogger
from bioetl.core.chembl import create_chembl_client
from bioetl.core.materialization import MaterializationManager
from bioetl.core.output_writer import UnifiedOutputWriter
from bioetl.pipelines.base import (
    EnrichmentStage,
    PipelineBase,
    enrichment_stage_registry,
)
from bioetl.pipelines.target_gold import (
    _split_accession_field,
    annotate_source_rank,
    coalesce_by_priority,
    expand_xrefs,
    merge_components,
)
from bioetl.schemas import (
    ProteinClassSchema,
    TargetComponentSchema,
    TargetSchema,
    XrefSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.sources.iuphar.pagination import PageNumberPaginator
from bioetl.sources.iuphar.parser import parse_api_response
from bioetl.sources.iuphar.request import (
    DEFAULT_PAGE_SIZE,
    build_families_request,
    build_targets_request,
)
from bioetl.sources.iuphar.service import IupharService, IupharServiceConfig
from bioetl.sources.uniprot.client import (
    UniProtIdMappingClient,
    UniProtOrthologClient,
    UniProtSearchClient,
)
from bioetl.sources.uniprot.normalizer import UniProtNormalizer
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    prepare_enrichment_metrics,
    prepare_missing_mappings,
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

__all__: Final[tuple[str, ...]] = ("TargetPipeline",)

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("target", "1.0.0", TargetSchema)  # type: ignore[arg-type]


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
    _IUPHAR_PAGE_SIZE = DEFAULT_PAGE_SIZE
    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = TargetSchema

        self.source_configs: dict[str, TargetSourceConfig] = {}
        self.api_clients: dict[str, UnifiedAPIClient] = {}

        chembl_context = create_chembl_client(
            config,
            defaults={
                "enabled": True,
                "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                "batch_size": 25,
            },
        )
        self.source_configs["chembl"] = chembl_context.source_config
        self.api_clients["chembl"] = chembl_context.client
        self.register_client(chembl_context.client)

        factory = APIClientFactory.from_pipeline_config(config)

        for source_name, source in config.sources.items():
            if source_name == "chembl" or source is None:
                continue

            source_config = ensure_target_source_config(source, defaults={})
            if not source_config.enabled:
                continue

            self.source_configs[source_name] = source_config
            api_client_config = factory.create(source_name, source_config)
            client = UnifiedAPIClient(api_client_config)
            self.api_clients[source_name] = client
            self.register_client(client)

        self.chembl_client = chembl_context.client
        self.uniprot_client = self.api_clients.get("uniprot")
        self.uniprot_idmapping_client = self.api_clients.get("uniprot_idmapping")
        self.uniprot_orthologs_client = self.api_clients.get("uniprot_orthologs")
        self.iuphar_client = self.api_clients.get("iuphar")
        self.iuphar_paginator: PageNumberPaginator | None = None
        if self.iuphar_client is not None:
            iuphar_config = self.source_configs.get("iuphar")
            batch_size = getattr(iuphar_config, "batch_size", None)
            if batch_size:
                try:
                    page_size = int(batch_size)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    page_size = self._IUPHAR_PAGE_SIZE
            else:
                page_size = self._IUPHAR_PAGE_SIZE
            self.iuphar_paginator = PageNumberPaginator(self.iuphar_client, page_size)
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
        self.uniprot_ortholog_client = UniProtOrthologClient(
            client=ortholog_base_client,
            fields=self._ORTHOLOG_FIELDS,
            priority_map=self._ORTHOLOG_PRIORITY,
        )
        self.uniprot_normalizer = UniProtNormalizer(
            search_client=self.uniprot_search_client,
            id_mapping_client=self.uniprot_id_mapping_client,
            ortholog_client=self.uniprot_ortholog_client,
        )

        # Backwards compatibility
        self.api_client = self.chembl_client

        self.batch_size = chembl_context.batch_size
        self._chembl_release = self._get_chembl_release() if self.chembl_client else None

        self.gold_targets: pd.DataFrame = pd.DataFrame()
        self.gold_components: pd.DataFrame = pd.DataFrame()
        self.gold_protein_class: pd.DataFrame = pd.DataFrame()
        self.gold_xref: pd.DataFrame = pd.DataFrame()
        self._qc_missing_mapping_records: list[dict[str, Any]] = []

        runtime_config = getattr(self.config, "runtime", None)

        determinism_copy = self.determinism.model_copy()
        determinism_copy.column_order = []
        self.materialization_writer = UnifiedOutputWriter(
            f"{self.run_id}-materialization",
            determinism=determinism_copy,
            pipeline_config=config,
        )

        self.materialization_manager = MaterializationManager(
            self.config.materialization,
            runtime=runtime_config,
            stage_context=self.stage_context,
            output_writer_factory=lambda: self.materialization_writer,
            run_id=self.run_id,
            determinism=self.determinism,
        )

    def close_resources(self) -> None:
        """Close API clients constructed by the target pipeline."""

        try:
            for name, client in getattr(self, "api_clients", {}).items():
                self._close_resource(client, resource_name=f"api_client.{name}")
        finally:
            super().close_resources()

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

        record: dict[str, Any] = {
            "stage": stage,
            "target_chembl_id": target_id,
            "input_accession": accession,
            "resolved_accession": resolved_accession,
            "resolution": resolution,
            "status": status,
        }

        if details:
            try:
                record["details"] = json.dumps(details, ensure_ascii=False, sort_keys=True)
            except TypeError:
                record["details"] = str(details)

        self._qc_missing_mapping_records.append(record)

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
        self._qc_missing_mapping_records = []
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

        gold_targets, gold_components, gold_protein_class, gold_xref = self._build_gold_outputs(
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

        gold_targets = finalize_output_dataset(
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

        if self._qc_missing_mapping_records:
            self.qc_missing_mappings = prepare_missing_mappings(self._qc_missing_mapping_records)
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

    def _enrich_uniprot(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich the target dataframe using UniProt entries via the shared service."""

        result = self.uniprot_normalizer.enrich_targets(
            df,
            accession_column="uniprot_accession",
            target_id_column="target_chembl_id",
            gene_symbol_column="gene_symbol",
            organism_column="organism",
            taxonomy_column="taxonomy_id",
        )

        for record in result.missing_mappings:
            self._record_missing_mapping(**record)

        for issue in result.validation_issues:
            self.record_validation_issue(issue)

        return result.dataframe, result.silver, result.components, result.metrics

    def _enrich_iuphar(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich target dataframe with IUPHAR classifications."""

        if self.iuphar_paginator is None or self.iuphar_client is None:
            return self.iuphar_service.enrich_targets(df, targets=[], families=[])

        targets = self._fetch_iuphar_collection(
            "/targets",
            unique_key="targetId",
            params=build_targets_request(annotation_status="CURATED"),
        )
        families = self._fetch_iuphar_collection(
            "/targets/families",
            unique_key="familyId",
            params=build_families_request(),
        )

        self.stage_context["raw_targets"] = targets
        self.stage_context["raw_families"] = families

        return self.iuphar_service.enrich_targets(
            df,
            targets=targets,
            families=families,
        )

    def _fetch_iuphar_collection(
        self,
        path: str,
        unique_key: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Delegate collection fetches to :class:`IupharService`."""

        if self.iuphar_paginator is None:
            return []

        parser = lambda payload: parse_api_response(payload, unique_key=unique_key)
        return self.iuphar_paginator.fetch_all(
            path,
            unique_key=unique_key,
            params=params or {},
            parser=parser,
        )

    def _build_family_hierarchy(
        self, families: dict[int, dict[str, Any]]
    ) -> dict[int, dict[str, list[Any]]]:
        return self.iuphar_service.build_family_hierarchy(families)

    def _normalize_iuphar_name(self, value: str | None) -> str:
        """Normalize identifiers for fuzzy matching."""

        return self.iuphar_service.normalize_name(value)

    def _candidate_names_from_row(self, row: pd.Series) -> list[str]:
        """Extract candidate names for IUPHAR matching from a row."""

        return self.iuphar_service.candidate_names_from_row(row)

    def _fallback_classification_record(self, row: pd.Series) -> dict[str, Any]:
        """Construct a fallback classification record for unmatched targets."""

        return self.iuphar_service.fallback_classification_record(row)

    def _select_best_classification(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """Select the best classification entry for a matched target."""

        result = self.iuphar_service.select_best_classification(records)
        return dict(result) if isinstance(result, dict) else result

    def _materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
    ) -> None:
        """Persist silver-level UniProt artifacts deterministically."""

        format_name = self._resolve_materialization_format()
        self.materialization_manager.materialize_silver(
            uniprot_df,
            component_df,
            format=format_name,
        )

    def _materialize_iuphar(
        self,
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
    ) -> None:
        """Persist IUPHAR classification artifacts."""

        format_name = self._resolve_materialization_format()
        self.materialization_manager.materialize_iuphar(
            classification_df,
            gold_df,
            format=format_name,
        )

    def _build_gold_outputs(
        self,
        df: pd.DataFrame,
        component_enrichment: pd.DataFrame,
        iuphar_gold: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Assemble deterministic gold-level DataFrames."""

        chembl_components = self._expand_json_column(df, "target_components")
        components_df = merge_components(
            chembl_components,
            component_enrichment,
            targets=df,
        )

        if not components_df.empty:
            components_df = annotate_source_rank(
                components_df,
                source_column="data_origin",
                output_column="merge_rank",
            )
            sort_columns = [
                col
                for col in [
                    "target_chembl_id",
                    "canonical_accession",
                    "isoform_accession",
                    "component_id",
                ]
                if col in components_df.columns
            ]
            if sort_columns:
                components_df = components_df.sort_values(sort_columns, kind="stable")
            components_df = components_df.reset_index(drop=True)

        protein_class_df = self._expand_protein_classifications(df)
        if not protein_class_df.empty:
            protein_class_df = protein_class_df.sort_values(
                ["target_chembl_id", "class_level", "class_name"],
                kind="stable",
            ).reset_index(drop=True)

        xref_df = expand_xrefs(df)
        if not xref_df.empty:
            sort_columns = [
                col
                for col in ["target_chembl_id", "xref_src_db", "xref_id", "component_id"]
                if col in xref_df.columns
            ]
            if sort_columns:
                xref_df = xref_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)

        targets_gold = self._build_targets_gold(df, components_df, iuphar_gold)
        targets_gold = targets_gold.sort_values(["target_chembl_id"], kind="stable").reset_index(drop=True)

        return targets_gold, components_df, protein_class_df, xref_df

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
        self.materialization_manager.materialize_gold(
            targets_df,
            components_df,
            protein_class_df,
            xref_df,
            format=format_name,
            output_path=gold_path,
        )

    def _resolve_materialization_format(self) -> str:
        """Determine the requested output format for materialization artefacts."""

        runtime_override = self.runtime_options.get("materialization_format") or self.runtime_options.get(
            "format"
        )
        if runtime_override:
            resolved = str(runtime_override).strip().lower()
            if resolved in {"csv", "parquet"}:
                return resolved
            logger.warning(
                "unsupported_materialization_format",
                format=runtime_override,
                default="parquet",
            )

        materialization_paths = getattr(self.config, "materialization", None)
        if materialization_paths:
            inferred = materialization_paths.infer_dataset_format("gold", "targets")
            if inferred:
                normalised = str(inferred).strip().lower()
                if normalised in {"csv", "parquet"}:
                    return normalised

        return "parquet"

    def _expand_json_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Expand a JSON encoded column into a flat DataFrame."""

        if column not in df.columns:
            return pd.DataFrame()

        records: list[dict[str, Any]] = []
        for row in df.itertuples(index=False):
            payload = getattr(row, column, None)
            if payload is None or payload is pd.NA:
                continue
            if isinstance(payload, str) and payload.strip() in {"", "[]"}:
                continue
            if isinstance(payload, list | tuple) and not payload:
                continue

            if isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    logger.debug("expand_json_column_invalid", column=column, value=payload)
                    continue
            else:
                parsed = payload

            if isinstance(parsed, dict):
                parsed = [parsed]

            if not isinstance(parsed, list):
                continue

            for item in parsed:
                if not isinstance(item, dict):
                    continue
                record = item.copy()
                if "target_chembl_id" not in record:
                    record["target_chembl_id"] = getattr(row, "target_chembl_id", None)
                records.append(record)

        if not records:
            return pd.DataFrame()

        return pd.DataFrame(records).convert_dtypes()

    def _expand_protein_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalise protein classification hierarchy."""

        raw_df = self._expand_json_column(df, "protein_classifications")
        if raw_df.empty:
            return pd.DataFrame(columns=["target_chembl_id", "class_level", "class_name", "full_path"])

        records: list[dict[str, Any]] = []
        level_keys = ["l1", "l2", "l3", "l4", "l5"]
        for entry in raw_df.to_dict("records"):
            target_id = entry.get("target_chembl_id")
            path: list[str] = []
            for idx, key in enumerate(level_keys, start=1):
                class_name = entry.get(key)
                if class_name in {None, "", pd.NA}:
                    continue
                class_name_str = str(class_name)
                path.append(class_name_str)
                records.append(
                    {
                        "target_chembl_id": target_id,
                        "class_level": f"L{idx}",
                        "class_name": class_name_str,
                        "full_path": " > ".join(path),
                    }
                )

        if not records:
            return pd.DataFrame(columns=["target_chembl_id", "class_level", "class_name", "full_path"])

        protein_class_df = pd.DataFrame(records).drop_duplicates()
        return protein_class_df.convert_dtypes()

    def _build_targets_gold(
        self,
        df: pd.DataFrame,
        components_df: pd.DataFrame,
        iuphar_gold: pd.DataFrame,
    ) -> pd.DataFrame:
        """Construct the final targets table with aggregated attributes."""

        working = df.copy()

        coalesce_map = {
            "pref_name": [
                ("pref_name", "chembl"),
                ("uniprot_protein_name", "uniprot"),
                ("iuphar_name", "iuphar"),
            ],
            "organism": [
                ("organism", "chembl"),
                ("uniprot_taxonomy_name", "uniprot"),
            ],
            "tax_id": [
                ("tax_id", "chembl"),
                ("taxonomy", "chembl"),
                ("uniprot_taxonomy_id", "uniprot"),
            ],
            "gene_symbol": [
                ("gene_symbol", "chembl"),
                ("uniprot_gene_primary", "uniprot"),
            ],
            "hgnc_id": [
                ("hgnc_id", "chembl"),
                ("uniprot_hgnc_id", "uniprot"),
                ("hgnc", "uniprot"),
            ],
            "lineage": [
                ("lineage", "chembl"),
                ("uniprot_lineage", "uniprot"),
            ],
            "uniprot_id_primary": [
                ("uniprot_canonical_accession", "uniprot"),
                ("uniprot_accession", "chembl"),
                ("primaryAccession", "chembl"),
            ],
        }

        working = coalesce_by_priority(working, coalesce_map, source_suffix="_origin")

        if not iuphar_gold.empty:
            iuphar_subset = iuphar_gold.drop_duplicates("target_chembl_id")
            working = working.merge(
                iuphar_subset,
                on="target_chembl_id",
                how="left",
                suffixes=("", "_iuphar"),
            )

        if not components_df.empty and "isoform_accession" in components_df.columns:
            isoform_counts = components_df.groupby("target_chembl_id")["isoform_accession"].nunique()
            isoform_map = (
                components_df.groupby("target_chembl_id")["isoform_accession"]
                .apply(
                    lambda values: sorted(
                        {str(value) for value in values if value not in {None, "", pd.NA}}
                    )
                )
                .to_dict()
            )
        else:
            isoform_counts = pd.Series(dtype="Int64")
            isoform_map = {}

        working["isoform_count"] = (
            working["target_chembl_id"].map(isoform_counts).fillna(0).astype("Int64")
        )
        working["has_alternative_products"] = (working["isoform_count"] > 1).astype("boolean")

        working["has_uniprot"] = working["uniprot_id_primary"].notna().astype("boolean")
        if "iuphar_target_id" in working.columns:
            working["has_iuphar"] = working["iuphar_target_id"].notna().astype("boolean")
        else:
            working["has_iuphar"] = pd.Series(False, index=working.index, dtype="boolean")

        if "uniprot_secondary_accessions" in working.columns:
            secondary_map = {
                key: _split_accession_field(value)
                for key, value in working.set_index("target_chembl_id")["uniprot_secondary_accessions"].items()
            }
        else:
            secondary_map = {}

        def combine_ids(row: pd.Series) -> Any:
            accs: list[str] = []
            primary = row.get("uniprot_id_primary")
            if pd.notna(primary):
                accs.append(str(primary))
            accs.extend(isoform_map.get(row.get("target_chembl_id"), []))
            accs.extend(secondary_map.get(row.get("target_chembl_id"), []))
            seen: set[str] = set()
            unique: list[str] = []
            for acc in accs:
                if not acc:
                    continue
                if acc not in seen:
                    seen.add(acc)
                    unique.append(acc)
            return "; ".join(unique) if unique else pd.NA

        working["uniprot_ids_all"] = working.apply(combine_ids, axis=1)

        origin_columns = [col for col in working.columns if col.endswith("_origin")]

        def derive_origin(row: pd.Series) -> str:
            origins = {
                str(row[col]).lower()
                for col in origin_columns
                if pd.notna(row[col]) and str(row[col]).strip()
            }
            if row.get("has_iuphar") is True:
                origins.add("iuphar")
            if not origins:
                origins.add("chembl")
            return ",".join(sorted(origins))

        working["data_origin"] = working.apply(derive_origin, axis=1)
        working = working.drop(columns=origin_columns, errors="ignore")

        expected_columns = [
            "target_chembl_id",
            "pref_name",
            "target_type",
            "organism",
            "tax_id",
            "gene_symbol",
            "hgnc_id",
            "lineage",
            "uniprot_accession",
            "uniprot_id_primary",
            "uniprot_ids_all",
            "isoform_count",
            "has_alternative_products",
            "has_uniprot",
            "has_iuphar",
            "iuphar_type",
            "iuphar_class",
            "iuphar_subclass",
            "data_origin",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "extracted_at",
        ]

        for column in expected_columns:
            if column not in working.columns:
                working[column] = pd.NA

        working["has_alternative_products"] = working["has_alternative_products"].astype("boolean")
        working["has_uniprot"] = working["has_uniprot"].astype("boolean")
        working["has_iuphar"] = working["has_iuphar"].astype("boolean")
        working["isoform_count"] = working["isoform_count"].astype("Int64")

        if "tax_id" in working.columns:
            working["tax_id"] = (
                pd.to_numeric(working["tax_id"], errors="coerce")
                .astype("Int64")
            )

        # Ensure we only expose the expected contract columns in a stable order.
        working = working[expected_columns]

        return working.convert_dtypes()

    def _evaluate_iuphar_qc(self, coverage: float) -> None:
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

        self.qc_enrichment_metrics = prepare_enrichment_metrics(records)

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
        enriched_df, silver_df, component_df, metrics = pipeline._enrich_uniprot(df)
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
        pipeline._materialize_silver(silver_df, component_df)

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
        enriched_df, classification_df, gold_df, metrics = pipeline._enrich_iuphar(df)
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
                pipeline._evaluate_iuphar_qc(float(coverage_value))
            except (TypeError, ValueError):  # pragma: no cover - defensive
                pipeline._evaluate_iuphar_qc(0.0)

    if not classification_df.empty or not gold_df.empty:
        pipeline._materialize_iuphar(classification_df, gold_df)

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

