"""Target Pipeline - ChEMBL target data extraction with multi-stage enrichment."""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.config.models import TargetSourceConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import (
    EnrichmentStage,
    PipelineBase,
    create_chembl_client,
    enrichment_stage_registry,
)
from bioetl.pipelines.target_gold import (
    _split_accession_field,
    annotate_source_rank,
    coalesce_by_priority,
    expand_xrefs,
    materialize_gold,
    merge_components,
)
from bioetl.schemas import (
    ProteinClassSchema,
    TargetComponentSchema,
    TargetSchema,
    XrefSchema,
)
from bioetl.schemas.registry import schema_registry
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    prepare_enrichment_metrics,
    prepare_missing_mappings,
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

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
    _IUPHAR_PAGE_SIZE = 200
    _NAME_CLEAN_RE = re.compile(r"[^a-z0-9]+")

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

        # Backwards compatibility
        self.api_client = self.chembl_client

        self.batch_size = chembl_context.batch_size
        self._chembl_release = self._get_chembl_release() if self.chembl_client else None

        self.gold_targets: pd.DataFrame = pd.DataFrame()
        self.gold_components: pd.DataFrame = pd.DataFrame()
        self.gold_protein_class: pd.DataFrame = pd.DataFrame()
        self.gold_xref: pd.DataFrame = pd.DataFrame()
        self._qc_missing_mapping_records: list[dict[str, Any]] = []

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
        pipeline_version = getattr(self.config.pipeline, "version", None) or "1.0.0"
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
            self.add_additional_table("target_components", gold_components)
        if not gold_protein_class.empty:
            self.add_additional_table(
                "target_protein_classifications",
                gold_protein_class,
            )
        if not gold_xref.empty:
            self.add_additional_table("target_xrefs", gold_xref)
        if not component_enrichment.empty:
            self.add_additional_table(
                "target_component_enrichment",
                component_enrichment,
            )
        if not iuphar_classification.empty:
            self.add_additional_table(
                "target_iuphar_classification",
                iuphar_classification,
            )
        if not iuphar_gold.empty:
            self.add_additional_table("target_iuphar_enrichment", iuphar_gold)

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
            schema=TargetSchema,
            dataset_name="targets",
            severity="critical",
        )
        self.gold_targets = validated_targets

        self.gold_components = self.run_schema_validation(
            self.gold_components,
            schema=TargetComponentSchema,
            dataset_name="target_components",
        )

        self.gold_protein_class = self.run_schema_validation(
            self.gold_protein_class,
            schema=ProteinClassSchema,
            dataset_name="protein_classifications",
        )

        self.gold_xref = self.run_schema_validation(
            self.gold_xref,
            schema=XrefSchema,
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

    @staticmethod
    def _chunked(values: Iterable[str], size: int) -> Iterator[list[str]]:
        """Yield chunks of values honoring API limits."""

        batch: list[str] = []
        for value in values:
            batch.append(value)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _fetch_uniprot_entries(self, accessions: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Fetch UniProt entries for the provided accessions using the configured client."""

        if self.uniprot_client is None:
            return {}

        entries: dict[str, dict[str, Any]] = {}
        for chunk in self._chunked(accessions, self._UNIPROT_BATCH_SIZE):
            query_terms = [f"(accession:{acc})" for acc in chunk]
            params = {
                "query": " OR ".join(query_terms),
                "fields": self._UNIPROT_FIELDS,
                "format": "json",
                "size": max(len(chunk), 25),
            }

            try:
                payload = self.uniprot_client.request_json("/search", params=params)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_search_failed", error=str(exc), query=params["query"])
                continue

            results = payload.get("results") or payload.get("entries") or []
            for result in results:
                primary = result.get("primaryAccession") or result.get("accession")
                if not primary:
                    continue
                entries[primary] = result

        return entries

    def _enrich_uniprot(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich the target dataframe using UniProt entries and compute QC metrics."""

        working_df = df.reset_index(drop=True).copy()
        working_df = working_df.convert_dtypes()

        accessions_series = working_df.get("uniprot_accession")
        if accessions_series is None:
            return working_df, pd.DataFrame(), pd.DataFrame(), {}

        accessions = {
            str(value).strip()
            for value in accessions_series.dropna().tolist()
            if str(value).strip()
        }

        if not accessions:
            logger.info("uniprot_enrichment_skipped", reason="empty_accessions")
            return working_df, pd.DataFrame(), pd.DataFrame(), {}

        logger.info("uniprot_enrichment_started", accession_count=len(accessions))

        entries = self._fetch_uniprot_entries(accessions)
        secondary_index: dict[str, dict[str, Any]] = {}
        for entry in entries.values():
            for field in ("secondaryAccession", "secondaryAccessions"):
                secondary_values = entry.get(field, [])
                if isinstance(secondary_values, str):
                    secondary_values = [secondary_values]
                for secondary in secondary_values or []:
                    secondary_index[str(secondary)] = entry

        used_entries: dict[str, dict[str, Any]] = {}
        unresolved: dict[int, str] = {}
        metrics: dict[str, Any] = {}
        fallback_counts: dict[str, int] = defaultdict(int)
        resolved_rows = 0
        total_with_accession = 0

        for idx, row in working_df.iterrows():
            accession = row.get("uniprot_accession")
            if pd.isna(accession) or not str(accession).strip():
                continue

            accession_str = str(accession).strip()
            total_with_accession += 1
            uniprot_entry: dict[str, Any] | None = entries.get(accession_str)
            merge_strategy = "direct"

            if uniprot_entry is None:
                uniprot_entry = secondary_index.get(accession_str)
                if uniprot_entry is not None:
                    merge_strategy = "secondary_accession"
                    fallback_counts["secondary_accession"] += 1
                    resolved = uniprot_entry.get("primaryAccession") or uniprot_entry.get("accession")
                    self._record_missing_mapping(
                        stage="uniprot",
                        target_id=row.get("target_chembl_id"),
                        accession=accession_str,
                        resolved_accession=resolved,
                        resolution="secondary_accession",
                        status="resolved",
                    )

            if uniprot_entry is None:
                unresolved[idx] = accession_str
                continue

            resolved_rows += 1
            canonical = uniprot_entry.get("primaryAccession") or uniprot_entry.get("accession")
            if canonical:
                used_entries[str(canonical)] = uniprot_entry
            self._apply_entry_enrichment(working_df, int(idx), uniprot_entry, merge_strategy)

        # Attempt ID mapping for unresolved accessions
        if unresolved:
            mapping_df = self._run_id_mapping(unresolved.values())
            if not mapping_df.empty:
                for idx, submitted in list(unresolved.items()):
                    mapped_rows = mapping_df[mapping_df["submitted_id"] == submitted]
                    if mapped_rows.empty:
                        continue

                    mapped_row = mapped_rows.iloc[0]
                    canonical_acc = mapped_row.get("canonical_accession")
                    if not canonical_acc:
                        continue

                    entry: dict[str, Any] | None = entries.get(canonical_acc)
                    if entry is None:
                        if canonical_acc not in used_entries:
                            extra_entries = self._fetch_uniprot_entries([canonical_acc])
                            if extra_entries:
                                entries.update(extra_entries)
                                entry = extra_entries.get(canonical_acc)
                    if entry is None:
                        continue

                    fallback_counts["idmapping"] += 1
                    used_entries[str(canonical_acc)] = entry
                    self._record_missing_mapping(
                        stage="uniprot",
                        target_id=working_df.at[idx, "target_chembl_id"],
                        accession=submitted,
                        resolved_accession=canonical_acc,
                        resolution="idmapping",
                        status="resolved",
                    )
                    self._apply_entry_enrichment(working_df, idx, entry, "idmapping")
                    resolved_rows += 1
                    unresolved.pop(idx, None)

        # Gene symbol fallback
        if unresolved:
            gene_symbol_cache: dict[str, dict[str, Any] | None] = {}
            for idx, accession in list(unresolved.items()):
                gene_symbol = working_df.at[idx, "gene_symbol"] if "gene_symbol" in working_df.columns else None
                organism = working_df.at[idx, "organism"] if "organism" in working_df.columns else None
                gene_key = f"{gene_symbol}|{organism}" if gene_symbol else None
                if not gene_symbol:
                    continue

                if gene_key not in gene_symbol_cache:
                    gene_symbol_cache[gene_key] = self._search_uniprot_by_gene(str(gene_symbol), organism)

                entry = gene_symbol_cache[gene_key]
                if entry is None:
                    continue

                canonical_acc = entry.get("primaryAccession") or entry.get("accession")
                if not canonical_acc:
                    continue

                fallback_counts["gene_symbol"] += 1
                used_entries[str(canonical_acc)] = entry
                self._record_missing_mapping(
                    stage="uniprot",
                    target_id=working_df.at[idx, "target_chembl_id"],
                    accession=accession,
                    resolved_accession=canonical_acc,
                    resolution="gene_symbol",
                    status="resolved",
                    details={
                        "gene_symbol": gene_symbol,
                        "organism": organism,
                    },
                )
                self._apply_entry_enrichment(working_df, idx, entry, "gene_symbol")
                resolved_rows += 1
                unresolved.pop(idx, None)

        # Ortholog fallback for remaining unresolved rows
        if unresolved:
            for idx, accession in list(unresolved.items()):
                canonical = working_df.at[idx, "uniprot_accession"]
                taxonomy_id = None
                if "taxonomy_id" in working_df.columns:
                    taxonomy_id = working_df.at[idx, "taxonomy_id"]
                orthologs = self._fetch_orthologs(str(canonical), taxonomy_id)
                if orthologs.empty:
                    continue

                orthologs = orthologs.sort_values(by="priority").reset_index(drop=True)
                best = orthologs.iloc[0]
                ortholog_acc = best.get("ortholog_accession")
                if not ortholog_acc:
                    continue

                entry: dict[str, Any] | None = entries.get(ortholog_acc)
                if entry is None:
                    extra_entries = self._fetch_uniprot_entries([ortholog_acc])
                    if extra_entries:
                        entries.update(extra_entries)
                        entry = extra_entries.get(ortholog_acc)

                if entry is None:
                    continue

                fallback_counts["ortholog"] += 1
                used_entries[str(ortholog_acc)] = entry
                self._record_missing_mapping(
                    stage="uniprot",
                    target_id=working_df.at[idx, "target_chembl_id"],
                    accession=accession,
                    resolved_accession=ortholog_acc,
                    resolution="ortholog",
                    status="resolved",
                    details={"ortholog_source": best.get("organism")},
                )
                self._apply_entry_enrichment(working_df, int(idx), entry, "ortholog")
                working_df.at[idx, "ortholog_source"] = best.get("organism")
                resolved_rows += 1
                unresolved.pop(idx, None)

        if unresolved:
            for idx, accession in unresolved.items():
                logger.warning("uniprot_enrichment_unresolved", accession=accession)
                target_id = working_df.at[idx, "target_chembl_id"] if idx in working_df.index else None
                self._record_missing_mapping(
                    stage="uniprot",
                    target_id=target_id,
                    accession=accession,
                    resolution="unresolved",
                    status="unresolved",
                )
                self.record_validation_issue(
                    {
                        "metric": "enrichment.uniprot.unresolved",
                        "issue_type": "missing_mapping",
                        "severity": "warning",
                        "target_chembl_id": target_id,
                        "accession": accession,
                    }
                )

        coverage = resolved_rows / total_with_accession if total_with_accession else 0.0
        metrics["enrichment_success.uniprot"] = coverage
        metrics["enrichment.total_with_accession"] = total_with_accession
        metrics["enrichment.resolved"] = resolved_rows
        unresolved_count = len(unresolved)
        if unresolved_count:
            metrics["enrichment.uniprot.unresolved"] = unresolved_count
            metrics["enrichment.uniprot.unresolved_rate"] = (
                unresolved_count / total_with_accession if total_with_accession else 0.0
            )

        for fallback_name, count in fallback_counts.items():
            metrics[f"fallback.{fallback_name}.count"] = count
            if total_with_accession:
                metrics[f"fallback.{fallback_name}.rate"] = count / total_with_accession

        logger.info(
            "uniprot_enrichment_completed",
            resolved=resolved_rows,
            total=total_with_accession,
            coverage=coverage,
            fallback_counts=dict(fallback_counts),
        )

        uniprot_silver_records: list[dict[str, Any]] = []
        component_records: list[dict[str, Any]] = []
        for canonical, entry in used_entries.items():
            record = {
                "canonical_accession": canonical,
                "protein_name": self._extract_protein_name(entry),
                "gene_primary": self._extract_gene_primary(entry),
                "gene_synonyms": "; ".join(self._extract_gene_synonyms(entry)),
                "organism_name": self._extract_organism(entry),
                "organism_id": self._extract_taxonomy_id(entry),
                "lineage": "; ".join(self._extract_lineage(entry)),
                "sequence_length": self._extract_sequence_length(entry),
                "secondary_accessions": "; ".join(self._extract_secondary(entry)),
            }
            uniprot_silver_records.append(record)

            isoform_df = self._expand_isoforms(entry)
            if not isoform_df.empty:
                component_records.extend(isoform_df.to_dict("records"))  # type: ignore[arg-type]

        uniprot_silver_df = pd.DataFrame(uniprot_silver_records).convert_dtypes()
        component_enrichment_df = pd.DataFrame(component_records).convert_dtypes()

        return working_df, uniprot_silver_df, component_enrichment_df, metrics

    def _enrich_iuphar(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Enrich target dataframe with IUPHAR classifications."""

        working_df = df.reset_index(drop=True).copy()
        working_df = working_df.convert_dtypes()

        targets = self._fetch_iuphar_collection(
            "/targets",
            unique_key="targetId",
            params={"annotationStatus": "CURATED"},
        )
        families = self._fetch_iuphar_collection(
            "/targets/families",
            unique_key="familyId",
        )

        if not targets or not families:
            logger.info(
                "iuphar_enrichment_no_data",
                targets=len(targets),
                families=len(families),
            )
            return working_df, pd.DataFrame(), pd.DataFrame(), {
                "enrichment_success.iuphar": 0.0,
                "iuphar_coverage": 0.0,
                "enrichment.iuphar.total": 0,
                "enrichment.iuphar.matched": 0,
            }

        family_index: dict[int, dict[str, Any]] = {}
        for raw_family in families:
            try:
                family_id = int(raw_family.get("familyId") or 0)
            except (TypeError, ValueError):
                continue
            family_index[family_id] = raw_family

        family_paths = self._build_family_hierarchy(family_index)

        classification_map: dict[int, dict[str, Any]] = {}
        normalized_index: dict[str, list[int]] = {}

        for target in targets:
            target_id = target.get("targetId")
            if target_id is None:
                continue

            try:
                target_id_int = int(target_id)
            except (TypeError, ValueError):
                continue

            families_ids = target.get("familyIds") or []
            if isinstance(families_ids, int):
                families_ids = [families_ids]

            family_records: list[dict[str, Any]] = []
            for family_id in families_ids:
                try:
                    family_id_int = int(family_id)
                except (TypeError, ValueError):
                    continue

                path_info = family_paths.get(family_id_int)
                if not path_info:
                    continue

                path_names = path_info.get("path_names", [])
                if not path_names:
                    continue

                record = {
                    "iuphar_family_id": family_id_int,
                    "iuphar_family_name": family_index.get(family_id_int, {}).get("name"),
                    "classification_path": " > ".join(path_names),
                    "classification_depth": len(path_names),
                    "iuphar_type": path_names[0] if len(path_names) > 0 else None,
                    "iuphar_class": path_names[1] if len(path_names) > 1 else None,
                    "iuphar_subclass": path_names[2] if len(path_names) > 2 else None,
                }
                family_records.append(record)

            classification_map[target_id_int] = {
                "target": target,
                "families": sorted(
                    family_records,
                    key=lambda item: (
                        -(item.get("classification_depth") or 0),
                        item.get("iuphar_family_id", 0),
                    ),
                ),
            }

            normalized_name = self._normalize_iuphar_name(str(target.get("name")))
            if normalized_name:
                normalized_index.setdefault(normalized_name, []).append(target_id_int)

        classification_records: list[dict[str, Any]] = []
        gold_records: list[dict[str, Any]] = []

        matched = 0
        total_candidates = 0

        for idx, row in working_df.iterrows():
            candidate_names = self._candidate_names_from_row(row)
            if not candidate_names:
                fallback = self._fallback_classification_record(row)
                classification_records.append(fallback)
                gold_records.append({
                    "target_chembl_id": row.get("target_chembl_id"),
                    "iuphar_target_id": None,
                    "iuphar_type": None,
                    "iuphar_class": None,
                    "iuphar_subclass": None,
                    "classification_source": "chembl",
                })
                self._record_missing_mapping(
                    stage="iuphar",
                    target_id=row.get("target_chembl_id"),
                    accession=None,
                    resolution="chembl_fallback",
                    status="fallback",
                    details={"reason": "no_candidate_names"},
                )
                continue

            total_candidates += 1

            matched_id: int | None = None
            for candidate in candidate_names:
                norm_candidate = self._normalize_iuphar_name(candidate)
                if not norm_candidate:
                    continue
                candidate_ids = normalized_index.get(norm_candidate)
                if candidate_ids:
                    matched_id = candidate_ids[0]
                    break

            if matched_id is None:
                fallback = self._fallback_classification_record(row)
                classification_records.append(fallback)
                gold_records.append({
                    "target_chembl_id": row.get("target_chembl_id"),
                    "iuphar_target_id": None,
                    "iuphar_type": None,
                    "iuphar_class": None,
                    "iuphar_subclass": None,
                    "classification_source": "chembl",
                })
                self._record_missing_mapping(
                    stage="iuphar",
                    target_id=row.get("target_chembl_id"),
                    accession=None,
                    resolution="chembl_fallback",
                    status="fallback",
                    details={"candidates": candidate_names},
                )
                continue

            matched += 1
            target_entry = classification_map.get(matched_id, {})
            families_for_target = target_entry.get("families", [])
            best_classification = self._select_best_classification(families_for_target)

            working_df.at[idx, "iuphar_target_id"] = matched_id
            if best_classification is not None:
                working_df.at[idx, "iuphar_type"] = best_classification.get("iuphar_type")
                working_df.at[idx, "iuphar_class"] = best_classification.get("iuphar_class")
                working_df.at[idx, "iuphar_subclass"] = best_classification.get("iuphar_subclass")

            for record in families_for_target:
                enriched_record = record.copy()
                enriched_record.update(
                    {
                        "target_chembl_id": row.get("target_chembl_id"),
                        "iuphar_target_id": matched_id,
                        "classification_source": "iuphar",
                    }
                )
                classification_records.append(enriched_record)

            gold_records.append(
                {
                    "target_chembl_id": row.get("target_chembl_id"),
                    "iuphar_target_id": matched_id,
                    "iuphar_type": best_classification.get("iuphar_type") if best_classification else None,
                    "iuphar_class": best_classification.get("iuphar_class") if best_classification else None,
                    "iuphar_subclass": best_classification.get("iuphar_subclass") if best_classification else None,
                    "classification_source": "iuphar",
                }
            )

        classification_df = pd.DataFrame(classification_records).convert_dtypes()
        gold_df = pd.DataFrame(gold_records).convert_dtypes()

        coverage = matched / total_candidates if total_candidates else 0.0
        metrics = {
            "enrichment_success.iuphar": coverage,
            "iuphar_coverage": coverage,
            "enrichment.iuphar.total": total_candidates,
            "enrichment.iuphar.matched": matched,
        }

        logger.info(
            "iuphar_enrichment_completed",
            matched=matched,
            total=total_candidates,
            coverage=coverage,
        )

        return working_df, classification_df, gold_df, metrics

    def _fetch_iuphar_collection(
        self,
        path: str,
        unique_key: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a paginated IUPHAR collection safely handling duplicates."""

        if self.iuphar_client is None:
            return []

        collected: list[dict[str, Any]] = []
        seen: set[Any] = set()
        page = 1

        while True:
            query: dict[str, Any] = dict(params or {})
            query.setdefault("page", page)
            query.setdefault("pageSize", self._IUPHAR_PAGE_SIZE)
            query.setdefault("offset", (page - 1) * self._IUPHAR_PAGE_SIZE)
            query.setdefault("limit", self._IUPHAR_PAGE_SIZE)

            payload = self.iuphar_client.request_json(path, params=query)

            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                items = (
                    payload.get("results")
                    or payload.get("data")
                    or payload.get("items")
                    or payload.get("records")
                )
                if items is None and unique_key in payload:
                    items = [payload]
                if items is None:
                    # Attempt to coerce nested dict values into a flat list
                    items = [value for value in payload.values() if isinstance(value, dict)]
            else:
                items = []

            if items is None:
                items = []

            new_items = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                key_value = item.get(unique_key)
                if key_value is not None and key_value in seen:
                    continue
                if key_value is not None:
                    seen.add(key_value)
                collected.append(item)
                new_items += 1

            if new_items == 0 or len(items) < self._IUPHAR_PAGE_SIZE:
                break

            page += 1

        return collected

    def _build_family_hierarchy(
        self, families: dict[int, dict[str, Any]]
    ) -> dict[int, dict[str, list[Any]]]:
        """Construct hierarchical paths for IUPHAR families."""

        cache: dict[int, dict[str, list[Any]]] = {}

        def _resolve(family_id: int) -> dict[str, list[Any]]:
            if family_id in cache:
                return cache[family_id]

            family = families.get(family_id)
            if not family:
                cache[family_id] = {"path_ids": [], "path_names": []}
                return cache[family_id]

            parents = family.get("parentFamilyIds") or []
            if isinstance(parents, int):
                parents = [parents]

            path_ids: list[int] = []
            path_names: list[str] = []

            if isinstance(parents, list) and parents:
                parent_paths: list[dict[str, list[Any]]] = []
                for parent in parents:
                    try:
                        parent_id = int(parent)
                    except (TypeError, ValueError):
                        continue
                    parent_paths.append(_resolve(parent_id))

                if parent_paths:
                    parent_paths.sort(key=lambda info: len(info.get("path_ids", [])), reverse=True)
                    best_parent = parent_paths[0]
                    path_ids.extend(best_parent.get("path_ids", []))
                    path_names.extend(best_parent.get("path_names", []))

            path_ids.append(family_id)
            name = family.get("name")
            if name is not None:
                path_names.append(str(name))

            cache[family_id] = {"path_ids": path_ids, "path_names": path_names}
            return cache[family_id]

        for fam_id in families:
            _resolve(fam_id)

        return cache

    def _normalize_iuphar_name(self, value: str | None) -> str:
        """Normalize identifiers for fuzzy matching."""

        if value is None:
            return ""
        normalized = str(value).lower()
        return self._NAME_CLEAN_RE.sub("", normalized)

    def _candidate_names_from_row(self, row: pd.Series) -> list[str]:
        """Extract candidate names for IUPHAR matching from a row."""

        candidates: list[str] = []

        for column in ("pref_name", "target_names"):
            value = row.get(column)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            if column == "target_names":
                names = [part.strip() for part in str(value).split("|") if part and part.strip()]
                candidates.extend(names)
            else:
                candidates.append(str(value).strip())

        gene_symbol = row.get("uniprot_gene_primary") or row.get("gene_symbol")
        if gene_symbol is not None and not (isinstance(gene_symbol, float) and pd.isna(gene_symbol)):
            candidates.append(str(gene_symbol).strip())

        result: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate:
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(candidate)

        return result

    def _fallback_classification_record(self, row: pd.Series) -> dict[str, Any]:
        """Construct a fallback classification record for unmatched targets."""

        return {
            "target_chembl_id": row.get("target_chembl_id"),
            "iuphar_target_id": None,
            "iuphar_family_id": None,
            "iuphar_family_name": None,
            "classification_path": None,
            "classification_depth": 0,
            "iuphar_type": None,
            "iuphar_class": None,
            "iuphar_subclass": None,
            "classification_source": "chembl",
        }

    def _select_best_classification(
        self, records: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """Select the best classification entry for a matched target."""

        if not records:
            return None
        return records[0]

    def _apply_entry_enrichment(
        self,
        df: pd.DataFrame,
        idx: int,
        entry: dict[str, Any],
        strategy: str,
    ) -> None:
        """Apply UniProt entry information to the target dataframe row."""

        canonical = entry.get("primaryAccession") or entry.get("accession")
        df.at[idx, "uniprot_canonical_accession"] = canonical
        df.at[idx, "uniprot_merge_strategy"] = strategy
        df.at[idx, "uniprot_gene_primary"] = self._extract_gene_primary(entry)
        df.at[idx, "uniprot_gene_synonyms"] = "; ".join(self._extract_gene_synonyms(entry))
        df.at[idx, "uniprot_protein_name"] = self._extract_protein_name(entry)
        df.at[idx, "uniprot_sequence_length"] = self._extract_sequence_length(entry)
        df.at[idx, "uniprot_taxonomy_id"] = self._extract_taxonomy_id(entry)
        df.at[idx, "uniprot_taxonomy_name"] = self._extract_organism(entry)
        df.at[idx, "uniprot_lineage"] = "; ".join(self._extract_lineage(entry))
        df.at[idx, "uniprot_secondary_accessions"] = "; ".join(self._extract_secondary(entry))

    def _run_id_mapping(self, accessions: Iterable[str]) -> pd.DataFrame:
        """Resolve obsolete accessions via UniProt ID mapping service."""

        if self.uniprot_idmapping_client is None:
            return pd.DataFrame(columns=["submitted_id", "canonical_accession"])

        all_rows: list[dict[str, Any]] = []
        for chunk in self._chunked(accessions, self._IDMAPPING_BATCH_SIZE):
            payload = {
                "from": "UniProtKB_AC-ID",
                "to": "UniProtKB",
                "ids": ",".join(chunk),
            }

            try:
                response = self.uniprot_idmapping_client.request_json("/run", method="POST", data=payload)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_submission_failed", error=str(exc))
                continue

            job_id = response.get("jobId") or response.get("job_id")
            if not job_id:
                logger.warning("uniprot_idmapping_no_job", payload=payload)
                continue

            job_result = self._poll_id_mapping(job_id)
            if not job_result:
                continue

            results = job_result.get("results") or job_result.get("mappedTo") or []
            for item in results:
                submitted = item.get("from") or item.get("accession") or item.get("input")
                to_entry = item.get("to") or item.get("mappedTo") or {}
                if isinstance(to_entry, dict):
                    canonical = to_entry.get("primaryAccession") or to_entry.get("accession")
                    isoform = to_entry.get("isoformAccession") or to_entry.get("isoform")
                else:
                    canonical = to_entry
                    isoform = None

                all_rows.append(
                    {
                        "submitted_id": submitted,
                        "canonical_accession": canonical,
                        "isoform_accession": isoform,
                    }
                )

        return pd.DataFrame(all_rows).convert_dtypes()

    def _poll_id_mapping(self, job_id: str) -> dict[str, Any] | None:
        """Poll UniProt ID mapping job until completion respecting rate limits."""

        if self.uniprot_idmapping_client is None:
            return None

        elapsed = 0.0
        while elapsed < self._IDMAPPING_MAX_WAIT:
            try:
                status = self.uniprot_idmapping_client.request_json(f"/status/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_status_failed", job_id=job_id, error=str(exc))
                return None

            job_status = status.get("jobStatus") or status.get("status")
            if job_status in {"RUNNING", "PENDING"}:
                time.sleep(self._IDMAPPING_POLL_INTERVAL)
                elapsed += self._IDMAPPING_POLL_INTERVAL
                continue

            if job_status not in {"FINISHED", "finished", "SUCCESS"}:
                logger.warning("uniprot_idmapping_failed", job_id=job_id, status=job_status)
                return None

            try:
                return self.uniprot_idmapping_client.request_json(f"/results/{job_id}")
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("uniprot_idmapping_results_failed", job_id=job_id, error=str(exc))
                return None

        logger.warning("uniprot_idmapping_timeout", job_id=job_id)
        return None

    def _expand_isoforms(self, entry: dict[str, Any]) -> pd.DataFrame:
        """Expand isoform details from a UniProt entry."""

        canonical = entry.get("primaryAccession") or entry.get("accession")
        sequence_length = self._extract_sequence_length(entry)
        records: list[dict[str, Any]] = [
            {
                "canonical_accession": canonical,
                "isoform_accession": canonical,
                "isoform_name": self._extract_protein_name(entry),
                "is_canonical": True,
                "sequence_length": sequence_length,
                "source": "canonical",
            }
        ]

        comments = entry.get("comments", []) or []
        for comment in comments:
            if comment.get("commentType") != "ALTERNATIVE PRODUCTS":
                continue
            for isoform in comment.get("isoforms", []) or []:
                isoform_ids = isoform.get("isoformIds") or isoform.get("isoformId")
                if isinstance(isoform_ids, list) and isoform_ids:
                    isoform_acc = isoform_ids[0]
                else:
                    isoform_acc = isoform_ids
                if not isoform_acc:
                    continue

                names = isoform.get("names") or []
                if isinstance(names, list):
                    iso_name = next((n.get("value") for n in names if isinstance(n, dict) and n.get("value")), None)
                elif isinstance(names, dict):
                    iso_name = names.get("value")
                else:
                    iso_name = None

                length = isoform.get("sequence", {}).get("length") if isinstance(isoform.get("sequence"), dict) else None
                records.append(
                    {
                        "canonical_accession": canonical,
                        "isoform_accession": isoform_acc,
                        "isoform_name": iso_name,
                        "is_canonical": False,
                        "sequence_length": length,
                        "source": "isoform",
                    }
                )

        return pd.DataFrame(records).convert_dtypes()

    def _fetch_orthologs(self, accession: str, taxonomy_id: Any | None = None) -> pd.DataFrame:
        """Fetch ortholog accessions using the dedicated UniProt client."""

        client = self.uniprot_orthologs_client or self.uniprot_client
        if client is None:
            return pd.DataFrame(columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"])

        params = {
            "query": f"relationship_type:ortholog AND (accession:{accession} OR xref:{accession})",
            "fields": self._ORTHOLOG_FIELDS,
            "format": "json",
            "size": 200,
        }

        try:
            payload = client.request_json("/", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("ortholog_fetch_failed", accession=accession, error=str(exc))
            return pd.DataFrame(columns=["source_accession", "ortholog_accession", "organism_id", "organism", "priority"])

        results = payload.get("results") or payload.get("entries") or []
        records: list[dict[str, Any]] = []
        for item in results:
            ortholog_acc = item.get("primaryAccession") or item.get("accession")
            organism = item.get("organism", {}) if isinstance(item.get("organism"), dict) else item.get("organism")
            if isinstance(organism, dict):
                organism_name = organism.get("scientificName") or organism.get("commonName")
                organism_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            else:
                organism_name = organism
                organism_id = item.get("organismId")

            organism_id_str = str(organism_id) if organism_id is not None else None
            priority = self._ORTHOLOG_PRIORITY.get(organism_id_str or "", 99)
            records.append(
                {
                    "source_accession": accession,
                    "ortholog_accession": ortholog_acc,
                    "organism": organism_name,
                    "organism_id": organism_id,
                    "priority": priority,
                }
            )

        if taxonomy_id is not None:
            try:
                taxonomy_str = str(int(taxonomy_id))
            except (TypeError, ValueError):
                taxonomy_str = None
            if taxonomy_str:
                for record in records:
                    if str(record.get("organism_id")) == taxonomy_str:
                        record["priority"] = min(record.get("priority", 99), -1)

        return pd.DataFrame(records).convert_dtypes()

    def _search_uniprot_by_gene(self, gene_symbol: str, organism: Any | None = None) -> dict[str, Any] | None:
        """Search UniProt entries by gene symbol as a fallback strategy."""

        if self.uniprot_client is None:
            return None

        query = f"gene_exact:{gene_symbol}"
        if organism:
            query = f"{query} AND organism_name:\"{organism}\""

        params = {
            "query": query,
            "fields": self._UNIPROT_FIELDS,
            "format": "json",
            "size": 1,
        }

        try:
            payload = self.uniprot_client.request_json("/search", params=params)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("uniprot_gene_search_failed", gene=gene_symbol, error=str(exc))
            return None

        results = payload.get("results") or payload.get("entries") or []
        if not results:
            return None
        return results[0]  # type: ignore[no-any-return]

    def _extract_gene_primary(self, entry: dict[str, Any]) -> str | None:
        genes = entry.get("genes") or []
        if isinstance(genes, list):
            for gene in genes:
                primary = gene.get("geneName") if isinstance(gene, dict) else None
                if isinstance(primary, dict):
                    return primary.get("value")
                if isinstance(primary, str):
                    return primary
        return entry.get("gene_primary") or entry.get("genePrimary")

    def _extract_gene_synonyms(self, entry: dict[str, Any]) -> list[str]:
        synonyms: list[str] = []
        genes = entry.get("genes") or []
        if isinstance(genes, list):
            for gene in genes:
                syn_list = gene.get("synonyms") if isinstance(gene, dict) else None
                if isinstance(syn_list, list):
                    for synonym in syn_list:
                        if isinstance(synonym, dict) and synonym.get("value"):
                            synonyms.append(synonym["value"])
                        elif isinstance(synonym, str):
                            synonyms.append(synonym)
        extra = entry.get("gene_synonym") or entry.get("geneSynonym")
        if isinstance(extra, list):
            synonyms.extend(str(v) for v in extra if v)
        elif isinstance(extra, str):
            synonyms.append(extra)
        return sorted({syn.strip() for syn in synonyms if syn})

    def _extract_secondary(self, entry: dict[str, Any]) -> list[str]:
        values = entry.get("secondaryAccession") or entry.get("secondaryAccessions") or []
        if isinstance(values, str):
            values = [values]
        return [str(value).strip() for value in values if value]

    def _extract_protein_name(self, entry: dict[str, Any]) -> str | None:
        protein = entry.get("proteinDescription") or entry.get("protein")
        if isinstance(protein, dict):
            recommended = protein.get("recommendedName")
            if isinstance(recommended, dict):
                full_name = recommended.get("fullName")
                if isinstance(full_name, dict):
                    return full_name.get("value")
                if isinstance(full_name, str):
                    return full_name
        if isinstance(protein, str):
            return protein
        return entry.get("protein_name")

    def _extract_sequence_length(self, entry: dict[str, Any]) -> int | None:
        sequence = entry.get("sequence")
        if isinstance(sequence, dict):
            length = sequence.get("length")
            if isinstance(length, int):
                return length
            if isinstance(length, str) and length.isdigit():
                return int(length)
        length_field = entry.get("sequence_length") or entry.get("sequenceLength")
        if isinstance(length_field, int):
            return length_field
        if isinstance(length_field, str) and length_field.isdigit():
            return int(length_field)
        return None

    def _extract_taxonomy_id(self, entry: dict[str, Any]) -> int | None:
        organism = entry.get("organism")
        if isinstance(organism, dict):
            taxon_id = organism.get("taxonId") or organism.get("taxonIdentifier")
            if isinstance(taxon_id, int):
                return taxon_id
            if isinstance(taxon_id, str) and taxon_id.isdigit():
                return int(taxon_id)
        identifier = entry.get("organism_id") or entry.get("organismId")
        if isinstance(identifier, int):
            return identifier
        if isinstance(identifier, str) and identifier.isdigit():
            return int(identifier)
        return None

    def _extract_organism(self, entry: dict[str, Any]) -> str | None:
        organism = entry.get("organism")
        if isinstance(organism, dict):
            return organism.get("scientificName") or organism.get("commonName")
        if isinstance(organism, str):
            return organism
        return entry.get("organism_name")

    def _extract_lineage(self, entry: dict[str, Any]) -> list[str]:
        lineage = entry.get("lineage")
        results: list[str] = []
        if isinstance(lineage, list):
            for item in lineage:
                if isinstance(item, dict) and item.get("scientificName"):
                    results.append(item["scientificName"])
                elif isinstance(item, str):
                    results.append(item)
        return results

    def _materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
    ) -> None:
        """Persist silver-level UniProt artifacts deterministically."""

        if uniprot_df.empty and component_df.empty:
            logger.info("silver_materialization_skipped", reason="empty_frames")
            return

        silver_path_config = self.config.materialization.silver or Path("data/output/target/targets_silver.parquet")
        silver_path = Path(silver_path_config)
        silver_path.parent.mkdir(parents=True, exist_ok=True)

        if not uniprot_df.empty:
            logger.info(
                "writing_silver_dataset",
                path=str(silver_path),
                rows=len(uniprot_df),
            )
            uniprot_df.sort_values("canonical_accession", inplace=True)
            uniprot_df.to_parquet(silver_path, index=False)

        component_path = silver_path.parent / "component_enrichment.parquet"
        if not component_df.empty:
            logger.info(
                "writing_component_enrichment",
                path=str(component_path),
                rows=len(component_df),
            )
            component_df.sort_values(["canonical_accession", "isoform_accession"], inplace=True)
            component_df.to_parquet(component_path, index=False)
        elif component_path.exists():
            logger.info("component_enrichment_empty", path=str(component_path))

    def _materialize_iuphar(
        self,
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
    ) -> None:
        """Persist IUPHAR classification artifacts."""

        if classification_df.empty and gold_df.empty:
            logger.info("iuphar_materialization_skipped", reason="empty_frames")
            return

        output_dir = Path(self.config.materialization.gold or Path("data/output/target/targets_final.parquet")).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        if not classification_df.empty:
            classification_path = output_dir / "targets_iuphar_classification.parquet"
            logger.info(
                "writing_iuphar_classification",
                path=str(classification_path),
                rows=len(classification_df),
            )
            classification_sorted = classification_df.sort_values(
                by=["target_chembl_id", "iuphar_family_id"],
                kind="stable",
            )
            classification_sorted.to_parquet(classification_path, index=False)

        if not gold_df.empty:
            gold_path = output_dir / "targets_iuphar_enrichment.parquet"
            logger.info(
                "writing_iuphar_gold",
                path=str(gold_path),
                rows=len(gold_df),
            )
            gold_sorted = gold_df.sort_values(by=["target_chembl_id"], kind="stable")
            gold_sorted.to_parquet(gold_path, index=False)

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

        runtime = getattr(self.config, "runtime", None)
        if runtime is not None and getattr(runtime, "dry_run", False):
            logger.info("gold_materialization_skipped", reason="dry_run")
            return

        gold_path = getattr(self.config.materialization, "gold", Path("data/output/target/targets_final.parquet"))
        materialize_gold(
            Path(gold_path),
            targets=targets_df,
            components=components_df,
            protein_class=protein_class_df,
            xref=xref_df,
        )

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

    if pipeline.iuphar_client is None:
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

