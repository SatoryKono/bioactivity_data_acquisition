"""UniProt pipeline for standalone enrichment and export."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.client_factory import APIClientFactory
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import UniProtSchema
from bioetl.schemas.pipeline_inputs import UniProtInputSchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import (
    prepare_enrichment_metrics,
    prepare_missing_mappings,
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

from .client import (
    UniProtIdMappingClient,
    UniProtOrthologsClient,
    UniProtSearchClient,
)
from .normalizer_service import UniProtNormalizer

UniProtOrthologClient = UniProtOrthologsClient

logger = UnifiedLogger.get(__name__)

schema_registry.register("uniprot", "1.0.0", UniProtSchema)  # type: ignore[arg-type]


class UniProtPipeline(PipelineBase):
    """Pipeline orchestrating standalone UniProt enrichment."""

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
        "9606": 0,
        "10090": 1,
        "10116": 2,
    }

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = UniProtSchema
        self.api_clients: dict[str, UnifiedAPIClient] = {}
        self._qc_missing_mapping_records: list[dict[str, Any]] = []

        factory = APIClientFactory.from_pipeline_config(config)
        for name, source in config.sources.items():
            if source is None or not getattr(source, "enabled", True):
                continue
            api_client_config = factory.create(name, source)
            client = UnifiedAPIClient(api_client_config)
            self.api_clients[name] = client
            self.register_client(client)

        search_client = self.api_clients.get("uniprot")
        id_mapping_client = self.api_clients.get("uniprot_idmapping")
        ortholog_client = self.api_clients.get("uniprot_orthologs") or search_client

        self.search_client = UniProtSearchClient(
            client=search_client,
            fields=self._UNIPROT_FIELDS,
            batch_size=self._UNIPROT_BATCH_SIZE,
        )
        self.id_mapping_client = UniProtIdMappingClient(
            client=id_mapping_client,
            batch_size=self._IDMAPPING_BATCH_SIZE,
            poll_interval=self._IDMAPPING_POLL_INTERVAL,
            max_wait=self._IDMAPPING_MAX_WAIT,
        )
        self.ortholog_client = UniProtOrthologClient(
            client=ortholog_client,
            fields=self._ORTHOLOG_FIELDS,
            priority_map=self._ORTHOLOG_PRIORITY,
        )

        self.normalizer = UniProtNormalizer(
            search_client=self.search_client,
            id_mapping_client=self.id_mapping_client,
            ortholog_client=self.ortholog_client,
        )

    def close_resources(self) -> None:
        """Close UniProt clients created for the pipeline."""

        try:
            for name, client in self.api_clients.items():
                self._close_resource(client, resource_name=f"api_client.{name}")
        finally:
            super().close_resources()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Read seed dataset for UniProt enrichment."""

        expected_columns = [
            "uniprot_accession",
            "gene_symbol",
            "organism",
            "taxonomy_id",
        ]
        df, _ = self.read_input_table(
            default_filename=Path("uniprot.csv"),
            expected_columns=expected_columns,
            input_file=input_file,
        )
        if "accession" in df.columns and "uniprot_accession" not in df.columns:
            df = df.rename(columns={"accession": "uniprot_accession"})
        schema_columns = UniProtInputSchema.to_schema().columns.keys()
        for column in schema_columns:
            if column not in df.columns:
                df[column] = pd.Series(pd.NA, index=df.index)
        df = df.convert_dtypes()
        if not df.empty:
            df = UniProtInputSchema.validate(df, lazy=True)
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with UniProt metadata and prepare QC artifacts."""

        result = self.normalizer.enrich_targets(
            df,
            accession_column="uniprot_accession",
            target_id_column=None,
            gene_symbol_column="gene_symbol",
            organism_column="organism",
            taxonomy_column="taxonomy_id",
        )

        self._qc_missing_mapping_records.clear()
        for record in result.missing_mappings:
            self._qc_missing_mapping_records.append(record)
        for issue in result.validation_issues:
            self.record_validation_issue(issue)

        metrics_records = [
            {"metric": name, "value": value}
            for name, value in result.metrics.items()
        ]
        if metrics_records:
            self.qc_enrichment_metrics = prepare_enrichment_metrics(metrics_records)
        else:
            self.qc_enrichment_metrics = pd.DataFrame()

        self.qc_missing_mappings = prepare_missing_mappings(self._qc_missing_mapping_records)
        self.set_qc_metrics(result.metrics, merge=False)

        self.reset_additional_tables()
        if not result.silver.empty:
            self.add_additional_table(
                "uniprot_entries",
                result.silver,
                formats=("csv", "parquet"),
            )
        if not result.components.empty:
            self.add_additional_table(
                "uniprot_isoforms",
                result.components,
                formats=("csv", "parquet"),
            )

        timestamp = pd.Timestamp.now(tz="UTC").isoformat()
        sort_config = getattr(self.config, "determinism", None)
        sort_settings = getattr(sort_config, "sort", None)
        sort_columns = list(getattr(sort_settings, "by", []) or [])
        if not sort_columns:
            sort_columns = ["uniprot_accession"]
        ascending_flags = list(getattr(sort_settings, "ascending", []) or [])
        ascending_param = ascending_flags if ascending_flags else None

        final_df = finalize_output_dataset(
            result.dataframe,
            business_key="uniprot_accession",
            sort_by=sort_columns,
            ascending=ascending_param,
            schema=UniProtSchema,
            metadata={
                "pipeline_version": self.config.pipeline.version,
                "run_id": self.run_id,
                "source_system": "uniprot",
                "chembl_release": None,
                "extracted_at": timestamp,
            },
        )

        self.set_export_metadata_from_dataframe(
            final_df,
            pipeline_version=self.config.pipeline.version,
            source_system="uniprot",
            chembl_release=None,
            column_order=list(final_df.columns),
        )

        update_summary_metrics(self.qc_summary_data, self.qc_metrics)
        update_validation_issue_summary(self.qc_summary_data, self.validation_issues)
        update_summary_section(
            self.qc_summary_data,
            "row_counts",
            {"uniprot": int(len(final_df))},
        )
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

        return final_df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate UniProt output against the registered Pandera schema."""

        return self.run_schema_validation(
            df,
            UniProtSchema,
            dataset_name="uniprot",
            severity="critical",
        )
