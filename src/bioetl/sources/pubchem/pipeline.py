"""Standalone PubChem enrichment pipeline using the modular source layout."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.registry import schema_registry
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import update_summary_metrics, update_summary_section

from .client.pubchem_client import PubChemClient
from .normalizer.pubchem_normalizer import PubChemNormalizer
from .schema import PubChemSchema

if TYPE_CHECKING:  # pragma: no cover - imported for type checkers only
    from bioetl.config import PipelineConfig

__all__: Final[tuple[str, ...]] = ("PubChemPipeline",)


logger = UnifiedLogger.get(__name__)

schema_registry.register("pubchem", "1.0.0", PubChemSchema)  # type: ignore[arg-type]


class PubChemPipeline(PipelineBase):
    """Pipeline orchestrating PubChem enrichment for ChEMBL molecules."""

    _LOOKUP_COLUMNS: Final[tuple[str, ...]] = ("molecule_chembl_id", "standard_inchi_key")

    def __init__(self, config: "PipelineConfig", run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = PubChemSchema
        self.normalizer = PubChemNormalizer()
        self.pubchem_client, self._api_client = PubChemClient.from_config(config)
        if self._api_client is not None:
            self.register_client(self._api_client)

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:  # noqa: D401
        """Load the input lookup table of ChEMBL identifiers and InChIKeys."""

        default_lookup = self._resolve_lookup_source()
        df, _ = self.read_input_table(
            default_filename=default_lookup,
            expected_columns=list(self._LOOKUP_COLUMNS),
            input_file=input_file,
        )
        if not df.empty:
            df = df.convert_dtypes()
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
        """Enrich lookup records with PubChem properties."""

        working = df.copy()
        for column in self._LOOKUP_COLUMNS:
            if column not in working.columns:
                working[column] = pd.NA

        if "standard_inchi_key" in working.columns:
            working["standard_inchi_key"] = (
                working["standard_inchi_key"].astype("string").str.upper().str.strip()
            )

        total_rows = int(len(working))
        inchi_present = (
            int(working["standard_inchi_key"].notna().sum())
            if "standard_inchi_key" in working.columns
            else 0
        )
        coverage = float(inchi_present / total_rows) if total_rows else 0.0

        thresholds = getattr(self.config.qc, "thresholds", {}) if self.config.qc else {}
        min_coverage = float(thresholds.get("pubchem.min_inchikey_coverage", 0.0))
        min_enrichment = float(thresholds.get("pubchem.min_enrichment_rate", 0.0))

        coverage_metric = {
            "count": inchi_present,
            "value": coverage,
            "threshold": min_coverage,
            "passed": coverage >= min_coverage,
            "severity": "warning" if coverage < min_coverage else "info",
        }

        enrichment_metric: dict[str, Any]
        enriched = working

        if self.pubchem_client is None:
            logger.warning("pubchem_enrichment_disabled", reason="client_unavailable")
            enriched = self.normalizer.ensure_columns(working)
            enrichment_metric = {
                "count": 0,
                "value": 0.0,
                "threshold": min_enrichment,
                "passed": min_enrichment == 0.0,
                "severity": "warning" if min_enrichment > 0 else "info",
            }
        elif inchi_present == 0:
            logger.warning("pubchem_enrichment_skipped", reason="no_inchikeys")
            enriched = self.normalizer.ensure_columns(working)
            enrichment_metric = {
                "count": 0,
                "value": 0.0,
                "threshold": min_enrichment,
                "passed": min_enrichment == 0.0,
                "severity": "warning" if min_enrichment > 0 else "info",
            }
        else:
            enriched = self.normalizer.enrich_dataframe(
                working,
                inchi_key_col="standard_inchi_key",
                client=self.pubchem_client,
            )
            enriched = self.normalizer.ensure_columns(enriched)
            enriched_rows = int(enriched["pubchem_cid"].notna().sum())
            enrichment_rate = float(enriched_rows / total_rows) if total_rows else 0.0
            enrichment_metric = {
                "count": enriched_rows,
                "value": enrichment_rate,
                "threshold": min_enrichment,
                "passed": enrichment_rate >= min_enrichment,
                "severity": "warning" if enrichment_rate < min_enrichment else "info",
            }

        update_summary_metrics(
            self.qc_summary_data,
            {
                "pubchem.inchikey_coverage": coverage_metric,
                "pubchem.enrichment_rate": enrichment_metric,
            },
        )

        enriched = self.normalizer.normalize_types(enriched)
        required_columns = list(self._LOOKUP_COLUMNS) + self.normalizer.get_pubchem_columns()
        enriched = enriched[required_columns]

        sort_config = getattr(self.determinism, "sort", None)
        if sort_config is None:
            sort_columns = ["molecule_chembl_id"]
            ascending_param: list[bool] | bool | None = True
        else:
            sort_columns = list(getattr(sort_config, "by", []) or ["molecule_chembl_id"])
            ascending_flags = list(getattr(sort_config, "ascending", []) or [])
            ascending_param = ascending_flags if ascending_flags else True

        timestamp = pd.Timestamp.now(tz="UTC").isoformat()
        final_df = finalize_output_dataset(
            enriched,
            business_key="molecule_chembl_id",
            sort_by=sort_columns,
            ascending=ascending_param,
            schema=PubChemSchema,
            metadata={
                "pipeline_version": self.config.pipeline.version,
                "run_id": self.run_id,
                "source_system": "pubchem",
                "chembl_release": None,
                "extracted_at": timestamp,
            },
        )

        self.set_export_metadata_from_dataframe(
            final_df,
            pipeline_version=self.config.pipeline.version,
            source_system="pubchem",
            chembl_release=None,
            column_order=list(final_df.columns),
        )

        return final_df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401
        """Validate the enriched dataset against the registered schema."""

        validated_df = self.run_schema_validation(
            df,
            PubChemSchema,
            dataset_name="pubchem",
            severity="error",
        )

        row_count = int(len(validated_df)) if validated_df is not None else 0
        update_summary_section(self.qc_summary_data, "row_counts", {"pubchem": row_count})
        update_summary_section(
            self.qc_summary_data,
            "datasets",
            {"pubchem": {"rows": row_count}},
        )
        return validated_df

    def close_resources(self) -> None:  # noqa: D401
        """Close the PubChem client if present."""

        self._close_resource(self._api_client, resource_name="api_client.pubchem")
        super().close_resources()

    def _resolve_lookup_source(self) -> Path:
        enrichment_config = getattr(self.config.postprocess, "enrichment", {})
        candidate: Path | None = None
        if isinstance(enrichment_config, Mapping):
            lookup_value = enrichment_config.get("pubchem_lookup_input") or enrichment_config.get(
                "pubchem_lookup"
            )
            if lookup_value:
                candidate = Path(str(lookup_value))
        if candidate is None:
            return Path("pubchem_lookup.csv")
        return candidate
