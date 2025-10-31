"""Standalone pipeline that materialises PubChem enrichment results."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import pandas as pd

from bioetl.adapters import PubChemAdapter
from bioetl.adapters.base import AdapterConfig
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import PubChemSchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.config import coerce_float_config, coerce_int_config
from bioetl.utils.qc import update_summary_metrics, update_summary_section

__all__: Final[tuple[str, ...]] = ("PubChemPipeline",)

logger = UnifiedLogger.get(__name__)

schema_registry.register("pubchem", "1.0.0", PubChemSchema)  # type: ignore[arg-type]


class PubChemPipeline(PipelineBase):
    """Pipeline that enriches ChEMBL molecules with PubChem metadata."""

    _LOOKUP_COLUMNS: Final[tuple[str, ...]] = ("molecule_chembl_id", "standard_inchi_key")
    _PUBCHEM_COLUMNS: Final[tuple[str, ...]] = tuple(PubChemAdapter._PUBCHEM_COLUMNS)
    _DEFAULT_LOOKUP_FILENAME: Final[Path] = Path("pubchem_lookup.csv")

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = PubChemSchema
        self.pubchem_adapter = self._create_pubchem_adapter()
        if self.pubchem_adapter is not None:
            self.register_client(self.pubchem_adapter.api_client)

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def extract(self, input_file: Path | None = None) -> pd.DataFrame:  # noqa: D401
        """Load the seed lookup table of ChEMBL identifiers and InChIKeys."""

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

        adapter = self.pubchem_adapter
        if adapter is None:
            logger.warning("pubchem_adapter_unavailable", reason="disabled_or_missing")
            enriched = self._ensure_pubchem_columns(working)
            enrichment_metric = {
                "count": 0,
                "value": 0.0,
                "threshold": min_enrichment,
                "passed": min_enrichment == 0.0,
                "severity": "warning" if min_enrichment > 0 else "info",
            }
        elif inchi_present == 0:
            logger.warning("pubchem_enrichment_skipped", reason="no_inchikeys")
            enriched = self._ensure_pubchem_columns(working)
            enrichment_metric = {
                "count": 0,
                "value": 0.0,
                "threshold": min_enrichment,
                "passed": min_enrichment == 0.0,
                "severity": "warning" if min_enrichment > 0 else "info",
            }
        else:
            enriched = adapter.enrich_with_pubchem(working, inchi_key_col="standard_inchi_key")
            enriched = self._ensure_pubchem_columns(enriched)
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

        enriched = self._normalise_pubchem_types(enriched)
        required_columns = list(self._LOOKUP_COLUMNS) + list(self._PUBCHEM_COLUMNS)
        enriched = enriched[required_columns]

        sort_config = getattr(self.determinism, "sort", None)
        if sort_config is None:
            sort_columns = ["molecule_chembl_id"]
            ascending_param: list[bool] | bool = True
        else:
            sort_columns = list(getattr(sort_config, "by", []) or ["molecule_chembl_id"])
            ascending_flags = list(getattr(sort_config, "ascending", []) or [])
            ascending_param = ascending_flags if ascending_flags else True

        final_df = self.finalize_with_standard_metadata(
            enriched,
            business_key="molecule_chembl_id",
            sort_by=sort_columns,
            ascending=ascending_param,
            schema=PubChemSchema,
            default_source="pubchem",
            chembl_release=None,
        )

        self.set_export_metadata_from_dataframe(
            final_df,
            pipeline_version=self.config.pipeline.version,
            source_system="pubchem",
            chembl_release=None,
            schema=PubChemSchema,
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
        """Close the PubChem adapter client if present."""

        self._close_resource(self.pubchem_adapter, resource_name="external_adapter.pubchem")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
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
            return self._DEFAULT_LOOKUP_FILENAME
        return candidate

    def _create_pubchem_adapter(self) -> PubChemAdapter | None:
        source = self.config.sources.get("pubchem") if self.config.sources else None
        if source is None or not getattr(source, "enabled", True):
            logger.info("pubchem_adapter_disabled", enabled=getattr(source, "enabled", False))
            return None

        default_base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
        base_url = getattr(source, "base_url", default_base_url) or default_base_url

        headers: dict[str, str] = {}
        source_headers = getattr(source, "headers", {})
        if isinstance(source_headers, Mapping):
            headers.update({str(key): str(value) for key, value in source_headers.items()})

        rate_limit_max_calls_raw = getattr(source, "rate_limit_max_calls", None)
        rate_limit_period_raw = getattr(source, "rate_limit_period", None)
        rate_limit_jitter = getattr(source, "rate_limit_jitter", True)
        batch_size_raw = getattr(source, "batch_size", None)
        workers_raw = getattr(source, "workers", 1)

        def _log(event: str, **payload: Any) -> None:
            logger.warning(event, source="pubchem", **payload)

        http_profile_name = getattr(source, "http_profile", None)
        http_profile = self.config.http.get(http_profile_name) if http_profile_name else None
        if http_profile is not None:
            http_headers = getattr(http_profile, "headers", {})
            if isinstance(http_headers, Mapping):
                headers.update({str(key): str(value) for key, value in http_headers.items()})
            rate_limit = getattr(http_profile, "rate_limit", None)
            if rate_limit is not None:
                rate_limit_max_calls_raw = getattr(rate_limit, "max_calls", rate_limit_max_calls_raw)
                rate_limit_period_raw = getattr(rate_limit, "period", rate_limit_period_raw)
            profile_jitter = getattr(http_profile, "rate_limit_jitter", None)
            if profile_jitter is not None:
                rate_limit_jitter = bool(profile_jitter)

        http_config = getattr(source, "http", None)
        if http_config is not None:
            http_headers = getattr(http_config, "headers", {})
            if isinstance(http_headers, Mapping):
                headers.update({str(key): str(value) for key, value in http_headers.items()})
            rate_limit = getattr(http_config, "rate_limit", None)
            if rate_limit is not None:
                rate_limit_max_calls_raw = getattr(rate_limit, "max_calls", rate_limit_max_calls_raw)
                rate_limit_period_raw = getattr(rate_limit, "period", rate_limit_period_raw)
            http_jitter = getattr(http_config, "rate_limit_jitter", None)
            if http_jitter is not None:
                rate_limit_jitter = bool(http_jitter)

        cache_maxsize = getattr(self.config.cache, "maxsize", None)
        if cache_maxsize is None:
            cache_maxsize = APIConfig.__dataclass_fields__["cache_maxsize"].default  # type: ignore[index]

        rate_limit_max_calls = coerce_int_config(
            rate_limit_max_calls_raw,
            5,
            field="rate_limit_max_calls",
            minimum=1,
            log=_log,
            invalid_event="pubchem_config_invalid_int",
            out_of_range_event="pubchem_config_out_of_range",
        )
        rate_limit_period = coerce_float_config(
            rate_limit_period_raw,
            1.0,
            field="rate_limit_period",
            minimum=0.0,
            exclusive_minimum=True,
            log=_log,
            invalid_event="pubchem_config_invalid_float",
            out_of_range_event="pubchem_config_out_of_range",
        )
        batch_size = coerce_int_config(
            batch_size_raw,
            100,
            field="batch_size",
            minimum=1,
            log=_log,
            invalid_event="pubchem_config_invalid_int",
            out_of_range_event="pubchem_config_out_of_range",
        )
        workers = coerce_int_config(
            workers_raw,
            1,
            field="workers",
            minimum=1,
            log=_log,
            invalid_event="pubchem_config_invalid_int",
            out_of_range_event="pubchem_config_out_of_range",
        )

        adapter_kwargs: dict[str, Any] = {
            "enabled": True,
            "batch_size": batch_size,
            "workers": workers,
        }
        for optional_field in ("tool", "email", "api_key", "mailto"):
            value = getattr(source, optional_field, None)
            if value:
                adapter_kwargs[optional_field] = value

        api_config = APIConfig(
            name="pubchem",
            base_url=base_url,
            headers=headers,
            cache_enabled=self.config.cache.enabled,
            cache_ttl=self.config.cache.ttl,
            cache_maxsize=cache_maxsize,
            rate_limit_max_calls=rate_limit_max_calls,
            rate_limit_period=rate_limit_period,
            rate_limit_jitter=bool(rate_limit_jitter),
        )

        adapter = PubChemAdapter(api_config, AdapterConfig(**adapter_kwargs))
        logger.info(
            "pubchem_adapter_initialized",
            base_url=base_url,
            batch_size=batch_size,
            rate_limit_max_calls=rate_limit_max_calls,
            rate_limit_period=rate_limit_period,
        )
        return adapter

    def _ensure_pubchem_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        enriched = df.copy()
        for column in self._PUBCHEM_COLUMNS:
            if column not in enriched.columns:
                enriched[column] = pd.NA
        if "standard_inchi_key" not in enriched.columns:
            enriched["standard_inchi_key"] = pd.Series(dtype="string")
        return enriched

    def _normalise_pubchem_types(self, df: pd.DataFrame) -> pd.DataFrame:
        working = df.copy()
        if "pubchem_cid" in working.columns:
            working["pubchem_cid"] = pd.to_numeric(
                working["pubchem_cid"], errors="coerce"
            ).astype("Int64")
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
