"""Упрощённый Activity-пайплайн ChEMBL на базе общего каркаса."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import math

import pandas as pd
from structlog.stdlib import BoundLogger

import bioetl.vocab.service as vocab_service
from bioetl.chembl.common.normalize import add_row_metadata
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.core.schema.normalizers import IdentifierRule, StringRule
from bioetl.pipelines.chembl.common import BaseChemblPipeline
from bioetl.pipelines.chembl._constants import API_ACTIVITY_FIELDS
from bioetl.pipelines.chembl.mixins import FieldMappingRule
from bioetl.schemas import get_schema
from bioetl.schemas.chembl_activity_schema import ACTIVITY_PROPERTY_KEYS


class ChemblActivityPipeline(BaseChemblPipeline):
    """Adapter-класс, реализующий правила нормализации/обогащения для активности.

    Конструктор поддерживает два режима и остаётся совместимым с существующим
    контрактом:

    - основной путь продакшена: ``ChemblActivityPipeline(config, run_id, source, *)``;
    - упрощённый тестовый путь: ``ChemblActivityPipeline(source)`` — используется
      в модульных тестах для быстрой smoke-проверки нормализации/обогащения без
      загрузки конфигурации.
    """

    entity_name = "activity"
    id_column = "activity_id"
    actor = "activity_pipeline_actor"
    descriptor_must_have_fields: tuple[str, ...] = ("activity_id",)
    descriptor_default_select_fields = API_ACTIVITY_FIELDS
    def __init__(
        self,
        config_or_source: Any | None = None,
        run_id: str | None = None,
        source: Iterable[dict[str, Any]] | None = None,
        *,
        config: Any | None = None,
        writer: Any = None,
    ) -> None:
        """Инициализировать пайплайн ChEMBL activity.

        Путь 1 (основной):
        ``ChemblActivityPipeline(config, run_id, source, writer=...)``

        Путь 2 (тестовый):
        ``ChemblActivityPipeline(source)`` — в этом случае будет создан минимальный
        in-memory ``PipelineConfig`` с детерминированными путями вывода и
        сгенерированным ``run_id``.
        """

        from bioetl.config.models.base import PipelineMetadata
        from bioetl.config.models.cli import CLIConfig
        from bioetl.config.models.determinism import (
            DeterminismConfig,
            DeterminismHashingConfig,
            DeterminismSortingConfig,
        )
        from bioetl.config.models.domain import PipelineDomainConfig
        from bioetl.config.models.http import (
            HTTPClientConfig,
            HTTPConfig,
            RetryConfig,
        )
        from bioetl.config.models.infrastructure import (
            PipelineInfrastructureConfig,
        )
        from bioetl.config.models.models import PipelineConfig
        from bioetl.config.models.paths import MaterializationConfig
        from bioetl.config.models.postprocess import PostprocessConfig
        from bioetl.config.models.source import SourceConfig, SourceParameters
        from bioetl.config.models.validation import ValidationConfig

        # Явный config= имеет приоритет над позиционным аргументом для совместимости
        if config is not None:
            # В config= пути объект трактуем как конфигурацию пайплайна, а не как source.
            # Это гарантирует, что extract_all делегирует в descriptor-driven
            # ChemblPipelineBase.run_extract_all, а не в legacy _source-путь.
            if run_id is None:
                msg = "ChemblActivityPipeline requires run_id when initialised with config="
                raise TypeError(msg)
            effective_source = source
            effective_run_id = run_id
        elif isinstance(config_or_source, PipelineConfig):
            # Основной продакшен-контракт: позиционный PipelineConfig.
            if run_id is None:
                msg = "ChemblActivityPipeline requires run_id when initialised with PipelineConfig"
                raise TypeError(msg)
            config = config_or_source
            effective_source = source
            effective_run_id = run_id
        else:
            # Тестовый путь: первый аргумент трактуем как source iterable.
            effective_source = (
                source if source is not None else config_or_source
            )

            # Сконструировать минимальный детерминированный PipelineConfig.
            http_config = HTTPConfig(
                default=HTTPClientConfig(
                    timeout_sec=30.0,
                    connect_timeout_sec=10.0,
                    read_timeout_sec=30.0,
                    retries=RetryConfig(
                        total=3, backoff_multiplier=2.0, backoff_max=10.0
                    ),
                ),
            )

            determinism_config = DeterminismConfig(
                sort=DeterminismSortingConfig(by=[], ascending=[]),
                hashing=DeterminismHashingConfig(business_key_fields=()),
            )

            from bioetl.config.models.cache import CacheConfig
            from bioetl.config.models.io import IOConfig
            from bioetl.config.models.logging import LoggingConfig
            from bioetl.config.models.paths import PathsConfig
            from bioetl.config.models.runtime import RuntimeConfig
            from bioetl.config.models.telemetry import TelemetryConfig

            infrastructure_config = PipelineInfrastructureConfig(
                runtime=RuntimeConfig(),
                io=IOConfig(),
                http=http_config,
                cache=CacheConfig(),
                paths=PathsConfig(),
                determinism=determinism_config,
                materialization=MaterializationConfig(root="data/output"),
                logging=LoggingConfig(),
                telemetry=TelemetryConfig(),
                cli=CLIConfig(date_tag="20240101"),
            )

            from bioetl.config.models.fallbacks import FallbacksConfig
            from bioetl.config.models.transform import TransformConfig

            domain_config = PipelineDomainConfig(
                validation=ValidationConfig(
                    schema_out=None, strict=True, coerce=True
                ),
                transform=TransformConfig(),
                postprocess=PostprocessConfig(),
                fallbacks=FallbacksConfig(),
                sources={
                    "chembl": SourceConfig(
                        enabled=True,
                        parameters=SourceParameters.from_mapping(
                            {
                                "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                                "max_url_length": 2000,
                            }
                        ),
                    )
                },
                chembl=None,
            )

            pipeline_metadata = PipelineMetadata(
                name="activity_chembl",
                version="1.0.0",
                description="Test activity pipeline",
            )

            # Используем model_validate для создания конфига
            config = PipelineConfig.model_validate(
                {
                    "version": 1,
                    "pipeline": pipeline_metadata.model_dump(),
                    "domain": domain_config.model_dump(),
                    "infrastructure": infrastructure_config.model_dump(),
                }
            )

            # Если run_id не передан, используем фиксированное значение для тестов.
            effective_run_id = run_id or "test-run"

        super().__init__(
            config, effective_run_id, effective_source, writer=writer
        )
        self.writer = writer

    def extract(
        self,
        *,
        mode: Any = None,
        ids: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Extract records, rejecting entity-specific keyword arguments."""
        # Check for entity-specific keyword arguments that should not be used
        invalid_kwargs = {
            k: v for k, v in kwargs.items() if k.endswith("_ids")
        }
        if invalid_kwargs:
            invalid_key = next(iter(invalid_kwargs))
            msg = (
                f"{type(self).__name__}.extract() does not accept '{invalid_key}' as a keyword argument. "
                "Use --input-file CLI option to provide identifiers for extraction."
            )
            raise TypeError(msg)
        # Call parent extract method
        return super().extract(mode=mode, ids=ids, **kwargs)

    def _enrich(self, records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Выполнить record-oriented обогащение родительского класса."""

        return super()._enrich(records)

    def logger_for(
        self,
        *,
        stage: str | None = None,
        component: str | None = None,
        **extra: Any,
    ) -> BoundLogger:
        """Return a logger bound to the pipeline and stage context."""
        return self._make_pipeline_logger(
            stage=stage, component=component, **extra
        )

    def _log_validity_comments_metrics(
        self,
        df: pd.DataFrame,
        log: BoundLogger | None = None,
    ) -> None:
        """Log metrics for validity comment fields.

        Parameters
        ----------
        df
            DataFrame with comment fields
        log
            Optional logger instance
        """
        from collections import Counter

        base_log = log or UnifiedLogger.get(__name__).bind(
            stage="log_validity_comments_metrics"
        )

        if df.empty:
            return

        comment_fields = [
            "activity_comment",
            "data_validity_comment",
        ]

        # Compute NA rates
        metrics: dict[str, Any] = {}
        total_rows = len(df)

        for field in comment_fields:
            if field not in df.columns:
                continue
            na_count = df[field].isna().sum()
            na_rate = na_count / total_rows if total_rows > 0 else 0.0
            metrics[f"{field}_na_rate"] = na_rate

        # Compute top 10 values for data_validity_comment
        if "data_validity_comment" in df.columns:
            non_na_values = df["data_validity_comment"].dropna().astype(str)
            value_counts = Counter(non_na_values)
            top_10 = dict(value_counts.most_common(10))
            metrics["top_10_data_validity_comments"] = top_10

        # Check for unknown values in data_validity_comment
        if "data_validity_comment" in df.columns:
            try:
                allowed_values = vocab_service.required_vocab_ids(
                    "data_validity_comment", allowed_statuses=("active",)
                )
                non_na_values = (
                    df["data_validity_comment"].dropna().astype(str)
                )
                unknown_values = set(non_na_values) - set(allowed_values)
                if unknown_values:
                    base_log.warning(
                        LogEvents.UNKNOWN_DATA_VALIDITY_COMMENTS_DETECTED,
                        unknown_count=len(unknown_values),
                        unknown_values=list(unknown_values)[
                            :10
                        ],  # Limit to first 10
                    )
            except Exception as exc:  # noqa: BLE001
                # If vocabulary service fails, log and skip unknown value detection
                base_log.debug(
                    "data_validity_comment_vocab_lookup_failed",
                    error=str(exc),
                )

        # Log metrics
        if metrics:
            base_log.info(LogEvents.VALIDITY_COMMENTS_METRICS, **metrics)

    def _validate_data_validity_comment_soft_enum(
        self,
        df: pd.DataFrame,
        log: BoundLogger | None = None,
    ) -> None:
        """Validate data_validity_comment values against vocabulary whitelist (soft enum).

        Parameters
        ----------
        df
            DataFrame with data_validity_comment field
        log
            Optional logger instance

        Raises
        ------
        RuntimeError
            If vocabulary service fails to provide whitelist
        """
        base_log = log or UnifiedLogger.get(__name__).bind(
            stage="validate_data_validity_comment_soft_enum"
        )

        if df.empty or "data_validity_comment" not in df.columns:
            return

        # Get whitelist from vocabulary service (raises if unavailable)
        allowed_values = vocab_service.required_vocab_ids(
            "data_validity_comment", allowed_statuses=("active",)
        )

        # Find unknown values
        non_na_values = df["data_validity_comment"].dropna().astype(str)
        unknown_values = set(non_na_values) - set(allowed_values)

        if unknown_values:
            base_log.warning(
                LogEvents.SOFT_ENUM_UNKNOWN_DATA_VALIDITY_COMMENT,
                unknown_count=len(unknown_values),
                unknown_values=list(unknown_values)[:10],  # Limit to first 10
            )

    def identifier_rules(self) -> Sequence[IdentifierRule]:
        """Return identifier normalization rules for ChEMBL and BAO identifiers."""
        return (
            IdentifierRule(
                columns=(
                    "molecule_chembl_id",
                    "assay_chembl_id",
                    "target_chembl_id",
                ),
                pattern=r"^CHEMBL\d+$",
                uppercase=True,
                strip=True,
                empty_to_null=True,
                name="chembl_id",
            ),
            IdentifierRule(
                columns=("bao_endpoint", "bao_format"),
                pattern=r"^BAO_\d{7}$",
                uppercase=True,
                strip=True,
                empty_to_null=True,
                name="bao_id",
            ),
        )

    def string_rules(self) -> Mapping[str, StringRule]:
        """Return string normalization rules for activity pipeline fields."""
        return {
            "target_organism": StringRule(
                trim=True,
                empty_to_null=True,
                title_case=True,
            ),
            "activity_comment": StringRule(
                trim=True,
                empty_to_null=True,
                lowercase=True,
            ),
            # Default rules for other string fields
            "canonical_smiles": StringRule(trim=True, empty_to_null=True),
            "bao_label": StringRule(trim=True, empty_to_null=True),
            "data_validity_comment": StringRule(trim=True, empty_to_null=True),
            "standard_text_value": StringRule(trim=True, empty_to_null=True),
            "type": StringRule(trim=True, empty_to_null=True),
            "units": StringRule(trim=True, empty_to_null=True),
        }

    def get_normalization_rules(self) -> Mapping[str, Any]:
        """Return normalization rules preserving ChEMBL activity fields.

        The rules are based on API_ACTIVITY_FIELDS and keep all domain
        columns needed by ActivitySchema while retaining the existing
        normalisation behaviour for ``activity_id`` and ``value``.
        """

        field_mapping_spec: dict[str, FieldMappingRule]
        field_mapping_spec = {}

        # Preserve all relevant fields from the ChEMBL activity API payload.
        # ``curated_by`` is intentionally omitted as it is not part of the
        # canonical ActivitySchema output.
        for field in API_ACTIVITY_FIELDS:
            if field == "curated_by":
                continue
            field_mapping_spec[field] = FieldMappingRule(source=field)

        # Backward-compatible aliases for identifiers when working with
        # legacy payloads that may expose *_id instead of *_chembl_id. For
        # test items we additionally fall back to molecule_chembl_id when
        # a dedicated testitem identifier is not available.

        field_mapping_spec["assay_chembl_id"] = FieldMappingRule(
            source="assay_chembl_id",
            aliases=("assay_id",),
            id_type="assay",
        )
        field_mapping_spec["testitem_chembl_id"] = FieldMappingRule(
            source="testitem_chembl_id",
            aliases=("testitem_id", "molecule_chembl_id"),
            id_type="testitem",
        )

        # Preserve numeric normalisation behaviour for identifiers and values.
        field_mapping_spec["activity_id"] = FieldMappingRule(
            source="activity_id",
            value_normalizer=lambda v: int(v) if v is not None else None,
        )
        field_mapping_spec["value"] = FieldMappingRule(
            source="value",
            value_normalizer=lambda v: float(v) if v is not None else None,
        )

        return self.build_normalization_rules_from_spec(field_mapping_spec)

    def _normalize_string_fields(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Normalize string fields and check invariants."""
        return super()._normalize_string_fields(df, log)

    def get_enrichment_rules(self) -> list[Any]:
        def add_flags(record: Mapping[str, Any]) -> Mapping[str, Any]:
            enriched = dict(record)
            enriched["is_active"] = bool(record.get("value"))
            return enriched

        return [add_flags]

    def get_schema(self) -> dict[str, Any]:
        return {
            "activity_id": lambda series: series.notna(),
            "assay_id": lambda series: series.notna(),
        }

    def _normalize_measurements(
        self, df: pd.DataFrame, log: BoundLogger
    ) -> pd.DataFrame:
        """Normalize measurement fields: standard_value, standard_relation, standard_type, standard_units."""
        from bioetl.schemas._validators import RELATIONS

        result = df.copy()

        def _to_numeric(val: Any) -> float | None:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                candidate = val.strip().replace(",", ".") if isinstance(val, str) else val
                num = float(candidate)
                if math.isnan(num):
                    return None
                return num
            except Exception:
                return None

        # Normalize standard_value
        if "standard_value" in result.columns:
            series = result["standard_value"].astype(str).str.strip()
            # Remove commas and spaces
            series = series.str.replace(r"[,\s]", "", regex=True)
            # Extract first numeric value from ranges (e.g., "10-20" -> "10")
            series = series.str.extract(r"([+-]?\d*\.?\d+)", expand=False)
            # Convert to numeric, invalid becomes NaN
            numeric_series = pd.to_numeric(series, errors="coerce")
            # Negative values -> None
            numeric_series = numeric_series.where(numeric_series >= 0, pd.NA)
            result["standard_value"] = numeric_series.astype("float64")

        # Normalize standard_relation and relation
        for col in ("standard_relation", "relation"):
            if col in result.columns:
                series = result[col].astype(str).str.strip()
                # Normalize Unicode symbols to ASCII
                series = series.str.replace("≤", "<=", regex=False)
                series = series.str.replace("≥", ">=", regex=False)
                series = series.str.replace("≠", "~", regex=False)
                # Validate against RELATIONS whitelist
                valid_mask = series.isin(RELATIONS)
                series = series.where(valid_mask, pd.NA)
                result[col] = series.astype("string")

        # Normalize bounds strictly: populate lower/upper only when relation is "=".
        def _compute_bounds(row: pd.Series[Any]) -> float | pd.NA:
            rel_raw = row.get("relation")
            rel = None
            if rel_raw is not None and not (
                isinstance(rel_raw, float) and pd.isna(rel_raw)
            ):
                rel = str(rel_raw).strip()

            std_val = row.get("standard_value")
            val_candidate = (
                std_val
                if std_val is not None
                and not (isinstance(std_val, float) and pd.isna(std_val))
                else row.get("value")
            )

            num = _to_numeric(val_candidate)
            if rel == "=" and num is not None:
                return num
            return pd.NA

        bounds_series = result.apply(_compute_bounds, axis=1)
        bounds_numeric = pd.to_numeric(bounds_series, errors="coerce")
        result["lower_value"] = bounds_numeric
        result["upper_value"] = bounds_numeric

        def _compute_standard_upper(row: pd.Series[Any]) -> float | pd.NA:
            rel_raw = row.get("standard_relation")
            rel = None
            if rel_raw is not None and not (
                isinstance(rel_raw, float) and pd.isna(rel_raw)
            ):
                rel = str(rel_raw).strip()

            num = _to_numeric(row.get("standard_value"))
            if rel == "=" and num is not None:
                return num
            return pd.NA

        if "standard_relation" in result.columns or "standard_value" in result.columns:
            std_upper_series = result.apply(_compute_standard_upper, axis=1)
            result["standard_upper_value"] = pd.to_numeric(
                std_upper_series, errors="coerce"
            )

        # Normalize standard_type
        if "standard_type" in result.columns:
            # Only "IC50" is active according to the dictionary
            # Other types like "EC50" are deprecated
            allowed_types = {"IC50", "Ki"}  # Only active types from dictionary
            series = result["standard_type"].astype(str).str.strip()
            valid_mask = series.isin(allowed_types)
            series = series.where(valid_mask, pd.NA)
            result["standard_type"] = series.astype("string")

        # Normalize standard_units
        if "standard_units" in result.columns:
            original = result["standard_units"].astype(str).str.strip()
            series_lower = original.str.lower()
            # Normalize synonyms
            unit_mapping = {
                "nanomolar": "nM",
                "nmol": "nM",
                "nm": "nM",
                "micromolar": "μM",
                "microm": "μM",
                "umol": "μM",
                "um": "μM",
                "millimolar": "mM",
                "millim": "mM",
                "mmol": "mM",
                "mm": "mM",
                "percent": "%",
                "pct": "%",
            }
            # Start with original values
            normalized = original.copy()
            # Apply mapping for synonyms (case-insensitive)
            for synonym, canonical in unit_mapping.items():
                mask = series_lower == synonym
                normalized = normalized.where(~mask, canonical)
            # Handle uM -> μM conversion
            normalized = normalized.str.replace("uM", "μM", regex=False)
            normalized = normalized.str.replace("UM", "μM", regex=False)
            # Preserve canonical values that are already correct (nM, μM, mM, %)
            canonical_values = {"nM", "μM", "mM", "%"}
            mask_canonical = original.isin(canonical_values)
            normalized = normalized.where(
                ~mask_canonical, original.where(mask_canonical)
            )
            result["standard_units"] = normalized.astype("string")

        return result

    def _normalize_activity_properties_items(
        self, payload: Any, log: BoundLogger | None = None
    ) -> list[dict[str, Any]] | None:
        """Normalize activity_properties items from various input formats.

        Parameters
        ----------
        payload
            Input can be:
            - List of dicts: [{"type": "IC50", "value": 10.5, ...}]
            - JSON string: '{"type": "Ki", "value": 5.0}'
            - Invalid JSON string: treated as text_value
            - Other types: returns None and logs warning
        log
            Optional logger for warnings

        Returns
        -------
        list[dict[str, Any]] | None
            Normalized list of activity property items, or None if unhandled type
        """
        import json

        base_log = log or UnifiedLogger.get(__name__)

        # Handle list of dicts
        if isinstance(payload, list):
            normalized: list[dict[str, Any]] = []
            for item in payload:
                if not isinstance(item, dict):
                    continue
                # Filter to allowed keys only
                normalized_item: dict[str, Any] = {}
                for key in ACTIVITY_PROPERTY_KEYS:
                    value = item.get(key)
                    # Normalize result_flag: int (0/1) -> bool
                    if key == "result_flag" and isinstance(value, int):
                        value = bool(value)
                    normalized_item[key] = value
                normalized.append(normalized_item)
            return normalized

        # Handle JSON string
        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
                # If parsed is a dict, wrap in list
                if isinstance(parsed, dict):
                    parsed = [parsed]
                # Recursively process
                if isinstance(parsed, list):
                    return self._normalize_activity_properties_items(
                        parsed, log
                    )
            except (json.JSONDecodeError, TypeError):
                # Invalid JSON - treat as text_value
                text_item: dict[str, Any] = {}
                for key in ACTIVITY_PROPERTY_KEYS:
                    if key == "text_value":
                        text_item[key] = payload
                    else:
                        text_item[key] = None
                return [text_item]

        # Unhandled type
        base_log.warning(
            LogEvents.ACTIVITY_PROPERTIES_UNHANDLED_TYPE,
            payload_type=type(payload).__name__,
        )
        return None

    def _serialize_activity_properties(
        self, payload: list[dict[str, Any]] | None
    ) -> str | None:
        """Serialize normalized activity_properties to canonical JSON string.

        Parameters
        ----------
        payload
            Normalized list of activity property items

        Returns
        -------
        str | None
            Canonical JSON string, or None if payload is None
        """
        import json

        if payload is None:
            return None

        try:
            return json.dumps(payload, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return None

    def _normalize_data_types(
        self, df: pd.DataFrame, schema: Any, log: BoundLogger
    ) -> pd.DataFrame:
        """Convert data types according to the schema for activity pipeline."""
        import pandera as pa

        result = df.copy()

        if schema is None:
            return result

        def _to_numeric_series(series: pd.Series[Any]) -> pd.Series[Any]:
            return pd.to_numeric(series, errors="coerce")

        # Get column definitions from schema
        for column_name, column_def in schema.columns.items():
            if column_name not in result.columns:
                continue

            try:
                column_series = result[column_name]

                # Handle Int64 (nullable and non-nullable)
                # Check for pd.Int64Dtype(), pa.Int64, or string "int64"/"Int64"
                dtype_name = (
                    getattr(column_def.dtype, "name", None)
                    if hasattr(column_def.dtype, "name")
                    else None
                )
                dtype_str = str(column_def.dtype).lower()
                dtype_type_name = type(column_def.dtype).__name__
                if (
                    column_def.dtype == pd.Int64Dtype()
                    or isinstance(column_def.dtype, type(pa.Int64))
                    or dtype_name == "Int64"
                    or dtype_str == "int64"
                    or dtype_type_name == "Int64"
                ):
                    # Convert to numeric first, then to Int64
                    numeric_series = pd.to_numeric(
                        column_series, errors="coerce"
                    )
                    result[column_name] = numeric_series.astype("Int64")
                # Handle Float64/float64
                elif (
                    isinstance(column_def.dtype, type(pa.Float64))
                    or dtype_name in ("Float64", "float64")
                    or dtype_str == "float64"
                    or dtype_type_name == "Float64"
                ):
                    numeric_series = _to_numeric_series(column_series)
                    result[column_name] = numeric_series.astype("float64")
                # Handle boolean flags
                elif (
                    column_def.dtype == pd.BooleanDtype()
                    or isinstance(column_def.dtype, type(pa.Bool))
                    or dtype_str == "boolean"
                    or dtype_type_name == "BOOL"
                ):
                    # Convert to boolean, handling int (0/1) and string ("0"/"1") inputs
                    mask = column_series.notna()
                    if mask.any():
                        # Convert numeric/string to boolean: 0/False -> False, 1/True -> True
                        numeric_values = pd.to_numeric(
                            column_series, errors="coerce"
                        )
                        result[column_name] = numeric_values.astype("boolean")
                    else:
                        result[column_name] = column_series.astype("boolean")
                # Handle strings
                elif (
                    (
                        hasattr(column_def.dtype, "name")
                        and column_def.dtype.name == "string"
                    )
                    or column_def.dtype == pa.String
                    or "string" in dtype_str
                    or "str" in dtype_str
                ):
                    mask = column_series.notna()
                    if mask.any():
                        result.loc[mask, column_name] = result.loc[
                            mask, column_name
                        ].astype(str)
            except (ValueError, TypeError) as exc:
                log.warning(
                    LogEvents.TYPE_CONVERSION_FAILED,
                    field=column_name,
                    error=str(exc),
                )

        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw DataFrame by applying normalization stages."""
        if df.empty:
            return df

        log = self.logger_for(stage="transform")

        # Harmonize identifier column aliases
        working_df = df.copy()
        # Rename assay_id -> assay_chembl_id
        if "assay_id" in working_df.columns:
            if "assay_chembl_id" in working_df.columns:
                # Both exist: combine values, preferring assay_chembl_id
                working_df["assay_chembl_id"] = working_df[
                    "assay_chembl_id"
                ].combine_first(working_df["assay_id"])
                # Drop old column
                working_df = working_df.drop(columns=["assay_id"])
            else:
                # Only assay_id exists: rename it
                working_df = working_df.rename(
                    columns={"assay_id": "assay_chembl_id"}
                )
        # Rename testitem_id -> testitem_chembl_id
        if "testitem_id" in working_df.columns:
            if "testitem_chembl_id" in working_df.columns:
                # Both exist: combine values, preferring testitem_chembl_id
                working_df["testitem_chembl_id"] = working_df[
                    "testitem_chembl_id"
                ].combine_first(working_df["testitem_id"])
                # Drop old column
                working_df = working_df.drop(columns=["testitem_id"])
            else:
                # Only testitem_id exists: rename it
                working_df = working_df.rename(
                    columns={"testitem_id": "testitem_chembl_id"}
                )

        # Convert DataFrame to records for processing
        records: list[dict[str, Any]] = [
            {str(k): v for k, v in record.items()}
            for record in working_df.to_dict("records")
        ]
        normalized = self._normalize(records)
        normalized_df = pd.DataFrame(normalized)

        normalized_df = self._normalize_measurements(normalized_df, log)

        # Normalize data types according to the Activity schema so that
        # Pandera receives columns with dtypes as close as possible to
        # the final contract.
        schema_descriptor = get_schema(
            "bioetl.schemas.chembl_activity_schema.ActivitySchema"
        )
        normalized_df = self._normalize_data_types(
            normalized_df, schema_descriptor.schema, log
        )

        # Ensure standard row metadata columns exist for determinism and hashing
        normalized_df, _ = add_row_metadata(
            normalized_df,
            subtype=self.pipeline_code,
            copy=False,
        )

        return normalized_df

    # Delegate to unified save_results implementation
    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> Any:
        # Convert None to False for IOArtifactsMixin compatibility
        include_correlation_val: bool = (
            include_correlation if include_correlation is not None else False
        )
        include_qc_metrics_val: bool = (
            include_qc_metrics if include_qc_metrics is not None else False
        )
        return super().save_results(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation_val,
            include_qc_metrics=include_qc_metrics_val,
        )


__all__ = ["ChemblActivityPipeline"]
