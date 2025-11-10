"""TestItem pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, ClassVar, cast, no_type_check

import pandas as pd
from pandas import Series
from structlog.stdlib import BoundLogger

from bioetl.clients.chembl import ChemblClient
from bioetl.clients.testitem.chembl_testitem import ChemblTestitemClient
from bioetl.clients.types import EntityClient
from bioetl.config import PipelineConfig, TestItemSourceConfig
from bioetl.config.models.models import SourceConfig
from bioetl.config.pipeline_source import ChemblPipelineSourceConfig
from bioetl.config.testitem import TestItemSourceParameters
from bioetl.core import UnifiedLogger
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.normalizers import StringRule, StringStats, normalize_string_columns
from bioetl.schemas.testitem import COLUMN_ORDER

from ..chembl_base import ChemblPipelineBase
from .testitem_transform import transform as transform_testitem

# Обязательные поля, которые всегда должны быть в запросе к API
MUST_HAVE_FIELDS: tuple[str, ...] = (
    # Скаляры
    "molecule_chembl_id",
    "pref_name",
    "molecule_type",
    "availability_type",
    "chirality",
    "first_approval",
    "first_in_class",
    "indication_class",
    "helm_notation",
    # Вложенные корни, чтобы сервер вернул объекты
    "molecule_properties",
    "molecule_structures",
    "molecule_hierarchy",
)


@no_type_check
def _coerce_numeric_series(series: pd.Series) -> Series:
    """Convert arbitrary series to float series with NaNs on errors."""

    return cast(Series, pd.to_numeric(series, errors="coerce"))


@no_type_check
def _ensure_iterable_records(
    fetched: Mapping[str, object] | Iterable[Mapping[str, object]]
) -> Iterable[Mapping[str, object]]:
    """Normalize fetch() result to an iterable of records."""

    if isinstance(fetched, Mapping):
        return (fetched,)
    return fetched


@no_type_check
def _series_isin(series: Series, values: Iterable[object]) -> Series:
    """Wrapper over Series.isin for static type checkers."""

    return cast(Series, series.isin(values))


class TestItemChemblPipeline(ChemblPipelineBase):
    """ETL pipeline extracting molecule records from the ChEMBL API."""

    ACTOR: ClassVar[str] = "testitem_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._chembl_db_version: str | None = None
        self._api_version: str | None = None

    @property
    def chembl_db_version(self) -> str | None:
        """Return the cached ChEMBL DB version captured during extraction."""
        return self._chembl_db_version

    @property
    def api_version(self) -> str | None:
        """Return the cached ChEMBL API version captured during extraction."""
        return self._api_version

    def _fetch_chembl_release(
        self,
        client: UnifiedAPIClient | ChemblClient | Any,  # noqa: ANN401
        log: BoundLogger | None = None,
        *,
        source_config: ChemblPipelineSourceConfig[Any] | SourceConfig | None = None,
    ) -> str | None:
        """Выполнить handshake и закешировать версии ChEMBL/API для пайплайна."""

        if log is None:
            bound_log: BoundLogger = UnifiedLogger.get(__name__).bind(
                component=f"{self.pipeline_code}.extract"
            )
        else:
            bound_log = log

        resolved_source_config: ChemblPipelineSourceConfig[TestItemSourceParameters]

        if source_config is None:
            source_raw = self._resolve_source_config("chembl")
            resolved_source_config = TestItemSourceConfig.from_source(source_raw)
        elif isinstance(source_config, TestItemSourceConfig):
            resolved_source_config = source_config
        elif isinstance(source_config, ChemblPipelineSourceConfig):
            resolved_source_config = cast(
                ChemblPipelineSourceConfig[TestItemSourceParameters],
                source_config,
            )
        else:
            resolved_source_config = TestItemSourceConfig.from_source(source_config)

        handshake_result = self.perform_source_handshake(
            client,
            source_config=resolved_source_config,
            log=bound_log,
            event="chembl_testitem.handshake",
        )
        payload: Mapping[str, Any] = handshake_result.payload

        release_value = handshake_result.release
        api_version: str | None = None
        api_candidate = payload.get("api_version")
        if isinstance(api_candidate, str) and api_candidate.strip():
            api_version = api_candidate
        if release_value is None:
            extracted_release = self._extract_chembl_release(payload)
            if extracted_release:
                release_value = extracted_release
                self._update_release(extracted_release)

        self._chembl_db_version = release_value
        self._api_version = api_version
        self.record_extract_metadata(
            chembl_release=release_value,
            requested_at_utc=handshake_result.requested_at_utc,
        )

        return release_value


    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch molecule payloads from ChEMBL using the unified HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))

        # Check for input file and extract IDs if present
        if self.config.cli.input_file:
            id_column_name = self._get_id_column_name()
            ids = self._read_input_ids(
                id_column_name=id_column_name,
                limit=self.config.cli.limit,
                sample=self.config.cli.sample,
            )
            if ids:
                log.info("chembl_testitem.extract_mode", mode="batch", ids_count=len(ids))
                return self.extract_by_ids(ids)

        log.info("chembl_testitem.extract_mode", mode="full")
        return self.extract_all()

    def extract_all(self) -> pd.DataFrame:
        """Extract all molecule records from ChEMBL using pagination."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = TestItemSourceConfig.from_source(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client(
            "chembl", base_url=base_url, client_name="chembl_testitem_http"
        )

        chembl_client = ChemblClient(
            http_client,
            load_meta_store=self.load_meta_store,
            job_id=self.run_id,
            operator=self.pipeline_code,
        )
        self._fetch_chembl_release(
            chembl_client,
            source_config=source_config,
            log=log,
        )

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_testitem.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_db_version=self._chembl_db_version,
                api_version=self._api_version,
            )
            return pd.DataFrame()

        limit = self.config.cli.limit
        page_size = self._resolve_page_size(source_config.page_size, limit)
        select_fields = self._resolve_select_fields(
            source_config,
            required_fields=MUST_HAVE_FIELDS,
            preserve_none=True,
        )
        log.debug("chembl_testitem.select_fields", fields=select_fields)
        records: list[Mapping[str, object]] = []

        filters_payload: dict[str, Any] = {
            "mode": "all",
            "limit": int(limit) if limit is not None else None,
            "page_size": page_size,
            "select_fields": list(select_fields) if select_fields else None,
            "max_url_length": source_config.max_url_length,
        }
        compact_filters = {
            key: value for key, value in filters_payload.items() if value is not None
        }
        self.record_extract_metadata(
            chembl_release=self.chembl_release,
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )

        # Используем специализированный клиент для testitem (molecule)
        testitem_client: EntityClient[Mapping[str, object]] = ChemblTestitemClient(
            chembl_client,
            batch_size=page_size,
            max_url_length=source_config.max_url_length,
        )
        for item in testitem_client.iter(
            limit=limit,
            page_size=page_size,
            select_fields=select_fields,
        ):
            records.append(item)

        dataframe = pd.DataFrame(records)
        if dataframe.empty:
            dataframe = pd.DataFrame({"molecule_chembl_id": pd.Series(dtype="string")})
        elif "molecule_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("molecule_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_testitem.extract_summary",
            rows=int(dataframe.shape[0]),
            duration_ms=duration_ms,
            chembl_db_version=self._chembl_db_version,
            api_version=self._api_version,
            limit=limit,
        )
        return dataframe

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract molecule records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of molecule_chembl_id values to extract.

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted molecule records.
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = TestItemSourceConfig.from_source(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client(
            "chembl", base_url=base_url, client_name="chembl_testitem_http"
        )

        chembl_client = ChemblClient(
            http_client,
            load_meta_store=self.load_meta_store,
            job_id=self.run_id,
            operator=self.pipeline_code,
        )
        self._fetch_chembl_release(
            chembl_client,
            source_config=source_config,
            log=log,
        )

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_testitem.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_db_version=self._chembl_db_version,
                api_version=self._api_version,
            )
            return pd.DataFrame()

        page_size = source_config.page_size
        limit = self.config.cli.limit
        select_fields = self._resolve_select_fields(
            source_config,
            required_fields=MUST_HAVE_FIELDS,
            preserve_none=True,
        )
        log.debug("chembl_testitem.select_fields", fields=select_fields)

        filters_payload = {
            "mode": "ids",
            "requested_ids": [str(identifier) for identifier in ids],
            "limit": int(limit) if limit is not None else None,
            "page_size": page_size,
            "max_url_length": source_config.max_url_length,
            "select_fields": list(select_fields) if select_fields else None,
        }
        compact_filters = {
            key: value for key, value in filters_payload.items() if value is not None
        }
        self.record_extract_metadata(
            chembl_release=self.chembl_release,
            filters=compact_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )

        records: list[Mapping[str, object]] = []
        testitem_client: EntityClient[Mapping[str, object]] = ChemblTestitemClient(
            chembl_client,
            batch_size=page_size,
            max_url_length=source_config.max_url_length,
        )
        fetched_records = testitem_client.fetch(ids, select_fields=select_fields)
        record_iterable = _ensure_iterable_records(fetched_records)
        for item in record_iterable:
            records.append(item)
            if limit is not None and len(records) >= limit:
                break

        dataframe = pd.DataFrame(records)
        if dataframe.empty:
            dataframe = pd.DataFrame({"molecule_chembl_id": pd.Series(dtype="string")})
        elif "molecule_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("molecule_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_testitem.extract_by_ids_summary",
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_db_version=self._chembl_db_version,
            api_version=self._api_version,
            limit=limit,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw molecule data by normalizing fields and extracting nested properties."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        # According to documentation, transform should accept df: pd.DataFrame
        # df is already a pd.DataFrame, so we can use it directly
        df = df.copy()

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        # Apply transform module: flatten nested objects and serialize arrays
        df = transform_testitem(df, self.config)

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Ensure all schema columns exist with proper types
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        # Normalize numeric fields (type coercion, negative values -> None)
        df = self._normalize_numeric_fields(df, log)

        # Check for empty columns and warn if too many are empty
        self._check_empty_columns(df, log)

        # Remove columns not in schema
        df = self._remove_extra_columns(df, log)

        # Add version fields
        df["_chembl_db_version"] = self._chembl_db_version or ""
        df["_api_version"] = self._api_version or ""

        # Deduplication
        df = self._deduplicate_molecules(df, log)

        # Sort by molecule_chembl_id for determinism
        if "molecule_chembl_id" in df.columns:
            df = df.sort_values("molecule_chembl_id").reset_index(drop=True)

        # Reorder columns according to schema COLUMN_ORDER
        df = self._order_schema_columns(df, COLUMN_ORDER)

        log.info("transform_completed", rows=len(df))
        return df

    def augment_metadata(
        self,
        metadata: Mapping[str, object],
        df: pd.DataFrame,
    ) -> Mapping[str, object]:
        """Enrich metadata with ChEMBL versions."""

        enriched = dict(super().augment_metadata(metadata, df))
        if self._chembl_db_version:
            enriched["chembl_db_version"] = self._chembl_db_version
        if self._api_version:
            enriched["api_version"] = self._api_version
        return enriched

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flatten_nested_structures(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Flatten nested molecule_structures and molecule_properties into flat columns."""

        if df.empty:
            return df

        # Flatten molecule_structures
        if "molecule_structures" in df.columns:
            structures_df = pd.json_normalize(  # pyright: ignore[reportUnknownMemberType]
                df["molecule_structures"].tolist(),
            )
            if "canonical_smiles" in structures_df.columns:
                df["canonical_smiles"] = structures_df["canonical_smiles"]
            if "standard_inchi_key" in structures_df.columns:
                df["standard_inchi_key"] = structures_df["standard_inchi_key"]

        # Flatten molecule_properties
        if "molecule_properties" in df.columns:
            properties_df = pd.json_normalize(  # pyright: ignore[reportUnknownMemberType]
                df["molecule_properties"].tolist(),
            )
            property_columns = [
                "full_mwt",
                "mw_freebase",
                "alogp",
                "hbd",
                "hba",
                "psa",
                "aromatic_rings",
                "rtb",
                "num_ro5_violations",
            ]
            for col in property_columns:
                if col in properties_df.columns:
                    df[col] = properties_df[col]

        log.debug("flatten_nested_structures_completed", columns=list(df.columns))
        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL identifiers and InChI keys."""

        if df.empty:
            return df

        working_df = df.copy()

        inchi_key_col = (
            "molecule_structures__standard_inchi_key"
            if "molecule_structures__standard_inchi_key" in working_df.columns
            else "standard_inchi_key"
        )

        rules: dict[str, StringRule] = {}
        if "molecule_chembl_id" in working_df.columns:
            rules["molecule_chembl_id"] = StringRule()
        if inchi_key_col in working_df.columns:
            rules[inchi_key_col] = StringRule(uppercase=True)

        if rules:
            normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)
        else:
            normalized_df = working_df
            stats = StringStats()

        if stats.has_changes:
            log.debug(
                "normalize_identifiers_completed",
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )
        else:
            log.debug("normalize_identifiers_completed")

        return normalized_df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields: trim, replace empty strings with NaN."""

        if df.empty:
            return df

        working_df = df.copy()

        rules = {
            "pref_name": StringRule(),
            "molecule_type": StringRule(),
            "molecule_structures__canonical_smiles": StringRule(),
            "canonical_smiles": StringRule(),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(
                "normalize_string_fields_completed",
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )
        else:
            log.debug("normalize_string_fields_completed")

        return normalized_df

    def _schema_column_specs(self) -> Mapping[str, Mapping[str, Any]]:
        specs = dict(super()._schema_column_specs())

        int_columns = [
            "max_phase",
            "first_approval",
            "first_in_class",
            "availability_type",
            "black_box_warning",
            "chirality",
            "dosed_ingredient",
            "inorganic_flag",
            "natural_product",
            "prodrug",
            "therapeutic_flag",
            "molecule_properties__aromatic_rings",
            "molecule_properties__hba",
            "molecule_properties__hba_lipinski",
            "molecule_properties__hbd",
            "molecule_properties__hbd_lipinski",
            "molecule_properties__heavy_atoms",
            "molecule_properties__num_lipinski_ro5_violations",
            "molecule_properties__num_ro5_violations",
            "molecule_properties__ro3_pass",
            "molecule_properties__rtb",
        ]

        float_columns = [
            "molecule_properties__alogp",
            "molecule_properties__cx_logd",
            "molecule_properties__cx_logp",
            "molecule_properties__cx_most_apka",
            "molecule_properties__cx_most_bpka",
            "molecule_properties__full_mwt",
            "molecule_properties__mw_freebase",
            "molecule_properties__mw_monoisotopic",
            "molecule_properties__psa",
            "molecule_properties__qed_weighted",
        ]

        for column in int_columns:
            specs[column] = {"dtype": "Int64", "default": pd.NA}
        for column in float_columns:
            specs[column] = {"dtype": "Float64", "default": pd.NA}

        for column in COLUMN_ORDER:
            if column not in specs:
                specs[column] = {"dtype": "string", "default": pd.NA}

        return specs

    def _normalize_numeric_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize numeric fields: convert types, replace negative values with None."""

        if df.empty:
            return df

        # Integer columns that should be >= 0
        int_ge_zero_columns = [
            "chirality",
            "availability_type",
            "hbd",
            "hba",
            "aromatic_rings",
            "rtb",
            "num_ro5_violations",
        ]

        # Integer columns that can have specific ranges
        int_columns = [
            "max_phase",
            "first_approval",
            "black_box_warning",
        ]

        # Boolean-like columns (0/1/-1) - convert -1 to None
        boolean_columns = [
            "first_in_class",
            "inorganic_flag",
            "natural_product",
            "prodrug",
            "therapeutic_flag",
            "dosed_ingredient",
        ]

        # Convert all integer columns, handling None/NaN properly
        for col in int_ge_zero_columns + int_columns + boolean_columns:
            if col in df.columns:
                # Convert to numeric, coercing errors and None to NaN
                numeric_series = _coerce_numeric_series(df[col])
                # For columns that should be >= 0, replace negative values with NaN
                if col in int_ge_zero_columns:
                    # Replace negative values with NaN
                    numeric_series = numeric_series.where(numeric_series >= 0)
                # For boolean-like columns, replace -1 with NaN (unknown)
                elif col in boolean_columns:
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    # Also ensure values are only 0 or 1
                    numeric_series = numeric_series.where(
                        (numeric_series == 0) | (numeric_series == 1) | numeric_series.isna()
                    )
                # Convert to nullable Int64 type
                # This will automatically convert NaN to pd.NA for nullable integers
                df[col] = numeric_series.astype("Int64")

        # Handle molecule_properties__ prefixed columns
        for col in df.columns:
            if col.startswith("molecule_properties__"):
                prop_name = col.replace("molecule_properties__", "")
                # Special handling for ro3_pass (Y/N to 1/0)
                if prop_name == "ro3_pass":
                    # Convert Y/N to 1/0
                    mask = df[col].notna()
                    if mask.any():
                        # Convert to string and uppercase for comparison
                        col_str_series = df.loc[mask, col].astype(str)
                        col_str: Series[str] = col_str_series.str.upper().str.strip()
                        # Create new series with converted values
                        converted = df[col].copy()
                        # Map Y to 1, N to 0
                        converted.loc[mask & (col_str == "Y")] = 1
                        converted.loc[mask & (col_str == "N")] = 0
                        # For values that are not Y/N, try numeric conversion
                        other_mask = mask & ~_series_isin(col_str, ["Y", "N"])
                        if other_mask.any():
                            # Try to convert existing numeric values
                            numeric_vals: Series[float] = _coerce_numeric_series(
                                df.loc[other_mask, col]
                            )
                            # Keep only valid numeric values (0 or 1)
                            valid_numeric: Series[bool] = numeric_vals.notna() & _series_isin(
                                numeric_vals, [0, 1]
                            )
                            # Set all other_mask values to NA first
                            converted.loc[other_mask] = pd.NA
                            # Then restore valid numeric values
                            if valid_numeric.any():
                                valid_indices = numeric_vals.index[valid_numeric]
                                converted.loc[valid_indices] = numeric_vals.loc[valid_indices]
                        df[col] = converted
                    # Convert to Int64, ensuring only 0 or 1
                    numeric_series = _coerce_numeric_series(df[col])
                    numeric_series = numeric_series.where(
                        (numeric_series == 0) | (numeric_series == 1) | numeric_series.isna()
                    )
                    df[col] = numeric_series.astype("Int64")
                # Handle other numeric properties
                elif prop_name in [
                    "aromatic_rings",
                    "hba",
                    "hba_lipinski",
                    "hbd",
                    "hbd_lipinski",
                    "heavy_atoms",
                    "num_lipinski_ro5_violations",
                    "num_ro5_violations",
                    "rtb",
                ]:
                    numeric_series = _coerce_numeric_series(df[col])
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    df[col] = numeric_series.astype("Int64")

        log.debug("normalize_numeric_fields_completed")
        return df

    def _check_empty_columns(self, df: pd.DataFrame, log: Any) -> None:
        """Проверка пустых колонок после трансформации.

        Логирует предупреждение, если процент пустых значений по ключевым полям > 95%.
        Это может указывать на проблему с select_fields или flatten_objects.

        Parameters
        ----------
        df:
            DataFrame после трансформации.
        log:
            UnifiedLogger для логирования.
        """
        if df.empty:
            return

        # Ключевые поля для проверки (скаляры и расплющенные из molecule_properties)
        key_fields = [
            "molecule_chembl_id",
            "pref_name",
            "molecule_type",
            "availability_type",
            "chirality",
            "first_approval",
            "first_in_class",
            "indication_class",
            "helm_notation",
            "molecule_properties__cx_logp",
            "molecule_properties__cx_logd",
            "molecule_properties__mw_monoisotopic",
            "molecule_properties__hba_lipinski",
            "molecule_properties__hbd_lipinski",
            "molecule_properties__molecular_species",
            "molecule_properties__num_lipinski_ro5_violations",
        ]

        # Проверяем только те поля, которые есть в DataFrame
        available_key_fields = [col for col in key_fields if col in df.columns]
        if not available_key_fields:
            return

        # Вычисляем процент пустых значений для каждого ключевого поля
        empty_percentages: dict[str, float] = {}
        for col in available_key_fields:
            if len(df) > 0:
                empty_count = df[col].isna().sum()
                empty_percentage = (empty_count / len(df)) * 100.0
                empty_percentages[col] = empty_percentage

        # Находим поля с > 95% пустых значений
        highly_empty_fields = {col: pct for col, pct in empty_percentages.items() if pct > 95.0}

        if highly_empty_fields:
            log.warning(
                "highly_empty_columns_detected",
                empty_fields=highly_empty_fields,
                message=(
                    f"Fields with >95% empty values detected: {highly_empty_fields}. "
                    "This may indicate missing fields in select_fields or flatten_objects configuration."
                ),
            )

    def _remove_extra_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Remove columns that are not in the schema."""

        if df.empty:
            return df

        from bioetl.schemas.testitem import COLUMN_ORDER

        schema_columns = set(COLUMN_ORDER)
        existing_columns = set(df.columns)
        extra_columns = existing_columns - schema_columns

        if extra_columns:
            df = df.drop(columns=list(extra_columns))
            log.debug(
                "remove_extra_columns_completed",
                removed_columns=list(extra_columns),
                remaining_columns=list(df.columns),
            )

        return df

    def _deduplicate_molecules(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Deduplicate molecules by standard_inchi_key (fallback to molecule_chembl_id)."""

        if df.empty:
            return df

        rows_before = len(df)

        # Check for standard_inchi_key in both old and new column names
        inchi_key_col = (
            "molecule_structures__standard_inchi_key"
            if "molecule_structures__standard_inchi_key" in df.columns
            else "standard_inchi_key"
        )
        canonical_smiles_col = (
            "molecule_structures__canonical_smiles"
            if "molecule_structures__canonical_smiles" in df.columns
            else "canonical_smiles"
        )
        full_mwt_col = (
            "molecule_properties__full_mwt"
            if "molecule_properties__full_mwt" in df.columns
            else "full_mwt"
        )
        alogp_col = (
            "molecule_properties__alogp" if "molecule_properties__alogp" in df.columns else "alogp"
        )

        # Prioritize records with non-empty canonical_smiles and more complete properties
        if inchi_key_col in df.columns:
            # Sort by completeness (more complete first)
            completeness_cols = [canonical_smiles_col, full_mwt_col, alogp_col]
            available_completeness = [col for col in completeness_cols if col in df.columns]
            if available_completeness:
                # Count non-null values in completeness columns
                df["_completeness"] = df[available_completeness].notna().sum(axis=1)
                df = df.sort_values(
                    ["_completeness", canonical_smiles_col],
                    ascending=[False, False],
                    na_position="last",
                )
                df = df.drop(columns=["_completeness"])

            # Deduplicate by standard_inchi_key, keeping first (most complete)
            df = df.drop_duplicates(subset=[inchi_key_col], keep="first")
        else:
            # Fallback: deduplicate by molecule_chembl_id
            df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

        rows_after = len(df)
        dropped = rows_before - rows_after
        if dropped > 0:
            log.info(
                "deduplicate_molecules_completed",
                rows_before=rows_before,
                rows_after=rows_after,
                dropped=dropped,
            )

        return df
