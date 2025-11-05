"""TestItem pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd

from bioetl.clients.chembl import ChemblClient
from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger

from ..base import PipelineBase
from .testitem_transform import transform as transform_testitem


class TestItemChemblPipeline(PipelineBase):
    """ETL pipeline extracting molecule records from the ChEMBL API."""

    actor = "testitem_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._client_factory = APIClientFactory(config)
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

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

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

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_testitem_http", http_client)

        chembl_client = ChemblClient(http_client)
        self._chembl_db_version = self._fetch_chembl_release(chembl_client, log)

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

        page_size = self._resolve_page_size(source_config)
        limit = self.config.cli.limit
        select_fields = self._resolve_select_fields(source_config)
        records: list[Mapping[str, Any]] = []

        params: Mapping[str, Any] | None = None
        if select_fields:
            params = {
                "limit": page_size,
                "only": ",".join(select_fields),
            }

        for item in chembl_client.paginate(
            "/molecule.json",
            params=params,
            page_size=page_size,
            items_key="molecules",
        ):
            records.append(item)
            if limit is not None and len(records) >= limit:
                break

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # type: ignore[arg-type]
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

        source_config = self._resolve_source_config("chembl")
        base_url = self._resolve_base_url(source_config.parameters)
        http_client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_testitem_http", http_client)

        chembl_client = ChemblClient(http_client)
        self._chembl_db_version = self._fetch_chembl_release(chembl_client, log)

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

        batch_size = self._resolve_page_size(source_config)
        # Ensure batch_size <= 25 for ChEMBL API URL length limit
        batch_size = min(batch_size, 25)
        limit = self.config.cli.limit
        select_fields = self._resolve_select_fields(source_config)

        records: list[Mapping[str, Any]] = []
        ids_list = list(ids)

        # Process IDs in chunks to avoid URL length limits
        chunk_size = min(batch_size, 25)  # Conservative limit for molecule_chembl_id__in
        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "molecule_chembl_id__in": ",".join(chunk),
                "limit": min(batch_size, 25),
            }
            if select_fields:
                params["only"] = ",".join(select_fields)

            try:
                for item in chembl_client.paginate(
                    "/molecule.json",
                    params=params,
                    page_size=min(batch_size, 25),
                    items_key="molecules",
                ):
                    records.append(item)
                    if limit is not None and len(records) >= limit:
                        break
            except Exception as exc:
                log.warning(
                    "chembl_testitem.fetch_error",
                    molecule_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

            if limit is not None and len(records) >= limit:
                break

        dataframe: pd.DataFrame = pd.DataFrame.from_records(records)  # type: ignore[arg-type]
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
        df = self._ensure_schema_columns(df, log)

        # Normalize numeric fields (type coercion, negative values -> None)
        df = self._normalize_numeric_fields(df, log)

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
        from bioetl.schemas.testitem_chembl import COLUMN_ORDER

        ordered = list(COLUMN_ORDER)
        existing = [col for col in ordered if col in df.columns]
        extras = [col for col in df.columns if col not in ordered]
        if existing:
            df = df[[*existing, *extras]]

        log.info("transform_completed", rows=len(df))
        return df

    def augment_metadata(
        self,
        metadata: Mapping[str, object],
        df: pd.DataFrame,
    ) -> Mapping[str, object]:
        """Enrich metadata with ChEMBL versions."""

        enriched = dict(metadata)
        if self._chembl_db_version:
            enriched["chembl_db_version"] = self._chembl_db_version
        if self._api_version:
            enriched["api_version"] = self._api_version
        return enriched

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_source_config(self, name: str) -> SourceConfig:
        try:
            return self.config.sources[name]
        except KeyError as exc:  # pragma: no cover - configuration error path
            msg = f"Source '{name}' is not configured for pipeline '{self.pipeline_code}'"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(parameters: Mapping[str, Any]) -> str:
        base_url = parameters.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            msg = "sources.chembl.parameters.base_url must be a non-empty string"
            raise ValueError(msg)
        return base_url.rstrip("/")

    @staticmethod
    def _resolve_page_size(source_config: SourceConfig) -> int:
        """Resolve page size from source config or defaults."""
        batch_size: int | None = getattr(source_config, "batch_size", None)
        if batch_size is None:
            parameters = getattr(source_config, "parameters", {})
            if isinstance(parameters, Mapping):
                candidate: Any = parameters.get("batch_size")  # type: ignore[assignment]
                if isinstance(candidate, int) and candidate > 0:
                    batch_size = candidate
        if batch_size is None or batch_size <= 0:
            batch_size = 200
        return batch_size

    @staticmethod
    def _resolve_max_pages(source_config: SourceConfig) -> int | None:
        """Resolve max pages from source config or defaults."""
        parameters = getattr(source_config, "parameters", {})
        if isinstance(parameters, Mapping):
            parameters_typed: Mapping[str, Any] = cast(Mapping[str, Any], parameters)
            max_pages_raw = parameters_typed.get("max_pages")
            if isinstance(max_pages_raw, int) and max_pages_raw > 0:
                return max_pages_raw
        return None

    def _resolve_select_fields(self, source_config: SourceConfig) -> list[str] | None:
        """Resolve select_fields from config or return None."""
        parameters_raw = getattr(source_config, "parameters", {})
        if isinstance(parameters_raw, Mapping):
            parameters = cast(Mapping[str, Any], parameters_raw)
            select_fields_raw = parameters.get("select_fields")
            if (
                select_fields_raw is not None
                and isinstance(select_fields_raw, Sequence)
                and not isinstance(select_fields_raw, (str, bytes))
            ):
                select_fields = cast(Sequence[Any], select_fields_raw)
                return [str(field) for field in select_fields]
        return None

    def _fetch_chembl_release(self, client: ChemblClient, log: Any) -> str | None:
        """Fetch ChEMBL release version from /status endpoint."""
        try:
            status = client.handshake("/status")
            release_value = status.get("chembl_db_version")
            api_value = status.get("api_version")
            if isinstance(release_value, str):
                self._chembl_db_version = release_value
            if isinstance(api_value, str):
                self._api_version = api_value
            log.info(
                "chembl_testitem.status",
                chembl_db_version=self._chembl_db_version,
                api_version=self._api_version,
            )
            return self._chembl_db_version
        except Exception as exc:
            log.warning("chembl_testitem.status_failed", error=str(exc))
            self._chembl_db_version = None
            self._api_version = None
            return None

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        if isinstance(payload, Mapping):
            return cast(dict[str, Any], payload)
        return {}

    @staticmethod
    def _extract_page_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Extract molecule items from ChEMBL API response."""
        candidates: list[dict[str, Any]] = []
        # ChEMBL molecule endpoint returns "molecules" key
        for key in ("molecules", "data", "items", "results"):
            value: Any = payload.get(key)
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)
                    for item in value  # type: ignore[misc]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        # Fallback: iterate all keys except page_meta
        for key, value in payload.items():
            if key == "page_meta":
                continue
            if isinstance(value, Sequence):
                candidates = [
                    cast(dict[str, Any], item)
                    for item in value  # type: ignore[misc]
                    if isinstance(item, Mapping)
                ]
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _next_link(payload: Mapping[str, Any], base_url: str) -> str | None:
        """Extract next link from ChEMBL page_meta."""
        page_meta: Any = payload.get("page_meta")
        if isinstance(page_meta, Mapping):
            next_link_raw: Any = page_meta.get("next")  # type: ignore[assignment]
            next_link: str | None = cast(str | None, next_link_raw) if next_link_raw is not None else None
            if isinstance(next_link, str) and next_link:
                # If next_link is a full URL, extract only the relative path
                if next_link.startswith("http://") or next_link.startswith("https://"):
                    parsed = urlparse(next_link)
                    base_parsed = urlparse(base_url)

                    # Normalize paths: remove trailing slashes for comparison
                    path = parsed.path.rstrip("/")
                    base_path = base_parsed.path.rstrip("/")

                    # If paths match, return just the path with query
                    if path == base_path or path.startswith(base_path + "/"):
                        relative_path = path[len(base_path) :] if path.startswith(base_path) else path
                        if parsed.query:
                            return f"{relative_path}?{parsed.query}"
                        return relative_path
                    # Otherwise return full URL (shouldn't happen for ChEMBL)
                    return next_link
                # Already a relative path
                return next_link
        return None

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

        # Normalize molecule_chembl_id
        if "molecule_chembl_id" in df.columns:
            df["molecule_chembl_id"] = df["molecule_chembl_id"].astype(str).str.strip()

        # Normalize standard_inchi_key (uppercase, trim)
        # Check both old and new column names for backward compatibility
        inchi_key_col = "molecule_structures__standard_inchi_key" if "molecule_structures__standard_inchi_key" in df.columns else "standard_inchi_key"
        if inchi_key_col in df.columns:
            df[inchi_key_col] = (
                df[inchi_key_col].astype(str).str.upper().str.strip()
            )
            # Replace empty strings with NaN
            df[inchi_key_col] = df[inchi_key_col].replace(  # pyright: ignore[reportUnknownMemberType]
                "",
                pd.NA,
            )

        log.debug("normalize_identifiers_completed")
        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields: trim, replace empty strings with NaN."""

        if df.empty:
            return df

        string_columns = [
            "pref_name",
            "molecule_type",
            "molecule_structures__canonical_smiles",  # New flattened column name
            "canonical_smiles",  # Keep for backward compatibility
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(  # pyright: ignore[reportUnknownMemberType]
                    "",
                    pd.NA,
                )

        log.debug("normalize_string_fields_completed")
        return df

    def _ensure_schema_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Ensure all schema columns exist with proper types."""

        if df.empty:
            return df

        # Get all schema columns from COLUMN_ORDER
        from bioetl.schemas.testitem_chembl import COLUMN_ORDER

        schema_columns = list(COLUMN_ORDER)

        # Define types for columns based on schema
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
        ]
        float_columns = []
        # Integer columns with molecule_properties__ prefix
        int_columns.extend([  # pyright: ignore[reportUnknownMemberType]
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
        ])
        # Float columns with molecule_properties__ prefix
        float_columns.extend([  # pyright: ignore[reportUnknownMemberType]
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
        ])

        for col in schema_columns:
            if col not in df.columns:
                if col in int_columns:
                    df[col] = pd.Series(dtype="Int64")
                elif col in float_columns:
                    df[col] = pd.Series(dtype="Float64")
                else:
                    df[col] = pd.Series(dtype="string")

        log.debug("ensure_schema_columns_completed", columns=list(df.columns))
        return df

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
                numeric_series = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
                    df[col],
                    errors="coerce",
                )
                # For columns that should be >= 0, replace negative values with NaN
                if col in int_ge_zero_columns:
                    # Replace negative values with NaN
                    numeric_series = numeric_series.where(numeric_series >= 0)
                # For boolean-like columns, replace -1 with NaN (unknown)
                elif col in boolean_columns:
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    # Also ensure values are only 0 or 1
                    numeric_series = numeric_series.where((numeric_series == 0) | (numeric_series == 1) | numeric_series.isna())
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
                        col_str = df.loc[mask, col].astype(str).str.upper().str.strip()
                        # Create new series with converted values
                        converted = df[col].copy()
                        # Map Y to 1, N to 0
                        converted.loc[mask & (col_str == "Y")] = 1
                        converted.loc[mask & (col_str == "N")] = 0
                        # For values that are not Y/N, try numeric conversion
                        other_mask = mask & ~col_str.isin(["Y", "N"])  # pyright: ignore[reportUnknownMemberType]
                        if other_mask.any():
                            # Try to convert existing numeric values
                            numeric_vals = pd.to_numeric(df.loc[other_mask, col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                            # Keep only valid numeric values (0 or 1)
                            valid_numeric = numeric_vals.notna() & numeric_vals.isin([0, 1])  # pyright: ignore[reportUnknownMemberType]
                            # Set all other_mask values to NA first
                            converted.loc[other_mask] = pd.NA
                            # Then restore valid numeric values
                            if valid_numeric.any():
                                valid_indices = numeric_vals.index[valid_numeric]
                                converted.loc[valid_indices] = numeric_vals.loc[valid_indices]
                        df[col] = converted
                    # Convert to Int64, ensuring only 0 or 1
                    numeric_series = pd.to_numeric(df[col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    numeric_series = numeric_series.where((numeric_series == 0) | (numeric_series == 1) | numeric_series.isna())
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
                    numeric_series = pd.to_numeric(df[col], errors="coerce")  # pyright: ignore[reportUnknownMemberType]
                    numeric_series = numeric_series.where(numeric_series >= 0)
                    df[col] = numeric_series.astype("Int64")

        log.debug("normalize_numeric_fields_completed")
        return df

    def _remove_extra_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Remove columns that are not in the schema."""

        if df.empty:
            return df

        from bioetl.schemas.testitem_chembl import COLUMN_ORDER

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
        inchi_key_col = "molecule_structures__standard_inchi_key" if "molecule_structures__standard_inchi_key" in df.columns else "standard_inchi_key"
        canonical_smiles_col = "molecule_structures__canonical_smiles" if "molecule_structures__canonical_smiles" in df.columns else "canonical_smiles"
        full_mwt_col = "molecule_properties__full_mwt" if "molecule_properties__full_mwt" in df.columns else "full_mwt"
        alogp_col = "molecule_properties__alogp" if "molecule_properties__alogp" in df.columns else "alogp"

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

