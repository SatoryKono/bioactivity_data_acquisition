"""TestItem pipeline implementation for ChEMBL."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlparse

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.config.models import SourceConfig
from bioetl.core import APIClientFactory, UnifiedLogger
from bioetl.core.api_client import UnifiedAPIClient

from ..base import PipelineBase


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
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_testitem_client", client)

        # Fetch status to get ChEMBL versions
        self._fetch_chembl_versions(client, log)

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

        # Pagination parameters
        page_size = self._resolve_page_size(source_config)
        limit = self.config.cli.limit
        max_pages = self._resolve_max_pages(source_config)

        records: list[dict[str, Any]] = []
        next_endpoint: str | None = "/molecule.json"
        params: Mapping[str, Any] | None = {"limit": page_size, "format": "json"}
        pages = 0

        # Apply filters if configured
        if hasattr(source_config, "filters") and source_config.filters:
            if params is None:
                params = {}
            params.update(source_config.filters)

        while next_endpoint:
            page_start = time.perf_counter()
            response = client.get(next_endpoint, params=params)
            payload = self._coerce_mapping(response.json())
            page_items = self._extract_page_items(payload)

            if limit is not None:
                remaining = max(limit - len(records), 0)
                if remaining == 0:
                    break
                page_items = page_items[:remaining]

            records.extend(page_items)
            pages += 1
            page_duration_ms = (time.perf_counter() - page_start) * 1000.0
            log.debug(
                "chembl_testitem.page_fetched",
                endpoint=next_endpoint,
                batch_size=len(page_items),
                total_records=len(records),
                duration_ms=page_duration_ms,
            )

            next_link = self._next_link(payload, base_url=base_url)
            if not next_link or (limit is not None and len(records) >= limit):
                break
            if max_pages is not None and pages >= max_pages:
                break

            log.debug(
                "chembl_testitem.next_link_resolved",
                next_link=next_link,
                base_url=base_url,
            )
            next_endpoint = next_link
            params = None  # ChEMBL provides full URL with params in next link

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
            pages=pages,
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
        client = self._client_factory.for_source("chembl", base_url=base_url)
        self.register_client("chembl_testitem_client", client)

        # Fetch status to get ChEMBL versions
        self._fetch_chembl_versions(client, log)

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

        # Batch extraction parameters
        batch_size = self._resolve_page_size(source_config)
        # Ensure batch_size <= 25 for ChEMBL API URL length limit
        batch_size = min(batch_size, 25)
        limit = self.config.cli.limit

        records: list[dict[str, Any]] = []
        total_batches = 0

        # Process IDs in batches
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i : i + batch_size]
            batch_start = time.perf_counter()

            # Construct batch request
            params: Mapping[str, Any] = {
                "molecule_chembl_id__in": ",".join(batch_ids),
                "format": "json",
                "limit": len(batch_ids),
            }

            try:
                response = client.get("/molecule.json", params=params)
                payload = self._coerce_mapping(response.json())
                page_items = self._extract_page_items(payload)

                # Apply limit if specified
                if limit is not None:
                    remaining = max(limit - len(records), 0)
                    if remaining == 0:
                        break
                    page_items = page_items[:remaining]

                records.extend(page_items)
                total_batches += 1

                batch_duration_ms = (time.perf_counter() - batch_start) * 1000.0
                log.debug(
                    "chembl_testitem.batch_fetched",
                    batch_size=len(batch_ids),
                    fetched=len(page_items),
                    total_records=len(records),
                    duration_ms=batch_duration_ms,
                )

            except Exception as exc:  # pragma: no cover - defensive path
                log.error(
                    "chembl_testitem.batch_error",
                    batch_size=len(batch_ids),
                    error=str(exc),
                    exc_info=True,
                )
                # Continue with next batch instead of failing completely
                total_batches += 1

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
            batches=total_batches,
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

        # Flatten nested structures
        df = self._flatten_nested_structures(df, log)

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Ensure all schema columns exist with proper types
        df = self._ensure_schema_columns(df, log)

        # Add version fields
        df["_chembl_db_version"] = self._chembl_db_version or ""
        df["_api_version"] = self._api_version or ""

        # Deduplication
        df = self._deduplicate_molecules(df, log)

        # Sort by molecule_chembl_id for determinism
        if "molecule_chembl_id" in df.columns:
            df = df.sort_values("molecule_chembl_id").reset_index(drop=True)

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
            max_pages: Any = parameters.get("max_pages")
            if isinstance(max_pages, int) and max_pages > 0:
                return max_pages
        return None

    def _fetch_chembl_versions(
        self,
        client: UnifiedAPIClient,
        log: UnifiedLogger,
    ) -> None:
        """Fetch ChEMBL DB and API versions from /status endpoint."""

        try:
            response = client.get("/status.json")
            status_payload = self._coerce_mapping(response.json())
            self._chembl_db_version = status_payload.get("chembl_db_version")
            self._api_version = status_payload.get("api_version")
            log.info(
                "chembl_testitem.status",
                chembl_db_version=self._chembl_db_version,
                api_version=self._api_version,
            )
        except Exception as exc:  # pragma: no cover - defensive path
            log.warning("chembl_testitem.status_failed", error=str(exc))
            self._chembl_db_version = None
            self._api_version = None

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

    def _flatten_nested_structures(self, df: pd.DataFrame, log: UnifiedLogger) -> pd.DataFrame:
        """Flatten nested molecule_structures and molecule_properties into flat columns."""

        if df.empty:
            return df

        # Flatten molecule_structures
        if "molecule_structures" in df.columns:
            structures_df = pd.json_normalize(df["molecule_structures"].tolist())
            if "canonical_smiles" in structures_df.columns:
                df["canonical_smiles"] = structures_df["canonical_smiles"]
            if "standard_inchi_key" in structures_df.columns:
                df["standard_inchi_key"] = structures_df["standard_inchi_key"]

        # Flatten molecule_properties
        if "molecule_properties" in df.columns:
            properties_df = pd.json_normalize(df["molecule_properties"].tolist())
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

    def _normalize_identifiers(self, df: pd.DataFrame, log: UnifiedLogger) -> pd.DataFrame:
        """Normalize ChEMBL identifiers and InChI keys."""

        if df.empty:
            return df

        # Normalize molecule_chembl_id
        if "molecule_chembl_id" in df.columns:
            df["molecule_chembl_id"] = df["molecule_chembl_id"].astype(str).str.strip()

        # Normalize standard_inchi_key (uppercase, trim)
        if "standard_inchi_key" in df.columns:
            df["standard_inchi_key"] = (
                df["standard_inchi_key"].astype(str).str.upper().str.strip()
            )
            # Replace empty strings with NaN
            df["standard_inchi_key"] = df["standard_inchi_key"].replace("", pd.NA)

        log.debug("normalize_identifiers_completed")
        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: UnifiedLogger) -> pd.DataFrame:
        """Normalize string fields: trim, replace empty strings with NaN."""

        if df.empty:
            return df

        string_columns = [
            "pref_name",
            "molecule_type",
            "canonical_smiles",
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace("", pd.NA)

        log.debug("normalize_string_fields_completed")
        return df

    def _ensure_schema_columns(self, df: pd.DataFrame, log: UnifiedLogger) -> pd.DataFrame:
        """Ensure all schema columns exist with proper types."""

        if df.empty:
            return df

        schema_columns = [
            "molecule_chembl_id",
            "pref_name",
            "molecule_type",
            "max_phase",
            "first_approval",
            "chirality",
            "black_box_warning",
            "availability_type",
            "canonical_smiles",
            "standard_inchi_key",
            "full_mwt",
            "mw_freebase",
            "alogp",
            "hbd",
            "hba",
            "psa",
            "aromatic_rings",
            "rtb",
            "num_ro5_violations",
            "_chembl_db_version",
            "_api_version",
        ]

        for col in schema_columns:
            if col not in df.columns:
                if col in ["max_phase", "first_approval", "chirality", "black_box_warning", "availability_type", "hbd", "hba", "aromatic_rings", "rtb", "num_ro5_violations"]:
                    df[col] = pd.Series(dtype="Int64")
                elif col in ["full_mwt", "mw_freebase", "alogp", "psa"]:
                    df[col] = pd.Series(dtype="Float64")
                else:
                    df[col] = pd.Series(dtype="string")

        log.debug("ensure_schema_columns_completed", columns=list(df.columns))
        return df

    def _deduplicate_molecules(self, df: pd.DataFrame, log: UnifiedLogger) -> pd.DataFrame:
        """Deduplicate molecules by standard_inchi_key (fallback to molecule_chembl_id)."""

        if df.empty:
            return df

        rows_before = len(df)

        # Prioritize records with non-empty canonical_smiles and more complete properties
        if "standard_inchi_key" in df.columns:
            # Sort by completeness (more complete first)
            completeness_cols = ["canonical_smiles", "full_mwt", "alogp"]
            available_completeness = [col for col in completeness_cols if col in df.columns]
            if available_completeness:
                # Count non-null values in completeness columns
                df["_completeness"] = df[available_completeness].notna().sum(axis=1)
                df = df.sort_values(
                    ["_completeness", "canonical_smiles"],
                    ascending=[False, False],
                    na_position="last",
                )
                df = df.drop(columns=["_completeness"])

            # Deduplicate by standard_inchi_key, keeping first (most complete)
            df = df.drop_duplicates(subset=["standard_inchi_key"], keep="first")
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

