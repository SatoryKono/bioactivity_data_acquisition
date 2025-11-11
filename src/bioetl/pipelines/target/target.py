"""Target pipeline implementation for ChEMBL."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from numbers import Integral, Real
from typing import Any, cast

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.clients.chembl import ChemblClient
from bioetl.clients.target.chembl_target import ChemblTargetClient
from bioetl.config import PipelineConfig, TargetSourceConfig
from bioetl.core import UnifiedLogger
from bioetl.core.normalizers import (
    IdentifierRule,
    StringRule,
    normalize_identifier_columns,
    normalize_string_columns,
)
from bioetl.schemas.target import COLUMN_ORDER, TargetSchema

from ..chembl_base import (
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from .target_transform import serialize_target_arrays


class ChemblTargetPipeline(ChemblPipelineBase):
    """ETL pipeline extracting target records from the ChEMBL API."""

    actor = "target_chembl"

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:
        """Fetch target payloads from ChEMBL using the configured HTTP client.

        Checks for input_file in config.cli.input_file and calls extract_by_ids()
        if present, otherwise calls extract_all().
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))

        return self._dispatch_extract_mode(
            log,
            event_name="chembl_target.extract_mode",
            batch_callback=self.extract_by_ids,
            full_callback=self.extract_all,
            id_column_name="target_chembl_id",
        )

    def extract_all(self) -> pd.DataFrame:
        """Extract all target records from ChEMBL using pagination."""

        descriptor = self._build_target_descriptor()
        return self.run_extract_all(descriptor)

    def _build_target_descriptor(self) -> ChemblExtractionDescriptor:
        """Return the descriptor powering target extraction."""

        def build_context(
            pipeline: "ChemblTargetPipeline",
            source_config: TargetSourceConfig,
            log: BoundLogger,
        ) -> ChemblExtractionContext:
            base_url = pipeline._resolve_base_url(source_config.parameters)
            http_client, _ = pipeline.prepare_chembl_client(
                "chembl",
                base_url=base_url,
                client_name="chembl_target_http",
            )
            chembl_client = ChemblClient(http_client)
            pipeline._chembl_release = pipeline.fetch_chembl_release(chembl_client, log)
            target_client = ChemblTargetClient(
                chembl_client,
                batch_size=min(source_config.batch_size, 25),
            )
            select_fields = source_config.parameters.select_fields
            return ChemblExtractionContext(
                source_config=source_config,
                iterator=target_client,
                chembl_client=chembl_client,
                select_fields=list(select_fields) if select_fields else None,
                chembl_release=pipeline._chembl_release,
                extra_filters={"batch_size": source_config.batch_size},
            )

        def empty_frame(
            _: "ChemblTargetPipeline",
            __: ChemblExtractionContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({"target_chembl_id": pd.Series(dtype="string")})

        def dry_run_handler(
            pipeline: "ChemblTargetPipeline",
            _: ChemblExtractionContext,
            log: BoundLogger,
            stage_start: float,
        ) -> pd.DataFrame:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_target.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=pipeline._chembl_release,
            )
            return pd.DataFrame()

        def summary_extra(
            pipeline: "ChemblTargetPipeline",
            _: pd.DataFrame,
            __: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            return {"limit": pipeline.config.cli.limit}

        return ChemblExtractionDescriptor(
            name="chembl_target",
            source_name="chembl",
            source_config_factory=TargetSourceConfig.from_source_config,
            build_context=build_context,
            id_column="target_chembl_id",
            summary_event="chembl_target.extract_summary",
            sort_by=("target_chembl_id",),
            empty_frame_factory=empty_frame,
            dry_run_handler=dry_run_handler,
            summary_extra=summary_extra,
        )

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        """Extract target records by a specific list of IDs using batch extraction.

        Parameters
        ----------
        ids:
            Sequence of target_chembl_id values to extract.

        Returns
        -------
        pd.DataFrame:
            DataFrame containing extracted target records.
        """
        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("extract"))
        stage_start = time.perf_counter()

        source_raw = self._resolve_source_config("chembl")
        source_config = TargetSourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client(
            "chembl", base_url=base_url, client_name="chembl_target_http"
        )

        chembl_client = ChemblClient(http_client)
        self._chembl_release = self.fetch_chembl_release(chembl_client, log)

        if self.config.cli.dry_run:
            duration_ms = (time.perf_counter() - stage_start) * 1000.0
            log.info(
                "chembl_target.extract_skipped",
                dry_run=True,
                duration_ms=duration_ms,
                chembl_release=self._chembl_release,
            )
            return pd.DataFrame()

        batch_size = source_config.batch_size
        limit = self.config.cli.limit
        select_fields = source_config.parameters.select_fields

        id_filters = {
            "mode": "ids",
            "requested_ids": [str(value) for value in ids],
            "limit": int(limit) if limit is not None else None,
            "batch_size": batch_size,
            "select_fields": list(select_fields) if select_fields else None,
        }
        compact_id_filters = {key: value for key, value in id_filters.items() if value is not None}
        self.record_extract_metadata(
            chembl_release=self._chembl_release,
            filters=compact_id_filters,
            requested_at_utc=datetime.now(timezone.utc),
        )

        # Process IDs in chunks to avoid URL length limits
        chunk_size = min(batch_size, 100)  # Conservative limit for target_chembl_id__in
        all_records: list[Mapping[str, Any]] = []
        ids_list = list(ids)

        for i in range(0, len(ids_list), chunk_size):
            chunk = ids_list[i : i + chunk_size]
            params: dict[str, Any] = {
                "target_chembl_id__in": ",".join(chunk),
                "limit": min(batch_size, 25),
            }
            if select_fields:
                params["only"] = ",".join(select_fields)

            try:
                # Используем специализированный клиент для target
                target_client = ChemblTargetClient(chembl_client, batch_size=min(batch_size, 25))
                for item in target_client.iterate_by_ids(chunk, select_fields=select_fields):
                    all_records.append(item)
                    if limit is not None and len(all_records) >= limit:
                        break
            except Exception as exc:
                log.warning(
                    "chembl_target.fetch_error",
                    target_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

            if limit is not None and len(all_records) >= limit:
                break

        records_for_frame: list[dict[str, Any]] = [dict(record) for record in all_records]
        dataframe = pd.DataFrame(records_for_frame)
        if not dataframe.empty and "target_chembl_id" in dataframe.columns:
            dataframe = dataframe.sort_values("target_chembl_id").reset_index(drop=True)

        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        log.info(
            "chembl_target.extract_by_ids_summary",
            rows=int(dataframe.shape[0]),
            requested=len(ids),
            duration_ms=duration_ms,
            chembl_release=self._chembl_release,
            limit=limit,
        )
        return dataframe

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw target data by normalizing fields and enriching with component/classification data."""

        log = UnifiedLogger.get(__name__).bind(component=self._component_for_stage("transform"))

        df = df.copy()

        df = self._harmonize_identifier_columns(df, log)
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        if df.empty:
            log.debug("transform_empty_dataframe")
            return df

        log.info("transform_started", rows=len(df))

        # Normalize identifiers
        df = self._normalize_identifiers(df, log)

        # Serialize array fields (cross_references, target_components, target_component_synonyms)
        df = serialize_target_arrays(df, self.config)

        # Enrich with target_component data
        if not self.config.cli.dry_run:
            df = self._enrich_target_components(df, log)

        # Enrich with protein_classification data
        if not self.config.cli.dry_run:
            df = self._enrich_protein_classifications(df, log)

        # Normalize string fields
        df = self._normalize_string_fields(df, log)

        # Ensure all schema columns exist after enrichment
        df = self._ensure_schema_columns(df, COLUMN_ORDER, log)

        # Normalize data types after ensuring columns (to fix types created as object)
        df = self._normalize_data_types(df, TargetSchema, log)

        df = self._order_schema_columns(df, COLUMN_ORDER)

        log.info("transform_completed", rows=len(df))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _harmonize_identifier_columns(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Harmonize identifier column names."""
        df = df.copy()
        actions: list[str] = []

        if "target_id" in df.columns and "target_chembl_id" not in df.columns:
            df["target_chembl_id"] = df["target_id"]
            actions.append("target_id->target_chembl_id")

        alias_columns = [column for column in ("target_id",) if column in df.columns]
        if alias_columns:
            df = df.drop(columns=alias_columns)
            actions.append(f"dropped_aliases:{','.join(alias_columns)}")

        if actions:
            log.debug("identifier_harmonization", actions=actions)

        return df

    def _normalize_identifiers(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize ChEMBL identifiers with regex validation."""
        rules = [
            IdentifierRule(
                name="target_chembl",
                columns=["target_chembl_id"],
                pattern=r"^CHEMBL\d+$",
            ),
        ]

        normalized_df, stats = normalize_identifier_columns(df, rules)

        invalid_info = stats.per_column.get("target_chembl_id")
        if invalid_info and invalid_info["invalid"] > 0:
            log.warning(
                "invalid_target_chembl_id",
                count=invalid_info["invalid"],
            )

        return normalized_df

    def _enrich_target_components(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Enrich targets with component data from /target_component endpoint.

        Only enriches targets where uniprot_accessions or component_count are missing.
        If data is already present from the main query (via serialize_target_arrays),
        it will not be overwritten.
        """
        df = df.copy()

        if df.empty or "target_chembl_id" not in df.columns:
            return df

        # Check which targets need enrichment
        needs_enrichment = df["target_chembl_id"].notna()
        if "uniprot_accessions" in df.columns:
            # Only enrich rows where uniprot_accessions is empty/NA
            needs_enrichment = needs_enrichment & (
                df["uniprot_accessions"].isna() | (df["uniprot_accessions"] == "")
            )
        if "component_count" in df.columns:
            # Also check component_count
            needs_enrichment = needs_enrichment | (
                df["target_chembl_id"].notna()
                & (df["component_count"].isna() | (df["component_count"] == 0))
            )

        target_ids_to_enrich: list[str] = (
            df.loc[needs_enrichment, "target_chembl_id"].dropna().unique().tolist()
        )

        if not target_ids_to_enrich:
            log.debug("enrich_target_components_skipped", reason="all_data_present")
            return df

        # Reuse existing client if available, otherwise create new one
        source_raw = self._resolve_source_config("chembl")
        source_config = TargetSourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client("chembl", base_url=base_url)
        chembl_client = ChemblClient(http_client)

        if "target_chembl_id" not in df.columns:
            return df

        component_map: dict[str, list[str]] = {}
        target_ids_set: set[str] = set(target_ids_to_enrich)

        def _is_target(value: object) -> bool:
            if value is None or value is pd.NA:
                return False
            if isinstance(value, Real) and math.isnan(float(value)):
                return False
            normalized = str(value).strip()
            if not normalized:
                return False
            return normalized in target_ids_set

        target_membership = df["target_chembl_id"].map(_is_target)

        log.info("enrich_target_components_start", target_count=len(target_ids_to_enrich))

        for target_id in target_ids_to_enrich:
            try:
                components: list[str] = []
                for item in chembl_client.paginate(
                    "/target_component.json",
                    params={"target_chembl_id": target_id},
                    page_size=25,
                    items_key="target_components",
                ):
                    accession = item.get("accession")
                    if isinstance(accession, str) and accession.strip():
                        components.append(accession.strip())

                if components:
                    component_map[target_id] = components
            except Exception as exc:
                log.warning(
                    "target_component_fetch_error",
                    target_chembl_id=target_id,
                    error=str(exc),
                )

        # Add uniprot_accessions column as JSON array (only for missing values)
        if "uniprot_accessions" in df.columns:
            mask = target_membership & (
                df["uniprot_accessions"].isna() | (df["uniprot_accessions"] == "")
            )
            if mask.any():
                df.loc[mask, "uniprot_accessions"] = df.loc[mask, "target_chembl_id"].map(
                    lambda x: json.dumps(component_map.get(str(x), [])) if pd.notna(x) else pd.NA
                )

        # Add component_count column (only for missing values)
        if "component_count" in df.columns:
            mask = target_membership & (
                df["component_count"].isna() | (df["component_count"] == 0)
            )
            if mask.any():
                df.loc[mask, "component_count"] = df.loc[mask, "target_chembl_id"].map(
                    lambda x: len(component_map.get(str(x), [])) if pd.notna(x) else pd.NA
                )

        log.info("enrich_target_components_complete", enriched_count=len(component_map))
        return df

    def _enrich_protein_classifications(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Enrich targets with full protein classification hierarchy.

        Extracts complete protein classification hierarchy with tree nodes and expanded paths l1..l8.
        Algorithm:
        1. For each target, get components via /target_component.json
        2. Filter only PROTEIN components (component_type = 'PROTEIN')
        3. For each protein component, get classes via /component_class.json → protein_class_id
        4. For each protein_class_id, get node metadata via /protein_classification.json
        5. For each protein_class_id, get expanded path via /protein_family_classification.json → l1..l8
        6. Aggregate at TID level: protein_class_list (array) and protein_class_top (min class_level)

        Only enriches targets where protein_class_list or protein_class_top are missing.
        If data is already present from the main query, it will not be overwritten.
        """
        df = df.copy()

        if df.empty or "target_chembl_id" not in df.columns:
            return df

        # Check which targets need enrichment
        needs_enrichment = df["target_chembl_id"].notna()
        if "protein_class_list" in df.columns:
            needs_enrichment = needs_enrichment & (
                df["protein_class_list"].isna() | (df["protein_class_list"] == "")
            )
        if "protein_class_top" in df.columns:
            needs_enrichment = needs_enrichment | (
                df["target_chembl_id"].notna()
                & (df["protein_class_top"].isna() | (df["protein_class_top"] == ""))
            )

        target_ids_to_enrich: list[str] = (
            df.loc[needs_enrichment, "target_chembl_id"].dropna().unique().tolist()
        )

        if not target_ids_to_enrich:
            log.debug("enrich_protein_classifications_skipped", reason="all_data_present")
            return df

        source_raw = self._resolve_source_config("chembl")
        source_config = TargetSourceConfig.from_source_config(source_raw)
        base_url = self._resolve_base_url(cast(Mapping[str, Any], dict(source_config.parameters)))
        http_client, _ = self.prepare_chembl_client("chembl", base_url=base_url)
        chembl_client = ChemblClient(http_client)

        # Initialize columns if they don't exist
        if "protein_class_list" not in df.columns:
            df["protein_class_list"] = pd.NA
        if "protein_class_top" not in df.columns:
            df["protein_class_top"] = pd.NA

        # Maps: target_id -> list of protein_class objects
        if "target_chembl_id" not in df.columns:
            return df

        classification_list_map: dict[str, list[dict[str, Any]]] = {}
        classification_top_map: dict[str, dict[str, Any]] = {}
        target_ids_set: set[str] = set(target_ids_to_enrich)

        def _is_target(value: object) -> bool:
            if value is None or value is pd.NA:
                return False
            if isinstance(value, Real) and math.isnan(float(value)):
                return False
            normalized = str(value).strip()
            if not normalized:
                return False
            return normalized in target_ids_set

        target_membership = df["target_chembl_id"].map(_is_target)

        log.info("enrich_protein_classifications_start", target_count=len(target_ids_to_enrich))

        for target_id in target_ids_to_enrich:
            try:
                # Step 1: Get target components
                component_ids: list[str] = []
                for item in chembl_client.paginate(
                    "/target_component.json",
                    params={"target_chembl_id": target_id},
                    page_size=25,
                    items_key="target_components",
                ):
                    component_id = item.get("component_id")
                    if component_id is not None:
                        component_ids.append(str(component_id))

                if not component_ids:
                    continue

                # Step 2: Filter only PROTEIN components
                protein_component_ids: list[str] = []
                for component_id in component_ids:
                    try:
                        # Get component_sequence to check component_type
                        for seq_item in chembl_client.paginate(
                            "/component_sequence.json",
                            params={"component_id": component_id},
                            page_size=25,
                            items_key="component_sequences",
                        ):
                            component_type = seq_item.get("component_type")
                            if (
                                isinstance(component_type, str)
                                and component_type.upper() == "PROTEIN"
                            ):
                                protein_component_ids.append(component_id)
                                break
                    except Exception as exc:
                        log.debug(
                            "component_sequence_fetch_error",
                            target_chembl_id=target_id,
                            component_id=component_id,
                            error=str(exc),
                        )

                if not protein_component_ids:
                    continue

                # Step 3: Get protein_class_id for each protein component
                protein_class_ids: set[str] = set()
                for component_id in protein_component_ids:
                    try:
                        for class_item in chembl_client.paginate(
                            "/component_class.json",
                            params={"component_id": component_id},
                            page_size=25,
                            items_key="component_classes",
                        ):
                            protein_class_id = class_item.get("protein_class_id")
                            if protein_class_id is not None:
                                protein_class_ids.add(str(protein_class_id))
                    except Exception as exc:
                        log.debug(
                            "component_class_fetch_error",
                            target_chembl_id=target_id,
                            component_id=component_id,
                            error=str(exc),
                        )

                if not protein_class_ids:
                    continue

                # Step 4 & 5: Get metadata and paths for each protein_class_id
                protein_classes: list[dict[str, Any]] = []
                for protein_class_id in protein_class_ids:
                    try:
                        # Get node metadata
                        node_metadata: dict[str, Any] | None = None
                        for node_item in chembl_client.paginate(
                            "/protein_classification.json",
                            params={"protein_classification_id": protein_class_id},
                            page_size=25,
                            items_key="protein_classifications",
                        ):
                            node_metadata = {
                                "protein_class_id": str(protein_class_id),
                                "pref_name": node_item.get("pref_name"),
                                "short_name": node_item.get("short_name"),
                                "class_level": node_item.get("class_level"),
                                "parent_id": node_item.get("parent_id"),
                                "protein_class_desc": node_item.get("protein_class_desc"),
                            }
                            break

                        # Get expanded path l1..l8
                        path_levels: list[str | None] = [None] * 8
                        try:
                            for path_item in chembl_client.paginate(
                                "/protein_family_classification.json",
                                params={"protein_classification_id": protein_class_id},
                                page_size=25,
                                items_key="protein_family_classifications",
                            ):
                                for i in range(1, 9):
                                    level_key = f"l{i}"
                                    level_value = path_item.get(level_key)
                                    if level_value is not None:
                                        # Convert to string, handling NaN values
                                        if isinstance(level_value, (float, int)):
                                            if pd.isna(level_value):
                                                path_levels[i - 1] = None
                                            else:
                                                path_levels[i - 1] = str(level_value)
                                        else:
                                            path_levels[i - 1] = str(level_value)
                                break
                        except Exception as exc:
                            log.debug(
                                "protein_family_classification_fetch_error",
                                target_chembl_id=target_id,
                                protein_class_id=protein_class_id,
                                error=str(exc),
                            )

                        # Build complete class object
                        if node_metadata:
                            class_obj: dict[str, Any] = {
                                "protein_class_id": node_metadata["protein_class_id"],
                                "pref_name": node_metadata.get("pref_name"),
                                "short_name": node_metadata.get("short_name"),
                                "class_level": node_metadata.get("class_level"),
                                "parent_id": node_metadata.get("parent_id"),
                                "protein_class_desc": node_metadata.get("protein_class_desc"),
                                "path": [level for level in path_levels if level is not None],
                            }
                            protein_classes.append(class_obj)
                    except Exception as exc:
                        log.warning(
                            "protein_classification_fetch_error",
                            target_chembl_id=target_id,
                            protein_class_id=protein_class_id,
                            error=str(exc),
                        )

                if protein_classes:
                    # Deduplicate by protein_class_id (keep first occurrence)
                    seen_ids: set[str] = set()
                    unique_classes: list[dict[str, Any]] = []
                    for class_obj in protein_classes:
                        class_id = class_obj.get("protein_class_id")
                        if class_id and class_id not in seen_ids:
                            seen_ids.add(class_id)
                            unique_classes.append(class_obj)

                    # Sort by class_level for deterministic order
                    unique_classes.sort(
                        key=lambda x: (x.get("class_level") is None, x.get("class_level") or 0)
                    )

                    classification_list_map[target_id] = unique_classes

                    # Find top class (minimum class_level)
                    top_class: dict[str, Any] | None = None
                    min_level: int | None = None
                    for class_obj in unique_classes:
                        level = class_obj.get("class_level")
                        if level is not None:
                            try:
                                level_int = int(level) if not isinstance(level, int) else level
                                if min_level is None or level_int < min_level:
                                    min_level = level_int
                                    top_class = class_obj
                            except (ValueError, TypeError):
                                continue

                    if top_class:
                        classification_top_map[target_id] = top_class

            except Exception as exc:
                log.warning(
                    "protein_classification_fetch_error",
                    target_chembl_id=target_id,
                    error=str(exc),
                )

        # Add protein_class_list column (only for missing values)
        if "protein_class_list" in df.columns:
            mask = target_membership & (
                df["protein_class_list"].isna() | (df["protein_class_list"] == "")
            )
            if mask.any():
                df.loc[mask, "protein_class_list"] = df.loc[mask, "target_chembl_id"].map(
                    lambda x: json.dumps(
                        classification_list_map.get(str(x), []), ensure_ascii=False, sort_keys=True
                    )
                    if pd.notna(x) and str(x) in classification_list_map
                    else pd.NA
                )

        # Add protein_class_top column (only for missing values)
        if "protein_class_top" in df.columns:
            mask = target_membership & (
                df["protein_class_top"].isna() | (df["protein_class_top"] == "")
            )
            if mask.any():
                df.loc[mask, "protein_class_top"] = df.loc[mask, "target_chembl_id"].map(
                    lambda x: json.dumps(
                        classification_top_map.get(str(x), {}), ensure_ascii=False, sort_keys=True
                    )
                    if pd.notna(x) and str(x) in classification_top_map
                    else pd.NA
                )

        log.info(
            "enrich_protein_classifications_complete",
            enriched_list_count=len(classification_list_map),
            enriched_top_count=len(classification_top_map),
        )
        return df

    def _normalize_string_fields(self, df: pd.DataFrame, log: Any) -> pd.DataFrame:
        """Normalize string fields by trimming whitespace."""
        working_df = df.copy()

        rules = {
            "pref_name": StringRule(),
            "target_type": StringRule(),
            "organism": StringRule(),
            "tax_id": StringRule(),
        }

        normalized_df, stats = normalize_string_columns(working_df, rules, copy=False)

        if stats.has_changes:
            log.debug(
                "string_fields_normalized",
                columns=list(stats.per_column.keys()),
                rows_processed=stats.processed,
            )

        return normalized_df

    def _normalize_data_types(
        self, df: pd.DataFrame, schema: Any | None, log: Any
    ) -> pd.DataFrame:
        """Normalize data types to match schema expectations.

        Overrides base implementation to handle component_count and species_group_flag specially.
        """
        df = super()._normalize_data_types(df, schema, log)

        def _coerce_nullable_int(value: object) -> object:
            if value is None or value is pd.NA:
                return pd.NA
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, Integral):
                return int(value)
            if isinstance(value, Decimal):
                if value.is_nan() or value % 1 != 0:
                    return pd.NA
                return int(value)
            if isinstance(value, Real):
                float_value = float(value)
                if not math.isfinite(float_value) or not float_value.is_integer():
                    return pd.NA
                return int(float_value)
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return pd.NA
                try:
                    decimal_value = Decimal(stripped)
                except (InvalidOperation, ValueError):
                    return pd.NA
                if decimal_value.is_nan() or decimal_value % 1 != 0:
                    return pd.NA
                return int(decimal_value)
            return pd.NA

        # Ensure component_count is Int64 (nullable integer)
        if "component_count" in df.columns:
            coerced_component_count = df["component_count"].map(_coerce_nullable_int)
            df["component_count"] = coerced_component_count.astype("Int64")

        # Normalize species_group_flag: convert bool/str/int to Int64 (0 or 1)
        if "species_group_flag" in df.columns:
            try:
                coerced_species_group_flag = df["species_group_flag"].map(_coerce_nullable_int)
                df["species_group_flag"] = coerced_species_group_flag.astype("Int64")
            except (ValueError, TypeError) as exc:
                log.warning("species_group_flag_conversion_failed", error=str(exc))

        return df
