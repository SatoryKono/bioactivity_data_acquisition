"""Target pipeline implementation for ChEMBL."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from numbers import Integral, Real
from typing import Any, cast

import pandas as pd
from pandas._libs import missing as libmissing
from structlog.stdlib import BoundLogger

from bioetl.chembl.common.descriptor import (
    ChemblContextSpec,
    ChemblDescriptorSpec,
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from bioetl.chembl.common.handlers import (
    make_dry_run_handler,
    make_empty_frame_factory,
)
from bioetl.clients.client_chembl import ChemblClient  # noqa: F401 - re-exported for tests
from bioetl.clients.entities.client_target import ChemblTargetClient
from bioetl.config import TargetSourceConfig
from bioetl.config.models.models import PipelineConfig
from bioetl.core.logging import LogEvents
from bioetl.core.schema import IdentifierRule, StringRule
from bioetl.core.schema.normalizers import IdentifierStats
from bioetl.pipelines.unified_base import UnifiedPipelineBase

from .transform import serialize_target_arrays

_ARRAY_SOURCE_COLUMNS: tuple[str, ...] = (
    "cross_references",
    "target_components",
    "target_component_synonyms",
)


def _protein_class_sort_key(class_obj: Mapping[str, Any]) -> tuple[int, int, str, str, str]:
    """Return deterministic sort key for protein class dictionaries."""

    def _coerce_int(value: Any) -> int:
        try:
            if value is None:
                return 0
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0

    level = class_obj.get("class_level")
    level_int = _coerce_int(level)
    class_id = str(class_obj.get("protein_class_id") or "")
    pref_name = str(class_obj.get("pref_name") or "")
    canonical_json = json.dumps(class_obj, ensure_ascii=False, sort_keys=True)
    level_flag = 1 if level is None else 0
    return (level_flag, level_int, class_id, pref_name, canonical_json)


class ChemblTargetPipeline(UnifiedPipelineBase):
    """ETL pipeline extracting target records from the ChEMBL API."""

    actor = "target_chembl"
    id_column = "target_chembl_id"
    extract_event_name = "chembl_target.extract_mode"
    id_extraction_summary_event = LogEvents.CHEMBL_TARGET_EXTRACT_BY_IDS_SUMMARY
    id_extraction_dry_run_event = LogEvents.CHEMBL_TARGET_EXTRACT_SKIPPED
    id_chunk_size_cap = 100
    id_max_batch_size = 200

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self._array_source_cache: pd.DataFrame | None = None
        self.initialize_output_schema()

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def descriptor_spec(self) -> ChemblDescriptorSpec[ChemblTargetPipeline]:
        """Return the declarative descriptor specification for targets."""

        def get_metadata(pipeline: ChemblPipelineBase) -> Mapping[str, Any]:
            return pipeline.publish_release_metadata()

        empty_frame = make_empty_frame_factory("target_chembl_id")
        dry_run_handler = make_dry_run_handler(
            LogEvents.CHEMBL_TARGET_EXTRACT_SKIPPED,
            get_metadata,
        )

        def summary_extra(
            pipeline: ChemblPipelineBase,
            _: pd.DataFrame,
            __: ChemblExtractionContext,
        ) -> Mapping[str, Any]:
            return {"limit": pipeline.config.cli.limit}

        def extra_filters_factory(
            source_config: TargetSourceConfig,
            _: ChemblPipelineBase,
        ) -> dict[str, Any]:
            batch_size = getattr(source_config, "batch_size", None)
            return {"batch_size": batch_size} if batch_size else {}

        context_spec = ChemblContextSpec(
            entity_name="target",
            entity_client_type=ChemblTargetClient,
            extra_filters_factory=extra_filters_factory,
        )

        return ChemblDescriptorSpec(
            name="chembl_target",
            source_name="chembl",
            source_config_factory=TargetSourceConfig.from_source_config,
            context=context_spec,
            id_column="target_chembl_id",
            summary_event="chembl_target.extract_summary",
            sort_by=("target_chembl_id",),
            empty_frame_factory=empty_frame,
            dry_run_handler=dry_run_handler,
            summary_extra=summary_extra,
        )

    def id_extraction_select_fields(
        self,
        source_config: Any,
        typed_source_config: TargetSourceConfig,
        descriptor: ChemblExtractionDescriptor[Any],
    ) -> list[str] | None:
        resolved = typed_source_config.parameters.select_fields
        return self._merge_select_fields(resolved, descriptor.must_have_fields)

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply identifier harmonization before the shared transform template."""

        log = self.logger_for(stage="transform")
        harmonized = self._harmonize_identifier_columns(df, log)
        array_columns = [column for column in _ARRAY_SOURCE_COLUMNS if column in harmonized.columns]
        if array_columns:
            self._array_source_cache = harmonized[array_columns].copy()
        else:
            self._array_source_cache = None
        return harmonized

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run domain-specific normalization and enrichment for targets."""

        log = self.logger_for(stage="transform")
        working_df = df
        cached_arrays: pd.DataFrame | None = getattr(self, "_array_source_cache", None)
        if cached_arrays is not None and not cached_arrays.empty:
            working_df = working_df.join(cached_arrays, how="left")
        working_df = serialize_target_arrays(working_df, self.config)
        self._array_source_cache = None

        if not self.config.cli.dry_run:
            working_df = self._enrich_target_components(working_df, log)
            working_df = self._enrich_protein_classifications(working_df, log)

        return working_df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure schema ordering and apply type normalization after enrichment."""

        log = self.logger_for(stage="transform")
        ensured = self._ensure_schema_columns(df, self._output_column_order, log)
        normalized = self._normalize_data_types(ensured, self._output_schema, log)
        return self._order_schema_columns(normalized, self._output_column_order)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_target_in_set(self, value: object, target_ids_set: set[str]) -> bool:
        """Check if value is a valid target ID in the given set.

        Parameters
        ----------
        value:
            Value to check (can be None, pd.NA, numeric, or string).
        target_ids_set:
            Set of target IDs to check against.

        Returns
        -------
        bool:
            True if value is a non-empty string that exists in target_ids_set, False otherwise.
        """
        if value is None or value is pd.NA:
            return False
        if isinstance(value, Real) and math.isnan(float(value)):
            return False
        normalized = str(value).strip()
        if not normalized:
            return False
        return normalized in target_ids_set

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
            log.debug(LogEvents.IDENTIFIER_HARMONIZATION, actions=actions)

        return df

    def identifier_rules(self) -> Sequence[IdentifierRule]:
        return (
            IdentifierRule(
                name="target_chembl",
                columns=["target_chembl_id"],
                pattern=r"^CHEMBL\d+$",
            ),
        )

    def postprocess_identifier_columns(
        self,
        df: pd.DataFrame,
        stats: IdentifierStats,
        log: BoundLogger,
    ) -> pd.DataFrame:
        invalid_info = stats.per_column.get("target_chembl_id")
        if invalid_info and invalid_info.get("invalid", 0) > 0:
            log.warning(
                LogEvents.INVALID_TARGET_CHEMBL_ID,
                count=invalid_info["invalid"],
            )
        return super().postprocess_identifier_columns(df, stats, log)

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
            log.debug(LogEvents.ENRICH_TARGET_COMPONENTS_SKIPPED, reason="all_data_present")
            return df

        # Reuse existing client if available, otherwise create new one
        source_raw = self._resolve_source_config("chembl")
        source_config = TargetSourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "target",
            source_name="chembl",
            source_config=source_config,
        )
        chembl_client = bundle.chembl_client

        if "target_chembl_id" not in df.columns:
            return df

        component_map: dict[str, list[str]] = {}
        target_ids_set: set[str] = set(target_ids_to_enrich)

        target_membership = df["target_chembl_id"].map(
            lambda x: self._is_target_in_set(x, target_ids_set)
        )

        log.info(LogEvents.ENRICH_TARGET_COMPONENTS_START, target_count=len(target_ids_to_enrich))

        for target_id in target_ids_to_enrich:
            try:
                components: list[str] = []
                for item in chembl_client.paginate(
                    "/target_component.json",
                    params={"target_chembl_id": target_id},
                    page_size=200,
                    items_key="target_components",
                ):
                    accession = item.get("accession")
                    if isinstance(accession, str) and accession.strip():
                        components.append(accession.strip())

                if components:
                    deduped_components = sorted({component.strip() for component in components})
                    component_map[target_id] = deduped_components
            except Exception as exc:
                log.warning(
                    LogEvents.TARGET_COMPONENT_FETCH_ERROR,
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
            mask = target_membership & (df["component_count"].isna() | (df["component_count"] == 0))
            if mask.any():
                df.loc[mask, "component_count"] = df.loc[mask, "target_chembl_id"].map(
                    lambda x: len(component_map.get(str(x), [])) if pd.notna(x) else pd.NA
                )

        log.info(LogEvents.ENRICH_TARGET_COMPONENTS_COMPLETE, enriched_count=len(component_map))
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
            log.debug(LogEvents.ENRICH_PROTEIN_CLASSIFICATIONS_SKIPPED, reason="all_data_present")
            return df

        source_raw = self._resolve_source_config("chembl")
        source_config = TargetSourceConfig.from_source_config(source_raw)
        bundle = self.build_chembl_entity_bundle(
            "target",
            source_name="chembl",
            source_config=source_config,
        )
        chembl_client = bundle.chembl_client

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

        target_membership = df["target_chembl_id"].map(
            lambda x: self._is_target_in_set(x, target_ids_set)
        )

        log.info(
            LogEvents.ENRICH_PROTEIN_CLASSIFICATIONS_START, target_count=len(target_ids_to_enrich)
        )

        # Get component_limit from config if available
        component_limit: int | None = None
        if hasattr(source_config, "parameters") and hasattr(
            source_config.parameters, "component_limit"
        ):
            component_limit = source_config.parameters.component_limit

        for target_id in target_ids_to_enrich:
            try:
                # Step 1: Get target components and filter PROTEIN components
                # Note: component_type is already available in target_component.json response
                # No need to make separate requests to component_sequence.json (endpoint doesn't exist)
                protein_component_ids: list[int] = []
                component_count = 0
                for item in chembl_client.paginate(
                    "/target_component.json",
                    params={"target_chembl_id": target_id},
                    page_size=200,
                    items_key="target_components",
                ):
                    component_id = item.get("component_id")
                    component_type = item.get("component_type")
                    
                    if component_id is not None:
                        try:
                            component_id_int = int(component_id) if not isinstance(component_id, int) else component_id
                            
                            # Filter only PROTEIN components - component_type is already in the response
                            if (
                                isinstance(component_type, str)
                                and component_type.upper() == "PROTEIN"
                            ):
                                protein_component_ids.append(component_id_int)
                            
                            component_count += 1
                            if component_limit is not None and component_count >= component_limit:
                                break
                        except (ValueError, TypeError):
                            # Skip invalid component_id
                            continue

                if not protein_component_ids:
                    continue

                # Step 2: Get protein_class_id for each protein component
                protein_class_ids: set[str] = set()
                for component_id in protein_component_ids:
                    try:
                        for class_item in chembl_client.paginate(
                            "/component_class.json",
                            params={"component_id": component_id},
                            page_size=200,
                            items_key="component_classes",
                        ):
                            protein_class_id = class_item.get("protein_class_id")
                            if protein_class_id is not None:
                                protein_class_ids.add(str(protein_class_id))
                    except Exception as exc:
                        log.debug(
                            LogEvents.COMPONENT_CLASS_FETCH_ERROR,
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
                            page_size=200,
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
                                page_size=200,
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
                                LogEvents.PROTEIN_FAMILY_CLASSIFICATION_FETCH_ERROR,
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
                            LogEvents.PROTEIN_CLASSIFICATION_FETCH_ERROR,
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
                    unique_classes.sort(key=_protein_class_sort_key)

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
                    LogEvents.PROTEIN_CLASSIFICATION_FETCH_ERROR,
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
            LogEvents.ENRICH_PROTEIN_CLASSIFICATIONS_COMPLETE,
            enriched_list_count=len(classification_list_map),
            enriched_top_count=len(classification_top_map),
        )
        return df

    def string_rules(self) -> Mapping[str, StringRule]:
        return {
            "pref_name": StringRule(),
            "target_type": StringRule(),
            "organism": StringRule(),
            "tax_id": StringRule(),
        }

    def _normalize_data_types(self, df: pd.DataFrame, schema: Any | None, log: Any) -> pd.DataFrame:
        """Normalize data types to match schema expectations.

        Overrides base implementation to handle component_count and species_group_flag specially.
        """
        df = super()._normalize_data_types(df, schema, log)

        def _coerce_nullable_int(value: object) -> int | libmissing.NAType:
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
                log.warning(LogEvents.SPECIES_GROUP_FLAG_CONVERSION_FAILED, error=str(exc))

        return df
