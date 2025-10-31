"""Service helpers for enriching Guide to Pharmacology (IUPHAR) payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.sources.iuphar.normalizer import (
    normalize_gene_symbol,
    normalize_target_name,
    unique_preserving_order,
)

logger = UnifiedLogger.get(__name__)


@dataclass(slots=True)
class IupharServiceConfig:
    """Runtime configuration for :class:`IupharService`."""

    identifier_column: str = "target_chembl_id"
    output_identifier_column: str | None = None
    candidate_columns: Sequence[str] = (
        "pref_name",
        "target_names",
        "iuphar_name",
        "name",
    )
    gene_symbol_columns: Sequence[str] = ("uniprot_gene_primary", "gene_symbol")
    fallback_source: str = "chembl"


class IupharService:
    """Encapsulates the logic required to enrich IUPHAR payloads."""

    def __init__(
        self,
        config: IupharServiceConfig | None = None,
        record_missing_mapping: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config or IupharServiceConfig()
        if self.config.output_identifier_column:
            self.output_identifier_column = self.config.output_identifier_column
        else:
            self.output_identifier_column = self.config.identifier_column
        self._record_missing_mapping = record_missing_mapping

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enrich_targets(
        self,
        df: pd.DataFrame,
        *,
        targets: Sequence[Mapping[str, Any]] | None = None,
        families: Sequence[Mapping[str, Any]] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        """Annotate the provided dataframe with IUPHAR classifications."""

        working_df = df.reset_index(drop=True).copy().convert_dtypes()

        targets_payload = list(targets or [])
        families_payload = list(families or [])

        if not targets_payload or not families_payload:
            logger.info(
                "iuphar_enrichment_no_data",
                targets=len(targets_payload),
                families=len(families_payload),
            )
            return working_df, pd.DataFrame(), pd.DataFrame(), {
                "enrichment_success.iuphar": 0.0,
                "iuphar_coverage": 0.0,
                "enrichment.iuphar.total": 0,
                "enrichment.iuphar.matched": 0,
            }

        family_index: dict[int, dict[str, Any]] = {}
        for raw_family in families_payload:
            try:
                family_id = int(raw_family.get("familyId") or 0)
            except (TypeError, ValueError):
                continue
            family_index[family_id] = dict(raw_family)

        family_paths = self.build_family_hierarchy(family_index)

        classification_map: dict[int, dict[str, Any]] = {}
        normalized_index: dict[str, list[int]] = {}

        for target in targets_payload:
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

            normalized_name = normalize_target_name(str(target.get("name")))
            if normalized_name:
                normalized_index.setdefault(normalized_name, []).append(target_id_int)

        classification_records: list[dict[str, Any]] = []
        gold_records: list[dict[str, Any]] = []

        matched = 0
        total_candidates = 0

        identifier_column = self.config.identifier_column
        output_identifier_column = self.output_identifier_column

        for idx, row in working_df.iterrows():
            candidate_names = self.candidate_names_from_row(row)
            if not candidate_names:
                fallback = self.fallback_classification_record(row)
                classification_records.append(fallback)
                gold_records.append(
                    {
                        output_identifier_column: row.get(identifier_column),
                        "iuphar_target_id": None,
                        "iuphar_type": None,
                        "iuphar_class": None,
                        "iuphar_subclass": None,
                        "classification_source": self.config.fallback_source,
                    }
                )
                self._emit_missing_mapping(
                    stage="iuphar",
                    target_id=row.get(identifier_column),
                    resolution="fallback",
                    status="no_candidate_names",
                    details={"reason": "no_candidate_names"},
                )
                continue

            total_candidates += 1

            matched_id: int | None = None
            for candidate in candidate_names:
                norm_candidate = normalize_target_name(candidate)
                if not norm_candidate:
                    continue
                candidate_ids = normalized_index.get(norm_candidate)
                if candidate_ids:
                    matched_id = candidate_ids[0]
                    break

            if matched_id is None:
                fallback = self.fallback_classification_record(row)
                classification_records.append(fallback)
                gold_records.append(
                    {
                        output_identifier_column: row.get(identifier_column),
                        "iuphar_target_id": None,
                        "iuphar_type": None,
                        "iuphar_class": None,
                        "iuphar_subclass": None,
                        "classification_source": self.config.fallback_source,
                    }
                )
                self._emit_missing_mapping(
                    stage="iuphar",
                    target_id=row.get(identifier_column),
                    resolution="fallback",
                    status="no_match",
                    details={"candidates": candidate_names},
                )
                continue

            matched += 1
            target_entry = classification_map.get(matched_id, {})
            families_for_target = target_entry.get("families", [])
            best_classification = self.select_best_classification(families_for_target)

            working_df.at[idx, "iuphar_target_id"] = matched_id
            if best_classification is not None:
                working_df.at[idx, "iuphar_type"] = best_classification.get("iuphar_type")
                working_df.at[idx, "iuphar_class"] = best_classification.get("iuphar_class")
                working_df.at[idx, "iuphar_subclass"] = best_classification.get("iuphar_subclass")

            for record in families_for_target:
                enriched_record = dict(record)
                enriched_record.update(
                    {
                        output_identifier_column: row.get(identifier_column),
                        "iuphar_target_id": matched_id,
                        "classification_source": "iuphar",
                    }
                )
                classification_records.append(enriched_record)

            gold_records.append(
                {
                    output_identifier_column: row.get(identifier_column),
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

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def build_family_hierarchy(
        self, families: Mapping[int, Mapping[str, Any]]
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

            if isinstance(parents, Iterable) and parents:
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
            _resolve(int(fam_id))

        return cache

    def normalize_name(self, value: str | None) -> str:
        """Backwards compatible wrapper around :func:`normalize_target_name`."""

        return normalize_target_name(value)

    def candidate_names_from_row(self, row: pd.Series) -> list[str]:
        candidates: list[str] = []

        for column in self.config.candidate_columns:
            value = row.get(column)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue

            if isinstance(value, list):
                parts = value
            elif isinstance(value, str):
                if column.endswith("names") or "synonym" in column:
                    parts = [part.strip() for part in value.split("|") if part and part.strip()]
                else:
                    parts = [value]
            else:
                parts = [str(value)]

            for part in parts:
                text = str(part).strip()
                if text:
                    candidates.append(text)

        for column in self.config.gene_symbol_columns:
            value = row.get(column)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            text = normalize_gene_symbol(str(value))
            if text:
                candidates.append(text)

        return unique_preserving_order(candidates)

    def fallback_classification_record(self, row: pd.Series) -> dict[str, Any]:
        return {
            self.output_identifier_column: row.get(self.config.identifier_column),
            "iuphar_target_id": None,
            "iuphar_family_id": None,
            "iuphar_family_name": None,
            "classification_path": None,
            "classification_depth": 0,
            "iuphar_type": None,
            "iuphar_class": None,
            "iuphar_subclass": None,
            "classification_source": self.config.fallback_source,
        }

    @staticmethod
    def select_best_classification(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        if not records:
            return None
        return records[0]

    def _emit_missing_mapping(
        self,
        *,
        stage: str,
        target_id: Any,
        resolution: str,
        status: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        if self._record_missing_mapping is None:
            return

        payload: dict[str, Any] = {
            "stage": stage,
            "target_id": target_id,
            "accession": None,
            "resolution": resolution,
            "status": status,
        }
        if details:
            payload["details"] = details
        try:
            self._record_missing_mapping(**payload)
        except TypeError:
            self._record_missing_mapping(payload)


__all__ = ["IupharService", "IupharServiceConfig"]
