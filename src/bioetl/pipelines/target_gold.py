"""Utilities for assembling the target gold layer outputs."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from bioetl.config.models import MaterializationPaths
from bioetl.core.logger import UnifiedLogger
from bioetl.core.materialization import MaterializationManager

# Runtime-safe aliases for pandas API to avoid stub issues
PD_NA: Any = getattr(pd, "NA", np.nan)
pd_concat: Any = getattr(pd, "concat", None)
pd_isna: Any = getattr(pd, "isna", None)
pd_to_numeric: Any = getattr(pd, "to_numeric", None)

logger = UnifiedLogger.get(__name__)


def _normalise_value(value: Any) -> Any:
    """Convert null-like values to ``pd.NA`` for stable operations."""

    if value is None:
        return PD_NA
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "" or stripped.lower() == "nan":
            return PD_NA
        return stripped
    # Явная проверка для float и np.floating, чтобы избежать частично неизвестного типа
    if isinstance(value, float):
        if np.isnan(value):
            return PD_NA
        return value
    if isinstance(value, np.floating):
        if np.isnan(cast(float, value)):
            return PD_NA
        # Приводим np.floating к float для устранения частично неизвестного типа
        return cast(float, value)
    return value


def split_accession_field(value: Any) -> list[str]:
    """Split semicolon-separated accession field values into a sorted list."""

    if value is None or value is PD_NA or pd_isna(value):
        return []
    if not isinstance(value, str):
        return []

    stripped = value.strip()
    if not stripped or stripped.lower() == "nan":
        return []

    parts = [p.strip() for p in stripped.split(";")]
    unique = sorted({p for p in parts if p})
    return unique




def coalesce_by_priority(
    df: pd.DataFrame,
    mapping: Mapping[str, Sequence[str | tuple[str, str]]],
    *,
    source_suffix: str = "_source",
) -> pd.DataFrame:
    """Populate output columns using values from multiple sources.

    The ``mapping`` argument describes priority for every resulting column.  Each
    value can either be a column name (``"pref_name_chembl"``) or a
    ``("column", "label")`` tuple.  ``label`` is stored alongside the resolved
    value helping downstream consumers understand the provenance of the data.
    """

    result = df.copy()

    for output_column, candidates in mapping.items():
        if not candidates:
            result[output_column] = PD_NA
            if source_suffix:
                result[f"{output_column}{source_suffix}"] = PD_NA
            continue

        column_names: list[str] = []
        column_sources: list[str] = []
        for candidate in candidates:
            if isinstance(candidate, tuple):
                column_name_str, label = candidate
                column_name = str(column_name_str)
            else:
                column_name, label = str(candidate), str(candidate)

            if column_name not in result.columns:
                result[column_name] = PD_NA
            column_names.append(column_name)
            column_sources.append(label)

        def normalize_column(col: Any) -> Any:
            """Normalize a single column."""
            return col.map(_normalise_value)

        candidate_frame = result[column_names].apply(normalize_column)
        if not candidate_frame.empty:
            merged = candidate_frame.bfill(axis=1).iloc[:, 0]
        else:
            merged = pd.Series(PD_NA, index=result.index)
        result[output_column] = merged

        if source_suffix:
            def resolve_source(row: Any, sources: list[str] = column_sources) -> Any:
                for idx, value in enumerate(row):
                    if value is not PD_NA and not pd_isna(value):
                        return sources[idx]
                return PD_NA

            result[f"{output_column}{source_suffix}"] = candidate_frame.apply(
                resolve_source, axis=1
            )

    return result


def _component_id_generator(existing: Any) -> Iterator[Any]:
    """Yield deterministic component identifiers for newly created rows."""

    used: set[str] = {
        str(value)
        for value in existing.dropna().astype(str).tolist()
        if str(value) not in {"", "<NA>"}
    }

    numeric = pd_to_numeric(existing, errors="coerce")
    if numeric.notna().any():
        start = int(numeric.max()) + 1

        def generator() -> Iterator[Any]:
            nonlocal start
            while True:
                candidate = start
                start += 1
                if str(candidate) in used:
                    continue
                used.add(str(candidate))
                yield candidate

    else:
        counter = 1

        def generator() -> Iterator[Any]:
            nonlocal counter
            while True:
                candidate = f"auto_{counter}"
                counter += 1
                if candidate in used:
                    continue
                used.add(candidate)
                yield candidate

    return generator()


def _resolve_target_lookup(
    chembl_components: pd.DataFrame,
    targets: pd.DataFrame | None,
    canonical_column: str,
) -> dict[str, list[str]]:
    """Build a mapping of canonical accession to owning targets."""

    lookup: dict[str, list[str]] = {}

    def aggregate_targets(items: Any) -> list[str]:
        """Aggregate target IDs from grouped items."""
        return sorted({str(value) for value in items if value not in {PD_NA, None}})

    if not chembl_components.empty and canonical_column in chembl_components.columns:
        grouped = (
            chembl_components.dropna(subset=[canonical_column, "target_chembl_id"])
            .groupby(canonical_column)["target_chembl_id"]
            .agg(aggregate_targets)
        )
        lookup.update(grouped.to_dict())

    if targets is not None and not targets.empty:
        candidate_columns = [
            canonical_column,
            "uniprot_canonical_accession",
            "uniprot_accession",
            "primaryAccession",
        ]
        canonical_candidates = [col for col in candidate_columns if col in targets.columns]
        if canonical_candidates:
            canonical_col = canonical_candidates[0]
            grouped = (
                targets.dropna(subset=[canonical_col, "target_chembl_id"])
                .groupby(canonical_col)["target_chembl_id"]
                .agg(aggregate_targets)
            )
            for key, values in grouped.to_dict().items():
                existing = lookup.setdefault(str(key), [])
                for value in values:
                    if value not in existing:
                        existing.append(value)

    return lookup


@dataclass
class MergeComponentsConfig:
    """Configuration for :func:`merge_components`."""

    accession_column: str = "accession"
    canonical_column: str = "canonical_accession"
    isoform_column: str = "isoform_accession"
    name_column: str = "isoform_name"
    length_column: str = "sequence_length"
    source_column: str = "data_origin"
    target_column: str = "target_chembl_id"
    component_id_column: str = "component_id"


def merge_components(
    chembl_components: pd.DataFrame,
    enrichment_components: pd.DataFrame | None = None,
    *,
    targets: pd.DataFrame | None = None,
    config: MergeComponentsConfig | None = None,
) -> pd.DataFrame:
    """Combine component payloads originating from different sources."""

    cfg = config or MergeComponentsConfig()

    chembl_df = chembl_components.copy()
    if chembl_df.empty:
        chembl_df = pd.DataFrame(
            columns=[cfg.target_column, cfg.component_id_column, cfg.accession_column]
        )

    for column, default in (
        (cfg.component_id_column, PD_NA),
        (cfg.accession_column, PD_NA),
        (cfg.canonical_column, PD_NA),
        (cfg.isoform_column, PD_NA),
        (cfg.name_column, PD_NA),
        (cfg.length_column, PD_NA),
        (cfg.source_column, "chembl"),
        ("is_canonical", True),
    ):
        if column not in chembl_df.columns:
            chembl_df[column] = default

    chembl_df[cfg.canonical_column] = chembl_df[cfg.canonical_column].fillna(
        chembl_df[cfg.accession_column]
    )
    chembl_df[cfg.isoform_column] = chembl_df[cfg.isoform_column].fillna(
        chembl_df[cfg.accession_column]
    )
    chembl_df["is_canonical"] = chembl_df["is_canonical"].fillna(True)
    chembl_df[cfg.source_column] = chembl_df[cfg.source_column].fillna("chembl")

    lookup = _resolve_target_lookup(chembl_df, targets, cfg.canonical_column)
    new_rows: list[dict[str, Any]] = []
    id_generator = _component_id_generator(chembl_df[cfg.component_id_column])

    if enrichment_components is not None and not enrichment_components.empty:
        enrichment = enrichment_components.copy()
        for column, default in (
            (cfg.canonical_column, PD_NA),
            (cfg.isoform_column, PD_NA),
            (cfg.name_column, PD_NA),
            (cfg.length_column, PD_NA),
            ("is_canonical", PD_NA),
            ("source", "uniprot"),
        ):
            if column not in enrichment.columns:
                enrichment[column] = default

        enrichment["source"] = enrichment["source"].fillna("uniprot")
        enrichment["is_canonical"] = enrichment["is_canonical"].fillna(
            enrichment[cfg.isoform_column] == enrichment[cfg.canonical_column]
        )

        for canonical, group in enrichment.groupby(cfg.canonical_column):
            if pd_isna(canonical):
                continue
            canonical_key = str(canonical)
            target_ids = lookup.get(canonical_key, [])
            if not target_ids:
                logger.debug("merge_components_missing_target", canonical=canonical_key)
                continue

            for target_id in target_ids:
                canonical_mask = (
                    chembl_df[cfg.target_column].astype(str) == str(target_id)
                ) & (chembl_df[cfg.canonical_column].astype(str) == canonical_key)

                if not canonical_mask.any():
                    new_canonical = {
                        cfg.target_column: target_id,
                        cfg.component_id_column: next(id_generator),
                        cfg.accession_column: canonical,
                        cfg.canonical_column: canonical,
                        cfg.isoform_column: canonical,
                        cfg.name_column: PD_NA,
                        cfg.length_column: PD_NA,
                        cfg.source_column: "uniprot",
                        "is_canonical": True,
                    }
                    chembl_df = pd_concat(
                        [chembl_df, pd.DataFrame([new_canonical])],
                        ignore_index=True,
                    )
                    canonical_mask = (
                        chembl_df[cfg.target_column].astype(str) == str(target_id)
                    ) & (chembl_df[cfg.canonical_column].astype(str) == canonical_key)

                for _, iso_row in group.iterrows():
                    isoform_accession = iso_row.get(cfg.isoform_column)
                    if pd_isna(isoform_accession):
                        isoform_accession = canonical
                    isoform_accession = _normalise_value(isoform_accession)

                    is_canonical = bool(iso_row.get("is_canonical", False))
                    isoform_name = _normalise_value(iso_row.get(cfg.name_column))
                    seq_length = _normalise_value(iso_row.get(cfg.length_column))
                    iso_source = _normalise_value(iso_row.get("source")) or "uniprot"

                    if is_canonical:
                        chembl_df.loc[canonical_mask, cfg.length_column] = (
                            chembl_df.loc[canonical_mask, cfg.length_column]
                            .where(chembl_df.loc[canonical_mask, cfg.length_column].notna())
                            .fillna(seq_length)
                        )
                        chembl_df.loc[canonical_mask, cfg.name_column] = (
                            chembl_df.loc[canonical_mask, cfg.name_column]
                            .where(chembl_df.loc[canonical_mask, cfg.name_column].notna())
                            .fillna(isoform_name)
                        )
                        chembl_df.loc[canonical_mask, cfg.source_column] = (
                            chembl_df.loc[canonical_mask, cfg.source_column]
                            .where(chembl_df.loc[canonical_mask, cfg.source_column].notna())
                            .fillna(iso_source)
                        )
                        continue

                    template_rows = chembl_df.loc[canonical_mask]
                    if template_rows.empty:
                        continue

                    template = template_rows.iloc[0].to_dict()
                    template[cfg.component_id_column] = next(id_generator)
                    template[cfg.accession_column] = isoform_accession
                    template[cfg.isoform_column] = isoform_accession
                    template[cfg.name_column] = isoform_name
                    template[cfg.length_column] = seq_length
                    template[cfg.source_column] = iso_source
                    template["is_canonical"] = False
                    new_rows.append(template)

    if new_rows:
        chembl_df = pd_concat([chembl_df, pd.DataFrame(new_rows)], ignore_index=True)

    chembl_df = chembl_df.convert_dtypes()
    return chembl_df


def expand_xrefs(
    targets_df: pd.DataFrame,
    *,
    column: str = "component_xrefs",
    target_column: str = "target_chembl_id",
) -> pd.DataFrame:
    """Expand a nested xref collection attached to target rows."""

    records: list[dict[str, Any]] = []

    if column not in targets_df.columns:
        return pd.DataFrame(columns=[target_column, "xref_src_db", "xref_id", "component_id"])

    for row in targets_df.itertuples(index=False):
        target_id = getattr(row, target_column, None)
        raw = getattr(row, column, None)
        if raw is None or raw is PD_NA:
            continue
        if isinstance(raw, str) and raw.strip() in {"", "[]"}:
            continue
        if isinstance(raw, list | tuple) and not raw:
            continue

        if isinstance(raw, str):
            try:
                payload: Any = json.loads(raw)
            except json.JSONDecodeError:
                logger.debug("expand_xrefs_invalid_json", target=target_id, value=raw)
                continue
        else:
            payload: Any = raw

        if isinstance(payload, dict):
            payload = [payload]

        if not isinstance(payload, list):
            continue

        typed_payload: list[Any] = cast(list[Any], payload)
        for item in typed_payload:
            if not isinstance(item, dict):
                continue
            record = {
                target_column: target_id,
                "xref_src_db": _normalise_value(item.get("xref_src_db") or item.get("xref_src")),
                "xref_id": _normalise_value(item.get("xref_id") or item.get("xref_acc")),
                "component_id": _normalise_value(item.get("component_id")),
            }
            if record["xref_src_db"] is PD_NA or record["xref_id"] is PD_NA:
                continue
            records.append(record)

    if not records:
        return pd.DataFrame(columns=[target_column, "xref_src_db", "xref_id", "component_id"])

    return pd.DataFrame(records).convert_dtypes()


def annotate_source_rank(
    df: pd.DataFrame,
    *,
    source_column: str = "data_origin",
    output_column: str = "merge_rank",
    priority: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Assign a deterministic rank based on the origin of the record."""

    if priority is None:
        priority = ("chembl", "uniprot", "iuphar", "ortholog", "fallback")

    ranking = {label: idx for idx, label in enumerate(priority)}
    default_rank = len(ranking)

    df = df.copy()
    df[output_column] = (
        df.get(source_column)
        .map(lambda value: ranking.get(str(value).lower(), default_rank) if value is not None else default_rank)
        .astype("Int64")
    )
    return df


def materialize_gold(
    output_path: Path,
    *,
    targets: pd.DataFrame,
    components: pd.DataFrame,
    protein_class: pd.DataFrame,
    xref: pd.DataFrame,
    format: str = "parquet",
) -> dict[str, Path]:
    """Persist deterministic gold-layer artefacts."""

    manager = MaterializationManager(
        MaterializationPaths.model_validate(
            {
                "root": output_path.parent,
                "stages": {
                    "gold": {
                        "directory": ".",
                        "datasets": {
                            "targets": {"path": output_path},
                            "target_components": {"directory": ".", "filename": "target_components"},
                            "protein_class": {"directory": ".", "filename": "protein_class"},
                            "target_xref": {"directory": ".", "filename": "target_xref"},
                        },
                    }
                },
            }
        ),
        runtime=None,
        stage_context=None,
    )
    return manager.materialize_gold(
        targets,
        components,
        protein_class,
        xref,
        format=format,
        output_path=output_path,
    )

