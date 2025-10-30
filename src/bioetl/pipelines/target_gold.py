"""Utilities for assembling the target gold layer outputs."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame, Series, concat, isna, to_numeric
from pandas import NA as PD_NA

from bioetl.core.logger import UnifiedLogger

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
    if isinstance(value, float | np.floating) and np.isnan(value):
        return PD_NA
    return value


def _split_accession_field(value: Any) -> list[str]:
    """Normalise accession strings to a list of unique identifiers."""

    if value is None or value is PD_NA:
        return []

    if isinstance(value, str):
        tokens = value.replace(";", " ").replace(",", " ").split()
        return [token.strip() for token in tokens if token.strip()]

    if isinstance(value, list | tuple | set):
        return [str(item).strip() for item in value if item not in {None, "", PD_NA}]

    return [str(value).strip()]


def coalesce_by_priority(
    df: DataFrame,
    mapping: Mapping[str, Sequence[str | tuple[str, str]]],
    *,
    source_suffix: str = "_source",
) -> DataFrame:
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
            if source_suffix is not None:
                result[f"{output_column}{source_suffix}"] = PD_NA
            continue

        column_names: list[str] = []
        column_sources: list[str] = []
        for candidate in candidates:
            if isinstance(candidate, tuple):
                column_name, label = candidate
            else:
                column_name, label = candidate, str(candidate)

            if column_name not in result.columns:
                result[column_name] = PD_NA
            column_names.append(column_name)
            column_sources.append(label)

        candidate_frame = result[column_names].apply(lambda col: col.map(_normalise_value))
        if not candidate_frame.empty:
            merged = candidate_frame.bfill(axis=1).iloc[:, 0]
        else:
            merged = Series(PD_NA, index=result.index)
        result[output_column] = merged

        if source_suffix is not None:
            def resolve_source(row: Series, sources: list[str] = column_sources) -> Any:
                for idx, value in enumerate(row):
                    if value is not PD_NA and not isna(value):
                        return sources[idx]
                return PD_NA

            result[f"{output_column}{source_suffix}"] = candidate_frame.apply(
                resolve_source, axis=1
            )

    return result


def _component_id_generator(existing: Series) -> Iterator[Any]:
    """Yield deterministic component identifiers for newly created rows."""

    used: set[str] = {
        str(value)
        for value in existing.dropna().astype(str).tolist()
        if str(value) not in {"", "<NA>"}
    }

    numeric = to_numeric(existing, errors="coerce")
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
    chembl_components: DataFrame,
    targets: DataFrame | None,
    canonical_column: str,
) -> dict[str, list[str]]:
    """Build a mapping of canonical accession to owning targets."""

    lookup: dict[str, list[str]] = {}

    if not chembl_components.empty and canonical_column in chembl_components.columns:
        grouped = (
            chembl_components.dropna(subset=[canonical_column, "target_chembl_id"])
            .groupby(canonical_column)["target_chembl_id"]
            .agg(lambda items: sorted({str(value) for value in items if value not in {PD_NA, None}}))
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
                .agg(lambda items: sorted({str(value) for value in items if value not in {PD_NA, None}}))
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
    chembl_components: DataFrame,
    enrichment_components: DataFrame | None = None,
    *,
    targets: DataFrame | None = None,
    config: MergeComponentsConfig | None = None,
) -> DataFrame:
    """Combine component payloads originating from different sources."""

    cfg = config or MergeComponentsConfig()

    chembl_df = chembl_components.copy()
    if chembl_df.empty:
        chembl_df = DataFrame(
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
            if isna(canonical):
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
                    chembl_df = concat(
                        [chembl_df, DataFrame([new_canonical])],
                        ignore_index=True,
                    )
                    canonical_mask = (
                        chembl_df[cfg.target_column].astype(str) == str(target_id)
                    ) & (chembl_df[cfg.canonical_column].astype(str) == canonical_key)

                for _, iso_row in group.iterrows():
                    isoform_accession = iso_row.get(cfg.isoform_column)
                    if isna(isoform_accession):
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
        chembl_df = concat([chembl_df, DataFrame(new_rows)], ignore_index=True)

    chembl_df = chembl_df.convert_dtypes()
    return chembl_df


def expand_xrefs(
    targets_df: DataFrame,
    *,
    column: str = "component_xrefs",
    target_column: str = "target_chembl_id",
) -> DataFrame:
    """Expand a nested xref collection attached to target rows."""

    records: list[dict[str, Any]] = []

    if column not in targets_df.columns:
        return DataFrame(columns=[target_column, "xref_src_db", "xref_id", "component_id"])

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
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.debug("expand_xrefs_invalid_json", target=target_id, value=raw)
                continue
        else:
            payload = raw

        if isinstance(payload, dict):
            payload = [payload]

        if not isinstance(payload, list):
            continue

        for item in payload:
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
        return DataFrame(columns=[target_column, "xref_src_db", "xref_id", "component_id"])

    return DataFrame(records).convert_dtypes()


def annotate_source_rank(
    df: DataFrame,
    *,
    source_column: str = "data_origin",
    output_column: str = "merge_rank",
    priority: Sequence[str] | None = None,
) -> DataFrame:
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
    targets: DataFrame,
    components: DataFrame,
    protein_class: DataFrame,
    xref: DataFrame,
    format: str = "parquet",
) -> dict[str, Path]:
    """Persist deterministic gold-layer artefacts."""

    output_path = Path(output_path)
    if output_path.suffix:
        base_dir = output_path.parent
        targets_path = output_path
    else:
        base_dir = output_path
        targets_path = base_dir / "targets.parquet"

    base_dir.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, Path] = {}

    def _write(df: DataFrame, path: Path, name: str) -> None:
        if df.empty:
            logger.info("gold_materialization_skipped", dataset=name, reason="empty_dataframe")
            return
        df = df.copy()
        if format == "parquet":
            df.to_parquet(path, index=False)
        elif format == "csv":
            df.to_csv(path.with_suffix(".csv"), index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        outputs[name] = path

    _write(targets, targets_path, "targets")
    _write(components, base_dir / "target_components.parquet", "target_components")
    _write(protein_class, base_dir / "protein_class.parquet", "protein_class")
    _write(xref, base_dir / "target_xref.parquet", "target_xref")

    return outputs

