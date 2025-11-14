"""Enrichment functions for Activity pipeline."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, TypeVar, cast

import numpy as np
import pandas as pd

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.core.io import ensure_columns
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.schemas.chembl_activity_enrichment import (
    ASSAY_ENRICHMENT_SCHEMA,
    COMPOUND_RECORD_ENRICHMENT_SCHEMA,
    DATA_VALIDITY_ENRICHMENT_SCHEMA,
)

__all__ = ["enrich_with_assay", "enrich_with_compound_record", "enrich_with_data_validity"]


_ensure_columns = ensure_columns


_COMPOUND_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "compound_name": ("compound_name", "pref_name", "PREF_NAME"),
    "compound_key": (
        "compound_key",
        "standard_inchi_key",
        "STANDARD_INCHI_KEY",
    ),
    "curated": ("curated", "CURATED"),
}

_ASSAY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("assay_organism", "string"),
    ("assay_tax_id", "Int64"),
)

_COMPOUND_COLUMNS: tuple[tuple[str, str], ...] = (
    ("compound_name", "string"),
    ("compound_key", "string"),
    ("curated", "boolean"),
    ("removed", "boolean"),
)

_DATA_VALIDITY_COLUMNS: tuple[tuple[str, str], ...] = (("data_validity_description", "string"),)


_NormalizedValue = TypeVar("_NormalizedValue", str | None, bool | None)


def _build_series_from_aliases(
    records: Sequence[Mapping[str, Any]],
    index: pd.Index,
    field_key: str,
    normalizer: Callable[[Any], _NormalizedValue],
    dtype: str,
) -> pd.Series:
    """Construct a normalized pandas Series for the requested compound record field."""
    aliases = _COMPOUND_FIELD_ALIASES.get(field_key, (field_key,))
    normalized_values = [
        normalizer(_extract_first_present(record, aliases)) for record in records
    ]
    return pd.Series(data=normalized_values, index=index, dtype=dtype)


def _extract_first_present(record: Mapping[str, Any], keys: Iterable[str]) -> Any:
    """Return the first available alias value from the record."""

    for key in keys:
        if key in record:
            return record[key]

    lowered_map = {str(k).lower(): v for k, v in record.items()}
    for key in keys:
        candidate = str(key).lower()
        if candidate in lowered_map:
            return lowered_map[candidate]
    return None


def _normalize_optional_string(value: Any) -> str | None:
    """Normalize optional string values by stripping whitespace and empty strings."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_bool_flag(value: Any) -> bool | None:
    """Coerce heterogeneous boolean representations into ``True``/``False``."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"true", "1", "yes", "y", "t", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "f", "off"}:
            return False
    return bool(value)


def enrich_with_assay(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the activity DataFrame with /assay fields (ChEMBL v2).

    Required input column: `assay_chembl_id`.
    Adds:
      - `assay_organism` : pandas.StringDtype (nullable)
      - `assay_tax_id`   : pandas.Int64Dtype  (nullable)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    df_act = _ensure_columns(df_act, _ASSAY_COLUMNS)

    if df_act.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return ASSAY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # 1) Validate that the key column is available.
    if "assay_chembl_id" not in df_act.columns:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS, missing_columns=["assay_chembl_id"])
        return ASSAY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # 2) Collect unique valid identifiers (vectorized approach).
    assay_ids = df_act["assay_chembl_id"].dropna().astype(str).str.strip()
    assay_ids = assay_ids[assay_ids.ne("")].unique().tolist()

    # Guarantee output columns even when the identifier set is empty.
    if not assay_ids:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_IDS)
        return ASSAY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # 3) Configuration step (explicitly pin valid ChEMBL /assay field names).
    #    Note: In the ChEMBL JSON payload the field is 'assay_organism', not 'organism'.
    fields_cfg = cfg.get(
        "fields",
        ["assay_chembl_id", "assay_organism", "assay_tax_id"],
    )
    # Ensure that mandatory fields are always present.
    required_fields = {"assay_chembl_id", "assay_organism", "assay_tax_id"}
    fields = list(dict.fromkeys(list(fields_cfg) + list(required_fields)))

    page_limit = int(cfg.get("page_limit", 1000))

    # 4) Execute client call.
    # The client is expected to return a DataFrame with the requested fields.
    log.info(LogEvents.ENRICHMENT_FETCHING_ASSAYS, ids_count=len(assay_ids))
    records_df = client.fetch_assays_by_ids(
        ids=assay_ids,
        fields=fields,
        page_limit=page_limit,
    )

    if isinstance(records_df, Mapping):
        hydrated_rows: list[dict[str, Any]] = []
        for payload in records_df.values():
            if isinstance(payload, Mapping):
                hydrated_rows.append(dict(payload))
        records_df = pd.DataFrame.from_records(hydrated_rows)

    # 5) Build enrichment table limited to the required output fields.
    if records_df.empty:
        log.debug(LogEvents.ENRICHMENT_NO_RECORDS_FOUND)
        return ASSAY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    df_enrich = (
        records_df.loc[:, ["assay_chembl_id", "assay_organism", "assay_tax_id"]]
        .dropna(subset=["assay_chembl_id"])
        .copy()
    )
    df_enrich["assay_chembl_id"] = (
        df_enrich["assay_chembl_id"].astype("string").str.strip().str.upper()
    )
    df_enrich = (
        df_enrich.drop_duplicates(subset=["assay_chembl_id"], keep="first")
        .sort_values(by=["assay_chembl_id"], kind="mergesort")
        .reset_index(drop=True)
    )

    # 6) Execute left join preserving the input row order.
    original_index = df_act.index
    df_merged = df_act.merge(
        df_enrich,
        on="assay_chembl_id",
        how="left",
        sort=False,
        suffixes=("", "_enrich"),
    ).reindex(original_index)

    # 7) Perform soft type coercion.
    if "assay_organism_enrich" in df_merged.columns:
        df_merged["assay_organism"] = df_merged["assay_organism_enrich"].combine_first(
            df_merged["assay_organism"]
        )
        df_merged = df_merged.drop(columns=["assay_organism_enrich"])
    if "assay_tax_id_enrich" in df_merged.columns:
        df_merged["assay_tax_id"] = df_merged["assay_tax_id_enrich"].combine_first(
            df_merged["assay_tax_id"]
        )
        df_merged = df_merged.drop(columns=["assay_tax_id_enrich"])

    if "assay_organism" not in df_merged.columns:
        df_merged["assay_organism"] = pd.NA
    if "assay_tax_id" not in df_merged.columns:
        df_merged["assay_tax_id"] = pd.NA

    df_merged["assay_organism"] = df_merged["assay_organism"].astype("string")

    # assay_tax_id may arrive as a string — coerce to Int64 with NA support.
    df_merged["assay_tax_id"] = pd.to_numeric(  # pyright: ignore[reportUnknownMemberType]
        df_merged["assay_tax_id"], errors="coerce"
    ).astype("Int64")

    log.info(LogEvents.ENRICHMENT_COMPLETED,
        rows_enriched=int(df_merged.shape[0]),
        records_matched=int(df_enrich.shape[0]),
    )
    return ASSAY_ENRICHMENT_SCHEMA.validate(df_merged, lazy=True)


def enrich_with_compound_record(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the activity DataFrame with compound_record fields.

    Parameters
    ----------
    df_act:
        Activity DataFrame; must contain `molecule_chembl_id`. For rows with
        `document_chembl_id`, enrichment uses (molecule_chembl_id, document_chembl_id) pairs.
        Rows without `document_chembl_id` fall back to `record_id`.
    client:
        ChemblClient used for ChEMBL API requests.
    cfg:
        Enrichment configuration from `config.chembl.activity.enrich.compound_record`.

    Returns
    -------
    pd.DataFrame
        Enriched DataFrame with columns:
        - `compound_name` (nullable string) from `compound_record.compound_name`
        - `compound_key` (nullable string) from `compound_record.compound_key`
        - `curated` (nullable bool) from `compound_record.curated`
        - `removed` (nullable bool) always NULL (not supplied by ChEMBL)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    df_act = _ensure_columns(df_act, _COMPOUND_COLUMNS)

    if df_act.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return COMPOUND_RECORD_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # Verify that required columns are present.
    if "molecule_chembl_id" not in df_act.columns:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=["molecule_chembl_id"],
        )
        return COMPOUND_RECORD_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    df_act = df_act.copy()

    # 1) Preserve input ordering: add temporary column _row_id.
    df_act["_row_id"] = np.arange(len(df_act))

    # 2) Split the DataFrame into rows with and without document_chembl_id.
    has_doc_id = "document_chembl_id" in df_act.columns
    if has_doc_id:
        mask_with_doc = df_act["document_chembl_id"].notna() & (
            df_act["document_chembl_id"].astype("string").str.strip() != ""
        )
        df_with_doc = df_act[mask_with_doc].copy()
        df_without_doc = df_act[~mask_with_doc].copy()
    else:
        df_with_doc = pd.DataFrame()
        df_without_doc = df_act.copy()

    # 3) Enrich rows with document_chembl_id using pairs.
    enrichment_by_pairs: pd.DataFrame | None = None
    if not df_with_doc.empty:
        enrichment_by_pairs = _enrich_by_pairs(df_with_doc, client, cfg, log)

    # 4) Determine rows requiring record_id fallback:
    #    - rows without document_chembl_id
    #    - rows with document_chembl_id but without pair enrichment results
    df_need_fallback = df_without_doc.copy()
    if enrichment_by_pairs is not None and not enrichment_by_pairs.empty:
        # Check pair-enriched rows with empty compound_name/compound_key.
        if {"compound_name", "compound_key"}.issubset(enrichment_by_pairs.columns):
            mask_empty = (
                enrichment_by_pairs["compound_name"].isna()
                | (enrichment_by_pairs["compound_name"].astype("string").str.strip() == "")
            ) & (
                enrichment_by_pairs["compound_key"].isna()
                | (enrichment_by_pairs["compound_key"].astype("string").str.strip() == "")
            )
        else:
            mask_empty = pd.Series(False, index=enrichment_by_pairs.index)
        rows_need_fallback = enrichment_by_pairs[mask_empty].copy()
        if not rows_need_fallback.empty and "record_id" in rows_need_fallback.columns:
            # Add these rows to df_need_fallback for fallback processing.
            df_need_fallback = pd.concat([df_need_fallback, rows_need_fallback], ignore_index=True)

    # 5) Fallback enrichment via record_id for the remaining rows.
    enrichment_by_record_id: pd.DataFrame | None = None
    if not df_need_fallback.empty and "record_id" in df_need_fallback.columns:
        # Drop duplicate _row_id entries if present.
        df_need_fallback = df_need_fallback.drop_duplicates(subset=["_row_id"], keep="first")
        enrichment_by_record_id = _enrich_by_record_id(df_need_fallback, client, cfg, log)

    # 6) Merge results with priority: pair data overrides fallback data.
    # Start from the original DataFrame and apply enrichments sequentially.
    df_result = df_act.copy()

    # Apply pair enrichment for rows containing document_chembl_id.
    if enrichment_by_pairs is not None and not enrichment_by_pairs.empty:
        # Build a mapping of pair enrichment data indexed by _row_id.
        pairs_dict: dict[int, dict[str, Any]] = {}
        for _, row in enrichment_by_pairs.iterrows():
            row_id_raw = row.get("_row_id")
            if not pd.isna(row_id_raw):
                try:
                    # Safe conversion to int.
                    if isinstance(row_id_raw, (int, float)):
                        row_id = int(row_id_raw)
                    else:
                        row_id = int(str(row_id_raw))
                    pairs_dict[row_id] = {
                        "compound_name": row.get("compound_name"),
                        "compound_key": row.get("compound_key"),
                        "curated": row.get("curated"),
                    }
                except (ValueError, TypeError):
                    continue

        # Apply pair enrichment data.
        if "_row_id" in df_result.columns:
            for idx in df_result.index:
                row_id_raw = df_result.loc[idx, "_row_id"]
                if not pd.isna(row_id_raw):
                    try:
                        # Safe conversion to int.
                        if isinstance(row_id_raw, (int, float)):
                            row_id = int(row_id_raw)
                        else:
                            row_id = int(str(row_id_raw))
                        if row_id in pairs_dict:
                            pairs_data = pairs_dict[row_id]
                            if pairs_data.get("compound_name") is not None:
                                df_result.loc[idx, "compound_name"] = pairs_data["compound_name"]
                            if pairs_data.get("compound_key") is not None:
                                df_result.loc[idx, "compound_key"] = pairs_data["compound_key"]
                            if pairs_data.get("curated") is not None:
                                df_result.loc[idx, "curated"] = pairs_data["curated"]
                    except (ValueError, TypeError):
                        continue

    # Restore original ordering by _row_id.
    if "_row_id" in df_result.columns:
        df_result = df_result.sort_values("_row_id").reset_index(drop=True)

    # Apply fallback data only for rows with empty compound_name/compound_key.
    if enrichment_by_record_id is not None and not enrichment_by_record_id.empty:
        # Build fallback data mapping by _row_id.
        fallback_dict: dict[int, dict[str, Any]] = {}
        for _, row in enrichment_by_record_id.iterrows():
            row_id_raw = row.get("_row_id")
            if not pd.isna(row_id_raw):
                try:
                    # Safe conversion to int.
                    if isinstance(row_id_raw, (int, float)):
                        row_id = int(row_id_raw)
                    else:
                        row_id = int(str(row_id_raw))
                    fallback_dict[row_id] = {
                        "compound_name": row.get("compound_name"),
                        "compound_key": row.get("compound_key"),
                    }
                except (ValueError, TypeError):
                    continue

        # Apply fallback data solely where compound_name/compound_key remain empty.
        if "_row_id" in df_result.columns:
            for idx in df_result.index:
                row_id_raw = df_result.loc[idx, "_row_id"]
                if not pd.isna(row_id_raw):
                    try:
                        # Safe conversion to int.
                        if isinstance(row_id_raw, (int, float)):
                            row_id = int(row_id_raw)
                        else:
                            row_id = int(str(row_id_raw))
                        if row_id in fallback_dict:
                            # Check whether fallback is needed (compound_name/compound_key empty).
                            compound_name = (
                                df_result.loc[idx, "compound_name"]
                                if "compound_name" in df_result.columns
                                else pd.NA
                            )
                            compound_key = (
                                df_result.loc[idx, "compound_key"]
                                if "compound_key" in df_result.columns
                                else pd.NA
                            )

                            name_empty = pd.isna(compound_name) or (
                                str(compound_name).strip() == ""
                            )
                            key_empty = pd.isna(compound_key) or (str(compound_key).strip() == "")

                            if name_empty or key_empty:
                                fallback_data = fallback_dict[row_id]
                                if name_empty and fallback_data.get("compound_name") is not None:
                                    df_result.loc[idx, "compound_name"] = fallback_data[
                                        "compound_name"
                                    ]
                                if key_empty and fallback_data.get("compound_key") is not None:
                                    df_result.loc[idx, "compound_key"] = fallback_data[
                                        "compound_key"
                                    ]
                    except (ValueError, TypeError):
                        continue

    # 6) Ensure all new columns exist (fill missing ones with NA).
    for col in ["compound_name", "compound_key", "curated", "removed"]:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    # 7) removed is always <NA> at this stage.
    df_result["removed"] = pd.NA

    # 8) Drop the temporary _row_id column.
    if "_row_id" in df_result.columns:
        df_result = df_result.drop(columns=["_row_id"])

    # 9) Robust boolean coercion for curated.
    if "curated" in df_result.columns:
        # Normalize possible representations before casting to boolean.
        curated_mapping: Mapping[object, bool] = {
            1: True,
            0: False,
            "1": True,
            "0": False,
            "true": True,
            "false": False,
            "True": True,
            "False": False,
        }
        curated_series = df_result["curated"]
        normalized_curated = curated_series.map(curated_mapping)
        mapped_mask = normalized_curated.notna()
        if mapped_mask.any():
            df_result.loc[mapped_mask, "curated"] = normalized_curated[mapped_mask]
        df_result["curated"] = df_result["curated"].astype("boolean")

    # 10) Normalize string field types.
    df_result["compound_name"] = df_result["compound_name"].astype("string")
    df_result["compound_key"] = df_result["compound_key"].astype("string")
    df_result["removed"] = df_result["removed"].astype("boolean")

    log.info(LogEvents.ENRICHMENT_COMPLETED,
        rows_enriched=df_result.shape[0],
        rows_with_doc_id=len(df_with_doc) if not df_with_doc.empty else 0,
        rows_without_doc_id=len(df_without_doc) if not df_without_doc.empty else 0,
    )
    return COMPOUND_RECORD_ENRICHMENT_SCHEMA.validate(df_result, lazy=True)


def _enrich_by_pairs(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Enrich the activity DataFrame using (molecule_chembl_id, document_chembl_id) pairs."""
    # Normalize keys before building pairs: upper and strip.
    pairs_df = df_act[["molecule_chembl_id", "document_chembl_id"]].astype("string").copy()
    for column in pairs_df.columns:
        pairs_df[column] = pairs_df[column].str.strip().str.upper()
    pairs_df = pairs_df.dropna()  # pyright: ignore[reportUnknownMemberType]
    pairs_df = pairs_df.drop_duplicates()
    pairs: set[tuple[str, str]] = set(map(tuple, pairs_df.to_numpy()))

    if not pairs:
        log.debug(LogEvents.ENRICHMENT_BY_PAIRS_SKIPPED_NO_VALID_PAIRS)
        return df_act

    # Request fields are flat names emitted by the ChEMBL API.
    fields = cfg.get(
        "fields",
        [
            "molecule_chembl_id",
            "document_chembl_id",
            "compound_name",
            "compound_key",
            "curated",
        ],
    )
    page_limit = cfg.get("page_limit", 1000)

    # Wrap the client call in try/except to avoid failing the pipeline.
    log.info(LogEvents.ENRICHMENT_FETCHING_COMPOUND_RECORDS_BY_PAIRS, pairs_count=len(pairs))
    try:
        records_df = client.fetch_compound_records_by_pairs(
            pairs=pairs,
            fields=list(fields),
            page_limit=page_limit,
        )
    except Exception as exc:
        log.warning(LogEvents.ENRICHMENT_FETCH_ERROR_BY_PAIRS,
            pairs_count=len(pairs),
            error=str(exc),
            exc_info=True,
        )
        return df_act

    if isinstance(records_df, Mapping):
        hydrated_rows: list[dict[str, Any]] = []
        for key_pair, payload in records_df.items():
            if not isinstance(key_pair, tuple) or len(key_pair) != 2:
                continue
            molecule_id, document_id = key_pair
            row: dict[str, Any] = {}
            if isinstance(payload, Mapping):
                row = dict(payload)
            row.setdefault("molecule_chembl_id", molecule_id)
            row.setdefault("document_chembl_id", document_id)
            hydrated_rows.append(row)
        records_df = pd.DataFrame.from_records(hydrated_rows)

    if records_df.empty:
        log.debug(LogEvents.ENRICHMENT_BY_PAIRS_NO_RECORDS_FOUND)
        return df_act

    records_df = records_df.copy()
    for column in ("molecule_chembl_id", "document_chembl_id"):
        if column not in records_df.columns:
            records_df[column] = pd.Series([pd.NA] * len(records_df), dtype="string")
        records_df[column] = records_df[column].astype("string").str.strip().str.upper()

    records_df = (
        records_df.dropna(subset=["molecule_chembl_id", "document_chembl_id"])
        .drop_duplicates(subset=["molecule_chembl_id", "document_chembl_id"], keep="first")
        .sort_values(by=["document_chembl_id", "molecule_chembl_id"], kind="mergesort")
        .reset_index(drop=True)
    )

    if records_df.empty:
        log.debug(LogEvents.ENRICHMENT_BY_PAIRS_NO_RECORDS_FOUND)
        return df_act

    record_dicts = cast(Sequence[Mapping[str, Any]], records_df.to_dict(orient="records"))
    compound_name_series = _build_series_from_aliases(
        records=record_dicts,
        index=records_df.index,
        field_key="compound_name",
        normalizer=_normalize_optional_string,
        dtype="string",
    )
    compound_key_series = _build_series_from_aliases(
        records=record_dicts,
        index=records_df.index,
        field_key="compound_key",
        normalizer=_normalize_optional_string,
        dtype="string",
    )
    curated_series = _build_series_from_aliases(
        records=record_dicts,
        index=records_df.index,
        field_key="curated",
        normalizer=_coerce_bool_flag,
        dtype="boolean",
    )

    df_enrich = pd.DataFrame(
        {
            "molecule_chembl_id": records_df["molecule_chembl_id"],
            "document_chembl_id": records_df["document_chembl_id"],
            "compound_name": compound_name_series,
            "compound_key": compound_key_series,
            "curated": curated_series,
            "removed": pd.Series([pd.NA] * len(records_df), dtype="boolean"),
        }
    )

    pairs_found = int(
        df_enrich[["molecule_chembl_id", "document_chembl_id"]]
        .drop_duplicates()
        .shape[0]
    )
    pairs_not_found = max(len(pairs) - pairs_found, 0)

    log.info(LogEvents.ENRICHMENT_BY_PAIRS_COMPLETE,
        pairs_requested=len(pairs),
        pairs_found=pairs_found,
        pairs_not_found=pairs_not_found,
        records_returned=len(df_enrich),
    )

    if pairs_not_found > 0:
        log.warning(LogEvents.ENRICHMENT_BY_PAIRS_SOME_PAIRS_NOT_FOUND,
            pairs_not_found=pairs_not_found,
            pairs_total=len(pairs),
            hint="Verify that (molecule_chembl_id, document_chembl_id) pairs exist in the ChEMBL API.",
        )

    # Normalize keys in df_enrich for the join (upper and strip).
    df_enrich["molecule_chembl_id"] = (
        df_enrich["molecule_chembl_id"].astype("string").str.strip().str.upper()
    )
    df_enrich["document_chembl_id"] = (
        df_enrich["document_chembl_id"].astype("string").str.strip().str.upper()
    )

    # Normalize keys in df_act for the join (upper and strip).
    df_act["molecule_chembl_id_normalized"] = (
        df_act["molecule_chembl_id"].astype("string").str.strip().str.upper()
    )
    df_act["document_chembl_id_normalized"] = (
        df_act["document_chembl_id"].astype("string").str.strip().str.upper()
    )

    # Left-join back to df_act on normalized keys.
    df_result = df_act.merge(
        df_enrich,
        left_on=["molecule_chembl_id_normalized", "document_chembl_id_normalized"],
        right_on=["molecule_chembl_id", "document_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Remove temporary normalized columns.
    df_result = df_result.drop(
        columns=["molecule_chembl_id_normalized", "document_chembl_id_normalized"]
    )

    # Coalesce *_enrich columns back into base columns.
    for col in ["compound_name", "compound_key", "curated", "removed"]:
        if f"{col}_enrich" in df_result.columns:
            if col not in df_result.columns:
                df_result[col] = df_result[f"{col}_enrich"]
            else:
                df_result[col] = df_result[col].where(
                    df_result[col].notna(),
                    df_result[f"{col}_enrich"],
                )
            df_result = df_result.drop(columns=[f"{col}_enrich"])
    for key_col in ["molecule_chembl_id_enrich", "document_chembl_id_enrich"]:
        if key_col in df_result.columns:
            df_result = df_result.drop(columns=[key_col])

    return df_result


def _enrich_by_record_id(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Enrich the activity DataFrame by record_id (fallback when document_chembl_id is absent)."""
    # Collect unique record_id values.
    record_ids: set[str] = set()
    for _, row in df_act.iterrows():
        rec = row.get("record_id")
        if rec is not None and not pd.isna(rec):
            rec_s = str(rec).strip()
            if rec_s:
                record_ids.add(rec_s)

    if not record_ids:
        log.debug(LogEvents.ENRICHMENT_BY_RECORD_ID_SKIPPED_NO_VALID_IDS)
        return df_act

    # Request fields are flat names.
    fields = ["record_id", "compound_name", "compound_key"]
    page_limit = cfg.get("page_limit", 1000)
    batch_size = int(cfg.get("batch_size", 100)) or 100

    # Fetch compound_record entries by record_id.
    log.info(LogEvents.ENRICHMENT_FETCHING_COMPOUND_RECORDS_BY_RECORD_ID,
        record_ids_count=len(record_ids),
    )
    compound_records_dict: dict[str, dict[str, Any]] = {}
    try:
        unique_ids = list(record_ids)
        all_records: list[dict[str, Any]] = []

        # Process in batches using record_id__in filter.
        for i in range(0, len(unique_ids), batch_size):
            chunk = unique_ids[i : i + batch_size]
            params: dict[str, Any] = {
                "record_id__in": ",".join(chunk),
                "limit": page_limit,
                "only": ",".join(fields),
                "order_by": "record_id",
            }

            try:
                for record in client.paginate(
                    "/compound_record.json",
                    params=params,
                    page_size=page_limit,
                    items_key="compound_records",
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                log.warning(LogEvents.ENRICHMENT_FETCH_ERROR_BY_RECORD_ID,
                    chunk_size=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        # Build lookup dictionary keyed by record_id.
        for record in all_records:
            rid_raw = record.get("record_id")
            if rid_raw is None:
                continue
            rid_str = str(rid_raw).strip()
            if rid_str and rid_str not in compound_records_dict:
                compound_records_dict[rid_str] = {
                    "record_id": rid_str,
                    "compound_key": record.get("compound_key"),
                    "compound_name": record.get("compound_name"),
                }
    except Exception as exc:
        log.warning(LogEvents.ENRICHMENT_FETCH_ERROR_BY_RECORD_ID,
            record_ids_count=len(record_ids),
            error=str(exc),
            exc_info=True,
        )
        return df_act

    if not compound_records_dict:
        log.debug(LogEvents.ENRICHMENT_BY_RECORD_ID_NO_RECORDS_FOUND)
        return df_act

    # Create DataFrame for the join.
    compound_data: list[dict[str, Any]] = []
    for record_id, record in compound_records_dict.items():
        compound_data.append(
            {
                "record_id": record_id,
                "compound_key": record.get("compound_key"),
                "compound_name": record.get("compound_name"),
            }
        )

    df_compound = (
        pd.DataFrame(compound_data)
        if compound_data
        else pd.DataFrame(columns=["record_id", "compound_key", "compound_name"])
    )

    # Normalize record_id in df_act for the join.
    df_act_normalized = df_act.copy()
    if "record_id" in df_act_normalized.columns:
        mask_na = df_act_normalized["record_id"].isna()
        df_act_normalized["record_id"] = df_act_normalized["record_id"].astype(str)
        df_act_normalized.loc[df_act_normalized["record_id"] == "nan", "record_id"] = pd.NA
        df_act_normalized.loc[mask_na, "record_id"] = pd.NA
        if "record_id" in df_compound.columns and not df_compound.empty:
            df_compound["record_id"] = df_compound["record_id"].astype(str)
            df_compound.loc[df_compound["record_id"] == "nan", "record_id"] = pd.NA

    # Left-join by record_id.
    df_result = df_act_normalized.merge(
        df_compound,
        on=["record_id"],
        how="left",
        suffixes=("", "_compound"),
    )

    # Coalesce *_compound columns back into the base columns.
    for col in ["compound_name", "compound_key"]:
        if f"{col}_compound" in df_result.columns:
            if col not in df_result.columns:
                df_result[col] = df_result[f"{col}_compound"]
            else:
                df_result[col] = df_result[col].where(
                    df_result[col].notna(),
                    df_result[f"{col}_compound"],
                )
            df_result = df_result.drop(columns=[f"{col}_compound"])

    # Add curated and removed (always None for this path).
    if "curated" not in df_result.columns:
        df_result["curated"] = pd.NA
    if "removed" not in df_result.columns:
        df_result["removed"] = pd.NA

    return df_result


def enrich_with_data_validity(
    df_act: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the activity DataFrame with `data_validity_lookup` fields.

    Parameters
    ----------
    df_act:
        Activity DataFrame; must contain `data_validity_comment`.
    client:
        ChemblClient used for ChEMBL API requests.
    cfg:
        Enrichment configuration from `config.chembl.activity.enrich.data_validity`.

    Returns
    -------
    pd.DataFrame
        Enriched DataFrame with column:
        - `data_validity_description` (nullable string) from `DATA_VALIDITY_LOOKUP.DESCRIPTION`.
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_enrichment")

    df_act = _ensure_columns(df_act, _DATA_VALIDITY_COLUMNS)

    if df_act.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return DATA_VALIDITY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # Ensure required columns are present.
    required_cols = ["data_validity_comment"]
    missing_cols = [col for col in required_cols if col not in df_act.columns]
    if missing_cols:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        return DATA_VALIDITY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # Collect unique data_validity_comment values, dropping NA.
    validity_comments: list[str] = []
    for _, row in df_act.iterrows():
        comment = row.get("data_validity_comment")

        # Skip NaN/None values.
        if pd.isna(comment) or comment is None:
            continue

        # Convert to string.
        comment_str = str(comment).strip()

        if comment_str:
            validity_comments.append(comment_str)

    if not validity_comments:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_COMMENTS)
        return DATA_VALIDITY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    # Retrieve configuration.
    fields = cfg.get("fields", ["data_validity_comment", "description"])
    page_limit = cfg.get("page_limit", 1000)

    # Invoke client.fetch_data_validity_lookup.
    log.info(LogEvents.ENRICHMENT_FETCHING_DATA_VALIDITY, comments_count=len(set(validity_comments)))
    records_df = client.fetch_data_validity_lookup(
        comments=validity_comments,
        fields=list(fields),
        page_limit=page_limit,
    )

    if isinstance(records_df, Mapping):
        hydrated_rows: list[dict[str, Any]] = []
        for payload in records_df.values():
            if isinstance(payload, Mapping):
                hydrated_rows.append(dict(payload))
        records_df = pd.DataFrame.from_records(hydrated_rows)

    if records_df.empty:
        log.debug(LogEvents.ENRICHMENT_NO_RECORDS_FOUND)
        return DATA_VALIDITY_ENRICHMENT_SCHEMA.validate(df_act, lazy=True)

    df_enrich = (
        records_df.loc[:, ["data_validity_comment", "description"]]
        .dropna(subset=["data_validity_comment"])
        .copy()
    )
    df_enrich["data_validity_comment"] = df_enrich["data_validity_comment"].astype("string").str.strip()
    df_enrich = (
        df_enrich.drop_duplicates(subset=["data_validity_comment"], keep="first")
        .sort_values(by=["data_validity_comment"], kind="mergesort")
        .rename(columns={"description": "data_validity_description"})
        .reset_index(drop=True)
    )

    # Left-join back to df_act on data_validity_comment while preserving row order via index.
    original_index = df_act.index.copy()
    df_result = df_act.merge(
        df_enrich,
        on=["data_validity_comment"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Ensure the column exists (fill NA when missing).
    if "data_validity_description" not in df_result.columns:
        df_result["data_validity_description"] = pd.NA

    # Restore original order.
    df_result = df_result.reindex(original_index)

    # Use the comment text when description is missing but comment is present.
    comment_series = df_result["data_validity_comment"].astype("string")
    description_series = df_result["data_validity_description"]
    missing_description_mask = comment_series.notna() & (comment_series.str.strip() != "")
    missing_description_mask &= description_series.isna()
    if bool(missing_description_mask.any()):
        df_result.loc[missing_description_mask, "data_validity_description"] = comment_series.loc[
            missing_description_mask
        ]

    # Normalize column types.
    df_result["data_validity_description"] = df_result["data_validity_description"].astype("string")

    log.info(LogEvents.ENRICHMENT_COMPLETED,
        rows_enriched=df_result.shape[0],
        records_matched=len(df_enrich),
    )
    return DATA_VALIDITY_ENRICHMENT_SCHEMA.validate(df_result, lazy=True)
