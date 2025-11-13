"""Join helpers for linking activity records to molecule metadata."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.core.logging import LogEvents
from bioetl.core.logging import UnifiedLogger

__all__ = ["join_activity_with_molecule"]


def _normalize_chembl_id(value: Any) -> str:
    """Normalize a ChEMBL identifier by converting to string and stripping whitespace."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    value_str = str(value).strip()
    return value_str if value_str else ""


def _canonical_record_id(value: Any) -> str:
    """Convert a record_id to its canonical string representation."""

    if value is None:
        return ""
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, float):
        if pd.isna(value) or not math.isfinite(value):
            return ""
        value_float: float = float(value)
        value_int = math.trunc(value_float)
        if value_float == value_int:
            return str(value_int)
        return format(value_float, "g")
    if pd.isna(value):
        return ""
    value_str = str(value).strip()
    if not value_str:
        return ""
    try:
        numeric: float = float(value_str)
    except ValueError:
        return value_str
    if pd.isna(numeric) or not math.isfinite(numeric):
        return ""
    numeric_int = math.trunc(numeric)
    if numeric == numeric_int:
        return str(numeric_int)
    return format(numeric, "g")


def join_activity_with_molecule(
    activity_ids: Sequence[str] | pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """
    Join activity records with compound_record and molecule metadata.

    Returns a DataFrame with columns:
      - activity_id
      - molecule_key (molecule_chembl_id)
      - molecule_name (pref_name → first synonym → molecule_key)
      - compound_key (from compound_record)
      - compound_name (from compound_record)
    """
    log = UnifiedLogger.get(__name__).bind(component="activity_molecule_join")

    if isinstance(activity_ids, pd.DataFrame):
        df_act = activity_ids.copy()
        if df_act.empty:
            log.debug(LogEvents.JOIN_SKIPPED_EMPTY_DATAFRAME)
            return _create_empty_result()
        required_cols = ["activity_id", "record_id", "molecule_chembl_id"]
        missing_cols = [c for c in required_cols if c not in df_act.columns]
        if missing_cols:
            log.warning(LogEvents.JOIN_SKIPPED_MISSING_COLUMNS, missing_columns=missing_cols)
            return _create_empty_result()
    else:
        df_act = _fetch_activity_by_ids(activity_ids, client, cfg, log)
        if df_act.empty:
            log.debug(LogEvents.JOIN_SKIPPED_NO_ACTIVITY_DATA)
            return _create_empty_result()

    record_ids: set[str] = set()
    molecule_ids: set[str] = set()

    for _, row in df_act.iterrows():
        rec = row.get("record_id")
        rec_s = _canonical_record_id(rec)
        if rec_s:
            record_ids.add(rec_s)

        mol = _normalize_chembl_id(row.get("molecule_chembl_id"))
        if mol:
            molecule_ids.add(mol)

    if not record_ids and not molecule_ids:
        out = df_act[["activity_id"]].copy()
        out["molecule_key"] = pd.NA
        out["molecule_name"] = pd.NA
        out["compound_key"] = pd.NA
        out["compound_name"] = pd.NA
        return out[
            ["activity_id", "molecule_key", "molecule_name", "compound_key", "compound_name"]
        ]

    compound_records_dict = _fetch_compound_records_by_ids(list(record_ids), client, cfg, log)
    molecules_dict = _fetch_molecules_for_join(list(molecule_ids), client, cfg, log)

    df_result = _perform_joins(df_act, compound_records_dict, molecules_dict, log)

    if "activity_id" in df_result.columns:
        df_result = df_result.sort_values("activity_id").reset_index(drop=True)

    log.info(LogEvents.JOIN_COMPLETED,
        rows=len(df_result),
        compound_records_matched=len(compound_records_dict),
        molecules_matched=len(molecules_dict),
    )
    return df_result


def _fetch_activity_by_ids(
    activity_ids: Sequence[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> pd.DataFrame:
    """Load activity records from the API for the provided activity_id list."""
    fields = ["activity_id", "record_id", "molecule_chembl_id"]
    batch_size = cfg.get("batch_size", 25)

    unique_ids: list[str] = []
    for act_id in activity_ids:
        if act_id and not (isinstance(act_id, float) and pd.isna(act_id)):
            unique_ids.append(str(act_id).strip())

    if not unique_ids:
        log.debug(LogEvents.ACTIVITY_NO_IDS)
        return pd.DataFrame(columns=fields)

    activity_client = ChemblActivityClient(client, batch_size=batch_size)
    all_records: list[dict[str, Any]] = []

    try:
        for record in activity_client.iterate_by_ids(unique_ids, select_fields=fields):
            all_records.append(dict(record))
    except Exception as exc:
        log.warning(LogEvents.ACTIVITY_FETCH_ERROR,
            ids_count=len(unique_ids),
            error=str(exc),
            exc_info=True,
        )

    if not all_records:
        log.debug(LogEvents.ACTIVITY_NO_RECORDS_FETCHED)
        return pd.DataFrame(columns=fields)

    df = pd.DataFrame.from_records(all_records)  # pyright: ignore[reportUnknownMemberType]
    return df


def _fetch_compound_records_by_ids(
    record_ids: list[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> dict[str, dict[str, Any]]:
    """Fetch compound_record entries by record_id (ChEMBL v2, only=, order_by, deterministic)."""
    if not record_ids:
        return {}

    fields = ["record_id", "compound_key", "compound_name"]

    page_limit_cfg = cfg.get("page_limit", 1000)
    page_limit = max(1, min(int(page_limit_cfg), 1000))
    batch_size = int(cfg.get("batch_size", 100)) or 100

    unique_ids: list[str] = []
    seen: set[str] = set()
    for rid in record_ids:
        rid_str = _canonical_record_id(rid)
        if not rid_str:
            continue
        if rid_str not in seen:
            seen.add(rid_str)
            unique_ids.append(rid_str)

    if not unique_ids:
        log.debug(LogEvents.COMPOUND_RECORD_NO_IDS_AFTER_CLEANUP)
        return {}

    all_records: list[dict[str, Any]] = []
    ids_list = unique_ids

    expected_total: int | None = None
    collected_from_api = 0

    for i in range(0, len(ids_list), batch_size):
        chunk = ids_list[i : i + batch_size]
        params: dict[str, Any] = {
            "record_id__in": ",".join(chunk),
            "limit": page_limit,
            "only": ",".join(fields),
            "order_by": "record_id",
        }

        try:
            first_page_seen = False
            for record in client.paginate(
                "/compound_record.json",
                params=params,
                page_size=page_limit,
                items_key="compound_records",
            ):
                if not first_page_seen:
                    first_page_seen = True

                all_records.append(dict(record))
                collected_from_api += 1

        except Exception as exc:
            log.warning(LogEvents.COMPOUND_RECORD_FETCH_ERROR,
                chunk_size=len(chunk),
                error=str(exc),
                exc_info=True,
            )

    result: dict[str, dict[str, Any]] = {}
    for record in all_records:
        rid_raw = record.get("record_id")
        rid_str = _canonical_record_id(rid_raw)
        if rid_str and rid_str not in result:
            result[rid_str] = {
                "record_id": rid_str,
                "compound_key": record.get("compound_key"),
                "compound_name": record.get("compound_name"),
            }

    log.info(LogEvents.COMPOUND_RECORD_FETCH_COMPLETE,
        ids_requested=len(unique_ids),
        records_fetched=len(all_records),
        records_deduped=len(result),
        page_limit=page_limit,
        order_by="record_id",
        has_total=(expected_total is not None),
        total_count=expected_total,
        collected=collected_from_api,
    )
    if expected_total is not None and collected_from_api < expected_total:
        log.warning(LogEvents.COMPOUND_RECORD_INCOMPLETE_PAGINATION,
            collected=collected_from_api,
            total_count=expected_total,
            hint="Check paginate() and items_key='compound_records', and ensure limit<=1000.",
        )

    return result


def _fetch_molecules_for_join(
    molecule_ids: list[str],
    client: ChemblClient,
    cfg: Mapping[str, Any],
    log: Any,
) -> dict[str, dict[str, Any]]:
    """Fetch molecule records by molecule_chembl_id including pref_name and molecule_synonyms."""
    if not molecule_ids:
        return {}

    fields = ["molecule_chembl_id", "pref_name", "molecule_synonyms"]
    page_limit = cfg.get("page_limit", 1000)

    return client.fetch_molecules_by_ids(
        ids=molecule_ids,
        fields=list(fields),
        page_limit=page_limit,
    )


def _perform_joins(
    df_act: pd.DataFrame,
    compound_records_dict: dict[str, dict[str, Any]],
    molecules_dict: dict[str, dict[str, Any]],
    log: Any,
) -> pd.DataFrame:
    """Perform two left joins and assemble the output fields."""
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

    molecule_data: list[dict[str, Any]] = []
    for mol_id, record in molecules_dict.items():
        molecule_name = _extract_molecule_name(record, mol_id)
        molecule_data.append(
            {
                "molecule_chembl_id": mol_id,
                "molecule_key": mol_id,
                "molecule_name": molecule_name,
            }
        )

    df_molecule = (
        pd.DataFrame(molecule_data)
        if molecule_data
        else pd.DataFrame(columns=["molecule_chembl_id", "molecule_key", "molecule_name"])
    )

    original_index = df_act.index.copy()

    df_act_normalized = df_act.copy()

    if "record_id" in df_act_normalized.columns:
        df_act_normalized["record_id"] = df_act_normalized["record_id"].map(_canonical_record_id)
        df_act_normalized.loc[df_act_normalized["record_id"] == "", "record_id"] = pd.NA
        if "record_id" in df_compound.columns and not df_compound.empty:
            df_compound["record_id"] = df_compound["record_id"].map(_canonical_record_id)
            df_compound.loc[df_compound["record_id"] == "", "record_id"] = pd.NA

    if "molecule_chembl_id" in df_act_normalized.columns:
        mask_na = df_act_normalized["molecule_chembl_id"].isna()
        df_act_normalized["molecule_chembl_id"] = df_act_normalized["molecule_chembl_id"].astype(
            str
        )
        df_act_normalized.loc[
            df_act_normalized["molecule_chembl_id"] == "nan", "molecule_chembl_id"
        ] = pd.NA
        df_act_normalized.loc[mask_na, "molecule_chembl_id"] = pd.NA
        if "molecule_chembl_id" in df_molecule.columns and not df_molecule.empty:
            df_molecule["molecule_chembl_id"] = df_molecule["molecule_chembl_id"].astype(str)
            df_molecule.loc[
                df_molecule["molecule_chembl_id"] == "nan", "molecule_chembl_id"
            ] = pd.NA

    df_result = df_act_normalized.merge(
        df_compound,
        on=["record_id"],
        how="left",
        suffixes=("", "_compound"),
    )

    df_result = df_result.merge(
        df_molecule,
        on=["molecule_chembl_id"],
        how="left",
        suffixes=("", "_molecule"),
    )

    df_result = df_result.reindex(original_index)

    output_columns = [
        "activity_id",
        "molecule_key",
        "molecule_name",
        "compound_key",
        "compound_name",
    ]
    for col in output_columns:
        if col not in df_result.columns:
            df_result[col] = pd.NA

    if "molecule_key" in df_result.columns and "molecule_chembl_id" in df_result.columns:
        molecule_key_series: pd.Series[Any] = df_result[
            "molecule_key"
        ]  # pyright: ignore[reportUnknownMemberType]
        molecule_key_series = molecule_key_series.fillna(
            df_result["molecule_chembl_id"]
        )  # pyright: ignore[reportUnknownMemberType]
        df_result["molecule_key"] = molecule_key_series

    if "molecule_name" in df_result.columns and "molecule_key" in df_result.columns:
        molecule_name_series: pd.Series[Any] = df_result[
            "molecule_name"
        ]  # pyright: ignore[reportUnknownMemberType]
        molecule_name_series = molecule_name_series.fillna(
            df_result["molecule_key"]
        )  # pyright: ignore[reportUnknownMemberType]
        df_result["molecule_name"] = molecule_name_series

    available_output_cols = [col for col in output_columns if col in df_result.columns]
    df_result = df_result[available_output_cols]

    for col in ["molecule_key", "molecule_name", "compound_key", "compound_name"]:
        if col in df_result.columns:
            df_result[col] = df_result[col].astype("string")

    return df_result


def _extract_molecule_name(record: dict[str, Any], fallback_id: str) -> str:
    """Extract molecule_name from a record using fallback logic."""
    pref_name = record.get("pref_name")
    if pref_name and not pd.isna(pref_name):
        pref_name_str = str(pref_name).strip()
        if pref_name_str:
            return pref_name_str

    synonyms = record.get("molecule_synonyms")
    candidate_synonym = _find_first_nonempty_synonym(synonyms)
    if candidate_synonym is not None:
        return candidate_synonym

    return fallback_id


def _find_first_nonempty_synonym(value: Any) -> str | None:
    """Return the first non-empty synonym string."""

    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None

    if isinstance(value, Mapping):
        mapping_value = cast(Mapping[str, Any], value)
        nested = mapping_value.get("molecule_synonym")
        return _find_first_nonempty_synonym(nested)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        sequence_value = cast(Sequence[object], value)
        return _find_first_nonempty_synonym_in_sequence(sequence_value)

    return None


def _find_first_nonempty_synonym_in_sequence(sequence: Sequence[object]) -> str | None:
    """Walk a synonym sequence and return the first non-empty entry."""

    for entry in sequence:
        nested_candidate = _find_first_nonempty_synonym(entry)
        if nested_candidate is not None:
            return nested_candidate

    return None


def _create_empty_result() -> pd.DataFrame:
    """Create an empty DataFrame with the expected columns."""
    return pd.DataFrame(
        columns=[
            "activity_id",
            "molecule_key",
            "molecule_name",
            "compound_key",
            "compound_name",
        ]
    )


