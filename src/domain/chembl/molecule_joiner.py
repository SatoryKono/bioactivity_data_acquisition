"""Domain service joining activities with compound and molecule metadata."""

from __future__ import annotations

import math
import numbers
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd

from infrastructure.chembl.repos.interfaces import (
    ActivityRepository,
    CompoundRecordRepository,
    MoleculeRepository,
)
from infrastructure.logging import LogEvents, UnifiedLogger

__all__ = ["MoleculeJoiner"]


class MoleculeJoiner:
    """Join activities with compound_record and molecule metadata."""

    def __init__(
        self,
        activity_repo: ActivityRepository,
        compound_repo: CompoundRecordRepository,
        molecule_repo: MoleculeRepository,
    ) -> None:
        self._activity_repo = activity_repo
        self._compound_repo = compound_repo
        self._molecule_repo = molecule_repo
        self._log = UnifiedLogger.get(__name__).bind(
            component="activity_molecule_join"
        )

    def join(
        self, activity_ids_or_frame: Sequence[str] | pd.DataFrame
    ) -> pd.DataFrame:
        df_act = self._prepare_activity_frame(activity_ids_or_frame)
        if df_act.empty:
            return _create_empty_result()

        record_ids = self._collect_record_ids(df_act)
        molecule_ids = self._collect_molecule_ids(df_act)

        if not record_ids and not molecule_ids:
            return self._fallback_without_join(df_act)

        compound_records = self._compound_repo.fetch_by_record_ids(sorted(record_ids))
        molecules_df = self._molecule_repo.fetch_by_ids(
            sorted(molecule_ids),
            fields=["molecule_chembl_id", "pref_name", "molecule_synonyms"],
        )
        molecules_dict = self._normalize_molecule_records(molecules_df)

        df_result = _perform_joins(df_act, compound_records, molecules_dict)

        if "activity_id" in df_result.columns:
            df_result = df_result.sort_values("activity_id").reset_index(drop=True)

        self._log.info(
            LogEvents.JOIN_COMPLETED,
            rows=len(df_result),
            compound_records_matched=len(compound_records),
            molecules_matched=len(molecules_dict),
        )
        return df_result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _prepare_activity_frame(
        self, activity_ids_or_frame: Sequence[str] | pd.DataFrame
    ) -> pd.DataFrame:
        if isinstance(activity_ids_or_frame, pd.DataFrame):
            df_act = activity_ids_or_frame.copy()
            if df_act.empty:
                self._log.debug(LogEvents.JOIN_SKIPPED_EMPTY_DATAFRAME)
                return _create_empty_result()
            required_cols = ["activity_id", "record_id", "molecule_chembl_id"]
            missing_cols = [c for c in required_cols if c not in df_act.columns]
            if missing_cols:
                self._log.warning(
                    LogEvents.JOIN_SKIPPED_MISSING_COLUMNS,
                    missing_columns=missing_cols,
                )
                return _create_empty_result()
            return df_act

        fields = ["activity_id", "record_id", "molecule_chembl_id"]
        df = self._activity_repo.fetch_by_ids(activity_ids_or_frame, fields=fields)
        if df.empty:
            self._log.debug(LogEvents.JOIN_SKIPPED_NO_ACTIVITY_DATA)
            return _create_empty_result()
        return df

    def _collect_record_ids(self, df_act: pd.DataFrame) -> set[str]:
        record_ids: set[str] = set()
        for _, row in df_act.iterrows():
            rec = row.get("record_id")
            rec_s = _canonical_record_id(rec)
            if rec_s:
                record_ids.add(rec_s)
        return record_ids

    def _collect_molecule_ids(self, df_act: pd.DataFrame) -> set[str]:
        molecule_ids: set[str] = set()
        for _, row in df_act.iterrows():
            mol = _normalize_chembl_id(row.get("molecule_chembl_id"))
            if mol:
                molecule_ids.add(mol)
        return molecule_ids

    def _normalize_molecule_records(
        self, molecules_df: pd.DataFrame
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        if not molecules_df.empty and "molecule_chembl_id" in molecules_df.columns:
            for _, row in molecules_df.iterrows():
                mol_id = row.get("molecule_chembl_id")
                if mol_id is not None and not pd.isna(mol_id):
                    mol_id_str = str(mol_id).strip()
                    if mol_id_str and mol_id_str not in result:
                        result[mol_id_str] = dict(row)
        return result

    def _fallback_without_join(self, df_act: pd.DataFrame) -> pd.DataFrame:
        out = df_act[["activity_id"]].copy()
        out["molecule_key"] = pd.NA
        out["molecule_name"] = pd.NA
        out["compound_key"] = pd.NA
        out["compound_name"] = pd.NA
        return out[
            [
                "activity_id",
                "molecule_key",
                "molecule_name",
                "compound_key",
                "compound_name",
            ]
        ]


# ----------------------------------------------------------------------
# Pure helpers
# ----------------------------------------------------------------------


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


def _perform_joins(
    df_act: pd.DataFrame,
    compound_records_dict: Mapping[str, Mapping[str, Any]],
    molecules_dict: Mapping[str, Mapping[str, Any]],
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
        else pd.DataFrame(
            columns=["molecule_chembl_id", "molecule_key", "molecule_name"]
        )
    )

    original_index = df_act.index.copy()

    df_act_normalized = df_act.copy()

    if "record_id" in df_act_normalized.columns:
        df_act_normalized["record_id"] = df_act_normalized["record_id"].map(
            _canonical_record_id
        )
        df_act_normalized.loc[df_act_normalized["record_id"] == "", "record_id"] = pd.NA
        if "record_id" in df_compound.columns and not df_compound.empty:
            df_compound["record_id"] = df_compound["record_id"].map(
                _canonical_record_id
            )
            df_compound.loc[df_compound["record_id"] == "", "record_id"] = pd.NA

    if "molecule_chembl_id" in df_act_normalized.columns:
        mask_na = df_act_normalized["molecule_chembl_id"].isna()
        df_act_normalized["molecule_chembl_id"] = df_act_normalized[
            "molecule_chembl_id"
        ].astype(str)
        df_act_normalized.loc[
            df_act_normalized["molecule_chembl_id"] == "nan",
            "molecule_chembl_id",
        ] = pd.NA
        df_act_normalized.loc[mask_na, "molecule_chembl_id"] = pd.NA
        if "molecule_chembl_id" in df_molecule.columns and not df_molecule.empty:
            df_molecule["molecule_chembl_id"] = df_molecule["molecule_chembl_id"].astype(
                str
            )
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

    if len(df_result) == len(original_index):
        df_result.index = original_index

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

    if (
        "molecule_key" in df_result.columns
        and "molecule_chembl_id" in df_result.columns
    ):
        molecule_key_series: pd.Series[Any] = df_result["molecule_key"]
        molecule_key_series = molecule_key_series.fillna(df_result["molecule_chembl_id"])
        df_result["molecule_key"] = molecule_key_series

    if "molecule_name" in df_result.columns and "molecule_key" in df_result.columns:
        molecule_name_series: pd.Series[Any] = df_result["molecule_name"]
        molecule_name_series = molecule_name_series.fillna(df_result["molecule_key"])
        df_result["molecule_name"] = molecule_name_series

    available_output_cols = [col for col in output_columns if col in df_result.columns]
    df_result = df_result[available_output_cols]

    for col in ["molecule_key", "molecule_name", "compound_key", "compound_name"]:
        if col in df_result.columns:
            df_result[col] = df_result[col].astype("string")

    return df_result


def _extract_molecule_name(record: Mapping[str, Any], fallback_id: str) -> str:
    """Extract molecule_name from a record using fallback logic."""

    pref_name = record.get("pref_name")
    if pref_name is not None and not pd.isna(pref_name):
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
