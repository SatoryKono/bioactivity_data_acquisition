"""Enrichment functions for Assay pipeline."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pandas as pd

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.core.io import ensure_columns
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.schemas.chembl_assay_enrichment import (
    ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA,
    ASSAY_PARAMETERS_ENRICHMENT_SCHEMA,
)

__all__ = [
    "enrich_with_assay_classifications",
    "enrich_with_assay_parameters",
]


_ensure_columns = ensure_columns


_CLASSIFICATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("assay_classifications", "string"),
    ("assay_class_id", "string"),
)

_PARAMETERS_COLUMNS: tuple[tuple[str, str], ...] = (("assay_parameters", "string"),)


def _should_nullify_string_value(value: Any) -> bool:
    """Return True when a value in a string column should be replaced with NA."""
    if value is None:
        return False
    if value is pd.NA:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return not isinstance(value, str)


def enrich_with_assay_classifications(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the assay DataFrame with ASSAY_CLASS_MAP and ASSAY_CLASSIFICATION data.

    Parameters
    ----------
    df_assay:
        Assay DataFrame; must contain `assay_chembl_id`.
    client:
        ChemblClient used for ChEMBL API requests.
    cfg:
        Enrichment configuration from `config.chembl.assay.enrich.classifications`.

    Returns
    -------
    pd.DataFrame:
        Enriched DataFrame with added or updated columns:
        - `assay_classifications` (string, nullable) — serialized classification array.
        - `assay_class_id` (string, nullable) — semicolon-delimited list of assay_class_id.
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _CLASSIFICATION_COLUMNS)

    if "assay_classifications" in df_assay.columns:
        invalid_mask = df_assay["assay_classifications"].map(_should_nullify_string_value)
        if bool(invalid_mask.any()):
            log.warning(LogEvents.ASSAY_CLASSIFICATIONS_RESET_NON_STRING,
                rows=int(invalid_mask.sum()),
            )
            df_assay.loc[invalid_mask, "assay_classifications"] = pd.NA

    if df_assay.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Ensure required columns are present.
    required_cols = ["assay_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_assay.columns]
    if missing_cols:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Collect unique assay_chembl_id values, dropping NA.
    assay_ids: list[str] = []
    for _, row in df_assay.iterrows():
        assay_id = row.get("assay_chembl_id")

        # Skip NaN/None values.
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Convert to string.
        assay_id_str = str(assay_id).strip()

        if assay_id_str:
            assay_ids.append(assay_id_str)

    if not assay_ids:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_IDS)
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Retrieve configuration.
    class_map_fields = cfg.get("class_map_fields", ["assay_chembl_id", "assay_class_id"])
    classification_fields = cfg.get(
        "classification_fields",
        ["assay_class_id", "l1", "l2", "l3", "pref_name"],
    )
    page_limit = cfg.get("page_limit", 1000)

    # Step 1: fetch ASSAY_CLASS_MAP by assay_chembl_id.
    log.info(LogEvents.ENRICHMENT_FETCHING_ASSAY_CLASS_MAP, ids_count=len(set(assay_ids)))
    class_map_dict = client.fetch_assay_class_map_by_assay_ids(
        assay_ids,
        list(class_map_fields),
        page_limit,
    )

    # Collect unique assay_class_id values.
    all_class_ids: set[str] = set()
    for mappings in class_map_dict.values():
        for mapping in mappings:
            class_id = mapping.get("assay_class_id")
            if class_id and not (isinstance(class_id, float) and pd.isna(class_id)):
                all_class_ids.add(str(class_id).strip())

    # Step 2: fetch ASSAY_CLASSIFICATION by assay_class_id.
    classification_dict: dict[str, dict[str, Any]] = {}
    if all_class_ids:
        log.info(LogEvents.ENRICHMENT_FETCHING_ASSAY_CLASSIFICATIONS, class_ids_count=len(all_class_ids))
        classification_dict = client.fetch_assay_classifications_by_class_ids(
            list(all_class_ids),
            list(classification_fields),
            page_limit,
        )

    # Step 3: combine data and build structures per assay.
    df_assay = df_assay.copy()

    # Initialize columns when missing.
    if "assay_classifications" not in df_assay.columns:
        df_assay["assay_classifications"] = pd.NA
    if "assay_class_id" not in df_assay.columns:
        df_assay["assay_class_id"] = pd.NA

    # Process each assay record.
    for idx, row in df_assay.iterrows():
        row_key: Any = idx
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue
        assay_id_str = str(assay_id).strip()

        # Retrieve mappings for this assay.
        mappings = class_map_dict.get(assay_id_str, [])

        if not mappings:
            # No classifications — keep NULL.
            df_assay.at[row_key, "assay_classifications"] = pd.NA
            df_assay.at[row_key, "assay_class_id"] = pd.NA
            continue

        # Collect classification data.
        classifications: list[dict[str, Any]] = []
        class_ids: list[str] = []

        for mapping in mappings:
            class_id = mapping.get("assay_class_id")
            if not class_id or (isinstance(class_id, float) and pd.isna(class_id)):
                continue

            class_id_str = str(class_id).strip()
            if not class_id_str:
                continue

            # Retrieve classification data.
            classification_data = classification_dict.get(class_id_str)
            if classification_data:
                # Create combined record.
                class_record: dict[str, Any] = {
                    "assay_class_id": class_id_str,
                }
                # Populate fields from classification_data.
                for field in classification_fields:
                    if field != "assay_class_id":
                        class_record[field] = classification_data.get(field)
                classifications.append(class_record)
            else:
                # No classification detail available; create minimal record.
                class_record = {"assay_class_id": class_id_str}
                classifications.append(class_record)

            class_ids.append(class_id_str)

        # Persist results.
        if classifications:
            serialized = json.dumps(classifications, ensure_ascii=False)
            class_id_joined = ";".join(class_ids)
            df_assay.at[row_key, "assay_classifications"] = serialized
            df_assay.at[row_key, "assay_class_id"] = class_id_joined

    log.info(LogEvents.ENRICHMENT_CLASSIFICATIONS_COMPLETE,
        assays_with_classifications=len(df_assay[df_assay["assay_classifications"].notna()]),
    )
    return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)


def enrich_with_assay_parameters(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the assay DataFrame with ASSAY_PARAMETERS data.

    Extracts the full TRUV set (TYPE, RELATION, VALUE, UNITS, TEXT_VALUE),
    standardized fields (standard_*), operational fields (active), and optional
    normalization fields (type_normalized, type_fixed).

    Parameters
    ----------
    df_assay:
        Assay DataFrame; must contain `assay_chembl_id`.
    client:
        ChemblClient used for ChEMBL API requests.
    cfg:
        Enrichment configuration from `config.chembl.assay.enrich.parameters`.
        Must provide fields (list to extract), page_limit, and active_only flag.

    Returns
    -------
    pd.DataFrame:
        Enriched DataFrame with an added or updated column:
        - `assay_parameters` (string, nullable) — serialized JSON array of parameters
          with fields: type, relation, value, units, text_value, standard_*,
          active, type_normalized, type_fixed (when present in the payload).

    Notes
    -----
    - Original values remain unchanged; they are not copied into standard_* automatically.
    - Optional fields (type_normalized, type_fixed) are extracted only when present in the payload.
    - Parameters are filtered by active=1 when active_only=True in the configuration.
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _PARAMETERS_COLUMNS)

    if df_assay.empty:
        if "assay_parameters" in df_assay.columns:
            df_assay["assay_parameters"] = df_assay["assay_parameters"].astype("string")
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    if "assay_parameters" in df_assay.columns:
        invalid_mask = df_assay["assay_parameters"].map(_should_nullify_string_value)
        if bool(invalid_mask.any()):
            log.warning(LogEvents.ASSAY_PARAMETERS_RESET_NON_STRING,
                rows=int(invalid_mask.sum()),
            )
            df_assay.loc[invalid_mask, "assay_parameters"] = pd.NA
        df_assay["assay_parameters"] = df_assay["assay_parameters"].astype("string")

    # Validate presence of required columns
    required_cols = ["assay_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_assay.columns]
    if missing_cols:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Collect unique assay_chembl_id values and drop missing entries
    assay_ids: list[str] = []
    for _, row in df_assay.iterrows():
        assay_id = row.get("assay_chembl_id")

        # Skip NaN / None values
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Convert identifier to string
        assay_id_str = str(assay_id).strip()

        if assay_id_str:
            assay_ids.append(assay_id_str)

    if not assay_ids:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_IDS)
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Retrieve configuration defaults
    # Defaults include the full TRUV set and standardized fields
    fields = cfg.get(
        "fields",
        [
            "assay_chembl_id",
            "type",
            "relation",
            "value",
            "units",
            "text_value",
            "standard_type",
            "standard_relation",
            "standard_value",
            "standard_units",
            "standard_text_value",
            "active",
        ],
    )
    page_limit = cfg.get("page_limit", 1000)
    active_only = cfg.get("active_only", True)

    # Fetch ASSAY_PARAMETERS by assay_chembl_id
    log.info(LogEvents.ENRICHMENT_FETCHING_ASSAY_PARAMETERS, ids_count=len(set(assay_ids)))
    parameters_dict = client.fetch_assay_parameters_by_assay_ids(
        assay_ids,
        list(fields),
        page_limit,
        active_only,
    )

    # Iterate over each assay record
    df_assay = df_assay.copy()

    # Initialize the column when missing
    if "assay_parameters" not in df_assay.columns:
        df_assay["assay_parameters"] = pd.NA

    # Process each assay entry
    for row_position, (_, row) in enumerate(df_assay.iterrows()):
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue
        assay_id_str = str(assay_id).strip()

        # Retrieve parameters for the current assay
        parameters = parameters_dict.get(assay_id_str, [])

        if not parameters:
            # No parameters found; keep NULL
            continue

        # Build a list containing the requested fields
        params_list: list[dict[str, Any]] = []
        for param in parameters:
            param_record: dict[str, Any] = {}
            for field in fields:
                if field != "assay_chembl_id":
                    param_record[field] = param.get(field)
            params_list.append(param_record)

        # Serialize the array into a JSON string
        if params_list:
            index_label = df_assay.index[row_position]
            df_assay.loc[index_label, "assay_parameters"] = json.dumps(
                params_list,
                ensure_ascii=False,
            )

    log.info(LogEvents.ENRICHMENT_PARAMETERS_COMPLETE,
        assays_with_parameters=len(df_assay[df_assay["assay_parameters"].notna()]),
    )
    return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)
