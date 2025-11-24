"""Enrichment functions for Assay pipeline."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd
from requests.exceptions import HTTPError

from infrastructure.clients.client_chembl import ChemblClient
from infrastructure.io import ensure_columns
from infrastructure.logging import LogEvents, UnifiedLogger
from infrastructure.schemas.chembl_assay_enrichment_schema import (
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

_PARAMETERS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("assay_parameters", "string"),
)


def _normalize_parameter_value(value: Any) -> Any:
    """Convert pandas-centric nulls into plain None for JSON payloads."""

    if value is pd.NA:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def enrich_with_assay_classifications(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the assay DataFrame with ASSAY_CLASS data.

    Uses data from the main /assay.json response (assay_classifications field) to extract
    assay_class_id, then optionally fetches additional details from /assay_class.json.

    Parameters
    ----------
    df_assay:
        Assay DataFrame; must contain `assay_chembl_id` and `assay_classifications`.
        The `assay_classifications` column should contain data from the main API response.
    client:
        ChemblClient used for fetching class details from /assay_class.json if needed.
    cfg:
        Enrichment configuration from `config.chembl.assay.enrich.classifications`.
        Must provide classification_fields (list of fields to extract) and page_limit.

    Returns
    -------
    pd.DataFrame:
        Enriched DataFrame with updated columns:
        - `assay_classifications` (string, nullable) — serialized classification array.
        - `assay_class_id` (string, nullable) — semicolon-delimited list of assay_class_id.

    Notes
    -----
    - Step 1: Extract assay_class_id from `assay_classifications` in /assay.json response.
    - Step 2: If additional fields (l1, l2, l3, pref_name) are needed and not present
      in the main response, fetch them from /assay_class.json?assay_class_id__in=...
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _CLASSIFICATION_COLUMNS)

    if df_assay.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(
            df_assay, lazy=True
        )

    # Ensure required columns are present.
    required_cols = ["assay_chembl_id"]
    missing_cols = [
        col for col in required_cols if col not in df_assay.columns
    ]
    if missing_cols:
        log.warning(
            LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(
            df_assay, lazy=True
        )

    # Retrieve configuration.
    classification_fields = cfg.get(
        "classification_fields",
        ["assay_class_id", "l1", "l2", "l3", "pref_name"],
    )
    page_limit = cfg.get("page_limit", 1000)

    # Initialize columns when missing.
    df_assay = df_assay.copy()
    if "assay_classifications" not in df_assay.columns:
        df_assay["assay_classifications"] = pd.NA
    if "assay_class_id" not in df_assay.columns:
        df_assay["assay_class_id"] = pd.NA

    # Step 1: Extract assay_class_id from assay_classifications in the DataFrame
    all_class_ids: set[str] = set()
    classifications_by_assay: dict[str, list[dict[str, Any]]] = {}

    for idx, row in df_assay.iterrows():
        row_key: Any = idx
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Get classifications from the DataFrame (already in the response from /assay.json)
        classifications_raw = row.get("assay_classifications")

        # Handle already serialized string
        if (
            isinstance(classifications_raw, str)
            and classifications_raw.strip()
        ):
            try:
                parsed = json.loads(classifications_raw)
                if isinstance(parsed, list):
                    classifications_raw = parsed
                else:
                    continue
            except (json.JSONDecodeError, TypeError):
                df_assay.at[row_key, "assay_classifications"] = pd.NA
                df_assay.at[row_key, "assay_class_id"] = pd.NA
                continue

        # Handle None/NA/empty
        if (
            classifications_raw is None
            or classifications_raw is pd.NA
            or (
                isinstance(classifications_raw, float)
                and pd.isna(classifications_raw)
            )
        ):
            df_assay.at[row_key, "assay_classifications"] = pd.NA
            df_assay.at[row_key, "assay_class_id"] = pd.NA
            continue

        # Parse if it's a list (from API response)
        classifications: list[dict[str, Any]] = []
        class_ids: list[str] = []

        if isinstance(classifications_raw, list):
            for item in classifications_raw:
                if not isinstance(item, dict):
                    continue
                class_id = item.get("assay_class_id")
                if class_id:
                    class_id_str = str(class_id).strip()
                    if class_id_str:
                        class_ids.append(class_id_str)
                        all_class_ids.add(class_id_str)
                        # Store raw classification data
                        classifications.append(dict(item))
        elif isinstance(classifications_raw, dict):
            class_id = classifications_raw.get("assay_class_id")
            if class_id:
                class_id_str = str(class_id).strip()
                if class_id_str:
                    class_ids.append(class_id_str)
                    all_class_ids.add(class_id_str)
                    classifications.append(dict(classifications_raw))

        # Store classifications for this assay
        if classifications:
            classifications_by_assay[str(assay_id).strip()] = classifications
            df_assay.at[row_key, "assay_class_id"] = ";".join(class_ids)
        else:
            df_assay.at[row_key, "assay_classifications"] = pd.NA
            df_assay.at[row_key, "assay_class_id"] = pd.NA

    # Step 2: Fetch additional class details from /assay_class.json if needed
    class_details: dict[str, dict[str, Any]] = {}
    if all_class_ids:
        # Check if we need additional fields that might not be in the main response
        fields_to_fetch = [
            f for f in classification_fields if f != "assay_class_id"
        ]
        if fields_to_fetch:
            log.info(
                LogEvents.ENRICHMENT_FETCHING_ASSAY_CLASSIFICATIONS,
                class_ids_count=len(all_class_ids),
            )
            try:
                # Use fetch_assay_classifications_by_class_ids which now points to /assay_class.json
                class_details_df = (
                    client.fetch_assay_classifications_by_class_ids(
                        list(all_class_ids),
                        fields=classification_fields,
                        page_limit=page_limit,
                    )
                )
                if not class_details_df.empty:
                    for _, class_row in class_details_df.iterrows():
                        class_id = class_row.get("assay_class_id")
                        if class_id:
                            class_id_str = str(class_id).strip()
                            class_details[class_id_str] = class_row.to_dict()
            except HTTPError as exc:
                # Re-raise non-404 HTTP errors
                response = getattr(exc, "response", None)
                if response is not None and hasattr(response, "status_code"):
                    if response.status_code == 404:
                        log.warning(
                            "enrichment.assay_class.fetch_failed",
                            error=str(exc),
                            message="Failed to fetch class details from /assay_class.json (404). Using data from main response only.",
                        )
                    else:
                        raise
                else:
                    raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "enrichment.assay_class.fetch_failed",
                    error=str(exc),
                    message="Failed to fetch class details from /assay_class.json. Using data from main response only.",
                )

    # Step 3: Combine data and build final structures
    for idx, row in df_assay.iterrows():
        row_key: Any = idx
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue

        assay_id_str = str(assay_id).strip()
        classifications = classifications_by_assay.get(assay_id_str, [])

        if not classifications:
            continue

        # Enrich with class details if available
        enriched_classifications: list[dict[str, Any]] = []
        for class_item in classifications:
            class_id = class_item.get("assay_class_id")
            if not class_id:
                continue

            class_id_str = str(class_id).strip()
            class_record: dict[str, Any] = {"assay_class_id": class_id_str}

            # Merge with details from /assay_class.json if available
            class_detail = class_details.get(class_id_str, {})
            for field in classification_fields:
                if field == "assay_class_id":
                    continue
                # Prefer detail from /assay_class.json, fallback to main response
                value = class_detail.get(field) or class_item.get(field)
                if value is not None:
                    class_record[field] = value

            enriched_classifications.append(class_record)

        if enriched_classifications:
            serialized = json.dumps(
                enriched_classifications, ensure_ascii=False
            )
            df_assay.at[row_key, "assay_classifications"] = serialized

    log.info(
        LogEvents.ENRICHMENT_CLASSIFICATIONS_COMPLETE,
        assays_with_classifications=len(
            df_assay[df_assay["assay_classifications"].notna()]
        ),
    )
    return ASSAY_CLASSIFICATION_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)


def enrich_with_assay_parameters(
    df_assay: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the assay DataFrame with ASSAY_PARAMETERS data.

    Uses data from the main /assay.json response (assay_parameters field) instead of
    making separate API calls to non-existent endpoints.

    Parameters
    ----------
    df_assay:
        Assay DataFrame; must contain `assay_chembl_id` and `assay_parameters`.
        The `assay_parameters` column should contain data from the main API response.
    client:
        ChemblClient (not used, kept for API compatibility).
    cfg:
        Enrichment configuration from `config.chembl.assay.enrich.parameters`.
        Must provide fields (list to extract) and active_only flag.

    Returns
    -------
    pd.DataFrame:
        Enriched DataFrame with an updated column:
        - `assay_parameters` (string, nullable) — serialized JSON array of parameters
          with fields: type, relation, value, units, text_value, standard_*,
          active, type_normalized, type_fixed (when present in the payload).

    Notes
    -----
    - Data is extracted from the `assay_parameters` column in the DataFrame,
      which comes from the main /assay.json API response.
    - Parameters are filtered by active=1 when active_only=True in the configuration.
    - If data is already serialized as JSON string, it's preserved.
    - If data is a list/dict, it's serialized to JSON string.
    """
    log = UnifiedLogger.get(__name__).bind(component="assay_enrichment")

    df_assay = _ensure_columns(df_assay, _PARAMETERS_COLUMNS)

    if df_assay.empty:
        if "assay_parameters" in df_assay.columns:
            df_assay["assay_parameters"] = df_assay["assay_parameters"].astype(
                "string"
            )
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Validate presence of required columns
    required_cols = ["assay_chembl_id"]
    missing_cols = [
        col for col in required_cols if col not in df_assay.columns
    ]
    if missing_cols:
        log.warning(
            LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)

    # Retrieve configuration defaults
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
            "type_normalized",
            "type_fixed",
        ],
    )
    active_only = cfg.get("active_only", True)

    # Initialize the column when missing
    df_assay = df_assay.copy()
    if "assay_parameters" not in df_assay.columns:
        df_assay["assay_parameters"] = pd.NA

    # If assay_parameters column is missing or all values are NA, try fetching from API
    if not df_assay.empty:
        # Check if we need to fetch from API
        needs_fetch = False
        if "assay_parameters" not in df_assay.columns:
            needs_fetch = True
        else:
            # Check if all values are NA
            try:
                needs_fetch = df_assay["assay_parameters"].isna().all()
            except Exception:  # noqa: BLE001
                needs_fetch = True

        if needs_fetch:
            try:
                assay_ids = (
                    df_assay["assay_chembl_id"]
                    .dropna()
                    .astype("string")
                    .tolist()
                )
                # Filter out empty strings
                assay_ids = [
                    aid for aid in assay_ids if aid and str(aid).strip()
                ]
                if assay_ids:
                    # Temporarily change column type to object to allow setting list values
                    if "assay_parameters" in df_assay.columns:
                        df_assay["assay_parameters"] = df_assay[
                            "assay_parameters"
                        ].astype("object")
                    fetched_params = (
                        client.fetch_assay_parameters_by_assay_ids(
                            assay_ids,
                            page_limit=cfg.get("page_limit"),
                            active_only=active_only,
                        )
                    )
                    # Handle both DataFrame (from real API) and Mapping (from mocks in tests)
                    if (
                        isinstance(fetched_params, pd.DataFrame)
                        and not fetched_params.empty
                    ):
                        # Group by assay_chembl_id and convert to list of dicts
                        if "assay_chembl_id" in fetched_params.columns:
                            for idx, row in df_assay.iterrows():
                                assay_id = row.get("assay_chembl_id")
                                if pd.isna(assay_id) or assay_id is None:
                                    continue
                                assay_id_str = str(assay_id).strip()
                                assay_params = fetched_params[
                                    fetched_params["assay_chembl_id"]
                                    == assay_id_str
                                ]
                                if not assay_params.empty:
                                    params_list = assay_params.to_dict(
                                        orient="records"
                                    )
                                    df_assay.at[idx, "assay_parameters"] = (
                                        params_list
                                    )
                    elif isinstance(fetched_params, Mapping):
                        # Fill assay_parameters column with fetched data (from mocks in tests)
                        for idx, row in df_assay.iterrows():
                            assay_id = row.get("assay_chembl_id")
                            if pd.isna(assay_id) or assay_id is None:
                                continue
                            assay_id_str = str(assay_id).strip()
                            if assay_id_str in fetched_params:
                                params_list = fetched_params[assay_id_str]
                                if isinstance(params_list, list):
                                    df_assay.at[idx, "assay_parameters"] = (
                                        params_list
                                    )
            except HTTPError as exc:
                # Re-raise non-404 HTTP errors
                response = getattr(exc, "response", None)
                if response is not None and hasattr(response, "status_code"):
                    if response.status_code == 404:
                        log.warning(
                            LogEvents.ENRICHMENT_FETCH_ERROR_BY_RECORD_ID,
                            error=str(exc),
                            message="Failed to fetch assay parameters from API (404). Using data from DataFrame only.",
                        )
                    else:
                        raise
                else:
                    raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    LogEvents.ENRICHMENT_FETCH_ERROR_BY_RECORD_ID,
                    error=str(exc),
                    message="Failed to fetch assay parameters from API. Using data from DataFrame only.",
                )

    # Process each assay entry
    for row_position, (_, row) in enumerate(df_assay.iterrows()):
        assay_id = row.get("assay_chembl_id")
        if pd.isna(assay_id) or assay_id is None:
            continue

        # Get parameters from the DataFrame (already in the response from /assay.json)
        params_raw = row.get("assay_parameters")

        # Skip if already serialized as string
        if isinstance(params_raw, str) and params_raw.strip():
            # Already serialized, keep as is
            continue

        # Handle None/NA/empty
        if (
            params_raw is None
            or params_raw is pd.NA
            or (isinstance(params_raw, float) and pd.isna(params_raw))
        ):
            continue

        # Parse if it's a list/dict (from API response)
        params_list: list[dict[str, Any]] = []
        if isinstance(params_raw, list):
            for param in params_raw:
                if not isinstance(param, dict):
                    continue
                # Filter by active if needed
                if active_only:
                    active_value = param.get("active")
                    # If active field is missing, include the parameter (default to active)
                    if active_value is not None and active_value not in (
                        1,
                        "1",
                        True,
                        "True",
                    ):
                        continue
                # Extract requested fields
                param_record: dict[str, Any] = {}
                for field in fields:
                    if field != "assay_chembl_id":
                        param_record[field] = _normalize_parameter_value(
                            param.get(field)
                        )
                params_list.append(param_record)
        elif isinstance(params_raw, dict):
            # Single parameter as dict
            if active_only:
                active_value = params_raw.get("active")
                # If active field is missing, include the parameter (default to active)
                if active_value is not None and active_value not in (
                    1,
                    "1",
                    True,
                    "True",
                ):
                    continue
            param_record: dict[str, Any] = {}
            for field in fields:
                if field != "assay_chembl_id":
                    param_record[field] = _normalize_parameter_value(
                        params_raw.get(field)
                    )
            params_list.append(param_record)

        # Serialize the array into a JSON string
        if params_list:
            index_label = df_assay.index[row_position]
            df_assay.loc[index_label, "assay_parameters"] = json.dumps(
                params_list,
                ensure_ascii=False,
            )
        else:
            # No valid parameters found; set to NA
            index_label = df_assay.index[row_position]
            df_assay.loc[index_label, "assay_parameters"] = pd.NA

    # Ensure string type for all values
    df_assay["assay_parameters"] = df_assay["assay_parameters"].astype(
        "string"
    )

    log.info(
        LogEvents.ENRICHMENT_PARAMETERS_COMPLETE,
        assays_with_parameters=len(
            df_assay[df_assay["assay_parameters"].notna()]
        ),
    )
    return ASSAY_PARAMETERS_ENRICHMENT_SCHEMA.validate(df_assay, lazy=True)
