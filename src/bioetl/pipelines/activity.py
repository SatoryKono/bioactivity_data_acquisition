"""Activity Pipeline - ChEMBL activity data extraction."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Sequence
from functools import lru_cache
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import (
    APIConfig,
    CircuitBreakerOpenError,
    UnifiedAPIClient,
)
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ActivitySchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dataframe import resolve_schema_column_order
from bioetl.utils.dtypes import coerce_nullable_int, coerce_retry_after
from bioetl.utils.fallback import build_fallback_payload

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("activity", "1.0.0", ActivitySchema)


NA_STRINGS = {"", "na", "n/a", "none", "null", "nan"}
RELATION_ALIASES = {
    "==": "=",
    "=": "=",
    "≡": "=",
    "<": "<",
    "≤": "<=",
    "⩽": "<=",
    "⩾": ">=",
    "≥": ">=",
    ">": ">",
    "~": "~",
}
UNIT_SYNONYMS = {
    "nm": "nM",
    "nanomolar": "nM",
    "μm": "µM",
    "µm": "µM",
    "um": "µM",
    "μmolar": "µM",
    "µmolar": "µM",
    "mum": "µM",
    "μmol/l": "µmol/L",
    "µmol/l": "µmol/L",
    "umol/l": "µmol/L",
    "mmol/l": "mmol/L",
    "mol/l": "mol/L",
    "μg/ml": "µg/mL",
    "μg/mL": "µg/mL",
    "µg/ml": "µg/mL",
    "µg/mL": "µg/mL",
    "ug/ml": "µg/mL",
    "ug/mL": "µg/mL",
    "ng/ml": "ng/mL",
    "pg/ml": "pg/mL",
    "mg/ml": "mg/mL",
    "mg/l": "mg/L",
    "ug/l": "µg/L",
    "μg/l": "µg/L",
    "µg/l": "µg/L",
}
BOOLEAN_TRUE = {"true", "1", "yes", "y", "t"}
BOOLEAN_FALSE = {"false", "0", "no", "n", "f"}
INTEGER_COLUMNS: tuple[str, ...] = (
    "standard_flag",
    "potential_duplicate",
    "src_id",
    "target_tax_id",
)
INTEGER_COLUMNS_WITH_ID: tuple[str, ...] = ("activity_id",) + INTEGER_COLUMNS
FLOAT_COLUMNS: tuple[str, ...] = ("fallback_retry_after_sec",)


@lru_cache(maxsize=1)
def _get_activity_column_order() -> list[str]:
    """Return the canonical Activity schema column order with robust fallbacks."""

    columns = resolve_schema_column_order(ActivitySchema)
    if not columns:
        try:
            columns = ActivitySchema.get_column_order()
        except AttributeError:  # pragma: no cover - defensive safeguard
            columns = []
    return list(columns)


def _is_na(value: Any) -> bool:
    """Return True if the provided value should be treated as NA."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        if value.strip() == "":
            return True
        return value.strip().lower() in NA_STRINGS
    return False


def _canonicalize_whitespace(text: str) -> str:
    """Collapse consecutive whitespace into single spaces."""

    return re.sub(r"\s+", " ", text.strip())


def _normalize_string(
    value: Any,
    *,
    uppercase: bool = False,
    title_case: bool = False,
    max_length: int | None = None,
) -> str | None:
    """Normalize textual values with canonical whitespace and casing."""

    if _is_na(value):
        return None

    text = str(value)
    text = _canonicalize_whitespace(text)
    if not text:
        return None

    if uppercase:
        text = text.upper()
    elif title_case:
        tokens = text.split(" ")
        normalized_tokens: list[str] = []
        for token in tokens:
            if token.isupper() and len(token) <= 4:
                normalized_tokens.append(token)
            elif token.lower() in {"sp.", "spp.", "cf."}:
                normalized_tokens.append(token.lower())
            else:
                normalized_tokens.append(token.capitalize())
        text = " ".join(normalized_tokens)

    if max_length is not None:
        text = text[:max_length]
    return text


def _normalize_chembl_id(value: Any) -> str | None:
    """Normalize ChEMBL identifiers to uppercase canonical form."""

    normalized = _normalize_string(value, uppercase=True)
    if normalized is None:
        return None
    return normalized


def _normalize_bao_id(value: Any) -> str | None:
    """Normalize BAO identifiers."""

    normalized = _normalize_string(value, uppercase=True)
    if normalized is None:
        return None
    return normalized


def _normalize_units(value: Any, default: str | None = None) -> str | None:
    """Normalize measurement units using known synonyms."""

    if _is_na(value):
        return default

    text = _canonicalize_whitespace(str(value))
    key = text.lower()
    canonical = UNIT_SYNONYMS.get(key)
    if canonical is not None:
        return canonical
    return text


def _normalize_relation(value: Any, default: str = "=") -> str:
    """Normalize inequality relations to ASCII equivalents."""

    if _is_na(value):
        return default
    relation = str(value).strip()
    if relation == "":
        return default
    return RELATION_ALIASES.get(relation, RELATION_ALIASES.get(relation.lower(), default))


def _normalize_float(value: Any) -> float | None:
    """Convert values to float when possible."""

    if _is_na(value):
        return None
    try:
        result = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _normalize_int(value: Any) -> int | None:
    """Convert values to integers when possible."""

    if _is_na(value):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_bool(value: Any, *, default: bool = False) -> bool:
    """Normalize boolean-like values to strict bools."""

    if _is_na(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in BOOLEAN_TRUE:
        return True
    if text in BOOLEAN_FALSE:
        return False
    return default


def _normalize_target_organism(value: Any) -> str | None:
    """Normalize organism names to title case with canonical whitespace."""

    return _normalize_string(value, title_case=True)


def _parse_numeric(value: Any) -> float | None:
    """Best-effort conversion to float for property parsing."""

    if isinstance(value, int | float) and not (isinstance(value, float) and math.isnan(value)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except (TypeError, ValueError):
            return None
    return None


def _normalize_activity_properties(
    raw: Any,
) -> tuple[str | None, list[dict[str, Any]]]:
    """Canonicalize activity properties preserving all entries."""

    if _is_na(raw):
        return None, []

    parsed: Any = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None, []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None, []

    if isinstance(parsed, dict):
        parsed = [parsed]

    if not isinstance(parsed, list):
        return None, []

    normalized_records: list[dict[str, Any]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        normalized_entry: dict[str, Any] = {}
        for key, value in entry.items():
            if isinstance(value, str):
                text_value = _normalize_string(value)
                if text_value is None:
                    normalized_entry[key] = None
                else:
                    numeric = _parse_numeric(text_value)
                    normalized_entry[key] = numeric if numeric is not None else text_value
            elif isinstance(value, int | float):
                if isinstance(value, float) and math.isnan(value):
                    normalized_entry[key] = None
                else:
                    normalized_entry[key] = value
            elif isinstance(value, bool) or value is None:
                normalized_entry[key] = value
            else:
                normalized_entry[key] = value
        if normalized_entry:
            normalized_records.append(dict(sorted(normalized_entry.items())))

    if not normalized_records:
        return None, []

    normalized_records.sort(
        key=lambda item: (
            str(
                item.get("name")
                or item.get("type")
                or item.get("property_name")
                or ""
            ).lower(),
            json.dumps(item, ensure_ascii=False, sort_keys=True),
        )
    )
    canonical = json.dumps(normalized_records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return canonical, normalized_records


def _normalize_ligand_efficiency(
    ligand_efficiency: Any,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Normalize ligand efficiency metrics once."""

    if not isinstance(ligand_efficiency, dict):
        return None, None, None, None

    bei = _normalize_float(ligand_efficiency.get("bei"))
    sei = _normalize_float(ligand_efficiency.get("sei"))
    le = _normalize_float(ligand_efficiency.get("le"))
    lle = _normalize_float(ligand_efficiency.get("lle"))
    return bei, sei, le, lle


def _derive_compound_key(
    molecule_id: str | None,
    standard_type: str | None,
    target_id: str | None,
) -> str | None:
    """Build compound key when all components are available."""

    if molecule_id and standard_type and target_id:
        return "|".join([molecule_id, standard_type, target_id])
    return None


def _derive_is_citation(
    document_id: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Infer citation flag from document linkage or properties."""

    if document_id:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower()
        if label.replace(" ", "_") == "is_citation":
            return _normalize_bool(prop.get("value"), default=False)
    return False


def _derive_exact_data_citation(
    comment: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Derive exact data citation flag from comments or properties."""

    text = (comment or "").lower()
    if "exact" in text and "citation" in text:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
        if label == "exact_data_citation":
            return _normalize_bool(prop.get("value"), default=False)
    return False


def _derive_rounded_data_citation(
    comment: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Derive rounded data citation flag."""

    text = (comment or "").lower()
    if "rounded" in text and "citation" in text:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
        if label == "rounded_data_citation":
            return _normalize_bool(prop.get("value"), default=False)
    return False


def _derive_high_citation_rate(properties: Sequence[dict[str, Any]]) -> bool:
    """Detect high citation rate from property payloads."""

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower()
        if "citation" not in label:
            continue
        numeric = None
        for candidate in ("value", "num_value", "property_value", "count"):
            if candidate in prop:
                numeric = _parse_numeric(prop.get(candidate))
                if numeric is not None:
                    break
        if numeric is not None and numeric >= 50:
            return True
        if label.replace(" ", "_") == "high_citation_rate":
            return _normalize_bool(prop.get("value"), default=False)
    return False


def _derive_is_censored(relation: str | None) -> bool | None:
    """Infer censorship flag from relation symbol."""

    if relation is None:
        return None
    return relation != "="


def _coerce_nullable_int_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Compatibility wrapper delegating to :func:`coerce_nullable_int`."""

    coerce_nullable_int(df, columns)


def _coerce_nullable_float_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    """Normalise optional float columns to ``float64`` with ``NaN`` placeholders."""

    for column in columns:
        if column not in df.columns:
            continue

        df[column] = pd.to_numeric(df[column], errors="coerce")


class ActivityPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL activity data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self._last_validation_report: dict[str, Any] | None = None
        self._fallback_stats: dict[str, Any] = {}

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 25) or 25
        elif chembl_source is not None:
            base_url = getattr(chembl_source, "base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = getattr(chembl_source, "batch_size", 25) or 25
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 25

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = min(batch_size, 25)

        # Status handshake for release metadata
        self._status_snapshot: dict[str, Any] | None = None
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract activity data from input file."""
        if input_file is None:
            # Default to data/input/activity.csv
            input_file = Path("data/input/activity.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with activity IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        # Read CSV file
        df = pd.read_csv(input_file)

        # Map activity_chembl_id to activity_id if needed
        if 'activity_chembl_id' in df.columns:
            df = df.rename(columns={'activity_chembl_id': 'activity_id'})
            df['activity_id'] = pd.to_numeric(df['activity_id'], errors='coerce').astype('Int64')

        # Extract activity IDs for API call
        activity_ids = df['activity_id'].dropna().astype(int).unique().tolist()

        limit_value = self.get_runtime_limit()
        if limit_value is not None and len(activity_ids) > limit_value:
            logger.info(
                "applying_input_limit",
                limit=limit_value,
                original_ids=len(activity_ids),
            )
            activity_ids = activity_ids[:limit_value]

        if not activity_ids:
            logger.warning("no_activity_ids_found")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("fetching_from_chembl_api", count=len(activity_ids))
        extracted_df = self._extract_from_chembl(activity_ids)

        if extracted_df.empty:
            logger.warning("no_api_data_returned")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("extraction_completed", rows=len(extracted_df), columns=len(extracted_df.columns))
        return extracted_df

    def _extract_from_chembl(self, activity_ids: list[int]) -> pd.DataFrame:
        """Extract activity data using release-scoped batching with caching."""

        if not activity_ids:
            return pd.DataFrame()

        success_count = 0
        fallback_count = 0
        error_count = 0
        api_calls = 0
        cache_hits = 0

        results: list[dict[str, Any]] = []

        for i in range(0, len(activity_ids), self.batch_size):
            batch_ids = activity_ids[i : i + self.batch_size]
            batch_number = i // self.batch_size + 1
            logger.info("fetching_batch", batch=batch_number, size=len(batch_ids))

            cached_records = self._load_batch_from_cache(batch_ids)
            if cached_records is not None:
                logger.info("cache_batch_hit", batch=batch_number, size=len(batch_ids))
                cache_hits += len(batch_ids)
                results.extend(cached_records)
                success_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") != "ChEMBL_FALLBACK"
                )
                fallback_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") == "ChEMBL_FALLBACK"
                )
                continue

            try:
                batch_records, batch_metrics = self._fetch_batch(batch_ids)
                api_calls += 1
                success_count += batch_metrics["success"]
                fallback_count += batch_metrics["fallback"]
                error_count += batch_metrics["error"]
                results.extend(batch_records)
                self._store_batch_in_cache(batch_ids, batch_records)
            except CircuitBreakerOpenError as error:
                logger.warning("circuit_breaker_open", batch=batch_number, error=str(error))
                fallback_records = [
                    self._create_fallback_record(
                        activity_id=activity_id,
                        reason="circuit_breaker_open",
                        error=error,
                    )
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)
            except Exception as error:  # noqa: BLE001 - surfaced for metrics
                error_count += len(batch_ids)
                logger.error("batch_fetch_failed", error=str(error), batch_ids=batch_ids)
                fallback_records = [
                    self._create_fallback_record(
                        activity_id=activity_id,
                        reason="exception",
                        error=error,
                    )
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        # Ensure deterministic ordering by activity_id
        results_sorted = sorted(results, key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")))

        logger.info(
            "chembl_activity_metrics",
            total_activities=len(activity_ids),
            success_count=success_count,
            fallback_count=fallback_count,
            error_count=error_count,
            success_rate=
            ((success_count + fallback_count) / len(activity_ids))
            if activity_ids
            else 0.0,
            api_calls=api_calls,
            cache_hits=cache_hits,
        )

        df = pd.DataFrame(results_sorted)
        if not df.empty:
            expected_columns = _get_activity_column_order()
            if expected_columns:
                extra_columns = [column for column in df.columns if column not in expected_columns]

                for column in expected_columns:
                    if column not in df.columns:
                        if column in INTEGER_COLUMNS_WITH_ID:
                            df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                        else:
                            df[column] = pd.Series(pd.NA, index=df.index)

                ordered_columns = [column for column in expected_columns if column in df.columns]
                df = df[ordered_columns + extra_columns]

            _coerce_nullable_int_columns(df, INTEGER_COLUMNS_WITH_ID)
        logger.info("extraction_completed", rows=len(df), from_api=True)
        return df

    def _fetch_batch(self, batch_ids: Iterable[int]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Fetch a batch of activities from the ChEMBL API."""

        ids_str = ",".join(map(str, batch_ids))
        url = "/activity.json"
        params = {"activity_id__in": ids_str}

        response = self.api_client.request_json(url, params=params)

        activities = response.get("activities", [])
        metrics = {"success": 0, "fallback": 0, "error": 0}

        records: dict[int, dict[str, Any]] = {}
        for activity in activities:
            activity_id = activity.get("activity_id")
            if activity_id is None:
                continue
            record = self._normalize_activity(activity)
            records[int(activity_id)] = record

        missing_ids = [activity_id for activity_id in batch_ids if activity_id not in records]
        if missing_ids:
            metrics["error"] += len(missing_ids)
            for missing_id in missing_ids:
                records[missing_id] = self._create_fallback_record(
                    activity_id=missing_id,
                    reason="not_in_response",
                )

        metrics["success"] = sum(
            1 for record in records.values() if record.get("source_system") != "ChEMBL_FALLBACK"
        )
        metrics["fallback"] = sum(
            1 for record in records.values() if record.get("source_system") == "ChEMBL_FALLBACK"
        )

        ordered_records = [records[activity_id] for activity_id in sorted(records)]
        return ordered_records, metrics

    def _normalize_activity(self, activity: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw activity payload into a flat record."""

        activity_id = _normalize_int(activity.get("activity_id"))
        molecule_id = _normalize_chembl_id(activity.get("molecule_chembl_id"))
        assay_id = _normalize_chembl_id(activity.get("assay_chembl_id"))
        target_id = _normalize_chembl_id(activity.get("target_chembl_id"))
        document_id = _normalize_chembl_id(activity.get("document_chembl_id"))

        published_type = _normalize_string(activity.get("type") or activity.get("published_type"), uppercase=True)
        published_relation = _normalize_relation(activity.get("relation") or activity.get("published_relation"), default="=")
        published_value = _normalize_float(activity.get("value") or activity.get("published_value"))
        published_units = _normalize_units(activity.get("units") or activity.get("published_units"))

        standard_type = _normalize_string(activity.get("standard_type"), uppercase=True)
        standard_relation = _normalize_relation(activity.get("standard_relation"), default="=")
        standard_value = _normalize_float(activity.get("standard_value"))
        standard_units = _normalize_units(activity.get("standard_units"), default="nM")
        standard_flag = _normalize_int(activity.get("standard_flag"))

        lower_bound = _normalize_float(activity.get("standard_lower_value") or activity.get("lower_value"))
        upper_bound = _normalize_float(activity.get("standard_upper_value") or activity.get("upper_value"))
        is_censored = _derive_is_censored(standard_relation)

        pchembl_value = _normalize_float(activity.get("pchembl_value"))
        activity_comment = _normalize_string(activity.get("activity_comment"))
        data_validity_comment = _normalize_string(activity.get("data_validity_comment"))

        bao_endpoint = _normalize_bao_id(activity.get("bao_endpoint"))
        bao_format = _normalize_bao_id(activity.get("bao_format"))
        bao_label = _normalize_string(activity.get("bao_label"), max_length=128)

        canonical_smiles = _normalize_string(activity.get("canonical_smiles"))
        target_organism = _normalize_target_organism(activity.get("target_organism"))
        target_tax_id = _normalize_int(activity.get("target_tax_id"))

        potential_duplicate = _normalize_int(activity.get("potential_duplicate"))
        uo_units = _normalize_string(activity.get("uo_units"), uppercase=True)
        qudt_units = _normalize_string(activity.get("qudt_units"))
        src_id = _normalize_int(activity.get("src_id"))
        action_type = _normalize_string(activity.get("action_type"))

        properties_str, properties = _normalize_activity_properties(activity.get("activity_properties"))
        ligand_efficiency = activity.get("ligand_efficiency") or activity.get("ligand_eff")
        bei, sei, le, lle = _normalize_ligand_efficiency(ligand_efficiency)

        compound_key = _derive_compound_key(molecule_id, standard_type, target_id)
        is_citation = _derive_is_citation(document_id, properties)
        exact_citation = _derive_exact_data_citation(data_validity_comment, properties)
        rounded_citation = _derive_rounded_data_citation(data_validity_comment, properties)
        high_citation_rate = _derive_high_citation_rate(properties)

        record: dict[str, Any] = {
            "activity_id": activity_id,
            "molecule_chembl_id": molecule_id,
            "assay_chembl_id": assay_id,
            "target_chembl_id": target_id,
            "document_chembl_id": document_id,
            "published_type": published_type,
            "published_relation": published_relation,
            "published_value": published_value,
            "published_units": published_units,
            "standard_type": standard_type,
            "standard_relation": standard_relation,
            "standard_value": standard_value,
            "standard_units": standard_units,
            "standard_flag": standard_flag,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "is_censored": is_censored,
            "pchembl_value": pchembl_value,
            "activity_comment": activity_comment,
            "data_validity_comment": data_validity_comment,
            "bao_endpoint": bao_endpoint,
            "bao_format": bao_format,
            "bao_label": bao_label,
            "canonical_smiles": canonical_smiles,
            "target_organism": target_organism,
            "target_tax_id": target_tax_id,
            "activity_properties": properties_str,
            "compound_key": compound_key,
            "is_citation": is_citation,
            "high_citation_rate": high_citation_rate,
            "exact_data_citation": exact_citation,
            "rounded_data_citation": rounded_citation,
            "potential_duplicate": potential_duplicate,
            "uo_units": uo_units,
            "qudt_units": qudt_units,
            "src_id": src_id,
            "action_type": action_type,
            "bei": bei,
            "sei": sei,
            "le": le,
            "lle": lle,
            "chembl_release": self._chembl_release,
            "source_system": "chembl",
            "fallback_reason": None,
            "fallback_error_type": None,
            "fallback_error_code": None,
            "fallback_error_message": None,
            "fallback_http_status": None,
            "fallback_retry_after_sec": None,
            "fallback_attempt": None,
            "fallback_timestamp": None,
            "run_id": self.run_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        return record

    def _create_fallback_record(
        self,
        activity_id: int,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Create deterministic fallback record enriched with error metadata."""

        record = {
            "activity_id": activity_id,
            "molecule_chembl_id": None,
            "assay_chembl_id": None,
            "target_chembl_id": None,
            "document_chembl_id": None,
            "published_type": None,
            "published_relation": "=",
            "published_value": None,
            "published_units": None,
            "standard_type": None,
            "standard_relation": "=",
            "standard_value": None,
            "standard_units": "nM",
            "standard_flag": None,
            "lower_bound": None,
            "upper_bound": None,
            "is_censored": None,
            "pchembl_value": None,
            "activity_comment": None,
            "data_validity_comment": None,
            "bao_endpoint": None,
            "bao_format": None,
            "bao_label": None,
            "canonical_smiles": None,
            "target_organism": None,
            "target_tax_id": None,
            "activity_properties": None,
            "compound_key": None,
            "is_citation": False,
            "high_citation_rate": False,
            "exact_data_citation": False,
            "rounded_data_citation": False,
            "potential_duplicate": None,
            "uo_units": None,
            "qudt_units": None,
            "src_id": None,
            "action_type": None,
            "bei": None,
            "sei": None,
            "le": None,
            "lle": None,
            "chembl_release": self._chembl_release,
            "source_system": "chembl",
            "fallback_reason": None,
            "fallback_error_type": None,
            "fallback_error_code": None,
            "fallback_error_message": None,
            "fallback_http_status": None,
            "fallback_retry_after_sec": None,
            "fallback_attempt": None,
            "fallback_timestamp": None,
            "run_id": self.run_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        metadata = build_fallback_payload(
            entity="activity",
            reason=reason,
            error=error,
            source="ChEMBL_FALLBACK",
            context={"chembl_release": self._chembl_release, "run_id": self.run_id},
        )
        record.update(metadata)

        return record

    def _cache_key(self, batch_ids: Iterable[int]) -> str:
        """Create deterministic cache key for a batch of IDs."""

        normalized = ",".join(map(str, sorted(batch_ids)))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _cache_path(self, batch_ids: Iterable[int]) -> Path:
        """Return the cache file path for a batch."""

        cache_dir = self._cache_base_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{self._cache_key(batch_ids)}.json"

    def _cache_base_dir(self) -> Path:
        """Build the base directory for cached responses."""

        base_dir = Path(self.config.cache.directory)
        if not base_dir.is_absolute():
            if self.config.paths.cache_root:
                base_dir = Path(self.config.paths.cache_root) / base_dir
            else:
                base_dir = Path(base_dir)

        entity_dir = base_dir / self.config.pipeline.entity
        if self.config.cache.release_scoped:
            release = self._chembl_release or "unknown"
            return entity_dir / release
        return entity_dir

    def _load_batch_from_cache(self, batch_ids: Iterable[int]) -> list[dict[str, Any]] | None:
        """Load cached batch if available."""

        if not self.config.cache.enabled:
            return None

        cache_path = self._cache_path(batch_ids)
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError:
            logger.warning("cache_corrupted", path=str(cache_path))
            cache_path.unlink(missing_ok=True)
            return None

        if not isinstance(data, list):
            logger.warning("cache_payload_unexpected", path=str(cache_path))
            return None

        ordered_records: list[dict[str, Any]] = []
        for raw_record in data:
            if not isinstance(raw_record, dict):
                logger.warning("cache_record_invalid", path=str(cache_path))
                return None

            ordered_records.append(dict(raw_record))

        return ordered_records

    def _store_batch_in_cache(self, batch_ids: Iterable[int], records: list[dict[str, Any]]) -> None:
        """Persist batch records into the local cache."""

        if not self.config.cache.enabled:
            return

        cache_path = self._cache_path(batch_ids)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        serializable = [
            dict(record)
            for record in sorted(
                records,
                key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")),
            )
        ]
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(serializable, handle, ensure_ascii=False)

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from the status endpoint."""

        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)
            self._status_snapshot = response

            version = response.get("chembl_db_version")
            release_date = response.get("chembl_release_date")
            activities = response.get("activities")

            if version:
                logger.info(
                    "chembl_version_fetched",
                    version=version,
                    release_date=release_date,
                    activities=activities,
                )
                return str(version)

            logger.warning("chembl_version_not_in_status_response")
        except Exception as error:  # noqa: BLE001 - handshake errors are non-fatal
            logger.warning("failed_to_get_chembl_version", error=str(error))

        return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform activity data."""
        if df.empty:
            return df

        df = df.copy()

        pipeline_version = getattr(self.config.pipeline, "version", None) or "1.0.0"
        df["pipeline_version"] = pipeline_version

        if "source_system" not in df.columns:
            df["source_system"] = "chembl"
        else:
            df["source_system"] = df["source_system"].fillna("chembl")

        release_value = self._chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip()

        if not release_value:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].where(
                    df["chembl_release"].notna(), pd.NA
                )
            else:
                df["chembl_release"] = pd.Series(pd.NA, index=df.index, dtype="string")
        else:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].fillna(release_value)
            else:
                df["chembl_release"] = release_value

        timestamp_now = datetime.now(timezone.utc).isoformat()
        if "extracted_at" in df.columns:
            df["extracted_at"] = df["extracted_at"].fillna(timestamp_now)
        else:
            df["extracted_at"] = timestamp_now

        _coerce_nullable_int_columns(df, INTEGER_COLUMNS_WITH_ID)

        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["activity_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        df = df.sort_values(["activity_id", "source_system"])
        df["index"] = range(len(df))

        self._update_fallback_artifacts(df)

        from bioetl.schemas import ActivitySchema

        expected_cols = _get_activity_column_order()
        if expected_cols:
            missing_columns: list[str] = []
            for col in expected_cols:
                if col not in df.columns:
                    missing_columns.append(col)
                    if col in INTEGER_COLUMNS_WITH_ID:
                        df[col] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                    elif col in FLOAT_COLUMNS:
                        df[col] = pd.Series(np.nan, index=df.index, dtype="float64")
                    else:
                        df[col] = pd.Series(pd.NA, index=df.index)
            if missing_columns:
                logger.debug(
                    "transform_missing_columns_filled",
                    columns=missing_columns,
                    total=len(missing_columns),
                )
            df = df[expected_cols]

        _coerce_nullable_int_columns(df, INTEGER_COLUMNS_WITH_ID)

        df = df.convert_dtypes()
        coerce_retry_after(df)

        _coerce_nullable_float_columns(df, FLOAT_COLUMNS)

        if "is_censored" in df.columns:
            df["is_censored"] = df["is_censored"].astype("boolean")

        for column in INTEGER_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data against schema."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self._last_validation_report = {
                "metrics": {},
                "schema_validation": {"status": "skipped"},
                "issues": [],
            }
            return df

        df = df.copy()

        expected_columns = _get_activity_column_order()
        if expected_columns:
            missing_columns = [column for column in expected_columns if column not in df.columns]
            if missing_columns:
                for column in missing_columns:
                    if column in INTEGER_COLUMNS_WITH_ID:
                        df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                    elif column in FLOAT_COLUMNS:
                        df[column] = pd.Series(np.nan, index=df.index, dtype="float64")
                    else:
                        df[column] = pd.Series(pd.NA, index=df.index)
                logger.debug(
                    "validation_missing_columns_filled",
                    columns=missing_columns,
                )

            extra_columns = [column for column in df.columns if column not in expected_columns]
            ordered_columns = list(expected_columns) + extra_columns
            if list(df.columns) != ordered_columns:
                logger.debug(
                    "validation_reordered_columns",
                    expected=len(expected_columns),
                    extras=extra_columns,
                )
                df = df[ordered_columns]

        coerce_retry_after(df)
        _coerce_nullable_int_columns(df, INTEGER_COLUMNS_WITH_ID)
        _coerce_nullable_float_columns(df, FLOAT_COLUMNS)

        qc_metrics = self._calculate_qc_metrics(df)
        fallback_stats = getattr(self, "_fallback_stats", None) or {}

        if fallback_stats:
            thresholds = getattr(self.config.qc, "thresholds", {}) or {}

            raw_count_threshold = thresholds.get("fallback.count")
            try:
                count_threshold = int(raw_count_threshold) if raw_count_threshold is not None else int(
                    fallback_stats.get("total_rows", len(df))
                )
            except (TypeError, ValueError):
                count_threshold = int(fallback_stats.get("total_rows", len(df)))

            raw_rate_threshold = thresholds.get("fallback.rate")
            try:
                rate_threshold = float(raw_rate_threshold) if raw_rate_threshold is not None else 1.0
            except (TypeError, ValueError):
                rate_threshold = 1.0

            fallback_count = int(fallback_stats.get("fallback_count", 0))
            fallback_rate = float(fallback_stats.get("fallback_rate", 0.0))

            count_severity = "info"
            if fallback_count > count_threshold:
                count_severity = "error"
            elif fallback_count > 0:
                count_severity = "warning"

            rate_severity = "info"
            if fallback_rate > rate_threshold:
                rate_severity = "error"
            elif fallback_rate > 0:
                rate_severity = "warning"

            qc_metrics["fallback.count"] = {
                "value": fallback_count,
                "threshold": count_threshold,
                "passed": fallback_count <= count_threshold,
                "severity": count_severity,
                "details": {
                    "activity_ids": fallback_stats.get("activity_ids", []),
                    "reason_counts": fallback_stats.get("reason_counts", {}),
                },
            }

            qc_metrics["fallback.rate"] = {
                "value": fallback_rate,
                "threshold": rate_threshold,
                "passed": fallback_rate <= rate_threshold,
                "severity": rate_severity,
                "details": {
                    "fallback_count": fallback_count,
                    "total_rows": fallback_stats.get("total_rows", len(df)),
                },
            }

            self.qc_summary_data.setdefault("metrics", {}).update(
                {
                    "fallback.count": qc_metrics["fallback.count"],
                    "fallback.rate": qc_metrics["fallback.rate"],
                }
            )

        self.qc_metrics = qc_metrics
        self.qc_summary_data.setdefault("metrics", {}).update(qc_metrics)
        self._last_validation_report = {"metrics": qc_metrics}

        severity_threshold = self.config.qc.severity_threshold.lower()
        severity_level_threshold = self._severity_level(severity_threshold)

        failing_metrics: dict[str, Any] = {}
        for metric_name, metric in qc_metrics.items():
            log_method = logger.error if not metric["passed"] else logger.info
            log_method(
                "qc_metric",
                metric=metric_name,
                value=metric["value"],
                threshold=metric["threshold"],
                severity=metric["severity"],
                details=metric.get("details"),
            )

            issue_payload: dict[str, Any] = {
                "metric": f"qc.{metric_name}",
                "issue_type": "qc_metric",
                "severity": metric.get("severity", "info"),
                "value": metric.get("value"),
                "threshold": metric.get("threshold"),
                "passed": metric.get("passed"),
            }
            if metric.get("details"):
                issue_payload["details"] = metric["details"]
            self.record_validation_issue(issue_payload)

            if (not metric["passed"]) and self._severity_level(metric["severity"]) >= severity_level_threshold:
                failing_metrics[metric_name] = metric

        if failing_metrics:
            self._last_validation_report["failing_metrics"] = failing_metrics
            if self._last_validation_report is not None:
                self._last_validation_report["issues"] = list(self.validation_issues)
            logger.error("qc_threshold_exceeded", failing_metrics=failing_metrics)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(failing_metrics.keys()))

        try:
            validated_df = ActivitySchema.validate(df, lazy=True)
        except SchemaErrors as exc:
            failure_cases = exc.failure_cases if hasattr(exc, "failure_cases") else None
            error_count = len(failure_cases) if failure_cases is not None else None
            schema_issue: dict[str, Any] = {
                "metric": "schema.validation",
                "issue_type": "schema_validation",
                "severity": "critical",
                "status": "failed",
                "errors": error_count,
            }

            if failure_cases is not None and not getattr(failure_cases, "empty", False):
                try:
                    grouped = failure_cases.groupby("column", dropna=False)
                    for column, group in grouped:
                        column_name = (
                            str(column)
                            if column is not None and not (isinstance(column, float) and pd.isna(column))
                            else "<dataframe>"
                        )
                        issue_details: dict[str, Any] = {
                            "metric": "schema.validation",
                            "issue_type": "schema_validation",
                            "severity": "critical",
                            "column": column_name,
                            "check": ", ".join(
                                sorted({str(check) for check in group["check"].dropna().unique()})
                            )
                            or "<unspecified>",
                            "count": int(group.shape[0]),
                        }
                        failure_examples = (
                            group["failure_case"].dropna().astype(str).unique().tolist()[:5]
                        )
                        if failure_examples:
                            issue_details["examples"] = failure_examples
                        self.record_validation_issue(issue_details)
                except Exception:  # pragma: no cover - defensive grouping fallback
                    schema_issue["details"] = "failed_to_group_failure_cases"

            self.record_validation_issue(schema_issue)

            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": error_count,
                    "failure_cases": failure_cases,
                }
                self._last_validation_report["issues"] = list(self.validation_issues)

            logger.error(
                "schema_validation_failed",
                error=str(exc),
                failure_count=error_count,
            )

            if self._severity_level("critical") >= severity_level_threshold:
                raise

            logger.warning(
                "schema_validation_below_threshold", severity_threshold=severity_threshold
            )
            return df

        self.record_validation_issue(
            {
                "metric": "schema.validation",
                "issue_type": "schema_validation",
                "severity": "info",
                "status": "passed",
                "errors": 0,
            }
        )

        if self._last_validation_report is not None:
            self._last_validation_report["schema_validation"] = {
                "status": "passed",
                "errors": 0,
            }
            self._last_validation_report["issues"] = list(self.validation_issues)

        logger.info("schema_validation_passed", rows=len(validated_df))
        return validated_df

    def _update_fallback_artifacts(self, df: pd.DataFrame) -> None:
        """Capture fallback diagnostics for QC reporting and additional outputs."""

        self._fallback_stats = {}

        if "source_system" not in df.columns:
            self.remove_additional_table("activity_fallback_records")
            self.qc_summary_data.pop("fallbacks", None)
            return

        source_series = df["source_system"].astype("string")
        fallback_mask = source_series.str.upper() == "CHEMBL_FALLBACK"

        total_rows = int(len(df))
        fallback_count = int(fallback_mask.sum())
        success_count = int(total_rows - fallback_count)
        fallback_rate = float(fallback_count / total_rows) if total_rows else 0.0

        fallback_columns = [
            "activity_id",
            "source_system",
            "fallback_reason",
            "fallback_error_type",
            "fallback_error_message",
            "fallback_http_status",
            "fallback_error_code",
            "fallback_retry_after_sec",
            "fallback_attempt",
            "fallback_timestamp",
            "chembl_release",
            "run_id",
            "extracted_at",
        ]
        available_columns = [column for column in fallback_columns if column in df.columns]

        fallback_records = (
            df.loc[fallback_mask, available_columns].copy()
            if fallback_count and available_columns
            else pd.DataFrame(columns=available_columns)
        )

        if not fallback_records.empty:
            fallback_records = fallback_records.reset_index(drop=True).convert_dtypes()
            coerce_retry_after(fallback_records)

        reason_counts: dict[str, int] = {}
        if fallback_count and "fallback_reason" in fallback_records.columns:
            counts = (
                fallback_records["fallback_reason"].fillna("<missing>")
                .value_counts(dropna=False)
                .to_dict()
            )
            reason_counts = {str(reason): int(count) for reason, count in counts.items()}

        fallback_ids: list[int] = []
        if "activity_id" in fallback_records.columns and not fallback_records.empty:
            id_series = pd.to_numeric(fallback_records["activity_id"], errors="coerce")
            fallback_ids = sorted({int(value) for value in id_series.dropna().astype(int).tolist()})

        fallback_summary = {
            "total_rows": total_rows,
            "success_count": success_count,
            "fallback_count": fallback_count,
            "fallback_rate": fallback_rate,
            "activity_ids": fallback_ids,
            "reason_counts": reason_counts,
        }

        self._fallback_stats = fallback_summary
        self.qc_summary_data["row_counts"] = {
            "total": total_rows,
            "success": success_count,
            "fallback": fallback_count,
        }
        self.qc_summary_data["fallbacks"] = fallback_summary

        if fallback_count:
            logger.warning(
                "chembl_fallback_records_detected",
                count=fallback_count,
                activity_ids=fallback_ids,
                reasons=reason_counts,
            )
            self.add_additional_table(
                "activity_fallback_records",
                fallback_records,
                relative_path=Path("qc") / "activity_fallback_records.csv",
            )
        else:
            self.remove_additional_table("activity_fallback_records")

    @property
    def last_validation_report(self) -> dict[str, Any] | None:
        """Return the most recent validation report."""
        return self._last_validation_report

    def _calculate_qc_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute QC metrics for validation."""
        qc_config = self.config.qc
        thresholds = qc_config.thresholds or {}

        metrics: dict[str, Any] = {}

        duplicate_threshold = thresholds.get("duplicates")
        duplicate_config = getattr(qc_config, "duplicate_check", None)
        duplicate_field = "activity_id"
        if duplicate_config and isinstance(duplicate_config, dict):
            duplicate_field = duplicate_config.get("field", duplicate_field)
            if duplicate_threshold is None:
                duplicate_threshold = duplicate_config.get("threshold")
        if duplicate_threshold is None:
            duplicate_threshold = 0

        duplicate_count = 0
        duplicate_values: list[Any] = []
        if duplicate_field in df.columns:
            duplicate_mask = df[duplicate_field].duplicated(keep=False)
            duplicate_count = int(df[duplicate_field].duplicated().sum())
            duplicate_values = df.loc[duplicate_mask, duplicate_field].tolist()

        metrics["duplicates"] = {
            "value": duplicate_count,
            "threshold": duplicate_threshold,
            "passed": duplicate_count <= duplicate_threshold,
            "severity": "error" if duplicate_count > duplicate_threshold else "info",
            "details": {"field": duplicate_field, "duplicate_values": duplicate_values},
        }

        critical_columns = ["standard_value", "standard_type", "molecule_chembl_id"]
        null_rates: dict[str, float] = {}
        column_thresholds: dict[str, float] = {}

        def _coerce_threshold(value: Any | None, default: float) -> float:
            """Return a float threshold, preserving explicit zero values."""

            if value is None:
                return float(default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        null_rate_threshold = _coerce_threshold(thresholds.get("null_rate_critical"), 1.0)
        null_threshold_default = _coerce_threshold(
            thresholds.get("activity.null_fraction"), null_rate_threshold
        )

        for column in critical_columns:
            column_present = column in df.columns
            if column_present and len(df) > 0:
                null_fraction = float(df[column].isna().mean())
                null_count = int(df[column].isna().sum())
            elif column_present:
                null_fraction = 0.0
                null_count = 0
            else:
                null_fraction = 1.0
                null_count = len(df)

            column_threshold = _coerce_threshold(
                thresholds.get(f"activity.null_fraction.{column}"), null_threshold_default
            )

            null_rates[column] = null_fraction
            column_thresholds[column] = column_threshold

            metrics[f"null_fraction.{column}"] = {
                "value": null_fraction,
                "threshold": column_threshold,
                "passed": null_fraction <= column_threshold,
                "severity": "error" if null_fraction > column_threshold else "info",
                "details": {
                    "column": column,
                    "column_present": column_present,
                    "null_count": null_count,
                },
            }

        max_null_rate = max(null_rates.values()) if null_rates else 0.0
        metrics["null_rate"] = {
            "value": max_null_rate,
            "threshold": null_rate_threshold,
            "passed": max_null_rate <= null_rate_threshold,
            "severity": "error" if max_null_rate > null_rate_threshold else "info",
            "details": {
                "column_null_rates": null_rates,
                "column_thresholds": column_thresholds,
            },
        }

        invalid_units_threshold = float(thresholds.get("invalid_units", 0))
        invalid_units_count = 0
        invalid_indices: list[int] = []
        invalid_fraction = 0.0
        if {"standard_value", "standard_units"}.issubset(df.columns) and len(df) > 0:
            mask = df["standard_value"].notna() & df["standard_units"].isna()
            invalid_units_count = int(mask.sum())
            invalid_indices = df.index[mask].tolist()
            invalid_fraction = invalid_units_count / len(df)

        metrics["invalid_units"] = {
            "value": invalid_units_count,
            "threshold": invalid_units_threshold,
            "passed": invalid_units_count <= invalid_units_threshold,
            "severity": "error" if invalid_units_count > invalid_units_threshold else "info",
            "details": {"invalid_row_indices": invalid_indices, "invalid_fraction": invalid_fraction},
        }

        return metrics

    @staticmethod
    def _severity_level(severity: str) -> int:
        """Map severity string to comparable level."""
        levels = {"info": 0, "warning": 1, "error": 2, "critical": 3}
        return levels.get(severity.lower(), 1)

