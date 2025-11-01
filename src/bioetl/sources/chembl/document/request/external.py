"""External adapter orchestration for the document pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd

from bioetl.adapters.base import AdapterConfig, AdapterFetchError
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.sources.document.pipeline import AdapterDefinition
from bioetl.utils.config import coerce_float_config, coerce_int_config
from bioetl.utils.dtypes import coerce_optional_bool

logger = UnifiedLogger.get(__name__)

_ID_TYPE_HINTS: dict[str, str] = {
    "pubmed": "pmid",
    "crossref": "doi",
    "openalex": "doi",
    "semantic_scholar": "pmid",
}

_ID_COLUMN_HINTS: dict[str, dict[str, tuple[str, ...]]] = {
    "pubmed": {
        "pmid": ("pubmed_pmid", "pmid", "pubmed_id"),
        "doi": ("pubmed_doi", "doi_clean", "doi"),
    },
    "crossref": {
        "doi": ("crossref_doi_clean", "crossref_doi", "doi_clean", "doi"),
    },
    "openalex": {
        "doi": ("openalex_doi_clean", "openalex_doi", "doi_clean", "doi"),
        "pmid": ("openalex_pmid",),
    },
    "semantic_scholar": {
        "pmid": ("semantic_scholar_pmid", "semantic_scholar_pubmed_id", "pubmed_id"),
        "doi": ("semantic_scholar_doi", "doi_clean", "doi"),
        "title": (
            "semantic_scholar_title",
            "semantic_scholar_title_for_join",
            "_semantic_scholar_title_key",
            "_title_for_join",
            "title",
        ),
    },
}

_DEFAULT_ID_COLUMNS: dict[str, tuple[str, ...]] = {
    "pmid": ("pmid",),
    "doi": ("doi_clean", "doi"),
    "title": ("title",),
}


def build_adapter_configs(
    config: PipelineConfig,
    source_name: str,
    source_cfg: Any,
    definition: AdapterDefinition,
) -> tuple[APIConfig, AdapterConfig]:
    """Construct API and adapter configuration objects for a source."""

    def _log(event: str, **kwargs: Any) -> None:
        logger.warning(event, source=source_name, **kwargs)

    cache_enabled = bool(config.cache.enabled)
    cache_enabled_raw = _get_source_attribute(source_cfg, "cache_enabled")
    if cache_enabled_raw is not None:
        coerced = coerce_optional_bool(cache_enabled_raw)
        if coerced is not pd.NA:
            cache_enabled = bool(coerced)

    cache_ttl_default = int(config.cache.ttl)
    cache_ttl_raw = _get_source_attribute(source_cfg, "cache_ttl")
    cache_ttl = coerce_int_config(
        cache_ttl_raw,
        cache_ttl_default,
        field="cache_ttl",
        log=_log,
        invalid_event="adapter_config_invalid_int",
    )

    cache_maxsize_default = getattr(config.cache, "maxsize", None)
    if cache_maxsize_default is None:
        cache_maxsize_default = APIConfig.__dataclass_fields__["cache_maxsize"].default  # type: ignore[index]
    cache_maxsize_raw = _get_source_attribute(source_cfg, "cache_maxsize")
    cache_maxsize: int = coerce_int_config(
        cache_maxsize_raw,
        int(cache_maxsize_default),
        field="cache_maxsize",
        log=_log,
        invalid_event="adapter_config_invalid_int",
    )

    http_profiles = getattr(config, "http", None)
    global_http = http_profiles.get("global") if isinstance(http_profiles, Mapping) else None

    timeout_default: float | None = None
    connect_default: float | None = None
    read_default: float | None = None
    if global_http is not None:
        timeout_default = getattr(global_http, "timeout_sec", None)
        if timeout_default is not None:
            timeout_default = float(timeout_default)
        connect_default = getattr(global_http, "connect_timeout_sec", None)
        if connect_default is not None:
            connect_default = float(connect_default)
        read_default = getattr(global_http, "read_timeout_sec", None)
        if read_default is not None:
            read_default = float(read_default)

    if connect_default is None:
        connect_default = (
            timeout_default
            if timeout_default is not None
            else float(APIConfig.__dataclass_fields__["timeout_connect"].default)  # type: ignore[index]
        )

    if read_default is None:
        read_default = (
            timeout_default
            if timeout_default is not None
            else float(APIConfig.__dataclass_fields__["timeout_read"].default)  # type: ignore[index]
        )

    if timeout_default is None:
        timeout_default = read_default

    timeout_override = _get_source_attribute(source_cfg, "timeout")
    if timeout_override is None:
        timeout_override = _get_source_attribute(source_cfg, "timeout_sec")

    timeout_value = coerce_float_config(
        timeout_override,
        float(timeout_default),
        field="timeout",
        log=_log,
        invalid_event="adapter_config_invalid_float",
    )

    connect_override = _get_source_attribute(source_cfg, "connect_timeout_sec")
    connect_default_final = float(timeout_value) if timeout_override is not None else float(connect_default)
    timeout_connect = coerce_float_config(
        connect_override,
        connect_default_final,
        field="connect_timeout_sec",
        log=_log,
        invalid_event="adapter_config_invalid_float",
    )

    read_override = _get_source_attribute(source_cfg, "read_timeout_sec")
    read_default_final = float(timeout_value) if timeout_override is not None else float(read_default)
    timeout_read = coerce_float_config(
        read_override,
        read_default_final,
        field="read_timeout_sec",
        log=_log,
        invalid_event="adapter_config_invalid_float",
    )

    api_kwargs: dict[str, Any] = {
        "name": source_name,
        "cache_enabled": cache_enabled,
        "cache_ttl": cache_ttl,
        "cache_maxsize": cache_maxsize,
        "timeout_connect": timeout_connect,
        "timeout_read": timeout_read,
    }

    for field_name, spec in definition.api_fields.items():
        raw_value = _get_source_attribute(source_cfg, field_name)
        value = _resolve_field_value(raw_value, spec)
        api_kwargs[field_name] = value

    adapter_kwargs: dict[str, Any] = {"enabled": True}
    for field_name, spec in definition.adapter_fields.items():
        raw_value = _get_source_attribute(source_cfg, field_name)
        value = _resolve_field_value(raw_value, spec)
        adapter_kwargs[field_name] = value

    api_config = APIConfig(**api_kwargs)
    adapter_config = AdapterConfig(**adapter_kwargs)
    return api_config, adapter_config


def init_external_adapters(
    config: PipelineConfig,
    definitions: Mapping[str, AdapterDefinition],
) -> dict[str, Any]:
    """Initialise external adapters using structured definitions."""

    adapters: dict[str, Any] = {}
    sources = config.sources

    for source_name, definition in definitions.items():
        source_cfg = sources.get(source_name)
        if source_cfg is None:
            continue

        enabled = bool(_get_source_attribute(source_cfg, "enabled", True))
        if not enabled:
            logger.info("adapter_skipped", source=source_name, reason="disabled")
            continue

        api_config, adapter_config = build_adapter_configs(config, source_name, source_cfg, definition)
        adapter = definition.adapter_cls(api_config, adapter_config)
        adapters[source_name] = adapter

    logger.info("adapters_initialized", count=len(adapters))
    return adapters


def run_enrichment_requests(
    adapters: Mapping[str, Any],
    *,
    pmids: Sequence[str],
    dois: Sequence[str],
    titles: Sequence[str],
    records: Sequence[Mapping[str, str]] | None = None,
    timeout: float = 300.0,
) -> tuple[
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    dict[str, str],
    dict[str, dict[str, Any]],
]:
    """Execute enrichment requests across adapters in parallel."""

    if not adapters:
        return None, None, None, None, {}, {}

    pmid_list = list(dict.fromkeys(pmids)) if pmids else []
    doi_list = list(dict.fromkeys(dois)) if dois else []
    title_list = list(dict.fromkeys(titles)) if titles else []
    record_list = list(records) if records else []

    pubmed_df = None
    crossref_df = None
    openalex_df = None
    semantic_scholar_df = None
    adapter_errors: dict[str, str] = {}
    requested: dict[str, dict[str, Any]] = {}

    def _submit(
        source: str,
        func_name: str,
        payload: Sequence[str],
        executor: ThreadPoolExecutor,
    ) -> None:
        adapter = adapters.get(source)
        if adapter is None or not payload:
            return
        unique_payload = _unique_preserve(payload)
        if not unique_payload:
            return
        requested[source] = {"ids": unique_payload, "type": _ID_TYPE_HINTS.get(source, "doi")}
        method = getattr(adapter, func_name)
        futures[source] = executor.submit(method, unique_payload)

    workers = min(4, len(adapters)) or 1
    futures: dict[str, Any] = {}

<<<<<<< HEAD
        adapter_inputs: dict[str, dict[str, Sequence[str] | list[Mapping[str, str]]]] = {
            "pubmed": {
                "pmids": pmid_list,
                "dois": doi_list,
                "titles": title_list,
                "records": record_list,
            },
            "crossref": {
                "pmids": pmid_list,
                "dois": doi_list,
                "titles": title_list,
                "records": record_list,
            },
            "openalex": {
                "pmids": pmid_list,
                "dois": doi_list,
                "titles": title_list,
                "records": record_list,
            },
            "semantic_scholar": {
                "pmids": pmid_list,
                "dois": doi_list,
                "titles": title_list,
                "records": record_list,
            },
        }

        for source, adapter in adapters.items():
            inputs = adapter_inputs.get(source)
            if inputs is None:
                continue
            futures[source] = executor.submit(_dispatch_adapter, adapter, inputs)
=======
    with ThreadPoolExecutor(max_workers=workers) as executor:
        if "pubmed" in adapters and pmids:
            _submit("pubmed", "process", pmids, executor)

        if "crossref" in adapters and dois:
            requested["crossref"] = {"ids": _unique_preserve(dois), "type": "doi"}
            futures["crossref"] = executor.submit(adapters["crossref"].process, requested["crossref"]["ids"])

        if "openalex" in adapters and dois:
            requested["openalex"] = {"ids": _unique_preserve(dois), "type": "doi"}
            futures["openalex"] = executor.submit(adapters["openalex"].process, requested["openalex"]["ids"])

        if "semantic_scholar" in adapters:
            if pmids:
                requested["semantic_scholar"] = {"ids": _unique_preserve(pmids), "type": "pmid"}
                futures["semantic_scholar"] = executor.submit(
                    adapters["semantic_scholar"].process,
                    requested["semantic_scholar"]["ids"],
                )
            elif titles:
                requested["semantic_scholar"] = {"ids": _unique_preserve(titles), "type": "title"}
                futures["semantic_scholar"] = executor.submit(
                    adapters["semantic_scholar"].process_titles,
                    requested["semantic_scholar"]["ids"],
                )
>>>>>>> origin/codex/enhance-enrichment-request-handling

        for source, future in futures.items():
            try:
                result = future.result(timeout=timeout)
            except AdapterFetchError as exc:
                error_message = str(exc) or f"{source} adapter failed"
                adapter_errors[source] = error_message
                logger.error(
                    "adapter_failed",
                    source=source,
                    error=error_message,
                    failed_ids=getattr(exc, "failed_ids", None) or None,
                )
                continue
            except Exception as exc:  # noqa: BLE001
                error_message = str(exc) or exc.__class__.__name__
                adapter_errors[source] = error_message
                logger.error("adapter_failed", source=source, error=error_message)
                continue

            if result is None:
                continue

            rows = len(result) if hasattr(result, "__len__") else 0
            logger.info("adapter_completed", source=source, rows=rows)
            if rows:
                logger.info("adapter_columns", source=source, columns=list(result.columns))

            results[source] = result

    coverage_summary: dict[str, dict[str, Any]] = {}

    for source in results.keys():
        request_info = requested.get(source)
        frame = results[source]
        coverage_entry = _compute_coverage_entry(source, request_info, frame)
        coverage_summary[source] = coverage_entry

    for source, request_info in requested.items():
        if source in adapter_errors:
            continue

        entry = coverage_summary[source]
        initial_missing = set(entry.get("initial_missing_ids", entry["missing_ids"]))
        if not initial_missing:
            continue

        adapter = adapters.get(source)
        fallback_df = _run_adapter_fallback(adapter, initial_missing)
        if fallback_df is not None:
            entry["fallback_attempted"] = True
            results[source] = _combine_frames(results[source], fallback_df)
            updated_entry = _compute_coverage_entry(source, request_info, results[source])
            recovered = sorted(initial_missing - set(updated_entry["missing_ids"]))
            entry.update(
                {
                    "matched": updated_entry["matched"],
                    "coverage": updated_entry["coverage"],
                    "missing_ids": updated_entry["missing_ids"],
                }
            )
            entry["recovered_ids"] = recovered
            if recovered:
                logger.info(
                    "adapter_fallback_recovered",
                    source=source,
                    recovered=recovered,
                )
        elif entry.get("initial_missing_ids"):
            entry["fallback_attempted"] = True

    for source, entry in coverage_summary.items():
        missing = entry.get("missing_ids", [])
        if entry.get("requested", 0) and missing:
            message = f"missing_identifiers: {', '.join(missing)}"
            logger.warning("adapter_missing_identifiers", source=source, missing=missing)
            if source in adapter_errors:
                adapter_errors[source] = f"{adapter_errors[source]}; {message}"
            else:
                adapter_errors[source] = message

    pubmed_df = results["pubmed"]
    crossref_df = results["crossref"]
    openalex_df = results["openalex"]
    semantic_scholar_df = results["semantic_scholar"]

    return (
        pubmed_df,
        crossref_df,
        openalex_df,
        semantic_scholar_df,
        adapter_errors,
        coverage_summary,
    )


def _dispatch_adapter(adapter: Any, identifiers: Mapping[str, Sequence[str] | list[Mapping[str, str]]]) -> pd.DataFrame | None:
    """Invoke adapter-specific processing with identifier fallbacks."""

    process_identifiers = getattr(adapter, "process_identifiers", None)
    if callable(process_identifiers):
        return process_identifiers(**identifiers)

    payload: dict[str, Sequence[str]] = {
        key: list(value) if isinstance(value, list) else list(value)
        for key, value in identifiers.items()
        if key != "records"
    }

    pmid_ids = payload.get("pmids") or []
    doi_ids = payload.get("dois") or []
    title_values = payload.get("titles") or []

    if doi_ids and hasattr(adapter, "process"):
        return adapter.process(list(doi_ids))

    if pmid_ids and hasattr(adapter, "process"):
        return adapter.process(list(pmid_ids))

    if title_values and hasattr(adapter, "process_titles"):
        return adapter.process_titles(list(title_values))

    if hasattr(adapter, "process"):
        return adapter.process([])

    return None


def collect_enrichment_metrics(
    frames: Mapping[str, pd.DataFrame | None],
    errors: Mapping[str, str],
    coverage: Mapping[str, Mapping[str, Any]] | None = None,
) -> pd.DataFrame:
    """Build a metrics dataframe summarising enrichment activity."""

    coverage = coverage or {}
    records: list[dict[str, Any]] = []
    for source, frame in frames.items():
        rows = int(len(frame)) if frame is not None else 0
        status = "failed" if source in errors else "completed"
        coverage_entry = coverage.get(source, {})
        missing_ids = list(coverage_entry.get("missing_ids", []))
        record = {
            "source": source,
            "rows": rows,
            "status": status,
            "requested": int(coverage_entry.get("requested", 0)),
            "matched": int(coverage_entry.get("matched", 0)),
            "coverage": float(coverage_entry.get("coverage", 1.0)),
            "missing_count": len(missing_ids),
            "missing_ids": missing_ids,
            "fallback_attempted": bool(coverage_entry.get("fallback_attempted", False)),
            "recovered_ids": list(coverage_entry.get("recovered_ids", [])),
        }
        error_message = errors.get(source)
        if error_message:
            record["error"] = error_message
        records.append(record)

    for source, error in errors.items():
        if source not in frames:
            coverage_entry = coverage.get(source, {})
            records.append(
                {
                    "source": source,
                    "rows": 0,
                    "status": "failed",
                    "error": error,
                    "requested": int(coverage_entry.get("requested", 0)),
                    "matched": int(coverage_entry.get("matched", 0)),
                    "coverage": float(coverage_entry.get("coverage", 0.0)),
                    "missing_count": len(list(coverage_entry.get("missing_ids", []))),
                    "missing_ids": list(coverage_entry.get("missing_ids", [])),
                    "fallback_attempted": bool(coverage_entry.get("fallback_attempted", False)),
                    "recovered_ids": list(coverage_entry.get("recovered_ids", [])),
                }
            )

    if not records:
        return pd.DataFrame(
            columns=[
                "source",
                "rows",
                "status",
                "error",
                "requested",
                "matched",
                "coverage",
                "missing_count",
                "missing_ids",
                "fallback_attempted",
                "recovered_ids",
            ]
        )

    df = pd.DataFrame(records).convert_dtypes()
    df = df.sort_values(by="source")
    return df.reset_index(drop=True)


def _unique_preserve(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for item in items:
        if item is None:
            continue
        candidate = str(item).strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        unique.append(candidate)
    return unique


def _compute_coverage_entry(
    source: str,
    request_info: Mapping[str, Any] | None,
    frame: pd.DataFrame | None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "requested": 0,
        "matched": 0,
        "coverage": 1.0,
        "missing_ids": [],
        "initial_missing_ids": [],
        "recovered_ids": [],
        "fallback_attempted": False,
    }

    if not request_info:
        return entry

    requested_ids = list(request_info.get("ids", []))
    id_type = str(request_info.get("type", "doi"))
    normalised_map: dict[str, str] = {}
    invalid_ids: set[str] = set()
    seen_original: set[str] = set()

    for original in requested_ids:
        text = str(original).strip()
        if not text or text in seen_original:
            continue
        seen_original.add(text)
        normalised = _normalise_identifier(original, id_type)
        if normalised is None:
            invalid_ids.add(text)
            continue
        normalised_map.setdefault(normalised, text)

    requested_total = len(normalised_map) + len(invalid_ids)
    if requested_total == 0:
        return entry

    results = _collect_normalised_results(frame, source, id_type)
    matched = len(results.intersection(normalised_map.keys()))
    missing_all: list[str] = []
    for original in requested_ids:
        text = str(original).strip()
        if not text or text in missing_all:
            continue
        normalised = _normalise_identifier(original, id_type)
        if normalised is None:
            if text in invalid_ids:
                missing_all.append(text)
            continue
        if normalised not in results:
            representative = normalised_map.get(normalised, text)
            if representative not in missing_all:
                missing_all.append(representative)

    entry.update(
        {
            "requested": requested_total,
            "matched": matched,
            "coverage": float(matched / requested_total) if requested_total else 1.0,
            "missing_ids": missing_all,
            "initial_missing_ids": list(missing_all),
        }
    )
    return entry


def _collect_normalised_results(
    frame: pd.DataFrame | None,
    source: str,
    id_type: str,
) -> set[str]:
    if frame is None or getattr(frame, "empty", True):
        return set()

    columns = _ID_COLUMN_HINTS.get(source, {}).get(id_type)
    if not columns:
        columns = _DEFAULT_ID_COLUMNS.get(id_type, ("id",))

    results: set[str] = set()
    for column in columns:
        if column not in frame.columns:
            continue
        series = frame[column]
        for value in series.tolist():
            normalised = _normalise_identifier(value, id_type)
            if normalised is not None:
                results.add(normalised)
    return results


def _normalise_identifier(value: Any, id_type: str) -> str | None:
    if value is None:
        return None

    if isinstance(value, float) and pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if id_type == "pmid":
        cleaned = text.replace("PMID", "").replace("pmid", "")
        cleaned = cleaned.replace(":", " ").replace(",", " ")
        cleaned = cleaned.strip()
        try:
            return str(int(float(cleaned)))
        except (TypeError, ValueError):
            digits = "".join(ch for ch in text if ch.isdigit())
            if digits:
                return str(int(digits))
            return None

    if id_type == "doi":
        lowered = text.lower()
        if lowered.startswith("doi:"):
            lowered = lowered[4:]
        return lowered

    if id_type == "title":
        return " ".join(text.lower().split())

    return text


def _run_adapter_fallback(adapter: Any, missing_ids: set[str]) -> pd.DataFrame | None:
    if adapter is None or not missing_ids:
        return None

    fallback_method = getattr(adapter, "process_with_fallback", None)
    if not callable(fallback_method):
        fallback_method = getattr(adapter, "process_fallback", None)
        if not callable(fallback_method):
            return None

    try:
        return fallback_method(sorted(missing_ids))
    except TypeError:
        try:
            return fallback_method(sorted(missing_ids), mode="fallback")
        except TypeError:
            return fallback_method(sorted(missing_ids), context={"missing": sorted(missing_ids)})
        except Exception as exc:  # noqa: BLE001
            logger.error("adapter_fallback_failed", source=getattr(adapter, "__class__", type(adapter)).__name__, error=str(exc))
            return None
    except Exception as exc:  # noqa: BLE001
        logger.error("adapter_fallback_failed", source=getattr(adapter, "__class__", type(adapter)).__name__, error=str(exc))
        return None


def _combine_frames(
    primary: pd.DataFrame | None,
    fallback: pd.DataFrame | None,
) -> pd.DataFrame | None:
    if fallback is None:
        return primary
    if primary is None or getattr(primary, "empty", True):
        return fallback
    if getattr(fallback, "empty", True):
        return primary

    combined = pd.concat([primary, fallback], ignore_index=True)
    combined = combined.drop_duplicates().reset_index(drop=True)
    return combined


def _get_source_attribute(source_cfg: Any, attr: str, default: Any = None) -> Any:
    if isinstance(source_cfg, dict):
        return source_cfg.get(attr, default)
    return getattr(source_cfg, attr, default)


def _resolve_field_value(raw_value: Any, spec: Any) -> Any:
    default_value = spec.get_default()
    value = default_value if raw_value is None else raw_value
    value = _apply_env_substitutions(value)

    if (value is None or (isinstance(value, str) and value == "")) and spec.env:
        env_value = os.getenv(spec.env)
        if env_value is not None:
            value = env_value

    if (
        isinstance(default_value, (int, float))
        and not isinstance(default_value, bool)
        and isinstance(value, str)
        and value
    ):
        try:
            value = type(default_value)(value)
        except ValueError:
            pass

    if spec.coalesce_default_on_blank and isinstance(value, str) and not value.strip():
        value = default_value

    if value is None:
        value = default_value

    return value


def _apply_env_substitutions(value: Any) -> Any:
    if isinstance(value, str):
        return _resolve_env_reference(value)
    if isinstance(value, dict):
        return {key: _apply_env_substitutions(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_apply_env_substitutions(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_apply_env_substitutions(item) for item in value)
    return value


def _resolve_env_reference(value: str) -> str:
    candidate = value.strip()
    if candidate.startswith("env:"):
        env_name = candidate.split(":", 1)[1]
        return os.getenv(env_name, "")
    if candidate.startswith("${") and candidate.endswith("}"):
        env_name = candidate[2:-1]
        return os.getenv(env_name, "")
    return value


__all__ = [
    "build_adapter_configs",
    "init_external_adapters",
    "run_enrichment_requests",
    "collect_enrichment_metrics",
]
