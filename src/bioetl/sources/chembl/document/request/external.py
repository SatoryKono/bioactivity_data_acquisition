"""External adapter orchestration for the document pipeline."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd

from bioetl.adapters.base import AdapterConfig
from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.sources.document.pipeline import AdapterDefinition
from bioetl.utils.config import coerce_float_config, coerce_int_config
from bioetl.utils.dtypes import coerce_optional_bool

logger = UnifiedLogger.get(__name__)


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
    cache_maxsize = coerce_int_config(
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
    timeout: float = 300.0,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, dict[str, str]]:
    """Execute enrichment requests across adapters in parallel."""

    if not adapters:
        return None, None, None, None, {}

    pubmed_df = None
    crossref_df = None
    openalex_df = None
    semantic_scholar_df = None
    adapter_errors: dict[str, str] = {}

    workers = min(4, len(adapters)) or 1
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[str, Any] = {}

        if "pubmed" in adapters and pmids:
            futures["pubmed"] = executor.submit(adapters["pubmed"].process, pmids)

        if "crossref" in adapters and dois:
            futures["crossref"] = executor.submit(adapters["crossref"].process, dois)

        if "openalex" in adapters and dois:
            futures["openalex"] = executor.submit(adapters["openalex"].process, dois)

        if "semantic_scholar" in adapters:
            if pmids:
                futures["semantic_scholar"] = executor.submit(adapters["semantic_scholar"].process, pmids)
            elif titles:
                futures["semantic_scholar"] = executor.submit(
                    adapters["semantic_scholar"].process_titles,
                    titles,
                )

        for source, future in futures.items():
            try:
                result = future.result(timeout=timeout)
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

            if source == "pubmed":
                pubmed_df = result
            elif source == "crossref":
                crossref_df = result
            elif source == "openalex":
                openalex_df = result
            elif source == "semantic_scholar":
                semantic_scholar_df = result

    return pubmed_df, crossref_df, openalex_df, semantic_scholar_df, adapter_errors


def collect_enrichment_metrics(
    frames: Mapping[str, pd.DataFrame | None],
    errors: Mapping[str, str],
) -> pd.DataFrame:
    """Build a metrics dataframe summarising enrichment activity."""

    records: list[dict[str, Any]] = []
    for source, frame in frames.items():
        rows = int(len(frame)) if frame is not None else 0
        status = "failed" if source in errors else "completed"
        records.append({"source": source, "rows": rows, "status": status})

    for source, error in errors.items():
        if source not in frames:
            records.append({"source": source, "rows": 0, "status": "failed", "error": error})

    if not records:
        return pd.DataFrame(columns=["source", "rows", "status", "error"])

    df = pd.DataFrame(records).convert_dtypes()
    df = df.sort_values(by="source")
    return df.reset_index(drop=True)


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
