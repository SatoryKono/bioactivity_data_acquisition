from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from typing import Any, cast

import yaml

from .helpers import build_env_overrides


def _apply_yaml_merge(payload: Any) -> Any:
    if isinstance(payload, MutableMapping):
        result: dict[str, Any] = {}
        merge_value = payload.get("<<")
        sources: list[Mapping[str, Any]] = []
        if merge_value is not None:
            if isinstance(merge_value, Mapping):
                sources.append(cast(Mapping[str, Any], merge_value))
            elif isinstance(merge_value, Sequence):
                sources.extend(cast(Sequence[Mapping[str, Any]], merge_value))
            else:
                msg = "YAML merge source must be a mapping or sequence"
                raise TypeError(msg)
        for source in sources:
            result = _deep_merge(result, source)
        for key, value in payload.items():
            if key == "<<":
                continue
            result[str(key)] = _apply_yaml_merge(value)
        return result
    if isinstance(payload, list):
        return [_apply_yaml_merge(item) for item in payload]
    return payload


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], MutableMapping)
            and isinstance(value, Mapping)
        ):
            merged[key] = _deep_merge(
                cast(Mapping[str, Any], merged[key]), cast(Mapping[str, Any], value)
            )
        else:
            merged[key] = value
    return merged


def _collect_env_overrides(env: Mapping[str, str], *, prefixes: Sequence[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for prefix in prefixes:
        for key, value in env.items():
            if not key.startswith(prefix):
                continue
            remainder = key[len(prefix) :]
            if not remainder:
                continue
            parts = [part.lower() for part in remainder.split("__") if part]
            if not parts:
                continue
            overrides = _deep_merge(overrides, build_env_overrides(((parts, _coerce(value)),)))
    return overrides


def _collect_short_env_overrides(settings: Any) -> dict[str, Any]:
    pairs = []
    mapping = settings.model_dump(exclude_none=True)
    if "pubmed_tool" in mapping:
        pairs.append((("sources", "pubmed", "http", "identify", "tool"), mapping["pubmed_tool"]))
    if "pubmed_email" in mapping:
        pairs.append((("sources", "pubmed", "http", "identify", "email"), mapping["pubmed_email"]))
    if "pubmed_api_key" in mapping:
        pairs.append(
            (("sources", "pubmed", "http", "identify", "api_key"), mapping["pubmed_api_key"])
        )
    if "crossref_mailto" in mapping:
        pairs.append(
            (("sources", "crossref", "http", "identify", "mailto"), mapping["crossref_mailto"])
        )
    if "semantic_scholar_api_key" in mapping:
        pairs.append(
            (
                ("sources", "semantic_scholar", "http", "headers", "x-api-key"),
                mapping["semantic_scholar_api_key"],
            )
        )
    if "iuphar_api_key" in mapping:
        pairs.append(
            (("sources", "iuphar", "http", "headers", "x-api-key"), mapping["iuphar_api_key"])
        )
    return build_env_overrides(pairs)


def _coerce(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return yaml.safe_load(value)
        except yaml.YAMLError:
            return value
    return value


__all__ = [
    "_apply_yaml_merge",
    "_coerce",
    "_collect_env_overrides",
    "_collect_short_env_overrides",
    "_deep_merge",
]
