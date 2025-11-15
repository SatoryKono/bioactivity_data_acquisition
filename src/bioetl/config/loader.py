"""Configuration loading utilities."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, TypeGuard, cast

import yaml
from yaml.nodes import ScalarNode

from bioetl.core.utils.iterables import is_non_string_iterable

from .environment import (
    EnvironmentSettings,
    build_env_override_mapping,
    load_environment_settings,
    resolve_env_layers,
)
from .helpers import build_env_overrides, resolve_directory
from .models.base import PipelineConfig

DEFAULTS_DIR = Path("configs/defaults")
_LAYER_GLOB_PATTERNS: tuple[str, ...] = ("*.yaml", "*.yml")


def load_raw_config(path: Path) -> dict[str, Any]:
    """Load a configuration file with support for ``extends``."""

    return _load_with_extends(path, stack=())


def apply_profiles(
    profile_paths: Sequence[Path],
    *,
    base_dir: Path,
) -> tuple[dict[str, Any], list[Path]]:
    """Apply profile files in order and return merged payload with metadata."""

    merged: dict[str, Any] = {}
    seen: set[Path] = set()
    applied: list[Path] = []

    for profile_path in profile_paths:
        resolved_profile = _resolve_reference(profile_path, base=base_dir)
        if resolved_profile in seen:
            continue
        profile_data = load_raw_config(resolved_profile)
        merged = _deep_merge(merged, profile_data)
        seen.add(resolved_profile)
        applied.append(resolved_profile)

    return merged, applied


def apply_env_overrides(
    payload: Mapping[str, Any],
    *,
    environment_files: Sequence[Path],
    env_mapping: Mapping[str, str],
    env_prefixes: Sequence[str],
    runtime_overrides: Mapping[str, Any],
) -> tuple[dict[str, Any], list[Path]]:
    """Apply environment layers, short overrides, and prefixed env vars."""

    merged: dict[str, Any] = dict(payload)
    env_payload, applied_files = _load_layer_files(environment_files)
    if env_payload:
        merged = _deep_merge(merged, env_payload)
    if runtime_overrides:
        merged = _deep_merge(merged, runtime_overrides)
    env_overrides = _collect_env_overrides(env_mapping, prefixes=env_prefixes)
    if env_overrides:
        merged = _deep_merge(merged, env_overrides)
    return merged, applied_files


def apply_cli_overrides(
    payload: Mapping[str, Any],
    *,
    cli_overrides: Mapping[str, Any] | None,
    cli_metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """Apply CLI ``--set`` overrides and attach metadata."""

    merged: dict[str, Any] = dict(payload)
    metadata: dict[str, Any] = dict(cli_metadata or {})

    if cli_overrides:
        path_value_pairs: list[tuple[Sequence[str], Any]] = []
        parsed_overrides: dict[str, Any] = {}
        for dotted_key, raw_value in cli_overrides.items():
            parsed_value = _coerce_value(raw_value)
            parsed_overrides[dotted_key] = parsed_value
            path_value_pairs.append((tuple(dotted_key.split(".")), parsed_value))
        cli_tree = build_env_overrides(path_value_pairs)
        if cli_tree:
            merged = _deep_merge(merged, cli_tree)
        metadata.setdefault("cli", {}).setdefault("set_overrides", {}).update(parsed_overrides)

    if metadata:
        merged = _deep_merge(merged, metadata)

    return merged


def finalize_pipeline_config(payload: Mapping[str, Any]) -> PipelineConfig:
    """Validate the fully merged payload."""

    return PipelineConfig.model_validate(payload)


def _resolve_config_path(config_path: str | Path) -> Path:
    """Normalize config path resolution logic."""

    candidate = Path(config_path).expanduser()
    if candidate.is_absolute():
        path = candidate.resolve()
    else:
        cwd_path = (Path.cwd() / candidate).resolve()
        if cwd_path.exists():
            path = cwd_path
        else:
            path = candidate.resolve()

    if not path.exists():
        msg = f"Configuration file not found: {path}"
        raise FileNotFoundError(msg)
    return path


def _load_layer_files(files: Sequence[Path]) -> tuple[dict[str, Any], list[Path]]:
    """Load a sequence of YAML files and return the merged payload + list."""

    payload: dict[str, Any] = {}
    applied: list[Path] = []
    seen: set[Path] = set()

    for file_path in files:
        resolved = file_path.resolve()
        if resolved in seen:
            continue
        data = load_raw_config(resolved)
        payload = _deep_merge(payload, data)
        applied.append(resolved)
        seen.add(resolved)

    return payload, applied


def _build_cli_metadata(
    *,
    profiles: Sequence[Path],
    environment_profiles: Sequence[Path],
    environment_name: str | None,
    base_dir: Path,
) -> dict[str, Any]:
    """Collect CLI metadata about applied layers."""

    cli_section: dict[str, Any] = {}

    if profiles:
        cli_section["profiles"] = [
            _stringify_profile(profile, base=base_dir) for profile in profiles
        ]
    if environment_profiles:
        cli_section["environment_profiles"] = [
            _stringify_profile(profile, base=base_dir) for profile in environment_profiles
        ]
    if environment_name is not None:
        cli_section["environment"] = environment_name

    if not cli_section:
        return {}

    return {"cli": cli_section}


def _selected_environment(settings: EnvironmentSettings) -> str | None:
    """Return the active environment when explicitly provided via env/config."""

    return settings.bioetl_env
def _is_any_list(value: Any) -> TypeGuard[list[Any]]:
    """Return True when ``value`` is a list instance."""
    return isinstance(value, list)


def load_config(
    config_path: str | Path,
    *,
    profiles: Sequence[str | Path] | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    env_prefixes: Sequence[str] = ("BIOETL__", "BIOACTIVITY__"),
    include_default_profiles: bool = False,
    environment_settings: EnvironmentSettings | None = None,
) -> PipelineConfig:
    """Load, merge, and validate a pipeline configuration.

    Order слоёв: raw YAML → profiles → environment (файлы + env/short overrides)
    → CLI overrides → финальная валидация.
    """

    path = _resolve_config_path(config_path)
    env_settings = environment_settings or load_environment_settings()
    env_mapping: Mapping[str, str] = env or os.environ
    selected_environment = _selected_environment(env_settings)

    requested_profiles: list[Path] = []
    if include_default_profiles:
        requested_profiles.extend(_discover_layer_files(DEFAULTS_DIR, base=path.parent))
    if profiles:
        requested_profiles.extend(Path(p).expanduser() for p in profiles)

    profile_payload, applied_profiles = apply_profiles(
        requested_profiles,
        base_dir=path.parent,
    )

    merged = _deep_merge(profile_payload, load_raw_config(path))

    env_layer_files = resolve_env_layers(
        selected_environment,
        base=path.parent,
    )
    merged, applied_environment_profiles = apply_env_overrides(
        merged,
        environment_files=env_layer_files,
        env_mapping=env_mapping,
        env_prefixes=env_prefixes,
        runtime_overrides=build_env_override_mapping(env_settings),
    )

    metadata = _build_cli_metadata(
        profiles=applied_profiles,
        environment_profiles=applied_environment_profiles,
        environment_name=selected_environment,
        base_dir=path.parent,
    )

    merged = apply_cli_overrides(
        merged,
        cli_overrides=cli_overrides,
        cli_metadata=metadata,
    )

    normalized = _migrate_legacy_sections(merged)
    return finalize_pipeline_config(normalized)


def _load_with_extends(path: Path, *, stack: Iterable[Path]) -> dict[str, Any]:
    """Load a YAML file and merge any declared ``extends`` recursively."""

    resolved = path.resolve()
    lineage = list(stack)
    if resolved in lineage:
        cycle = " -> ".join(str(p) for p in (*lineage, resolved))
        msg = f"Circular extends detected: {cycle}"
        raise ValueError(msg)

    data = _ensure_mapping(_load_yaml(resolved), resolved)
    extends = data.pop("extends", ())
    if isinstance(extends, (str, Path)):
        extends = (extends,)

    merged: dict[str, Any] = {}
    for reference in extends or ():
        reference_path = _resolve_reference(reference, base=resolved.parent)
        merged = _deep_merge(merged, _load_with_extends(reference_path, stack=(*lineage, resolved)))

    return _deep_merge(merged, data)


def _migrate_legacy_sections(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize legacy configuration sections before validation."""

    migrated: dict[str, Any] = dict(payload)
    clients_section = migrated.pop("clients", None)

    if isinstance(clients_section, Mapping):
        chembl_legacy = clients_section.get("chembl")
        if isinstance(chembl_legacy, Mapping):
            chembl_target = migrated.get("chembl")
            if isinstance(chembl_target, Mapping):
                chembl_payload: dict[str, Any] = dict(chembl_target)
            else:
                chembl_payload = {}

            status_endpoint = chembl_legacy.get("status_endpoint")
            if isinstance(status_endpoint, str):
                normalized_status = status_endpoint.strip()
                if normalized_status and "status_endpoint" not in chembl_payload:
                    chembl_payload["status_endpoint"] = normalized_status

            if chembl_payload:
                migrated["chembl"] = chembl_payload

    _maybe_normalize_http_headers(migrated)
    return migrated


def _maybe_normalize_http_headers(payload: MutableMapping[str, Any]) -> None:
    """Ensure HTTP header keys follow canonical formatting."""

    http_section = payload.get("http")
    if not isinstance(http_section, Mapping):
        return

    normalized_http = _normalize_http_section(http_section)
    if normalized_http is not None:
        payload["http"] = normalized_http


def _normalize_http_section(http_section: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return normalized HTTP section when header keys require updates."""

    normalized: dict[str, Any] = dict(http_section)
    changed = False

    default_block = http_section.get("default")
    normalized_default = _normalize_http_header_block(default_block)
    if normalized_default is not None:
        normalized["default"] = normalized_default
        changed = True

    profiles_block = http_section.get("profiles")
    if isinstance(profiles_block, Mapping):
        profiles_payload: dict[str, Any] = dict(profiles_block)
        profiles_changed = False
        for profile_name, profile_payload in profiles_block.items():
            normalized_profile = _normalize_http_header_block(profile_payload)
            if normalized_profile is not None:
                profiles_payload[profile_name] = normalized_profile
                profiles_changed = True
        if profiles_changed:
            normalized["profiles"] = profiles_payload
            changed = True

    if not changed:
        return None
    return normalized


def _normalize_http_header_block(block: Any) -> dict[str, Any] | None:
    """Normalize header keys inside a single HTTP client block."""

    if not isinstance(block, Mapping):
        return None

    normalized_block: dict[str, Any] = dict(block)
    headers = block.get("headers")
    normalized_headers = _normalize_header_mapping(headers)
    if normalized_headers is None:
        return None

    normalized_block["headers"] = normalized_headers
    return normalized_block


def _normalize_header_mapping(headers: Any) -> dict[str, Any] | None:
    """Convert snake_case header keys into canonical HTTP header names."""

    if not isinstance(headers, Mapping):
        return None

    normalized: dict[str, Any] = {}
    changed = False

    for raw_key, raw_value in headers.items():
        if isinstance(raw_key, str):
            formatted_key = _format_http_header_name(raw_key)
            if formatted_key != raw_key:
                changed = True
            normalized_key: Any = formatted_key
        else:
            normalized_key = raw_key
        normalized[normalized_key] = raw_value

    if not changed:
        return None
    return normalized


def _format_http_header_name(raw_key: str) -> str:
    """Convert snake_case/bare header names into Header-Case format."""

    stripped = raw_key.strip()
    if not stripped:
        return raw_key
    if "-" in stripped or not stripped.islower():
        return stripped

    parts = [part for part in stripped.split("_") if part]
    if not parts:
        return stripped
    formatted = "-".join(part.capitalize() for part in parts)
    return formatted


def _load_yaml(path: Path) -> Any:
    """Load a YAML file supporting ``!include`` directives."""

    class Loader(yaml.SafeLoader):
        """Custom YAML loader adding ``!include`` support."""

    def construct_include(loader: Loader, node: ScalarNode) -> Any:
        """Resolve ``!include`` directives relative to the current file."""
        filename = loader.construct_scalar(node)
        include_path = _resolve_reference(filename, base=path.parent)
        return _load_yaml(include_path)

    Loader.add_constructor("!include", construct_include)

    with path.open("r", encoding="utf-8") as handle:
        raw_text = handle.read()

    normalized_text = raw_text.replace("<<:", "__merge__:")
    data = yaml.load(normalized_text, Loader=Loader)
    if data is None:
        return {}
    return _apply_yaml_merge(data)


def _convert_mapping_to_string_keys(
    value: Any,
    *,
    context: str,
) -> dict[str, Any]:
    """Ensure a mapping uses string keys and raise with context on failure."""
    candidate_mapping = cast(Mapping[Any, Any], value)
    normalized: dict[str, Any] = {}
    invalid_keys: list[Any] = []
    for raw_key, raw_value in candidate_mapping.items():
        key_any: Any = raw_key
        value_any: Any = raw_value
        if isinstance(key_any, str):
            normalized[key_any] = value_any
        else:
            invalid_keys.append(key_any)
    if invalid_keys:
        keys = ", ".join(map(str, invalid_keys))
        msg = f"{context} (invalid keys: {keys})"
        raise TypeError(msg)
    return normalized


def _apply_yaml_merge(payload: Any) -> Any:
    """Recursively normalise YAML merge keys and return a merged structure."""
    if isinstance(payload, MutableMapping):
        typed_payload = cast(MutableMapping[str, Any], payload)
        result: dict[str, Any] = {}

        merge_value: Any | None = typed_payload.get("__merge__")
        if merge_value is None:
            merge_value = typed_payload.get("<<")

        if merge_value is not None:
            def _normalize_merge_source(candidate: Any) -> Mapping[str, Any]:
                """Normalize individual YAML merge candidates into mappings."""
                merged_candidate_any: Any = _apply_yaml_merge(candidate)
                if not isinstance(merged_candidate_any, Mapping):
                    msg = "YAML merge source must be a mapping"
                    raise TypeError(msg)
                normalized = _convert_mapping_to_string_keys(
                    merged_candidate_any,
                    context="YAML merge source must use string keys",
                )
                return normalized

            if isinstance(merge_value, Mapping):
                typed_sources: tuple[Mapping[str, Any], ...] = (
                    _normalize_merge_source(merge_value),
                )
            elif is_non_string_iterable(merge_value):
                merge_iterable: Iterable[Any] = merge_value
                typed_sources = tuple(
                    _normalize_merge_source(source_any) for source_any in merge_iterable
                )
            else:
                typed_sources = (
                    _normalize_merge_source(merge_value),
                )

            for merged_source in typed_sources:
                result = _deep_merge(result, merged_source)

        for raw_key, raw_value in typed_payload.items():
            if raw_key in ("<<", "__merge__"):
                continue

            processed_value = _apply_yaml_merge(raw_value)
            key_str = str(raw_key)

            existing_value = result.get(key_str)
            if isinstance(existing_value, Mapping) and isinstance(processed_value, Mapping):
                existing_mapping = cast(Mapping[str, Any], existing_value)
                processed_mapping = cast(Mapping[str, Any], processed_value)
                result[key_str] = _deep_merge(existing_mapping, processed_mapping)
            else:
                result[key_str] = processed_value

        return result

    if _is_any_list(payload):
        payload_list: list[Any] = payload
        return [
            _apply_yaml_merge(element_any)
            for element_any in payload_list
        ]

    return payload


def _ensure_mapping(value: Any, path: Path) -> dict[str, Any]:
    """Validate that a YAML payload is a mapping and enforce string keys."""
    if not isinstance(value, MutableMapping):
        msg = f"Configuration file must produce a mapping: {path}"
        raise TypeError(msg)
    return _convert_mapping_to_string_keys(
        value,
        context=f"Configuration mapping must use string keys: {path}",
    )


def _resolve_reference(value: str | Path, *, base: Path) -> Path:
    """Resolve configuration references relative to ``base`` and the filesystem."""
    candidate = Path(value).expanduser()
    search_paths: list[Path] = []

    if candidate.is_absolute():
        search_paths.append(candidate)
    else:
        search_paths.extend((root / candidate) for root in (base, *base.parents))
        search_paths.append(Path.cwd() / candidate)
        search_paths.append(candidate)

    seen: set[Path] = set()
    for raw_path in search_paths:
        potential = raw_path.expanduser().resolve()
        if potential in seen:
            continue
        seen.add(potential)
        if potential.exists():
            return potential

    msg = f"Referenced configuration file not found: {value} (resolved from {base})"
    raise FileNotFoundError(msg)


def _deep_merge(
    base: Mapping[str, Any],
    override: Mapping[str, Any],
) -> dict[str, Any]:
    """Recursively merge two mapping-like objects."""
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], MutableMapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(
                cast(Mapping[str, Any], merged[key]),
                cast(Mapping[str, Any], value),
            )
        else:
            merged[key] = value
    return merged


def _assign_nested(target: MutableMapping[str, Any], parts: Sequence[str], value: Any) -> None:
    """Assign a value to a nested mapping according to dotted parts."""

    built = build_env_overrides(((tuple(parts), value),))
    if not built:
        return
    merged = _deep_merge(target, built)
    target.clear()
    target.update(merged)


def _coerce_value(value: Any) -> Any:
    """Best-effort conversion of CLI/environment override values."""
    if isinstance(value, str):
        try:
            return yaml.safe_load(value)
        except yaml.YAMLError:
            return value
    return value


def _collect_env_overrides(env: Mapping[str, str], *, prefixes: Sequence[str]) -> dict[str, Any]:
    """Collect prefixed environment variables and build a nested override tree."""
    overrides: dict[str, Any] = {}
    for prefix in prefixes:
        if not prefix:
            continue
        scoped_items = (
            (key[len(prefix) :], value)
            for key, value in env.items()
            if key.startswith(prefix) and key[len(prefix) :]
        )
        scoped_pairs: list[tuple[Sequence[str], Any]] = []
        for raw_key, raw_value in scoped_items:
            parts = [segment.strip().lower() for segment in raw_key.split("__") if segment.strip()]
            if not parts:
                continue
            parsed_value = _coerce_value(raw_value)
            scoped_pairs.append((tuple(parts), parsed_value))
        scoped_tree = build_env_overrides(scoped_pairs)
        if scoped_tree:
            overrides = _deep_merge(overrides, scoped_tree)
    return overrides


def _discover_layer_files(
    directory: Path,
    *,
    base: Path,
    strict: bool = False,
) -> list[Path]:
    """Discover configuration layer files under ``directory``."""
    resolved_dir = resolve_directory(directory, base=base)
    if not resolved_dir.exists():
        if strict:
            msg = f"Configuration directory not found: {resolved_dir}"
            raise FileNotFoundError(msg)
        return []
    files: set[Path] = set()
    for pattern in _LAYER_GLOB_PATTERNS:
        for item in resolved_dir.glob(pattern):
            if item.is_file():
                files.add(item.resolve())
    return sorted(files)


def _stringify_profile(profile: Path, *, base: Path) -> str:
    """Represent a profile path relative to ``base`` when possible."""
    try:
        return str(profile.relative_to(base))
    except ValueError:
        return str(profile)
