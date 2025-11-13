"""Configuration loading utilities."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, TypeGuard, cast

import yaml
from yaml.nodes import ScalarNode

from .models.base import PipelineConfig

DEFAULTS_DIR = Path("configs/defaults")
ENV_ROOT_DIR = Path("configs/env")
ENVIRONMENT_VARIABLE = "BIOETL_ENV"
VALID_ENVIRONMENTS: frozenset[str] = frozenset({"dev", "stage", "prod"})
_LAYER_GLOB_PATTERNS: tuple[str, ...] = ("*.yaml", "*.yml")


def _is_non_string_iterable(value: Any) -> TypeGuard[Iterable[Any]]:
    """Return True when ``value`` is an iterable but not a string-like object."""
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes))


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
) -> PipelineConfig:
    """Load, merge, and validate a pipeline configuration.

    The loader performs the following steps:

    1. Recursively resolves and merges any ``extends`` references declared by the
       configuration file or by the optional ``profiles`` argument.
    2. Applies overrides supplied via ``cli_overrides`` (such as ``--set`` flags).
    3. Applies overrides from environment variables starting with one of the
       ``env_prefixes`` (``BIOETL__`` or ``BIOACTIVITY__`` by default).
    4. Validates the merged mapping against :class:`PipelineConfig`.

    Parameters
    ----------
    config_path:
        Path to the main pipeline configuration YAML file.
    profiles:
        Additional profile files to merge before the main configuration.
    cli_overrides:
        Mapping of dotted keys to override values coming from the CLI.
    env:
        Mapping used to resolve environment overrides. ``os.environ`` is used if
        omitted.
    env_prefixes:
        Allowed prefixes for environment overrides.
    include_default_profiles:
        When ``True``, automatically prepends the built-in ``base`` and
        ``determinism`` profiles.
    """

    # Resolve relative paths relative to current working directory first
    candidate = Path(config_path).expanduser()
    if candidate.is_absolute():
        path = candidate.resolve()
    else:
        # Try current working directory first for relative paths
        cwd_path = (Path.cwd() / candidate).resolve()
        if cwd_path.exists():
            path = cwd_path
        else:
            # Fall back to resolving relative to candidate's parent
            path = candidate.resolve()

    if not path.exists():
        msg = f"Configuration file not found: {path}"
        raise FileNotFoundError(msg)

    env_mapping: Mapping[str, str] = env or os.environ
    selected_environment = _select_environment(env_mapping)

    requested_profiles: list[Path] = []
    if include_default_profiles:
        requested_profiles.extend(_discover_layer_files(DEFAULTS_DIR, base=path.parent))
    if profiles:
        requested_profiles.extend(Path(p).expanduser() for p in profiles)

    merged: dict[str, Any] = {}
    seen_profiles: set[Path] = set()
    applied_profiles: list[Path] = []
    for profile_path in requested_profiles:
        resolved_profile = _resolve_reference(profile_path, base=path.parent)
        if resolved_profile in seen_profiles:
            continue
        profile_data = _load_with_extends(resolved_profile, stack=())
        merged = _deep_merge(merged, profile_data)
        seen_profiles.add(resolved_profile)
        applied_profiles.append(resolved_profile)

    config_data = _load_with_extends(path, stack=())
    merged = _deep_merge(merged, config_data)

    applied_environment_profiles: list[Path] = []
    if selected_environment is not None:
        env_payload, applied_environment_profiles = _load_environment_overrides(
            selected_environment, base=path.parent
        )
        if env_payload:
            merged = _deep_merge(merged, env_payload)

    cli_metadata: dict[str, Any] = {}
    cli_section: dict[str, Any] | None = None
    if applied_profiles:
        if cli_section is None:
            cli_section = {}
        cli_section["profiles"] = [
            _stringify_profile(p, base=path.parent) for p in applied_profiles
        ]
    if applied_environment_profiles:
        if cli_section is None:
            cli_section = {}
        cli_section["environment_profiles"] = [
            _stringify_profile(p, base=path.parent) for p in applied_environment_profiles
        ]
    if selected_environment is not None:
        if cli_section is None:
            cli_section = {}
        cli_section["environment"] = selected_environment
    if cli_section:
        cli_metadata = {"cli": cli_section}

    if cli_overrides:
        cli_tree: dict[str, Any] = {}
        parsed_overrides: dict[str, Any] = {}
        for dotted_key, raw_value in cli_overrides.items():
            parsed_value = _coerce_value(raw_value)
            parsed_overrides[dotted_key] = parsed_value
            _assign_nested(cli_tree, dotted_key.split("."), parsed_value)
        merged = _deep_merge(merged, cli_tree)
        if cli_metadata:
            cli_metadata.setdefault("cli", {})
        cli_metadata.setdefault("cli", {}).setdefault("set_overrides", {}).update(parsed_overrides)

    if cli_metadata:
        merged = _deep_merge(merged, cli_metadata)

    env_overrides = _collect_env_overrides(env_mapping, prefixes=env_prefixes)
    if env_overrides:
        merged = _deep_merge(merged, env_overrides)

    normalized = _migrate_legacy_sections(merged)
    return PipelineConfig.model_validate(normalized)


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

    return migrated


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
            elif _is_non_string_iterable(merge_value):
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
    current = target
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], MutableMapping):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


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
        scoped_tree: dict[str, Any] = {}
        for raw_key, raw_value in scoped_items:
            parts = [segment.strip().lower() for segment in raw_key.split("__") if segment.strip()]
            if not parts:
                continue
            parsed_value = _coerce_value(raw_value)
            _assign_nested(scoped_tree, parts, parsed_value)
        if scoped_tree:
            overrides = _deep_merge(overrides, scoped_tree)
    return overrides


def _select_environment(env_mapping: Mapping[str, str]) -> str | None:
    """Resolve and validate the active environment from the mapping."""
    raw_value = env_mapping.get(ENVIRONMENT_VARIABLE)
    if raw_value is None:
        return None
    normalized = raw_value.strip().lower()
    if not normalized:
        return None
    if normalized not in VALID_ENVIRONMENTS:
        msg = (
            f"Unsupported environment '{raw_value}' for {ENVIRONMENT_VARIABLE}. "
            f"Expected one of: {sorted(VALID_ENVIRONMENTS)}"
        )
        raise ValueError(msg)
    return normalized


def _load_environment_overrides(
    environment: str,
    *,
    base: Path,
) -> tuple[dict[str, Any], list[Path]]:
    """Load environment-specific configuration layers and return applied files."""
    env_directory = ENV_ROOT_DIR / environment
    files = _discover_layer_files(env_directory, base=base, strict=True)
    payload: dict[str, Any] = {}
    applied: list[Path] = []
    seen: set[Path] = set()
    for file_path in files:
        if file_path in seen:
            continue
        data = _load_with_extends(file_path, stack=())
        payload = _deep_merge(payload, data)
        applied.append(file_path)
        seen.add(file_path)
    return payload, applied


def _discover_layer_files(
    directory: Path,
    *,
    base: Path,
    strict: bool = False,
) -> list[Path]:
    """Discover configuration layer files under ``directory``."""
    resolved_dir = _resolve_layer_directory(directory, base=base)
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


def _resolve_layer_directory(directory: Path, *, base: Path) -> Path:
    """Resolve a directory relative to known roots."""
    if directory.is_absolute():
        return directory.resolve()

    search_roots: list[Path] = [base, *base.parents, Path.cwd()]
    for root in search_roots:
        candidate = (root / directory).resolve()
        if candidate.exists():
            return candidate

    return (Path.cwd() / directory).resolve()


def _stringify_profile(profile: Path, *, base: Path) -> str:
    """Represent a profile path relative to ``base`` when possible."""
    try:
        return str(profile.relative_to(base))
    except ValueError:
        return str(profile)
