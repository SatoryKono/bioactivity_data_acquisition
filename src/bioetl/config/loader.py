"""Configuration loading utilities."""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, Iterable

import yaml

from .models import PipelineConfig

DEFAULT_PROFILE_PATHS: tuple[Path, ...] = (
    Path("configs/profiles/base.yaml"),
    Path("configs/profiles/determinism.yaml"),
)


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

    requested_profiles: list[Path] = []
    if include_default_profiles:
        requested_profiles.extend(DEFAULT_PROFILE_PATHS)
    if profiles:
        requested_profiles.extend(Path(p).expanduser() for p in profiles)

    merged: MutableMapping[str, Any] = {}
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

    cli_metadata: MutableMapping[str, Any] = {}
    if applied_profiles:
        cli_metadata = {
            "cli": {
                "profiles": [_stringify_profile(p, base=path.parent) for p in applied_profiles]
            }
        }

    if cli_overrides:
        cli_tree: MutableMapping[str, Any] = {}
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

    env_overrides = _collect_env_overrides(env or os.environ, prefixes=env_prefixes)
    if env_overrides:
        merged = _deep_merge(merged, env_overrides)

    return PipelineConfig.model_validate(merged)


def _load_with_extends(path: Path, *, stack: Iterable[Path]) -> MutableMapping[str, Any]:
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

    merged: MutableMapping[str, Any] = {}
    for reference in extends or ():
        reference_path = _resolve_reference(reference, base=resolved.parent)
        merged = _deep_merge(merged, _load_with_extends(reference_path, stack=(*lineage, resolved)))

    return _deep_merge(merged, data)


def _load_yaml(path: Path) -> Any:
    """Load a YAML file supporting ``!include`` directives."""

    class Loader(yaml.SafeLoader):
        pass

    def construct_include(loader: Loader, node: yaml.Node) -> Any:  # type: ignore[override]
        filename = loader.construct_scalar(node)
        include_path = _resolve_reference(filename, base=path.parent)
        return _load_yaml(include_path)

    Loader.add_constructor("!include", construct_include)

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.load(handle, Loader=Loader)
    return {} if data is None else data


def _ensure_mapping(value: Any, path: Path) -> MutableMapping[str, Any]:
    if not isinstance(value, MutableMapping):
        msg = f"Configuration file must produce a mapping: {path}"
        raise TypeError(msg)
    return dict(value)


def _resolve_reference(value: str | Path, *, base: Path) -> Path:
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
    base: MutableMapping[str, Any],
    override: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    merged: MutableMapping[str, Any] = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], MutableMapping)
            and isinstance(value, Mapping)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _assign_nested(target: MutableMapping[str, Any], parts: Sequence[str], value: Any) -> None:
    current = target
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], MutableMapping):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _coerce_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return yaml.safe_load(value)
        except yaml.YAMLError:
            return value
    return value


def _collect_env_overrides(env: Mapping[str, str], *, prefixes: Sequence[str]) -> MutableMapping[str, Any]:
    overrides: MutableMapping[str, Any] = {}
    for key, raw_value in env.items():
        prefix = next((p for p in prefixes if key.startswith(p)), None)
        if prefix is None:
            continue
        trimmed = key[len(prefix) :]
        if not trimmed:
            continue
        parts = [segment.lower() for segment in trimmed.split("__") if segment]
        if not parts:
            continue
        _assign_nested(overrides, parts, _coerce_value(raw_value))
    return overrides


def _stringify_profile(profile: Path, *, base: Path) -> str:
    try:
        return str(profile.relative_to(base))
    except ValueError:
        return str(profile)
