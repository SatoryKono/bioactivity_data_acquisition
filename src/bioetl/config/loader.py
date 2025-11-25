from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, Iterable, cast

import yaml
from yaml.nodes import ScalarNode

from .environment import load_environment_settings
from .helpers import build_env_overrides
from .models import PipelineConfig

DEFAULT_PROFILE_DIR = Path("configs/defaults")
_LAYER_PATTERNS: tuple[str, ...] = ("*.yaml", "*.yml")


def load_config(
    config_path: str | Path,
    *,
    profiles: Sequence[str | Path] | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    env_prefixes: Sequence[str] = ("BIOETL__",),
    include_default_profiles: bool = False,
) -> PipelineConfig:
    """Load, merge and validate the pipeline configuration.

    Merge priority (low → high): profiles → main config → CLI ``--set`` overrides
    → prefixed environment variables. ``extends`` and ``!include`` are resolved
    eagerly for every loaded YAML file.
    """

    path = _resolve_config_path(config_path)
    base_dir = path.parent

    profile_paths: list[Path] = []
    if include_default_profiles:
        profile_paths.extend(_discover_layer_files(DEFAULT_PROFILE_DIR, base=base_dir))
    if profiles:
        profile_paths.extend(Path(p).expanduser() for p in profiles)

    profile_payload = _merge_layers(profile_paths, base=base_dir)
    main_payload = load_raw_config(path)
    merged = _deep_merge(profile_payload, main_payload)

    if cli_overrides:
        nested = build_env_overrides(
            (tuple(key.split(".")), value) for key, value in cli_overrides.items()
        )
        merged = _deep_merge(merged, nested)

    env_mapping = env or os.environ
    env_overrides = _collect_env_overrides(env_mapping, prefixes=env_prefixes)
    merged = _deep_merge(merged, env_overrides)

    settings = load_environment_settings()
    env_settings_overrides = _collect_short_env_overrides(settings)
    merged = _deep_merge(merged, env_settings_overrides)

    return PipelineConfig.model_validate(merged)


def load_raw_config(path: Path) -> dict[str, Any]:
    """Load a YAML config file with support for ``extends``."""

    return _load_with_extends(path, stack=())


# internal helpers ---------------------------------------------------------


def _resolve_config_path(config_path: str | Path) -> Path:
    candidate = Path(config_path).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    if not candidate.exists():
        msg = f"Configuration file not found: {candidate}"
        raise FileNotFoundError(msg)
    return candidate


def _merge_layers(layer_paths: Sequence[Path], *, base: Path) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    seen: set[Path] = set()
    for layer in layer_paths:
        resolved = _resolve_reference(layer, base=base)
        if resolved in seen:
            continue
        payload = load_raw_config(resolved)
        merged = _deep_merge(merged, payload)
        seen.add(resolved)
    return merged


def _load_with_extends(path: Path, *, stack: Iterable[Path]) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    lineage = list(stack)
    if resolved in lineage:
        chain = " -> ".join(str(p) for p in (*lineage, resolved))
        raise ValueError(f"Circular extends detected: {chain}")

    data = _ensure_mapping(_load_yaml(resolved), resolved)
    extends = data.pop("extends", None)
    parents: list[Path] = []
    if isinstance(extends, (str, Path)):
        parents = [Path(extends)]
    elif isinstance(extends, Sequence):
        parents = [Path(p) for p in extends]

    merged: dict[str, Any] = {}
    for parent in parents:
        parent_path = _resolve_reference(parent, base=resolved.parent)
        parent_payload = _load_with_extends(parent_path, stack=(*lineage, resolved))
        merged = _deep_merge(merged, parent_payload)

    return _deep_merge(merged, data)


def _load_yaml(path: Path) -> Any:
    class Loader(yaml.SafeLoader):
        pass

    def construct_include(loader: Loader, node: ScalarNode) -> Any:
        filename = loader.construct_scalar(node)
        include_path = _resolve_reference(Path(filename), base=path.parent)
        return _load_yaml(include_path)

    Loader.add_constructor("!include", construct_include)

    with path.open("r", encoding="utf-8") as handle:
        try:
            raw = handle.read()
        except OSError as exc:
            raise FileNotFoundError(path) from exc

    try:
        loaded = yaml.load(raw, Loader=Loader)
    except yaml.YAMLError:
        raise

    if loaded is None:
        return {}
    return _apply_yaml_merge(loaded)


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


def _ensure_mapping(data: Any, path: Path) -> dict[str, Any]:
    if isinstance(data, Mapping):
        return dict(data)
    msg = f"Configuration file must contain a mapping at top-level: {path}"
    raise TypeError(msg)


def _resolve_reference(value: str | Path, *, base: Path) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        resolved = candidate.expanduser().resolve()
        if not resolved.exists():
            raise FileNotFoundError(resolved)
        return resolved

    search_paths = [base / candidate, *base.parents, Path.cwd() / candidate]
    for candidate_path in search_paths:
        resolved = candidate_path.expanduser().resolve()
        if resolved.exists():
            return resolved
    raise FileNotFoundError(candidate)


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


def _coerce(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return yaml.safe_load(value)
        except yaml.YAMLError:
            return value
    return value


def _discover_layer_files(directory: Path, *, base: Path) -> list[Path]:
    resolved_dir = (base / directory).resolve() if not directory.is_absolute() else directory
    if not resolved_dir.exists():
        return []
    files: list[Path] = []
    for pattern in _LAYER_PATTERNS:
        files.extend(sorted(resolved_dir.glob(pattern)))
    return files


def _collect_short_env_overrides(settings: Any) -> dict[str, Any]:
    pairs = []
    mapping = settings.model_dump(exclude_none=True)
    if "pubmed_tool" in mapping:
        pairs.append((("sources", "pubmed", "http", "identify", "tool"), mapping["pubmed_tool"]))
    if "pubmed_email" in mapping:
        pairs.append((("sources", "pubmed", "http", "identify", "email"), mapping["pubmed_email"]))
    if "pubmed_api_key" in mapping:
        pairs.append((
            ("sources", "pubmed", "http", "identify", "api_key"),
            mapping["pubmed_api_key"],
        ))
    if "crossref_mailto" in mapping:
        pairs.append((
            ("sources", "crossref", "http", "identify", "mailto"),
            mapping["crossref_mailto"],
        ))
    if "semantic_scholar_api_key" in mapping:
        pairs.append((
            ("sources", "semantic_scholar", "http", "headers", "x-api-key"),
            mapping["semantic_scholar_api_key"],
        ))
    if "iuphar_api_key" in mapping:
        pairs.append((
            ("sources", "iuphar", "http", "headers", "x-api-key"),
            mapping["iuphar_api_key"],
        ))
    return build_env_overrides(pairs)


__all__ = ["load_config", "load_raw_config", "PipelineConfig"]
