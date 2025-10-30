"""YAML configuration loader with inheritance support."""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, IO

import yaml

if TYPE_CHECKING:
    from yaml import Node as YamlNode
else:
    YamlNode = Any

from bioetl.config.models import PipelineConfig


class _IncludedList(list[Any]):
    """Marker type for sequences that should be spliced into parent lists."""


def _config_loader_factory(
    base_path: Path, include_stack: tuple[Path, ...]
) -> type[yaml.SafeLoader]:
    """Create a YAML loader class bound to a specific base path."""

    class ConfigLoader(yaml.SafeLoader):
        def __init__(self, stream: IO[str] | IO[bytes]) -> None:
            super().__init__(stream)
            self._root = base_path
            self._include_stack = include_stack

    def construct_include(loader: yaml.Loader, node: "YamlNode") -> Any:
        """Load referenced YAML content relative to the current file."""

        if not isinstance(loader, ConfigLoader):
            raise TypeError("!include constructor received unexpected loader instance")

        if isinstance(node, yaml.nodes.ScalarNode):
            relative_path = loader.construct_scalar(node)
        else:
            raise TypeError("!include only supports scalar values with file paths")

        include_path = Path(relative_path)
        if not include_path.is_absolute():
            include_path = loader._root / include_path

        resolved_include = include_path.resolve()
        if resolved_include in loader._include_stack:
            raise ValueError(f"Circular !include detected involving {resolved_include}")

        value = load_yaml(include_path, _include_stack=loader._include_stack)
        if isinstance(value, list):
            return _IncludedList(value)
        return value

    ConfigLoader.add_constructor("!include", construct_include)
    return ConfigLoader


def _resolve_includes(data: Any) -> Any:
    """Recursively resolve include markers and splice lists."""

    if isinstance(data, _IncludedList):
        return _resolve_includes(list(data))

    if isinstance(data, list):
        resolved_list: list[Any] = []
        for item in data:
            if isinstance(item, _IncludedList):
                included_values = _resolve_includes(list(item))
                if not isinstance(included_values, list):
                    raise TypeError(
                        "Included value must resolve to a list when used in a list context"
                    )
                resolved_list.extend(included_values)
            else:
                resolved_list.append(_resolve_includes(item))
        return resolved_list

    if isinstance(data, dict):
        return {key: _resolve_includes(value) for key, value in data.items()}

    return data


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Deep merge two dictionaries.

    Lists are replaced (not merged).
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_yaml(path: Path, *, _include_stack: tuple[Path, ...] | None = None) -> Any:
    """Load YAML file, resolving anchors, aliases, and custom includes."""

    resolved_path = path.resolve()
    include_stack = _include_stack or ()
    if resolved_path in include_stack:
        raise ValueError(f"Circular !include detected involving {resolved_path}")

    loader_cls = _config_loader_factory(resolved_path.parent, include_stack + (resolved_path,))
    with resolved_path.open("r", encoding="utf-8") as f:
        data = yaml.load(f, Loader=loader_cls)
    return _resolve_includes(data)


def load_config(
    config_path: Path | str,
    overrides: dict[str, Any] | None = None,
    env_prefix: str = "BIOETL_",
) -> PipelineConfig:
    """
    Load configuration from YAML file with inheritance and overrides.

    Priority: base.yaml < profile.yaml < CLI overrides < environment variables

    Args:
        config_path: Path to configuration file
        overrides: CLI overrides as dict
        env_prefix: Prefix for environment variables

    Returns:
        Validated PipelineConfig
    """
    overrides = overrides or {}

    # Convert to Path if string
    if isinstance(config_path, str):
        config_path = Path(config_path)

    # Load config file and resolve extends
    config_data = _load_with_extends(config_path)

    # Apply CLI overrides
    if overrides:
        config_data = deep_merge(config_data, overrides)

    # Apply environment variables
    env_overrides = _load_env_overrides(env_prefix)
    if env_overrides:
        config_data = deep_merge(config_data, env_overrides)

    # Validate and return
    result: PipelineConfig = PipelineConfig.model_validate(config_data)
    result.attach_source_path(config_path)
    return result


def _load_with_extends(path: Path, visited: set[Path] | None = None) -> Any:
    """Load YAML file and recursively resolve extends."""
    visited = visited or set()

    if path in visited:
        raise ValueError(f"Circular extends detected: {path}")

    visited.add(path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    data = load_yaml(path)

    # Resolve extends recursively
    if "extends" in data:
        extends_value = data.pop("extends")
        if isinstance(extends_value, (str, Path)):
            extends_iterable = [extends_value]
        elif isinstance(extends_value, list):
            extends_iterable = extends_value
        else:
            raise TypeError("'extends' must be a string, Path, or list of those values")

        base_data: dict[str, Any] = {}
        for extends_entry in extends_iterable:
            extends_path = Path(extends_entry)
            if not extends_path.is_absolute():
                extends_path = path.parent / extends_path

            inherited_data = _load_with_extends(extends_path, visited)
            base_data = deep_merge(base_data, inherited_data)

        data = deep_merge(base_data, data)

    return data


def _load_env_overrides(prefix: str) -> dict[str, Any]:
    """Load environment variable overrides."""
    overrides: dict[str, Any] = {}

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue

        # Remove prefix and split by double underscore
        path_parts = key[len(prefix) :].lower().split("__")
        if len(path_parts) < 1:
            continue

        # Parse value (try JSON first, then treat as string)
        try:
            parsed_value = yaml.safe_load(value)
        except Exception:
            parsed_value = value

        # Build nested dict
        current = overrides
        for part in path_parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[path_parts[-1]] = parsed_value

    return overrides


def parse_cli_overrides(overrides: list[str]) -> dict[str, Any]:
    """
    Parse CLI overrides from list of 'key=value' strings.

    Example:
        ['http.global.timeout_sec=45', 'sources.chembl.batch_size=20']
    """
    result: dict[str, Any] = {}

    for override in overrides:
        if "=" not in override:
            raise ValueError(f"Invalid override format: {override}")

        key_str, value_str = override.split("=", 1)

        # Parse value (try JSON, then YAML, then string)
        try:
            value = json.loads(value_str)
        except json.JSONDecodeError:
            try:
                value = yaml.safe_load(value_str)
            except Exception:
                value = value_str

        # Build nested dict
        path_parts = key_str.split(".")
        current = result
        for part in path_parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[path_parts[-1]] = value

    return result
