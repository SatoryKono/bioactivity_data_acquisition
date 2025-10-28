"""YAML configuration loader with inheritance support."""

import json
import os
from pathlib import Path
from typing import Any

import yaml  # type: ignore

from bioetl.config.models import PipelineConfig


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


def load_yaml(path: Path) -> Any:
    """Load YAML file and resolve anchors/aliases."""
    with path.open("r") as f:
        return yaml.safe_load(f)


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
        extends_path = Path(data.pop("extends"))
        if not extends_path.is_absolute():
            extends_path = path.parent / extends_path

        base_data = _load_with_extends(extends_path, visited)
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

