from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, Callable, cast

from .environment import load_environment_settings
from .file_resolver import (
    DEFAULT_PROFILE_DIR,
    _discover_layer_files,
    _resolve_config_path,
    _resolve_reference,
)
from .helpers import build_env_overrides
from .merge_utils import _coerce, _collect_env_overrides, _collect_short_env_overrides, _deep_merge
from .models import PipelineConfig
from .yaml_loader import _load_with_extends


class SecretProviderABC(ABC):
    """Access to external secret storage."""

    @abstractmethod
    def get_secret(self, name: str) -> str:
        """Fetch secret value by ``name``."""


class ConfigResolverABC(ABC):
    """Абстракция для материализации ``PipelineConfig``."""

    @abstractmethod
    def resolve(self, profile: str | Path | None = None, overrides: Mapping[str, Any] | None = None) -> PipelineConfig:
        """Resolve configuration for ``profile`` applying ``overrides``."""


class FileConfigResolver(ConfigResolverABC):
    """Resolve pipeline configuration from YAML files."""

    def __init__(
        self,
        config_path: str | Path,
        *,
        profiles: Sequence[str | Path] | None = None,
        env: Mapping[str, str] | None = None,
        env_prefixes: Sequence[str] = ("BIOETL__",),
        include_default_profiles: bool = False,
        secret_provider: SecretProviderABC | None = None,
        environment_loader: Callable[[], Any] = load_environment_settings,
    ) -> None:
        self._config_path = config_path
        self._profiles = [Path(p) for p in profiles or ()]
        self._env = env or os.environ
        self._env_prefixes = env_prefixes
        self._include_default_profiles = include_default_profiles
        self._secret_provider = secret_provider
        self._environment_loader = environment_loader

    def resolve(self, profile: str | Path | None = None, overrides: Mapping[str, Any] | None = None) -> PipelineConfig:
        path = _resolve_config_path(self._config_path)
        base_dir = path.parent

        profile_paths: list[Path] = []
        if self._include_default_profiles:
            profile_paths.extend(_discover_layer_files(DEFAULT_PROFILE_DIR, base=base_dir))
        profile_paths.extend(self._profiles)
        if profile is not None:
            profile_paths.append(Path(profile))

        profile_payload = self._merge_layers(profile_paths, base=base_dir)
        main_payload = load_raw_config(path)
        merged = _deep_merge(profile_payload, main_payload)

        env_settings = self._environment_loader()
        env_name = getattr(env_settings, "bioetl_env", None)
        merged = self._merge_environment_layers(merged, env_name=env_name, base=base_dir)

        if overrides:
            nested = build_env_overrides(
                (tuple(key.split(".")), value) for key, value in overrides.items()
            )
            merged = _deep_merge(merged, nested)

        env_overrides = _collect_env_overrides(self._env, prefixes=self._env_prefixes)
        merged = _deep_merge(merged, env_overrides)

        env_settings_overrides = _collect_short_env_overrides(env_settings)
        merged = _deep_merge(merged, env_settings_overrides)

        injected = self._inject_placeholders(merged)
        return PipelineConfig.model_validate(injected)

    def _merge_layers(self, layer_paths: Sequence[Path], *, base: Path) -> dict[str, Any]:
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

    def _merge_environment_layers(self, payload: Mapping[str, Any], *, env_name: str | None, base: Path) -> dict[str, Any]:
        merged = dict(payload)
        if not env_name:
            return merged

        env_dir = base / "env" / env_name
        if not env_dir.exists():
            msg = f"Environment layer directory missing: {env_dir}"
            raise FileNotFoundError(msg)

        env_layers = _discover_layer_files(env_dir, base=env_dir)
        merged_env = self._merge_layers(env_layers, base=env_dir)
        return _deep_merge(merged, merged_env)

    def _inject_placeholders(self, payload: Any) -> Any:
        if isinstance(payload, MutableMapping):
            return {key: self._inject_placeholders(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._inject_placeholders(item) for item in payload]
        if isinstance(payload, str):
            return self._resolve_placeholder(payload)
        return payload

    def _resolve_placeholder(self, value: str) -> Any:
        pattern = re.compile(r"\${(?P<kind>ENV|SECRET):(?P<name>[^}]+)}")
        match = pattern.fullmatch(value.strip())
        if not match:
            return value

        kind = match.group("kind").upper()
        name = match.group("name")

        if kind == "ENV":
            if name not in self._env:
                msg = f"Environment variable not found: {name}"
                raise KeyError(msg)
            return _coerce(self._env[name])

        if kind == "SECRET":
            if self._secret_provider is None:
                msg = "Secret provider is not configured"
                raise ValueError(msg)
            return self._secret_provider.get_secret(name)

        return value


def load_raw_config(path: Path) -> dict[str, Any]:
    """Load a YAML config file with support for ``extends``."""

    return _load_with_extends(path, stack=())


__all__ = ["ConfigResolverABC", "FileConfigResolver", "SecretProviderABC", "load_raw_config"]
