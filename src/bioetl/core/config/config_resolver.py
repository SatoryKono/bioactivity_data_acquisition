from __future__ import annotations

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
    """Доступ к внешним секретам и переменным окружения."""

    @abstractmethod
    def get_secret(self, name: str) -> str:
        """Получить секрет по имени ``name``."""

    @abstractmethod
    def get_variable(self, name: str) -> str:
        """Получить переменную окружения по имени ``name``."""

    @abstractmethod
    def iter_variables(self) -> Mapping[str, str]:
        """Вернуть доступные переменные окружения."""


class EnvSecretProvider(SecretProviderABC):
    """Провайдер секретов на базе окружения и ``.env`` файлов."""

    def __init__(
        self,
        env: Mapping[str, str] | None = None,
        *,
        env_file: Path | None = None,
        environment_loader: Callable[..., Any] = load_environment_settings,
    ) -> None:
        self._env_file = env_file
        self._environment_settings = environment_loader(env_file=env_file)
        self._env: dict[str, str] = {}
        self._env.update(self._load_env_file_values(env_file))
        self._env.update(self._extract_environment_settings(self._environment_settings))
        self._env.update(env or {})

    @property
    def environment_settings(self) -> Any:
        return self._environment_settings

    def get_secret(self, name: str) -> str:
        if name in self._env:
            return self._env[name]
        msg = f"Secret not found: {name}"
        raise KeyError(msg)

    def get_variable(self, name: str) -> str:
        if name in self._env:
            return self._env[name]
        msg = f"Environment variable not found: {name}"
        raise KeyError(msg)

    def iter_variables(self) -> Mapping[str, str]:
        return dict(self._env)

    def _load_env_file_values(self, env_file: Path | None) -> dict[str, str]:
        if env_file is None or not env_file.exists():
            return {}

        values: dict[str, str] = {}
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", maxsplit=1)
            values[key.strip()] = value.strip()
        return values

    def _extract_environment_settings(self, settings: Any) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for field_name, field in settings.model_fields.items():
            value = getattr(settings, field_name)
            if value is None:
                continue
            alias = field.alias or field_name
            if hasattr(value, "get_secret_value"):
                mapping[alias] = value.get_secret_value()
            else:
                mapping[alias] = str(value)
        return mapping


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
        environment_loader: Callable[..., Any] = load_environment_settings,
        env_file: Path | None = None,
    ) -> None:
        self._config_path = config_path
        self._profiles = [Path(p) for p in profiles or ()]
        self._env_prefixes = env_prefixes
        self._include_default_profiles = include_default_profiles
        self._env_file = env_file
        self._secret_provider = secret_provider or EnvSecretProvider(
            env, env_file=env_file, environment_loader=environment_loader
        )
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

        env_settings = getattr(self._secret_provider, "environment_settings", None)
        if env_settings is None:
            env_settings = self._environment_loader(env_file=self._env_file)
        env_name = getattr(env_settings, "bioetl_env", None)
        merged = self._merge_environment_layers(merged, env_name=env_name, base=base_dir)

        if overrides:
            nested = build_env_overrides(
                (tuple(key.split(".")), value) for key, value in overrides.items()
            )
            merged = _deep_merge(merged, nested)

        env_overrides = _collect_env_overrides(
            self._secret_provider.iter_variables(), prefixes=self._env_prefixes
        )
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
            return _coerce(self._secret_provider.get_variable(name))

        if kind == "SECRET":
            return self._secret_provider.get_secret(name)

        return value


def load_raw_config(path: Path) -> dict[str, Any]:
    """Load a YAML config file with support for ``extends``."""

    return _load_with_extends(path, stack=())


__all__ = [
    "ConfigResolverABC",
    "EnvSecretProvider",
    "FileConfigResolver",
    "SecretProviderABC",
    "load_raw_config",
]
