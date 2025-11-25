from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, MutableMapping, TypeVar

import yaml
from pydantic import BaseModel, ValidationError

from .interfaces import ConfigResolverABC, SecretProviderABC

ConfigT = TypeVar("ConfigT", bound=BaseModel)


class DotenvSecretProvider(SecretProviderABC):
    """Загрузка секретов из `.env` и переменных окружения.`"""

    def __init__(self, env_path: str | os.PathLike[str] | None = ".env", override_env: bool = True):
        self._env_path = Path(env_path) if env_path else None
        self._override_env = override_env
        self._values: dict[str, str] = self._load_values()

    def _load_values(self) -> dict[str, str]:
        values: dict[str, str] = {}
        if self._env_path and self._env_path.exists():
            try:
                from dotenv import dotenv_values  # type: ignore
            except ImportError:
                values.update(self._parse_dotenv(self._env_path))
            else:
                values.update({k: str(v) for k, v in dotenv_values(self._env_path).items() if v is not None})
        if self._override_env:
            values.update({k: v for k, v in os.environ.items()})
        return values

    def _parse_dotenv(self, path: Path) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            parsed[key.strip()] = value.strip().strip('"')
        return parsed

    def get(self, name: str) -> str:
        try:
            return self._values[name]
        except KeyError as err:  # pragma: no cover - контрольный поток
            raise KeyError(f"Секрет '{name}' не найден") from err


class YamlEnvConfigResolver(ConfigResolverABC[ConfigT]):
    """Загрузчик конфигураций из YAML с поддержкой ENV и секретов."""

    def __init__(
        self,
        env_prefix: str = "BIOETL",
        env_separator: str = "__",
        secret_provider: SecretProviderABC | None = None,
    ) -> None:
        self.env_prefix = env_prefix
        self.env_separator = env_separator
        self.secret_provider = secret_provider

    def load(self, path: str) -> Mapping[str, Any]:
        content = Path(path).read_text(encoding="utf-8")
        data = yaml.safe_load(content) or {}
        if not isinstance(data, MutableMapping):
            raise ValueError("Файл конфигурации должен содержать словарь")
        return data

    def resolve(
        self,
        path: str,
        model: type[ConfigT],
        overrides: Mapping[str, Any] | None = None,
    ) -> ConfigT:
        raw_config: MutableMapping[str, Any] = dict(self.load(path))
        self._merge_env(raw_config)
        if overrides:
            self._deep_update(raw_config, overrides)
        if self.secret_provider:
            raw_config = self._inject_secrets(raw_config)
        try:
            return model.model_validate(raw_config)
        except ValidationError as err:  # pragma: no cover - passthrough
            raise ValueError(f"Конфигурация не прошла валидацию: {err}") from err

    def _merge_env(self, config: MutableMapping[str, Any]) -> None:
        prefix = f"{self.env_prefix}{self.env_separator}" if self.env_prefix else ""
        for env_key, env_value in os.environ.items():
            if not env_key.startswith(prefix):
                continue
            path_key = env_key.removeprefix(prefix)
            key_parts = [part for part in path_key.split(self.env_separator) if part]
            if not key_parts:
                continue
            self._set_nested(config, key_parts, self._coerce(env_value))

    def _inject_secrets(self, config: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        def _walk(value: Any) -> Any:
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                secret_name = value[2:-1]
                return self.secret_provider.get(secret_name) if self.secret_provider else value
            if isinstance(value, MutableMapping):
                return {k: _walk(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_walk(item) for item in value]
            return value

        return _walk(config)

    def _set_nested(self, target: MutableMapping[str, Any], keys: list[str], value: Any) -> None:
        current: MutableMapping[str, Any] = target
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], MutableMapping):
                current[key] = {}
            current = current[key]  # type: ignore[assignment]
        current[keys[-1]] = value

    def _deep_update(self, target: MutableMapping[str, Any], overrides: Mapping[str, Any]) -> None:
        for key, value in overrides.items():
            if isinstance(value, Mapping) and isinstance(target.get(key), MutableMapping):
                self._deep_update(target[key], value)  # type: ignore[arg-type]
            else:
                target[key] = value

    def _coerce(self, value: str) -> Any:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            lower = value.lower()
            if lower in {"true", "false"}:
                return lower == "true"
            return value
