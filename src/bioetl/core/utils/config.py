from __future__ import annotations

import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, MutableMapping, TypeVar

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError

from .interfaces import ConfigResolverABC, SecretProviderABC

ModelT = TypeVar("ModelT", bound=BaseModel)
_SECRET_PATTERN = re.compile(r"^secret://(?P<name>[A-Z0-9_]+)$")


def _set_nested(mapping: MutableMapping[str, Any], path: list[str], value: Any) -> None:
    current: MutableMapping[str, Any] = mapping
    for part in path[:-1]:
        if part not in current or not isinstance(current[part], MutableMapping):
            current[part] = {}
        current = current[part]
    current[path[-1]] = value


def _coerce_value(raw: str) -> Any:
    """Попытка привести строку из окружения к подходящему типу."""

    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        parsed = yaml.safe_load(raw)
        if isinstance(parsed, (dict, list)):
            return parsed
    except yaml.YAMLError:
        pass
    return raw


class YamlConfigResolver(ConfigResolverABC):
    """Резолвит YAML-конфигурацию с подстановкой переменных окружения и секретов."""

    def __init__(
        self,
        model_cls: type[ModelT],
        *,
        env_prefix: str | None = None,
        env_file: str | None = ".env",
        secret_provider: SecretProviderABC | None = None,
    ) -> None:
        self._model_cls = model_cls
        self._env_prefix = (env_prefix or "").upper()
        self._secret_provider = secret_provider
        if env_file:
            load_dotenv(env_file, override=True)

    def load(self, path: str | Path) -> Mapping[str, Any]:
        config_path = Path(path)
        raw_config = yaml.safe_load(config_path.read_text()) or {}
        config: MutableMapping[str, Any] = deepcopy(raw_config)

        self._apply_env_overrides(config)
        self._inject_secrets(config)

        try:
            model = self._model_cls.model_validate(config)
        except ValidationError as exc:  # pragma: no cover - pydantic handles coverage
            raise ValueError(f"Config validation failed: {exc}") from exc

        return model.model_dump()

    def _apply_env_overrides(self, config: MutableMapping[str, Any]) -> None:
        prefix = self._env_prefix
        for env_key, env_value in os.environ.items():
            if prefix:
                if not env_key.startswith(prefix):
                    continue
                normalized_key = env_key[len(prefix) :]
            else:
                normalized_key = env_key

            normalized_key = normalized_key.lstrip("_")
            if not normalized_key:
                continue
            path = normalized_key.lower().split("__")
            _set_nested(config, path, _coerce_value(env_value))

    def _inject_secrets(self, config: MutableMapping[str, Any]) -> None:
        if self._secret_provider is None:
            return

        for key, value in list(config.items()):
            if isinstance(value, MutableMapping):
                self._inject_secrets(value)
                continue

            if isinstance(value, str):
                match = _SECRET_PATTERN.match(value.strip())
                if match:
                    secret_name = match.group("name")
                    config[key] = self._secret_provider.get(secret_name)

