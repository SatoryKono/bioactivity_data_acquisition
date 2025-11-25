from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, MutableMapping

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel


class ErrorAction(Enum):
    """Доступные действия при ошибках."""

    FAIL = "fail"
    SKIP = "skip"
    RETRY = "retry"


class ConfigResolverABC(ABC):
    """Загружает и резолвит конфигурацию пайплайна."""

    @abstractmethod
    def load(self, path: str) -> Mapping[str, Any]:
        """Возвращает словарь конфигурации из файла."""


class YamlConfigResolver(ConfigResolverABC):
    """Резолвер конфигурации на основе YAML + переменных окружения + Pydantic."""

    def __init__(
        self,
        model: type[BaseModel] | None = None,
        env_prefix: str = "BIOETL",
        secret_provider: "SecretProviderABC | None" = None,
    ) -> None:
        self._model = model
        self._env_prefix = env_prefix
        self._secret_provider = secret_provider

    def load(self, path: str) -> Mapping[str, Any]:
        config = self._load_yaml(path)
        config = self._substitute_env(config)
        config = self._merge_env_overrides(config)
        if self._model:
            return self._model.model_validate(config).model_dump()
        return config

    def _load_yaml(self, path: str) -> dict[str, Any]:
        content = Path(path).read_text(encoding="utf-8")
        return yaml.safe_load(content) or {}

    def _substitute_env(self, payload: Any) -> Any:
        if isinstance(payload, str):
            if payload.startswith("${") and payload.endswith("}"):
                env_name = payload[2:-1]
                if self._secret_provider:
                    try:
                        return self._secret_provider.get(env_name)
                    except KeyError:
                        pass
                return os.environ.get(env_name, payload)
            return payload
        if isinstance(payload, Mapping):
            return {k: self._substitute_env(v) for k, v in payload.items()}
        if isinstance(payload, Iterable) and not isinstance(payload, (bytes, bytearray)):
            return type(payload)(self._substitute_env(v) for v in payload)
        return payload

    def _merge_env_overrides(self, config: dict[str, Any]) -> dict[str, Any]:
        overrides: dict[str, Any] = {}
        prefix = f"{self._env_prefix}__"
        for name, value in os.environ.items():
            if not name.startswith(prefix):
                continue
            path = name.removeprefix(prefix).split("__")
            self._assign_nested(overrides, path, yaml.safe_load(value))
        if not overrides:
            return config
        return self._deep_merge(config, overrides)

    def _assign_nested(self, target: dict[str, Any], path: list[str], value: Any) -> None:
        cursor = target
        for key in path[:-1]:
            cursor = cursor.setdefault(key.lower(), {})
        cursor[path[-1].lower()] = value

    def _deep_merge(self, base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in patch.items():
            if key in merged and isinstance(merged[key], Mapping) and isinstance(value, Mapping):
                merged[key] = self._deep_merge(dict(merged[key]), dict(value))
            else:
                merged[key] = value
        return merged


class SecretProviderABC(ABC):
    """Получает секреты (API-ключи, токены)."""

    @abstractmethod
    def get(self, name: str) -> str:
        """Возвращает секрет по имени."""


class DotenvSecretProvider(SecretProviderABC):
    """Поставщик секретов из файла .env и окружения."""

    def __init__(self, env_file: str | Path = ".env", override: bool = False) -> None:
        load_dotenv(dotenv_path=env_file, override=override)

    def get(self, name: str) -> str:
        try:
            return os.environ[name]
        except KeyError as exc:
            raise KeyError(f"Secret '{name}' is not set") from exc


class CacheABC(ABC):
    """Кэширует промежуточные результаты."""

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Возвращает значение по ключу или None."""

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Сохраняет значение по ключу."""


class InMemoryCache(CacheABC):
    """Простейший in-memory кэш без TTL."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    def get(self, key: str) -> Any | None:
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value


class LoggerAdapterABC(ABC):
    """Адаптер для структурированного логирования."""

    @abstractmethod
    def info(self, message: str, **kwargs: Any) -> None:
        """Логирует информационное сообщение."""

    @abstractmethod
    def warning(self, message: str, **kwargs: Any) -> None:
        """Логирует предупреждение."""

    @abstractmethod
    def error(self, message: str, **kwargs: Any) -> None:
        """Логирует ошибку."""


class StructuredLoggerAdapter(LoggerAdapterABC):
    """Структурированное логирование поверх стандартного logging."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger("bioetl")
        self._context = dict(context or {})
        if not self._logger.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s | %(levelname)s | %(message)s",
            )

    def bind(self, **kwargs: Any) -> "StructuredLoggerAdapter":
        context = {**self._context, **kwargs}
        return StructuredLoggerAdapter(self._logger, context)

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(self._format(message, kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(self._format(message, kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(self._format(message, kwargs))

    def _format(self, message: str, extra: Mapping[str, Any]) -> str:
        payload = {**self._context, **extra}
        if payload:
            return f"{message} | {json.dumps(payload, ensure_ascii=False)}"
        return message


class TracerABC(ABC):
    """Адаптер для трассировки (open telemetry и т.п.)."""

    @abstractmethod
    def start_span(self, name: str, **kwargs: Any) -> object:
        """Создает новый span."""


class _Span:
    def __init__(self, name: str, logger: StructuredLoggerAdapter | None = None, **metadata: Any) -> None:
        self.name = name
        self.logger = logger
        self.metadata = metadata
        self._start_ts: float | None = None

    def __enter__(self) -> "_Span":
        self._start_ts = time.perf_counter()
        if self.logger:
            self.logger.info("span.start", span=self.name, **self.metadata)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = None
        if self._start_ts is not None:
            duration = time.perf_counter() - self._start_ts
        if self.logger:
            self.logger.info("span.finish", span=self.name, duration=duration, error=bool(exc_val))


class SimpleTracer(TracerABC):
    """Минимальный трейсинг для стадий пайплайна."""

    def __init__(self, logger: StructuredLoggerAdapter | None = None) -> None:
        self._logger = logger

    def start_span(self, name: str, **kwargs: Any) -> _Span:
        return _Span(name=name, logger=self._logger, **kwargs)


class ErrorPolicyABC(ABC):
    """Описывает стратегию обработки ошибок."""

    @abstractmethod
    def decide(self, error: Exception) -> ErrorAction:
        """Возвращает действие для заданной ошибки."""


class MappingErrorPolicy(ErrorPolicyABC):
    """Простая политика ошибок на основе маппинга исключений."""

    def __init__(
        self,
        rules: Mapping[type[Exception], ErrorAction] | None = None,
        default: ErrorAction = ErrorAction.FAIL,
    ) -> None:
        self._rules = dict(rules or {})
        self._default = default

    def decide(self, error: Exception) -> ErrorAction:
        for exc_type, action in self._rules.items():
            if isinstance(error, exc_type):
                return action
        return self._default


class ProgressReporterABC(ABC):
    """Сообщает о прогрессе выполнения задач."""

    @abstractmethod
    def start(self, total: int | None = None) -> None:
        """Инициализирует отчетчик прогресса."""

    @abstractmethod
    def advance(self, step: int = 1, metadata: MutableMapping[str, Any] | None = None) -> None:
        """Продвигает прогресс на указанное количество шагов."""

    @abstractmethod
    def finish(self) -> None:
        """Завершает отчет прогресса."""


class SimpleProgressReporter(ProgressReporterABC):
    """Легковесный отчетчик прогресса с логированием."""

    def __init__(self, logger: StructuredLoggerAdapter | None = None, task: str = "task") -> None:
        self._logger = logger
        self._task = task
        self._total: int | None = None
        self._current = 0

    def start(self, total: int | None = None) -> None:
        self._total = total
        self._current = 0
        if self._logger:
            self._logger.info("progress.start", task=self._task, total=total)

    def advance(self, step: int = 1, metadata: MutableMapping[str, Any] | None = None) -> None:
        self._current += step
        payload = metadata or {}
        payload.update({"task": self._task, "current": self._current, "total": self._total})
        if self._logger:
            self._logger.info("progress.tick", **payload)

    def finish(self) -> None:
        if self._logger:
            self._logger.info("progress.finish", task=self._task, total=self._total, current=self._current)
