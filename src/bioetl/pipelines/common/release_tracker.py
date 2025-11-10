"""Унифицированное управление ChEMBL release для пайплайнов и итераторов."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from inspect import Parameter, signature
from typing import Any, ClassVar, Protocol, cast, runtime_checkable

from structlog.stdlib import BoundLogger

from bioetl.core.mapping_utils import stringify_mapping

__all__ = [
    "ChemblHandshakeResult",
    "ChemblReleaseMixin",
    "ChemblReleaseTracker",
]


@dataclass(frozen=True)
class ChemblHandshakeResult:
    """Результат handshake-запроса к ChEMBL."""

    payload: dict[str, Any]
    release: str | None
    requested_at_utc: datetime


@runtime_checkable
class ChemblReleaseTracker(Protocol):
    """Контракт для объектов, предоставляющих доступ к `chembl_release`."""

    _chembl_release: str | None

    @property
    def chembl_release(self) -> str | None:
        """Вернуть кешированный номер релиза ChEMBL."""

    def _update_release(self, value: str | None) -> None:
        """Обновить кеш релиза ChEMBL."""


class ChemblReleaseMixin:
    """Mixin с реализацией отслеживания ChEMBL release и handshake-хелперами."""

    _chembl_release: str | None
    _RELEASE_KEYS: ClassVar[tuple[str, ...]] = (
        "chembl_release",
        "chembl_db_version",
        "release",
        "version",
    )

    def __init__(self, *args: object, **kwargs: object) -> None:
        self._chembl_release = None
        super().__init__(*args, **kwargs)

    @property
    def chembl_release(self) -> str | None:
        """Вернуть актуальный релиз ChEMBL (если он был зафиксирован)."""

        return self._chembl_release

    def _update_release(self, value: str | None) -> None:
        """Сохранить номер релиза ChEMBL (None допустимо при ошибках handshake)."""

        self._chembl_release = self._normalize_release(value)

    def perform_chembl_handshake(
        self,
        handshake_target: Any,
        *,
        log: BoundLogger,
        event: str,
        endpoint: str,
        enabled: bool,
        release_attr_fallback: str = "chembl_release",
        timeout: float | tuple[float, float] | None = None,
        budget_seconds: float | None = None,
    ) -> ChemblHandshakeResult:
        """Выполнить handshake с клиентом ChEMBL и обновить кеш релиза.

        Parameters
        ----------
        handshake_target:
            Объект с методом `handshake(endpoint=..., enabled=...)`.
        log:
            Структурный логгер для фиксирования событий handshake.
        event:
            Имя события для логирования (например, ``"chembl_assay.handshake"``).
        endpoint:
            Endpoint, по которому выполняется handshake.
        enabled:
            Флаг включенности handshake согласно конфигурации.
        release_attr_fallback:
            Имя атрибута handshake_target, из которого можно взять релиз,
            если payload его не содержит.
        timeout:
            Таймаут (сек) для запроса handshake. Может быть float или кортеж
            (connect, read). Если None, используется значение по умолчанию
            в клиенте.
        budget_seconds:
            Максимальный временной бюджет на выполнение handshake. None означает,
            что используется значение по умолчанию на стороне клиента.

        Returns
        -------
        ChemblHandshakeResult
            Структура с payload, релизом и временем запроса.
        """

        requested_at = datetime.now(timezone.utc)

        handshake_method = getattr(handshake_target, "handshake", None)
        if not callable(handshake_method):
            log.debug(
                f"{event}.skipped",
                reason="handshake_method_missing",
                handshake_endpoint=endpoint,
            )
            fallback_release = self._extract_fallback_release(
                handshake_target,
                release_attr_fallback=release_attr_fallback,
            )
            if fallback_release is not None:
                self._update_release(fallback_release)
            return ChemblHandshakeResult(
                payload={},
                release=self.chembl_release,
                requested_at_utc=requested_at,
            )

        if not enabled:
            log.info(
                f"{event}.skipped",
                handshake_enabled=False,
                handshake_endpoint=endpoint,
            )
            return ChemblHandshakeResult(
                payload={},
                release=self.chembl_release,
                requested_at_utc=requested_at,
            )

        raw_payload = self._invoke_handshake(
            handshake_method,
            endpoint=endpoint,
            enabled=enabled,
            timeout=timeout,
            budget_seconds=budget_seconds,
        )
        payload = self._coerce_mapping(raw_payload)
        release = self._extract_chembl_release(payload)

        if release is not None:
            self._update_release(release)
        else:
            fallback_release = self._extract_fallback_release(
                handshake_target,
                release_attr_fallback=release_attr_fallback,
            )
            if fallback_release is not None:
                self._update_release(fallback_release)

        log.info(
            event,
            chembl_release=self.chembl_release,
            handshake_endpoint=endpoint,
            handshake_enabled=enabled,
        )
        return ChemblHandshakeResult(
            payload=payload,
            release=self.chembl_release,
            requested_at_utc=requested_at,
        )

    @staticmethod
    def _normalize_release(value: Any) -> str | None:
        """Привести значение релиза к каноническому виду."""

        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        normalized = value.strip()
        return normalized or None

    @classmethod
    def _coerce_mapping(cls, payload: Any) -> dict[str, Any]:
        """Привести payload к словарю со строковыми ключами."""

        if isinstance(payload, Mapping):
            mapping = cast(Mapping[object, Any], payload)
            return stringify_mapping(mapping)
        return {}

    @classmethod
    def _extract_chembl_release(cls, payload: Mapping[str, Any]) -> str | None:
        """Извлечь номер релиза из payload."""

        for key in cls._RELEASE_KEYS:
            candidate = payload.get(key)
            normalized = cls._normalize_release(candidate)
            if normalized is not None:
                return normalized
        return None

    @staticmethod
    def _supports_keyword_argument(
        callable_obj: Callable[..., Any],
        keyword: str,
    ) -> bool:
        """Проверить, принимает ли вызываемый объект указанный keyword-аргумент."""

        try:
            sig = signature(callable_obj)
        except (TypeError, ValueError):
            # Невозможно определить сигнатуру — считаем, что аргумент поддерживается.
            return True

        for parameter in sig.parameters.values():
            if parameter.kind is Parameter.VAR_KEYWORD:
                return True
            if (
                parameter.name == keyword
                and parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
            ):
                return True
        return False

    @classmethod
    def _invoke_handshake(
        cls,
        handshake_callable: Callable[..., Any],
        *,
        endpoint: str,
        enabled: bool,
        timeout: float | tuple[float, float] | None,
        budget_seconds: float | None,
    ) -> Mapping[str, Any]:
        """Безопасно вызвать handshake с поддержкой исторических сигнатур."""

        supports_enabled = cls._supports_keyword_argument(handshake_callable, "enabled")
        supports_timeout = cls._supports_keyword_argument(handshake_callable, "timeout")
        supports_budget = cls._supports_keyword_argument(handshake_callable, "budget_seconds")

        base_kwargs: dict[str, Any] = {"endpoint": endpoint}
        if supports_enabled:
            base_kwargs["enabled"] = enabled
        if timeout is not None and supports_timeout:
            base_kwargs["timeout"] = timeout
        if budget_seconds is not None and supports_budget:
            base_kwargs["budget_seconds"] = budget_seconds

        attempt_kwargs: list[dict[str, Any]] = [base_kwargs]

        if "timeout" in base_kwargs:
            without_timeout = dict(base_kwargs)
            without_timeout.pop("timeout", None)
            attempt_kwargs.append(without_timeout)

        if "enabled" in base_kwargs:
            without_enabled = dict(base_kwargs)
            without_enabled.pop("enabled", None)
            attempt_kwargs.append(without_enabled)

        if "budget_seconds" in base_kwargs:
            without_budget = dict(base_kwargs)
            without_budget.pop("budget_seconds", None)
            attempt_kwargs.append(without_budget)

        attempt_kwargs.append({"endpoint": endpoint})

        seen: set[tuple[tuple[str, Any], ...]] = set()
        for candidate in attempt_kwargs:
            candidate.setdefault("endpoint", endpoint)
            key = tuple(sorted(candidate.items(), key=lambda item: item[0]))
            if key in seen:
                continue
            seen.add(key)
            try:
                result = handshake_callable(**candidate)
            except TypeError:
                continue
            else:
                return cast(Mapping[str, Any], result)

        result = handshake_callable(endpoint=endpoint)
        return cast(Mapping[str, Any], result)

    def _extract_fallback_release(
        self,
        handshake_target: Any,
        *,
        release_attr_fallback: str,
    ) -> str | None:
        """Получить релиз из запасного атрибута клиента."""

        if not release_attr_fallback:
            return None
        candidate = getattr(handshake_target, release_attr_fallback, None)
        if candidate is None:
            return None
        # Игнорируем объекты unittest.mock, чтобы не писать их строковое представление
        module_name = getattr(candidate, "__module__", "") or ""
        is_mock_object = module_name.startswith("unittest.mock") or hasattr(candidate, "_mock_children")
        if is_mock_object:
            return None
        return self._normalize_release(candidate)

