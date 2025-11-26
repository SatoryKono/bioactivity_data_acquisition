from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

import structlog

if TYPE_CHECKING:
    from bioetl.clients import client_exceptions as client_exceptions_module
    from bioetl.core.http.interfaces import ApiTransportProtocol, BaseApiClient
else:
    ApiTransportProtocol = BaseApiClient = Any

_T = TypeVar("_T")


class ApiClientMixin:
    """Миксин для обработки исключений и логирования в API-клиентах.

    Отвечает за:
    1. Логирование ошибок при выполнении запросов через ``_logger``.
    2. Перехват исключений и приведение их к ``bioetl.clients.client_exceptions.RequestException``.
    3. Предоставление методов-обёрток ``_wrap_callable`` и ``_wrap_iterator``.

    Клиентский код не должен дублировать логику обработки исключений, а использовать
    эти методы.
    """
    api_client: BaseApiClient | ApiTransportProtocol
    _logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger

    def _transport(self) -> ApiTransportProtocol | BaseApiClient:
        transport = getattr(self, "transport", None) or getattr(self, "api_client", None)
        if transport is None:
            raise AttributeError("ApiClientMixin requires 'transport' or 'api_client' attribute")
        return transport

    def _normalize_payload(
        self, payload: Any, *, page_key: str | None = "results"
    ) -> Iterator[dict[str, Any]]:
        effective_page_key = (
            page_key if page_key is not None else getattr(self, "_page_key_override", "results")
        )
        if isinstance(payload, Mapping):
            results = payload.get(effective_page_key)
            if isinstance(results, Iterable) and not isinstance(results, (str, bytes, bytearray)):
                for item in results:
                    if isinstance(item, Mapping):
                        yield dict(item)
                return

            yield dict(payload)
            return

        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)
            return

        if payload is not None:
            yield {"result": payload}

    def _wrap_callable(
        self, func: Callable[[], _T], *, log_context: Mapping[str, Any] | None = None
    ) -> _T:
        """Обернуть вызов функции для обработки ошибок и логирования.

        Args:
            func: Функция для выполнения (обычно lambda с вызовом клиента).
            log_context: Дополнительный контекст для логирования ошибки.

        Returns:
            Результат выполнения ``func``.

        Raises:
            client_exceptions.HTTPError: Пробрасывается без изменений (ожидаемая ошибка).
            client_exceptions.RequestException: Оборачивает любые другие исключения (IOError и т.д.).
        """
        from bioetl.clients import client_exceptions

        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(
        self, func: Callable[[], Iterator[dict[str, Any]]], *, log_context: Mapping[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Обернуть итератор для обработки ошибок и логирования.

        Аналогичен ``_wrap_callable``, но для генераторов/итераторов.

        Args:
            func: Функция, возвращающая итератор.
            log_context: Дополнительный контекст для логирования.

        Yields:
            Элементы из итератора.

        Raises:
            client_exceptions.HTTPError: Пробрасывается.
            client_exceptions.RequestException: Оборачивает прочие ошибки.
        """
        from bioetl.clients import client_exceptions

        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", error=str(exc), **context)
            raise client_exceptions.RequestException(str(exc)) from exc


class ClosableMixin:
    api_client: BaseApiClient | ApiTransportProtocol
    _logger: structlog.stdlib.BoundLogger | structlog.types.BindableLogger

    def close(self) -> None:
        transport = getattr(self, "_transport", None)
        if callable(transport):
            transport = transport()
        if transport is None:
            transport = getattr(self, "api_client", None)
        close_fn = getattr(transport, "close", None)
        if callable(close_fn):
            close_fn()


__all__ = ["ApiClientMixin", "ClosableMixin"]
