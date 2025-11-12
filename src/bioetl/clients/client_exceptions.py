"""Публичные исключения HTTP-клиентов BioETL.

Модуль предоставляет стабильный контракт для верхних слоёв (например, CLI),
скрывая конкретные детали реализации сетевых клиентов. Все внешние компоненты
должны импортировать сетевые исключения только отсюда, чтобы избежать прямой
зависимости от ``requests``.
"""

from __future__ import annotations

from requests.exceptions import ConnectionError as _RequestsConnectionError
from requests.exceptions import HTTPError as _RequestsHTTPError
from requests.exceptions import RequestException as _RequestsRequestException
from requests.exceptions import Timeout as _RequestsTimeout

__all__ = [
    "RequestException",
    "HTTPError",
    "Timeout",
    "ConnectionError",
]

# Переэкспорт исключений requests с сохранением базовых типов.
RequestException = _RequestsRequestException
HTTPError = _RequestsHTTPError
Timeout = _RequestsTimeout
ConnectionError = _RequestsConnectionError


