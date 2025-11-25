from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Optional

import requests


class RequestBuilderABC(ABC):
    """Формирует HTTP-запросы для выборки данных из источника."""

    @abstractmethod
    def build(self, cursor: Optional[Any] = None) -> requests.Request:
        """Возвращает объект запроса для текущего положения курсора."""


class PaginatorABC(ABC):
    """Определяет стратегию обхода страниц API."""

    @abstractmethod
    def initial_cursor(self) -> Optional[Any]:
        """Начальное значение курсора перед первой загрузкой."""

    @abstractmethod
    def advance(self, current_cursor: Optional[Any], next_cursor: Optional[Any]) -> Optional[Any]:
        """Возвращает новое значение курсора после обработки страницы."""

    @abstractmethod
    def should_continue(self, cursor: Optional[Any]) -> bool:
        """Определяет, нужно ли продолжать пагинацию с переданным курсором."""


class ResponseParserABC(ABC):
    """Извлекает записи и курсор для следующей страницы из ответа API."""

    @abstractmethod
    def parse_records(self, response: requests.Response) -> Iterable[Mapping[str, Any]]:
        """Возвращает последовательность записей из ответа."""

    @abstractmethod
    def get_next_cursor(self, response: requests.Response) -> Optional[Any]:
        """Извлекает курсор следующей страницы или ``None`` если страниц больше нет."""


class CursorPaginator(PaginatorABC):
    """Простая реализация пагинации по курсору с ограничением числа страниц."""

    def __init__(self, initial_cursor: Optional[Any] = None, max_pages: Optional[int] = None) -> None:
        self._initial_cursor = initial_cursor
        self._max_pages = max_pages
        self._pages_processed = 0

    def initial_cursor(self) -> Optional[Any]:
        return self._initial_cursor

    def advance(self, current_cursor: Optional[Any], next_cursor: Optional[Any]) -> Optional[Any]:
        self._pages_processed += 1
        return next_cursor

    def should_continue(self, cursor: Optional[Any]) -> bool:
        if self._max_pages is not None and self._pages_processed >= self._max_pages:
            return False
        return cursor is not None
