from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Iterable, Mapping, Sequence


class RequestBuilderABC(ABC):
    """Собирает параметры HTTP-запроса к источнику."""

    @abstractmethod
    def build(self, endpoint: str, params: Mapping[str, object] | None = None) -> Mapping[str, object]:
        """Возвращает структуру запроса (url, заголовки, параметры)."""


class PaginatorABC(ABC):
    """Управляет пагинацией для последовательного получения страниц."""

    @abstractmethod
    def paginate(self, initial_request: Mapping[str, object]) -> Iterable[Mapping[str, object]]:
        """Генерирует запросы для каждой страницы."""


class RateLimiterABC(ABC):
    """Ограничивает частоту запросов к API."""

    @abstractmethod
    def acquire(self) -> None:
        """Блокирует до момента, когда можно выполнить запрос."""


class RetryPolicyABC(ABC):
    """Стратегия повторных попыток при временных ошибках."""

    @abstractmethod
    def execute(self, func: Callable[..., object], *args, **kwargs) -> object:
        """Выполняет функцию с учетом правил повторов."""


class ResponseParserABC(ABC):
    """Парсит ответ источника в удобный формат."""

    @abstractmethod
    def parse(self, response: object) -> Sequence[Mapping[str, object]]:
        """Преобразует сырой ответ в последовательность записей."""


class SourceClientABC(ABC):
    """Единая точка взаимодействия с источником данных."""

    @abstractmethod
    def fetch(self, request: Mapping[str, object]) -> object:
        """Отправляет запрос и возвращает сырой ответ."""

    @abstractmethod
    def stream(self, endpoint: str, params: Mapping[str, object] | None = None) -> Iterable[Mapping[str, object]]:
        """Стриминговое получение данных с учетом пагинации, лимитов и повторов."""
