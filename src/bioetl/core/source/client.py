from __future__ import annotations

import contextlib
import time
from collections import deque
from typing import Any, Iterable, Mapping, Optional

import backoff
import requests

from .interfaces import PaginatorABC, RequestBuilderABC, ResponseParserABC


class RateLimiter:
    """Простейший лимитер частоты вызовов по принципу скользящего окна."""

    def __init__(self, max_calls: int, period: float) -> None:
        if max_calls <= 0:
            raise ValueError("max_calls должен быть положительным")
        if period <= 0:
            raise ValueError("period должен быть положительным")
        self._max_calls = max_calls
        self._period = period
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        self._drain_expired(now)
        if len(self._timestamps) >= self._max_calls:
            sleep_for = self._period - (now - self._timestamps[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            self._drain_expired(now)
        self._timestamps.append(time.monotonic())

    def _drain_expired(self, now: float) -> None:
        while self._timestamps and now - self._timestamps[0] >= self._period:
            self._timestamps.popleft()


class HttpSourceClient:
    """HTTP-клиент для источников данных с поддержкой retry, backoff и rate limiting."""

    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        retry_statuses: Optional[set[int]] = None,
        rate_limit_max_calls: int = 5,
        rate_limit_period: float = 1.0,
        timeout: float = 10.0,
    ) -> None:
        self._session = session or requests.Session()
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._retry_statuses = retry_statuses or {429, 500, 502, 503, 504}
        self._rate_limiter = RateLimiter(rate_limit_max_calls, rate_limit_period)
        self._timeout = timeout

    def fetch_pages(
        self,
        request_builder: RequestBuilderABC,
        paginator: PaginatorABC,
        response_parser: ResponseParserABC,
    ) -> Iterable[list[Mapping[str, Any]]]:
        """Возвращает страницы записей, следуя логике пагинатора."""

        cursor = paginator.initial_cursor()
        while True:
            request = request_builder.build(cursor)
            response = self._send_request(request)
            records = list(response_parser.parse_records(response))
            yield records
            next_cursor = response_parser.get_next_cursor(response)
            cursor = paginator.advance(cursor, next_cursor)
            if not paginator.should_continue(cursor):
                break

    def fetch_records(
        self,
        request_builder: RequestBuilderABC,
        paginator: PaginatorABC,
        response_parser: ResponseParserABC,
    ) -> Iterable[Mapping[str, Any]]:
        """Итерирует все записи из всех страниц."""

        for page in self.fetch_pages(request_builder, paginator, response_parser):
            for record in page:
                yield record

    def _send_request(self, request: requests.Request) -> requests.Response:
        prepared = self._session.prepare_request(request)

        @backoff.on_exception(
            backoff.expo,
            requests.RequestException,
            max_tries=self._max_retries,
            factor=self._backoff_factor,
            giveup=self._should_give_up,
        )
        def _do_request() -> requests.Response:
            self._rate_limiter.acquire()
            response = self._session.send(prepared, timeout=self._timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    with contextlib.suppress(ValueError):
                        wait_for = float(retry_after)
                        if wait_for > 0:
                            time.sleep(wait_for)
            if response.status_code >= 400:
                response.raise_for_status()
            return response

        return _do_request()

    def _should_give_up(self, exc: Exception) -> bool:
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            status = exc.response.status_code
            if status in self._retry_statuses:
                return False
            if 400 <= status < 500:
                return True
        return False
