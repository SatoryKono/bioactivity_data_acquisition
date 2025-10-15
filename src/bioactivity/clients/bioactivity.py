"""HTTP client for retrieving bioactivity records."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from functools import partial
from types import TracebackType
from typing import Any, cast

import backoff
import requests

from bioactivity.config import APIClientConfig, RetrySettings

Json = dict[str, Any]


class BioactivityClient:
    """Client that retrieves bioactivity data from an HTTP API."""

    def __init__(
        self,
        config: APIClientConfig,
        *,
        session: requests.Session | None = None,
    ) -> None:
        self._config = config
        self._retries = config.retries
        self._session = session or requests.Session()

    def fetch_records(self) -> list[Json]:
        """Fetch all records from the configured API."""

        records: list[Json] = []
        page = 0
        max_pages = self._config.max_pages or float("inf")
        while page < max_pages:
            payload = self._perform_request(page)
            page_records = self._extract_records(payload)
            if not page_records:
                break
            records.extend(page_records)
            if not self._config.pagination_param:
                break
            page += 1
        return records

    def _extract_records(self, payload: Json) -> list[Json]:
        if isinstance(payload, dict) and "results" in payload:
            data = payload["results"]
        else:
            data = payload
        if not isinstance(data, Iterable):
            raise ValueError("API payload must contain an iterable of results")
        results: list[Json] = []
        for item in data:
            if not isinstance(item, dict):
                raise ValueError("Each record must be a JSON object")
            item.setdefault("source", self._config.name)
            item.setdefault("retrieved_at", dt.datetime.utcnow().isoformat())
            results.append(item)
        return results

    def _build_params(self, page: int) -> dict[str, Any]:
        params = dict(self._config.params)
        if self._config.pagination_param is not None:
            params[self._config.pagination_param] = page
        if self._config.page_size_param and self._config.page_size:
            params[self._config.page_size_param] = self._config.page_size
        return params

    def _perform_request(self, page: int) -> Json:
        params = self._build_params(page)
        response = self._request_with_retries(params)
        response.raise_for_status()
        return cast(Json, response.json())

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> BioactivityClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _request(self, params: dict[str, Any]) -> requests.Response:
        url = self._config.resolved_base_url
        return self._session.get(
            url,
            headers=self._config.headers,
            params=params,
            timeout=self._config.timeout,
        )

    def _backoff(self) -> Any:
        wait_gen = partial(backoff.expo, factor=self._retries.backoff_multiplier)
        return backoff.on_exception(
            wait_gen,
            requests.exceptions.RequestException,
            max_tries=self._retries.max_tries,
        )

    def _request_with_retries(self, params: dict[str, Any]) -> requests.Response:
        decorator = self._backoff()
        wrapped = cast(Callable[[dict[str, Any]], requests.Response], decorator(self._request))
        return wrapped(params)


__all__ = ["BioactivityClient"]
