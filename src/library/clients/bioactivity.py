"""HTTP client for retrieving bioactivity records."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from functools import partial
from types import TracebackType
from typing import Any, cast

import backoff
import requests

from library.config import APIClientConfig

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

            # Проверяем, есть ли следующая страница
            if not self._config.pagination_param:
                break

            # Для ChEMBL API проверяем наличие следующей страницы в page_meta
            if isinstance(payload, dict) and "page_meta" in payload:
                page_meta = payload["page_meta"]
                if isinstance(page_meta, dict) and not page_meta.get("next"):
                    break  # Нет следующей страницы

            page += 1

        return records

    def fetch_records_by_ids(
        self,
        identifiers: list[str | int],
        batch_size: int = 50,
        filter_param: str = "activity_id__in",
    ) -> list[Json]:
        """Fetch records by list of IDs using batch requests.

        Args:
            identifiers: List of activity IDs to fetch
            batch_size: Number of IDs per request
            filter_param: API parameter name for filtering (e.g., 'activity_id__in')

        Returns:
            List of activity records
        """
        records: list[Json] = []

        # Short-circuit for empty input
        if not identifiers:
            return records

        for i in range(0, len(identifiers), batch_size):
            batch = identifiers[i : i + batch_size]
            ids_str = ",".join(str(id_val) for id_val in batch)
            payload = self._perform_request_with_filter(filter_param, ids_str)
            page_records = self._extract_records(payload)
            records.extend(page_records)
            self._log_batch_progress(i // batch_size + 1, len(identifiers), batch_size)

        return records

    def fetch_records_by_ids_parallel(
        self,
        identifiers: list[str | int],
        batch_size: int = 50,
        filter_param: str = "activity_id__in",
        max_workers: int = 4,
    ) -> list[Json]:
        """Fetch records in parallel batches using ThreadPoolExecutor."""
        import logging
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger = logging.getLogger(__name__)

        if not identifiers:
            return []

        batches = [identifiers[i : i + batch_size] for i in range(0, len(identifiers), batch_size)]

        all_records: list[Json] = []

        def fetch_batch(batch_ids: list[str | int]) -> list[Json]:
            ids_str = ",".join(str(id_val) for id_val in batch_ids)
            payload = self._perform_request_with_filter(filter_param, ids_str)
            return self._extract_records(payload)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(fetch_batch, batch): idx for idx, batch in enumerate(batches)}
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_records = future.result()
                    all_records.extend(batch_records)
                    logger.info(f"Batch {batch_idx + 1}/{len(batches)} completed: {len(batch_records)} records")
                except Exception as exc:  # pragma: no cover - defensive
                    logger.error(f"Batch {batch_idx + 1} failed: {exc}")

        return all_records

    def _perform_request_with_filter(self, filter_param: str, filter_value: str) -> Json:
        """Perform API request with custom filter parameter."""
        params = dict(self._config.params)
        params[filter_param] = filter_value

        if self._config.page_size_param and self._config.page_size:
            params[self._config.page_size_param] = self._config.page_size

        response = self._request_with_retries(params)
        response.raise_for_status()
        return cast(Json, response.json())

    def _log_batch_progress(self, batch_num: int, total_ids: int, batch_size: int) -> None:
        """Log progress of batch processing."""
        import logging

        logger = logging.getLogger(__name__)
        total_batches = (total_ids + batch_size - 1) // batch_size
        processed = min(batch_num * batch_size, total_ids)
        logger.info(f"Processed batch {batch_num}/{total_batches} ({processed}/{total_ids} IDs)")

    def _extract_records(self, payload: Json) -> list[Json]:
        # Поддерживаем различные форматы ответов API
        data = None
        if isinstance(payload, dict):
            # ChEMBL API возвращает данные в поле "activities"
            if "activities" in payload:
                data = payload["activities"]
            # Стандартный формат с полем "results"
            elif "results" in payload:
                data = payload["results"]
            # Если payload сам является массивом
            else:
                data = payload
        else:
            data = payload

        if not isinstance(data, Iterable):
            raise ValueError("API payload must contain an iterable of results")

        results: list[Json] = []
        for item in data:
            if not isinstance(item, dict):
                raise ValueError("Each record must be a JSON object")
            item.setdefault("source", self._config.name)
            item.setdefault("retrieved_at", dt.datetime.now(dt.timezone.utc).isoformat())
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
            max_tries=self._retries.total,
        )

    def _request_with_retries(self, params: dict[str, Any]) -> requests.Response:
        decorator = self._backoff()
        wrapped = cast(Callable[[dict[str, Any]], requests.Response], decorator(self._request))
        return wrapped(params)


__all__ = ["BioactivityClient"]
