from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

from bioetl.clients import client_exceptions


class ApiClientMixin:
    def _normalize_payload(self, payload: Any) -> Iterator[dict[str, Any]]:
        if isinstance(payload, Mapping):
            results = payload.get("results")
            if isinstance(results, Iterable) and not isinstance(
                results, (str, bytes, bytearray)
            ):
                for item in results:
                    if isinstance(item, Mapping):
                        yield dict(item)
                return

            if payload:
                yield dict(payload)
            return

        if isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
            for item in payload:
                if isinstance(item, Mapping):
                    yield dict(item)

    def _wrap_callable(
        self,
        func: Callable[[], Any],
        *,
        log_context: Mapping[str, Any] | None = None,
    ) -> Any:
        try:
            return func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", **context, error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def _wrap_iterator(
        self,
        func: Callable[[], Iterator[dict[str, Any]]],
        *,
        log_context: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        try:
            yield from func()
        except client_exceptions.HTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            context = dict(log_context or {})
            self._logger.error("api_call_failed", **context, error=str(exc))
            raise client_exceptions.RequestException(str(exc)) from exc

    def iter_ids(self, ids: Sequence[str], path_template: str = "/{entity}/{id}") -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            for raw_id in ids:
                entity_id = str(raw_id)
                path = path_template.format(entity=self.entity, id=entity_id)
                payload = self.api_client.get_json(path)
                self._logger.info("api_call", entity=self.entity, entity_id=entity_id)
                yield from self._normalize_payload(payload)

        return self._wrap_iterator(iterator)


__all__ = ["ApiClientMixin"]
