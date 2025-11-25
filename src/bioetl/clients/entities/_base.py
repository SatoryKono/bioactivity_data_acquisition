from __future__ import annotations

from typing import Any, Iterator, Mapping, MutableMapping, Sequence

from bioetl.core.http.api_client import UnifiedAPIClient


class _BaseEntityClient:
    def __init__(self, api_client: UnifiedAPIClient, entity: str) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")

    def fetch_by_ids(self, ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        for entity_id in ids:
            response = self.api_client.get(f"/{self.entity}/{entity_id}")
            response.raise_for_status()
            results[str(entity_id)] = response.json()
        return results

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        query_params: MutableMapping[str, Any] = {"limit": page_size}
        if params:
            query_params.update(params)

        next_path: str | None = f"/{self.entity}"
        while next_path:
            response = self.api_client.get(next_path, params=query_params)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, Mapping):
                items = payload.get("results")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, Mapping):
                            yield dict(item)
                elif payload:
                    yield dict(payload)
                next_path = payload.get("next") if isinstance(payload.get("next"), str) else None
                query_params = {}
            elif isinstance(payload, list):
                for item in payload:
                    if isinstance(item, Mapping):
                        yield dict(item)
                next_path = None
            else:
                next_path = None
