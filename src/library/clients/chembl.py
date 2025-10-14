"""ChEMBL HTTP client."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .base import BaseClient


class ChEMBLClient(BaseClient):
    """Client specialised for ChEMBL bioactivity endpoints."""

    def fetch_activities(
        self,
        endpoint: str,
        *,
        page_size: int,
    ) -> Iterable[dict[str, Any]]:
        page = 0
        while True:
            page += 1
            payload = self.get_json(
                endpoint,
                params={"page": page, "page_size": page_size},
            )
            activities: list[dict[str, Any]] = payload.get("activities", [])
            yield from activities
            if not payload.get("next_page") or not activities:
                break

    def with_token(self, token: str | None) -> ChEMBLClient:
        """Return a client with an Authorization header if ``token`` provided."""
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        return self
