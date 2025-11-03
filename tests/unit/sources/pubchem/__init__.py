"""Shared helpers for PubChem source tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class StubUnifiedAPIClient:
    """In-memory stand-in for :class:`bioetl.core.api_client.UnifiedAPIClient`."""

    responses: Dict[str, Any] = field(default_factory=dict)
    requests: List[str] = field(default_factory=list)

    def request_json(self, endpoint: str) -> Any:
        self.requests.append(endpoint)
        if endpoint in self.responses:
            payload = self.responses[endpoint]
            if isinstance(payload, Exception):
                raise payload
            return payload
        return None
