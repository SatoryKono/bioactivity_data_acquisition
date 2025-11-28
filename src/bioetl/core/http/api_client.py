"""Unified HTTP client for BioETL pipeline components."""

from __future__ import annotations

from typing import Any, Dict, Iterator, Mapping

import requests
import structlog

from bioetl.core.http.config import APIConfig

__all__ = ["APIConfig", "UnifiedAPIClient"]


class UnifiedAPIClient:
    """Simple adapter bundling executor, builder, and pagination strategy."""

    def __init__(
        self,
        api_config: APIConfig,
        *,
        request_executor: Any,
        request_builder: Any,
        pagination_strategy: Any,
    ) -> None:
        self.api_config = api_config
        self.request_executor = request_executor
        self.request_builder = request_builder
        self.pagination_strategy = pagination_strategy

    def close(self) -> None:
        close_fn = getattr(self.request_executor, "close", None)
        if callable(close_fn):
            close_fn()
            return
        close_fn = getattr(self.request_builder, "close", None)
        if callable(close_fn):
            close_fn()
