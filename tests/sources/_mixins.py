"""Shared helpers for adapter unit tests."""

from __future__ import annotations

from typing import Any, ClassVar

from bioetl.adapters.base import AdapterConfig, ExternalAdapter
from bioetl.core.api_client import APIConfig


class AdapterTestMixin:
    """Provide convenience helpers for adapter unit tests."""

    ADAPTER_CLASS: ClassVar[type[ExternalAdapter] | None] = None
    API_CONFIG_OVERRIDES: ClassVar[dict[str, Any]] = {}
    ADAPTER_CONFIG_OVERRIDES: ClassVar[dict[str, Any]] = {}

    _API_CONFIG_DEFAULTS: ClassVar[dict[str, Any]] = {
        "name": "test-adapter",
        "base_url": "https://example.org",
        "rate_limit_max_calls": 1,
        "rate_limit_period": 1.0,
    }
    _ADAPTER_CONFIG_DEFAULTS: ClassVar[dict[str, Any]] = {
        "enabled": True,
        "batch_size": 50,
        "workers": 1,
    }

    def make_api_config(self, **overrides: Any) -> APIConfig:
        """Build an :class:`APIConfig` tailored for a test."""

        params = {**self._API_CONFIG_DEFAULTS, **self.API_CONFIG_OVERRIDES, **overrides}
        return APIConfig(**params)

    def make_adapter_config(self, **overrides: Any) -> AdapterConfig:
        """Build an :class:`AdapterConfig` tailored for a test."""

        params = {
            **self._ADAPTER_CONFIG_DEFAULTS,
            **self.ADAPTER_CONFIG_OVERRIDES,
            **overrides,
        }
        return AdapterConfig(**params)

    def setUp(self) -> None:  # pragma: no cover - exercised via inheriting tests
        """Auto-initialize ``self.adapter`` when possible."""

        super().setUp()  # type: ignore[misc]
        if self.ADAPTER_CLASS is not None:
            self.api_config = self.make_api_config()
            self.adapter_config = self.make_adapter_config()
            self.adapter = self.ADAPTER_CLASS(self.api_config, self.adapter_config)
