"""Factory for creating HTTP API clients from configuration.

This module provides a factory for creating UnifiedAPIClient instances
from PipelineConfig, with support for HTTP profiles and per-source overrides.
"""

from __future__ import annotations

from typing import Any

from bioetl.configs.models import CacheConfig, HTTPClientConfig, HTTPConfig, PipelineConfig, SourceConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import get_logger


class APIClientFactory:
    """Factory for creating HTTP API clients from configuration."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize factory with pipeline configuration.

        Args:
            config: Pipeline configuration containing HTTP settings.
        """
        self.config = config
        self.logger = get_logger(__name__)
        self._clients: dict[str, UnifiedAPIClient] = {}

    def get_http_config(self, source_name: str) -> HTTPClientConfig:
        """Get HTTP configuration for a source.

        Args:
            source_name: Name of the source.

        Returns:
            HTTPClientConfig with merged settings from profile and source overrides.
        """
        http_config = self.config.http

        # Start with default profile
        base_config = http_config.default.model_copy(deep=True)

        # Get source configuration
        source_config = self.config.sources.get(source_name)
        if not source_config:
            self.logger.warning(
                "source_not_found",
                source=source_name,
                message=f"Source '{source_name}' not found in config, using default HTTP profile",
            )
            return base_config

        # Apply profile if specified
        if source_config.http_profile:
            profile = http_config.profiles.get(source_config.http_profile)
            if profile:
                # Merge profile settings into base
                base_config = self._merge_http_config(base_config, profile)
            else:
                self.logger.warning(
                    "http_profile_not_found",
                    source=source_name,
                    profile=source_config.http_profile,
                    message=f"HTTP profile '{source_config.http_profile}' not found, using default",
                )

        # Apply source-specific overrides if present
        if source_config.http:
            base_config = self._merge_http_config(base_config, source_config.http)

        return base_config

    def _merge_http_config(self, base: HTTPClientConfig, override: HTTPClientConfig) -> HTTPClientConfig:
        """Merge HTTP configuration with overrides.

        Args:
            base: Base configuration.
            override: Configuration with overrides.

        Returns:
            Merged configuration.
        """
        # Create a copy of base
        merged = base.model_copy(deep=True)

        # Override top-level fields
        if override.timeout_sec != base.timeout_sec:
            merged.timeout_sec = override.timeout_sec
        if override.connect_timeout_sec != base.connect_timeout_sec:
            merged.connect_timeout_sec = override.connect_timeout_sec
        if override.read_timeout_sec != base.read_timeout_sec:
            merged.read_timeout_sec = override.read_timeout_sec
        if override.rate_limit_jitter != base.rate_limit_jitter:
            merged.rate_limit_jitter = override.rate_limit_jitter

        # Merge retry config
        if override.retries != base.retries:
            merged.retries = override.retries.model_copy(deep=True)

        # Merge rate limit config
        if override.rate_limit != base.rate_limit:
            merged.rate_limit = override.rate_limit.model_copy(deep=True)

        # Merge headers
        if override.headers != base.headers:
            merged.headers = {**merged.headers, **override.headers}

        return merged

    def create_client(
        self,
        source_name: str,
        base_url: str,
        cache_config: CacheConfig | None = None,
    ) -> UnifiedAPIClient:
        """Create or get cached HTTP client for a source.

        Args:
            source_name: Name of the source.
            base_url: Base URL for the API.
            cache_config: Optional cache configuration (uses config.cache if not provided).

        Returns:
            UnifiedAPIClient instance.
        """
        # Check cache first
        if source_name in self._clients:
            return self._clients[source_name]

        # Get HTTP configuration
        http_config = self.get_http_config(source_name)

        # Use provided cache config or fall back to pipeline config
        cache = cache_config or self.config.cache

        # Create client
        client = UnifiedAPIClient(
            name=source_name,
            base_url=base_url,
            config=http_config,
            cache_config=cache,
        )

        # Cache client
        self._clients[source_name] = client

        self.logger.info(
            "client_created",
            source=source_name,
            base_url=base_url,
            http_profile=getattr(self.config.sources.get(source_name), "http_profile", None),
        )

        return client

    def get_client(self, source_name: str) -> UnifiedAPIClient | None:
        """Get cached client for a source.

        Args:
            source_name: Name of the source.

        Returns:
            UnifiedAPIClient if found, None otherwise.
        """
        return self._clients.get(source_name)

    def register_client(self, name: str, client: UnifiedAPIClient) -> None:
        """Register a client with the factory.

        Args:
            name: Name to register the client under.
            client: UnifiedAPIClient instance.
        """
        self._clients[name] = client
        self.logger.debug("client_registered", name=name)

    def close_all(self) -> None:
        """Close all registered clients."""
        for name, client in self._clients.items():
            try:
                client.close()
                self.logger.debug("client_closed", name=name)
            except Exception as e:
                self.logger.warning("client_close_failed", name=name, error=str(e))

        self._clients.clear()

    def __enter__(self) -> APIClientFactory:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close_all()

