"""Factories that build configured HTTP clients for sources."""

from __future__ import annotations

from bioetl.config.models import HTTPClientConfig, PipelineConfig, SourceConfig

from .api_client import UnifiedAPIClient, merge_http_configs
from .logger import UnifiedLogger

__all__ = ["APIClientFactory"]


class APIClientFactory:
    """Create fully configured :class:`UnifiedAPIClient` instances."""

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._log = UnifiedLogger.get(__name__).bind(component="client_factory")

    def build(
        self,
        *,
        base_url: str,
        source: str | None = None,
        profile: str | None = None,
        overrides: HTTPClientConfig | None = None,
        name: str | None = None,
    ) -> UnifiedAPIClient:
        """Return a :class:`UnifiedAPIClient` for the given settings."""

        http_config = self._resolve_http_config(profile=profile, overrides=overrides)
        client_name = name or source or profile or "default"
        self._log.debug(
            "client_factory.build",
            client=client_name,
            profile=profile or "default",
            base_url=base_url,
        )
        return UnifiedAPIClient(http_config, base_url=base_url, name=client_name)

    def for_source(self, source_name: str, *, base_url: str) -> UnifiedAPIClient:
        """Build a client using the configuration for ``source_name``."""

        source_config = self._get_source(source_name)
        profile = source_config.http_profile
        overrides = source_config.http
        parameters = source_config.parameters or {}
        max_url_length = parameters.get("max_url_length")
        if isinstance(max_url_length, int) and max_url_length > 0:
            if overrides is not None:
                overrides = overrides.model_copy(update={"max_url_length": max_url_length})
            else:
                overrides = HTTPClientConfig(max_url_length=max_url_length)
        return self.build(
            base_url=base_url,
            source=source_name,
            profile=profile,
            overrides=overrides,
            name=source_name,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_http_config(
        self,
        *,
        profile: str | None,
        overrides: HTTPClientConfig | None,
    ) -> HTTPClientConfig:
        default = self._config.http.default
        profile_config: HTTPClientConfig | None = None
        if profile:
            try:
                profile_config = self._config.http.profiles[profile]
            except KeyError as exc:
                msg = f"Unknown HTTP profile '{profile}'"
                raise KeyError(msg) from exc
        return merge_http_configs(default, profile_config, overrides)

    def _get_source(self, name: str) -> SourceConfig:
        try:
            return self._config.sources[name]
        except KeyError as exc:
            msg = f"Unknown source '{name}'"
            raise KeyError(msg) from exc
