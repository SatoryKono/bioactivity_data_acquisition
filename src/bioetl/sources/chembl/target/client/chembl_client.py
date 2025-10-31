from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from bioetl.config import PipelineConfig
from bioetl.config.models import TargetSourceConfig
from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.chembl import ChemblClientContext, create_chembl_client
from bioetl.core.client_factory import APIClientFactory, ensure_target_source_config
from bioetl.core.deprecation import warn_legacy_client

__all__ = ["TargetClientManager", "ClientRegistration"]

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.target")


@dataclass(frozen=True)
class ClientRegistration:
    """Resolved client registration entry."""

    name: str
    client: UnifiedAPIClient
    source_config: TargetSourceConfig


class TargetClientManager:
    """Helper responsible for constructing and registering target API clients."""

    def __init__(
        self,
        config: PipelineConfig,
        register_client: Callable[[UnifiedAPIClient], None],
        *,
        defaults: Mapping[str, Any] | None = None,
    ) -> None:
        self._config = config
        self._register_client = register_client
        self._factory = APIClientFactory.from_pipeline_config(config)
        self.source_configs: dict[str, TargetSourceConfig] = {}
        self.api_clients: dict[str, UnifiedAPIClient] = {}

        self.chembl_context: ChemblClientContext = create_chembl_client(
            config,
            defaults=defaults,
        )
        self._register("chembl", self.chembl_context)

        self._initialise_optional_clients()

    def _register(self, name: str, context: ChemblClientContext | ClientRegistration) -> None:
        """Persist a client registration and expose it to the pipeline."""

        if isinstance(context, ChemblClientContext):
            client = context.client
            source_config = context.source_config
        else:
            client = context.client
            source_config = context.source_config

        self.source_configs[name] = source_config
        self.api_clients[name] = client
        self._register_client(client)

    def _initialise_optional_clients(self) -> None:
        """Create non-primary clients declared in the pipeline configuration."""

        for source_name, raw_source in self._config.sources.items():
            if source_name == "chembl" or raw_source is None:
                continue

            source_config = ensure_target_source_config(raw_source, defaults={})
            if not source_config.enabled:
                continue

            api_client_config = self._factory.create(source_name, source_config)
            client = UnifiedAPIClient(api_client_config)
            registration = ClientRegistration(
                name=source_name,
                client=client,
                source_config=source_config,
            )
            self._register(source_name, registration)

    def get_client(self, name: str) -> UnifiedAPIClient | None:
        """Retrieve a client by its registry name."""

        return self.api_clients.get(name)

    def get_source_config(self, name: str) -> TargetSourceConfig | None:
        """Retrieve the resolved source configuration."""

        return self.source_configs.get(name)

    @property
    def batch_size(self) -> int:
        """Expose the resolved ChEMBL batch size for pagination helpers."""

        return int(self.chembl_context.batch_size)

