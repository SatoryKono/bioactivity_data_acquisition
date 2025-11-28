from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from bioetl.clients.enrichers.facade import EnrichmentStrategy


class StrategyRegistry(Mapping[str, "EnrichmentStrategy"]):
    """Реестр стратегий обогащения."""

    def __init__(self, strategies: Mapping[str, EnrichmentStrategy] | None = None):
        self._strategies = dict(strategies or {})

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any] | None
    ) -> "StrategyRegistry | None":
        """Построить реестр стратегий из конфигурации ``enrichers``."""

        if not isinstance(config, Mapping):
            return None

        strategies_cfg = config.get("strategies")
        if isinstance(strategies_cfg, StrategyRegistry):
            return strategies_cfg
        if isinstance(strategies_cfg, Mapping):
            return cls(strategies_cfg)

        return cls(cls._derive_default_strategies(config))

    @staticmethod
    def _derive_default_strategies(
        config: Mapping[str, Any]
    ) -> dict[str, "EnrichmentStrategy"]:
        default_methods: Mapping[str, str] = {
            "pubchem_client": "lookup",
            "uniprot_client": "fetch",
            "iuphar_client": "fetch",
        }

        strategies: dict[str, "EnrichmentStrategy"] = {}
        for key, method_name in default_methods.items():
            client = config.get(key)
            if client is None:
                continue
            if not callable(getattr(client, method_name, None)):
                continue

            from bioetl.clients.enrichers.facade import ClientMethodStrategy

            strategies[key.removesuffix("_client")] = ClientMethodStrategy(
                lambda _factory, client=client: client,
                method_name,
            )

        return strategies

    def __getitem__(self, key: str) -> EnrichmentStrategy:
        return self._strategies[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._strategies)

    def __len__(self) -> int:  # pragma: no cover - simple passthrough
        return len(self._strategies)


__all__ = ["StrategyRegistry"]
