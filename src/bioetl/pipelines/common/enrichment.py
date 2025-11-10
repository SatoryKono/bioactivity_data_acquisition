"""Common abstractions for configurable enrichment rules.

This module defines a light-weight strategy layer that pipelines can reuse
for enrichment steps. Each enrichment rule exposes a simple protocol driven
by a ``pandas.DataFrame`` input, an optional API client, and a configuration
mapping resolved from the pipeline configuration tree. Pipelines register the
rules they need together with a client factory; the strategy then resolves the
appropriate configuration blocks and executes enabled rules sequentially.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable

import pandas as pd
from structlog.stdlib import BoundLogger

ConfigPath = tuple[str, ...]
EnrichmentFn = Callable[[pd.DataFrame, Any, Mapping[str, Any]], pd.DataFrame]
LoggerCallback = Callable[[BoundLogger], None]


def _normalize_mapping(candidate: Any) -> Mapping[str, Any] | None:
    """Return candidate as an immutable mapping when possible."""

    if candidate is None:
        return None

    if isinstance(candidate, Mapping):
        return MappingProxyType(dict(candidate))

    model_dump = getattr(candidate, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, Mapping):
            return MappingProxyType(dict(dumped))

    as_dict = getattr(candidate, "dict", None)
    if callable(as_dict):
        dumped = as_dict()
        if isinstance(dumped, Mapping):
            return MappingProxyType(dict(dumped))

    return None


@runtime_checkable
class EnrichmentRule(Protocol):
    """Minimal interface implemented by enrichment rules."""

    name: str
    config_path: ConfigPath
    requires_client: bool

    def handle_missing_config(self, logger: BoundLogger) -> None:
        """Emit diagnostic log when configuration for the rule is absent."""

    def handle_disabled(self, logger: BoundLogger) -> None:
        """Emit diagnostic log when the rule is explicitly disabled."""

    def is_enabled(self, config_section: Mapping[str, Any]) -> bool:
        """Return ``True`` when the rule should run for the provided config."""

    def apply(
        self,
        df: pd.DataFrame,
        *,
        client: Any,
        config: Mapping[str, Any],
    ) -> pd.DataFrame:
        """Execute the enrichment logic and return an updated DataFrame."""


@dataclass(frozen=True)
class FunctionEnrichmentRule:
    """Adapter turning a callable into an :class:`EnrichmentRule` instance."""

    name: str
    config_path: ConfigPath
    function: EnrichmentFn
    requires_client: bool = True
    default_enabled: bool = True
    on_missing_config: LoggerCallback | None = None
    on_disabled: LoggerCallback | None = None

    def handle_missing_config(self, logger: BoundLogger) -> None:
        if self.on_missing_config is not None:
            self.on_missing_config(logger)
            return
        logger.debug(
            "enrichment_rule_skipped_missing_config",
            rule=self.name,
            config_path=self.config_path,
        )

    def handle_disabled(self, logger: BoundLogger) -> None:
        if self.on_disabled is not None:
            self.on_disabled(logger)
            return
        logger.debug(
            "enrichment_rule_disabled",
            rule=self.name,
            config_path=self.config_path,
        )

    def is_enabled(self, config_section: Mapping[str, Any]) -> bool:
        enabled = config_section.get("enabled")
        if enabled is None:
            return self.default_enabled
        return bool(enabled)

    def apply(
        self,
        df: pd.DataFrame,
        *,
        client: Any,
        config: Mapping[str, Any],
    ) -> pd.DataFrame:
        if self.requires_client and client is None:
            msg = f"Enrichment rule '{self.name}' requires a client provider"
            raise RuntimeError(msg)
        return self.function(df, client, config)


class EnrichmentStrategy:
    """Coordinator executing registered enrichment rules in sequence."""

    def __init__(
        self,
        *,
        config_root: Mapping[str, Any] | None,
        rules: Sequence[EnrichmentRule],
        logger: BoundLogger,
        client_provider: Callable[[], Any] | None = None,
        base_path: Sequence[str] = (),
    ) -> None:
        self._config_root = _normalize_mapping(config_root)
        self._rules = tuple(rules)
        self._logger = logger
        self._client_provider = client_provider
        self._base_path = tuple(base_path)
        self._shared_client: Any | None = None

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply enabled rules to ``df`` and return the resulting frame."""

        result = df
        if not self._rules:
            return result

        for rule in self._rules:
            config_section = self._resolve_rule_config(rule)
            if config_section is None:
                rule.handle_missing_config(self._logger)
                continue

            if not rule.is_enabled(config_section):
                rule.handle_disabled(self._logger)
                continue

            client: Any = None
            if rule.requires_client:
                client = self._ensure_client()

            result = rule.apply(result, client=client, config=config_section)

        return result

    def _ensure_client(self) -> Any:
        if self._client_provider is None:
            msg = "EnrichmentStrategy requires a client provider for client-bound rules"
            raise RuntimeError(msg)
        if self._shared_client is None:
            self._shared_client = self._client_provider()
        return self._shared_client

    def _resolve_rule_config(self, rule: EnrichmentRule) -> Mapping[str, Any] | None:
        if self._config_root is None:
            return None

        path = self._base_path + rule.config_path
        current: Any = self._config_root
        for key in path:
            mapping = _normalize_mapping(current)
            if mapping is None:
                return None
            current = mapping.get(key)
        return _normalize_mapping(current)


__all__ = [
    "ConfigPath",
    "EnrichmentFn",
    "EnrichmentRule",
    "EnrichmentStrategy",
    "FunctionEnrichmentRule",
]
