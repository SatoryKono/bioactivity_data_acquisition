from __future__ import annotations

from typing import Any, Mapping

from bioetl.config import load_client_config
from bioetl.clients.legacy import PaginationParams


def split_name(name: str) -> tuple[str, str]:
    source, _, resource = name.partition(".")
    if not source or not resource:
        msg = "Client name must look like '<source>.<resource>'"
        raise ValueError(msg)
    return source, resource


def load_resource_settings(source: str, resource: str) -> Mapping[str, Any]:
    resources = load_client_config(source)
    try:
        return resources[resource]
    except KeyError as exc:
        msg = f"Resource '{resource}' is not defined in configs/{source}.yaml"
        raise ValueError(msg) from exc


def build_pagination(config: Mapping[str, Any] | None) -> PaginationParams | None:
    if config is None:
        return None
    return PaginationParams(**dict(config))


__all__ = ["build_pagination", "load_resource_settings", "split_name"]
