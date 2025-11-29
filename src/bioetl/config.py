from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PROJECT_ROOT / "configs"


def _merge_pagination(
    default_pagination: Mapping[str, Any] | None,
    resource_pagination: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if default_pagination is None and resource_pagination is None:
        return None
    merged: dict[str, Any] = {}
    if default_pagination:
        merged.update(default_pagination)
    if resource_pagination:
        merged.update(resource_pagination)
    return merged


def load_client_config(source: str) -> dict[str, dict[str, Any]]:
    """Загрузить конфигурацию клиента из ``configs/<source>.yaml``.

    Возвращает словарь ресурсов с уже применёнными дефолтами пагинации.
    Ожидается структура вида:

    .. code-block:: yaml

       pagination: {page_size: 100, max_pages: 5}
       resources:
         article:
           endpoint: /article
           id_field: pmid
           filter_mapping: {status: term}
           pagination: {page_size: 50}
    """

    config_path = CONFIGS_DIR / f"{source}.yaml"
    if not config_path.exists():
        msg = f"Client config not found: {config_path}"
        raise FileNotFoundError(msg)

    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    resources = payload.get("resources")
    if not isinstance(resources, Mapping):
        msg = f"Client config must define 'resources' mapping: {config_path}"
        raise ValueError(msg)

    default_pagination = payload.get("pagination")
    if default_pagination is not None and not isinstance(default_pagination, Mapping):
        msg = "pagination must be a mapping when provided"
        raise TypeError(msg)

    resolved: dict[str, dict[str, Any]] = {}
    for resource_name, config in resources.items():
        if not isinstance(config, Mapping):
            msg = f"Resource config must be a mapping: {resource_name} in {config_path}"
            raise TypeError(msg)

        merged_pagination = _merge_pagination(
            default_pagination,
            config.get("pagination") if isinstance(config.get("pagination"), Mapping) else None,
        )
        resource_config: dict[str, Any] = dict(config)
        resource_config["pagination"] = merged_pagination
        resolved[str(resource_name)] = resource_config

    return resolved


__all__ = ["load_client_config"]
