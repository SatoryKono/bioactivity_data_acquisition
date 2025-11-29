from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from bioetl.clients.config.models import SourceConfig

# Клиентские YAML-файлы хранятся вместе с кодом для упрощения поставки.
DEFAULT_CONFIG_ROOT = Path(__file__).resolve().parent / "yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)
    content = path.read_text(encoding="utf-8")
    data = yaml.safe_load(content) or {}
    if not isinstance(data, dict):
        msg = f"Expected mapping at root of {path}"
        raise ValueError(msg)
    return data


def load_source_config(name: str, root: Path | None = None) -> SourceConfig:
    """Загрузить конфигурацию источника по имени файла без расширения."""

    config_root = root or DEFAULT_CONFIG_ROOT
    path = Path(config_root) / f"{name}.yml"
    data = _load_yaml(path)
    if "source" not in data:
        data["source"] = name
    return SourceConfig.model_validate(data)


def load_all_sources(root: Path | None = None) -> dict[str, SourceConfig]:
    """Загрузить все YAML в каталоге root → SourceConfig."""

    config_root = root or DEFAULT_CONFIG_ROOT
    sources: dict[str, SourceConfig] = {}
    for path in Path(config_root).glob("*.yml"):
        cfg = load_source_config(path.stem, root=config_root)
        sources[cfg.source] = cfg
    return sources


__all__ = ["DEFAULT_CONFIG_ROOT", "load_all_sources", "load_source_config"]
