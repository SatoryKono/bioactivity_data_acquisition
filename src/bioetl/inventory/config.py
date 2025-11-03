"""Configuration loader for the pipeline inventory tooling."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import yaml
from pydantic import BaseModel, ConfigDict, field_validator


class ClusterConfig(BaseModel):
    """Settings controlling the clustering heuristics."""

    model_config = ConfigDict(validate_assignment=True)

    ngram_sizes: list[int] = [2, 3]
    min_jaccard: float = 0.3
    min_import_overlap: float = 0.25
    min_shared_ngrams: int = 2
    min_shared_imports: int = 1
    focus_keywords: list[str] = []

    @field_validator("ngram_sizes", mode="after")
    @classmethod
    def _validate_sizes(cls, value: Iterable[int]) -> list[int]:
        sizes = sorted({int(size) for size in value if int(size) > 0})
        if not sizes:
            msg = "At least one positive n-gram size must be provided"
            raise ValueError(msg)
        return sizes


class InventoryConfig(BaseModel):
    """Top level configuration for the inventory generation."""

    model_config = ConfigDict(validate_assignment=True)

    csv_output: Path
    cluster_report: Path
    root_dirs: list[Path]
    config_dirs: list[Path]
    include_extensions: list[str] = [".py", ".yaml", ".yml", ".md"]
    cluster: ClusterConfig = ClusterConfig()

    @field_validator("include_extensions", mode="after")
    @classmethod
    def _lowercase_extensions(cls, value: Iterable[str]) -> list[str]:
        return sorted({ext.lower() if ext.startswith(".") else f".{ext.lower()}" for ext in value})

    def with_base(self, base: Path) -> "InventoryConfig":
        """Return a copy of the configuration with all paths resolved."""

        def resolve_paths(paths: Iterable[Path]) -> list[Path]:
            return [ (base / path).resolve() if not path.is_absolute() else path.resolve() for path in paths ]

        csv_output = (base / self.csv_output).resolve() if not self.csv_output.is_absolute() else self.csv_output.resolve()
        cluster_report = (
            (base / self.cluster_report).resolve()
            if not self.cluster_report.is_absolute()
            else self.cluster_report.resolve()
        )
        return self.model_copy(
            update={
                "csv_output": csv_output,
                "cluster_report": cluster_report,
                "root_dirs": resolve_paths(self.root_dirs),
                "config_dirs": resolve_paths(self.config_dirs),
            }
        )

    @property
    def extension_set(self) -> set[str]:
        return set(self.include_extensions)


def load_inventory_config(path: Path, project_root: Path | None = None) -> InventoryConfig:
    """Load configuration from YAML file."""

    if not path.exists():
        msg = f"Inventory configuration file not found: {path}"
        raise FileNotFoundError(msg)

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    config = InventoryConfig.model_validate(data)
    base = project_root or path.parent.parent
    return config.with_base(base)
