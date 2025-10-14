"""Configuration loader for pipeline CLI applications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import tomllib

try:  # pragma: no cover - optional dependency guard
    from pydantic import BaseModel, Field, ValidationError
except ImportError as exc:  # pragma: no cover - guidance for developers
    raise RuntimeError(
        "pydantic is required to load configuration. Install project dependencies first."
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "pipelines.toml"


class RuntimeDefaults(BaseModel):
    """Common runtime settings that can be overridden via CLI."""

    limit: Optional[int] = None
    output_dir: Optional[Path] = None
    date_tag: Optional[str] = None
    dry_run: Optional[bool] = None
    postprocess: Optional[bool] = None


class CommonSettings(BaseModel):
    """Top-level shared defaults for every pipeline."""

    defaults: RuntimeDefaults = Field(default_factory=RuntimeDefaults)


class PipelineDefinition(BaseModel):
    """Configuration specific to a named pipeline."""

    description: Optional[str] = None
    defaults: RuntimeDefaults = Field(default_factory=RuntimeDefaults)
    parameters: Dict[str, Any] = Field(default_factory=dict)


class AppConfig(BaseModel):
    """Root model for pipeline configuration."""

    common: CommonSettings = Field(default_factory=CommonSettings)
    pipelines: Dict[str, PipelineDefinition]

    @classmethod
    def from_path(cls, path: Path | None = None) -> "AppConfig":
        config_path = path or DEFAULT_CONFIG_PATH
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with config_path.open("rb") as file_obj:
            data = tomllib.load(file_obj)
        try:
            return cls(**data)
        except ValidationError as exc:  # pragma: no cover - direct feedback for CLI users
            raise ValueError(f"Invalid configuration: {exc}") from exc

    def get_pipeline(self, name: str) -> PipelineDefinition:
        try:
            return self.pipelines[name]
        except KeyError as exc:
            available = ", ".join(sorted(self.pipelines))
            raise KeyError(f"Pipeline '{name}' is not defined. Available: {available}") from exc


@dataclass(frozen=True)
class RuntimeOverrides:
    """Overrides supplied via CLI parameters."""

    limit: Optional[int] = None
    output_dir: Optional[Path] = None
    date_tag: Optional[str] = None
    dry_run: Optional[bool] = None
    postprocess: Optional[bool] = None


@dataclass(frozen=True)
class PipelineRuntime:
    """Resolved runtime settings with configuration parameters."""

    name: str
    limit: Optional[int]
    output_dir: Path
    date_tag: str
    dry_run: bool
    postprocess: bool
    parameters: Dict[str, Any]


def build_runtime(
    config: AppConfig,
    pipeline_name: str,
    overrides: RuntimeOverrides,
) -> PipelineRuntime:
    """Merge configuration defaults with CLI overrides for a pipeline."""

    pipeline_cfg = config.get_pipeline(pipeline_name)
    common_defaults = config.common.defaults

    limit = _first_not_none(
        overrides.limit, pipeline_cfg.defaults.limit, common_defaults.limit
    )
    output_dir = _first_not_none(
        overrides.output_dir, pipeline_cfg.defaults.output_dir, common_defaults.output_dir
    )
    date_tag = _first_not_none(
        overrides.date_tag,
        pipeline_cfg.defaults.date_tag,
        common_defaults.date_tag,
        datetime.utcnow().strftime("%Y%m%d"),
    )
    dry_run = _first_not_none(
        overrides.dry_run, pipeline_cfg.defaults.dry_run, common_defaults.dry_run, False
    )
    postprocess = _first_not_none(
        overrides.postprocess,
        pipeline_cfg.defaults.postprocess,
        common_defaults.postprocess,
        False,
    )

    if output_dir is None:
        raise ValueError(
            f"Output directory is not configured for pipeline '{pipeline_name}'."
        )

    return PipelineRuntime(
        name=pipeline_name,
        limit=limit,
        output_dir=output_dir if isinstance(output_dir, Path) else Path(output_dir),
        date_tag=str(date_tag),
        dry_run=bool(dry_run),
        postprocess=bool(postprocess),
        parameters=dict(pipeline_cfg.parameters),
    )


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
