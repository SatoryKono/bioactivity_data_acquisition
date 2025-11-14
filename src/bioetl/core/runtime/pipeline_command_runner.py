"""High-level abstractions for preparing and executing pipelines from the CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

from bioetl.config.environment import (
    EnvironmentSettings,
    apply_runtime_overrides,
    load_environment_settings,
)
from bioetl.config.loader import load_config
from bioetl.config.models.models import PipelineConfig
from bioetl.core import LoggerConfig, UnifiedLogger
from bioetl.pipelines.base import PipelineBase, RunResult

__all__ = [
    "ConfigLoadError",
    "EnvironmentSetupError",
    "PipelineCommandOptions",
    "PipelineCommandRunner",
    "PipelineConfigFactory",
    "PipelineExecutionPlan",
    "PipelineFactory",
    "PipelineDryRunPlan",
]


def _apply_runtime_overrides_safely(settings: EnvironmentSettings) -> None:
    """Apply runtime overrides for environment settings, discarding the return value."""

    apply_runtime_overrides(settings)


def _replace_config_section(
    config: PipelineConfig,
    *,
    section: str,
    updates: Mapping[str, Any],
) -> PipelineConfig:
    """Return a new PipelineConfig with an updated section without mutations."""

    if not updates:
        return config

    section_model = getattr(config, section)
    updated_section = section_model.model_copy(update=dict(updates))
    return config.model_copy(update={section: updated_section})


class PipelineFactory(Protocol):
    """Contract for pipeline factories used by the CLI layer."""

    def __call__(self, config: PipelineConfig, run_id: str) -> PipelineBase:
        ...


class PipelineCommandError(RuntimeError):
    """Base exception for CLI pipeline preparation failures."""


class EnvironmentSetupError(PipelineCommandError):
    """Environment initialization failure prior to pipeline execution."""

    def __init__(self, original: Exception) -> None:
        super().__init__(str(original))
        self.original = original


class ConfigLoadError(PipelineCommandError):
    """Pipeline configuration loading error."""

    def __init__(self, original: Exception, *, missing_reference: bool = False) -> None:
        super().__init__(str(original))
        self.original = original
        self.missing_reference = missing_reference


@dataclass(frozen=True)
class PipelineCommandOptions:
    """Normalized CLI options consumed by the runner."""

    config_path: Path
    output_dir: Path
    dry_run: bool = False
    verbose: bool = False
    set_overrides: Mapping[str, Any] = field(default_factory=dict)
    sample: int | None = None
    limit: int | None = None
    extended: bool = False
    fail_on_schema_drift: bool = True
    validate_columns: bool = True
    golden: Path | None = None
    input_file: Path | None = None


@dataclass(frozen=True)
class PipelineDryRunPlan:
    """Execution plan signalling dry-run mode."""

    config: PipelineConfig


@dataclass
class PipelineExecutionPlan:
    """Execution plan prepared by the runner."""

    run_id: str
    config: PipelineConfig
    options: PipelineCommandOptions
    run_kwargs: dict[str, Any]

    def run(self, pipeline_factory: PipelineFactory) -> RunResult:
        """Instantiate the pipeline through a factory and execute it."""

        pipeline = pipeline_factory(self.config, self.run_id)
        return pipeline.run(
            self.options.output_dir,
            **self.run_kwargs,
        )


class PipelineConfigFactory:
    """Factory that builds PipelineConfig instances from CLI options."""

    def __init__(
        self,
        *,
        environment_loader: Callable[[], EnvironmentSettings] = load_environment_settings,
        environment_runtime_applier: Callable[[EnvironmentSettings], object] | None = None,
        config_loader: Callable[..., PipelineConfig] | None = None,
    ) -> None:
        self._environment_loader = environment_loader
        self._environment_runtime_applier = (
            environment_runtime_applier or _apply_runtime_overrides_safely
        )
        self._config_loader = config_loader or load_config

    def create(self, options: PipelineCommandOptions) -> PipelineConfig:
        """Construct a PipelineConfig from CLI options."""

        try:
            environment_settings = self._environment_loader()
        except ValueError as exc:
            raise EnvironmentSetupError(exc) from exc

        self._environment_runtime_applier(environment_settings)

        cli_overrides = dict(options.set_overrides)
        try:
            pipeline_config = self._config_loader(
                config_path=options.config_path,
                cli_overrides=cli_overrides,
                include_default_profiles=True,
            )
        except FileNotFoundError as exc:
            raise ConfigLoadError(exc, missing_reference=True) from exc
        except ValueError as exc:
            raise ConfigLoadError(exc) from exc

        cli_updates: dict[str, Any] = {
            "dry_run": options.dry_run,
            "verbose": options.verbose,
            "extended": options.extended,
            "fail_on_schema_drift": options.fail_on_schema_drift,
            "validate_columns": options.validate_columns,
            "set_overrides": cli_overrides,
        }
        if options.limit is not None:
            cli_updates["limit"] = options.limit
        if options.sample is not None:
            cli_updates["sample"] = options.sample
        if options.golden is not None:
            cli_updates["golden"] = str(options.golden)
        if options.input_file is not None:
            cli_updates["input_file"] = str(options.input_file)

        pipeline_config = _replace_config_section(
            pipeline_config,
            section="cli",
            updates=cli_updates,
        )

        if not options.validate_columns:
            pipeline_config = _replace_config_section(
                pipeline_config,
                section="validation",
                updates={"strict": False},
            )

        pipeline_config = _replace_config_section(
            pipeline_config,
            section="materialization",
            updates={"root": str(options.output_dir)},
        )

        return pipeline_config


class PipelineCommandRunner:
    """Runner that prepares and executes pipelines from CLI-provided options."""

    def __init__(
        self,
        *,
        config_factory: PipelineConfigFactory | None = None,
        uuid_factory: Callable[[], str] | None = None,
        now_factory: Callable[[ZoneInfo], datetime] | None = None,
    ) -> None:
        self._config_factory = config_factory or PipelineConfigFactory()
        self._uuid_factory = uuid_factory or (lambda: str(uuid4()))
        self._now_factory = now_factory or (lambda tz: datetime.now(tz))

    def prepare(self, options: PipelineCommandOptions) -> PipelineExecutionPlan | PipelineDryRunPlan:
        """Prepare an execution plan based on CLI options."""

        config = self._config_factory.create(options)
        log_level = "DEBUG" if options.verbose else "INFO"
        UnifiedLogger.configure(LoggerConfig(level=log_level))

        if options.dry_run:
            return PipelineDryRunPlan(config=config)

        run_id = self._uuid_factory()

        timezone_name = config.determinism.environment.timezone
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:  # pragma: no cover - defensive fallback
            tz = ZoneInfo("UTC")

        if not config.cli.date_tag:
            config = _replace_config_section(
                config,
                section="cli",
                updates={"date_tag": self._now_factory(tz).strftime("%Y%m%d")},
            )

        run_kwargs = self._build_run_kwargs(config=config, options=options)

        return PipelineExecutionPlan(
            run_id=run_id,
            config=config,
            options=options,
            run_kwargs=run_kwargs,
        )

    @staticmethod
    def _build_run_kwargs(
        *,
        config: PipelineConfig,
        options: PipelineCommandOptions,
    ) -> dict[str, Any]:
        postprocess_config = getattr(config, "postprocess", None)
        correlation_section = getattr(postprocess_config, "correlation", None)
        correlation_enabled = bool(getattr(correlation_section, "enabled", False))

        effective_extended = bool(options.extended or getattr(config.cli, "extended", False))

        return {
            "extended": effective_extended,
            "include_correlation": effective_extended or correlation_enabled,
            "include_qc_metrics": effective_extended,
        }

