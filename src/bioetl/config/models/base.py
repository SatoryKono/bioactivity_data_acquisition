"""Base pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

# ruff: noqa: I001
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .cache import CacheConfig
from .cli import CLIConfig
from .determinism import DeterminismConfig
from .domain import PipelineDomainConfig
from .fallbacks import FallbacksConfig
from .http import HTTPConfig
from .infrastructure import PipelineInfrastructureConfig
from .io import IOConfig
from .logging import LoggingConfig
from .paths import MaterializationConfig
from .paths import PathsConfig
from .postprocess import PostprocessConfig
from .runtime import RuntimeConfig
from .source import SourceConfig
from .telemetry import TelemetryConfig
from .transform import TransformConfig
from .validation import ValidationConfig

class PipelineMetadata(BaseModel):
    """Descriptive metadata for the pipeline itself."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Unique name of the pipeline (e.g., activity, assay).")
    version: str = Field(..., description="Semantic version of the pipeline implementation.")
    owner: str | None = Field(
        default=None, description="Team or individual responsible for the pipeline."
    )
    description: str | None = Field(default=None, description="Short human readable description.")


DOMAIN_SECTION_FIELDS: tuple[str, ...] = (
    "validation",
    "transform",
    "postprocess",
    "fallbacks",
    "sources",
    "chembl",
)
INFRASTRUCTURE_SECTION_FIELDS: tuple[str, ...] = (
    "runtime",
    "io",
    "http",
    "cache",
    "paths",
    "determinism",
    "materialization",
    "logging",
    "telemetry",
    "cli",
)


class PipelineConfig(BaseModel):
    """Root configuration split into explicit domain and infrastructure sections.

    Domain controls business-facing behavior: schema validation, transforms,
    domain fallbacks, enrichment toggles, and source metadata.

    Infrastructure covers cross-cutting execution layers: IO, HTTP, runtime,
    deterministic write policies, logging, telemetry, paths, and CLI overrides.
    """

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = Field(
        ..., description="Configuration schema version. Only version=1 is currently supported."
    )
    extends: Sequence[str] = Field(
        default_factory=tuple,
        description="Optional list of profile paths merged before this configuration.",
    )
    pipeline: PipelineMetadata = Field(..., description="Metadata describing the pipeline.")
    domain: PipelineDomainConfig = Field(
        default_factory=PipelineDomainConfig,
        description="Domain-level configuration: schemas, transforms, enrichment, sources.",
    )
    infrastructure: PipelineInfrastructureConfig = Field(
        ...,
        description="Infrastructure configuration: runtime, IO, HTTP, logging, determinism.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_sections(cls, data: Any) -> Mapping[str, Any]:
        if not isinstance(data, Mapping):
            return data

        normalized: dict[str, Any] = dict(data)
        domain_payload = cls._as_section_mapping(normalized.pop("domain", None))
        infra_payload = cls._as_section_mapping(normalized.pop("infrastructure", None))

        for field in DOMAIN_SECTION_FIELDS:
            if field in normalized:
                domain_payload.setdefault(field, normalized.pop(field))
        for field in INFRASTRUCTURE_SECTION_FIELDS:
            if field in normalized:
                infra_payload.setdefault(field, normalized.pop(field))

        normalized["domain"] = domain_payload
        normalized["infrastructure"] = infra_payload
        return normalized

    @staticmethod
    def _as_section_mapping(payload: Any) -> dict[str, Any]:
        if payload is None:
            return {}
        if isinstance(payload, BaseModel):
            return payload.model_dump()
        if isinstance(payload, Mapping):
            return dict(payload)
        raise TypeError(
            "Section payloads must be mappings or BaseModel instances; "
            f"received {type(payload)!r}"
        )

    @classmethod
    def _prepare_section_update(cls, update: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(update)
        domain_patch = cls._as_section_mapping(normalized.pop("domain", None))
        infra_patch = cls._as_section_mapping(normalized.pop("infrastructure", None))

        for field in DOMAIN_SECTION_FIELDS:
            if field in normalized:
                domain_patch[field] = normalized.pop(field)
        for field in INFRASTRUCTURE_SECTION_FIELDS:
            if field in normalized:
                infra_patch[field] = normalized.pop(field)

        if domain_patch:
            normalized["domain"] = domain_patch
        if infra_patch:
            normalized["infrastructure"] = infra_patch
        return normalized

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> "PipelineConfig":
        normalized_update = self._prepare_section_update(update) if update else None
        if not normalized_update:
            return super().model_copy(update=None, deep=deep)

        update_payload = dict(normalized_update)
        domain_delta = update_payload.pop("domain", None)
        infra_delta = update_payload.pop("infrastructure", None)

        if domain_delta:
            update_payload["domain"] = self.domain.model_copy(update=domain_delta)
        if infra_delta:
            update_payload["infrastructure"] = self.infrastructure.model_copy(update=infra_delta)

        return super().model_copy(
            update=update_payload if update_payload else None,
            deep=deep,
        )

    @property
    def common(self) -> PipelineCommonCompat:
        """Compatibility shim exposing legacy `config.common` attributes.

        Older test helpers expect `config.common` to surface runtime overrides
        such as `input_file`, `limit`, and `extended`. The modern configuration
        schema captures these under `config.cli`; this property delegates
        attribute access without duplicating state.
        """

        return PipelineCommonCompat(self.cli)

    @model_validator(mode="after")
    def ensure_column_order_when_set(self) -> PipelineConfig:
        if self.determinism.column_order and not self.validation.schema_out:
            msg = (
                "determinism.column_order requires validation.schema_out to be set so the schema "
                "can enforce the order"
            )
            raise ValueError(msg)
        return self

    @property
    def runtime(self) -> RuntimeConfig:
        return self.infrastructure.runtime

    @runtime.setter
    def runtime(self, value: RuntimeConfig) -> None:
        self.infrastructure.runtime = value

    @property
    def io(self) -> IOConfig:
        return self.infrastructure.io

    @io.setter
    def io(self, value: IOConfig) -> None:
        self.infrastructure.io = value

    @property
    def http(self) -> HTTPConfig:
        return self.infrastructure.http

    @http.setter
    def http(self, value: HTTPConfig) -> None:
        self.infrastructure.http = value

    @property
    def cache(self) -> CacheConfig:
        return self.infrastructure.cache

    @cache.setter
    def cache(self, value: CacheConfig) -> None:
        self.infrastructure.cache = value

    @property
    def paths(self) -> PathsConfig:
        return self.infrastructure.paths

    @paths.setter
    def paths(self, value: PathsConfig) -> None:
        self.infrastructure.paths = value

    @property
    def determinism(self) -> DeterminismConfig:
        return self.infrastructure.determinism

    @determinism.setter
    def determinism(self, value: DeterminismConfig) -> None:
        self.infrastructure.determinism = value

    @property
    def materialization(self) -> MaterializationConfig:
        return self.infrastructure.materialization

    @materialization.setter
    def materialization(self, value: MaterializationConfig) -> None:
        self.infrastructure.materialization = value

    @property
    def logging(self) -> LoggingConfig:
        return self.infrastructure.logging

    @logging.setter
    def logging(self, value: LoggingConfig) -> None:
        self.infrastructure.logging = value

    @property
    def telemetry(self) -> TelemetryConfig:
        return self.infrastructure.telemetry

    @telemetry.setter
    def telemetry(self, value: TelemetryConfig) -> None:
        self.infrastructure.telemetry = value

    @property
    def cli(self) -> CLIConfig:
        return self.infrastructure.cli

    @cli.setter
    def cli(self, value: CLIConfig) -> None:
        self.infrastructure.cli = value

    @property
    def fallbacks(self) -> FallbacksConfig:
        return self.domain.fallbacks

    @fallbacks.setter
    def fallbacks(self, value: FallbacksConfig) -> None:
        self.domain.fallbacks = value

    @property
    def validation(self) -> ValidationConfig:
        return self.domain.validation

    @validation.setter
    def validation(self, value: ValidationConfig) -> None:
        self.domain.validation = value

    @property
    def transform(self) -> TransformConfig:
        return self.domain.transform

    @transform.setter
    def transform(self, value: TransformConfig) -> None:
        self.domain.transform = value

    @property
    def postprocess(self) -> PostprocessConfig:
        return self.domain.postprocess

    @postprocess.setter
    def postprocess(self, value: PostprocessConfig) -> None:
        self.domain.postprocess = value

    @property
    def sources(self) -> dict[str, SourceConfig]:
        return self.domain.sources

    @sources.setter
    def sources(self, value: Mapping[str, SourceConfig]) -> None:
        self.domain.sources = dict(value)

    @property
    def chembl(self) -> Mapping[str, Any] | None:
        return self.domain.chembl

    @chembl.setter
    def chembl(self, value: Mapping[str, Any] | None) -> None:
        self.domain.chembl = value


class PipelineCommonCompat:
    """Read-only facade mapping legacy `common` fields onto ``CLIConfig``."""

    __slots__ = ("_cli",)

    def __init__(self, cli: CLIConfig) -> None:
        self._cli = cli

    @property
    def input_file(self) -> str | None:
        return self._cli.input_file

    @property
    def limit(self) -> int | None:
        return self._cli.limit

    @property
    def sample(self) -> int | None:
        return self._cli.sample

    @property
    def extended(self) -> bool:
        return self._cli.extended

    @property
    def dry_run(self) -> bool:
        return self._cli.dry_run

    @property
    def fail_on_schema_drift(self) -> bool:
        return self._cli.fail_on_schema_drift

    @property
    def validate_columns(self) -> bool:
        return self._cli.validate_columns

    @property
    def golden(self) -> str | None:
        return self._cli.golden

    @property
    def verbose(self) -> bool:
        return self._cli.verbose

    def __getattr__(self, item: str) -> Any:
        return getattr(self._cli, item)
