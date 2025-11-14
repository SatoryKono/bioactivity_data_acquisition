"""Base pipeline configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

# ruff: noqa: I001
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._proxy_utils import ProxyDefinition, build_section_proxies
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

    if TYPE_CHECKING:
        runtime: RuntimeConfig
        io: IOConfig
        http: HTTPConfig
        cache: CacheConfig
        paths: PathsConfig
        determinism: DeterminismConfig
        materialization: MaterializationConfig
        logging: LoggingConfig
        telemetry: TelemetryConfig
        cli: CLIConfig
        fallbacks: FallbacksConfig
        validation: ValidationConfig
        transform: TransformConfig
        postprocess: PostprocessConfig
        sources: dict[str, SourceConfig]
        chembl: Mapping[str, Any] | None


_PIPELINE_CONFIG_PROXY_FIELDS: tuple[ProxyDefinition, ...] = (
    ProxyDefinition(
        attr="runtime",
        path=("infrastructure", "runtime"),
        return_type=RuntimeConfig,
    ),
    ProxyDefinition(
        attr="io",
        path=("infrastructure", "io"),
        return_type=IOConfig,
    ),
    ProxyDefinition(
        attr="http",
        path=("infrastructure", "http"),
        return_type=HTTPConfig,
    ),
    ProxyDefinition(
        attr="cache",
        path=("infrastructure", "cache"),
        return_type=CacheConfig,
    ),
    ProxyDefinition(
        attr="paths",
        path=("infrastructure", "paths"),
        return_type=PathsConfig,
    ),
    ProxyDefinition(
        attr="determinism",
        path=("infrastructure", "determinism"),
        return_type=DeterminismConfig,
    ),
    ProxyDefinition(
        attr="materialization",
        path=("infrastructure", "materialization"),
        return_type=MaterializationConfig,
    ),
    ProxyDefinition(
        attr="logging",
        path=("infrastructure", "logging"),
        return_type=LoggingConfig,
    ),
    ProxyDefinition(
        attr="telemetry",
        path=("infrastructure", "telemetry"),
        return_type=TelemetryConfig,
    ),
    ProxyDefinition(
        attr="cli",
        path=("infrastructure", "cli"),
        return_type=CLIConfig,
    ),
    ProxyDefinition(
        attr="fallbacks",
        path=("domain", "fallbacks"),
        return_type=FallbacksConfig,
    ),
    ProxyDefinition(
        attr="validation",
        path=("domain", "validation"),
        return_type=ValidationConfig,
    ),
    ProxyDefinition(
        attr="transform",
        path=("domain", "transform"),
        return_type=TransformConfig,
    ),
    ProxyDefinition(
        attr="postprocess",
        path=("domain", "postprocess"),
        return_type=PostprocessConfig,
    ),
    ProxyDefinition(
        attr="sources",
        path=("domain", "sources"),
        return_type=dict[str, SourceConfig],
        value_type=Mapping[str, SourceConfig],
        setter=dict,
    ),
    ProxyDefinition(
        attr="chembl",
        path=("domain", "chembl"),
        return_type=Mapping[str, Any] | None,
    ),
)

for _attr, _descriptor in build_section_proxies(_PIPELINE_CONFIG_PROXY_FIELDS).items():
    setattr(PipelineConfig, _attr, _descriptor)


class PipelineCommonCompat:
    """Read-only facade mapping legacy `common` fields onto ``CLIConfig``."""

    __slots__ = ("_cli",)

    def __init__(self, cli: CLIConfig) -> None:
        self._cli = cli

    def __getattr__(self, item: str) -> Any:
        return getattr(self._cli, item)

    if TYPE_CHECKING:
        input_file: str | None
        limit: int | None
        sample: int | None
        extended: bool
        dry_run: bool
        fail_on_schema_drift: bool
        validate_columns: bool
        golden: str | None
        verbose: bool


_PIPELINE_COMMON_PROXY_FIELDS: tuple[ProxyDefinition, ...] = (
    ProxyDefinition(
        attr="input_file",
        path=("_cli", "input_file"),
        return_type=str | None,
        read_only=True,
    ),
    ProxyDefinition(
        attr="limit",
        path=("_cli", "limit"),
        return_type=int | None,
        read_only=True,
    ),
    ProxyDefinition(
        attr="sample",
        path=("_cli", "sample"),
        return_type=int | None,
        read_only=True,
    ),
    ProxyDefinition(
        attr="extended",
        path=("_cli", "extended"),
        return_type=bool,
        read_only=True,
    ),
    ProxyDefinition(
        attr="dry_run",
        path=("_cli", "dry_run"),
        return_type=bool,
        read_only=True,
    ),
    ProxyDefinition(
        attr="fail_on_schema_drift",
        path=("_cli", "fail_on_schema_drift"),
        return_type=bool,
        read_only=True,
    ),
    ProxyDefinition(
        attr="validate_columns",
        path=("_cli", "validate_columns"),
        return_type=bool,
        read_only=True,
    ),
    ProxyDefinition(
        attr="golden",
        path=("_cli", "golden"),
        return_type=str | None,
        read_only=True,
    ),
    ProxyDefinition(
        attr="verbose",
        path=("_cli", "verbose"),
        return_type=bool,
        read_only=True,
    ),
)

for _attr, _descriptor in build_section_proxies(_PIPELINE_COMMON_PROXY_FIELDS).items():
    setattr(PipelineCommonCompat, _attr, _descriptor)
