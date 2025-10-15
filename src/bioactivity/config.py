"""Configuration management for the bioactivity ETL pipeline."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError, computed_field, field_validator, model_validator

ENV_PREFIX_FALLBACK = "BIOACTIVITY"
ENV_SEPARATOR = "__"


def _ensure_directory(path: Path) -> Path:
    path.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    return path


def _deep_update(target: dict[str, Any], updates: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in updates.items():
        if (
            isinstance(value, Mapping)
            and key in target
            and isinstance(target[key], Mapping)
        ):
            target[key] = _deep_update(dict(target[key]), value)
        else:
            target[key] = value
    return target


def _build_nested(path: Iterable[str], value: Any) -> dict[str, Any]:
    keys = list(path)
    if not keys:
        msg = "Override keys must not be empty"
        raise ValueError(msg)
    nested: dict[str, Any] = {}
    cursor = nested
    for key in keys[:-1]:
        cursor[key] = {}
        cursor = cursor[key]  # type: ignore[assignment]
    cursor[keys[-1]] = value
    return nested


def _coerce_value(raw: str) -> Any:
    try:
        loaded = yaml.safe_load(raw)
    except yaml.YAMLError:
        return raw
    return loaded


class RetrySettings(BaseModel):
    """Retry configuration for HTTP clients."""

    max_tries: int = Field(default=5, ge=1)
    backoff_multiplier: float = Field(default=2.0, gt=0)
    jitter: float = Field(default=0.5, ge=0)


class RateLimitSettings(BaseModel):
    """Rate limiting configuration for HTTP clients."""

    per_seconds: int = Field(default=5, ge=1)
    burst: int = Field(default=5, ge=1)


class PaginationSettings(BaseModel):
    """Pagination metadata for API sources."""

    param: str | None = Field(default="page")
    size_param: str | None = Field(default="page_size")
    size: int = Field(default=200, gt=0)
    max_pages: int | None = Field(default=None, ge=1)


class AuthSettings(BaseModel):
    """Authentication configuration referencing secrets."""

    secret_name: str | None = None
    header_name: str = Field(default="Authorization")
    scheme: str | None = Field(default="Bearer")

    def apply(self, headers: Mapping[str, str], secrets: SecretsSettings) -> dict[str, str]:  # type: ignore[name-defined]
        if self.secret_name is None:
            return dict(headers)
        secret_value = secrets.get(self.secret_name)
        if secret_value is None:
            return dict(headers)
        applied = dict(headers)
        token = secret_value if self.scheme is None else f"{self.scheme} {secret_value}".strip()
        applied[self.header_name] = token
        return applied


class OutputPaths(BaseModel):
    """Paths for ETL outputs."""

    data_path: Path = Field(default=Path("data/output/bioactivities.csv"))
    qc_report_path: Path = Field(default=Path("data/output/qc_report.csv"))
    correlation_path: Path = Field(default=Path("data/output/correlation.csv"))

    @field_validator("data_path", "qc_report_path", "correlation_path")
    @classmethod
    def ensure_parent(cls, value: Path) -> Path:
        return _ensure_directory(value)


class IOSettings(BaseModel):
    """Input/output configuration for the pipeline."""

    input_dir: Path = Field(default=Path("data/input"))
    output_dir: Path = Field(default=Path("data/output"))
    temp_dir: Path = Field(default=Path("data/tmp"))
    output: OutputPaths = Field(default_factory=OutputPaths)

    @field_validator("input_dir", "output_dir", "temp_dir")
    @classmethod
    def ensure_directory(cls, value: Path) -> Path:
        value.expanduser().resolve().mkdir(parents=True, exist_ok=True)
        return value


class ProjectSettings(BaseModel):
    """Metadata about the project."""

    name: str = Field(default="Bioactivity Data Acquisition")
    version: str = Field(default="0.1.0")
    description: str = Field(
        default=(
            "Canonical configuration for the ChEMBL bioactivity ETL pipeline."
        )
    )


class RuntimeSettings(BaseModel):
    """Runtime configuration toggles."""

    log_level: str = Field(default="INFO")
    max_workers: int = Field(default=4, ge=1)
    progress: bool = Field(default=True)


class DeterminismSettings(BaseModel):
    """Deterministic execution toggles."""

    random_seed: int = Field(default=42)
    sort_rows: bool = Field(default=True)
    sort_columns: bool = Field(default=True)
    row_sort_keys: list[str] = Field(default_factory=lambda: ["source", "compound_id"])


class ValidationThresholds(BaseModel):
    """Quality thresholds for QC reports."""

    max_missing_percent: float = Field(default=0.05, ge=0.0, le=1.0)
    max_duplicate_rows: int = Field(default=0, ge=0)


class ValidationSettings(BaseModel):
    """Data validation configuration."""

    strict: bool = Field(default=True)
    enforce_output_schema: bool = Field(default=True)
    thresholds: ValidationThresholds = Field(default_factory=ValidationThresholds)


class HTTPSettings(BaseModel):
    """Global HTTP client settings."""

    timeout: float = Field(default=30.0, gt=0)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    user_agent: str = Field(default="bioactivity-data-acquisition/0.1.0")


class PostprocessSettings(BaseModel):
    """Controls post-processing outputs."""

    qc: dict[str, bool] = Field(default_factory=lambda: {"enabled": True, "correlation": True})
    reporting: dict[str, bool] = Field(default_factory=lambda: {"include_timestamp": True})


class CLISettings(BaseModel):
    """Command-line interface related configuration."""

    env_prefix: str = Field(default=ENV_PREFIX_FALLBACK)
    defaults_file: Path = Field(default=Path("configs/config.yaml"))
    allow_env_override: bool = Field(default=True)


class SecretDefinition(BaseModel):
    """Description of a secret value that must come from the environment."""

    name: str
    env: str
    description: str | None = None


class SecretsSettings(BaseModel):
    """Secret configuration referencing environment variables."""

    model_config = ConfigDict(extra="forbid")

    required: list[SecretDefinition] = Field(default_factory=list)
    optional: list[SecretDefinition] = Field(default_factory=list)
    resolved: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def ensure_unique_names(self) -> SecretsSettings:
        names = [secret.name for secret in [*self.required, *self.optional]]
        if len(names) != len(set(names)):
            msg = "Secret names must be unique"
            raise ValueError(msg)
        return self

    def load(self, environ: Mapping[str, str]) -> SecretsSettings:
        resolved: dict[str, str] = {}
        missing: list[str] = []
        for secret in self.required:
            value = environ.get(secret.env)
            if value is None:
                missing.append(secret.env)
            else:
                resolved[secret.name] = value
        for secret in self.optional:
            value = environ.get(secret.env)
            if value is not None:
                resolved[secret.name] = value
        if missing:
            joined = ", ".join(sorted(missing))
            msg = f"Missing required secrets: {joined}"
            raise ValueError(msg)
        return self.model_copy(update={"resolved": resolved})

    def get(self, name: str) -> str | None:
        return self.resolved.get(name)


class APIClientConfig(BaseModel):
    """Configuration for a single HTTP API client."""

    model_config = ConfigDict(extra="forbid")

    name: str
    url: HttpUrl
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    pagination_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = Field(default=None, gt=0)
    max_pages: int | None = Field(default=None, gt=0)
    timeout: float = Field(default=30.0, gt=0)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            msg = "Client name must not be empty"
            raise ValueError(msg)
        return value


class SourceSettings(BaseModel):
    """Configuration for a single source."""

    model_config = ConfigDict(extra="forbid")

    name: str
    url: HttpUrl
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    pagination: PaginationSettings = Field(default_factory=PaginationSettings)
    timeout: float | None = Field(default=None, gt=0)
    auth: AuthSettings = Field(default_factory=AuthSettings)

    @field_validator("name")
    @classmethod
    def ensure_name(cls, value: str) -> str:
        if not value.strip():
            msg = "Source name must not be empty"
            raise ValueError(msg)
        return value

    def to_api_client_config(
        self,
        http_settings: HTTPSettings,
        secrets: SecretsSettings,
    ) -> APIClientConfig:
        pagination_param = self.pagination.param
        page_size_param = self.pagination.size_param
        headers = self.auth.apply(self.headers, secrets)
        return APIClientConfig(
            name=self.name,
            url=self.url,
            headers=headers,
            params=self.params,
            pagination_param=pagination_param,
            page_size_param=page_size_param,
            page_size=self.pagination.size,
            max_pages=self.pagination.max_pages,
            timeout=self.timeout or http_settings.timeout,
        )


class Config(BaseModel):
    """Top-level configuration for the ETL pipeline."""

    model_config = ConfigDict(extra="forbid")

    project: ProjectSettings = Field(default_factory=ProjectSettings)
    io: IOSettings = Field(default_factory=IOSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    determinism: DeterminismSettings = Field(default_factory=DeterminismSettings)
    validation: ValidationSettings = Field(default_factory=ValidationSettings)
    http: HTTPSettings = Field(default_factory=HTTPSettings)
    sources: dict[str, SourceSettings] = Field(default_factory=dict)
    postprocess: PostprocessSettings = Field(default_factory=PostprocessSettings)
    cli: CLISettings = Field(default_factory=CLISettings)
    secrets: SecretsSettings = Field(default_factory=SecretsSettings)

    @computed_field
    @property
    def clients(self) -> list[APIClientConfig]:
        return [source.to_api_client_config(self.http, self.secrets) for source in self.sources.values()]

    @classmethod
    def load(
        cls,
        path: Path | str,
        *,
        environ: Mapping[str, str] | None = None,
        cli_overrides: Mapping[str, Any] | None = None,
    ) -> Config:
        environ = environ or os.environ
        config_path = Path(path)
        if not config_path.exists():
            msg = f"Config file not found: {config_path}"
            raise FileNotFoundError(msg)

        defaults = cls().model_dump()
        defaults.pop("clients", None)
        merged = deepcopy(defaults)

        yaml_data = cls._load_yaml(config_path)
        merged = _deep_update(merged, yaml_data)

        prefix = merged.get("cli", {}).get("env_prefix", ENV_PREFIX_FALLBACK)
        env_overrides = cls._extract_env_overrides(environ, prefix)
        merged = _deep_update(merged, env_overrides)
        updated_prefix = merged.get("cli", {}).get("env_prefix", prefix)
        if updated_prefix != prefix:
            merged = _deep_update(merged, cls._extract_env_overrides(environ, updated_prefix))

        if cli_overrides:
            cli_data = cls._normalise_cli_overrides(cli_overrides)
            merged = _deep_update(merged, cli_data)

        try:
            config = cls.model_validate(merged)
        except ValidationError as exc:
            msg = f"Invalid configuration: {exc}"
            raise ValueError(msg) from exc

        secrets = config.secrets.load(environ)
        return config.model_copy(update={"secrets": secrets})

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
        if not isinstance(data, Mapping):
            msg = "Configuration file must contain a mapping"
            raise ValueError(msg)
        return dict(data)

    @classmethod
    def _extract_env_overrides(
        cls, environ: Mapping[str, str], prefix: str
    ) -> dict[str, Any]:
        overrides: dict[str, Any] = {}
        if not prefix:
            return overrides
        prefix_with_sep = f"{prefix}{ENV_SEPARATOR}"
        for key, value in environ.items():
            if not key.startswith(prefix_with_sep):
                continue
            path = key[len(prefix_with_sep) :].split(ENV_SEPARATOR)
            normalised_path = [part.lower() for part in path if part]
            if not normalised_path:
                continue
            overrides = _deep_update(
                overrides,
                _build_nested(normalised_path, _coerce_value(value)),
            )
        return overrides

    @staticmethod
    def _normalise_cli_overrides(overrides: Mapping[str, Any]) -> dict[str, Any]:
        normalised: dict[str, Any] = {}
        for key, value in overrides.items():
            if isinstance(key, str):
                path = [part.strip() for part in key.split(".") if part.strip()]
            else:
                path = [str(key)]
            coerced = value
            if isinstance(value, str):
                coerced = _coerce_value(value)
            normalised = _deep_update(normalised, _build_nested(path, coerced))
        return normalised

    @staticmethod
    def parse_cli_overrides(pairs: Iterable[str]) -> dict[str, Any]:
        overrides: dict[str, Any] = {}
        for pair in pairs:
            if "=" not in pair:
                msg = "Overrides must use KEY=VALUE syntax"
                raise ValueError(msg)
            key, raw_value = pair.split("=", 1)
            path = [part.strip() for part in key.split(".") if part.strip()]
            if not path:
                msg = "Override keys must not be empty"
                raise ValueError(msg)
            overrides = _deep_update(
                overrides,
                _build_nested(path, _coerce_value(raw_value)),
            )
        return overrides


__all__ = [
    "APIClientConfig",
    "AuthSettings",
    "CLISettings",
    "Config",
    "DeterminismSettings",
    "HTTPSettings",
    "IOSettings",
    "OutputPaths",
    "PaginationSettings",
    "PostprocessSettings",
    "ProjectSettings",
    "RateLimitSettings",
    "RetrySettings",
    "RuntimeSettings",
    "SecretsSettings",
    "SourceSettings",
    "ValidationSettings",
]
