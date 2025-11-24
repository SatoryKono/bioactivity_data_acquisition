"""Composable validation chain for :class:`application.pipelines.specs.base.PipelineBase`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import pandas as pd
import pandera.errors
from pandera import DataFrameSchema
from structlog.stdlib import BoundLogger

from infrastructure.io import ensure_hash_columns
from infrastructure.logging import LogEvents, UnifiedLogger
from infrastructure.schema import format_failure_cases, summarize_schema_errors
from infrastructure.schemas import SchemaMigration, SchemaRegistryEntry
from domain.vocab.exceptions import (
    VocabularyValidationError,
    VocabularyViolation,
)


class ValidationPipelineProtocol(Protocol):
    config: Any
    _validation_schema: SchemaRegistryEntry | None
    _validation_schema_version: str | None
    _validation_summary: dict[str, Any] | None

    def _resolve_schema_entry(
        self,
        schema_identifier: str,
        *,
        expected_version: str | None,
        dataset_role: str,
        allow_migration: bool,
    ) -> tuple[SchemaRegistryEntry, Sequence[SchemaMigration], str | None]: ...

    def _apply_schema_migrations(
        self,
        df: pd.DataFrame,
        *,
        schema_identifier: str,
        dataset_role: str,
        migrations: Sequence[SchemaMigration],
    ) -> pd.DataFrame: ...

    def _clone_schema_with_options(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...

    def _ensure_schema_columns(
        self,
        df: pd.DataFrame,
        *args: Any,
        **kwargs: Any,
    ) -> pd.DataFrame: ...

    def _reorder_columns(
        self,
        df: pd.DataFrame,
        column_order: Sequence[str],
    ) -> pd.DataFrame: ...

    def _ensure_load_meta_ids(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame: ...

    def _check_vocabulary_bindings(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Sequence[VocabularyViolation]: ...


@dataclass
class ValidationContext:
    """Mutable context propagated through validation steps."""

    pipeline: ValidationPipelineProtocol
    df: pd.DataFrame
    dataset_role: str = "primary"
    schema_entry: SchemaRegistryEntry | None = None
    schema_identifier: str | None = None
    expected_version: str | None = None
    allow_migration: bool = False
    migrations: Sequence[SchemaMigration] = field(default_factory=tuple)
    migrations_applied: int = 0
    migrated_from_version: str | None = None
    schema: DataFrameSchema | None = None
    schema_error: pandera.errors.SchemaErrors | None = None
    df_for_validation: pd.DataFrame | None = None
    fallback_schema: DataFrameSchema | None = None
    schema_valid: bool = True
    failure_count: int | None = None
    error_summary: str | None = None
    vocabulary_failures_details: list[dict[str, object]] = field(
        default_factory=list
    )
    skip_remaining: bool = False
    log: BoundLogger | None = None

    def get_logger(self) -> BoundLogger:
        """Return a structlog logger bound to the pipeline context."""

        if self.log is None:
            self.log = UnifiedLogger.get(__name__)
        return self.log


@dataclass
class ValidationResult:
    """Outcome of a validation step."""

    df: pd.DataFrame
    continue_steps: bool = True


class ValidationBehavior(Protocol):
    """Strategy describing how validation steps handle failures."""

    fail_open: bool

    def handle_schema_errors(
        self,
        context: ValidationContext,
        error: pandera.errors.SchemaErrors,
    ) -> None: ...

    def handle_vocabulary_violations(
        self,
        context: ValidationContext,
        violations: Sequence[VocabularyViolation],
    ) -> None: ...


class StrictValidation:
    """Fail-fast behavior mirroring the legacy strict validation flow."""

    fail_open = False

    def handle_schema_errors(
        self,
        context: ValidationContext,
        error: pandera.errors.SchemaErrors,
    ) -> None:
        raise error

    def handle_vocabulary_violations(
        self,
        context: ValidationContext,
        violations: Sequence[VocabularyViolation],
    ) -> None:
        raise VocabularyValidationError(violations=violations)


class FailOpenValidation:
    """Best-effort strategy that records failures while keeping execution going."""

    fail_open = True

    def handle_schema_errors(
        self,
        context: ValidationContext,
        error: pandera.errors.SchemaErrors,
    ) -> None:
        log = context.get_logger()
        schema_entry = context.schema_entry
        summary = summarize_schema_errors(error)
        failure_cases_df = getattr(error, "failure_cases", None)
        failure_details: dict[str, Any] | None = None
        if (
            isinstance(failure_cases_df, pd.DataFrame)
            and not failure_cases_df.empty
        ):
            failure_details = format_failure_cases(failure_cases_df)
            summary["failure_count"] = int(len(failure_cases_df))

        payload: dict[str, Any] = {**summary}
        if schema_entry is not None:
            payload.update(
                schema=schema_entry.identifier, version=schema_entry.version
            )
        if failure_details:
            payload["failure_details"] = failure_details

        log.warning(LogEvents.SCHEMA_VALIDATION_FAILED, **payload)
        context.schema_valid = False
        context.error_summary = summary.get("message")
        context.failure_count = summary.get("failure_count")

    def handle_vocabulary_violations(
        self,
        context: ValidationContext,
        violations: Sequence[VocabularyViolation],
    ) -> None:
        if not violations:
            return
        log = context.get_logger()
        schema_entry = context.schema_entry
        context.schema_valid = False
        total_invalid = sum(
            violation.invalid_count for violation in violations
        )
        context.failure_count = (context.failure_count or 0) + total_invalid
        context.vocabulary_failures_details = [
            {
                "column": violation.column,
                "vocabulary_id": violation.vocabulary_id,
                "invalid_values": violation.invalid_values,
                "invalid_count": violation.invalid_count,
            }
            for violation in violations
        ]

        payload: dict[str, Any] = {
            "reason": "vocabulary_bindings",
            "failure_count": total_invalid,
            "violations": context.vocabulary_failures_details,
        }
        if schema_entry is not None:
            payload.update(
                schema=schema_entry.identifier, version=schema_entry.version
            )

        log.warning(LogEvents.SCHEMA_VALIDATION_FAILED, **payload)
        if context.error_summary is None:
            context.error_summary = "vocabulary binding violations"


class ValidationStep(Protocol):
    """Protocol implemented by validation chain steps."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult: ...


class SchemaResolutionStep:
    """Resolve validation schema and migrations based on pipeline configuration."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        schema_identifier = getattr(
            context.pipeline.config.validation, "schema_out", None
        )
        context.schema_identifier = schema_identifier
        log = context.get_logger()

        if not schema_identifier:
            log.debug(
                LogEvents.VALIDATION_SKIPPED, reason="no_schema_configured"
            )
            context.skip_remaining = True
            return ValidationResult(df=context.df, continue_steps=False)

        expected_version = getattr(
            context.pipeline.config.validation,
            "schema_out_version",
            None,
        )
        allow_migration = bool(
            getattr(
                context.pipeline.config.validation,
                "allow_schema_migration",
                False,
            )
        )
        schema_entry, migrations, source_version = (
            context.pipeline._resolve_schema_entry(
                schema_identifier,
                expected_version=expected_version,
                dataset_role=context.dataset_role,
                allow_migration=allow_migration,
            )
        )
        context.schema_entry = schema_entry
        context.expected_version = expected_version
        context.allow_migration = allow_migration
        context.migrations = migrations
        context.migrations_applied = len(migrations)
        context.migrated_from_version = source_version if migrations else None
        return ValidationResult(df=context.df)


class MigrationStep:
    """Apply schema migrations when required."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        if not context.schema_entry or not context.migrations:
            return ValidationResult(df=context.df)

        df = context.pipeline._apply_schema_migrations(
            context.df,
            schema_identifier=context.schema_entry.identifier,
            dataset_role=context.dataset_role,
            migrations=context.migrations,
        )
        context.df = df
        return ValidationResult(df=df)


class SchemaValidationStep:
    """Execute the configured Pandera schema against the dataframe."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        if context.schema_entry is None:
            return ValidationResult(df=context.df, continue_steps=False)

        config = context.pipeline.config.validation
        # ``schema_entry.schema`` may be backed by different concrete Pandera
        # implementations depending on import style. Avoid over-constraining the
        # type here and rely on structural compatibility instead.
        base_schema = context.schema_entry.schema
        if hasattr(base_schema, "replace") and callable(
            getattr(base_schema, "replace", None)
        ):
            schema = base_schema.replace(  # type: ignore[operator]
                strict=config.strict,
                coerce=config.coerce,
            )
        else:
            schema = context.pipeline._clone_schema_with_options(
                base_schema,
                strict=config.strict,
                coerce=config.coerce,
            )
        # Preserve schema name so tests patching DataFrameSchema.get_backend based on
        # schema.name (e.g. "SimpleSchema") continue to work after cloning/replacing.
        if getattr(base_schema, "name", None) is not None:
            schema.name = base_schema.name
        context.schema = schema

        df_for_validation = ensure_hash_columns(
            context.df, config=context.pipeline.config
        )
        df_for_validation = context.pipeline._ensure_schema_columns(
            df_for_validation,
            context.schema_entry.column_order,
            context.get_logger(),
        )
        df_for_validation = context.pipeline._reorder_columns(
            df_for_validation,
            context.schema_entry.column_order,
        )
        df_for_validation = context.pipeline._ensure_load_meta_ids(
            df_for_validation
        )
        context.df_for_validation = df_for_validation

        log = context.get_logger()
        log.debug(
            LogEvents.VALIDATION_SCHEMA_LOADED,
            schema=context.schema_entry.identifier,
            version=context.schema_entry.version,
        )

        try:
            backend = schema.get_backend(df_for_validation)
            validated_candidate: Any = backend.validate(
                df_for_validation,
                schema,
                lazy=True,
            )
        except pandera.errors.SchemaErrors as exc:
            context.df = df_for_validation
            context.schema_error = exc
        else:
            validated = context.pipeline._reorder_columns(
                validated_candidate,
                context.schema_entry.column_order,
            )
            context.df = validated
            context.schema_error = None

        return ValidationResult(df=context.df)


class CoerceRetryStep:
    """Retry schema validation without coercion when possible."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        if context.schema_entry is None:
            return ValidationResult(df=context.df)
        if context.schema_error is None:
            return ValidationResult(df=context.df)

        log = context.get_logger()
        fallback_validated: pd.DataFrame | None = None
        affected_columns: list[str] = []
        fallback_schema: DataFrameSchema | None = None
        coerce_only = False
        # Decide whether to attempt a retry based on the actual schema used in the
        # first validation pass. This is more robust than relying solely on the
        # configuration flag, which may be normalised or overridden elsewhere.
        if context.schema is not None:
            schema_for_flags = context.schema
        elif context.schema_entry is not None:
            schema_for_flags = context.schema_entry.schema
        else:
            schema_for_flags = None

        if bool(getattr(schema_for_flags, "coerce", False)):
            coerce_only, affected_columns = self._coerce_failures_only(
                context.schema_error
            )

            # Build fallback schema from the already configured schema instance
            # (context.schema) to preserve attributes such as ``name``.
            # This is important for tests that patch DataFrameSchema.get_backend
            # based on ``schema.name`` (e.g. "SimpleSchema").
            base_schema = (
                context.schema
                if context.schema is not None
                else context.schema_entry.schema
            )
            if hasattr(base_schema, "replace") and callable(
                getattr(base_schema, "replace", None)
            ):
                fallback_schema = base_schema.replace(  # type: ignore[operator]
                    strict=context.pipeline.config.validation.strict,
                    coerce=False,
                )
            else:
                fallback_schema = context.pipeline._clone_schema_with_options(
                    base_schema,
                    strict=context.pipeline.config.validation.strict,
                    coerce=False,
                )
            if (
                fallback_schema is not None
                and getattr(base_schema, "name", None) is not None
            ):
                fallback_schema.name = base_schema.name
            df_candidate = (
                context.df_for_validation
                if context.df_for_validation is not None
                else context.df
            )
            try:
                backend = cast(DataFrameSchema, fallback_schema).get_backend(
                    df_candidate
                )
                retried_candidate: Any = backend.validate(
                    df_candidate,
                    fallback_schema,
                    lazy=True,
                )
            except pandera.errors.SchemaErrors:
                fallback_validated = None
            else:
                fallback_validated = context.pipeline._reorder_columns(
                    retried_candidate,
                    context.schema_entry.column_order,
                )
                context.schema = fallback_schema
                log.debug(
                    LogEvents.VALIDATION_RETRY_WITHOUT_COERCE,
                    columns=affected_columns,
                    rows=len(df_candidate),
                )

        if fallback_validated is not None:
            context.df = fallback_validated
            context.schema_error = None
            return ValidationResult(df=context.df)

        # Delegate to the configured ValidationBehavior: strict mode raises and
        # fail-open records failures in the summary.
        behavior.handle_schema_errors(context, context.schema_error)
        context.schema_error = None
        return ValidationResult(df=context.df)

    @staticmethod
    def _coerce_failures_only(
        error: pandera.errors.SchemaErrors,
    ) -> tuple[bool, list[str]]:
        failure_cases_df = getattr(error, "failure_cases", None)
        if (
            not isinstance(failure_cases_df, pd.DataFrame)
            or failure_cases_df.empty
        ):
            return False, []

        checks_series = failure_cases_df.get("check")
        if checks_series is None:
            return False, []

        checks_str = checks_series.astype(str)
        # Be tolerant of how Pandera renders checks in failure_cases; in practice
        # the string representation may wrap the underlying check object, so we
        # look for the coercion marker as a substring instead of a strict prefix.
        coerce_mask = checks_str.str.contains("coerce_dtype", na=False)
        # Treat errors as "coerce-only" whenever there is at least one
        # coercion-related failure (coerce_dtype). In practice Pandera often
        # reports a mix of DATATYPE_COERCION and other checks for the same
        # column; for the purposes of the retry step we consider any
        # presence of coercion failures sufficient to attempt a fallback
        # run with ``coerce=False``.
        if not bool(coerce_mask.any()):
            return False, []

        columns_series = failure_cases_df.get("column")
        if columns_series is None:
            return True, []

        columns_all = columns_series.dropna().astype(str)
        columns_with_coerce = columns_all[coerce_mask].unique().tolist()

        return True, columns_with_coerce


class VocabularyValidationStep:
    """Validate schema vocabulary bindings using the configured behavior."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        if context.schema_entry is None:
            return ValidationResult(df=context.df)

        violations = context.pipeline._check_vocabulary_bindings(
            context.df, context.schema_entry
        )
        if not violations:
            return ValidationResult(df=context.df)

        behavior.handle_vocabulary_violations(context, violations)
        return ValidationResult(df=context.df)


class SummaryStep:
    """Persist validation summary metadata on the pipeline instance."""

    def run(
        self, context: ValidationContext, behavior: ValidationBehavior
    ) -> ValidationResult:
        if context.schema_entry is None:
            return ValidationResult(df=context.df, continue_steps=False)

        summary: dict[str, Any] = {
            "schema_identifier": context.schema_entry.identifier,
            "schema_name": context.schema_entry.name,
            "schema_version": context.schema_entry.version,
            "column_order": list(context.schema_entry.column_order),
            "strict": bool(context.pipeline.config.validation.strict),
            "coerce": bool(context.pipeline.config.validation.coerce),
            "row_count": int(len(context.df)),
            "schema_valid": context.schema_valid,
            "expected_version": context.expected_version,
            "migrated_from_version": context.migrated_from_version,
            "migrations_applied": context.migrations_applied,
        }

        if context.error_summary is not None:
            summary["error"] = context.error_summary
        if context.failure_count is not None:
            summary["failure_count"] = context.failure_count
        if context.vocabulary_failures_details:
            summary["vocabulary_failures"] = (
                context.vocabulary_failures_details
            )

        context.pipeline._validation_schema = context.schema_entry
        context.pipeline._validation_schema_version = (
            context.schema_entry.version
        )
        context.pipeline._validation_summary = summary

        if context.schema_valid:
            context.get_logger().debug(
                LogEvents.VALIDATION_COMPLETED,
                schema=context.schema_entry.identifier,
                version=context.schema_entry.version,
                rows=len(context.df),
            )

        return ValidationResult(df=context.df, continue_steps=False)


__all__ = [
    "ValidationBehavior",
    "ValidationContext",
    "ValidationResult",
    "ValidationStep",
    "StrictValidation",
    "FailOpenValidation",
    "SchemaResolutionStep",
    "MigrationStep",
    "SchemaValidationStep",
    "CoerceRetryStep",
    "VocabularyValidationStep",
    "SummaryStep",
]
