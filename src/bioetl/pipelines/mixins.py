"""Reusable mixins shared by the unified pipeline base."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.core.http import UnifiedAPIClient
from bioetl.core.io import WriteResult
from bioetl.core.pipeline import RunResult


class LoggingMixin:
    """Shared helpers that provide structured logging utilities."""

    @contextmanager
    def stage_logger(
        self,
        stage: str,
        *,
        component: str | None = None,
        rows: int | None = None,
        **extra: Any,
    ) -> Iterator[BoundLogger]:
        log = self.logger_for(stage=stage, component=component, **extra)
        start = time.perf_counter()
        log.info("stage_started", rows=rows)
        try:
            yield log
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0
            if getattr(self, "_stage_durations_ms", None) is not None:
                self._stage_durations_ms[stage] = duration_ms  # type: ignore[attr-defined]
            log.info("stage_completed", duration_ms=duration_ms, rows=rows)

    def logger_for(
        self,
        *,
        stage: str | None = None,
        component: str | None = None,
        **extra: Any,
    ) -> BoundLogger:
        return self._make_pipeline_logger(stage=stage, component=component, **extra)


class ReleaseHandshakeMixin:
    """Provide handshake helpers against the ChEMBL status endpoint."""

    _handshake_cache: dict[str, tuple[float, Mapping[str, Any]]]

    def perform_handshake(
        self,
        chembl_client: Any,
        endpoint: str,
        *,
        enabled: bool = True,
        ttl_seconds: int = 3600,
    ) -> Mapping[str, Any]:
        if not enabled:
            self.logger_for(stage="handshake").info(
                "handshake_skipped", endpoint=endpoint
            )
            return {}

        cache: dict[str, tuple[float, Mapping[str, Any]]] = getattr(
            self, "_handshake_cache", {}
        )
        now = time.monotonic()
        cached = cache.get(endpoint)
        if cached and cached[0] > now:
            return cached[1]

        log = self.logger_for(stage="handshake")
        started = time.perf_counter()
        log.info("handshake_started", endpoint=endpoint)

        status_payload: Mapping[str, Any]
        handshake_method = getattr(chembl_client, "handshake", None)
        if callable(handshake_method):
            status_payload = handshake_method(endpoint=endpoint)
        else:
            response = chembl_client.get(endpoint)
            status_payload = response.json()

        duration_ms = (time.perf_counter() - started) * 1000.0
        log.info("handshake_completed", duration_ms=duration_ms)

        self.record_extract_metadata(
            chembl_release=status_payload.get("chembl_release"),
            api_version=status_payload.get("api_version"),
        )

        cache[endpoint] = (now + float(ttl_seconds), status_payload)
        self._handshake_cache = cache
        return status_payload


class PaginatedExtractorMixin:
    """Utility helpers for paginated extractions."""

    def iterate_pages(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        params: Mapping[str, Any],
        page_size: int,
        *,
        items_key: str = "results",
    ) -> Iterator[tuple[int, list[Mapping[str, Any]]]]:
        next_endpoint = endpoint
        page_index = 0
        while next_endpoint:
            page_params = dict(params)
            page_params.setdefault("limit", page_size)
            with self.stage_logger("extract", component="pagination", page=page_index) as log:
                log.info("page_started", page=page_index)
                response = client.get(next_endpoint, params=page_params)
                payload = response.json()
                items = list(payload.get(items_key, []))
                log.info(
                    "page_completed",
                    page=page_index,
                    rows=len(items),
                    endpoint=next_endpoint,
                )
                yield page_index, items
                if hasattr(self, "on_page"):
                    try:
                        self.on_page(page_index, payload.get("page_meta", {}))
                    except AttributeError:
                        pass
            next_endpoint = payload.get("page_meta", {}).get("next")
            page_index += 1
            if not items:
                break

    def run_batched_extraction(self, *args: Any, **kwargs: Any) -> Any:
        return super().run_batched_extraction(*args, **kwargs)


class SchemaValidationMixin:
    """Wrapper around the base validation routine with logging hooks."""

    def load_validation_schema(self) -> Any:
        schema_identifier = self.config.validation.schema_out
        if not schema_identifier:
            return None
        expected_version = getattr(self.config.validation, "schema_out_version", None)
        allow_migration = bool(getattr(self.config.validation, "allow_schema_migration", False))
        schema_entry, _, _ = self._resolve_schema_entry(
            schema_identifier,
            expected_version=expected_version,
            dataset_role="primary",
            allow_migration=allow_migration,
        )
        return schema_entry

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        with self.stage_logger("validate", rows=len(df)):
            return super().validate(df)


class TransformMixin:
    """Provide a default transform lifecycle that normalises payloads."""

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        with self.stage_logger("transform", rows=len(df)) as log:
            working_df = self.pre_transform(df.copy())
            column_order = getattr(self, "_output_column_order", tuple(working_df.columns))
            working_df = self._normalize_and_enforce_schema(  # type: ignore[attr-defined]
                working_df,
                column_order,
                log,
                order_columns=True,
            )
            enriched = self.domain_enrich(working_df)
            return self.post_transform(enriched)


class IOArtifactsMixin:
    """Bridge helpers between deterministic IO helpers and the run template."""

    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool = False,
        include_qc_metrics: bool = False,
    ) -> WriteResult:
        run_result: RunResult = super().write(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
        )
        return run_result.write_result

