"""Reusable mixins shared by the unified pipeline base.

Each mixin documents the expectations it has from the concrete pipeline
class.  This keeps responsibilities discoverable and prevents each pipeline
from re‑implementing boilerplate lifecycle hooks.
"""

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
    """Provide structured stage logging helpers.

    Contract
    --------
    Concrete pipelines are expected to expose ``_make_pipeline_logger`` and
    ``_stage_durations_ms`` attributes, both of which are initialised by
    :class:`bioetl.pipelines.base.PipelineBase`.  The mixin exposes
    :meth:`stage_logger` and :meth:`logger_for` so that downstream mixins can
    rely on a consistent logging surface.
    """

    @contextmanager
    def stage_logger(
        self,
        stage: str,
        *,
        component: str | None = None,
        rows: int | None = None,
        **extra: Any,
    ) -> Iterator[BoundLogger]:
        """Yield a logger bound to the given stage and capture timings."""
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
        """Return a logger bound to the pipeline and stage context."""
        return self._make_pipeline_logger(stage=stage, component=component, **extra)


class ReleaseHandshakeMixin:
    """Provide handshake helpers against the ChEMBL status endpoint.

    Contract
    --------
    ``logger_for`` and ``record_extract_metadata`` are required and are
    implemented by :class:`bioetl.pipelines.base.PipelineBase`.  Pipelines may
    call :meth:`perform_handshake` with the desired endpoint; caching and
    telemetry are handled by the mixin so the pipelines no longer need to wire
    logging or metadata updates manually.
    """

    _handshake_cache: dict[str, tuple[float, Mapping[str, Any]]]

    def perform_handshake(
        self,
        chembl_client: Any,
        endpoint: str,
        *,
        enabled: bool = True,
        ttl_seconds: int = 3600,
    ) -> Mapping[str, Any]:
        """Fetch and cache the ChEMBL status payload for the endpoint."""
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
    """Utility helpers for paginated extractions.

    Contract
    --------
    The mixin relies on :meth:`stage_logger` for telemetry and therefore should
    be composed with :class:`LoggingMixin`.  Pipelines may optionally implement
    ``on_page`` to observe pagination metadata (for example, to capture rate
    limiting hints or diagnostics per page).
    """

    def iterate_pages(
        self,
        client: UnifiedAPIClient,
        endpoint: str,
        params: Mapping[str, Any],
        page_size: int,
        *,
        items_key: str = "results",
    ) -> Iterator[tuple[int, list[Mapping[str, Any]]]]:
        """Yield result batches produced by the paginated endpoint."""
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
        """Delegate to :class:`ChemblPipelineBase` batching helpers."""
        return super().run_batched_extraction(*args, **kwargs)


class SchemaValidationMixin:
    """Wrapper around the base validation routine with logging hooks.

    Contract
    --------
    ``config.validation`` and ``_resolve_schema_entry`` must be present on the
    pipeline (both are provided by :class:`bioetl.pipelines.base.PipelineBase`).
    The mixin defers the heavy lifting to :meth:`PipelineBase.validate` but adds
    logging via :meth:`stage_logger`.
    """

    def load_validation_schema(self) -> Any:
        """Return the configured output schema entry when available."""
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
        """Wrap ``PipelineBase.validate`` with stage logging."""
        with self.stage_logger("validate", rows=len(df)):
            return super().validate(df)


class TransformMixin:
    """Provide a default transform lifecycle that normalises payloads.

    Contract
    --------
    Pipelines are expected to expose ``_output_column_order`` and
    ``_normalize_and_enforce_schema`` (supplied by
    :class:`bioetl.chembl.common.descriptor.ChemblPipelineBase`).  Hooks such as
    ``pre_transform``, ``domain_enrich`` and ``post_transform`` can be
    overridden to customise the lifecycle, while the mixin guarantees that the
    schema normalisation and logging are executed consistently.
    """

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare a working copy before schema normalisation."""
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finalise the dataset after enrichment."""
        return df

    def domain_enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich the dataset with domain-specific metadata."""
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute the default transform lifecycle with logging."""
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
    """Bridge deterministic IO helpers with the pipeline run template.

    Contract
    --------
    Requires ``write`` from :class:`bioetl.pipelines.base.PipelineBase`.  The
    mixin ensures that pipelines exposing ``save_results`` have a uniform
    implementation that honours deterministic CSV emission and QC options.
    """

    def save_results(
        self,
        df: pd.DataFrame,
        output_dir: Any,
        *,
        extended: bool = False,
        include_correlation: bool = False,
        include_qc_metrics: bool = False,
    ) -> WriteResult:
        """Persist the dataframe using :meth:`PipelineBase.write`."""
        run_result: RunResult = super().write(
            df,
            output_dir,
            extended=extended,
            include_correlation=include_correlation,
            include_qc_metrics=include_qc_metrics,
        )
        return run_result.write_result

