"""Common handlers for ChEMBL pipelines.

This module provides factory functions for creating reusable empty_frame
and dry_run_handler functions used across ChEMBL pipelines.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

import pandas as pd
from structlog.stdlib import BoundLogger

from bioetl.core.logging import LogEvents

from .descriptor import ChemblExtractionContext, ChemblPipelineBase

PipelineT = TypeVar("PipelineT", bound=ChemblPipelineBase, contravariant=True)


def make_empty_frame_factory(
    id_column: str,
) -> Callable[[PipelineT, ChemblExtractionContext], pd.DataFrame]:
    """Create an empty_frame factory function for a given ID column.

    Parameters
    ----------
    id_column
        Name of the ID column to include in the empty DataFrame.

    Returns
    -------
    Callable[[PipelineT, ChemblExtractionContext], pd.DataFrame]:
        Function that creates an empty DataFrame with the specified ID column.

    Examples
    --------
    >>> empty_frame = make_empty_frame_factory("target_chembl_id")
    >>> df = empty_frame(pipeline, context)
    >>> assert df.columns.tolist() == ["target_chembl_id"]
    """

    def empty_frame(
        _: PipelineT,
        __: ChemblExtractionContext,
    ) -> pd.DataFrame:
        return pd.DataFrame({id_column: pd.Series(dtype="string")})

    return empty_frame


def make_dry_run_handler(
    log_event: LogEvents,
    get_metadata: Callable[[PipelineT], Mapping[str, Any]],
) -> Callable[
    [PipelineT, ChemblExtractionContext, BoundLogger, float], pd.DataFrame
]:
    """Create a dry_run_handler function for a given log event and metadata extractor.

    Parameters
    ----------
    log_event
        Log event to emit when dry run is detected.
    get_metadata
        Function that extracts metadata from the pipeline instance for logging.

    Returns
    -------
    Callable[[PipelineT, ChemblExtractionContext, BoundLogger, float], pd.DataFrame]:
        Function that handles dry run mode by logging and returning empty DataFrame.

    Examples
    --------
    >>> def get_metadata(pipeline):
    ...     return {"chembl_release": pipeline.chembl_release}
    >>> handler = make_dry_run_handler(LogEvents.CHEMBL_TARGET_EXTRACT_SKIPPED, get_metadata)
    >>> df = handler(pipeline, context, log, stage_start)
    """

    def dry_run_handler(
        pipeline: PipelineT,
        _: ChemblExtractionContext,
        log: BoundLogger,
        stage_start: float,
    ) -> pd.DataFrame:
        duration_ms = (time.perf_counter() - stage_start) * 1000.0
        metadata = get_metadata(pipeline)
        log.info(
            log_event,
            dry_run=True,
            duration_ms=duration_ms,
            **metadata,
        )
        return pd.DataFrame()

    return dry_run_handler
