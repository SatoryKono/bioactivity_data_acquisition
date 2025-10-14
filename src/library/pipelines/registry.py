"""Registry of pipeline entry points."""

from __future__ import annotations

import logging
from typing import Callable, Dict

from ..config_loader import PipelineRuntime

logger = logging.getLogger(__name__)

PipelineCallable = Callable[[PipelineRuntime], None]


def _log_pipeline_execution(runtime: PipelineRuntime, action: str) -> None:
    logger.info(
        "Pipeline %s",
        action,
        extra={
            "pipeline": runtime.name,
            "limit": runtime.limit,
            "output_dir": str(runtime.output_dir),
            "date_tag": runtime.date_tag,
            "dry_run": runtime.dry_run,
            "postprocess": runtime.postprocess,
            "parameters": runtime.parameters,
        },
    )


def _execute_pipeline(runtime: PipelineRuntime) -> None:
    _log_pipeline_execution(runtime, "started")
    if runtime.dry_run:
        logger.info(
            "Dry-run execution completed", extra={"pipeline": runtime.name}
        )
    else:
        logger.info(
            "Execution completed", extra={"pipeline": runtime.name, "records": runtime.limit}
        )


def _make_handler(name: str) -> PipelineCallable:
    def _handler(runtime: PipelineRuntime) -> None:
        if runtime.name != name:
            logger.warning(
                "Runtime pipeline mismatch", extra={"expected": name, "actual": runtime.name}
            )
        _execute_pipeline(runtime)

    return _handler


_PIPELINES: Dict[str, PipelineCallable] = {
    "activity": _make_handler("activity"),
    "assay": _make_handler("assay"),
    "target": _make_handler("target"),
    "document": _make_handler("document"),
    "testitem": _make_handler("testitem"),
}


def get_pipeline(name: str) -> PipelineCallable:
    try:
        return _PIPELINES[name]
    except KeyError as exc:
        available = ", ".join(sorted(_PIPELINES))
        raise KeyError(f"Pipeline '{name}' is not registered. Available: {available}") from exc
