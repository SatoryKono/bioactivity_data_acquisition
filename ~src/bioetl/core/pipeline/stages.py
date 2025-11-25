from __future__ import annotations

from typing import TYPE_CHECKING

from bioetl.core.logging import LogEvents
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    StageContext,
    StageExecutionOptions,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from bioetl.core.pipeline.orchestration import PipelineBaseCommon


class BaseStageCommand(PipelineStageCommand):
    """Convenience base class for default stage commands."""

    def __init__(self, pipeline: PipelineBaseCommon, name: str) -> None:
        self.pipeline = pipeline
        self.name = name

    def should_run(self, options: StageExecutionOptions) -> bool:
        return True

    def execute(self, context: StageContext) -> None:  # pragma: no cover
        raise NotImplementedError


_EXTRACT_PAYLOAD_KEY = "extract"
_TRANSFORM_PAYLOAD_KEY = "transform"
_VALIDATE_PAYLOAD_KEY = "validate"


class _ExtractStageCommand(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "extract")

    def execute(self, context: StageContext) -> None:
        options = context.options
        with context.pipeline_stage(self.name) as stage_log:
            stage_log.info(LogEvents.STAGE_EXTRACT_START)
            extracted = self.pipeline.extract(
                mode=options.extract_mode,
                ids=options.extract_ids,
            )
            rows = self.pipeline._safe_len(extracted)  # pylint: disable=protected-access
            stage_log.info(LogEvents.STAGE_EXTRACT_FINISH, rows=rows)
        context.set_payload(_EXTRACT_PAYLOAD_KEY, extracted)


class _TransformStageCommand(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "transform")

    def execute(self, context: StageContext) -> None:
        extracted = context.require_payload(_EXTRACT_PAYLOAD_KEY)
        with context.pipeline_stage(self.name) as stage_log:
            stage_log.info(LogEvents.STAGE_TRANSFORM_START)
            transformed = self.pipeline.transform(extracted)
            rows = self.pipeline._safe_len(transformed)  # pylint: disable=protected-access
            stage_log.info(LogEvents.STAGE_TRANSFORM_FINISH, rows=rows)
        context.set_payload(_TRANSFORM_PAYLOAD_KEY, transformed)


class _ValidateStageCommand(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "validate")

    def execute(self, context: StageContext) -> None:
        transformed = context.require_payload(_TRANSFORM_PAYLOAD_KEY)
        payload = self.pipeline._apply_cli_sample(  # pylint: disable=protected-access
            transformed
        )
        with context.pipeline_stage(self.name) as stage_log:
            stage_log.info(LogEvents.STAGE_VALIDATE_START)
            validated = self.pipeline.validate(payload)
            rows = self.pipeline._safe_len(validated)  # pylint: disable=protected-access
            stage_log.info(LogEvents.STAGE_VALIDATE_FINISH, rows=rows)
        context.set_payload(_VALIDATE_PAYLOAD_KEY, validated)


class _WriteStageCommand(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "write")

    def execute(self, context: StageContext) -> None:
        validated = context.require_payload(_VALIDATE_PAYLOAD_KEY)
        options = context.options
        config = self.pipeline.config
        effective_extended = bool(
            options.extended or getattr(config.cli, "extended", False)
        )
        postprocess_config = getattr(config, "postprocess", None)
        correlation_config = getattr(postprocess_config, "correlation", None)
        correlation_default = bool(
            getattr(correlation_config, "enabled", False)
        )
        include_correlation_flag = (
            bool(options.include_correlation)
            or effective_extended
            or correlation_default
        )
        include_qc_metrics_flag = (
            bool(options.include_qc_metrics) or effective_extended
        )
        self.pipeline._qc_fail_on_threshold = bool(  # pylint: disable=protected-access
            options.fail_on_qc_violation
        )
        with context.pipeline_stage(self.name) as stage_log:
            stage_log.info(
                LogEvents.STAGE_WRITE_START,
                output_path=str(context.output_dir),
            )
            result = self.pipeline.save_results(
                validated,
                context.output_dir,
                extended=effective_extended,
                include_correlation=include_correlation_flag,
                include_qc_metrics=include_qc_metrics_flag,
            )
            stage_log.info(
                LogEvents.STAGE_WRITE_FINISH,
                dataset=str(result.write_result.dataset),
            )
        context.set_result(result)


class _CleanupStageCommand(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "cleanup")

    def execute(self, context: StageContext) -> None:
        with context.pipeline_stage(self.name) as cleanup_log:
            cleanup_log.info(LogEvents.STAGE_CLEANUP_START)
            context.pipeline._cleanup_registered_clients()  # pylint: disable=protected-access
            try:
                context.pipeline.close_resources()
            except Exception as cleanup_error:  # pragma: no cover  # pylint: disable=broad-exception-caught
                # defensive cleanup path
                cleanup_log.warning(
                    LogEvents.STAGE_CLEANUP_ERROR,
                    error=str(cleanup_error),
                )
            context.pipeline._qc_report_options = None  # pylint: disable=protected-access
            context.pipeline._qc_thresholds = {}  # pylint: disable=protected-access
            context.pipeline._qc_fail_on_threshold = False  # pylint: disable=protected-access
            cleanup_log.info(
                LogEvents.STAGE_CLEANUP_FINISH,
            )


__all__ = [
    "BaseStageCommand",
    "_ExtractStageCommand",
    "_TransformStageCommand",
    "_ValidateStageCommand",
    "_WriteStageCommand",
    "_CleanupStageCommand",
    "_EXTRACT_PAYLOAD_KEY",
    "_TRANSFORM_PAYLOAD_KEY",
    "_VALIDATE_PAYLOAD_KEY",
]
