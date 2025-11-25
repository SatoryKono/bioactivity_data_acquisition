from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from bioetl.core.pipeline.unified import PipelineBase


@dataclass(slots=True)
class StageAlias:
    stage: str
    handler: Callable[[PipelineBase, Mapping[str, Any]], Any]


class StageRunner:
    """Простой раннер стадий ChEMBL пайплайна."""

    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline
        self._aliases: dict[str, StageAlias] = {}

    def register_alias(self, alias: str, stage: str) -> None:
        self._aliases[alias] = StageAlias(stage=stage, handler=self._resolve_stage(stage))

    def _resolve_stage(self, stage: str) -> Callable[[PipelineBase, Mapping[str, Any]], Any]:
        def _runner(pipeline: PipelineBase, options: Mapping[str, Any]) -> Any:
            method = getattr(pipeline, stage)
            if stage == "write":
                return method(options.get("df"), Path(options["output_dir"]), extended=options.get("extended", False))
            if stage == "run":
                return method(Path(options["output_dir"]), **{k: v for k, v in options.items() if k != "output_dir"})
            if "df" in options:
                return method(options["df"])
            return method()

        return _runner

    def run_stage(self, name: str, **options: Any) -> Any:
        alias = self._aliases.get(name)
        handler = alias.handler if alias else self._resolve_stage(name)
        return handler(self.pipeline, options)

