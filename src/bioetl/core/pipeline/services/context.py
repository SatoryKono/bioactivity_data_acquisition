"""Context building services for pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from bioetl.core.pipeline.types import StageContext, StageContextProtocol


@dataclass(slots=True)
class ContextBuilder:
    """Builder for stage context."""

    def build(
        self,
        execution: Any,
        domain: Any,
        infrastructure: Any,
        artifacts: Any,
    ) -> StageContextProtocol:
        """
        Build the stage context.

        Args:
            execution: Execution context.
            domain: Domain context.
            infrastructure: Infrastructure context.
            artifacts: Artifact context.

        Returns:
            The constructed stage context.
        """
        return StageContext(
            execution=execution,
            domain=domain,
            infrastructure=infrastructure,
            artifacts=artifacts,
        )


def default_context_builder_factory() -> Callable[[Any], ContextBuilder]:
    """Create a factory for the default context builder."""
    def _factory(pipeline: Any) -> ContextBuilder:
        _ = pipeline
        return ContextBuilder()

    return _factory
