"""Координаторы для работы с метаданными запуска."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from bioetl.core.pipeline.services import MetadataRuntimeService

from bioetl.core.pipeline.types import StageContextProtocol, StageProtocol


class MetadataRuntimeProtocol(Protocol):
    """Минимальный интерфейс для получения метаданных."""

    metadata_service: Any
    builder: Any | None
    git_commit: str | None
    config_hash: str | None

    def build_run_metadata(
        self,
        context: StageContextProtocol,
        stage_plan: tuple[StageProtocol, ...] | tuple[Any, ...],
        durations: dict[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
        ...

    def build_run_result(
        self,
        *,
        context: StageContextProtocol,
        stage_plan: tuple[StageProtocol, ...] | tuple[Any, ...],
        run_state: Any,
        run_tag: str | None,
        mode: str | None,
        rows: int,
        qc_metrics_path: Path | None,
        success: bool,
        output_dir: Path,
        logs_directory: Path,
    ) -> Any:
        ...


class MetadataCoordinator:
    """Координатор, отвечающий за построение сервисов метаданных."""

    def __init__(
        self,
        *,
        metadata_runtime_service: MetadataRuntimeProtocol,
        logs_directory_resolver: Callable[[Path], Path],
    ) -> None:
        self.metadata_runtime_service = metadata_runtime_service
        self.logs_directory_resolver = logs_directory_resolver

    @property
    def metadata_service(self) -> Any:
        return getattr(self.metadata_runtime_service, "metadata_service", None)

    @property
    def git_commit(self) -> str | None:
        return getattr(self.metadata_runtime_service, "git_commit", None)

    @property
    def config_hash(self) -> str | None:
        return getattr(self.metadata_runtime_service, "config_hash", None)

    @classmethod
    def from_factory(
        cls,
        *,
        metadata_runtime_service_factory: Callable[
            ["MetadataCoordinator"], "MetadataRuntimeService"
        ],
        logs_directory_resolver: Callable[[Path], Path],
    ) -> "MetadataCoordinator":
        placeholder = cls.__new__(cls)
        runtime_service = metadata_runtime_service_factory(placeholder)
        cls.__init__(
            placeholder,
            metadata_runtime_service=runtime_service,
            logs_directory_resolver=logs_directory_resolver,
        )
        return placeholder


__all__ = [
    "MetadataCoordinator",
    "MetadataRuntimeProtocol",
]
