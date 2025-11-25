from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional

from .dto import RunResult


class PipelineBase(ABC):
    """Базовый интерфейс для управления жизненным циклом ETL-пайплайна."""

    @abstractmethod
    def run(self) -> RunResult:
        """Запускает пайплайн и возвращает агрегированный результат выполнения."""

    @abstractmethod
    def register_hook(self, hook: "PipelineHookABC") -> None:
        """Добавляет хук, который будет вызываться на ключевых этапах выполнения."""


class PipelineHookABC(ABC):
    """Хуки позволяют расширять поведение пайплайна пользовательскими действиями."""

    @abstractmethod
    def before_run(self, pipeline: PipelineBase) -> None:
        """Вызывается перед запуском пайплайна."""

    @abstractmethod
    def after_run(self, pipeline: PipelineBase, result: RunResult) -> None:
        """Вызывается после завершения пайплайна."""

    @abstractmethod
    def before_stage(self, stage: "StageABC") -> None:
        """Вызывается перед выполнением отдельной стадии."""

    @abstractmethod
    def after_stage(self, stage: "StageABC") -> None:
        """Вызывается после выполнения отдельной стадии."""


class StageABC(ABC):
    """Стадия пайплайна (extract/transform/load и т.д.)."""

    name: str

    @abstractmethod
    def run(self) -> None:
        """Выполняет работу стадии."""

    @abstractmethod
    def dependencies(self) -> Iterable["StageABC"]:
        """Возвращает список стадий, которые должны быть выполнены раньше."""


class CLICommandABC(ABC):
    """Базовый интерфейс для команд CLI, управляющих пайплайном."""

    @abstractmethod
    def name(self) -> str:
        """Имя команды (используется в CLI-фреймворке)."""

    @abstractmethod
    def run(self, argv: Optional[list[str]] = None) -> int:
        """Выполнение команды, возвращает код выхода."""
