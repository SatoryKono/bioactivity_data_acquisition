from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dc_field
from typing import Iterable, Mapping, Sequence


@dataclass
class ValidationError:
    """Ошибка валидации для конкретной записи или поля."""

    message: str
    field: str | None = None
    row_index: int | None = None
    context: Mapping[str, object] = dc_field(default_factory=dict)


@dataclass
class ValidationResult:
    """Итог проверки данных."""

    is_valid: bool
    errors: Sequence[ValidationError] = dc_field(default_factory=tuple)
    warnings: Sequence[str] = dc_field(default_factory=tuple)


@dataclass
class DQIssue:
    """Запись об инциденте качества данных."""

    rule_name: str
    severity: str
    message: str
    affected_rows: int | None = None
    context: Mapping[str, object] = dc_field(default_factory=dict)


class SchemaProviderABC(ABC):
    """Предоставляет схему (например, Pandera) для валидации."""

    @abstractmethod
    def schema(self) -> object:
        """Возвращает объект схемы для проверки."""


class ValidatorABC(ABC):
    """Выполняет валидацию данных."""

    @abstractmethod
    def validate(self, data: Sequence[Mapping[str, object]]) -> ValidationResult:
        """Проверяет входные данные и возвращает результат."""


class DQRuleABC(ABC):
    """Правило контроля качества данных."""

    @abstractmethod
    def name(self) -> str:
        """Возвращает человекочитаемое имя правила."""

    @abstractmethod
    def evaluate(self, data: Sequence[Mapping[str, object]]) -> Iterable[DQIssue]:
        """Генерирует найденные проблемы качества данных."""
