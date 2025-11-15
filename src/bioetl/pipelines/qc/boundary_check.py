"""Анализ границы CLI ↔ QC на уровне пайплайнового слоя."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple

from bioetl.core.utils.mixins import CollectionFlagMixin
from bioetl.tools import qc_boundary as boundary_tools

__all__ = [
    "DEFAULT_PACKAGE",
    "DEFAULT_SOURCE_ROOT",
    "QC_MODULE_PREFIX",
    "QCBoundaryViolation",
    "QCBoundaryReport",
    "collect_cli_qc_boundary_report",
]

DEFAULT_PACKAGE = boundary_tools.DEFAULT_PACKAGE
DEFAULT_SOURCE_ROOT = boundary_tools.DEFAULT_SRC_ROOT
QC_MODULE_PREFIX = boundary_tools.QC_MODULE_PREFIX


@dataclass(frozen=True, slots=True)
class QCBoundaryViolation:
    """Нарушение границы импортов между CLI и QC."""

    module: str
    qc_module: str
    import_chain: Tuple[str, ...]
    source_path: Path

    def format_chain(self) -> str:
        """Вернуть человеческое представление цепочки импорта."""

        return " -> ".join(self.import_chain)


@dataclass(frozen=True, slots=True)
class QCBoundaryReport(CollectionFlagMixin):
    """Результат анализа границы CLI ↔ QC."""

    package: str
    violations: tuple[QCBoundaryViolation, ...]

    @property
    def has_violations(self) -> bool:
        """Признак наличия нарушений."""

        return self.has_items(self.violations)

    def iter_paths(self) -> Iterable[Path]:
        """Перечислить пути исходников с нарушениями в детерминированном порядке."""

        return (violation.source_path for violation in self.violations)


def collect_cli_qc_boundary_report(
    *,
    cli_root: Path | None = None,
    package: str | None = None,
    source_root: Path | None = None,
) -> QCBoundaryReport:
    """Собрать отчёт о нарушениях границы CLI ↔ QC."""

    effective_package = package or DEFAULT_PACKAGE
    violations = boundary_tools.collect_qc_boundary_violations(
        cli_root=cli_root,
        package=effective_package,
        source_root=source_root,
    )
    normalized: list[QCBoundaryViolation] = []
    for violation in violations:
        if not violation.chain:
            continue
        normalized.append(
            QCBoundaryViolation(
                module=violation.chain[0],
                qc_module=violation.chain[-1],
                import_chain=violation.chain,
                source_path=violation.source_path,
            )
        )
    normalized_tuple = tuple(normalized)
    return QCBoundaryReport(package=effective_package, violations=normalized_tuple)



