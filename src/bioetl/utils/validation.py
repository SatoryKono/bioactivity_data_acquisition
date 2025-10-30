"""Helpers for working with schema validation payloads."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd

from bioetl.schemas.base import BaseSchema
from bioetl.schemas.registry import SchemaRegistry
from bioetl.utils.column_constants import (
    DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES,
    normalise_ignore_suffixes,
)
from bioetl.utils.dataframe import resolve_schema_column_order

_DEFAULT_VALIDATION_CHUNK_SIZE = 50_000

if TYPE_CHECKING:
    pass


def _summarize_schema_errors(failure_cases: pd.DataFrame | None) -> list[dict[str, Any]]:
    """Convert Pandera ``failure_cases`` frames into structured issue payloads."""

    issues: list[dict[str, Any]] = []
    if not isinstance(failure_cases, pd.DataFrame) or failure_cases.empty:
        return issues

    working = failure_cases.copy()
    if "column" not in working.columns:
        working["column"] = None

    for column, group in working.groupby("column", dropna=False):
        if pd.isna(column):
            column_name = "<dataframe>"
        else:
            column_name = str(column)

        if "check" in group.columns:
            check_values = group["check"].dropna().astype(str).unique().tolist()
        else:
            check_values = []

        if "failure_case" in group.columns:
            failure_values = group["failure_case"].dropna().astype(str).unique().tolist()
        else:
            failure_values = []

        issues.append(
            {
                "issue_type": "schema",
                "severity": "error",
                "column": column_name,
                "check": ", ".join(sorted(check_values)) if check_values else "<unspecified>",
                "count": int(group.shape[0]),
                "details": ", ".join(failure_values[:5]),
            }
        )

    return issues


class ColumnComparisonResult:
    """Результат сравнения колонок."""

    def __init__(
        self,
        entity: str,
        expected_columns: list[str],
        actual_columns: list[str],
        missing_columns: list[str],
        extra_columns: list[str],
        order_matches: bool,
        column_count_matches: bool,
        empty_columns: list[str],
        non_empty_columns: list[str],
        duplicate_columns: dict[str, int],
    ):
        self.entity = entity
        self.expected_columns = expected_columns
        self.actual_columns = actual_columns
        self.missing_columns = missing_columns
        self.extra_columns = extra_columns
        self.order_matches = order_matches
        self.column_count_matches = column_count_matches
        self.empty_columns = empty_columns
        self.non_empty_columns = non_empty_columns
        self.duplicate_columns = duplicate_columns
        self.overall_match = (
            len(missing_columns) == 0
            and len(extra_columns) == 0
            and order_matches
            and column_count_matches
        )

    def to_dict(self) -> dict[str, Any]:
        """Преобразовать результат в словарь."""
        return {
            "entity": self.entity,
            "overall_match": self.overall_match,
            "expected_columns": self.expected_columns,
            "actual_columns": self.actual_columns,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "order_matches": self.order_matches,
            "column_count_matches": self.column_count_matches,
            "empty_columns": self.empty_columns,
            "non_empty_columns": self.non_empty_columns,
            "duplicate_columns": self.duplicate_columns,
            "expected_count": len(self.expected_columns),
            "actual_count": len(self.actual_columns),
            "empty_count": len(self.empty_columns),
            "non_empty_count": len(self.non_empty_columns),
            "duplicate_unique_count": len(self.duplicate_columns),
            "duplicate_total": sum(count - 1 for count in self.duplicate_columns.values()),
        }


class ColumnValidator:
    """Валидатор колонок для сравнения с требованиями."""

    def __init__(self, skip_suffixes: Iterable[str] | None = None):
        from bioetl.core.logger import UnifiedLogger

        self.logger = UnifiedLogger.get(__name__)
        if skip_suffixes is None:
            skip_suffixes = DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES
        self._skip_suffixes = normalise_ignore_suffixes(skip_suffixes)

    def compare_columns(
        self,
        entity: str,
        actual_df: pd.DataFrame | None,
        schema_version: str = "latest",
        *,
        column_non_null_counts: Sequence[tuple[str, int]] | None = None,
    ) -> ColumnComparisonResult:
        """
        Сравнить колонки в DataFrame с ожидаемой схемой.

        Args:
            entity: Имя сущности (assay, activity, testitem, target, document)
            actual_df: DataFrame для проверки
            schema_version: Версия схемы для сравнения

        Returns:
            ColumnComparisonResult с результатами сравнения
        """
        try:
            # Получить схему из реестра
            schema = SchemaRegistry.get(entity, schema_version)
            expected_columns = self._get_expected_columns(schema)  # type: ignore[arg-type]

            # Получить фактические колонки
            if actual_df is None and column_non_null_counts is None:
                raise ValueError(
                    "Either actual_df or column_non_null_counts must be provided to determine actual columns."
                )

            if actual_df is not None:
                actual_columns = list(actual_df.columns)
            else:
                actual_columns = [col for col, _ in column_non_null_counts or []]
            duplicates_counter = Counter(actual_columns)
            duplicate_columns = {
                column: count
                for column, count in duplicates_counter.items()
                if count > 1
            }

            # Сравнить колонки
            missing_columns = list(set(expected_columns) - set(actual_columns))
            extra_columns = list(set(actual_columns) - set(expected_columns))
            order_matches = expected_columns == actual_columns
            column_count_matches = len(expected_columns) == len(actual_columns)

            # Проверить пустые колонки
            empty_columns, non_empty_columns = self._analyze_column_data(
                actual_df,
                column_non_null_counts=column_non_null_counts,
            )

            result = ColumnComparisonResult(
                entity=entity,
                expected_columns=expected_columns,
                actual_columns=actual_columns,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                order_matches=order_matches,
                column_count_matches=column_count_matches,
                empty_columns=empty_columns,
                non_empty_columns=non_empty_columns,
                duplicate_columns=duplicate_columns,
            )

            self.logger.info(
                "column_comparison_completed",
                entity=entity,
                overall_match=result.overall_match,
                expected_count=len(expected_columns),
                actual_count=len(actual_columns),
                missing_count=len(missing_columns),
                extra_count=len(extra_columns),
                duplicate_unique_count=len(duplicate_columns),
            )

            return result

        except Exception as e:
            self.logger.error(
                "column_comparison_failed",
                entity=entity,
                error=str(e),
            )
            raise

    def _get_expected_columns(self, schema: type[BaseSchema]) -> list[str]:
        """Получить ожидаемые колонки из схемы."""
        return resolve_schema_column_order(schema)

    def _analyze_column_data(
        self,
        df: pd.DataFrame | None,
        *,
        column_non_null_counts: Sequence[tuple[str, int]] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Анализировать данные в колонках и определить пустые/непустые.

        Args:
            df: DataFrame для анализа

        Returns:
            Tuple[empty_columns, non_empty_columns]
        """
        empty_columns: list[str] = []
        non_empty_columns: list[str] = []

        if column_non_null_counts is not None:
            for column, non_null_count in column_non_null_counts:
                if non_null_count == 0:
                    empty_columns.append(column)
                else:
                    non_empty_columns.append(column)
            return empty_columns, non_empty_columns

        if df is None:
            return empty_columns, non_empty_columns

        for idx, column in enumerate(df.columns):
            series = df.iloc[:, idx]
            non_null_count = series.notna().sum()

            if non_null_count == 0:
                empty_columns.append(column)
            else:
                non_empty_columns.append(column)

        return empty_columns, non_empty_columns

    def _calculate_non_null_counts(
        self,
        csv_file: Path,
        column_names: Sequence[str],
        chunk_size: int = _DEFAULT_VALIDATION_CHUNK_SIZE,
    ) -> list[tuple[str, int]]:
        """Подсчитать количество непустых значений для каждой колонки."""

        if not column_names:
            return []

        counts = [0] * len(column_names)

        try:
            reader = pd.read_csv(csv_file, chunksize=chunk_size)
            for chunk_obj in reader:
                # Возможны неожиданные типы чанк-элементов; продолжаем только для DataFrame с данными
                if not isinstance(chunk_obj, pd.DataFrame) or chunk_obj.empty:
                    continue

                chunk = cast(pd.DataFrame, chunk_obj)
                available_columns = min(len(column_names), chunk.shape[1])
                for idx in range(available_columns):
                    series = chunk.iloc[:, idx]
                    counts[idx] += int(series.notna().sum())
        except ValueError:
            return [(column, 0) for column in column_names]
        except Exception as exc:  # pragma: no cover - защитный случай
            self.logger.warning(
                "column_non_null_count_failed",
                file=str(csv_file),
                error=str(exc),
            )

        return list(zip(column_names, counts, strict=False))

    def generate_report(
        self,
        results: list[ColumnComparisonResult],
        output_path: Path,
    ) -> Path:
        """
        Сгенерировать отчет о сравнении колонок.

        Args:
            results: Список результатов сравнения
            output_path: Путь для сохранения отчета

        Returns:
            Путь к созданному отчету
        """
        report_data = {
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            "summary": self._generate_summary(results),
            "details": [result.to_dict() for result in results],
        }

        # Сохранить JSON отчет
        json_path = output_path / "column_comparison_report.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        # Создать Markdown отчет
        md_path = output_path / "column_comparison_report.md"
        self._generate_markdown_report(report_data, md_path)

        self.logger.info(
            "column_comparison_report_generated",
            json_path=str(json_path),
            md_path=str(md_path),
        )

        return md_path

    def _generate_summary(self, results: list[ColumnComparisonResult]) -> dict[str, Any]:
        """Сгенерировать сводку результатов."""
        total_entities = len(results)
        matching_entities = sum(1 for r in results if r.overall_match)
        entities_with_issues = total_entities - matching_entities

        return {
            "total_entities": total_entities,
            "matching_entities": matching_entities,
            "entities_with_issues": entities_with_issues,
            "overall_success_rate": matching_entities / total_entities if total_entities > 0 else 0,
            "issues": {
                "missing_columns": sum(len(r.missing_columns) for r in results),
                "extra_columns": sum(len(r.extra_columns) for r in results),
                "order_mismatches": sum(1 for r in results if not r.order_matches),
                "count_mismatches": sum(1 for r in results if not r.column_count_matches),
                "empty_columns": sum(len(r.empty_columns) for r in results),
                "duplicate_columns": sum(
                    sum(count - 1 for count in r.duplicate_columns.values())
                    for r in results
                ),
            },
        }

    def _generate_markdown_report(
        self,
        report_data: dict[str, Any],
        output_path: Path,
    ) -> None:
        """Сгенерировать Markdown отчет."""
        results: list[dict[str, Any]] = report_data["details"]
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("# Отчет о сравнении колонок\n\n")
            f.write(f"**Дата:** {report_data['timestamp']}\n\n")

            # Сводка
            summary = report_data["summary"]
            f.write("## Сводка\n\n")
            f.write(f"- **Всего сущностей:** {summary['total_entities']}\n")
            f.write(f"- **Соответствуют требованиям:** {summary['matching_entities']}\n")
            f.write(f"- **Имеют проблемы:** {summary['entities_with_issues']}\n")
            f.write(f"- **Процент успеха:** {summary['overall_success_rate']:.1%}\n\n")

            # Проблемы
            issues = summary["issues"]
            f.write("## Проблемы\n\n")
            f.write(f"- **Отсутствующие колонки:** {issues['missing_columns']}\n")
            f.write(f"- **Лишние колонки:** {issues['extra_columns']}\n")
            f.write(f"- **Несоответствие порядка:** {issues['order_mismatches']}\n")
            f.write(f"- **Несоответствие количества:** {issues['count_mismatches']}\n")
            f.write(f"- **Пустые колонки:** {issues['empty_columns']}\n\n")
            f.write(f"- **Дубликаты колонок:** {issues['duplicate_columns']}\n\n")

            # Детали по каждой сущности
            f.write("## Детали по сущностям\n\n")
            for detail in results:
                f.write(f"### {detail['entity'].upper()}\n\n")
                f.write(f"**Статус:** {'Соответствует' if detail['overall_match'] else 'Не соответствует'}\n\n")
                f.write(f"**Количество колонок:** {detail['actual_count']} (ожидается {detail['expected_count']})\n")
                f.write(f"**Пустые колонки:** {detail['empty_count']} из {detail['actual_count']}\n\n")

                if detail["missing_columns"]:
                    f.write("**Отсутствующие колонки:**\n")
                    for col in detail["missing_columns"]:
                        f.write(f"- `{col}`\n")
                    f.write("\n")

                if detail["extra_columns"]:
                    f.write("**Лишние колонки:**\n")
                    for col in detail["extra_columns"]:
                        f.write(f"- `{col}`\n")
                    f.write("\n")

                if detail["duplicate_columns"]:
                    f.write("**Дубликаты колонок:**\n")
                    for col, count in detail["duplicate_columns"].items():
                        f.write(f"- `{col}` — {count} раз(а)\n")
                    f.write("\n")

                if not detail["order_matches"]:
                    f.write("**Порядок колонок не соответствует требованиям**\n\n")

                if detail["empty_columns"]:
                    f.write("**Пустые колонки:**\n")
                    for col in detail["empty_columns"]:
                        f.write(f"- `{col}`\n")
                    f.write("\n")

                f.write("---\n\n")

    def validate_pipeline_output(
        self,
        pipeline_name: str,
        output_dir: Path,
        schema_version: str = "latest",
    ) -> list[ColumnComparisonResult]:
        """
        Валидировать выходные данные pipeline.

        Args:
            pipeline_name: Имя pipeline (assay, activity, testitem, target, document)
            output_dir: Директория с выходными данными
            schema_version: Версия схемы для сравнения

        Returns:
            Список результатов сравнения
        """
        results: list[ColumnComparisonResult] = []

        # Найти CSV файлы в выходной директории
        csv_files = sorted(
            [
                csv_file
                for csv_file in output_dir.glob("**/*.csv")
                if not self._should_skip_file(csv_file)
            ]
        )

        if not csv_files:
            self.logger.warning(
                "no_csv_files_found",
                pipeline_name=pipeline_name,
                output_dir=str(output_dir),
            )
            return results

        git_lfs_pointers: list[Path] = []

        for csv_file in csv_files:
            try:
                if self._is_git_lfs_pointer(csv_file):
                    git_lfs_pointers.append(csv_file)
                    self.logger.warning(
                        "git_lfs_pointer_detected",
                        file=str(csv_file),
                        hint="Fetch real artifacts with `git lfs pull` or rerun the pipeline.",
                    )
                    continue

                try:
                    header_df = pd.read_csv(csv_file, nrows=0)
                except ValueError:
                    header_df = pd.DataFrame()

                column_names = list(header_df.columns)
                non_null_counts = self._calculate_non_null_counts(
                    csv_file, column_names
                )

                # Определить сущность по имени файла
                entity = self._extract_entity_from_filename(csv_file.name)

                # Сравнить колонки
                result = self.compare_columns(
                    entity,
                    None,
                    schema_version,
                    column_non_null_counts=non_null_counts,
                )
                results.append(result)

            except Exception as e:
                self.logger.error(
                    "validation_failed_for_file",
                    file=str(csv_file),
                    error=str(e),
                )

        if git_lfs_pointers and not results:
            pointer_list = ", ".join(str(path) for path in git_lfs_pointers)
            raise ValueError(
                "Не удалось валидировать колонки: обнаружены только Git LFS pointer файлы. "
                "Загрузите реальные артефакты (git lfs pull) или перезапустите пайплайн. "
                f"Файлы: {pointer_list}"
            )

        return results

    def _extract_entity_from_filename(self, filename: str) -> str:
        """Извлечь имя сущности из имени файла."""
        # Убрать расширение и путь
        name = Path(filename).stem

        # Базовое имя определяется до первого разделителя и приводится к нижнему регистру
        base_name = name.split("_", 1)[0].lower()
        normalized_name = name.lower()

        # Маппинг имен файлов на сущности
        entity_mapping = {
            "assay": "assay",
            "assays": "assay",
            "activity": "activity",
            "activities": "activity",
            "testitem": "testitem",
            "testitems": "testitem",
            "target": "target",
            "targets": "target",
            "document": "document",
            "documents": "document",
        }

        # Сначала ищем точное совпадение по базовому имени
        if base_name in entity_mapping:
            return entity_mapping[base_name]

        # Затем пытаемся сопоставить по префиксу полного имени
        for candidate, entity in entity_mapping.items():
            if normalized_name.startswith(candidate):
                return entity

        return base_name

    def _should_skip_file(self, path: Path) -> bool:
        """Определить, следует ли пропустить CSV файл при валидации."""

        filename = path.name.lower()

        return filename.endswith(self._skip_suffixes)

    def _is_git_lfs_pointer(self, path: Path) -> bool:
        """Проверить, представляет ли файл указатель Git LFS."""

        try:
            with path.open("r", encoding="utf-8") as handle:
                first_line = handle.readline().strip()
        except OSError:
            return False

        return first_line.startswith("version https://git-lfs.github.com/spec/")


__all__ = [
    "_summarize_schema_errors",
    "ColumnComparisonResult",
    "ColumnValidator",
]

