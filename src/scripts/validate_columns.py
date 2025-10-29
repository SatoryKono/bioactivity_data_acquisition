#!/usr/bin/env python3
"""CLI для валидации колонок в выходных данных pipeline'ов."""

from pathlib import Path

import typer

from bioetl.utils.column_validator import ColumnValidator

app = typer.Typer(help="Валидация колонок в выходных данных pipeline'ов")


@app.command()
def validate(
    pipeline_name: str = typer.Argument(..., help="Имя pipeline (assay, activity, testitem, target, document)"),
    output_dir: Path = typer.Option(
        Path("data/output"),
        "--output-dir",
        "-o",
        help="Директория с выходными данными",
    ),
    schema_version: str = typer.Option(
        "latest",
        "--schema-version",
        help="Версия схемы для сравнения",
    ),
    report_dir: Path = typer.Option(
        Path("data/output/validation_reports"),
        "--report-dir",
        help="Директория для сохранения отчетов",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Подробный вывод",
    ),
) -> None:
    """Валидировать колонки в выходных данных pipeline."""

    # Создать директорию для отчетов
    report_dir.mkdir(parents=True, exist_ok=True)

    # Создать валидатор
    validator = ColumnValidator()

    # Найти директорию с данными для конкретного pipeline
    pipeline_output_dir = output_dir / pipeline_name

    if not pipeline_output_dir.exists():
        typer.echo(f"❌ Директория {pipeline_output_dir} не найдена")
        raise typer.Exit(1)

    typer.echo(f"🔍 Валидация колонок для pipeline: {pipeline_name}")
    typer.echo(f"📁 Выходная директория: {pipeline_output_dir}")
    typer.echo(f"📋 Версия схемы: {schema_version}")
    typer.echo()

    # Валидировать выходные данные
    results = validator.validate_pipeline_output(
        pipeline_name=pipeline_name,
        output_dir=pipeline_output_dir,
        schema_version=schema_version,
    )

    if not results:
        typer.echo("⚠️  Не найдено CSV файлов для валидации")
        return

    # Показать краткие результаты
    typer.echo("📊 Результаты валидации:")
    typer.echo()

    for result in results:
        status = "✅" if result.overall_match else "❌"
        typer.echo(f"{status} {result.entity}: {len(result.actual_columns)} колонок ({len(result.empty_columns)} пустых)")

        if result.missing_columns:
            typer.echo(f"   Отсутствуют: {', '.join(result.missing_columns)}")

        if result.extra_columns:
            typer.echo(f"   Лишние: {', '.join(result.extra_columns)}")

        if result.empty_columns:
            typer.echo(f"   Пустые: {', '.join(result.empty_columns)}")

        if not result.order_matches:
            typer.echo("   Порядок колонок не соответствует требованиям")

    typer.echo()

    # Сгенерировать отчет
    report_path = validator.generate_report(results, report_dir)

    typer.echo(f"📄 Отчет сохранен: {report_path}")

    # Показать общую статистику
    total_entities = len(results)
    matching_entities = sum(1 for r in results if r.overall_match)
    success_rate = matching_entities / total_entities if total_entities > 0 else 0

    typer.echo()
    typer.echo("📈 Общая статистика:")
    typer.echo(f"   Соответствуют требованиям: {matching_entities}/{total_entities} ({success_rate:.1%})")

    if success_rate < 1.0:
        typer.echo("⚠️  Обнаружены несоответствия требованиям")
        raise typer.Exit(1)
    else:
        typer.echo("✅ Все колонки соответствуют требованиям")


@app.command()
def compare_all(
    output_dir: Path = typer.Option(
        Path("data/output"),
        "--output-dir",
        "-o",
        help="Директория с выходными данными",
    ),
    schema_version: str = typer.Option(
        "latest",
        "--schema-version",
        help="Версия схемы для сравнения",
    ),
    report_dir: Path = typer.Option(
        Path("data/output/validation_reports"),
        "--report-dir",
        help="Директория для сохранения отчетов",
    ),
) -> None:
    """Валидировать колонки для всех pipeline'ов."""

    # Создать директорию для отчетов
    report_dir.mkdir(parents=True, exist_ok=True)

    # Создать валидатор
    validator = ColumnValidator()

    # Список pipeline'ов для проверки
    pipelines = ["assay", "activity", "testitem", "target", "document"]

    all_results = []

    typer.echo("🔍 Валидация колонок для всех pipeline'ов")
    typer.echo(f"📁 Выходная директория: {output_dir}")
    typer.echo(f"📋 Версия схемы: {schema_version}")
    typer.echo()

    for pipeline_name in pipelines:
        pipeline_output_dir = output_dir / pipeline_name

        if not pipeline_output_dir.exists():
            typer.echo(f"⚠️  Пропуск {pipeline_name}: директория не найдена")
            continue

        typer.echo(f"🔍 Проверка {pipeline_name}...")

        results = validator.validate_pipeline_output(
            pipeline_name=pipeline_name,
            output_dir=pipeline_output_dir,
            schema_version=schema_version,
        )

        all_results.extend(results)

    if not all_results:
        typer.echo("⚠️  Не найдено данных для валидации")
        return

    # Сгенерировать общий отчет
    report_path = validator.generate_report(all_results, report_dir)

    typer.echo()
    typer.echo(f"📄 Общий отчет сохранен: {report_path}")

    # Показать общую статистику
    total_entities = len(all_results)
    matching_entities = sum(1 for r in all_results if r.overall_match)
    success_rate = matching_entities / total_entities if total_entities > 0 else 0

    typer.echo()
    typer.echo("📈 Общая статистика:")
    typer.echo(f"   Соответствуют требованиям: {matching_entities}/{total_entities} ({success_rate:.1%})")

    if success_rate < 1.0:
        typer.echo("⚠️  Обнаружены несоответствия требованиям")
        raise typer.Exit(1)
    else:
        typer.echo("✅ Все колонки соответствуют требованиям")


if __name__ == "__main__":
    app()
