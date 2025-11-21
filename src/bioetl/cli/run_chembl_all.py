"""Команда для последовательного запуска всех ChEMBL-пайплайнов с агрегацией результатов."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer
from structlog.stdlib import BoundLogger

from bioetl.cli.cli_registry import COMMAND_REGISTRY, CommandConfig
from bioetl.config.environment import load_environment_settings
from bioetl.core import LoggerConfig, UnifiedLogger
from bioetl.core.logging import LogEvents
from bioetl.core.pipeline.orchestration import RunResult
from bioetl.core.runtime.cli_pipeline_runner import (
    PipelineCommandOptions,
    PipelineCommandRunner,
    PipelineConfigFactory,
    PipelineExecutionPlan,
)

__all__ = ["run_chembl_all_command"]

_log = UnifiedLogger.get(__name__)

# Порядок запуска ChEMBL-пайплайнов
CHEMBL_PIPELINES = [
    "assay_chembl",
    "activity_chembl",
    "target_chembl",
    "document_chembl",
    "testitem_chembl",
]


@dataclass
class PipelineRunResult:
    """Результат выполнения одного пайплайна."""

    pipeline_name: str
    success: bool
    run_result: RunResult | None = None
    error: str | None = None
    duration_ms: float = 0.0
    row_count: int = 0
    qc_metrics: dict[str, Any] | None = None
    quality_report_path: Path | None = None
    qc_metrics_path: Path | None = None


@dataclass
class AggregatedResults:
    """Агрегированные результаты всех пайплайнов."""

    run_id: str
    start_time: datetime
    end_time: datetime | None = None
    total_duration_ms: float = 0.0
    pipeline_results: dict[str, PipelineRunResult] = field(
        default_factory=dict
    )
    summary: dict[str, Any] = field(default_factory=dict)


def _load_qc_metrics(qc_path: Path | None) -> dict[str, Any] | None:
    """Загрузить QC-метрики из JSON или CSV файла."""
    if qc_path is None or not qc_path.exists():
        return None
    try:
        content = qc_path.read_text(encoding="utf-8")
        # Пытаемся загрузить как JSON
        if qc_path.suffix.lower() == ".json":
            data: dict[str, Any] = json.loads(content)
            return data
        # Для CSV файлов возвращаем None (можно расширить в будущем)
        # так как CSV требует парсинга и преобразования в структуру
        return None
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "Failed to load QC metrics", path=str(qc_path), error=str(exc)
        )
        return None


def _find_qc_files(run_result: RunResult) -> tuple[Path | None, Path | None]:
    """Найти файлы QC-отчётов из результата выполнения пайплайна."""
    quality_report: Path | None = None
    qc_metrics: Path | None = None

    # Используем пути из write_result
    if (
        run_result.write_result.quality_report
        and run_result.write_result.quality_report.exists()
    ):
        quality_report = run_result.write_result.quality_report

    # Используем qc_metrics из write_result или qc_summary
    if (
        run_result.write_result.qc_metrics
        and run_result.write_result.qc_metrics.exists()
    ):
        qc_metrics = run_result.write_result.qc_metrics
    elif run_result.qc_summary and run_result.qc_summary.exists():
        qc_metrics = run_result.qc_summary

    return quality_report, qc_metrics


def _run_single_pipeline(
    pipeline_name: str,
    options: PipelineCommandOptions,
    runner: PipelineCommandRunner,
    logger: BoundLogger,
) -> PipelineRunResult:
    """Запустить один пайплайн и вернуть результат."""
    start_time = datetime.now(timezone.utc)
    logger.info(
        LogEvents.PIPELINE_RUN_START,
        pipeline=pipeline_name,
        output_dir=str(options.output_dir),
    )

    try:
        # Получаем конфигурацию команды из реестра
        config_factory = COMMAND_REGISTRY.get(pipeline_name)
        if config_factory is None:
            raise ValueError(
                f"Pipeline '{pipeline_name}' not found in registry"
            )

        command_config = config_factory()
        if not isinstance(command_config, CommandConfig):
            raise ValueError(f"Invalid command config for '{pipeline_name}'")

        # Создаём план выполнения
        plan = runner.prepare(options)

        # Проверяем, что это не dry-run
        if isinstance(plan, PipelineExecutionPlan):
            # Создаём фабрику пайплайна
            def pipeline_factory(config: Any, run_id: str) -> Any:
                return command_config.pipeline_class(config, run_id)

            # Выполняем пайплайн
            result = runner.execute_plan(
                plan,
                pipeline_factory=pipeline_factory,
                logger=logger,
                command_name=pipeline_name,
                config_path=options.config_path,
                output_dir=options.output_dir,
            )
        else:
            # Dry-run режим
            logger.info(
                "Dry-run mode, skipping execution", pipeline=pipeline_name
            )
            return PipelineRunResult(
                pipeline_name=pipeline_name,
                success=True,
                duration_ms=0.0,
            )

        end_time = datetime.now(timezone.utc)
        duration_ms = (end_time - start_time).total_seconds() * 1000

        # Загружаем QC-метрики
        quality_report_path, qc_metrics_path = _find_qc_files(result)
        qc_metrics = _load_qc_metrics(qc_metrics_path)

        logger.info(
            LogEvents.PIPELINE_RUN_FINISH,
            pipeline=pipeline_name,
            run_id=plan.run_id,
            duration_ms=duration_ms,
            row_count=result.records,
        )

        return PipelineRunResult(
            pipeline_name=pipeline_name,
            success=True,
            run_result=result,
            duration_ms=duration_ms,
            row_count=result.records,
            qc_metrics=qc_metrics,
            quality_report_path=quality_report_path,
            qc_metrics_path=qc_metrics_path,
        )

    except Exception as exc:  # noqa: BLE001
        end_time = datetime.now(timezone.utc)
        duration_ms = (end_time - start_time).total_seconds() * 1000
        error_msg = str(exc)

        logger.error(
            "Pipeline execution failed",
            pipeline=pipeline_name,
            error=error_msg,
            duration_ms=duration_ms,
            exc_info=True,
        )

        return PipelineRunResult(
            pipeline_name=pipeline_name,
            success=False,
            error=error_msg,
            duration_ms=duration_ms,
        )


def _aggregate_results(
    results: list[PipelineRunResult], run_id: str, start_time: datetime
) -> AggregatedResults:
    """Агрегировать результаты всех пайплайнов."""
    end_time = datetime.now(timezone.utc)
    total_duration_ms = sum(r.duration_ms for r in results)
    total_rows = sum(r.row_count for r in results)
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful

    pipeline_results_dict = {r.pipeline_name: r for r in results}

    summary = {
        "total_pipelines": len(results),
        "successful": successful,
        "failed": failed,
        "total_rows": total_rows,
        "total_duration_ms": total_duration_ms,
        "pipelines": {
            name: {
                "success": r.success,
                "row_count": r.row_count,
                "duration_ms": r.duration_ms,
                "error": r.error,
            }
            for name, r in pipeline_results_dict.items()
        },
    }

    return AggregatedResults(
        run_id=run_id,
        start_time=start_time,
        end_time=end_time,
        total_duration_ms=total_duration_ms,
        pipeline_results=pipeline_results_dict,
        summary=summary,
    )


def _write_summary_report(
    aggregated: AggregatedResults, output_root: Path
) -> Path:
    """Создать сводный отчёт в Markdown."""
    report_dir = output_root / "reports" / "chembl_all"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / "summary.md"

    lines = [
        "# ChEMBL All Pipelines Summary Report",
        "",
        f"**Run ID:** {aggregated.run_id}",
        f"**Start Time:** {aggregated.start_time.isoformat().replace('+00:00', 'Z')}",
        f"**End Time:** {aggregated.end_time.isoformat().replace('+00:00', 'Z') if aggregated.end_time else 'N/A'}",
        f"**Total Duration:** {aggregated.total_duration_ms:.2f} ms",
        "",
        "## Summary",
        "",
        f"- **Total Pipelines:** {aggregated.summary['total_pipelines']}",
        f"- **Successful:** {aggregated.summary['successful']}",
        f"- **Failed:** {aggregated.summary['failed']}",
        f"- **Total Rows:** {aggregated.summary['total_rows']:,}",
        "",
        "## Pipeline Results",
        "",
    ]

    for pipeline_name in CHEMBL_PIPELINES:
        result = aggregated.pipeline_results.get(pipeline_name)
        if result is None:
            lines.append(f"### {pipeline_name}")
            lines.append("- **Status:** Not executed")
            lines.append("")
            continue

        status = "[OK] Success" if result.success else "[ERROR] Failed"
        lines.append(f"### {pipeline_name}")
        lines.append(f"- **Status:** {status}")
        lines.append(f"- **Rows:** {result.row_count:,}")
        lines.append(f"- **Duration:** {result.duration_ms:.2f} ms")
        if result.error:
            lines.append(f"- **Error:** {result.error}")
        if result.run_result:
            lines.append(
                f"- **Dataset:** {result.run_result.write_result.dataset}"
            )
            lines.append(
                f"- **Run Directory:** {result.run_result.run_directory}"
            )
        lines.append("")

    content = "\n".join(lines)
    # Атомарная запись файла
    tmp_path = report_path.with_suffix(".tmp.md")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, report_path)

    return report_path


def _write_qc_json(aggregated: AggregatedResults, output_root: Path) -> Path:
    """Создать агрегированный QC JSON."""
    report_dir = output_root / "reports" / "chembl_all"
    report_dir.mkdir(parents=True, exist_ok=True)

    qc_path = report_dir / "qc.json"

    qc_data: dict[str, Any] = {
        "run_id": aggregated.run_id,
        "start_time": aggregated.start_time.isoformat().replace("+00:00", "Z"),
        "end_time": (
            aggregated.end_time.isoformat().replace("+00:00", "Z")
            if aggregated.end_time
            else None
        ),
        "total_duration_ms": aggregated.total_duration_ms,
        "summary": aggregated.summary,
        "pipeline_qc_metrics": {},
    }

    # Собираем QC-метрики из всех успешных пайплайнов
    for pipeline_name, result in aggregated.pipeline_results.items():
        if result.success and result.qc_metrics:
            qc_data["pipeline_qc_metrics"][pipeline_name] = result.qc_metrics

    # Атомарная запись JSON
    tmp_path = qc_path.with_suffix(".tmp.json")
    tmp_path.write_text(
        json.dumps(qc_data, indent=2, sort_keys=True), encoding="utf-8"
    )
    os.replace(tmp_path, qc_path)

    return qc_path


def run_chembl_all_command(
    output_root: Path = typer.Option(
        ...,
        "--output-root",
        "-o",
        help="Корневая директория для артефактов всех пайплайнов",
    ),
    configs_dir: Path = typer.Option(
        Path("configs"),
        "--configs-dir",
        help="Корневая директория конфигов",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        help="Ограничить количество строк для каждого пайплайна (для тестирования)",
        min=1,
    ),
    extended: bool = typer.Option(
        False,
        "--extended",
        help="Включить расширенные QC-артефакты",
    ),
    golden: Path | None = typer.Option(
        None,
        "--golden",
        help="Путь к golden-набору для сравнения (опционально)",
        exists=False,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Включить подробное логирование",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Проверить конфигурацию без выполнения",
    ),
) -> None:
    """Последовательно запустить все ChEMBL-пайплайны и собрать единый отчёт."""
    from uuid import uuid4

    run_id = str(uuid4())
    start_time = datetime.now(timezone.utc)

    # Настройка логирования
    log_level = "DEBUG" if verbose else "INFO"
    UnifiedLogger.configure(LoggerConfig(level=log_level))
    logger = UnifiedLogger.get(__name__)

    logger.info(
        "Starting ChEMBL all pipelines run",
        run_id=run_id,
        output_root=str(output_root),
        limit=limit,
        extended=extended,
    )

    # Создаём runner с общим run_id для всех пайплайнов
    # Используем общий run_id с суффиксами для каждого пайплайна
    base_run_id = run_id
    pipeline_run_ids: dict[str, str] = {}
    for idx, pipeline_name in enumerate(CHEMBL_PIPELINES):
        # Создаём уникальный run_id для каждого пайплайна на основе общего
        pipeline_run_ids[pipeline_name] = (
            f"{base_run_id}-{idx:02d}-{pipeline_name}"
        )

    # Фабрика для генерации run_id для каждого пайплайна
    def make_uuid_factory(pipeline_name: str) -> Callable[[], str]:
        pipeline_run_id = pipeline_run_ids[pipeline_name]

        def uuid_factory() -> str:
            return pipeline_run_id

        return uuid_factory

    config_factory = PipelineConfigFactory(
        environment_loader=load_environment_settings
    )

    results: list[PipelineRunResult] = []

    # Запускаем каждый пайплайн последовательно
    for pipeline_name in CHEMBL_PIPELINES:
        # Создаём runner с фабрикой run_id для этого пайплайна
        runner = PipelineCommandRunner(
            config_factory=config_factory,
            uuid_factory=make_uuid_factory(pipeline_name),
        )
        # Определяем конфиг и output_dir для каждого пайплайна
        config_factory_func = COMMAND_REGISTRY.get(pipeline_name)
        if config_factory_func is None:
            logger.warning(
                "Pipeline not found in registry", pipeline=pipeline_name
            )
            results.append(
                PipelineRunResult(
                    pipeline_name=pipeline_name,
                    success=False,
                    error=f"Pipeline '{pipeline_name}' not found in registry",
                )
            )
            continue

        try:
            command_config = config_factory_func()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to load pipeline config",
                pipeline=pipeline_name,
                error=str(exc),
            )
            results.append(
                PipelineRunResult(
                    pipeline_name=pipeline_name,
                    success=False,
                    error=f"Failed to load config: {exc}",
                )
            )
            continue

        # Определяем пути
        default_config = command_config.default_config_path
        if default_config is None:
            # Пытаемся найти конфиг по стандартному пути
            entity_name = pipeline_name.split("_")[0]
            default_config = (
                configs_dir
                / "pipelines"
                / entity_name
                / f"{pipeline_name}.yaml"
            )
            if not default_config.exists():
                # Пробуем альтернативный путь
                default_config = (
                    configs_dir
                    / "pipelines"
                    / entity_name
                    / f"{entity_name}_chembl.yaml"
                )

        # Output dir для каждого пайплайна
        entity_name = pipeline_name.split("_")[0]
        pipeline_output_dir = output_root / entity_name

        # Создаём опции для пайплайна
        options = PipelineCommandOptions(
            config_path=default_config,
            output_dir=pipeline_output_dir,
            dry_run=dry_run,
            verbose=verbose,
            set_overrides={},
            sample=None,
            limit=limit,
            extended=extended,
            fail_on_schema_drift=True,
            validate_columns=True,
            golden=golden,
            input_file=None,
        )

        # Запускаем пайплайн
        result = _run_single_pipeline(pipeline_name, options, runner, logger)
        results.append(result)

        # Если пайплайн упал и это критично, можно остановиться
        # (сейчас продолжаем выполнение остальных)

    # Агрегируем результаты
    aggregated = _aggregate_results(results, run_id, start_time)

    # Создаём отчёты
    summary_report = _write_summary_report(aggregated, output_root)
    qc_json = _write_qc_json(aggregated, output_root)

    logger.info(
        "ChEMBL all pipelines run completed",
        run_id=run_id,
        successful=aggregated.summary["successful"],
        failed=aggregated.summary["failed"],
        total_rows=aggregated.summary["total_rows"],
        summary_report=str(summary_report),
        qc_json=str(qc_json),
    )

    # Выводим итоги
    typer.echo("\n[OK] ChEMBL All Pipelines Run Complete")
    typer.echo(
        f"   Successful: {aggregated.summary['successful']}/{aggregated.summary['total_pipelines']}"
    )
    typer.echo(f"   Total Rows: {aggregated.summary['total_rows']:,}")
    typer.echo(f"   Summary Report: {summary_report}")
    typer.echo(f"   QC JSON: {qc_json}")

    # Выход с кодом ошибки, если были неудачи
    if aggregated.summary["failed"] > 0:
        typer.echo(
            f"\n[ERROR] {aggregated.summary['failed']} pipeline(s) failed"
        )
        raise typer.Exit(code=1)

    typer.echo("\n[OK] All pipelines completed successfully")
    raise typer.Exit(code=0)
