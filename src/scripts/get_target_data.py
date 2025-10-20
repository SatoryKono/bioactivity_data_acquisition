#!/usr/bin/env python3
"""
CLI скрипт для извлечения и обогащения target данных.

Этот скрипт предоставляет интерфейс командной строки для запуска
target ETL пайплайна с различными опциями конфигурации.

Использование:
    python -m library.scripts.get_target_data --help
    python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# Добавляем корневую директорию проекта в путь для импортов
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from library.target import (
    TargetConfig,
    load_target_config,
    TargetValidationError,
    TargetHTTPError,
    TargetQCError,
    TargetIOError,
    read_target_input,
    run_target_etl,
    write_target_outputs,
)
from library.logging_setup import configure_logging


def create_argument_parser() -> argparse.ArgumentParser:
    """Создать парсер аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="Extract and enrich target data from ChEMBL/UniProt/IUPHAR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with config file
  python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv

  # With custom output directory and date tag
  python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv --output-dir data/output/target --date-tag 20251020

  # Development mode (allows incomplete sources)
  python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv --dev-mode

  # Dry run (no output files)
  python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv --dry-run

  # With limit and custom timeout
  python -m library.scripts.get_target_data --config configs/config_target_full.yaml --input data/input/target_ids.csv --limit 100 --timeout-sec 120
        """,
    )

    # Required arguments
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        required=True,
        help="Path to the YAML configuration file",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="CSV file containing target_chembl_id column",
    )

    # Optional arguments
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for generated files (overrides config)",
    )
    parser.add_argument(
        "--date-tag",
        help="Date tag (YYYYMMDD) for output filenames (overrides config)",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        help="HTTP timeout in seconds (overrides config)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        help="Total retry attempts for HTTP requests (overrides config)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of targets to process (overrides config)",
    )

    # Development and testing options
    parser.add_argument(
        "--dev-mode",
        action="store_true",
        help="Enable development mode (allows incomplete sources for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing output files",
    )

    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file (default: stdout)",
    )
    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Console log format (default: text)",
    )

    return parser


def build_config_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """Построить словарь переопределений конфигурации из аргументов CLI."""
    overrides: dict[str, Any] = {}

    if args.output_dir is not None:
        overrides["io"] = overrides.get("io", {})
        overrides["io"]["output"] = overrides["io"].get("output", {})
        overrides["io"]["output"]["dir"] = str(args.output_dir)

    if args.date_tag is not None:
        overrides["runtime"] = overrides.get("runtime", {})
        overrides["runtime"]["date_tag"] = args.date_tag

    if args.timeout_sec is not None:
        overrides["http"] = overrides.get("http", {})
        overrides["http"]["global"] = overrides["http"].get("global", {})
        overrides["http"]["global"]["timeout_sec"] = args.timeout_sec

    if args.retries is not None:
        overrides["http"] = overrides.get("http", {})
        overrides["http"]["global"] = overrides["http"].get("global", {})
        overrides["http"]["global"]["retries"] = overrides["http"]["global"].get("retries", {})
        overrides["http"]["global"]["retries"]["total"] = args.retries

    if args.limit is not None:
        overrides["runtime"] = overrides.get("runtime", {})
        overrides["runtime"]["limit"] = args.limit

    if args.dev_mode:
        overrides["runtime"] = overrides.get("runtime", {})
        overrides["runtime"]["dev_mode"] = True
        overrides["runtime"]["allow_incomplete_sources"] = True

    if args.dry_run:
        overrides["runtime"] = overrides.get("runtime", {})
        overrides["runtime"]["dry_run"] = True

    return overrides


def main() -> int:
    """Основная функция CLI скрипта."""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Проверяем существование файлов
    if not args.config.exists():
        print(f"Error: Configuration file not found: {args.config}", file=sys.stderr)
        return 1

    if not args.input.exists():
        print(f"Error: Input CSV file not found: {args.input}", file=sys.stderr)
        return 1

    # Строим переопределения конфигурации
    overrides = build_config_overrides(args)

    try:
        # Загружаем конфигурацию
        config = load_target_config(args.config, overrides=overrides)
    except Exception as exc:
        print(f"Error loading configuration: {exc}", file=sys.stderr)
        return 1

    # Настраиваем логирование
    logger = configure_logging(
        level=args.log_level,
        file_enabled=args.log_file is not None,
        console_format=args.log_format,
        log_file=args.log_file,
        logging_config=config.logging.model_dump() if hasattr(config, "logging") else None,
    )

    logger.info("Target data extraction started", config=str(args.config), input_csv=str(args.input))

    try:
        # Читаем входные данные
        input_frame = read_target_input(args.input)
        logger.info(f"Loaded {len(input_frame)} target IDs from input file")
    except TargetValidationError as exc:
        logger.error(f"Input validation failed: {exc}")
        print(f"Error: Input validation failed: {exc}", file=sys.stderr)
        return 1
    except TargetIOError as exc:
        logger.error(f"Input I/O error: {exc}")
        print(f"Error: Input I/O error: {exc}", file=sys.stderr)
        return 1

    try:
        # Запускаем ETL пайплайн
        result = run_target_etl(config, input_frame=input_frame)
        logger.info(f"ETL completed successfully", records=len(result.targets))
    except TargetValidationError as exc:
        logger.error(f"ETL validation failed: {exc}")
        print(f"Error: ETL validation failed: {exc}", file=sys.stderr)
        return 1
    except TargetHTTPError as exc:
        logger.error(f"ETL HTTP error: {exc}")
        print(f"Error: ETL HTTP error: {exc}", file=sys.stderr)
        return 1
    except TargetQCError as exc:
        logger.error(f"ETL QC error: {exc}")
        print(f"Error: ETL QC error: {exc}", file=sys.stderr)
        return 1

    # Проверяем режим dry run
    if config.runtime.dry_run:
        logger.info("Dry run completed; no output files written")
        print("Dry run completed; no output files written")
        return 0

    try:
        # Записываем результаты
        outputs = write_target_outputs(
            result,
            config.io.output.dir,
            config.runtime.date_tag or "",
            config,
        )
        logger.info("Output files written successfully", outputs=list(outputs.keys()))
    except TargetIOError as exc:
        logger.error(f"Output I/O error: {exc}")
        print(f"Error: Output I/O error: {exc}", file=sys.stderr)
        return 1

    # Выводим информацию о созданных файлах
    print("\nGenerated files:")
    for name, path in outputs.items():
        print(f"  {name}: {path}")

    print(f"\nTarget data extraction completed successfully!")
    print(f"Processed {len(result.targets)} targets")
    print(f"QC metrics: {len(result.qc)} metrics generated")
    if result.correlation_analysis:
        print(f"Correlation analysis: {len(result.correlation_analysis)} analyses completed")

    return 0


if __name__ == "__main__":
    sys.exit(main())