from __future__ import annotations

import argparse
from typing import Callable

from bioetl.core.pipeline.base import PipelineBase


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BioETL pipeline runner")
    parser.add_argument("--output", help="Путь для вывода данных", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Пропустить запись результата")
    parser.add_argument(
        "--strict-validation",
        action="store_true",
        help="Трактовать предупреждения валидации как ошибки",
    )
    return parser


def run_cli(pipeline_factory: Callable[[bool], PipelineBase], argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        pipeline = pipeline_factory(args.strict_validation)
        pipeline.run(output_path=args.output, dry_run=args.dry_run)
        return 0
    except Exception as exc:  # pragma: no cover - CLI guard
        parser.exit(status=1, message=f"Pipeline failed: {exc}\n")
