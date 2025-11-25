from __future__ import annotations

import argparse
from typing import Mapping

from bioetl.pipelines import PIPELINE_REGISTRY
from bioetl.pipelines.base import load_pipeline_config


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BioETL pipeline runner")
    parser.add_argument("pipeline", choices=sorted(PIPELINE_REGISTRY.keys()), help="Имя пайплайна")
    parser.add_argument("--run-id", required=True, help="Уникальный идентификатор запуска")
    parser.add_argument("--config", required=True, help="Путь к YAML конфигу")
    parser.add_argument("--output", help="Путь для вывода данных (override config)")
    parser.add_argument("--dry-run", action="store_true", help="Пропустить запись результата")
    parser.add_argument(
        "--strict-validation",
        action="store_true",
        help="Трактовать предупреждения валидации как ошибки",
    )
    return parser


def _resolve_pipeline_config(path: str, pipeline_name: str) -> Mapping[str, object]:
    return load_pipeline_config(path, pipeline_name)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    pipeline_name: str = args.pipeline
    if pipeline_name not in PIPELINE_REGISTRY:
        parser.exit(status=1, message=f"Неизвестный пайплайн: {pipeline_name}\n")

    config = _resolve_pipeline_config(args.config, pipeline_name)
    factory = PIPELINE_REGISTRY[pipeline_name]
    pipeline = factory(args.run_id, config, args.strict_validation)
    output_path = args.output or config.get("output_path") if isinstance(config, Mapping) else None

    try:
        pipeline.run(output_path=output_path, dry_run=args.dry_run)
        return 0
    except Exception as exc:  # pragma: no cover - CLI guard
        parser.exit(status=1, message=f"Pipeline failed: {exc}\n")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
