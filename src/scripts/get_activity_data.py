"""CLI для запуска пайплайна activity по образцу documents.

Поддерживает аргументы:
  --config <path>     Путь к YAML конфигу (по умолчанию configs/config_activity_full.yaml)
  --input <path>      Путь к входному CSV (по умолчанию из конфига: io.input.activity_csv)
  --limit <int>       Ограничение числа строк входа (срезается перед запуском)
  --date-tag <YYYYMMDD>  Тег даты для имен выходных файлов (по умолчанию из конфига или сегодня)
"""

from __future__ import annotations

import argparse
import datetime as dt
from datetime import timezone
import os
import sys
from pathlib import Path

# Setup path for library imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
# Always insert src_dir at the beginning to ensure it's found first
sys.path.insert(0, src_dir)

import pandas as pd  # noqa: E402
from library.activity import (  # noqa: E402
    load_activity_config,
    read_activity_input,
    run_activity_etl,
    write_activity_outputs,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run activity ETL pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default=str(Path("configs") / "config_activity_full.yaml"),
        help="Path to activity YAML config",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to input activity CSV (overrides config io.input.activity_csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of input rows before processing",
    )
    parser.add_argument(
        "--date-tag",
        type=str,
        default=None,
        help="Date tag in YYYYMMDD format for output files",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Загружаем конфиг с возможными оверрайдами лимита
    overrides: dict[str, object] = {}
    if args.limit is not None:
        overrides = {"runtime": {"limit": int(args.limit)}}
    cfg = load_activity_config(args.config, overrides=overrides)

    # Определяем входной CSV
    input_path = Path(args.input) if args.input else Path(cfg.io.input.activity_csv)

    # Читаем вход
    df = read_activity_input(input_path)
    if args.limit is not None:
        df = df.head(int(args.limit))

    # Запускаем ETL
    result = run_activity_etl(cfg, df)

    # Определяем date_tag
    date_tag = (
        str(args.date_tag)
        if args.date_tag
        else (cfg.runtime.date_tag or dt.datetime.now(timezone.utc).strftime("%Y%m%d"))
    )

    # Записываем выводы
    output_dir = Path(cfg.io.output.dir)
    paths = write_activity_outputs(result, output_dir, date_tag=date_tag, config=cfg)

    # Печатаем пути для интеграции в CI
    print({k: str(v) for k, v in paths.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
