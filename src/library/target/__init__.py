"""Target ETL pipeline components."""

from __future__ import annotations

from library.target.config import (
    ConfigLoadError,
    TargetConfig,
    load_target_config,
)
from library.target.normalize import TargetNormalizer
from library.target.pipeline import TargetPipeline
from library.target.quality import TargetQualityFilter
from library.target.validate import TargetValidator
from library.target.writer import write_target_outputs


# Исключения
class TargetValidationError(Exception):
    """Ошибка валидации данных target."""
    pass

class TargetHTTPError(Exception):
    """Ошибка HTTP запросов для target."""
    pass

class TargetIOError(Exception):
    """Ошибка ввода-вывода для target."""
    pass

class TargetQCError(Exception):
    """Ошибка контроля качества для target."""
    pass

# Функции для совместимости с существующими скриптами
def read_target_input(input_path):
    """Читает входные данные target из CSV файла."""
    import pandas as pd
    return pd.read_csv(input_path)

def run_target_etl(config, input_data=None):
    """Запускает ETL пайплайн для target данных."""
    pipeline = TargetPipeline(config)
    return pipeline.run(input_data)

__all__ = [
    "ConfigLoadError",
    "TargetConfig",
    "TargetNormalizer",
    "TargetPipeline",
    "TargetQualityFilter",
    "TargetValidator",
    "TargetValidationError",
    "TargetHTTPError",
    "TargetIOError",
    "TargetQCError",
    "load_target_config",
    "write_target_outputs",
    "read_target_input",
    "run_target_etl",
]
