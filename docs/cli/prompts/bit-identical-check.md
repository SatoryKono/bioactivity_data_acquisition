# Промпт: Добавить автоматическую проверку бит-идентичности (P1-5)

## Контекст задачи

**Проблема:** Отсутствует автоматическая проверка бит-идентичности output артефактов согласно ACCEPTANCE_CRITERIA.md раздел C.

**Статус:** ❌ **НЕ ИСПРАВЛЕНО** (P1-5)

**Ссылки:**

- `docs/architecture/refactoring/ACCEPTANCE_CRITERIA.md` (строка 17): "Два подряд запуска одного пайплайна на одинаковом входе формируют бит-идентичные артефакты (CSV/Parquet/JSON) при одинаковых конфигурациях окружения"
- `docs/architecture/refactoring/ACCEPTANCE_CRITERIA.md` (строка 57): "Golden-снимки обновлены детерминированно единым helper'ом"
- `docs/architecture/refactoring/AUDIT_REPORT_2025.md` (строка 993): "Golden-тесты есть, но нет автоматической проверки бит-идентичности"

## Требования из ACCEPTANCE_CRITERIA.md

**Раздел C. Детерминизм и идемпотентность:**
> "Два подряд запуска одного пайплайна на одинаковом входе формируют бит-идентичные артефакты (CSV/Parquet/JSON) при одинаковых конфигурациях окружения; допускается отличие только поля времени в `meta.yaml`."

> "Все артефакты пишутся атомарно: через временный файл на той же ФС с последующей атомарной заменой; при сбое частичных файлов не остаётся."

> "Репродуцируемость зафиксирована: стабильный порядок строк и столбцов, детерминированные сериализаторы, исключение недетерминированных источников (случайных сидов, нефиксированных локалей)."

**Раздел J. Тестовый контур:**
> "Golden-снимки обновлены детерминированно единым helper'ом; изменения артефактов допускаются только с явным ревью и причинами в CHANGELOG."

## Существующие примеры

### 1. Golden тесты CLI

**Файл:** `tests/golden/test_cli_golden.py`

**Пример проверки хэшей:**

```python
def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

@pytest.mark.integration
@pytest.mark.golden
def test_cli_run_matches_expected_csv_hash(tmp_path: Path) -> None:
    """Full CLI execution should materialise a dataset matching the golden hash."""
    # Запуск пайплайна
    actual_hash = _sha256(actual_path)
    expected_hash = _sha256(expected_path)
    assert actual_hash == expected_hash
```

### 2. Проверка checksums в metadata

**Файл:** `tests/unit/test_output_writer.py` (строки 231-232)

```python
expected_checksums = {
    artifacts.dataset.name: hashlib.sha256(artifacts.dataset.read_bytes()).hexdigest(),
}
assert metadata["file_checksums"] == expected_checksums
```

### 3. Расширенные артефакты

**Файл:** `tests/integration/pipelines/test_extended_mode_outputs.py`

Проверяет генерацию metadata, QC отчетов, но не проверяет бит-идентичность между запусками.

## Что должно быть реализовано

### 1. Автоматическая проверка бит-идентичности

**Требования:**

- Тест запускает пайплайн дважды с одинаковым входом и конфигурацией
- Сравнивает output файлы (CSV/Parquet/JSON) бит-в-бит
- Проверяет что хэши файлов идентичны (SHA256)
- Исключает из сравнения поля времени в `meta.yaml` (если отличаются)

**Местоположение:** `tests/integration/pipelines/test_bit_identical_output.py` или `tests/integration/test_determinism.py`

### 2. Golden файлы helper

**Требования:**

- Единый helper для обновления golden файлов
- Детерминированная генерация golden файлов
- Валидация что golden файлы соответствуют ожидаемым хэшам
- Механизм обновления golden файлов с явным подтверждением

**Местоположение:** `tests/golden/helpers.py` или `tests/utils/golden.py`

### 3. Интеграция с существующими тестами

**Расширить существующие golden тесты:**

- `tests/golden/test_cli_golden.py` — добавить проверку бит-идентичности между запусками
- `tests/integration/pipelines/test_extended_mode_outputs.py` — добавить проверку детерминизма артефактов

## Структура реализации

### Файл 1: `tests/integration/pipelines/test_bit_identical_output.py`

```python
"""Integration tests for bit-identical output verification."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.pipelines.base import PipelineBase

pytestmark = pytest.mark.integration

def _file_sha256(path: Path) -> str:
    """Calculate SHA256 hash of file contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()

def _compare_files_bitwise(path1: Path, path2: Path) -> bool:
    """Compare two files byte-by-byte."""
    return path1.read_bytes() == path2.read_bytes()

@pytest.mark.determinism
def test_pipeline_output_is_bit_identical_on_repeated_runs(
    tmp_path: Path,
    pipeline_class: type[PipelineBase],
    config_path: Path,
    input_file: Path | None = None,
) -> None:
    """Two consecutive runs with same input should produce bit-identical outputs."""

    config = load_config(config_path)

    # Первый запуск
    run1_dir = tmp_path / "run1"
    run1_dir.mkdir()
    pipeline1 = pipeline_class(config, "run1")
    artifacts1 = pipeline1.run(run1_dir / "output.csv", input_file=input_file)

    # Второй запуск
    run2_dir = tmp_path / "run2"
    run2_dir.mkdir()
    pipeline2 = pipeline_class(config, "run2")
    artifacts2 = pipeline2.run(run2_dir / "output.csv", input_file=input_file)

    # Проверка бит-идентичности основных файлов
    assert _compare_files_bitwise(artifacts1.dataset, artifacts2.dataset), \
        "Dataset files must be bit-identical"

    # Проверка хэшей
    hash1 = _file_sha256(artifacts1.dataset)
    hash2 = _file_sha256(artifacts2.dataset)
    assert hash1 == hash2, f"Dataset SHA256 mismatch: {hash1} != {hash2}"

    # Проверка quality_report (если есть)
    if artifacts1.quality_report and artifacts2.quality_report:
        assert _compare_files_bitwise(
            artifacts1.quality_report,
            artifacts2.quality_report
        ), "Quality report files must be bit-identical"

    # Проверка metadata (исключая поля времени)
    # См. helper для нормализации meta.yaml
```

### Файл 2: `tests/golden/helpers.py`

```python
"""Helpers for golden file management and bit-identical verification."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import yaml

def calculate_file_hash(path: Path, algorithm: str = "sha256") -> str:
    """Calculate hash of file contents.

    Args:
        path: Path to file
        algorithm: Hash algorithm (sha256, md5, etc.)

    Returns:
        Hex digest of file hash
    """
    hasher = hashlib.new(algorithm)
    hasher.update(path.read_bytes())
    return hasher.hexdigest()

def compare_files_bitwise(path1: Path, path2: Path) -> tuple[bool, str | None]:
    """Compare two files byte-by-byte.

    Returns:
        Tuple of (are_identical, error_message)
    """
    bytes1 = path1.read_bytes()
    bytes2 = path2.read_bytes()

    if bytes1 == bytes2:
        return True, None

    # Найти первое различие
    min_len = min(len(bytes1), len(bytes2))
    for i in range(min_len):
        if bytes1[i] != bytes2[i]:
            return False, f"Files differ at byte offset {i}: 0x{bytes1[i]:02x} != 0x{bytes2[i]:02x}"

    if len(bytes1) != len(bytes2):
        return False, f"Files differ in length: {len(bytes1)} != {len(bytes2)}"

    return False, "Unknown difference"

def normalize_meta_yaml_for_comparison(path: Path) -> dict[str, Any]:
    """Load meta.yaml and normalize time fields for comparison.

    Removes or normalizes fields that may differ between runs:

    - generated_at
    - extracted_at (если отличается только время)
    - run_id (может отличаться)

    Args:
        path: Path to meta.yaml file

    Returns:
        Normalized metadata dict
    """
    with path.open(encoding="utf-8") as f:
        meta = yaml.safe_load(f)

    # Удалить поля времени для сравнения
    meta_normalized = meta.copy()
    meta_normalized.pop("generated_at", None)

    # Нормализовать extracted_at (если нужно, оставить только дату)
    if "extracted_at" in meta_normalized:
        # Можно оставить только дату или удалить полностью
        pass

    # Удалить run_id если он разный
    meta_normalized.pop("run_id", None)

    return meta_normalized

def compare_meta_yaml(path1: Path, path2: Path) -> tuple[bool, str | None]:
    """Compare two meta.yaml files excluding time fields.

    Returns:
        Tuple of (are_identical, error_message)
    """
    meta1 = normalize_meta_yaml_for_comparison(path1)
    meta2 = normalize_meta_yaml_for_comparison(path2)

    if meta1 == meta2:
        return True, None

    # Найти различия
    keys1 = set(meta1.keys())
    keys2 = set(meta2.keys())

    missing_in_2 = keys1 - keys2
    missing_in_1 = keys2 - keys1
    different_values = {
        k: (meta1[k], meta2[k])
        for k in keys1 & keys2
        if meta1[k] != meta2[k]
    }

    errors = []
    if missing_in_2:
        errors.append(f"Keys missing in file2: {missing_in_2}")
    if missing_in_1:
        errors.append(f"Keys missing in file1: {missing_in_1}")
    if different_values:
        errors.append(f"Different values: {different_values}")

    return False, "; ".join(errors)

def verify_bit_identical_outputs(
    artifacts1: Any,  # OutputArtifacts
    artifacts2: Any,  # OutputArtifacts
    ignore_meta_time: bool = True,
) -> tuple[bool, list[str]]:
    """Verify that two pipeline outputs are bit-identical.

    Args:
        artifacts1: First run artifacts
        artifacts2: Second run artifacts
        ignore_meta_time: Whether to ignore time fields in meta.yaml

    Returns:
        Tuple of (are_identical, list_of_errors)
    """
    errors = []

    # Проверка dataset
    identical, error = compare_files_bitwise(artifacts1.dataset, artifacts2.dataset)
    if not identical:
        errors.append(f"Dataset mismatch: {error}")

    # Проверка quality_report (если есть)
    if artifacts1.quality_report and artifacts2.quality_report:
        identical, error = compare_files_bitwise(
            artifacts1.quality_report,
            artifacts2.quality_report,
        )
        if not identical:
            errors.append(f"Quality report mismatch: {error}")

    # Проверка metadata
    if artifacts1.metadata and artifacts2.metadata:
        if ignore_meta_time:
            identical, error = compare_meta_yaml(artifacts1.metadata, artifacts2.metadata)
        else:
            identical, error = compare_files_bitwise(artifacts1.metadata, artifacts2.metadata)

        if not identical:
            errors.append(f"Metadata mismatch: {error}")

    # Проверка correlation_report (если есть)
    if hasattr(artifacts1, "correlation_report") and artifacts1.correlation_report:
        if hasattr(artifacts2, "correlation_report") and artifacts2.correlation_report:
            identical, error = compare_files_bitwise(
                artifacts1.correlation_report,
                artifacts2.correlation_report,
            )
            if not identical:
                errors.append(f"Correlation report mismatch: {error}")

    return len(errors) == 0, errors

def update_golden_file(
    actual_path: Path,
    golden_path: Path,
    force: bool = False,
) -> bool:
    """Update golden file with actual output.

    Args:
        actual_path: Path to actual output file
        golden_path: Path to golden reference file
        force: Force update even if files differ

    Returns:
        True if updated, False if skipped
    """
    if not force:
        if golden_path.exists():
            identical, _ = compare_files_bitwise(actual_path, golden_path)
            if identical:
                return False  # Файлы идентичны, обновление не нужно

    # Создать директорию если нужно
    golden_path.parent.mkdir(parents=True, exist_ok=True)

    # Скопировать файл
    import shutil
    shutil.copy2(actual_path, golden_path)

    # Записать хэш в отдельный файл
    hash_path = golden_path.with_suffix(golden_path.suffix + ".sha256")
    hash_value = calculate_file_hash(golden_path)
    hash_path.write_text(f"{hash_value}  {golden_path.name}\n", encoding="utf-8")

    return True
```

### Файл 3: Тесты для конкретных пайплайнов

**Местоположение:** `tests/integration/pipelines/test_<entity>_determinism.py`

**Пример для activity:**

```python
"""Determinism tests for activity pipeline."""

import pytest
from pathlib import Path

from tests.golden.helpers import verify_bit_identical_outputs

@pytest.mark.integration
@pytest.mark.determinism
def test_activity_pipeline_bit_identical_output(tmp_path, activity_config):
    """Activity pipeline should produce bit-identical outputs on repeated runs."""

    from bioetl.pipelines.activity import ActivityPipeline

    # Первый запуск
    run1_dir = tmp_path / "run1"
    pipeline1 = ActivityPipeline(activity_config, "run1")
    artifacts1 = pipeline1.run(run1_dir / "activity.csv")

    # Второй запуск (тот же конфиг, тот же input)
    run2_dir = tmp_path / "run2"
    pipeline2 = ActivityPipeline(activity_config, "run2")
    artifacts2 = pipeline2.run(run2_dir / "activity.csv")

    # Проверка бит-идентичности
    identical, errors = verify_bit_identical_outputs(artifacts1, artifacts2)

    assert identical, f"Outputs not bit-identical: {'; '.join(errors)}"
```

## Требования к реализации

### 1. Структура файлов

```
tests/
├── golden/
│   ├── __init__.py
│   ├── helpers.py                    # Helper функции для golden файлов
│   └── test_cli_golden.py            # Расширить существующий
├── integration/
│   └── pipelines/
│       ├── test_bit_identical_output.py        # Общий тест бит-идентичности
│       ├── test_activity_determinism.py        # Тест для activity
│       ├── test_assay_determinism.py           # Тест для assay
│       ├── test_document_determinism.py         # Тест для document
│       ├── test_target_determinism.py           # Тест для target
│       └── test_testitem_determinism.py         # Тест для testitem
```

### 2. Маркеры pytest

**Добавить в `pytest.ini` или `pyproject.toml`:**

```ini
[pytest]
markers =
    determinism: tests for bit-identical output verification
    golden: golden file tests
```

### 3. Фикстуры

**Добавить в `tests/conftest.py`:**

```python
@pytest.fixture
def frozen_time(monkeypatch):
    """Freeze time for deterministic tests."""
    from datetime import datetime, timezone
    frozen = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("bioetl.utils.datetime.datetime.now", lambda: frozen)
    monkeypatch.setattr("bioetl.core.output_writer.datetime.now", lambda: frozen)
    return frozen
```

### 4. Интеграция с CI

**Добавить в CI workflow:**

- Запуск determinism тестов в отдельной job или после основных тестов
- Проверка что golden файлы не изменяются без явного обновления
- Автоматическое обновление golden файлов только через специальный флаг

### 5. Документация

**Обновить:**

- `docs/TESTING.md` — добавить раздел о бит-идентичности
- `README.md` — упомянуть determinism тесты
- `CHANGELOG.md` — при обновлении golden файлов

## Критерии завершения

- ✅ Создан helper модуль `tests/golden/helpers.py` с функциями для проверки бит-идентичности
- ✅ Создан интеграционный тест `test_bit_identical_output.py` с параметризацией по пайплайнам
- ✅ Добавлены determinism тесты для всех основных пайплайнов (activity, assay, document, target, testitem)
- ✅ Тесты проверяют бит-идентичность CSV/Parquet/JSON файлов
- ✅ Тесты корректно обрабатывают различия во времени в `meta.yaml`
- ✅ Golden файлы можно обновлять через helper функцию
- ✅ Все тесты проходят в CI
- ✅ Документация обновлена

## Приоритеты реализации

**Приоритет 1:**

- Helper функции для проверки бит-идентичности
- Общий интеграционный тест
- Тесты для activity и assay пайплайнов

**Приоритет 2:**

- Тесты для document, target, testitem пайплайнов
- Интеграция с существующими golden тестами

**Приоритет 3:**

- Механизм автоматического обновления golden файлов
- Документация и примеры использования

## Примечания

- При проверке `meta.yaml` исключать поля времени (`generated_at`, `extracted_at` если отличается только время)
- Использовать фиксированное время в тестах через monkeypatch для детерминизма
- Проверять все артефакты: dataset, quality_report, metadata, correlation_report (если есть)
- Использовать SHA256 для проверки целостности файлов
- Golden файлы должны храниться в `tests/golden/` или `tests/integration/pipelines/golden/`
