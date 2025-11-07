# Контекст проекта BioETL

## Описание проекта

BioETL - это унифицированная ETL система для извлечения и обработки данных биоактивности из различных источников (ChEMBL, UniProt, PubChem).

## Ключевые принципы

- **Детерминизм**: одинаковый вход → одинаковый выход
- **Типизация**: строгая типизация с mypy --strict
- **Валидация**: Pandera схемы для всех таблиц
- **Качество**: QC отчеты и sidecar метаданные
- **Воспроизводимость**: атомарные операции, версионирование

## Архитектура

- **Звездная схема**: documents_dim, targets_dim, assays_dim, testitems_dim, activity_fact
- **Унифицированные компоненты**: UnifiedLogger, UnifiedOutputWriter, UnifiedAPIClient
- **Модульные пайплайны**: каждый источник данных имеет свой пайплайн

## Технологический стек

- Python 3.10+
- pandas, pandera, structlog, typer, pydantic
- ruff, black, isort для форматирования
- pytest для тестирования
- mypy для типизации

## Структура данных

- **Входные данные**: `data/input/`
- **Выходные данные**: `data/output/`
- **Кэш**: `data/cache/`
- **Логи**: `logs/`

## Ключевые идентификаторы

- ChEMBL ID (CHEMBL1234567)
- UniProt Accession (P12345)
- PubChem CID (12345678)
- DOI (10.1000/182)

## Контроль качества

- QC отчеты с распределениями полей
- Sidecar meta.yaml с метаданными
- Детерминированная дедупликация
- Валидация схем Pandera

## Команды

- `/run_assay` - запуск assay pipeline
- `/run_activity` - запуск activity pipeline
- `/run_document` - запуск document pipeline
- `/run_target` - запуск target pipeline
- `/run_testitem` - запуск testitem pipeline
- `/validate_columns` - валидация колонок
- `/validate_all_columns` - валидация всех колонок
