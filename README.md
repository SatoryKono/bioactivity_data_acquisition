# Bioactivity Data Acquisition

Модульный ETL-пайплайн для загрузки биоактивностных данных из внешних API (ChEMBL и др.), нормализации, валидации (Pandera) и детерминированного экспорта CSV, включая QC-отчёты и корреляционные матрицы. CLI основан на Typer; конфигурация — YAML + Pydantic.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install .[dev]
```

Альтернатива для быстрого запуска (без dev-инструментов):

```bash
pip install .
```

## Быстрый старт

```bash
# Проверка установки и доступных команд
bioactivity-data-acquisition --help

# Установка автодополнения для bash/zsh (опционально)
bioactivity-data-acquisition install-completion bash

# Запуск основного пайплайна по конфигу
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set http.global.timeout_sec=45

# Обогащение данных документов из разных источников
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --documents-csv data/input/documents.csv \
  --output-dir data/output/full \
  --date-tag 20250101 --all --limit 100
```

Полный справочник флагов и рецептов: см. `docs/cli.md`.

## Конфигурация

- Базовый файл: `configs/config.yaml`
- Приоритеты: дефолты в коде < YAML < ENV `BIOACTIVITY__*` < CLI `--set key=value`
- Секреты задаются только через переменные окружения и подставляются в плейсхолдеры вида `{CHEMBL_API_TOKEN}`

Подробнее: `docs/configuration.md`.

## Схемы и валидация

- Pandera-схемы для сырья и нормализованных данных: `docs/data_schemas.md`
- Схемы документов и инварианты: `docs/document_schemas.md`

## Выходные артефакты

- Детерминированные CSV/Parquet
- QC-отчёты (базовые/расширенные) и корреляционные отчёты

Подробнее: `docs/outputs.md`.

## Качество и CI

- Тесты, типы и линтеры: `docs/quality.md`
- CI workflow и триггеры: `docs/ci.md`

## Документация

Локальный предпросмотр:

```bash
pip install -r requirements.txt  # при необходимости
mkdocs serve
```

Полный портал: см. материалы в `docs/` и навигацию `mkdocs.yml`.

## Лицензия и вклад

Правила контрибьюшенов: `docs/contributing.md`. Изменения: `docs/changelog.md`.


