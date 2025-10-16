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

### Интеграционные тесты

Интеграционные тесты требуют сетевого доступа и могут потребовать API ключи:

```bash
# Установка API ключей (опционально)
export CHEMBL_API_TOKEN="your_chembl_token"
export PUBMED_API_KEY="your_pubmed_key"
export SEMANTIC_SCHOLAR_API_KEY="your_semantic_scholar_key"

# Запуск интеграционных тестов
pytest tests/integration/ --run-integration -v

# Запуск медленных тестов
pytest -m slow

# Пропуск медленных тестов
pytest -m "not slow"
```

Интеграционные тесты включают:
- **ChEMBL API**: Поиск соединений и активностей, ограничение скорости, обработка ошибок
- **Pipeline**: Сквозная обработка документов с реальными данными
- **API Limits**: Ограничение скорости, параллельный доступ, обработка таймаутов

Подробнее: `tests/integration/README.md`

## Документация

Локальный предпросмотр:

```bash
pip install -r requirements.txt  # при необходимости
mkdocs serve
```

Сайт документации: <https://satorykono.github.io/bioactivity_data_acquisition/>

## Лицензия и вклад

Правила контрибьюшенов: `docs/contributing.md`. Изменения: `docs/changelog.md`.
