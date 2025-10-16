# Bioactivity Data Acquisition

Модульный ETL-пайплайн для загрузки биоактивностных данных из внешних API (ChEMBL и др.), нормализации, валидации (Pandera) и детерминированного экспорта CSV, включая QC-отчёты и корреляционные матрицы. CLI основан на Typer; конфигурация — YAML + Pydantic.

## Назначение

Проект предназначен для:

- **Извлечения данных** из множественных API источников (ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar)
- **Валидации и нормализации** данных с помощью Pandera схем
- **Детерминированного экспорта** в CSV с контролем качества
- **Автоматической генерации** QC-отчётов и корреляционных матриц
- **Мониторинга** через OpenTelemetry и структурированное логирование

## Особенности нормализации данных

### Сохранение регистра в чувствительных полях

По умолчанию система **сохраняет исходный регистр** для всех строковых полей, что критично для:
- **SMILES** - химические формулы чувствительны к регистру (например, `CCO` ≠ `cco`)
- **Заголовки публикаций** - должны сохранять оригинальное форматирование
- **Названия белков** - часто содержат заглавные буквы (например, `ProteinA`, `EGFR`)

### Конфигурируемое приведение к нижнему регистру

Для полей, где регистр не важен, можно настроить селективное приведение к нижнему регистру через параметр `determinism.lowercase_columns`:

```yaml
determinism:
  lowercase_columns: ["source", "journal"]  # Только эти поля будут в нижнем регистре
```

Это позволяет:
- Сохранить химические формулы и научные названия в правильном регистре
- Нормализовать служебные поля (источники, названия журналов) для лучшей консистентности
- Обеспечить воспроизводимость результатов при сохранении данных

## Быстрый старт

### Установка

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

### Пример конфигурации

Создайте файл `config.example.yaml`:

```yaml
http:
  global:
    timeout_sec: 30.0
    retries:
      total: 5
      backoff_multiplier: 2.0
    headers:
      User-Agent: bioactivity-data-acquisition/0.1.0

sources:
  chembl:
    name: chembl
    enabled: true
    endpoint: document
    params:
      document_type: article
    pagination:
      page_param: page
      size_param: page_size
      size: 200
      max_pages: 10
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      timeout_sec: 60.0
      headers:
        Accept: application/json
        Authorization: "Bearer {CHEMBL_API_TOKEN}"

io:
  input:
    documents_csv: data/input/documents.csv
  output:
    data_path: data/output/documents.csv
    qc_report_path: data/output/documents_qc_report.csv
    correlation_path: data/output/documents_correlation.csv
    format: csv
    csv:
      encoding: utf-8
      float_format: "%.3f"
      date_format: "%Y-%m-%dT%H:%M:%SZ"

runtime:
  workers: 4
  limit: null
  dry_run: false
  date_tag: null

logging:
  level: INFO

validation:
  strict: true
  qc:
    max_missing_fraction: 0.05
    max_duplicate_fraction: 0.01

determinism:
  sort:
    by:
      - document_chembl_id
      - title
    ascending:
      - true
      - true
    na_position: last
  column_order:
    - document_chembl_id
    - title
    - doi
    - journal
    - volume
    - issue
    - first_page
    - last_page
    - document_citation
    - source
    - retrieved_at
  # Колонки для приведения к нижнему регистру (по умолчанию пустой - регистр сохраняется)
  lowercase_columns: []

postprocess:
  qc:
    enabled: true
  correlation:
    enabled: true
  journal_normalization:
    enabled: true
    columns: ["journal", "pubmed_journal", "chembl_journal", "crossref_journal"]
  citation_formatting:
    enabled: true
    columns:
      journal: "journal"
      volume: "volume"
      issue: "issue"
      first_page: "first_page"
      last_page: "last_page"
```

### Переменные окружения

Создайте файл `.env.example`:

```bash
# API ключи (опционально)
CHEMBL_API_TOKEN=your_chembl_token_here
PUBMED_API_KEY=your_pubmed_key_here
SEMANTIC_SCHOLAR_API_KEY=your_semantic_scholar_key_here
CROSSREF_API_KEY=your_crossref_key_here

# Глобальные переопределения конфигурации
BIOACTIVITY__LOGGING__LEVEL=INFO
BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=30
BIOACTIVITY__RUNTIME__WORKERS=4
BIOACTIVITY__RUNTIME__LIMIT=null
BIOACTIVITY__VALIDATION__STRICT=true
```

### Команды CLI

| Команда | Аргументы | Описание | Пример |
|---------|-----------|----------|---------|
| `pipeline` | `--config PATH` `--set KEY=VALUE` | Запуск основного ETL пайплайна | `bioactivity-data-acquisition pipeline --config configs/config.yaml` |
| `get-document-data` | `--config PATH` `--documents-csv PATH` `--output-dir PATH` `--date-tag YYYYMMDD` `--all` `--limit N` | Обогащение документов из API | `bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --all` |
| `version` | - | Показать версию пакета | `bioactivity-data-acquisition version` |
| `install-completion` | `SHELL` | Установить автодополнение для shell | `bioactivity-data-acquisition install-completion bash` |

### Основные команды

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

## Структура проекта

```text
bioactivity_data_acquisition/
├── 📁 configs/                    # Конфигурационные файлы
│   ├── config.yaml               # Основной конфиг ETL
│   ├── config_documents_full.yaml # Конфиг для документов
│   └── schema.json               # JSON Schema валидации
├── 📁 data/                      # Данные (входные/выходные)
│   ├── input/                    # Входные CSV файлы
│   └── output/                   # Результаты ETL
├── 📁 docs/                      # Документация (MkDocs)
├── 📁 src/library/               # Основной код
│   ├── 📁 cli/                   # CLI интерфейс (Typer)
│   ├── 📁 clients/               # HTTP клиенты для API
│   ├── 📁 etl/                   # ETL пайплайн
│   ├── 📁 schemas/               # Pandera схемы валидации
│   └── config.py                 # Управление конфигурацией
├── 📁 tests/                     # Тесты
├── 📁 .github/workflows/         # CI/CD
├── pyproject.toml                # Конфигурация проекта
├── Dockerfile                    # Multi-stage контейнер
└── docker-compose.yml            # Разработка
```

## Схемы и валидация

- Pandera-схемы для сырья и нормализованных данных: `docs/data_schemas.md`
- Схемы документов и инварианты: `docs/document_schemas.md`

## Выходные артефакты

- Детерминированные CSV/Parquet
- QC-отчёты (базовые/расширенные) и корреляционные отчёты

Подробнее: `docs/outputs.md`.

## Качество кода

### Инструменты качества

| Инструмент | Конфигурация | Описание |
|------------|--------------|----------|
| **pytest** | `pyproject.toml:55-68` | Тестирование с покрытием ≥90% |
| **mypy** | `pyproject.toml:92-105` | Статическая типизация |
| **ruff** | `pyproject.toml:79-90` | Линтинг и форматирование |
| **black** | `pyproject.toml:75-77` | Форматирование кода |
| **pre-commit** | `.pre-commit-config.yaml` | Git хуки |

### Запуск проверок

```bash
# Тесты с покрытием
pytest --cov=library --cov=tests --cov-report=term-missing --cov-fail-under=90

# Типизация
mypy src

# Линтинг
ruff check .
black --check .

# Pre-commit проверки
pre-commit run --all-files
```

Подробнее: `docs/quality.md`.

## CI/CD

### GitHub Actions

- **CI workflow** (`.github/workflows/ci.yaml`): тесты, линтеры, типизация
- **Docs workflow** (`.github/workflows/docs.yml`): сборка и деплой документации

### Триггеры

- `push` в ветки `main`, `work`
- `pull_request`
- `workflow_dispatch` (ручной запуск)

### Проверки

1. Установка зависимостей: `pip install .[dev]`
2. Smoke тест конфигурации
3. Ruff линтинг
4. Black форматирование
5. MyPy типизация
6. Pytest с покрытием ≥75%

Подробнее: `docs/ci.md`.

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

## Rate Limiting (Ограничение скорости запросов)

Для предотвращения получения ошибок 429 (Too Many Requests) от API источников, система поддерживает настройку ограничения скорости запросов. Это особенно важно для источников без API ключей (Semantic Scholar, OpenAlex), которые имеют строгие лимиты.

### Поддерживаемые форматы конфигурации

#### 1. Формат `max_calls`/`period`
```yaml
sources:
  semantic_scholar:
    enabled: true
    rate_limit:
      max_calls: 1      # Количество запросов
      period: 60.0      # Период в секундах
```

#### 2. Формат `requests_per_second`
```yaml
sources:
  semantic_scholar:
    enabled: true
    rate_limit:
      requests_per_second: 0.5  # 1 запрос каждые 2 секунды
```

### Рекомендуемые настройки для источников без API ключей

```yaml
sources:
  semantic_scholar:
    enabled: true
    rate_limit:
      requests_per_second: 0.5  # 1 запрос каждые 2 секунды (очень консервативно)
  
  openalex:
    enabled: true
    rate_limit:
      requests_per_second: 1.0  # 1 запрос в секунду
```

### Источники с API ключами

Для источников с API ключами можно использовать более агрессивные настройки:

```yaml
sources:
  chembl:
    enabled: true
    rate_limit:
      requests_per_second: 10.0  # 10 запросов в секунду
  
  crossref:
    enabled: true
    rate_limit:
      max_calls: 50
      period: 1.0  # 50 запросов в секунду
```

### Важные замечания

1. **Semantic Scholar** без API ключа имеет очень строгие ограничения (1 запрос в минуту)
2. **OpenAlex** без API ключа позволяет до 1 запроса в секунду
3. **ChEMBL** и **Crossref** с API ключами имеют более высокие лимиты
4. Система автоматически конвертирует `requests_per_second` в формат `max_calls`/`period`
5. При получении ошибки 429 система логирует предупреждение и продолжает работу

## Лицензия и вклад

Правила контрибьюшенов: `docs/contributing.md`. Изменения: `docs/changelog.md`.
