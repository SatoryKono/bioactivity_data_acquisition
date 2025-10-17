# Постпроцессинг документов

## Обзор

Постпроцессинг документов — это финальный этап пайплайна обработки данных, который выполняется после извлечения и нормализации метаданных из внешних источников (ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar). Цель постпроцессинга — создание детерминированного, валидированного и готового к использованию набора данных документов.

### Входные источники

- **raw.documents** — исходные данные из ChEMBL CSV
- **raw.crossref** — метаданные из Crossref API
- **raw.openalex** — метаданные из OpenAlex API  
- **raw.pubmed** — метаданные из PubMed API
- **raw.semantic_scholar** — метаданные из Semantic Scholar API

### Ожидаемые артефакты

- `documents.csv` — основной файл с обработанными данными
- `documents.parquet` — эталонный формат с сохранением типов данных
- `documents_meta.json` — метаданные о процессе обработки
- `documents_quality_report.csv` — отчет о качестве данных
- `documents_correlation_report.csv` — корреляционный анализ

## Схема данных

### Входные поля

| Поле исхода | Правило/источник | Тип | Примечание |
|-------------|------------------|-----|------------|
| document_chembl_id | ChEMBL CSV | str | Обязательный ключ |
| title | ChEMBL CSV | str | Обязательное поле |
| doi | ChEMBL CSV | str | Опциональный DOI |
| document_pubmed_id | ChEMBL CSV | str | Опциональный PMID |
| chembl_* | ChEMBL API | mixed | Метаданные из ChEMBL |
| crossref_* | Crossref API | mixed | Метаданные из Crossref |
| openalex_* | OpenAlex API | mixed | Метаданные из OpenAlex |
| pubmed_* | PubMed API | mixed | Метаданные из PubMed |
| semantic_scholar_* | Semantic Scholar API | mixed | Метаданные из Semantic Scholar |

### Выходные поля

| Поле результата | Правило нормализации | Тип | Примечание |
|-----------------|---------------------|-----|------------|
| document_id | document_chembl_id | str | Первичный ключ |
| doi_key | normalize_doi_advanced() | str | Нормализованный DOI |
| pmid_key | str.strip().lower() | str | Нормализованный PMID |
| journal_normalized | normalize_journal_name() | str | Нормализованное название журнала |
| year_str | str(year) | str | Строковое представление года |
| title_norm | str.strip().lower() | str | Нормализованный заголовок |
| document_citation | format_citation() | str | Форматированная ссылка |
| valid_* | validate_*_fields() | mixed | Валидированные поля |
| invalid_* | validate_*_fields() | bool | Флаги невалидности |

### Валидация через Pandera

Схемы валидации определены в:
- `src/library/schemas/document_input_schema.py` — входные данные
- `src/library/schemas/document_output_schema.py` — выходные данные

```python
from library.schemas.document_output_schema import DocumentOutputSchema
import pandera as pa

# Валидация выходных данных
schema = DocumentOutputSchema()
validated_df = schema.validate(df)
```

## Шаги постпроцессинга

### P01_normalize_keys

**Цель:** Нормализация ключей DOI и PMID для последующей дедупликации.

**Входные наборы/колонки:**
- `document_chembl_id` — идентификатор документа в ChEMBL
- `doi` — Digital Object Identifier
- `document_pubmed_id` — PubMed идентификатор

**Трансформации:**
```python
# Нормализация DOI
doi_key = normalize_doi_advanced(doi)
# Удаляет префиксы https://doi.org/, приводит к нижнему регистру, обрезает пробелы

# Нормализация PMID  
pmid_key = str(document_pubmed_id).strip().lower() if document_pubmed_id else None
```

**Выход и инварианты:**
- `doi_key` — нормализованный DOI или None
- `pmid_key` — нормализованный PMID или None
- Инвариант: если DOI/PMID пустой, ключ = None

**Логирование:**
- `row_count_in/out` — количество обработанных строк
- `doi_normalized_count` — количество нормализованных DOI
- `pmid_normalized_count` — количество нормализованных PMID

### P02_merge_bibliography

**Цель:** Детерминированный merge метаданных из внешних источников с приоритизацией полей.

**Входные наборы/колонки:**
- Все поля с префиксами `chembl_*`, `crossref_*`, `openalex_*`, `pubmed_*`, `semantic_scholar_*`

**Трансформации:**
```python
# Приоритет источников (от высшего к низшему)
priority_sources = ["chembl", "pubmed", "crossref", "openalex", "semantic_scholar"]

# Детерминированный выбор значения
def merge_field(field_name: str, row: dict) -> Any:
    for source in priority_sources:
        source_field = f"{source}_{field_name}"
        if source_field in row and not pd.isna(row[source_field]):
            return row[source_field]
    return None
```

**Выход и инварианты:**
- Объединенные поля с приоритетом ChEMBL > PubMed > Crossref > OpenAlex > Semantic Scholar
- Инвариант: каждое поле имеет максимум одно непустое значение

**Логирование:**
- `merge_conflicts_count` — количество конфликтов при слиянии
- `source_usage_stats` — статистика использования каждого источника

### P03_dedupe_and_conflicts

**Цель:** Удаление дублей и разрешение конфликтов метаданных.

**Входные наборы/колонки:**
- `document_chembl_id` — первичный ключ
- `doi_key` — нормализованный DOI
- `pmid_key` — нормализованный PMID  
- `title` — заголовок документа

**Трансформации:**
```python
# Дедупликация по ключам
duplicate_keys = df.duplicated(subset=['document_chembl_id'], keep='first')
df_deduped = df[~duplicate_keys]

# Эвристика совпадений заголовков
from difflib import SequenceMatcher
def title_similarity(title1: str, title2: str) -> float:
    return SequenceMatcher(None, title1.lower(), title2.lower()).ratio()

# Порог похожести заголовков
SIMILARITY_THRESHOLD = 0.92
```

**Выход и инварианты:**
- Уникальные записи по `document_chembl_id`
- Лог причин удаления дублей
- Отчет по конфликтам метаданных

**Логирование:**
- `duplicates_removed` — количество удаленных дублей
- `conflicts_resolved` — количество разрешенных конфликтов
- `similarity_matches` — количество найденных похожих заголовков

### P04_enrich_fields

**Цель:** Вычисление производных полей и нормализация данных.

**Входные наборы/колонки:**
- `journal` — название журнала
- `year` — год публикации
- `title` — заголовок
- `volume`, `issue`, `first_page`, `last_page` — библиографические данные

**Трансформации:**
```python
# Нормализация журналов
from library.tools.journal_normalizer import normalize_journal_name
journal_normalized = df['journal'].apply(normalize_journal_name)

# Строковое представление года
year_str = df['year'].astype(str)

# Нормализация заголовка
title_norm = df['title'].str.strip().str.lower()

# Форматирование цитат
from library.tools.citation_formatter import format_citation
document_citation = format_citation(
    journal=df['journal'],
    year=df['year'], 
    volume=df['volume'],
    issue=df['issue'],
    first_page=df['first_page'],
    last_page=df['last_page']
)
```

**Выход и инварианты:**
- `journal_normalized` — нормализованное название журнала
- `year_str` — строковое представление года
- `title_norm` — нормализованный заголовок
- `document_citation` — форматированная литературная ссылка

**Логирование:**
- `journal_normalized_count` — количество нормализованных журналов
- `citations_generated` — количество сгенерированных цитат

### P05_qc_validation

**Цель:** Валидация данных через Pandera схемы и проверки качества.

**Входные наборы/колонки:**
- Все поля обработанных данных

**Трансформации:**
```python
from library.tools.data_validator import validate_all_fields
from library.schemas.document_output_schema import DocumentOutputSchema

# Валидация полей из разных источников
df_validated = validate_all_fields(df)

# Pandera валидация схемы
schema = DocumentOutputSchema()
df_validated = schema.validate(df_validated)

# QC проверки
qc_checks = [
    "missing_required_keys",      # document_chembl_id, doi|pmid
    "duplicate_primary_keys",     # уникальность document_chembl_id
    "invalid_doi_fraction",       # доля невалидных DOI
    "invalid_journal_fraction",   # доля невалидных журналов
    "year_range_check",          # год в разумных пределах
    "column_order_check"         # порядок колонок соответствует схеме
]
```

**Выход и инварианты:**
- `valid_*` — валидированные значения полей
- `invalid_*` — флаги невалидности
- QC отчет с метриками качества

**Логирование:**
- `validation_errors` — количество ошибок валидации
- `qc_metrics` — метрики качества данных
- `schema_violations` — нарушения схемы Pandera

### P06_deterministic_export

**Цель:** Экспорт данных в детерминированном формате с фиксированным порядком.

**Входные наборы/колонки:**
- Все обработанные и валидированные данные

**Трансформации:**
```python
from library.etl.load import write_deterministic_csv

# Детерминированная сортировка
df_sorted = df.sort_values(
    by=['document_chembl_id', 'doi_key'],
    ascending=[True, True],
    na_position='last'
)

# Фиксированный порядок колонок
column_order = config.determinism.column_order
df_ordered = df_sorted[column_order]

# Политика NA значений
na_policy = {
    "string": "",
    "int": None, 
    "float": None,
    "bool": None
}

# Экспорт в CSV и Parquet
write_deterministic_csv(
    df_ordered,
    csv_path,
    determinism=config.determinism,
    output=config.output
)
```

**Выход и инварианты:**
- `documents.csv` — CSV с детерминированным порядком
- `documents.parquet` — Parquet как эталон типов данных
- `documents_meta.json` — метаданные о процессе

**Логирование:**
- `export_rows` — количество экспортированных строк
- `export_columns` — количество экспортированных колонок
- `file_sizes` — размеры созданных файлов

## Разрешение коллизий

### Стратегия при конфликте метаданных

При наличии конфликтующих данных из разных источников применяется детерминированная стратегия приоритизации:

1. **ChEMBL** — высший приоритет (источник истины)
2. **PubMed** — второй приоритет (медицинская литература)
3. **Crossref** — третий приоритет (DOI реестр)
4. **OpenAlex** — четвертый приоритет (открытые данные)
5. **Semantic Scholar** — низший приоритет (AI-обработка)

### Правила deterministic pick

```python
def resolve_field_conflict(field_name: str, sources_data: dict) -> Any:
    """Детерминированное разрешение конфликта полей."""
    priority_order = ["chembl", "pubmed", "crossref", "openalex", "semantic_scholar"]
    
    for source in priority_order:
        source_field = f"{source}_{field_name}"
        if source_field in sources_data and not pd.isna(sources_data[source_field]):
            return sources_data[source_field]
    
    return None
```

**Запрещено:** использование "last write wins" или случайного выбора.

## Дедупликация

### Ключи дедупликации

1. **document_chembl_id** — первичный ключ (обязательный)
2. **doi_key** — нормализованный DOI (опциональный)
3. **pmid_key** — нормализованный PMID (опциональный)

### Эвристики совпадений заголовков

```python
def find_similar_titles(df: pd.DataFrame, threshold: float = 0.92) -> list:
    """Поиск похожих заголовков для выявления потенциальных дублей."""
    from difflib import SequenceMatcher
    
    similar_pairs = []
    titles = df['title'].fillna('').str.lower().str.strip()
    
    for i in range(len(titles)):
        for j in range(i + 1, len(titles)):
            similarity = SequenceMatcher(None, titles.iloc[i], titles.iloc[j]).ratio()
            if similarity >= threshold:
                similar_pairs.append({
                    'index1': i,
                    'index2': j, 
                    'similarity': similarity,
                    'title1': titles.iloc[i],
                    'title2': titles.iloc[j]
                })
    
    return similar_pairs
```

### Пороги похожести

- **0.95+** — почти идентичные заголовки (вероятный дубль)
- **0.90-0.94** — очень похожие заголовки (требует ручной проверки)
- **0.85-0.89** — похожие заголовки (возможный дубль)
- **<0.85** — разные заголовки

### Трекинг причин удаления

```python
removal_reasons = {
    'duplicate_chembl_id': 'Дублирующийся document_chembl_id',
    'similar_title_high': 'Высокая похожесть заголовков (>0.95)',
    'conflicting_metadata': 'Конфликтующие метаданные из разных источников',
    'invalid_doi_conflict': 'Конфликтующие DOI из разных источников'
}
```

## Детерминизм экспорта

### Фиксированный порядок колонок

Порядок колонок определяется в `config.determinism.column_order` и строго соблюдается:

1. Служебные поля (`index`, `document_chembl_id`)
2. Основные метаданные (`title`, `doi`, `document_pubmed_id`)
3. Группированные поля по источникам (ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar)
4. Валидационные поля (`valid_*`, `invalid_*`)

### Фиксированный порядок строк

```python
# Сортировка по приоритету
sort_columns = ['document_chembl_id', 'doi_key']
sort_ascending = [True, True]  # по возрастанию
na_position = 'last'  # NULL значения в конце
```

### Формат CSV/Parquet

**CSV:**
- Кодировка: UTF-8
- Разделитель: запятая
- Формат чисел: `%.3f` для float
- Формат дат: ISO 8601
- NA представление: пустая строка для строк, пустое поле для чисел

**Parquet:**
- Движок: PyArrow
- Сжатие: gzip
- Приведение временных меток: миллисекунды
- Сохранение типов данных: полное

### Политика пропусков

```python
na_policy = {
    "string": "",      # пустая строка
    "int": None,       # None
    "float": None,     # None  
    "bool": None       # None
}
```

### Запрет на потерю dtypes

**CSV ограничения:**
- Не сохраняет типы данных
- Все числовые поля становятся строками
- Булевы значения теряются

**Parquet как эталон:**
- Сохраняет все типы данных
- Поддерживает сложные типы (datetime, categorical)
- Рекомендуется для дальнейшей обработки

## CLI-интеграция

### Команда

```bash
etl documents postprocess --config config.yaml --output data/output
```

### Флаги Typer

```python
@app.command("postprocess")
def postprocess_documents(
    config: Path = typer.Option(..., "--config", "-c", help="Путь к конфигурации"),
    output: Path = typer.Option(..., "--output", "-o", help="Директория вывода"),
    qc_report: Path = typer.Option(None, "--qc-report", help="Путь к QC отчету"),
    format: str = typer.Option("csv", "--format", help="Формат вывода: csv или parquet"),
    fail_on_qc: bool = typer.Option(False, "--fail-on-qc", help="Остановить при ошибках QC")
) -> None:
```

### Примеры запуска

```bash
# Базовый запуск
etl documents postprocess --config configs/config_documents_full.yaml

# С указанием выходной директории
etl documents postprocess --config config.yaml --output data/processed

# С генерацией QC отчета
etl documents postprocess --config config.yaml --qc-report reports/qc.json

# Экспорт в Parquet
etl documents postprocess --config config.yaml --format parquet

# Остановка при ошибках QC
etl documents postprocess --config config.yaml --fail-on-qc
```

### Возврат кодов ошибок

```python
class ExitCode(int):
    OK = 0                    # Успешное выполнение
    VALIDATION_ERROR = 1      # Ошибка валидации данных
    HTTP_ERROR = 2           # Ошибка HTTP запросов
    QC_ERROR = 3             # Ошибка контроля качества
    IO_ERROR = 4             # Ошибка ввода-вывода
    SCHEMA_ERROR = 5         # Ошибка схемы Pandera
```

## Логирование

### Формат JSON

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "logger": "library.documents.postprocess",
  "message": "postprocess_step_completed",
  "run_id": "run_20240115_103045_abc123",
  "step": "P01_normalize_keys",
  "elapsed_ms": 1250,
  "rows_processed": 1000,
  "doi_normalized": 850,
  "pmid_normalized": 720
}
```

### Поля контекста

- `run_id` — уникальный идентификатор запуска
- `step` — текущий шаг постпроцессинга (P01-P06)
- `elapsed_ms` — время выполнения в миллисекундах
- `rows_processed` — количество обработанных строк
- `source_stats` — статистика по источникам данных

### Ротация файлов

```python
# Настройки ротации в configs/logging.yaml
handlers:
  file:
    class: logging.handlers.RotatingFileHandler
    maxBytes: 10485760  # 10MB
    backupCount: 5
    filename: logs/documents_post.log
```

## Ограничения и TODO

### Явные пробелы

1. **Отсутствует схема дедупликации по DOI** — TODO: реализовать cross-reference проверку DOI между источниками
2. **Нет валидации ISSN** — TODO: добавить проверку формата ISSN
3. **Отсутствует нормализация авторов** — TODO: стандартизация формата списка авторов
4. **Нет проверки референциальной целостности** — TODO: валидация связей с внешними таблицами

### Необходимые источники истины

1. **Справочник журналов** — `configs/journal_map.yaml` для нормализации названий
2. **Схема валидации ISSN** — регулярные выражения для проверки ISSN
3. **Справочник типов документов** — маппинг типов между источниками
4. **Конфигурация приоритетов источников** — настройка порядка приоритизации

### Рекомендации по улучшению

1. Добавить метрики производительности для каждого шага
2. Реализовать инкрементальную обработку для больших объемов данных
3. Добавить поддержку параллельной обработки на уровне шагов
4. Создать дашборд для мониторинга качества данных
