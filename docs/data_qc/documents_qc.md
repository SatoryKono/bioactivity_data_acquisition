# Контроль качества данных документов

## Обзор

Контроль качества (QC) данных документов обеспечивает валидность, полноту и консистентность обработанных метаданных. QC выполняется на нескольких уровнях: валидация схемы, проверки бизнес-логики, статистический анализ и корреляционные проверки.

## Правила QC

### Полнота ключей

**Обязательные поля:**
- `document_chembl_id` — должен присутствовать в 100% записей
- `doi` или `document_pubmed_id` — хотя бы один должен присутствовать в >95% записей
- `title` — должен присутствовать в >98% записей

**Проверки:**
```python
def check_required_keys(df: pd.DataFrame) -> dict:
    """Проверка полноты обязательных ключей."""
    total_rows = len(df)
    
    checks = {
        'missing_document_chembl_id': df['document_chembl_id'].isna().sum(),
        'missing_doi_and_pmid': df[['doi', 'document_pubmed_id']].isna().all(axis=1).sum(),
        'missing_title': df['title'].isna().sum()
    }
    
    # Вычисляем доли
    for key, count in checks.items():
        checks[f"{key}_fraction"] = count / total_rows if total_rows > 0 else 0
    
    return checks
```

### Уникальность

**Первичные ключи:**
- `document_chembl_id` — должен быть уникальным (0 дублей)
- `doi_key` — должен быть уникальным среди непустых значений
- `pmid_key` — должен быть уникальным среди непустых значений

**Проверки:**
```python
def check_uniqueness(df: pd.DataFrame) -> dict:
    """Проверка уникальности ключевых полей."""
    checks = {
        'duplicate_document_chembl_id': df['document_chembl_id'].duplicated().sum(),
        'duplicate_doi_key': df['doi_key'].dropna().duplicated().sum(),
        'duplicate_pmid_key': df['pmid_key'].dropna().duplicated().sum()
    }
    return checks
```

### Диапазоны значений

**Год публикации:**
- Минимальный год: 1800
- Максимальный год: текущий год + 1
- Допустимые значения: целые числа

**Страницы:**
- `first_page` > 0
- `last_page` >= `first_page`
- `last_page` < 10000 (защита от аномальных значений)

**Проверки:**
```python
def check_value_ranges(df: pd.DataFrame) -> dict:
    """Проверка диапазонов значений."""
    current_year = datetime.now().year
    
    checks = {
        'invalid_year_min': (df['year'] < 1800).sum(),
        'invalid_year_max': (df['year'] > current_year + 1).sum(),
        'invalid_first_page': (df['first_page'] <= 0).sum(),
        'invalid_last_page': (df['last_page'] < df['first_page']).sum(),
        'anomalous_last_page': (df['last_page'] > 10000).sum()
    }
    return checks
```

### Референциальная целостность

**Связи с внешними таблицами:**
- `document_chembl_id` должен существовать в справочнике ChEMBL
- `doi` должен быть валидным согласно DOI реестру
- `document_pubmed_id` должен существовать в PubMed

**Проверки:**
```python
def check_referential_integrity(df: pd.DataFrame) -> dict:
    """Проверка референциальной целостности."""
    checks = {
        'invalid_doi_format': df['doi'].apply(is_valid_doi).sum(),
        'invalid_pmid_format': df['document_pubmed_id'].apply(is_valid_pmid).sum(),
        'orphaned_chembl_ids': check_chembl_references(df['document_chembl_id'])
    }
    return checks
```

### Детерминированные проверки

**Порядок колонок:**
- Колонки должны следовать порядку из `config.determinism.column_order`
- Не должно быть лишних колонок
- Не должно быть отсутствующих колонок

**Стабильность количества строк:**
- Количество строк должно быть стабильным между запусками
- Допустимое отклонение: ±0.1%

**Проверки:**
```python
def check_determinism(df: pd.DataFrame, expected_columns: list) -> dict:
    """Проверка детерминизма данных."""
    actual_columns = list(df.columns)
    
    checks = {
        'column_order_correct': actual_columns == expected_columns,
        'extra_columns': set(actual_columns) - set(expected_columns),
        'missing_columns': set(expected_columns) - set(actual_columns),
        'row_count_stable': check_row_count_stability(len(df))
    }
    return checks
```

## Спецификация Pandera

### Схемы валидации

**Основная схема:** `src/library/schemas/document_output_schema.py`

```python
import pandera as pa
from pandera.typing import Series

class DocumentQCSchema(pa.DataFrameModel):
    """Схема для QC валидации документов."""
    
    # Обязательные поля
    document_chembl_id: Series[str] = pa.Field(
        description="ChEMBL document identifier",
        nullable=False
    )
    
    # DOI валидация
    doi_key: Series[str] = pa.Field(
        nullable=True,
        description="Normalized DOI key"
    )
    
    # Год валидация
    year: Series[int] = pa.Field(
        nullable=True,
        ge=1800,
        le=2030,
        description="Publication year"
    )
    
    # Страницы валидация
    first_page: Series[int] = pa.Field(
        nullable=True,
        ge=1,
        description="First page number"
    )
    
    last_page: Series[int] = pa.Field(
        nullable=True,
        ge=1,
        le=10000,
        description="Last page number"
    )
    
    class Config:
        strict = True
        coerce = True
```

### Примеры Column/Check

```python
# Проверка уникальности
document_chembl_id: Series[str] = pa.Field(
    checks=[
        pa.Check(lambda x: not x.duplicated().any(), 
                name="unique_document_chembl_id")
    ]
)

# Проверка диапазона годов
year: Series[int] = pa.Field(
    checks=[
        pa.Check(lambda x: x.between(1800, 2030), 
                name="year_in_range")
    ]
)

# Проверка логики страниц
last_page: Series[int] = pa.Field(
    checks=[
        pa.Check(lambda df: df['last_page'] >= df['first_page'],
                name="last_page_gte_first_page")
    ]
)
```

### Hints для категориальных полей

```python
# Категориальные поля с предопределенными значениями
doc_type: Series[str] = pa.Field(
    nullable=True,
    isin=["article", "review", "letter", "editorial", "conference"],
    description="Document type"
)

# Журналы с проверкой на известные названия
journal: Series[str] = pa.Field(
    nullable=True,
    description="Journal name"
)

# ISSN формат
issn: Series[str] = pa.Field(
    nullable=True,
    regex=r"^\d{4}-\d{3}[\dX]$",
    description="ISSN in format XXXX-XXXX"
)
```

### Полный пример QC схемы

```python
from pandera import Column, DataFrameSchema, Check
import pandas as pd

def create_documents_qc_schema() -> DataFrameSchema:
    """Создает полную QC схему для документов."""
    
    return DataFrameSchema({
        # Обязательные поля
        "document_chembl_id": Column(
            str,
            checks=[
                Check(lambda x: not x.isna().any(), name="not_null"),
                Check(lambda x: not x.duplicated().any(), name="unique"),
                Check(lambda x: x.str.len() > 0, name="not_empty")
            ]
        ),
        
        # DOI валидация
        "doi_key": Column(
            str,
            nullable=True,
            checks=[
                Check(lambda x: x.dropna().str.match(r"^10\.\d+/"), 
                      name="valid_doi_format")
            ]
        ),
        
        # Год валидация
        "year": Column(
            int,
            nullable=True,
            checks=[
                Check(lambda x: x.between(1800, 2030), name="year_range"),
                Check(lambda x: x.dtype == 'int64', name="integer_type")
            ]
        ),
        
        # Страницы валидация
        "first_page": Column(
            int,
            nullable=True,
            checks=[
                Check(lambda x: x > 0, name="positive_page")
            ]
        ),
        
        "last_page": Column(
            int,
            nullable=True,
            checks=[
                Check(lambda x: x > 0, name="positive_page"),
                Check(lambda x: x < 10000, name="reasonable_page_count")
            ]
        ),
        
        # Валидационные флаги
        "invalid_doi": Column(
            bool,
            nullable=True
        ),
        
        "invalid_journal": Column(
            bool,
            nullable=True
        )
    })
```

## Отчёт QC

### Формат отчета

**JSON структура:**
```json
{
  "qc_summary": {
    "total_records": 1000,
    "qc_passed": true,
    "timestamp": "2024-01-15T10:30:45Z",
    "run_id": "run_20240115_103045_abc123"
  },
  "metrics": [
    {
      "metric": "missing_document_chembl_id",
      "value": 0,
      "threshold": 0.0,
      "ratio": 0.0,
      "status": "pass"
    },
    {
      "metric": "duplicate_document_chembl_id", 
      "value": 0,
      "threshold": 0.0,
      "ratio": 0.0,
      "status": "pass"
    },
    {
      "metric": "invalid_doi_fraction",
      "value": 45,
      "threshold": 0.05,
      "ratio": 0.045,
      "status": "pass"
    }
  ],
  "schema_validation": {
    "pandera_passed": true,
    "violations": []
  },
  "determinism_checks": {
    "column_order_correct": true,
    "row_count_stable": true
  }
}
```

### Метрики QC

**Базовые метрики:**
- `total_records` — общее количество записей
- `missing_required_keys` — количество записей без обязательных ключей
- `duplicate_primary_keys` — количество дублирующихся первичных ключей
- `invalid_doi_fraction` — доля невалидных DOI
- `invalid_journal_fraction` — доля невалидных журналов
- `year_out_of_range` — количество записей с годом вне диапазона
- `page_logic_errors` — количество ошибок в логике страниц

**Расширенные метрики:**
- `source_coverage` — покрытие данными по источникам
- `field_completeness` — полнота полей по источникам
- `data_quality_score` — общий балл качества данных (0-100)
- `correlation_anomalies` — аномалии в корреляциях между полями

### Пороги фейла

```python
QC_THRESHOLDS = {
    "missing_document_chembl_id": 0.0,      # 0% - абсолютно недопустимо
    "missing_doi_and_pmid": 0.05,           # 5% - максимум без DOI/PMID
    "missing_title": 0.02,                  # 2% - максимум без заголовка
    "duplicate_document_chembl_id": 0.0,    # 0% - дубли недопустимы
    "invalid_doi_fraction": 0.1,            # 10% - максимум невалидных DOI
    "invalid_journal_fraction": 0.15,       # 15% - максимум невалидных журналов
    "year_out_of_range": 0.01,              # 1% - максимум лет вне диапазона
    "page_logic_errors": 0.05               # 5% - максимум ошибок страниц
}
```

### Включение --fail-on-qc

```python
def check_qc_thresholds(qc_report: pd.DataFrame, fail_on_qc: bool = False) -> bool:
    """Проверка порогов QC с возможностью остановки при ошибках."""
    
    failed_checks = qc_report[
        (qc_report['status'] == 'fail') & 
        (qc_report['threshold'].notna())
    ]
    
    if fail_on_qc and len(failed_checks) > 0:
        failed_metrics = failed_checks['metric'].tolist()
        raise QCError(f"QC checks failed: {failed_metrics}")
    
    return len(failed_checks) == 0
```

### Автоматическая генерация отчетов

```python
def generate_qc_report(df: pd.DataFrame, config: Config) -> dict:
    """Генерация полного QC отчета."""
    
    # Базовые проверки
    basic_checks = check_required_keys(df)
    uniqueness_checks = check_uniqueness(df)
    range_checks = check_value_ranges(df)
    
    # Pandera валидация
    schema = create_documents_qc_schema()
    try:
        validated_df = schema.validate(df)
        pandera_passed = True
        pandera_violations = []
    except pa.errors.SchemaError as e:
        pandera_passed = False
        pandera_violations = str(e)
    
    # Детерминизм
    determinism_checks = check_determinism(df, config.determinism.column_order)
    
    # Сборка отчета
    report = {
        "qc_summary": {
            "total_records": len(df),
            "qc_passed": all([
                basic_checks.get('missing_document_chembl_id', 0) == 0,
                uniqueness_checks.get('duplicate_document_chembl_id', 0) == 0,
                pandera_passed
            ]),
            "timestamp": datetime.now().isoformat(),
            "run_id": get_current_run_id()
        },
        "metrics": _format_metrics(basic_checks, uniqueness_checks, range_checks),
        "schema_validation": {
            "pandera_passed": pandera_passed,
            "violations": pandera_violations
        },
        "determinism_checks": determinism_checks
    }
    
    return report
```

### Интеграция с CLI

```python
@app.command("postprocess")
def postprocess_documents(
    # ... другие параметры
    qc_report: Path = typer.Option(None, "--qc-report", help="Путь к QC отчету"),
    fail_on_qc: bool = typer.Option(False, "--fail-on-qc", help="Остановить при ошибках QC")
) -> None:
    """Постпроцессинг документов с QC проверками."""
    
    # ... основная обработка
    
    # Генерация QC отчета
    qc_report_data = generate_qc_report(processed_df, config)
    
    # Сохранение отчета
    if qc_report:
        with open(qc_report, 'w', encoding='utf-8') as f:
            json.dump(qc_report_data, f, indent=2, ensure_ascii=False)
    
    # Проверка порогов
    qc_passed = check_qc_thresholds(qc_report_data, fail_on_qc)
    
    if not qc_passed and fail_on_qc:
        typer.echo("QC checks failed. Stopping execution.", err=True)
        raise typer.Exit(ExitCode.QC_ERROR)
    
    typer.echo(f"QC report generated: {qc_report}")
    typer.echo(f"QC status: {'PASSED' if qc_passed else 'FAILED'}")
```

## Мониторинг качества

### Дашборд метрик

**Ключевые показатели:**
- Тренд качества данных по времени
- Распределение ошибок по типам
- Покрытие данными по источникам
- Производительность QC проверок

### Алерты

**Критические алерты:**
- Увеличение доли невалидных DOI > 15%
- Появление дублирующихся document_chembl_id
- Падение покрытия данными < 80%

**Предупреждения:**
- Увеличение доли невалидных журналов > 10%
- Аномалии в распределении годов публикации
- Нестабильность количества строк между запусками

### Рекомендации по улучшению

1. **Автоматическая коррекция** — исправление очевидных ошибок (лишние пробелы, неправильный регистр)
2. **Машинное обучение** — предсказание качества данных на основе исторических паттернов
3. **Интерактивная валидация** — веб-интерфейс для ручной проверки спорных случаев
4. **Интеграция с внешними API** — проверка DOI и ISSN через официальные реестры
