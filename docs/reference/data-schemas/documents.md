# Схемы документов

## Обзор

Данный документ описывает Pandera схемы для валидации данных документов, включая входные данные документов и обогащенные выходные данные с метаданными из различных источников.

## Основные схемы

### DocumentInputSchema

**Расположение**: `src/library/schemas/document_input_schema.py`

**Назначение**: Валидация входных данных документов из CSV файлов

**Основные поля**:
- `document_id`: Уникальный идентификатор документа
- `title`: Название документа
- `authors`: Авторы документа
- `journal`: Журнал публикации
- `publication_date`: Дата публикации
- `doi`: DOI идентификатор
- `pmid`: PubMed ID

**Пример схемы**:
```python
class DocumentInputSchema(pa.DataFrameModel):
    """Схема для входных данных документов."""
    
    document_id: pa.typing.Series[str] = pa.Field(
        description="Уникальный идентификатор документа",
        min_length=1,
        max_length=100
    )
    
    title: pa.typing.Series[str] = pa.Field(
        description="Название документа",
        min_length=10,
        max_length=1000
    )
    
    authors: pa.typing.Series[str] = pa.Field(
        description="Авторы документа",
        min_length=3,
        max_length=2000
    )
    
    journal: pa.typing.Series[str] = pa.Field(
        description="Название журнала",
        min_length=3,
        max_length=200
    )
    
    publication_date: pa.typing.Series[str] = pa.Field(
        description="Дата публикации",
        regex=r"^\d{4}-\d{2}-\d{2}$"
    )
    
    doi: pa.typing.Series[str] = pa.Field(
        description="DOI идентификатор",
        regex=r"^10\.\d{4,}/[^\s]+$",
        nullable=True
    )
    
    pmid: pa.typing.Series[str] = pa.Field(
        description="PubMed ID",
        regex=r"^\d+$",
        nullable=True
    )
```

### DocumentOutputSchema

**Расположение**: `src/library/schemas/document_output_schema.py`

**Назначение**: Валидация обогащенных данных документов после обработки пайплайном

**Дополнительные поля**:
- `pubmed_issn`: ISSN из PubMed
- `pubmed_volume`: Том журнала
- `pubmed_issue`: Номер выпуска
- `pubmed_pages`: Страницы
- `crossref_citations`: Количество цитирований из Crossref
- `openalex_citations`: Количество цитирований из OpenAlex
- `semantic_scholar_citations`: Количество цитирований из Semantic Scholar
- `abstract`: Аннотация документа
- `keywords`: Ключевые слова
- `document_sortorder`: Порядок сортировки

**Пример схемы**:
```python
class DocumentOutputSchema(pa.DataFrameModel):
    """Схема для выходных данных документов."""
    
    # Базовые поля из входной схемы
    document_id: pa.typing.Series[str] = pa.Field(
        description="Уникальный идентификатор документа",
        min_length=1,
        max_length=100
    )
    
    title: pa.typing.Series[str] = pa.Field(
        description="Название документа",
        min_length=10,
        max_length=1000
    )
    
    authors: pa.typing.Series[str] = pa.Field(
        description="Авторы документа",
        min_length=3,
        max_length=2000
    )
    
    journal: pa.typing.Series[str] = pa.Field(
        description="Название журнала",
        min_length=3,
        max_length=200
    )
    
    publication_date: pa.typing.Series[str] = pa.Field(
        description="Дата публикации",
        regex=r"^\d{4}-\d{2}-\d{2}$"
    )
    
    doi: pa.typing.Series[str] = pa.Field(
        description="DOI идентификатор",
        regex=r"^10\.\d{4,}/[^\s]+$",
        nullable=True
    )
    
    pmid: pa.typing.Series[str] = pa.Field(
        description="PubMed ID",
        regex=r"^\d+$",
        nullable=True
    )
    
    # Дополнительные поля из PubMed
    pubmed_issn: pa.typing.Series[str] = pa.Field(
        description="ISSN из PubMed",
        regex=r"^\d{4}-\d{4}$",
        nullable=True
    )
    
    pubmed_volume: pa.typing.Series[str] = pa.Field(
        description="Том журнала",
        nullable=True
    )
    
    pubmed_issue: pa.typing.Series[str] = pa.Field(
        description="Номер выпуска",
        nullable=True
    )
    
    pubmed_pages: pa.typing.Series[str] = pa.Field(
        description="Страницы",
        nullable=True
    )
    
    # Данные о цитированиях
    crossref_citations: pa.typing.Series[int] = pa.Field(
        description="Количество цитирований из Crossref",
        ge=0,
        nullable=True
    )
    
    openalex_citations: pa.typing.Series[int] = pa.Field(
        description="Количество цитирований из OpenAlex",
        ge=0,
        nullable=True
    )
    
    semantic_scholar_citations: pa.typing.Series[int] = pa.Field(
        description="Количество цитирований из Semantic Scholar",
        ge=0,
        nullable=True
    )
    
    # Дополнительные метаданные
    abstract: pa.typing.Series[str] = pa.Field(
        description="Аннотация документа",
        nullable=True
    )
    
    keywords: pa.typing.Series[str] = pa.Field(
        description="Ключевые слова",
        nullable=True
    )
    
    # Поле для детерминированной сортировки
    document_sortorder: pa.typing.Series[str] = pa.Field(
        description="Порядок сортировки документа",
        min_length=1
    )
```

## Валидация документов

### Базовые проверки

**Проверка обязательных полей**:
```python
def validate_required_fields(dataframe: pd.DataFrame) -> bool:
    """Проверяет наличие обязательных полей."""
    
    required_fields = [
        "document_id", "title", "authors", 
        "journal", "publication_date"
    ]
    
    missing_fields = set(required_fields) - set(dataframe.columns)
    
    if missing_fields:
        raise ValueError(f"Отсутствуют обязательные поля: {missing_fields}")
    
    return True
```

**Проверка уникальности идентификаторов**:
```python
def validate_unique_ids(dataframe: pd.DataFrame) -> bool:
    """Проверяет уникальность идентификаторов документов."""
    
    if dataframe["document_id"].duplicated().any():
        raise ValueError("Найдены дублирующиеся document_id")
    
    return True
```

### Проверка форматов данных

**Валидация DOI**:
```python
def validate_doi_format(doi: str) -> bool:
    """Проверяет формат DOI."""
    
    if pd.isna(doi):
        return True
    
    doi_pattern = r"^10\.\d{4,}/[^\s]+$"
    return bool(re.match(doi_pattern, doi))
```

**Валидация PubMed ID**:
```python
def validate_pmid_format(pmid: str) -> bool:
    """Проверяет формат PubMed ID."""
    
    if pd.isna(pmid):
        return True
    
    pmid_pattern = r"^\d+$"
    return bool(re.match(pmid_pattern, pmid))
```

**Валидация даты публикации**:
```python
def validate_publication_date(date_str: str) -> bool:
    """Проверяет формат даты публикации."""
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
```

## Обогащение данных

### Источники данных

**PubMed**:
- ISSN журнала
- Том и номер выпуска
- Страницы
- Аннотация
- Ключевые слова

**Crossref**:
- Количество цитирований
- Дополнительные метаданные
- Ссылки на связанные документы

**OpenAlex**:
- Количество цитирований
- Информация об авторах
- Классификация по темам

**Semantic Scholar**:
- Количество цитирований
- Реферат
- Связанные документы

### Процесс обогащения

```python
def enrich_document_data(
    input_data: pd.DataFrame,
    config: DocumentConfig
) -> pd.DataFrame:
    """Обогащает данные документов из различных источников."""
    
    enriched_data = input_data.copy()
    
    # Обогащение из PubMed
    if config.sources.pubmed.enabled:
        enriched_data = enrich_from_pubmed(enriched_data, config)
    
    # Обогащение из Crossref
    if config.sources.crossref.enabled:
        enriched_data = enrich_from_crossref(enriched_data, config)
    
    # Обогащение из OpenAlex
    if config.sources.openalex.enabled:
        enriched_data = enrich_from_openalex(enriched_data, config)
    
    # Обогащение из Semantic Scholar
    if config.sources.semantic_scholar.enabled:
        enriched_data = enrich_from_semantic_scholar(enriched_data, config)
    
    # Добавление поля сортировки
    enriched_data = add_document_sortorder(enriched_data)
    
    return enriched_data
```

### Добавление порядка сортировки

```python
def add_document_sortorder(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Добавляет поле для детерминированной сортировки."""
    
    df = dataframe.copy()
    
    def create_sortorder(row):
        """Создает строку для сортировки."""
        issn = row.get("pubmed_issn", "0000-0000")
        date = row.get("publication_date", "1900-01-01")
        index = row.name
        
        # Форматирование индекса с ведущими нулями
        padded_index = f"{index:06d}"
        
        return f"{issn}:{date}:{padded_index}"
    
    df["document_sortorder"] = df.apply(create_sortorder, axis=1)
    
    return df
```

## Нормализация данных

### Нормализация названий журналов

```python
def normalize_journal_name(journal_name: str) -> str:
    """Нормализует название журнала."""
    
    if pd.isna(journal_name):
        return ""
    
    # Удаление лишних пробелов
    normalized = str(journal_name).strip()
    
    # Стандартизация сокращений
    abbreviations = {
        "J.": "Journal of",
        "Proc.": "Proceedings of",
        "Ann.": "Annals of",
        "Int.": "International"
    }
    
    for abbr, full in abbreviations.items():
        normalized = normalized.replace(abbr, full)
    
    return normalized
```

### Нормализация авторов

```python
def normalize_authors(authors_str: str) -> str:
    """Нормализует список авторов."""
    
    if pd.isna(authors_str):
        return ""
    
    # Разделение авторов
    authors = [author.strip() for author in str(authors_str).split(";")]
    
    # Нормализация каждого автора
    normalized_authors = []
    for author in authors:
        if author:
            # Формат: Фамилия, И.О.
            parts = author.split(",")
            if len(parts) >= 2:
                last_name = parts[0].strip()
                first_names = parts[1].strip()
                normalized_authors.append(f"{last_name}, {first_names}")
            else:
                normalized_authors.append(author)
    
    return "; ".join(normalized_authors)
```

### Нормализация дат

```python
def normalize_publication_date(date_str: str) -> str:
    """Нормализует дату публикации."""
    
    if pd.isna(date_str):
        return "1900-01-01"
    
    try:
        # Попытка парсинга различных форматов
        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%Y"]:
            try:
                parsed_date = datetime.strptime(str(date_str), fmt)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # Если не удалось распарсить, возвращаем исходное значение
        return str(date_str)
        
    except Exception:
        return "1900-01-01"
```

## Валидация обогащенных данных

### Проверка целостности данных

```python
def validate_enriched_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Валидирует обогащенные данные документов."""
    
    try:
        validated = DocumentOutputSchema.validate(dataframe, lazy=True)
        return validated
    except pa.errors.SchemaError as e:
        logger.error(f"Ошибка валидации обогащенных данных: {e}")
        
        # Исправление ошибок валидации
        corrected_data = fix_validation_errors(dataframe, e.failure_cases)
        return corrected_data
```

### Проверка качества обогащения

```python
def check_enrichment_quality(dataframe: pd.DataFrame) -> dict:
    """Проверяет качество обогащения данных."""
    
    quality_metrics = {}
    
    # Полнота данных по источникам
    sources = ["pubmed", "crossref", "openalex", "semantic_scholar"]
    
    for source in sources:
        if source == "pubmed":
            fields = ["pubmed_issn", "pubmed_volume", "abstract"]
        elif source == "crossref":
            fields = ["crossref_citations"]
        elif source == "openalex":
            fields = ["openalex_citations"]
        elif source == "semantic_scholar":
            fields = ["semantic_scholar_citations"]
        
        completeness = {}
        for field in fields:
            if field in dataframe.columns:
                non_null_count = dataframe[field].count()
                completeness[field] = non_null_count / len(dataframe)
        
        quality_metrics[source] = completeness
    
    return quality_metrics
```

## Примеры использования

### Загрузка и валидация входных данных

```python
from library.schemas import DocumentInputSchema

# Загрузка данных
input_data = pd.read_csv("data/input/documents.csv")

# Валидация входных данных
validated_input = DocumentInputSchema.validate(input_data)

print(f"Загружено {len(validated_input)} документов")
```

### Обогащение данных

```python
from library.documents import enrich_document_data
from library.documents.config import DocumentConfig

# Загрузка конфигурации
config = DocumentConfig.from_file("configs/config_documents_full.yaml")

# Обогащение данных
enriched_data = enrich_document_data(validated_input, config)

print(f"Обогащено {len(enriched_data)} документов")
```

### Валидация выходных данных

```python
from library.schemas import DocumentOutputSchema

# Валидация обогащенных данных
validated_output = DocumentOutputSchema.validate(enriched_data)

# Проверка качества обогащения
quality_metrics = check_enrichment_quality(validated_output)

print("Метрики качества обогащения:")
for source, metrics in quality_metrics.items():
    print(f"{source}: {metrics}")
```

### Сохранение результатов

```python
from library.documents import write_document_outputs

# Сохранение обогащенных данных
output_path = write_document_outputs(
    dataframe=validated_output,
    config=config
)

print(f"Данные сохранены в: {output_path}")
```

## Мониторинг и отчетность

### Метрики обогащения

**Статистика по источникам**:
- Количество успешно обогащенных документов
- Процент успешных запросов к API
- Время выполнения запросов
- Количество ошибок по источникам

**Качество данных**:
- Полнота полей по источникам
- Консистентность данных между источниками
- Количество дубликатов
- Качество нормализации

### Отчеты качества

```python
def generate_enrichment_report(dataframe: pd.DataFrame) -> dict:
    """Генерирует отчет о качестве обогащения."""
    
    report = {
        "total_documents": len(dataframe),
        "enrichment_stats": {},
        "quality_metrics": check_enrichment_quality(dataframe)
    }
    
    # Статистика по источникам
    sources = ["pubmed", "crossref", "openalex", "semantic_scholar"]
    
    for source in sources:
        if source == "pubmed":
            enriched_count = dataframe["pubmed_issn"].count()
        elif source == "crossref":
            enriched_count = dataframe["crossref_citations"].count()
        elif source == "openalex":
            enriched_count = dataframe["openalex_citations"].count()
        elif source == "semantic_scholar":
            enriched_count = dataframe["semantic_scholar_citations"].count()
        
        report["enrichment_stats"][source] = {
            "enriched_count": enriched_count,
            "enrichment_rate": enriched_count / len(dataframe)
        }
    
    return report
```
