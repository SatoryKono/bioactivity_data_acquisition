# Схемы данных для документов

## Обзор

Система извлечения данных документов использует две основные схемы:

1. DocumentInputSchema — входной CSV с ChEMBL ID, DOI, названием
2. DocumentOutputSchema — обогащённые поля из ChEMBL/Crossref/OpenAlex/PubMed/Semantic Scholar

## Схема входных данных (DocumentInputSchema)

### Обязательные поля

- `document_chembl_id` (str) — Идентификатор документа ChEMBL

- `doi` (str) — Digital Object Identifier

- `title` (str) — Название документа

### Опциональные поля

-`abstract`(str, nullable) - Аннотация документа

- `pubmed_authors` (str, nullable) - Авторы документа из PubMed

- `document_classification` (float, nullable) - Классификация документа

- `referenses_on_previous_experiments` (bool, nullable) - Содержит внешние ссылки

- `first_page` (int, nullable) - Номер первой страницы

- `original_experimental_document` (bool, nullable) - Экспериментальный документ

-`issue`(int, nullable) - Номер выпуска журнала

-`journal`(str, nullable) - Название журнала

- `last_page` (float, nullable) - Номер последней страницы

-`month`(int, nullable) - Месяц публикации

- `postcodes` (str, nullable) — Почтовые коды (устарело; поле удаляется на этапе нормализации, если присутствует)

- `document_pubmed_id` (int, nullable) - Идентификатор PubMed

-`volume`(float, nullable) - Том журнала

-`year`(int, nullable) - Год публикации

## Схема выходных данных (DocumentOutputSchema)

### Исходные поля ChEMBL

Все поля из DocumentInputSchema плюс:

### Обогащенные поля

- `source` (str) — Источник данных: "chembl", "crossref", "openalex", "pubmed", "semantic_scholar"

### Поля из Crossref

-`issued`(str, nullable) - Дата публикации из Crossref

### Поля из OpenAlex

- `openalex_id` (str, nullable) - Идентификатор работы OpenAlex

- `publication_year` (int, nullable) - Год публикации из OpenAlex

### Поля из PubMed

-`pmid`(str, nullable) - Идентификатор PubMed из внешнего источника

### Поля из Semantic Scholar

- `publication_venue` (str, nullable) - Место публикации из Semantic Scholar

- `publication_types` (str, nullable) - Типы публикации из Semantic Scholar

## Схема QC метрик (DocumentQCSchema)

-`metric`(str) - Название метрики QC

-`value`(int, ge=0) - Значение метрики QC

### Примеры QC метрик

- `row_count` - Количество обработанных строк

- `enabled_sources` - Количество включенных источников

- `chembl_records` - Количество записей из ChEMBL

- `crossref_records` - Количество записей из Crossref

- `openalex_records` - Количество записей из OpenAlex

- `pubmed_records` - Количество записей из PubMed

- `semantic_scholar_records` - Количество записей из Semantic Scholar

## Диаграмма потока данных

```mermaid
flowchart TB
  A[Входные данные (documents.csv)] --> B[DocumentInputSchema валидация]
  B --> C[Нормализация входа]
  C --> D{Источники}
  D --> D1[ChEMBL]
  D --> D2[Crossref]
  D --> D3[OpenAlex]
  D --> D4[PubMed]
  D --> D5[Semantic Scholar]
  D1 & D2 & D3 & D4 & D5 --> E[DocumentOutputSchema]
  E --> F[QC: row_count, enabled_sources, *_records]
  F --> G[Экспорт documents_<date>.csv и documents_<date>_qc.csv]
```

## Примеры использования

### Валидация входных данных

```python
from library.schemas.document_input_schema import DocumentInputSchema

# Загрузка и валидация данных
df = pd.read_csv("data/input/documents.csv")
validated_df = DocumentInputSchema.validate(df)
```

### Валидация выходных данных

```python
from library.schemas.document_output_schema import DocumentOutputSchema

# Валидация обогащенных данных
enriched_df = DocumentOutputSchema.validate(df)
```

### Валидация QC метрик

```python
from library.schemas.document_output_schema import DocumentQCSchema

# Валидация QC метрик
qc_df = DocumentQCSchema.validate(qc_data)
```
