# Схемы данных для документов

## Обзор

Система извлечения данных документов использует две основные схемы:

1. **DocumentInputSchema**- для входных данных из ChEMBL
2.**DocumentOutputSchema** - для обогащенных данных из всех источников

## Схема входных данных (DocumentInputSchema)

### Обязательные поля

- `document*chembl*id`(str) - Идентификатор документа ChEMBL

-`doi`(str) - Digital Object Identifier

-`title`(str) - Название документа

### Опциональные поля

-`abstract`(str, nullable) - Аннотация документа

-`pubmed*authors`(str, nullable) - Авторы документа из PubMed

-`document*classification`(float, nullable) - Классификация документа

-`referenses*on*previous*experiments`(bool, nullable) - Содержит внешние ссылки

-`first*page`(int, nullable) - Номер первой страницы

-`original*experimental*document`(bool, nullable) - Экспериментальный документ

-`issue`(int, nullable) - Номер выпуска журнала

-`journal`(str, nullable) - Название журнала

-`last*page`(float, nullable) - Номер последней страницы

-`month`(int, nullable) - Месяц публикации

-`postcodes`(str, nullable) - Почтовые коды

-`document*pubmed*id`(int, nullable) - Идентификатор PubMed

-`volume`(float, nullable) - Том журнала

-`year`(int, nullable) - Год публикации

## Схема выходных данных (DocumentOutputSchema)

### Исходные поля ChEMBL

Все поля из DocumentInputSchema плюс:

### Обогащенные поля

-`source`(str) - Источник данных: "chembl", "crossref", "openalex", "pubmed", "semantic*scholar"

### Поля из Crossref

-`issued`(str, nullable) - Дата публикации из Crossref

### Поля из OpenAlex

-`openalex*id`(str, nullable) - Идентификатор работы OpenAlex

-`publication*year`(int, nullable) - Год публикации из OpenAlex

### Поля из PubMed

-`pmid`(str, nullable) - Идентификатор PubMed из внешнего источника

### Поля из Semantic Scholar

-`publication*venue`(str, nullable) - Место публикации из Semantic Scholar

-`publication*types`(str, nullable) - Типы публикации из Semantic Scholar

## Схема QC метрик (DocumentQCSchema)

-`metric`(str) - Название метрики QC

-`value`(int, ge=0) - Значение метрики QC

### Примеры QC метрик

-`row*count`- Количество обработанных строк

-`enabled*sources`- Количество включенных источников

-`chembl*records`- Количество записей из ChEMBL

-`crossref*records`- Количество записей из Crossref

-`openalex*records`- Количество записей из OpenAlex

-`pubmed*records`- Количество записей из PubMed

-`semantic*scholar*records`- Количество записей из Semantic Scholar

## Диаграмма потока данных

```

Входные данные (ChEMBL CSV)
    ↓
DocumentInputSchema (валидация)
    ↓
Извлечение данных из источников:
    ├── ChEMBL API
    ├── Crossref API
    ├── OpenAlex API
    ├── PubMed API
    └── Semantic Scholar API
    ↓
DocumentOutputSchema (обогащенные данные)
    ↓
Выходные файлы:
    ├── documents*YYYYMMDD.csv
    └── documents*YYYYMMDD*qc.csv

```

## Примеры использования

### Валидация входных данных

```

from library.schemas.document*input*schema import DocumentInputSchema

## Загрузка и валидация данных

df = pd.read*csv("data/input/documents.csv")
validated*df = DocumentInputSchema.validate(df)

```

### Валидация выходных данных

```

from library.schemas.document*output*schema import DocumentOutputSchema

## Валидация обогащенных данных

enriched*df = DocumentOutputSchema.validate(df)

```

### Валидация QC метрик

```

from library.schemas.document*output*schema import DocumentQCSchema

## Валидация QC метрик

qc*df = DocumentQCSchema.validate(qc*data)

```
