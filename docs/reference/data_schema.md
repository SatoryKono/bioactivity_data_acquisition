# Схема данных системы извлечения биоактивности

## Введение

Система использует **звездную схему данных** (star schema), состоящую из:

- **Фактовые таблицы** (fact tables): `activity_fact` - основная таблица с измерениями биоактивности
- **Измерительные таблицы** (dimension tables): `document_dim`, `target_dim`, `assay_dim`, `testitem_dim` - справочники с метаданными

### Архитектура данных

```text
activity_fact (центральная таблица)
├── FK → document_dim (документы/публикации)
├── FK → target_dim (биологические мишени)
├── FK → assay_dim (экспериментальные методы)
└── FK → testitem_dim (химические соединения)
```

## Пайплайны и источники данных

### 1. Documents (document_dim)

**Назначение**: Метаданные научных публикаций и патентов

**Источники данных**:

- **ChEMBL**: `GET https://www.ebi.ac.uk/chembl/api/data/document/{document_chembl_id}` - единичные документы
- **Crossref**: `GET https://api.crossref.org/works/{DOI}` - DOI метаданные
- **OpenAlex**: `GET https://api.openalex.org/works/https://doi.org/{DOI}` - научные публикации
- **PubMed**: NCBI E-utilities ESearch/EFetch - медицинские публикации
- **Semantic Scholar**: `GET https://api.semanticscholar.org/graph/v1/paper/PMID:{PMID}` - академические публикации

**Извлекаемые поля**:

| Источник | Ключевые поля |
|----------|---------------|
| ChEMBL | document_chembl_id, document_pubmed_id, chembl_title, chembl_abstract, chembl_authors, chembl_journal, chembl_year, chembl_doi |
| Crossref | crossref_doi, crossref_title, crossref_pmid, crossref_abstract, crossref_authors, crossref_journal |
| OpenAlex | openalex_doi, openalex_title, openalex_pmid, openalex_abstract, openalex_authors, openalex_journal |
| PubMed | pubmed_pmid, pubmed_doi, pubmed_article_title, pubmed_abstract, pubmed_journal, pubmed_mesh_descriptors |
| Semantic Scholar | semantic_scholar_pmid, semantic_scholar_doi, semantic_scholar_title, semantic_scholar_abstract |

**Нормализованные поля**: Объединение данных по приоритету источников с дедупликацией

### 2. Targets (target_dim)

**Назначение**: Биологические мишени (белки, ферменты, рецепторы)

**Источники данных**:

- **ChEMBL**: `GET https://www.ebi.ac.uk/chembl/api/data/target/{target_chembl_id}` - основная информация о мишенях
- **UniProt**: `GET https://rest.uniprot.org/uniprotkb/{UniProtAccession}.json` - белковые данные
- **IUPHAR/BPS GtoP**: `GET https://www.guidetopharmacology.org/targets/{iuphar_id}` - фармакологические данные

**Извлекаемые поля**:

| Источник | Ключевые поля |
|----------|---------------|
| ChEMBL | target_chembl_id, pref_name, target_type, tax_id, species_group_flag, target_components |
| UniProt | uniprot_id_primary, recommendedName, geneName, organism, sequence_length, molecular_function, cellular_component |
| IUPHAR/GtoP | iuphar_target_id, iuphar_type, iuphar_class, iuphar_name, gtop_target_id, gtop_interactions_n |

**Нормализованные поля**: Объединение с приоритетом ChEMBL → UniProt → IUPHAR

### 3. Assays (assay_dim)

**Назначение**: Экспериментальные методы измерения биоактивности

**Источники данных**:

- **ChEMBL**: `GET https://www.ebi.ac.uk/chembl/api/data/assay/{assay_chembl_id}` - методы экспериментов

**Извлекаемые поля**:

| Поле | Описание |
|------|----------|
| assay_chembl_id | Уникальный идентификатор ассая |
| assay_type | Тип ассая (B, F, A, P, T, U) |
| target_chembl_id | Связь с мишенью |
| assay_organism | Организм для эксперимента |
| assay_cell_type | Тип клеток |
| assay_tissue | Тип ткани |
| bao_format, bao_label | BAO онтологические аннотации |

**Нормализованные поля**: Стандартизация типов ассаев и BAO классификация

### 4. Testitems (testitem_dim)

**Назначение**: Химические соединения и молекулы

**Источники данных**:

- **ChEMBL**: `GET https://www.ebi.ac.uk/chembl/api/data/molecule/{molecule_chembl_id}` - химические данные
- **PubChem**: PUG-REST API - дополнительные химические свойства

**Извлекаемые поля**:

| Источник | Ключевые поля |
|----------|---------------|
| ChEMBL | molecule_chembl_id, pref_name, max_phase, therapeutic_flag, mw_freebase, alogp, hba, hbd, psa |
| PubChem | pubchem_cid, pubchem_molecular_formula, pubchem_molecular_weight, pubchem_canonical_smiles, pubchem_inchi |

**Нормализованные поля**: Объединение структурных данных с приоритетом канонических форм

### 5. Activities (activity_fact)

**Назначение**: Измерения биоактивности соединений против мишеней

**Источники данных**:

- **ChEMBL**: `GET https://www.ebi.ac.uk/chembl/api/data/activity` - данные активностей с пагинацией

**Извлекаемые поля**:

| Поле | Описание |
|------|----------|
| activity_chembl_id | Уникальный идентификатор активности |
| assay_chembl_id | Связь с ассаем |
| molecule_chembl_id | Связь с соединением |
| target_chembl_id | Связь с мишенью |
| document_chembl_id | Связь с документом |
| standard_type | Стандартизованный тип активности (IC50, Ki, etc.) |
| standard_value | Стандартизованное значение |
| standard_units | Стандартизованные единицы |
| pchembl_value | -log10(стандартизованное значение) |

**Нормализованные поля**: Стандартизация единиц измерения и вычисление pChEMBL значений

## Детальные схемы таблиц

### document_dim

| Поле | Тип | Описание | Источник | Ограничения | Единицы |
|------|-----|----------|----------|-------------|---------|
| document_chembl_id | STRING | PK документа | ChEMBL API | NOT NULL, UNIQUE, ^CHEMBL\d+$ | - |
| doi | STRING | Digital Object Identifier | Crossref/OpenAlex/PubMed | UNIQUE, ^10\.\d+/[^\s]+$ | - |
| pmid | STRING | PubMed ID | PubMed/Semantic Scholar | UNIQUE, ^\d+$ | - |
| title | TEXT | Название публикации | Все источники | nullable | - |
| abstract | TEXT | Аннотация | Все источники | nullable | - |
| authors | TEXT | Авторы | Все источники | nullable | - |
| journal | STRING | Название журнала | Все источники | nullable | - |
| year | INT | Год публикации | Все источники | 1900-текущий год | - |
| volume | STRING | Том журнала | Все источники | nullable | - |
| issue | STRING | Номер выпуска | Все источники | nullable | - |
| first_page | STRING | Первая страница | Все источники | nullable | - |
| last_page | STRING | Последняя страница | Все источники | nullable | - |
| publication_type | STRING | Тип публикации | Все источники | nullable | - |
| is_oa | BOOL | Open Access флаг | Crossref/OpenAlex | boolean | - |
| license | STRING | Лицензия | Crossref/OpenAlex | nullable | - |
| citation_count | INT | Количество цитирований | OpenAlex/Semantic Scholar | nullable, >= 0 | - |

**Справочники**:

- publication_type: article, patent, review, conference
- is_oa: true/false
- license: CC-BY, CC-BY-SA, etc.

### target_dim

| Поле | Тип | Описание | Источник | Ограничения | Единицы |
|------|-----|----------|----------|-------------|---------|
| target_chembl_id | STRING | PK мишени | ChEMBL API | NOT NULL, UNIQUE, ^CHEMBL\d+$ | - |
| target_type | STRING | Тип мишени | ChEMBL | nullable | - |
| pref_name | STRING | Предпочтительное название | ChEMBL | nullable | - |
| organism | STRING | Организм | UniProt | nullable | - |
| taxonomy_id | INT | Таксономический ID | ChEMBL/UniProt | nullable, > 0 | - |
| uniprot_accession | STRING | UniProt ID | UniProt | UNIQUE, UniProt format | - |
| gene_symbol | STRING | Символ гена | UniProt | nullable | - |
| protein_name | STRING | Название белка | UniProt | nullable | - |
| iuphar_target_id | INT | IUPHAR ID | IUPHAR | UNIQUE, > 0 | - |
| iuphar_name | STRING | Название по IUPHAR | IUPHAR | nullable | - |
| target_family | STRING | Семейство мишени | ChEMBL/IUPHAR | nullable | - |
| target_class | STRING | Класс мишени | ChEMBL/IUPHAR | nullable | - |
| sequence_length | INT | Длина последовательности | UniProt | nullable, > 0 | аминокислоты |

**Справочники**:

- target_type: SINGLE PROTEIN, PROTEIN FAMILY, PROTEIN COMPLEX
- organism: научные названия видов
- target_family: GPCR, Ion Channel, Enzyme, etc.

### assay_dim

| Поле | Тип | Описание | Источник | Ограничения | Единицы |
|------|-----|----------|----------|-------------|---------|
| assay_chembl_id | STRING | PK ассая | ChEMBL API | NOT NULL, UNIQUE, ^CHEMBL\d+$ | - |
| target_chembl_id | STRING | FK к target_dim | ChEMBL API | NOT NULL, FK | - |
| assay_type | STRING | Тип ассая | ChEMBL | nullable | - |
| assay_category | STRING | Категория ассая | ChEMBL | nullable | - |
| relationship_type | STRING | Тип связи | ChEMBL | nullable | - |
| description | TEXT | Описание ассая | ChEMBL | nullable | - |
| assay_format | STRING | Формат ассая | ChEMBL | nullable | - |
| assay_protocol | TEXT | Протокол ассая | ChEMBL | nullable | - |
| assay_cell_type | STRING | Тип клеток | ChEMBL | nullable | - |
| assay_tissue | STRING | Тип ткани | ChEMBL | nullable | - |
| confidence_score | DECIMAL | Уровень уверенности | ChEMBL | nullable, 0-1 | - |
| bao_label | STRING | BAO классификация | ChEMBL | nullable | - |

**Справочники**:

- assay_type: B (Binding), F (Functional), A (ADMET), P (Physicochemical), T (Toxicity), U (Unclassified)
- relationship_type: functional, binding, inhibitory
- confidence_score: 0.0-1.0

### testitem_dim

| Поле | Тип | Описание | Источник | Ограничения | Единицы |
|------|-----|----------|----------|-------------|---------|
| molecule_chembl_id | STRING | PK соединения | ChEMBL API | NOT NULL, UNIQUE, ^CHEMBL\d+$ | - |
| canonical_smiles | STRING | Канонические SMILES | ChEMBL/PubChem | nullable | - |
| standard_inchi | STRING | Стандартный InChI | ChEMBL/PubChem | nullable | - |
| standard_inchi_key | STRING | Стандартный InChI Key | ChEMBL/PubChem | nullable, InChIKey format | - |
| full_mwt | DECIMAL | Молекулярная масса | ChEMBL | nullable, 50-2000 | г/моль |
| alogp | DECIMAL | ALogP значение | ChEMBL | nullable | безразмерный |
| hba | INT | Акцепторы водорода | ChEMBL | nullable, >= 0 | количество |
| hbd | INT | Доноры водорода | ChEMBL | nullable, >= 0 | количество |
| psa | DECIMAL | Полярная площадь поверхности | ChEMBL | nullable, >= 0 | Å² |
| rtb | INT | Вращающиеся связи | ChEMBL | nullable, >= 0 | количество |
| pubchem_cid | INT | PubChem CID | PubChem | nullable, > 0 | - |
| iupac_name | STRING | IUPAC название | PubChem | nullable | - |

**Справочники**:

- molecular_weight: 50-2000 г/моль (разумный диапазон для лекарственных соединений)
- LogP: безразмерный (логарифм коэффициента распределения)
- HBA/HBD: количество атомов
- PSA: полярная площадь поверхности в квадратных ангстремах

### activity_fact

| Поле | Тип | Описание | Источник | Ограничения | Единицы |
|------|-----|----------|----------|-------------|---------|
| activity_chembl_id | STRING | PK активности | ChEMBL API | NOT NULL, UNIQUE, ^CHEMBL\d+$ | - |
| assay_chembl_id | STRING | FK к assay_dim | ChEMBL API | NOT NULL, FK | - |
| molecule_chembl_id | STRING | FK к testitem_dim | ChEMBL API | NOT NULL, FK | - |
| target_chembl_id | STRING | FK к target_dim | ChEMBL API | NOT NULL, FK | - |
| document_chembl_id | STRING | FK к document_dim | ChEMBL API | NOT NULL, FK | - |
| standard_type | STRING | Тип активности | ChEMBL | NOT NULL | - |
| standard_relation | STRING | Отношение | ChEMBL | nullable | - |
| standard_value | DECIMAL | Стандартизованное значение | ChEMBL | NOT NULL, >= 1e-12, <= 1e-3 | - |
| standard_units | STRING | Стандартизованные единицы | ChEMBL | NOT NULL | - |
| pchembl_value | DECIMAL | -log10(значение) | вычисляется | nullable, 3.0-12.0 | безразмерный |
| data_validity_comment | STRING | Комментарий валидности | ChEMBL | nullable | - |
| bao_label | STRING | BAO классификация | ChEMBL | nullable | - |

**Справочники**:

- standard_type: IC50, EC50, Ki, Kd, AC50
- standard_relation: =, >, <, >=, <=
- standard_units: nM, uM, mM, M, %
- pchembl_value: -log10(стандартизованное значение в моль/л)

## Связи между таблицами

```text
activity_fact
├── assay_chembl_id → assay_dim.assay_chembl_id
├── molecule_chembl_id → testitem_dim.molecule_chembl_id
├── target_chembl_id → target_dim.target_chembl_id
└── document_chembl_id → document_dim.document_chembl_id

assay_dim
└── target_chembl_id → target_dim.target_chembl_id
```

## Валидация данных

### Критерии качества

1. **Первичные ключи**: Все PK должны быть уникальными и не NULL
2. **Внешние ключи**: Все FK должны ссылаться на существующие записи
3. **Диапазоны значений**:
   - Годы: 1900-текущий год
   - Молекулярная масса: 50-2000 г/моль
   - pChEMBL: 3.0-12.0
   - Стандартные значения активности: 1e-12 - 1e-3
4. **Форматы**:
   - ChEMBL ID: ^CHEMBL\d+$
   - DOI: ^10\.\d+/[^\s]+$
   - PMID: ^\d+$
   - InChI Key: стандартный формат

### Профили качества

- **default**: базовые проверки для всех полей
- **strict**: строгие проверки с остановкой при ошибках
- **permissive**: минимальные проверки для быстрой обработки

## Единицы измерения

| Параметр | Единица | Описание |
|----------|---------|----------|
| Молекулярная масса | г/моль | Атомные единицы массы |
| Концентрация | nM, uM, mM, M | Наномоль, микромоль, миллимоль, моль на литр |
| LogP | безразмерный | Логарифм коэффициента распределения |
| PSA | Å² | Полярная площадь поверхности в квадратных ангстремах |
| HBA/HBD | количество | Количество атомов |
| pChEMBL | безразмерный | -log10(концентрация в моль/л) |

## Версионирование схемы

- **Версия 2.0**: Текущая схема с расширенными метаданными и мульти-источниковой интеграцией
- **Версия 1.0**: Базовая схема только с ChEMBL данными

## Мониторинг и телеметрия

Система собирает метрики по:
- Количеству обработанных записей
- Времени выполнения запросов
- Процент успешных извлечений данных
- Качество данных по источникам
