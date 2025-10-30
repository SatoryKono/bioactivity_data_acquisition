# Сопоставление колонок и источников данных для пайплайнов

**Дата создания:** 2025-01-27  
**Версия:** 1.0

## Обзор

Документ содержит детальное сопоставление колонок выходных таблиц с источниками данных, запросами, извлекаемыми параметрами, типами данных, нормализацией и валидацией для всех 5 пайплайнов проекта BioETL.

## Общие нормализаторы

Проект использует унифицированную систему нормализации через `NormalizerRegistry`:

### NumericNormalizer (`registry.normalize("numeric", value)`)

- **normalize_int()** - приведение к целым числам
- **normalize_float()** - приведение к числам с плавающей точкой  
- **normalize_bool()** - нормализация булевых значений
- **normalize_units()** - стандартизация единиц измерения
- **normalize_relation()** - нормализация операторов сравнения

### BooleanNormalizer (`registry.normalize("boolean", value)`)

- **normalize()** - строгая нормализация булевых значений
- **normalize_with_default()** - нормализация с значением по умолчанию

### StringNormalizer (`registry.normalize("string", value)`)

- **normalize()** - базовая нормализация строк
- Поддержка параметров: `uppercase`, `max_length`, `trim`

### IdentifierNormalizer (`registry.normalize("identifier", value)`)

- **normalize()** - нормализация идентификаторов (ChEMBL ID, DOI, etc.)

### ChemistryNormalizer (`registry.normalize("chemistry", value)`)

- **normalize()** - нормализация химических представлений (SMILES, InChI)

### Общие утилиты

- **coerce_nullable_int_columns()** - приведение к nullable integer типам
- **coerce_nullable_float_columns()** - приведение к nullable float типам  
- **coerce_optional_bool()** - приведение к nullable boolean типам
- **canonical_json()** - каноническая сериализация JSON
- **normalize_json_list()** - нормализация списков JSON объектов

## Структура документа

- [Activity Pipeline](#activity-pipeline)
- [Assay Pipeline](#assay-pipeline)
- [TestItem Pipeline](#testitem-pipeline)
- [Document Pipeline](#document-pipeline)
- [Target Pipeline](#target-pipeline)

---

## Activity Pipeline

**Источник данных:** ChEMBL API  
**Endpoint:** `GET /activity.json?activity_id__in=...`  
**Batch size:** 25 записей

### Системные поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `index` | Генерируется | - | `int` | Последовательный счетчик | `Series[int]` |
| `hash_row` | Генерируется | - | `str` | SHA256 от канонической строки | `Series[str]` |
| `hash_business_key` | Генерируется | - | `str` | SHA256 от `activity_id` | `Series[str]` |
| `pipeline_version` | Конфиг | - | `str` | Из конфигурации | `Series[str]` |
| `source_system` | Константа | - | `str` | "chembl" | `Series[str]` |
| `chembl_release` | `/status.json` | `chembl_db_version` | `str` | Из статуса API | `Series[str]` |
| `extracted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |

### Основные поля активности

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `activity_id` | `/activity.json` | `activity_id` | `int` | `registry.normalize("numeric", value)` | `Series[int]` (NOT NULL) |
| `molecule_chembl_id` | `/activity.json` | `molecule_chembl_id` | `str` | `_normalize_chembl_id()` | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `assay_chembl_id` | `/activity.json` | `assay_chembl_id` | `str` | `_normalize_chembl_id()` | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `target_chembl_id` | `/activity.json` | `target_chembl_id` | `str` | `_normalize_chembl_id()` | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `document_chembl_id` | `/activity.json` | `document_chembl_id` | `str` | `_normalize_chembl_id()` | `Series[str]` (regex: `^CHEMBL\d+$`) |

### Опубликованные данные активности

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `published_type` | `/activity.json` | `type` или `published_type` | `str` | `registry.normalize("string", value, uppercase=True)` | `Series[str]` |
| `published_relation` | `/activity.json` | `relation` или `published_relation` | `str` | `registry.normalize("numeric", value).normalize_relation()` | `Series[str]` (isin: `["=", ">", "<", ">=", "<="]`) |
| `published_value` | `/activity.json` | `value` или `published_value` | `float` | `registry.normalize("numeric", value)` | `Series[float]` (ge=0) |
| `published_units` | `/activity.json` | `units` или `published_units` | `str` | `registry.normalize("numeric", value).normalize_units()` | `Series[str]` |

### Стандартизированные данные активности

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `standard_type` | `/activity.json` | `standard_type` | `str` | `registry.normalize("string", value, uppercase=True)` | `Series[str]` |
| `standard_relation` | `/activity.json` | `standard_relation` | `str` | `registry.normalize("numeric", value).normalize_relation()` | `Series[str]` (isin: `["=", ">", "<", ">=", "<="]`) |
| `standard_value` | `/activity.json` | `standard_value` | `float` | `registry.normalize("numeric", value)` | `Series[float]` (ge=0) |
| `standard_units` | `/activity.json` | `standard_units` | `str` | `registry.normalize("numeric", value).normalize_units(default="nM")` | `Series[str]` (isin: `STANDARD_UNITS_ALLOWED`) |
| `standard_flag` | `/activity.json` | `standard_flag` | `int` | `registry.normalize("numeric", value)` | `Series[pd.Int64Dtype]` |
| `pchembl_value` | `/activity.json` | `pchembl_value` | `float` | `registry.normalize("numeric", value)` | `Series[float]` (ge=0) |

### Границы и цензурирование

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `lower_bound` | `/activity.json` | `standard_lower_value` или `lower_value` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |
| `upper_bound` | `/activity.json` | `standard_upper_value` или `upper_value` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |
| `is_censored` | Вычисляется | - | `bool` | `_derive_is_censored(standard_relation)` | `Series[pd.BooleanDtype]` |

### Комментарии и метаданные

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `activity_comment` | `/activity.json` | `activity_comment` | `str` | `registry.normalize("string", value)` | `Series[str]` |
| `data_validity_comment` | `/activity.json` | `data_validity_comment` | `str` | `registry.normalize("string", value)` | `Series[str]` |

### BAO аннотации

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `bao_endpoint` | `/activity.json` | `bao_endpoint` | `str` | `_normalize_bao_id()` | `Series[str]` |
| `bao_format` | `/activity.json` | `bao_format` | `str` | `_normalize_bao_id()` | `Series[str]` |
| `bao_label` | `/activity.json` | `bao_label` | `str` | `registry.normalize("string", value, max_length=128)` | `Series[str]` |

### Дополнительные поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `canonical_smiles` | `/activity.json` | `canonical_smiles` | `str` | `registry.normalize("string", value)` | `Series[str]` |
| `target_organism` | `/activity.json` | `target_organism` | `str` | `_normalize_target_organism()` | `Series[str]` |
| `target_tax_id` | `/activity.json` | `target_tax_id` | `int` | `registry.normalize("numeric", value)` | `Series[pd.Int64Dtype]` (ge=1) |
| `potential_duplicate` | `/activity.json` | `potential_duplicate` | `int` | `registry.normalize("numeric", value)` | `Series[pd.Int64Dtype]` (isin: `[0, 1]`) |
| `uo_units` | `/activity.json` | `uo_units` | `str` | `registry.normalize("string", value, uppercase=True)` | `Series[str]` (regex: `^UO_\d{7}$`) |
| `qudt_units` | `/activity.json` | `qudt_units` | `str` | `registry.normalize("string", value)` | `Series[str]` |
| `src_id` | `/activity.json` | `src_id` | `int` | `registry.normalize("numeric", value)` | `Series[pd.Int64Dtype]` |
| `action_type` | `/activity.json` | `action_type` | `str` | `registry.normalize("string", value)` | `Series[str]` |

### Свойства активности и эффективность лиганда

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `activity_properties` | `/activity.json` | `activity_properties` | `str` | `normalize_json_list()` | `Series[str]` |
| `compound_key` | Вычисляется | - | `str` | `_derive_compound_key()` | `Series[str]` |
| `is_citation` | Вычисляется | - | `bool` | `_derive_is_citation()` | `Series[bool]` |
| `high_citation_rate` | Вычисляется | - | `bool` | `_derive_high_citation_rate()` | `Series[bool]` |
| `exact_data_citation` | Вычисляется | - | `bool` | `_derive_exact_data_citation()` | `Series[bool]` |
| `rounded_data_citation` | Вычисляется | - | `bool` | `_derive_rounded_data_citation()` | `Series[bool]` |
| `bei` | `/activity.json` | `ligand_efficiency.bei` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |
| `sei` | `/activity.json` | `ligand_efficiency.sei` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |
| `le` | `/activity.json` | `ligand_efficiency.le` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |
| `lle` | `/activity.json` | `ligand_efficiency.lle` | `float` | `registry.normalize("numeric", value)` | `Series[float]` |

---

## Assay Pipeline

**Источник данных:** ChEMBL API + Enrichment  
**Основной endpoint:** `GET /assay.json?assay_chembl_id__in=...`  
**Enrichment endpoints:** `/target/{id}.json`, `/assay_class/{id}.json`  
**Batch size:** 25 записей

### Системные поля (continued)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `index` | Генерируется | - | `int` | Последовательный счетчик | `Series[int]` |
| `hash_row` | Генерируется | - | `str` | SHA256 от канонической строки | `Series[str]` |
| `hash_business_key` | Генерируется | - | `str` | SHA256 от `assay_chembl_id` | `Series[str]` |
| `pipeline_version` | Конфиг | - | `str` | Из конфигурации | `Series[str]` |
| `source_system` | Константа | - | `str` | "chembl" | `Series[str]` |
| `chembl_release` | `/status.json` | `chembl_db_version` | `str` | Из статуса API | `Series[str]` |
| `extracted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |

### Основные поля ассая

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `assay_chembl_id` | `/assay.json` | `assay_chembl_id` | `str` | Прямое извлечение | `Series[str]` (NOT NULL, regex: `^CHEMBL\d+$`) |
| `row_subtype` | Генерируется | - | `str` | "assay" | `Series[str]` (NOT NULL) |
| `row_index` | Генерируется | - | `int` | Индекс для детерминизма | `Series[pd.Int64Dtype]` (NOT NULL, ge=0) |
| `assay_type` | `/assay.json` | `assay_type` | `str` | `registry.normalize("string", value)` | `Series[str]` |
| `assay_category` | `/assay.json` | `assay_category` | `str` | Прямое извлечение | `Series[str]` |
| `assay_cell_type` | `/assay.json` | `assay_cell_type` | `str` | Прямое извлечение | `Series[str]` |
| `assay_classifications` | `/assay.json` | `assay_classifications` | `str` | JSON сериализация | `Series[str]` |
| `assay_group` | `/assay.json` | `assay_group` | `str` | Прямое извлечение | `Series[str]` |
| `assay_organism` | `/assay.json` | `assay_organism` | `str` | Прямое извлечение | `Series[str]` |
| `assay_parameters_json` | `/assay.json` | `assay_parameters` | `str` | JSON сериализация | `Series[str]` |
| `assay_strain` | `/assay.json` | `assay_strain` | `str` | Прямое извлечение | `Series[str]` |
| `assay_subcellular_fraction` | `/assay.json` | `assay_subcellular_fraction` | `str` | Прямое извлечение | `Series[str]` |
| `assay_tax_id` | `/assay.json` | `assay_tax_id` | `int` | `coerce_nullable_int_columns()` | `Series[pd.Int64Dtype]` (ge=0) |
| `assay_test_type` | `/assay.json` | `assay_test_type` | `str` | Прямое извлечение | `Series[str]` |
| `assay_tissue` | `/assay.json` | `assay_tissue` | `str` | Прямое извлечение | `Series[str]` |
| `assay_type_description` | `/assay.json` | `assay_type_description` | `str` | Прямое извлечение | `Series[str]` |

### BAO поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `bao_format` | `/assay.json` | `bao_format` | `str` | Прямое извлечение | `Series[str]` (regex: `^BAO_\d+$`) |
| `bao_label` | `/assay.json` | `bao_label` | `str` | `registry.normalize("chemistry.string", value, max_length=128)` | `Series[str]` |
| `bao_endpoint` | `/assay.json` | `bao_endpoint` | `str` | Прямое извлечение | `Series[str]` (regex: `^BAO_\d{7}$`) |

### Связи

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `cell_chembl_id` | `/assay.json` | `cell_chembl_id` | `str` | Прямое извлечение | `Series[str]` |
| `confidence_description` | `/assay.json` | `confidence_description` | `str` | Прямое извлечение | `Series[str]` |
| `confidence_score` | `/assay.json` | `confidence_score` | `int` | `coerce_nullable_int_columns()` | `Series[pd.Int64Dtype]` (ge=0, le=9) |
| `assay_description` | `/assay.json` | `description` | `str` | `registry.normalize("chemistry.string", value)` | `Series[str]` |
| `document_chembl_id` | `/assay.json` | `document_chembl_id` | `str` | Прямое извлечение | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `relationship_description` | `/assay.json` | `relationship_description` | `str` | Прямое извлечение | `Series[str]` |
| `relationship_type` | `/assay.json` | `relationship_type` | `str` | Прямое извлечение | `Series[str]` |
| `src_assay_id` | `/assay.json` | `src_assay_id` | `str` | Прямое извлечение | `Series[str]` |
| `src_id` | `/assay.json` | `src_id` | `int` | `coerce_nullable_int_columns()` | `Series[pd.Int64Dtype]` |
| `target_chembl_id` | `/assay.json` | `target_chembl_id` | `str` | Прямое извлечение | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `tissue_chembl_id` | `/assay.json` | `tissue_chembl_id` | `str` | Прямое извлечение | `Series[str]` |
| `variant_sequence_json` | `/assay.json` | `variant_sequence` | `str` | JSON сериализация | `Series[str]` |

### Target Enrichment (whitelist)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pref_name` | `/target/{id}.json` | `target.pref_name` | `str` | Прямое извлечение | `Series[str]` |
| `organism` | `/target/{id}.json` | `target.organism` | `str` | Прямое извлечение | `Series[str]` |
| `target_type` | `/target/{id}.json` | `target.target_type` | `str` | Прямое извлечение | `Series[str]` |
| `species_group_flag` | `/target/{id}.json` | `target.species_group_flag` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0, le=1) |
| `tax_id` | `/target/{id}.json` | `target.tax_id` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `component_count` | `/target/{id}.json` | `target.component_count` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |

### Assay Parameters (explode из JSON)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `assay_param_type` | `/assay.json` | `assay_parameters[].type` | `str` | Explode из JSON | `Series[str]` |
| `assay_param_relation` | `/assay.json` | `assay_parameters[].relation` | `str` | Explode из JSON | `Series[str]` |
| `assay_param_value` | `/assay.json` | `assay_parameters[].value` | `float` | Explode из JSON | `Series[pd.Float64Dtype]` |
| `assay_param_units` | `/assay.json` | `assay_parameters[].units` | `str` | Explode из JSON | `Series[str]` |
| `assay_param_text_value` | `/assay.json` | `assay_parameters[].text_value` | `str` | Explode из JSON | `Series[str]` |
| `assay_param_standard_type` | `/assay.json` | `assay_parameters[].standard_type` | `str` | Explode из JSON | `Series[str]` |
| `assay_param_standard_value` | `/assay.json` | `assay_parameters[].standard_value` | `float` | Explode из JSON | `Series[pd.Float64Dtype]` |
| `assay_param_standard_units` | `/assay.json` | `assay_parameters[].standard_units` | `str` | Explode из JSON | `Series[str]` |

### Assay Class (из /assay_class endpoint)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `assay_class_id` | `/assay_class/{id}.json` | `assay_class.assay_class_id` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` |
| `assay_class_bao_id` | `/assay_class/{id}.json` | `assay_class.bao_id` | `str` | Прямое извлечение | `Series[str]` (regex: `^BAO_\d{7}$`) |
| `assay_class_type` | `/assay_class/{id}.json` | `assay_class.assay_class_type` | `str` | Прямое извлечение | `Series[str]` |
| `assay_class_l1` | `/assay_class/{id}.json` | `assay_class.class_level_1` | `str` | Прямое извлечение | `Series[str]` |
| `assay_class_l2` | `/assay_class/{id}.json` | `assay_class.class_level_2` | `str` | Прямое извлечение | `Series[str]` |
| `assay_class_l3` | `/assay_class/{id}.json` | `assay_class.class_level_3` | `str` | Прямое извлечение | `Series[str]` |
| `assay_class_description` | `/assay_class/{id}.json` | `assay_class.assay_class_description` | `str` | Прямое извлечение | `Series[str]` |

### Variant Sequences (explode из JSON)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `variant_id` | `/assay.json` | `variant_sequence[].variant_id` | `int` | Explode из JSON | `Series[pd.Int64Dtype]` |
| `variant_base_accession` | `/assay.json` | `variant_sequence[].base_accession` | `str` | Explode из JSON | `Series[str]` |
| `variant_mutation` | `/assay.json` | `variant_sequence[].mutation` | `str` | Explode из JSON | `Series[str]` |
| `variant_sequence` | `/assay.json` | `variant_sequence[].variant_seq` | `str` | Explode из JSON | `Series[str]` (regex: `^[A-Z\*]+$`) |
| `variant_accession_reported` | `/assay.json` | `variant_sequence[].accession_reported` | `str` | Explode из JSON | `Series[str]` |

---

## TestItem Pipeline

**Источник данных:** ChEMBL API + PubChem Enrichment  
**Основной endpoint:** `GET /molecule.json?molecule_chembl_id__in=...`  
**Enrichment:** PubChem PUG-REST API  
**Batch size:** 25 записей (ChEMBL), 100 записей (PubChem)

### Системные поля (continued) (continued)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pipeline_version` | Конфиг | - | `str` | Из конфигурации | `Series[str]` |
| `source_system` | Константа | - | `str` | "chembl" | `Series[str]` |
| `chembl_release` | `/status.json` | `chembl_db_version` | `str` | Из статуса API | `Series[str]` |
| `extracted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |
| `hash_business_key` | Генерируется | - | `str` | SHA256 от `molecule_chembl_id` | `Series[str]` |
| `hash_row` | Генерируется | - | `str` | SHA256 от канонической строки | `Series[str]` |
| `index` | Генерируется | - | `int` | Последовательный счетчик | `Series[int]` |

### Основные поля молекулы

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `molecule_chembl_id` | `/molecule.json` | `molecule_chembl_id` | `str` | Прямое извлечение | `Series[str]` (NOT NULL, regex: `^CHEMBL\d+$`) |
| `molregno` | `/molecule.json` | `molregno` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=1) |
| `pref_name` | `/molecule.json` | `pref_name` | `str` | `registry.normalize("chemistry.string", value)` | `Series[str]` |
| `pref_name_key` | Вычисляется | - | `str` | Нормализованный ключ | `Series[str]` |
| `parent_chembl_id` | `/molecule.json` | `parent_chembl_id` | `str` | Прямое извлечение | `Series[str]` (regex: `^CHEMBL\d+$`) |
| `parent_molregno` | `/molecule.json` | `parent_molregno` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=1) |

### Флаги и классификация

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `therapeutic_flag` | `/molecule.json` | `therapeutic_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `structure_type` | `/molecule.json` | `structure_type` | `str` | Прямое извлечение | `Series[str]` |
| `molecule_type` | `/molecule.json` | `molecule_type` | `str` | Прямое извлечение | `Series[str]` |
| `molecule_type_chembl` | `/molecule.json` | `molecule_type_chembl` | `str` | Прямое извлечение | `Series[str]` |
| `max_phase` | `/molecule.json` | `max_phase` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `first_approval` | `/molecule.json` | `first_approval` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `dosed_ingredient` | `/molecule.json` | `dosed_ingredient` | `bool` | Прямое извлечение | `Series[bool]` |
| `availability_type` | `/molecule.json` | `availability_type` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |

### Химические свойства

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `chirality` | `/molecule.json` | `chirality` | `str` | Прямое извлечение | `Series[str]` |
| `chirality_chembl` | `/molecule.json` | `chirality_chembl` | `str` | Прямое извлечение | `Series[str]` |
| `mechanism_of_action` | `/molecule.json` | `mechanism_of_action` | `str` | Прямое извлечение | `Series[str]` |
| `direct_interaction` | `/molecule.json` | `direct_interaction` | `bool` | Прямое извлечение | `Series[bool]` |
| `molecular_mechanism` | `/molecule.json` | `molecular_mechanism` | `bool` | Прямое извлечение | `Series[bool]` |

### Административные пути

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `oral` | `/molecule.json` | `oral` | `bool` | Прямое извлечение | `Series[bool]` |
| `parenteral` | `/molecule.json` | `parenteral` | `bool` | Прямое извлечение | `Series[bool]` |
| `topical` | `/molecule.json` | `topical` | `bool` | Прямое извлечение | `Series[bool]` |
| `black_box_warning` | `/molecule.json` | `black_box_warning` | `bool` | Прямое извлечение | `Series[bool]` |
| `natural_product` | `/molecule.json` | `natural_product` | `bool` | Прямое извлечение | `Series[bool]` |
| `first_in_class` | `/molecule.json` | `first_in_class` | `bool` | Прямое извлечение | `Series[bool]` |
| `prodrug` | `/molecule.json` | `prodrug` | `bool` | Прямое извлечение | `Series[bool]` |
| `inorganic_flag` | `/molecule.json` | `inorganic_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `polymer_flag` | `/molecule.json` | `polymer_flag` | `bool` | Прямое извлечение | `Series[bool]` |

### USAN классификация

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `usan_year` | `/molecule.json` | `usan_year` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `usan_stem` | `/molecule.json` | `usan_stem` | `str` | Прямое извлечение | `Series[str]` |
| `usan_substem` | `/molecule.json` | `usan_substem` | `str` | Прямое извлечение | `Series[str]` |
| `usan_stem_definition` | `/molecule.json` | `usan_stem_definition` | `str` | Прямое извлечение | `Series[str]` |
| `indication_class` | `/molecule.json` | `indication_class` | `str` | Прямое извлечение | `Series[str]` |

### Отзыв с рынка

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `withdrawn_flag` | `/molecule.json` | `withdrawn_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `withdrawn_year` | `/molecule.json` | `withdrawn_year` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `withdrawn_country` | `/molecule.json` | `withdrawn_country` | `str` | Прямое извлечение | `Series[str]` |
| `withdrawn_reason` | `/molecule.json` | `withdrawn_reason` | `str` | Прямое извлечение | `Series[str]` |

### Drug информация

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `drug_chembl_id` | `/molecule.json` | `drug_chembl_id` | `str` | Прямое извлечение | `Series[str]` |
| `drug_name` | `/molecule.json` | `drug_name` | `str` | Прямое извлечение | `Series[str]` |
| `drug_type` | `/molecule.json` | `drug_type` | `str` | Прямое извлечение | `Series[str]` |
| `drug_substance_flag` | `/molecule.json` | `drug_substance_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_indication_flag` | `/molecule.json` | `drug_indication_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antibacterial_flag` | `/molecule.json` | `drug_antibacterial_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antiviral_flag` | `/molecule.json` | `drug_antiviral_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antifungal_flag` | `/molecule.json` | `drug_antifungal_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antiparasitic_flag` | `/molecule.json` | `drug_antiparasitic_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antineoplastic_flag` | `/molecule.json` | `drug_antineoplastic_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_immunosuppressant_flag` | `/molecule.json` | `drug_immunosuppressant_flag` | `bool` | Прямое извлечение | `Series[bool]` |
| `drug_antiinflammatory_flag` | `/molecule.json` | `drug_antiinflammatory_flag` | `bool` | Прямое извлечение | `Series[bool]` |

### Молекулярные свойства

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `mw_freebase` | `/molecule.json` | `mw_freebase` | `float` | Прямое извлечение | `Series[float]` (ge=0) |
| `alogp` | `/molecule.json` | `alogp` | `float` | Прямое извлечение | `Series[float]` |
| `hba` | `/molecule.json` | `hba` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `hbd` | `/molecule.json` | `hbd` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `psa` | `/molecule.json` | `psa` | `float` | Прямое извлечение | `Series[float]` (ge=0) |
| `rtb` | `/molecule.json` | `rtb` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `ro3_pass` | `/molecule.json` | `ro3_pass` | `bool` | Прямое извлечение | `Series[bool]` |
| `num_ro5_violations` | `/molecule.json` | `num_ro5_violations` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `acd_most_apka` | `/molecule.json` | `acd_most_apka` | `float` | Прямое извлечение | `Series[float]` |
| `acd_most_bpka` | `/molecule.json` | `acd_most_bpka` | `float` | Прямое извлечение | `Series[float]` |
| `acd_logp` | `/molecule.json` | `acd_logp` | `float` | Прямое извлечение | `Series[float]` |
| `acd_logd` | `/molecule.json` | `acd_logd` | `float` | Прямое извлечение | `Series[float]` |
| `molecular_species` | `/molecule.json` | `molecular_species` | `str` | Прямое извлечение | `Series[str]` |
| `full_mwt` | `/molecule.json` | `full_mwt` | `float` | Прямое извлечение | `Series[float]` (ge=0) |
| `aromatic_rings` | `/molecule.json` | `aromatic_rings` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `heavy_atoms` | `/molecule.json` | `heavy_atoms` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `qed_weighted` | `/molecule.json` | `qed_weighted` | `float` | Прямое извлечение | `Series[float]` |
| `mw_monoisotopic` | `/molecule.json` | `mw_monoisotopic` | `float` | Прямое извлечение | `Series[float]` (ge=0) |
| `full_molformula` | `/molecule.json` | `full_molformula` | `str` | Прямое извлечение | `Series[str]` |
| `hba_lipinski` | `/molecule.json` | `hba_lipinski` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `hbd_lipinski` | `/molecule.json` | `hbd_lipinski` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `num_lipinski_ro5_violations` | `/molecule.json` | `num_lipinski_ro5_violations` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `lipinski_ro5_violations` | `/molecule.json` | `lipinski_ro5_violations` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=0) |
| `lipinski_ro5_pass` | `/molecule.json` | `lipinski_ro5_pass` | `bool` | Прямое извлечение | `Series[bool]` |

### Структурные данные

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `standardized_smiles` | `/molecule.json` | `standardized_smiles` | `str` | Прямое извлечение | `Series[str]` |
| `standard_inchi` | `/molecule.json` | `standard_inchi` | `str` | Прямое извлечение | `Series[str]` |
| `standard_inchi_key` | `/molecule.json` | `standard_inchi_key` | `str` | Прямое извлечение | `Series[str]` |

### JSON поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `all_names` | `/molecule.json` | `all_names` | `str` | Прямое извлечение | `Series[str]` |
| `molecule_hierarchy` | `/molecule.json` | `molecule_hierarchy` | `str` | `canonical_json()` | `Series[str]` |
| `molecule_properties` | `/molecule.json` | `molecule_properties` | `str` | `canonical_json()` | `Series[str]` |
| `molecule_structures` | `/molecule.json` | `molecule_structures` | `str` | `canonical_json()` | `Series[str]` |
| `molecule_synonyms` | `/molecule.json` | `molecule_synonyms` | `str` | `canonical_json()` | `Series[str]` |
| `atc_classifications` | `/molecule.json` | `atc_classifications` | `str` | JSON сериализация | `Series[str]` |
| `cross_references` | `/molecule.json` | `cross_references` | `str` | JSON сериализация | `Series[str]` |
| `biotherapeutic` | `/molecule.json` | `biotherapeutic` | `str` | JSON сериализация | `Series[str]` |
| `chemical_probe` | `/molecule.json` | `chemical_probe` | `str` | JSON сериализация | `Series[str]` |
| `orphan` | `/molecule.json` | `orphan` | `str` | JSON сериализация | `Series[str]` |
| `veterinary` | `/molecule.json` | `veterinary` | `str` | JSON сериализация | `Series[str]` |
| `helm_notation` | `/molecule.json` | `helm_notation` | `str` | JSON сериализация | `Series[str]` |

### PubChem Enrichment

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pubchem_cid` | PubChem API | `CID` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` (ge=1) |
| `pubchem_molecular_formula` | PubChem API | `MolecularFormula` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_molecular_weight` | PubChem API | `MolecularWeight` | `float` | Прямое извлечение | `Series[float]` (ge=0) |
| `pubchem_canonical_smiles` | PubChem API | `CanonicalSMILES` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_isomeric_smiles` | PubChem API | `IsomericSMILES` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_inchi` | PubChem API | `InChI` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_inchi_key` | PubChem API | `InChIKey` | `str` | Прямое извлечение | `Series[str]` (regex: `^[A-Z]{14}-[A-Z]{10}-[A-Z]$`) |
| `pubchem_iupac_name` | PubChem API | `IUPACName` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_registry_id` | PubChem API | `RegistryID` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_rn` | PubChem API | `RN` | `str` | Прямое извлечение | `Series[str]` |
| `pubchem_synonyms` | PubChem API | `Synonyms` | `str` | JSON сериализация | `Series[str]` |
| `pubchem_enriched_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |
| `pubchem_cid_source` | Генерируется | - | `str` | Источник CID | `Series[str]` |
| `pubchem_fallback_used` | Генерируется | - | `bool` | Флаг fallback | `Series[bool]` |
| `pubchem_enrichment_attempt` | Генерируется | - | `int` | Номер попытки | `Series[pd.Int64Dtype]` (ge=0) |
| `pubchem_lookup_inchikey` | Генерируется | - | `str` | InChIKey для поиска | `Series[str]` |

### Fallback поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `fallback_error_code` | Генерируется | - | `str` | Код ошибки | `Series[str]` |
| `fallback_http_status` | Генерируется | - | `int` | HTTP статус | `Series[pd.Int64Dtype]` (ge=0) |
| `fallback_retry_after_sec` | Генерируется | - | `float` | Retry-After | `Series[float]` (ge=0) |
| `fallback_attempt` | Генерируется | - | `int` | Номер попытки | `Series[pd.Int64Dtype]` (ge=0) |
| `fallback_error_message` | Генерируется | - | `str` | Сообщение об ошибке | `Series[str]` |

---

## Document Pipeline

**Источник данных:** ChEMBL API + Multi-source Enrichment  
**Основной endpoint:** `GET /document.json?document_chembl_id__in=...`  
**Enrichment:** PubMed, Crossref, OpenAlex, Semantic Scholar  
**Batch size:** 10 записей (ChEMBL), 200 записей (PubMed), 100 записей (Crossref), 100 записей (OpenAlex), 50 записей (Semantic Scholar)

### Системные поля (continued) (continued)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `index` | Генерируется | - | `int` | Последовательный счетчик | `Series[int]` |
| `hash_row` | Генерируется | - | `str` | SHA256 от канонической строки | `Series[str]` |
| `hash_business_key` | Генерируется | - | `str` | SHA256 от `document_chembl_id` | `Series[str]` |
| `pipeline_version` | Конфиг | - | `str` | Из конфигурации | `Series[str]` |
| `source_system` | Константа | - | `str` | "chembl" | `Series[str]` |
| `chembl_release` | `/status.json` | `chembl_db_version` | `str` | Из статуса API | `Series[str]` |
| `extracted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |

### Основные поля документа

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `document_chembl_id` | `/document.json` | `document_chembl_id` | `str` | Прямое извлечение | `Series[str]` (NOT NULL) |
| `document_pubmed_id` | `/document.json` | `pubmed_id` | `int` | `int()` преобразование | `Series[int]` |
| `document_classification` | `/document.json` | `classification` | `str` | Прямое извлечение | `Series[str]` |
| `referenses_on_previous_experiments` | `/document.json` | `document_contains_external_links` | `bool` | `coerce_optional_bool()` | `Series[bool]` |
| `original_experimental_document` | `/document.json` | `is_experimental_doc` | `bool` | `coerce_optional_bool()` | `Series[bool]` |

### Resolved поля (с precedence)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pmid` | Multi-source | - | `int` | `merge_with_precedence()` | `Series[int]` |
| `pmid_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `doi_clean` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `doi_clean_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `title` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `title_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `abstract` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `abstract_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `journal` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `journal_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `journal_abbrev` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `journal_abbrev_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `authors` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `authors_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `year` | Multi-source | - | `int` | `merge_with_precedence()` | `Series[int]` |
| `year_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `volume` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `volume_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `issue` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `issue_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `first_page` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `first_page_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `last_page` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `last_page_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `issn_print` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `issn_print_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `issn_electronic` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `issn_electronic_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `is_oa` | Multi-source | - | `bool` | `merge_with_precedence()` | `Series[bool]` |
| `is_oa_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `oa_status` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `oa_status_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `oa_url` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `oa_url_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `citation_count` | Multi-source | - | `int` | `merge_with_precedence()` | `Series[int]` |
| `citation_count_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `influential_citations` | Multi-source | - | `int` | `merge_with_precedence()` | `Series[int]` |
| `influential_citations_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `fields_of_study` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `fields_of_study_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `concepts_top3` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `concepts_top3_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `mesh_terms` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `mesh_terms_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |
| `chemicals` | Multi-source | - | `str` | `merge_with_precedence()` | `Series[str]` |
| `chemicals_source` | Multi-source | - | `str` | Источник данных | `Series[str]` |

### Conflict поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `conflict_doi` | Вычисляется | - | `bool` | Обнаружение конфликтов | `Series[bool]` |
| `conflict_pmid` | Вычисляется | - | `bool` | Обнаружение конфликтов | `Series[bool]` |

### ChEMBL поля (с префиксом)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `chembl_pmid` | `/document.json` | `pubmed_id` | `int` | `int()` преобразование | `Series[int]` |
| `chembl_title` | `/document.json` | `title` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_abstract` | `/document.json` | `abstract` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_authors` | `/document.json` | `authors` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_doi` | `/document.json` | `doi` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_journal` | `/document.json` | `journal` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_year` | `/document.json` | `year` | `int` | `int()` преобразование | `Series[int]` (ge=1800, le=2100) |
| `chembl_volume` | `/document.json` | `volume` | `str` | Прямое извлечение | `Series[str]` |
| `chembl_issue` | `/document.json` | `issue` | `str` | Прямое извлечение | `Series[str]` |

### PubMed поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pubmed_pmid` | PubMed API | `uid` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_article_title` | PubMed API | `title` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_abstract` | PubMed API | `abstract` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_authors` | PubMed API | `authors` | `str` | JSON сериализация | `Series[str]` |
| `pubmed_doi` | PubMed API | `doi` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_doc_type` | PubMed API | `pubtype` | `str` | JSON сериализация | `Series[str]` |
| `pubmed_journal` | PubMed API | `source` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_volume` | PubMed API | `volume` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_issue` | PubMed API | `issue` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_first_page` | PubMed API | `pages` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_last_page` | PubMed API | `pages` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_issn` | PubMed API | `issn` | `str` | Прямое извлечение | `Series[str]` |
| `pubmed_mesh_descriptors` | PubMed API | `mesh_terms` | `str` | JSON сериализация | `Series[str]` |
| `pubmed_mesh_qualifiers` | PubMed API | `mesh_qualifiers` | `str` | JSON сериализация | `Series[str]` |
| `pubmed_chemical_list` | PubMed API | `chemicals` | `str` | JSON сериализация | `Series[str]` |
| `pubmed_year_completed` | PubMed API | `year_completed` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_month_completed` | PubMed API | `month_completed` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_day_completed` | PubMed API | `day_completed` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_year_revised` | PubMed API | `year_revised` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_month_revised` | PubMed API | `month_revised` | `int` | Прямое извлечение | `Series[int]` |
| `pubmed_day_revised` | PubMed API | `day_revised` | `int` | Прямое извлечение | `Series[int]` |

### Crossref поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `crossref_title` | Crossref API | `title[0]` | `str` | Прямое извлечение | `Series[str]` |
| `crossref_authors` | Crossref API | `author` | `str` | JSON сериализация | `Series[str]` |
| `crossref_doi` | Crossref API | `DOI` | `str` | Прямое извлечение | `Series[str]` |
| `crossref_doc_type` | Crossref API | `type` | `str` | Прямое извлечение | `Series[str]` |
| `crossref_subject` | Crossref API | `subject` | `str` | JSON сериализация | `Series[str]` |

### OpenAlex поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `openalex_pmid` | OpenAlex API | `pmids[0]` | `int` | Прямое извлечение | `Series[int]` |
| `openalex_title` | OpenAlex API | `title` | `str` | Прямое извлечение | `Series[str]` |
| `openalex_authors` | OpenAlex API | `authorships` | `str` | JSON сериализация | `Series[str]` |
| `openalex_doi` | OpenAlex API | `doi` | `str` | Прямое извлечение | `Series[str]` |
| `openalex_doc_type` | OpenAlex API | `type` | `str` | Прямое извлечение | `Series[str]` |
| `openalex_crossref_doc_type` | OpenAlex API | `crossref_type` | `str` | Прямое извлечение | `Series[str]` |
| `openalex_year` | OpenAlex API | `publication_year` | `int` | Прямое извлечение | `Series[int]` |
| `openalex_issn` | OpenAlex API | `host_venue.issn` | `str` | Прямое извлечение | `Series[str]` |

### Semantic Scholar поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `semantic_scholar_pmid` | Semantic Scholar API | `externalIds.PubMed` | `int` | Прямое извлечение | `Series[int]` |
| `semantic_scholar_title` | Semantic Scholar API | `title` | `str` | Прямое извлечение | `Series[str]` |
| `semantic_scholar_authors` | Semantic Scholar API | `authors` | `str` | JSON сериализация | `Series[str]` |
| `semantic_scholar_doi` | Semantic Scholar API | `externalIds.DOI` | `str` | Прямое извлечение | `Series[str]` |
| `semantic_scholar_doc_type` | Semantic Scholar API | `paperType` | `str` | Прямое извлечение | `Series[str]` |
| `semantic_scholar_journal` | Semantic Scholar API | `venue` | `str` | Прямое извлечение | `Series[str]` |
| `semantic_scholar_issn` | Semantic Scholar API | `venue` | `str` | Прямое извлечение | `Series[str]` |

### Error tracking поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `crossref_error` | Генерируется | - | `str` | Код ошибки | `Series[str]` |
| `openalex_error` | Генерируется | - | `str` | Код ошибки | `Series[str]` |
| `pubmed_error` | Генерируется | - | `str` | Код ошибки | `Series[str]` |
| `semantic_scholar_error` | Генерируется | - | `str` | Код ошибки | `Series[str]` |
| `error_type` | Генерируется | - | `str` | Тип ошибки | `Series[str]` |
| `error_message` | Генерируется | - | `str` | Сообщение об ошибке | `Series[str]` |
| `attempted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |

---

## Target Pipeline

**Источник данных:** ChEMBL API + UniProt + IUPHAR Enrichment  
**Основной endpoint:** `GET /target/{id}.json` (single record)  
**UniProt endpoints:** `/search`, `/idmapping`, `/uniprotkb/{accession}`  
**IUPHAR endpoint:** `/services/targets`  
**Batch size:** 25 записей (ChEMBL), 50 записей (UniProt), 200 записей (IUPHAR)

### Системные поля (continued) (continued)

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `pipeline_version` | Конфиг | - | `str` | Из конфигурации | `Series[str]` |
| `source_system` | Константа | - | `str` | "chembl" | `Series[str]` |
| `chembl_release` | `/status.json` | `chembl_db_version` | `str` | Из статуса API | `Series[str]` |
| `extracted_at` | Генерируется | - | `str` | ISO8601 timestamp | `Series[str]` |
| `hash_business_key` | Генерируется | - | `str` | SHA256 от `target_chembl_id` | `Series[str]` |
| `hash_row` | Генерируется | - | `str` | SHA256 от канонической строки | `Series[str]` |
| `index` | Генерируется | - | `int` | Последовательный счетчик | `Series[int]` |

### Основные поля таргета

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `target_chembl_id` | `/target/{id}.json` | `target_chembl_id` | `str` | Прямое извлечение | `Series[str]` (NOT NULL) |
| `isoform_ids` | `/target/{id}.json` | `isoform_ids` | `str` | JSON сериализация | `Series[str]` |
| `isoform_names` | `/target/{id}.json` | `isoform_names` | `str` | JSON сериализация | `Series[str]` |
| `isoforms` | `/target/{id}.json` | `isoforms` | `str` | JSON сериализация | `Series[str]` |
| `pref_name` | `/target/{id}.json` | `pref_name` | `str` | Прямое извлечение | `Series[str]` |
| `target_type` | `/target/{id}.json` | `target_type` | `str` | Прямое извлечение | `Series[str]` |

### Таксономия

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `organism` | `/target/{id}.json` | `organism` | `str` | Прямое извлечение | `Series[str]` |
| `tax_id` | `/target/{id}.json` | `tax_id` | `int` | Прямое извлечение | `Series[pd.Int64Dtype]` |
| `gene_symbol` | `/target/{id}.json` | `gene_symbol` | `str` | Прямое извлечение | `Series[str]` |
| `hgnc_id` | `/target/{id}.json` | `hgnc_id` | `str` | Прямое извлечение | `Series[str]` |
| `lineage` | `/target/{id}.json` | `lineage` | `str` | JSON сериализация | `Series[str]` |

### UniProt данные

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `primaryAccession` | UniProt API | `primaryAccession` | `str` | Прямое извлечение | `Series[str]` |
| `target_names` | UniProt API | `protein_name` | `str` | Прямое извлечение | `Series[str]` |
| `target_uniprot_id` | UniProt API | `accession` | `str` | Прямое извлечение | `Series[str]` |
| `organism_chembl` | UniProt API | `organism_name` | `str` | Прямое извлечение | `Series[str]` |
| `species_group_flag` | UniProt API | `organism_id` | `str` | Прямое извлечение | `Series[str]` |
| `target_components` | UniProt API | `features` | `str` | JSON сериализация | `Series[str]` |
| `protein_classifications` | UniProt API | `protein_classifications` | `str` | JSON сериализация | `Series[str]` |
| `cross_references` | UniProt API | `cross_references` | `str` | JSON сериализация | `Series[str]` |
| `target_names_chembl` | UniProt API | `protein_name` | `str` | Прямое извлечение | `Series[str]` |
| `pH_dependence` | UniProt API | `cc_ptm` | `str` | Прямое извлечение | `Series[str]` |
| `pH_dependence_chembl` | UniProt API | `cc_ptm` | `str` | Прямое извлечение | `Series[str]` |
| `target_organism` | UniProt API | `organism_name` | `str` | Прямое извлечение | `Series[str]` |
| `target_tax_id` | UniProt API | `organism_id` | `str` | Прямое извлечение | `Series[str]` |
| `target_uniprot_accession` | UniProt API | `accession` | `str` | Прямое извлечение | `Series[str]` |
| `target_isoform` | UniProt API | `isoform` | `str` | Прямое извлечение | `Series[str]` |
| `isoform_ids_chembl` | UniProt API | `isoform_ids` | `str` | JSON сериализация | `Series[str]` |
| `isoform_names_chembl` | UniProt API | `isoform_names` | `str` | JSON сериализация | `Series[str]` |
| `isoforms_chembl` | UniProt API | `isoforms` | `str` | JSON сериализация | `Series[str]` |

### UniProt enrichment поля

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `uniprot_accession` | UniProt API | `accession` | `str` | Прямое извлечение | `Series[str]` |
| `uniprot_id_primary` | UniProt API | `primaryAccession` | `str` | Прямое извлечение | `Series[str]` |
| `uniprot_ids_all` | UniProt API | `secondaryAccession` | `str` | JSON сериализация | `Series[str]` |
| `isoform_count` | Вычисляется | - | `int` | Подсчет изоформ | `Series[int]` (ge=0) |
| `has_alternative_products` | Вычисляется | - | `bool` | Наличие альтернативных продуктов | `Series[bool]` |
| `has_uniprot` | Вычисляется | - | `bool` | Наличие UniProt данных | `Series[bool]` |
| `has_iuphar` | Вычисляется | - | `bool` | Наличие IUPHAR данных | `Series[bool]` |

### IUPHAR данные

| Колонка | Запрос | JSON Path | Тип | Нормализация | Валидация |
|---------|--------|-----------|-----|--------------|-----------|
| `iuphar_type` | IUPHAR API | `type` | `str` | `_normalize_iuphar_name()` | `Series[str]` |
| `iuphar_class` | IUPHAR API | `class` | `str` | `_normalize_iuphar_name()` | `Series[str]` |
| `iuphar_subclass` | IUPHAR API | `subclass` | `str` | `_normalize_iuphar_name()` | `Series[str]` |
| `data_origin` | Генерируется | - | `str` | Источник данных | `Series[str]` |

---

## Общие принципы нормализации

### ChEMBL ID нормализация

- Приведение к верхнему регистру
- Валидация формата `^CHEMBL\d+$`

### Строковая нормализация

- Удаление лишних пробелов
- Приведение к каноническому виду
- Обработка NULL значений

### Числовая нормализация

- Преобразование строк в числа
- Обработка NaN значений
- Валидация диапазонов

### JSON нормализация

- Сериализация в канонический JSON
- Сортировка ключей
- Обработка ошибок сериализации

### Временные метки

- ISO8601 формат
- UTC временная зона
- Детерминированная генерация

---

## Источники данных

1. **ChEMBL API** - основной источник для всех пайплайнов
2. **PubChem API** - enrichment для TestItem Pipeline
3. **UniProt API** - enrichment для Target Pipeline
4. **IUPHAR API** - enrichment для Target Pipeline
5. **PubMed API** - enrichment для Document Pipeline
6. **Crossref API** - enrichment для Document Pipeline
7. **OpenAlex API** - enrichment для Document Pipeline
8. **Semantic Scholar API** - enrichment для Document Pipeline

---

*Документ создан автоматически на основе анализа кода пайплайнов и схем валидации.*

