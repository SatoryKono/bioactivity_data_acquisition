# Нормализация данных

## Введение

Система нормализации данных предназначена для стандартизации и улучшения качества данных во всех пайплайнах ETL. Она обеспечивает единообразное представление данных, валидацию форматов и преобразование значений согласно бизнес-правилам.

## Цели нормализации

- **Стандартизация форматов**: Приведение данных к единому формату (например, DOI, ChEMBL ID, даты)
- **Валидация данных**: Проверка корректности значений и форматов
- **Очистка данных**: Удаление лишних пробелов, нормализация Unicode
- **Типизация**: Приведение к правильным типам данных
- **Доменная нормализация**: Специфические правила для биологических данных

## Типы данных и их преобразования

### Стандартные типы

#### Строковые данные (string)

**Обязательные преобразования:**
- `strip()` - удаление ведущих и завершающих пробелов
- `normalize_empty_to_null()` - преобразование пустых строк в NULL
- `normalize_string_whitespace()` - нормализация внутренних пробелов

**Опциональные преобразования:**
- `normalize_string_nfc()` - Unicode NFC нормализация
- `normalize_string_upper()` - приведение к верхнему регистру
- `normalize_string_lower()` - приведение к нижнему регистру
- `normalize_string_titlecase()` - приведение к title case

#### Числовые данные (int, float)

**Обязательные преобразования:**
- `normalize_int()` / `normalize_float()` - приведение к правильному типу
- Проверка на NaN и бесконечности
- `normalize_int_positive()` - проверка положительных значений

**Опциональные преобразования:**
- `normalize_int_range()` - проверка диапазона значений
- `normalize_float_precision()` - округление до заданной точности

#### Временные данные (datetime)

**Обязательные преобразования:**
- `normalize_datetime_iso8601()` - преобразование к ISO 8601 UTC
- `normalize_datetime_validate()` - проверка валидности

**Опциональные преобразования:**
- `normalize_datetime_precision()` - обрезка до нужной точности

#### Булевы данные (boolean)

**Обязательные преобразования:**
- `normalize_boolean()` - маппинг строк/чисел к булевым значениям

**Опциональные преобразования:**
- `normalize_boolean_strict()` - строгая проверка значений

### Специфические типы

#### DOI (Digital Object Identifier)

**Функция:** `normalize_doi()`

**Преобразования:**
- Удаление префиксов: `doi:`, `urn:doi:`, `info:doi/`
- Удаление URL оболочек: `https://doi.org/`, `http://dx.doi.org/`
- Декодирование percent-encoding
- Нормализация пробелов и регистра
- Удаление хвостовой пунктуации
- Валидация формата: `^10\.\d{4,9}/[-._;()/:A-Z0-9]+$`

**Примеры:**
`	ext
https://doi.org/10.1021/acs.jmedchem.0c01234 → 10.1021/acs.jmedchem.0c01234
DOI:10.1021/acs.jmedchem.0c01234 → 10.1021/acs.jmedchem.0c01234
`	ext

#### ChEMBL ID

**Функция:** `normalize_chembl_id()`

**Преобразования:**
- Приведение к верхнему регистру
- Обеспечение префикса `CHEMBL`
- Валидация формата: `^CHEMBL\d+$`

**Примеры:**
`	ext
chembl25 → CHEMBL25
25 → CHEMBL25
`	ext

#### UniProt Accession

**Функция:** `normalize_uniprot_id()`

**Преобразования:**
- Приведение к верхнему регистру
- Валидация формата: `^[OPQ][0-9][A-Z0-9]{3}[0-9]$` или старые формы

**Примеры:**
`	ext
p12345 → P12345
`	ext

#### IUPHAR ID

**Функция:** `normalize_iuphar_id()`

**Преобразования:**
- Удаление префиксов: `GTOPDB:`, `IUPHAR:`
- Проверка положительного целого числа

**Примеры:**
`	ext
GTOPDB:1234 → 1234
`	ext

#### PubChem CID

**Функция:** `normalize_pubchem_cid()`

**Преобразования:**
- Удаление префиксов: `CID:`, URL части
- Проверка положительного целого числа

**Примеры:**
`	ext
CID:2244 → 2244
`	ext

#### SMILES

**Функция:** `normalize_smiles()`

**Преобразования:**
- Удаление пробелов
- Базовая валидация структуры
- Проверка баланса скобок
- Канонизация (в будущем с RDKit)

**Примеры:**
`	ext
C[C@H](O)Cl → C[C@H](O)Cl
`	ext

#### InChI

**Функция:** `normalize_inchi()`

**Преобразования:**
- Проверка префикса: `InChI=` или `InChI=1S/`
- Валидация структуры

**Примеры:**
`	ext
InChI=1S/CH4/h1H4 → InChI=1S/CH4/h1H4
`	ext

#### InChI Key

**Функция:** `normalize_inchi_key()`

**Преобразования:**
- Приведение к верхнему регистру
- Валидация формата: `^[A-Z]{14}-[A-Z]{10}-[A-Z]$`

**Примеры:**
`	ext
bsynrymutxbxsq-uhfffaoysa-n → BSYNRYMUTXBXSQ-UHFFFAOYSA-N
`	ext

#### BAO Format / Label

**Функция:** `normalize_bao_id()` / `normalize_bao_label()`

**Преобразования:**
- Приведение к верхнему регистру (для ID)
- Приведение к title case (для label)
- Валидация формата: `^BAO_\d+$`

**Примеры:**
`	ext
bao:0000357 → BAO_0000357
`	ext

#### pChEMBL

**Функция:** `normalize_pchembl()`

**Преобразования:**
- Приведение к float64
- Проверка диапазона [0-14]
- Округление до 3 знаков

**Примеры:**
`	ext
7.1234 → 7.123
`	ext

#### Единицы измерения

**Функция:** `normalize_units()`

**Преобразования:**
- Приведение к нижнему регистру
- Маппинг на стандартизованные единицы

**Примеры:**
`	ext
um → μm
nanomolar → nm
`	ext

## Нормализация по пайплайнам

### Documents

| Поле | Тип | Функции нормализации |
|------|-----|---------------------|
| `document_chembl_id` | string | strip, uppercase, ensure_prefix_chembl, validate_pattern |
| `chembl_doi` | string | strip, lowercase, remove_doi_prefix, validate_doi |
| `chembl_pmid` | int | to_int64, check_positive |
| `chembl_year` | int | to_int64, range_1800_current |
| `valid_doi` | boolean | map_boolean |
| `publication_date` | datetime | to_datetime_utc |

### Targets

| Поле | Тип | Функции нормализации |
|------|-----|---------------------|
| `target_chembl_id` | string | strip, uppercase, ensure_prefix_chembl, validate_pattern |
| `uniprot_id_primary` | string | strip, uppercase, validate_uniprot_format |
| `hgnc_id` | string | strip, uppercase |
| `tax_id` | int | to_int64, check_positive |
| `transmembrane` | boolean | map_boolean |
| `organism` | string | strip, titlecase |

### Assays

| Поле | Тип | Функции нормализации |
|------|-----|---------------------|
| `assay_chembl_id` | string | strip, uppercase, ensure_prefix_chembl |
| `target_chembl_id` | string | strip, uppercase, ensure_prefix_chembl |
| `assay_type` | string | strip, uppercase, validate_enum |
| `bao_format` | string | strip, uppercase, validate_bao_pattern |
| `is_variant` | boolean | map_boolean |

### Testitems

| Поле | Тип | Функции нормализации |
|------|-----|---------------------|
| `molecule_chembl_id` | string | strip, uppercase, ensure_prefix_chembl |
| `pubchem_cid` | int | to_int64, check_positive |
| `pubchem_canonical_smiles` | string | strip, canonicalize_smiles |
| `pubchem_inchi` | string | strip, ensure_prefix_inchi, validate |
| `pubchem_inchi_key` | string | strip, uppercase, validate_inchikey_format |
| `mw_freebase` | float | to_float64, check_ranges |

### Activities

| Поле | Тип | Функции нормализации |
|------|-----|---------------------|
| `activity_chembl_id` | string | strip, uppercase, ensure_prefix_chembl |
| `activity_value` | float | to_float64, check_positive |
| `activity_unit` | string | strip, lowercase, map_units |
| `pchembl_value` | float | to_float64, range_0_14, round_precision |
| `is_censored` | boolean | map_boolean |

## Последовательность выполнения

Нормализация выполняется в следующем порядке:

1. **Приведение типа (cast)** - преобразование к правильному типу данных
2. **Очистка строк и пробелов** - удаление лишних символов
3. **Доменная нормализация** - применение специфических правил (префиксы, канонизация)
4. **Валидация формата и диапазонов** - проверка корректности значений

## Примеры использования

### Программное использование

```python
from library.normalizers import get_normalizer

# Получение функции нормализации
doi_normalizer = get_normalizer("normalize_doi")

# Применение нормализации
normalized_doi = doi_normalizer("https://doi.org/10.1021/acs.jmedchem.0c01234")
# Результат: "10.1021/acs.jmedchem.0c01234"
`	ext

### Интеграция в пайплайны

```python
# В модулях normalize.py
def _apply_schema_normalizations(self, df: pd.DataFrame) -> pd.DataFrame:
    schema = DocumentNormalizedSchema.get_schema()
    
    for column_name, column_schema in schema.columns.items():
        if column_name in df.columns:
            norm_funcs = column_schema.metadata.get("normalization_functions", [])
            for func_name in norm_funcs:
                func = get_normalizer(func_name)
                df[column_name] = df[column_name].apply(func)
    
    return df
`	ext

## Отчетность по нормализации

Система автоматически отслеживает изменения данных и генерирует отчеты:

- Количество измененных значений по каждому полю
- Количество отброшенных значений (→ NULL)
- Процент успешных нормализаций
- Примеры трансформаций

## Версионирование

- **Версия:** 1.0.0
- **Дата обновления:** 2025-01-23
- **Ответственная команда:** Data Engineering Team

## Changelog

### v1.0.0 (2025-01-23)
- Первоначальная реализация системы нормализации
- Поддержка всех стандартных и специфических типов данных
- Интеграция во все пайплайны ETL
- Система отчетности и мониторинга

## Ответственные лица

- **Разработка:** Data Engineering Team
- **Тестирование:** QA Team
- **Документация:** Technical Writing Team
