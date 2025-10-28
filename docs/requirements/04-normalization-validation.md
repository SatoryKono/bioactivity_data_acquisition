# 4. Нормализация и валидация (UnifiedSchema)

## Обзор

UnifiedSchema — система нормализации и валидации, объединяющая:
- **Модульные нормализаторы** с реестром (bioactivity_data_acquisition5)
- **Источник-специфичные схемы** для разных API (ChEMBL_data_acquisition6)
- **Pandera валидацию** с метаданными
- **Фабрики полей** для типовых идентификаторов

## Архитектура

```text
Normalization System
├── BaseNormalizer (ABC)
│   ├── StringNormalizer
│   ├── NumericNormalizer
│   ├── DateTimeNormalizer
│   ├── BooleanNormalizer
│   ├── ChemistryNormalizer
│   ├── IdentifierNormalizer
│   └── OntologyNormalizer
│
├── NormalizerRegistry
│   └── registration and lookup
│
Schema System (Pandera)
├── BaseSchema
│   ├── InputSchema
│   ├── IntermediateSchema
│   └── OutputSchema
│       ├── DocumentSchema
│       │   ├── ChEMBLDocumentSchema
│       │   ├── PubMedDocumentSchema
│       │   ├── CrossRefDocumentSchema
│       │   ├── OpenAlexDocumentSchema
│       │   └── SemanticScholarDocumentSchema
│       ├── TargetSchema
│       │   ├── ChEMBLTargetSchema
│       │   ├── UniProtTargetSchema
│       │   └── IUPHARTargetSchema
│       ├── AssaySchema (ChEMBL)
│       ├── ActivitySchema (ChEMBL)
│       └── TestItemSchema
│           ├── ChEMBLTestItemSchema
│           └── PubChemTestItemSchema
```

## Компоненты нормализации

### 1. BaseNormalizer (ABC)

Базовый абстрактный класс для нормализаторов:

```python
from abc import ABC, abstractmethod

class BaseNormalizer(ABC):
    """Базовый класс для нормализаторов."""
    
    @abstractmethod
    def normalize(self, value: Any) -> Any:
        """Нормализует значение."""
        pass
    
    @abstractmethod
    def validate(self, value: Any) -> bool:
        """Проверяет корректность значения."""
        pass
    
    def safe_normalize(self, value: Any) -> Any:
        """Безопасная нормализация с обработкой ошибок."""
        try:
            return self.normalize(value)
        except Exception as e:
            logger.warning(f"Normalization failed: {e}", value=value)
            return None
```

### 2. StringNormalizer

Нормализация строк:

```python
class StringNormalizer(BaseNormalizer):
    """Нормализатор строк."""
    
    def __init__(self):
        self.normalizations = [
            self.strip,
            self.nfc,
            self.whitespace
        ]
    
    def normalize(self, value: str | None) -> str | None:
        """Нормализует строку."""
        if value is None or not isinstance(value, str):
            return None
        
        result = value
        for func in self.normalizations:
            result = func(result)
        
        return result if result else None
    
    def strip(self, s: str) -> str:
        """Удаляет пробелы в начале и конце."""
        return s.strip()
    
    def nfc(self, s: str) -> str:
        """Приводит к Unicode NFC."""
        import unicodedata
        return unicodedata.normalize('NFC', s)
    
    def whitespace(self, s: str) -> str:
        """Нормализует множественные пробелы."""
        import re
        return re.sub(r'\s+', ' ', s)
```

### 3. IdentifierNormalizer

Нормализация идентификаторов:

```python
class IdentifierNormalizer(BaseNormalizer):
    """Нормализатор идентификаторов."""
    
    PATTERNS = {
        'doi': r'^10\.\d+/.+$',
        'pmid': r'^\d+$',
        'chembl_id': r'^CHEMBL\d+$',
        'uniprot': r'^[A-Z0-9]{6,10}(-[0-9]+)?$',
        'pubchem_cid': r'^\d+$',
        'inchi_key': r'^[A-Z]{14}-[A-Z]{10}-[A-Z]$'
    }
    
    def normalize(self, value: str) -> str | None:
        """Нормализует идентификатор."""
        if not value or not isinstance(value, str):
            return None
        
        s = value.strip().upper()
        
        # ChEMBL ID
        if s.startswith('CHEMBL'):
            return s
        
        # DOI
        if s.startswith('10.'):
            return s.lower()
        
        # PMID, PubChem CID
        if s.isdigit():
            return s
        
        # UniProt
        if re.match(self.PATTERNS['uniprot'], s):
            return s.upper()
        
        return s
```

### 4. ChemistryNormalizer

Нормализация химических структур:

```python
class ChemistryNormalizer(BaseNormalizer):
    """Нормализатор химических структур."""
    
    def normalize_smiles(self, value: str) -> str | None:
        """Нормализует SMILES."""
        if not value:
            return None
        
        # Базовая нормализация
        s = value.strip()
        
        # Удаляем дублирующиеся пробелы
        s = re.sub(r'\s+', ' ', s)
        
        return s if s else None
    
    def normalize_inchi(self, value: str) -> str | None:
        """Нормализует InChI."""
        if not value:
            return None
        
        s = value.strip()
        
        # InChI должен начинаться с "InChI="
        if not s.startswith('InChI='):
            return None
        
        return s
```

### 5. NormalizerRegistry

Централизованный реестр:

```python
class NormalizerRegistry:
    """Реестр нормализаторов."""
    
    _registry: dict[str, BaseNormalizer] = {}
    
    @classmethod
    def register(cls, name: str, normalizer: BaseNormalizer):
        """Регистрирует нормализатор."""
        cls._registry[name] = normalizer
    
    @classmethod
    def get(cls, name: str) -> BaseNormalizer:
        """Получает нормализатор по имени."""
        if name not in cls._registry:
            raise ValueError(f"Normalizer {name} not found")
        return cls._registry[name]
    
    @classmethod
    def normalize(cls, name: str, value: Any) -> Any:
        """Нормализует значение через нормализатор."""
        normalizer = cls.get(name)
        return normalizer.safe_normalize(value)

# Инициализация
registry = NormalizerRegistry()
registry.register("string", StringNormalizer())
registry.register("identifier", IdentifierNormalizer())
registry.register("chemistry", ChemistryNormalizer())
```

## Компоненты схем Pandera

### 1. BaseSchema

Базовый класс для всех схем:

```python
class BaseSchema(pa.DataFrameModel):
    """Базовый класс для Pandera схем."""
    
    # Системные поля
    index: int = pa.Field(ge=0, nullable=False)
    pipeline_version: str = pa.Field(nullable=False)
    source_system: str = pa.Field(nullable=False)
    chembl_release: str | None = pa.Field(nullable=True)
    extracted_at: str = pa.Field(nullable=False)  # ISO8601 UTC
    hash_row: str = pa.Field(nullable=False, str_length=64)  # SHA256
    hash_business_key: str = pa.Field(nullable=False, str_length=64)
    
    class Config:
        strict = True
        coerce = True
        ordered = True
```

### 2. DocumentSchema — ChEMBL

```python
class ChEMBLDocumentSchema(BaseSchema):
    """Схема для ChEMBL документов."""
    
    document_chembl_id: str = pa.Field(
        nullable=False,
        str_matches=r'^CHEMBL\d+$'
    )
    title: str = pa.Field(nullable=False, str_length_max=1000)
    journal: str | None = pa.Field(nullable=True)
    year: int | None = pa.Field(ge=1800, le=2030, nullable=True)
    volume: str | None = pa.Field(nullable=True)
    issue: str | None = pa.Field(nullable=True)
    first_page: str | None = pa.Field(nullable=True)
    last_page: str | None = pa.Field(nullable=True)
    doi: str | None = pa.Field(nullable=True, str_matches=r'^10\.\d+/.+$')
    pmid: str | None = pa.Field(nullable=True, str_matches=r'^\d+$')
```

### 3. DocumentSchema — PubMed

```python
class PubMedDocumentSchema(BaseSchema):
    """Схема для PubMed документов."""
    
    pmid: str = pa.Field(nullable=False, str_matches=r'^\d+$')
    title: str = pa.Field(nullable=False)
    journal_info: str | None = pa.Field(nullable=True)
    pub_date: str | None = pa.Field(nullable=True)
    authors: str | None = pa.Field(nullable=True)
    abstract: str | None = pa.Field(nullable=True)
    mesh_terms: str | None = pa.Field(nullable=True)
    doi: str | None = pa.Field(nullable=True)
```

### 4. DocumentSchema — CrossRef

```python
class CrossRefDocumentSchema(BaseSchema):
    """Схема для CrossRef документов."""
    
    doi: str = pa.Field(nullable=False, str_matches=r'^10\.\d+/.+$')
    title: str = pa.Field(nullable=False)
    type: str | None = pa.Field(nullable=True)
    subtype: str | None = pa.Field(nullable=True)
    subject: str | None = pa.Field(nullable=True)
    publisher: str | None = pa.Field(nullable=True)
```

### 5. DocumentSchema — OpenAlex

```python
class OpenAlexDocumentSchema(BaseSchema):
    """Схема для OpenAlex документов."""
    
    openalex_id: str | None = pa.Field(nullable=True)
    publication_types: str | None = pa.Field(nullable=True)
    genre: str | None = pa.Field(nullable=True)
    venue: str | None = pa.Field(nullable=True)
    mesh_descriptors: str | None = pa.Field(nullable=True)
    mesh_qualifiers: str | None = pa.Field(nullable=True)
```

### 6. DocumentSchema — Semantic Scholar

```python
class SemanticScholarDocumentSchema(BaseSchema):
    """Схема для Semantic Scholar документов."""
    
    s2_id: str | None = pa.Field(nullable=True)
    title: str | None = pa.Field(nullable=True)
    authors: str | None = pa.Field(nullable=True)
    citation_count: int | None = pa.Field(ge=0, nullable=True)
    influential_citations: int | None = pa.Field(ge=0, nullable=True)
```

### 7. TargetSchema — ChEMBL

```python
class ChEMBLTargetSchema(BaseSchema):
    """Схема для ChEMBL целей."""
    
    target_chembl_id: str = pa.Field(
        nullable=False,
        str_matches=r'^CHEMBL\d+$'
    )
    target_type: str = pa.Field(nullable=False)
    target_components: str | None = pa.Field(nullable=True)
    organism: str | None = pa.Field(nullable=True)
    pref_name: str | None = pa.Field(nullable=True)
```

### 8. TargetSchema — UniProt

```python
class UniProtTargetSchema(BaseSchema):
    """Схема для UniProt целей."""
    
    uniprot_accession: str = pa.Field(
        nullable=False,
        str_matches=r'^[A-Z0-9]{6,10}(-[0-9]+)?$'
    )
    gene_name: str | None = pa.Field(nullable=True)
    protein_name: str | None = pa.Field(nullable=True)
    organism: str | None = pa.Field(nullable=True)
    function: str | None = pa.Field(nullable=True)
    sequence: str | None = pa.Field(nullable=True)
```

### 9. TargetSchema — IUPHAR

```python
class IUPHARTargetSchema(BaseSchema):
    """Схема для IUPHAR целей."""
    
    iuphar_id: str | None = pa.Field(nullable=True)
    target_name: str | None = pa.Field(nullable=True)
    target_family: str | None = pa.Field(nullable=True)
    species: str | None = pa.Field(nullable=True)
    ligands: str | None = pa.Field(nullable=True)
```

### 10. TestItemSchema — ChEMBL

```python
class ChEMBLTestItemSchema(BaseSchema):
    """Схема для ChEMBL молекул."""
    
    molecule_chembl_id: str = pa.Field(
        nullable=False,
        str_matches=r'^CHEMBL\d+$'
    )
    parent_chembl_id: str | None = pa.Field(nullable=True)
    pref_name: str | None = pa.Field(nullable=True)
    max_phase: int | None = pa.Field(ge=0, le=4, nullable=True)
    molecule_type: str | None = pa.Field(nullable=True)
    canonical_smiles: str | None = pa.Field(nullable=True)
    standard_inchi: str | None = pa.Field(nullable=True)
```

### 11. TestItemSchema — PubChem

```python
class PubChemTestItemSchema(BaseSchema):
    """Схема для PubChem соединений."""
    
    pubchem_cid: str = pa.Field(
        nullable=False,
        str_matches=r'^\d+$'
    )
    iupac_name: str | None = pa.Field(nullable=True)
    molecular_formula: str | None = pa.Field(nullable=True)
    molecular_weight: float | None = pa.Field(ge=0, nullable=True)
    canonical_smiles: str | None = pa.Field(nullable=True)
    inchi: str | None = pa.Field(nullable=True)
    inchikey: str | None = pa.Field(nullable=True)
```

## Использование

### Нормализация данных

```python
from unified_schema import NormalizerRegistry

registry = NormalizerRegistry()

# Нормализация строки
result = registry.normalize("string", "  Hello  World  ")
# → "Hello World"

# Нормализация идентификатора
result = registry.normalize("identifier", " chembl25 ")
# → "CHEMBL25"

# Нормализация DOI
result = registry.normalize("identifier", "10.1234/example")
# → "10.1234/example"
```

### Валидация через схемы

```python
from unified_schema import ChEMBLDocumentSchema, PubMedDocumentSchema

# Валидация ChEMBL документа
schema = ChEMBLDocumentSchema
validated_df = schema.validate(df, lazy=True)

# Валидация PubMed документа
schema = PubMedDocumentSchema
validated_df = schema.validate(df, lazy=True)
```

### Применение нормализации к DataFrame

```python
def normalize_dataframe(df: pd.DataFrame, schema: pa.DataFrameModel):
    """Применяет нормализацию на основе метаданных схемы."""
    normalized_df = df.copy()
    
    for column, column_info in schema.__fields__.items():
        if column in normalized_df.columns:
            # Получаем нормализаторы из metadata
            metadata = column_info.metadata or {}
            normalizers = metadata.get('normalization_functions', [])
            
            for normalizer_name in normalizers:
                normalizer = NormalizerRegistry.get(normalizer_name)
                normalized_df[column] = normalized_df[column].apply(
                    lambda x: normalizer.safe_normalize(x)
                )
    
    return normalized_df
```

## Best Practices

1.  **Всегда валидируйте через Pandera**: перед записью данных
2.  **Используйте lazy=True**: для получения всех ошибок сразу
3.  **Применяйте нормализацию до валидации**: очистка данных
4.  **Выбирайте правильную схему**: в зависимости от источника
5.  **Добавляйте системные поля**: hash_row, hash_business_key
6.  **Логируйте ошибки валидации**: для отладки

## Schema Registry

Централизованный реестр Pandera-схем с версионированием.

### Структура схемы

Каждая схема содержит:
- `schema_id`: уникальный идентификатор (например, `document.chembl`)
- `schema_version`: семантическая версия (semver: MAJOR.MINOR.PATCH)
- `column_order`: источник истины для порядка колонок
- `precision_map`: настройки точности для числовых полей

```python
class DocumentSchema(BaseSchema):
    """Схема для ChEMBL документов."""
    
    # Метаданные схемы
    schema_id = "document.chembl"
    schema_version = "2.1.0"
    
    # Порядок колонок (источник истины)
    column_order = [
        "document_chembl_id", "title", "journal", "year",
        "doi", "pmid", "hash_business_key", "hash_row"
    ]
    
    # Настройки точности
    precision_map = {
        "year": 0,  # Целое число
        "other_numeric": 2  # По умолчанию 2 знака
    }
    
    # Поля схемы
    document_chembl_id: str = pa.Field(str_matches=r'^CHEMBL\d+$')
    title: str = pa.Field(nullable=False)
    journal: str | None = pa.Field(nullable=True)
    year: int | None = pa.Field(ge=1800, le=2030, nullable=True)
    ...
```

### Правила эволюции схем

**Semantic Versioning (MAJOR.MINOR.PATCH):**

| Изменение | Impact | Пример | Версия |
|-----------|--------|--------|--------|
| Удаление колонки | Breaking | Удалить `pmid` | MAJOR++ |
| Переименование колонки | Breaking | `title` → `article_title` | MAJOR++ |
| Добавление обязательной колонки | Breaking | Добавить обязательный `source` | MAJOR++ |
| Изменение типа колонки | Breaking | `int` → `float` | MAJOR++ |
| Добавление опциональной колонки | Compatible | Добавить опциональный `abstract` | MINOR++ |
| Добавление constraint | Backward | Добавить `min=0` | MINOR++ |
| Изменение column_order | Compatible | Переставить колонки | PATCH++ |
| Документация/комментарии | None | Обновить docstring | PATCH++ |

**Правило "заморозки" колонок:**
- Добавление колонки: minor или major (если обязательная)
- Удаление колонки: только major
- Изменение типа колонки: только major

### Матрица совместимости

Таблица допустимых апгрейдов:

| From | To | Compatibility | Required Actions |
|------|-----|---------------|------------------|
| 2.0.0 | 2.1.0 | ✅ Compatible | Нет |
| 2.0.0 | 3.0.0 | ⚠️ Breaking | Migration script |
| 2.1.0 | 2.0.0 | ❌ Incompatible | Downgrade запрещен |
| 2.x.x | 3.0.0 | ⚠️ Breaking | Полный перезапуск пайплайна |

**Проверка совместимости:**
```python
def is_compatible(from_version: str, to_version: str) -> bool:
    """Проверяет совместимость версий."""
    from_major = int(from_version.split('.')[0])
    to_major = int(to_version.split('.')[0])
    
    return from_major == to_major  # Major версия должна совпадать
```

### Хранение column_order в схеме (источник истины)

**Инвариант:** column_order — единственный источник истины в схеме; meta.yaml содержит копию; несоответствие column_order схеме — fail-fast до записи; precision_map и NA-policy обязательны для всех таблиц.

```python
# schema.py (Schema Registry) — источник истины
class DocumentSchema(BaseSchema):
    column_order = ["document_chembl_id", "title", "journal", ...]
    precision_map = {"year": 0}
    na_policy = {"na_strings": ["", "N/A"], "na_numeric": None}

# При экспорте
df = df[schema.column_order]  # используем order из схемы

# Валидация перед записью
assert list(df.columns) == schema.column_order, "Column order mismatch!"

# В meta.yaml (только для справки)
meta = {
    "column_order": schema.column_order,  # Копия из схемы
    "precision_map": schema.precision_map,  # Копия
    ...
}
```

**Fail-fast при несоответствии**:
```python
def validate_column_order(df: pd.DataFrame, schema: BaseSchema) -> None:
    """Валидация соответствия порядка колонок схеме."""
    if list(df.columns) != schema.column_order:
        raise SchemaValidationError(
            f"Column order mismatch: expected {schema.column_order}, "
            f"got {list(df.columns)}"
        )
```

**Преимущества:**
- Единый источник истины
- Невозможность рассинхронизации
- Простая миграция при изменении порядка
- Fail-fast до записи

**См. также**: [gaps.md](../gaps.md) (G4, G5), [acceptance-criteria.md](../acceptance-criteria.md) (AC2, AC10).

### Метрики precision

Настраиваемая точность для разных типов метрик:

```python
# По умолчанию
precision_map = {
    "pIC50": 3,
    "Ki": 3,
    "IC50": 3,
    "EC50": 3,
    "molecular_weight": 2,
    "LogP": 2,
    "correlation": 4,
    "coefficient": 4,
    "score": 2,
    "ratio": 2,
    "fraction": 4
}

# В схеме ActivitySchema
class ActivitySchema(BaseSchema):
    pic50: float = pa.Field(precision=3)  # 3 знака
    molecular_weight: float = pa.Field(precision=2)  # 2 знака
    correlation: float = pa.Field(precision=4)  # 4 знака
    
    # Применение
    def apply_precision(self, value: float, field_name: str) -> float:
        precision = self.precision_map.get(field_name, 2)
        return round(value, precision)
```

**Примеры:**
- `pIC50 = 7.234567` → `7.235`
- `molecular_weight = 456.789012` → `456.79`
- `correlation = 0.82345678` → `0.8235`

**Расширенный precision_map для ActivitySchema:**

```python
class ActivitySchema(BaseSchema):
    """Схема для activity с явной precision_map."""
    
    schema_id = "activity.chembl"
    schema_version = "1.0.0"
    
    # Явная precision_map для числовых полей
    precision_map = {
        "pchembl_value": 4,      # 4 знака для pChEMBL
        "standard_value": 3,      # 3 знака для стандартных значений
        "pic50": 3,               # 3 знака для pIC50
        "ki": 3,                  # 3 знака для Ki
        "ic50": 3,                # 3 знака для IC50
        "ec50": 3,                # 3 знака для EC50
        "molecular_weight": 2,    # 2 знака для молекулярного веса
        "logp": 2,                # 2 знака для LogP
        "correlation": 4,         # 4 знака для корреляций
        "coefficient": 4,         # 4 знака для коэффициентов
        "default": 2              # По умолчанию 2 знака
    }
    
    # Применение precision при нормализации
    @classmethod
    def normalize_value(cls, field_name: str, value: float | None) -> float | None:
        """Нормализует значение с применением precision."""
        if value is None:
            return None
        
        precision = cls.precision_map.get(field_name, cls.precision_map["default"])
        return round(value, precision)
```

### Semver Policy и Schema Evolution (fail-fast на major)

**Инвариант:** Семантика schema drift: major incompatible (fail-fast), minor backward-compatible; CLI-флаг `--fail-on-schema-drift` (default=True).

**Критическое правило:** Изменения схемы требуют bump версии и валидацию совместимости.

**Флаг --fail-on-schema-drift:**

При несовпадении major-версии схемы пайплайн **обязан** упасть, если включен флаг `--fail-on-schema-drift` (по умолчанию в production — включен).

```python
def validate_schema_compatibility(schema: type[BaseSchema], expected_version: str, fail_on_drift: bool) -> None:
    """
    Проверяет совместимость версий схем.
    
    Raises:
        SchemaDriftError: при несовместимых изменениях и fail_on_drift=True
    """
    actual_version = schema.schema_version
    
    # Разбор версий
    expected_major = int(expected_version.split('.')[0])
    actual_major = int(actual_version.split('.')[0])
    
    # Major mismatch = breaking change
    if expected_major != actual_major:
        if fail_on_drift:
            raise SchemaDriftError(
                f"Schema version mismatch: expected {expected_version}, got {actual_version}. "
                f"Major version change indicates breaking changes."
            )
        else:
            logger.warning(
                "Schema drift detected",
                expected=expected_version,
                actual=actual_version
            )
```

### Реестр схем

Централизованное хранилище всех схем:

```python
class SchemaRegistry:
    """Реестр всех Pandera схем с валидацией версий."""
    
    _schemas: dict[str, type[BaseSchema]] = {}
    
    @classmethod
    def register(cls, schema: type[BaseSchema]):
        """Регистрирует схему."""
        schema_id = schema.schema_id
        cls._schemas[schema_id] = schema
    
    @classmethod
    def get(
        cls, 
        schema_id: str, 
        expected_version: str | None = None,
        fail_on_drift: bool = True
    ) -> type[BaseSchema]:
        """Получает схему по ID с проверкой версии."""
        schema = cls._schemas.get(schema_id)
        if not schema:
            raise ValueError(f"Schema {schema_id} not found")
        
        # Проверка версии
        if expected_version:
            validate_schema_compatibility(schema, expected_version, fail_on_drift)
        
        return schema

# Инициализация
SchemaRegistry.register(DocumentSchema)
SchemaRegistry.register(ActivitySchema)
SchemaRegistry.register(TargetSchema)
```

## Расширение

### Добавление нового нормализатора

```python
class CustomNormalizer(BaseNormalizer):
    def normalize(self, value: Any) -> Any:
        # Ваша логика
        return normalized_value
    
    def validate(self, value: Any) -> bool:
        # Ваша проверка
        return is_valid

# Регистрация
NormalizerRegistry.register("custom", CustomNormalizer())
```

### Создание новой схемы

```python
class MyCustomSchema(BaseSchema):
    # Метаданные
    schema_id = "custom.mytable"
    schema_version = "1.0.0"
    column_order = ["id", "name", "value"]
    
    # Поля
    custom_field: str = pa.Field(nullable=False)
    other_field: int | None = pa.Field(ge=0, nullable=True)
    
    # Регистрация
    SchemaRegistry.register(MyCustomSchema)
```

## Acceptance Criteria

### AC-08: Schema Drift Detection

**Цель:** Гарантировать fail-fast при несовместимых изменениях схемы.

**Тест:**
```python
# Запуск с несовпадающей major-версией и --fail-on-schema-drift
schema = SchemaRegistry.get("document.chembl", expected_version="3.0.0", fail_on_drift=True)
# Ожидаемое: SchemaDriftError

# Без флага - warning
schema = SchemaRegistry.get("document.chembl", expected_version="3.0.0", fail_on_drift=False)
# Ожидаемое: warning в логах
```

**Порог:** exit != 0 при fail_on_drift=True и major mismatch.

### AC-03: Column Order Validation

**Цель:** Гарантировать, что порядок колонок соответствует схеме.

**Тест:**
```python
df = pd.DataFrame({...})
schema = ActivitySchema()

# Применение column_order из схемы
df_ordered = df[schema.column_order]

assert df_ordered.columns.tolist() == schema.column_order
```

**Ожидаемое:** Полное совпадение порядка колонок.

---

**Назад к обзору**: [00-architecture-overview.md](00-architecture-overview.md)
