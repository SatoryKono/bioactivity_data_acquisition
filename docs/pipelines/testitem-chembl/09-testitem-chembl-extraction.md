# ChEMBL TestItem Extraction Pipeline

This document specifies the `testitem` pipeline, which extracts molecule data from the ChEMBL API. The pipeline flattens nested JSON structures from ChEMBL responses to create comprehensive, flat records for each molecule.

> **Note**: PubChem enrichment is described in a separate document: [PubChem TestItem Pipeline](../testitem-pubchem/09-testitem-pubchem-extraction.md).

## 1. Overview

The `testitem` pipeline is responsible for fetching detailed information about chemical compounds (molecules) from the ChEMBL database. It flattens nested structures from the ChEMBL API response to create a comprehensive, flat record for each molecule.

- **Primary Source**: ChEMBL API `/molecule.json` endpoint
- **Implementation**: `src/bioetl/pipelines/chembl_testitem.py`
- **Schema**: `src/bioetl/schemas/chembl_testitem.py`

## 2. CLI Command

The pipeline is executed via the `testitem` CLI command.

**Usage:**

```bash
python -m bioetl.cli.main testitem [OPTIONS]
```

**Required options:**

- `--config PATH`: Path to the pipeline configuration YAML (e.g., `configs/pipelines/chembl/chembl_testitem.yaml`)
- `--output-dir PATH`: Directory where run artifacts are materialized

**Optional options:**

- `--dry-run`: Load, merge, and validate configuration without executing the pipeline
- `--limit N`: Process at most `N` rows (useful for smoke runs)
- `--sample N`: Randomly sample `N` rows
- `--set KEY=VALUE`: Override individual configuration keys at runtime (repeatable)
- `--verbose`: Emit verbose (development) logging

**Examples:**

```bash
# Basic run with canonical config
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/chembl_testitem.yaml \
  --output-dir data/output/chembl_testitem

# Dry run to validate configuration
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/chembl_testitem.yaml \
  --output-dir data/output/chembl_testitem \
  --dry-run

# Override batch size for smoke test
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/chembl_testitem.yaml \
  --output-dir data/output/chembl_testitem \
  --set sources.chembl.batch_size=10 \
  --limit 100
```

**Configuration loading precedence:**

1. Base profiles (`configs/defaults/base.yaml`, `configs/defaults/determinism.yaml`)
2. Pipeline YAML (`--config`)
3. CLI overrides (`--set`)
4. Environment variables

For more details, see [CLI Overview](../cli/00-cli-overview.md) and [CLI Commands](../cli/01-cli-commands.md).

## 3. Архитектура

Pipeline использует компонентную архитектуру с разделением ответственности:

| Component | Implementation | Назначение |
|---|---|---|
| **Pipeline** | `src/bioetl/pipelines/chembl_testitem.py::TestItemPipeline` | Основной класс пайплайна, наследует `PipelineBase` |
| **Client** | `src/bioetl/sources/chembl/testitem/client/` | Батчевое извлечение данных из ChEMBL API |
| **Parser** | `src/bioetl/sources/chembl/testitem/parser/` | Разбор и flattening вложенных JSON структур |
| **Schema** | `src/bioetl/schemas/chembl_testitem.py::TestItemSchema` | Pandera схема для валидации данных |

### 3.1. Pipeline Flow

```text
TestItemPipeline
├── Extract Stage
│   ├── ChEMBLClient (batch_size=25, URL limit)
│   ├── CacheManager (TTL, release-scoped invalidation)
│   ├── CircuitBreaker (failure tracking, recovery)
│   └── FallbackManager (extended error metadata)
├── Transform Stage
│   ├── Parser (nested structure flattening)
│   │   ├── MoleculeHierarchyExpansion
│   │   ├── MoleculePropertiesExpansion
│   │   ├── MoleculeStructuresExpansion
│   │   └── MoleculeSynonymsAggregation
│   └── Normalization (strict NA policy)
├── Validate Stage
│   ├── PanderaSchema (strict=True, nullable dtypes)
│   ├── ReferentialIntegrityCheck (parent_chembl_id)
│   └── QualityProfile (fail thresholds)
└── Write Stage
    ├── AtomicWriter (run_id-scoped temp dirs)
    ├── CanonicalSerialization (hash generation)
    └── MetadataBuilder (full provenance)
```

## 4. Конфигурация

**Базовый стандарт:** см. `docs/configs/00-typed-configs-and-profiles.md`.

**Профильный файл:** `configs/pipelines/chembl/chembl_testitem.yaml`, который объявляет `extends: "../base.yaml"` и проходит валидацию `PipelineConfig`.

**Критические переопределения:**

| Путь | Значение по умолчанию | Ограничение | Комментарий |
|------|-----------------------|-------------|-------------|
| `sources.chembl.batch_size` | 25 | `≤ 25` (жёсткое требование ChEMBL URL) | Проверяется на этапе валидации конфигурации. |
| `sources.chembl.max_url_length` | 2000 | `≤ 2000` | Используется для предиктивного троттлинга запросов. |
| `cache.namespace` | `"chembl"` | Не пусто | Обеспечивает release-scoped invalidation. |
| `determinism.sort.by` | `['molecule_chembl_id']` | Первый ключ — `molecule_chembl_id` | Сортировка применяется перед записью; итоговый CSV следует `TestItemSchema.Config.column_order`. |
| `determinism.column_order` | `TestItemSchema._column_order` (~150 колонок) | Полный список обязателен | Нарушение приводит к падению `PipelineConfig`. |
| `qc.thresholds.testitem.duplicate_ratio` | 0.0 | `≥ 0` | Критично: дубликаты недопустимы. |
| `qc.thresholds.testitem.fallback_ratio` | 0.2 | `≥ 0` | Процент fallback записей при ошибках API. |
| `qc.thresholds.testitem.parent_missing_ratio` | 0.0 | `≥ 0` | Referential integrity для `parent_chembl_id`. |

Секреты (API ключи) прокидываются через переменные окружения `BIOETL_SOURCES__CHEMBL__API_KEY` и `BIOETL_HTTP__GLOBAL__HEADERS__AUTHORIZATION`. Для быстрой настройки допускается использование CLI-переопределений, например `--set sources.chembl.batch_size=20`, однако изменения должны сопровождаться обоснованием в run metadata.

**Пример конфигурации:**

```yaml
extends:
  - ../base.yaml
  - ../includes/determinism.yaml
  - ../includes/chembl_source.yaml

pipeline:
  name: chembl_testitem
  entity: testitem
  version: "1.0.0"

materialization:
  pipeline_subdir: "chembl_testitem"

sources:
  chembl:
    batch_size: 25
    headers:
      User-Agent: "bioetl-chembl-testitem-pipeline/1.0"

determinism:
  sort:
    by: ["molecule_chembl_id"]
    ascending: [true]
  column_order:
    - "index"
    - "hash_row"
    - "hash_business_key"
    # ... полный список из TestItemSchema._column_order

qc:
  enabled: true
  severity_threshold: warning
  thresholds:
    testitem.duplicate_ratio: 0.0
    testitem.fallback_ratio: 0.2
    testitem.parent_missing_ratio: 0.0
```

## 5. Входные данные

### 5.1 Формат входных данных

**Файл:** CSV или DataFrame

**Обязательные поля:**

- `molecule_chembl_id` (StringDtype, NOT NULL): ChEMBL идентификатор молекулы в формате `CHEMBL\d+`

**Опциональные поля:**

- Любые дополнительные колонки из схемы `TestItemSchema` (будут использованы при merge с данными из API)

**Схема валидации:**

```python
# src/bioetl/schemas/input_schemas.py

class TestItemInputSchema(pa.DataFrameModel):
    molecule_chembl_id: Series[str] = pa.Field(
        description="ChEMBL molecule identifier",
        regex=r'^CHEMBL\d+$'
    )

    class Config:
        strict = True
        coerce = True
```

### 5.2 Процесс чтения

Pipeline использует метод `read_input_table()` из `PipelineBase` для чтения входных данных:

- Автоматическое разрешение пути через `config.paths.input_root`
- Поддержка `--limit` и `--sample` для ограничения размера выборки
- Логирование операций чтения с метриками
- Возврат пустого DataFrame с ожидаемыми колонками, если файл отсутствует

## 6. ChEMBL Data Extraction

### 6.1 Инициализация пайплайна

**Класс:** `TestItemPipeline` (`src/bioetl/pipelines/chembl_testitem.py`)

**Наследование:** `PipelineBase[PipelineConfig]`

**Run-level метаданные:**

```python
run_id: str = uuid4().hex[:16]  # UUID для идентификации запуска
git_commit: str = get_git_commit()  # SHA текущего коммита
config_hash: str = sha256(config_yaml).hexdigest()[:16]
python_version: str = sys.version_info[:3]
deps_fingerprint: str = get_deps_fingerprint()  # fingerprint pyproject.toml

chembl_release: str = None  # Фиксируется один раз в начале
chembl_base_url: str  # URL для воспроизводимости
```

**КРИТИЧЕСКИ ВАЖНО:**

1. Снимок `/status` выполняется **один раз** в начале run
2. `chembl_release` записывается в `run_config.yaml` в output_dir
3. Все последующие запросы **БЛОКИРУЮТСЯ** при смене release
4. Кэш-ключи **ОБЯЗАТЕЛЬНО** содержат release: `molecule:{release}:{molecule_chembl_id}`

### 6.2 Режимы извлечения

Pipeline поддерживает два режима извлечения:

1. **Full pagination** (по умолчанию): извлечение всех записей через `extract_all()`
2. **Batch extraction** (опционально): извлечение по списку ID через `extract_by_ids()` при наличии `--input-file`

Режим определяется автоматически в методе `extract()`:

- Если указан `--input-file` с колонкой `molecule_chembl_id`, вызывается `extract_by_ids()`
- Если `--input-file` не указан, вызывается `extract_all()`

### 6.3 Полное извлечение (Full Pagination)

**Метод:** `TestItemChemblPipeline.extract_all()`

**Эндпоинт ChEMBL:** `/molecule.json` (пагинация без фильтров)

**Описание:** Итерируется по всем доступным молекулам через пагинацию ChEMBL API с использованием `page_meta.next` для навигации.

### 6.4 Батчевое извлечение из ChEMBL API

**Метод:** `TestItemChemblPipeline.extract_by_ids()`

**Эндпоинт ChEMBL:** `/molecule.json?molecule_chembl_id__in={ids}`

**Размер батча:**

- **Конфиг:** `sources.chembl.batch_size: 25` (ОБЯЗАТЕЛЬНО)
- **Причина:** Жесткое ограничение длины URL в ChEMBL API (~2000 символов)
- **Валидация конфига:**

```python
if batch_size > 25:
    raise ValueError(
        "sources.chembl.batch_size must be <= 25 due to ChEMBL API URL length limit"
    )
```

**Алгоритм:**

```python
def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
    """Extract molecule records by a specific list of IDs using batch extraction."""
    
    # Batch extraction parameters
    batch_size = min(self._resolve_page_size(source_config), 25)
    limit = self.config.cli.limit
    
    records: list[dict[str, Any]] = []
    total_batches = 0
    
    # Process IDs in batches
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i : i + batch_size]
        
        # Construct batch request
        params = {
            "molecule_chembl_id__in": ",".join(batch_ids),
            "format": "json",
            "limit": len(batch_ids),
        }
        
        response = client.get("/molecule.json", params=params)
        payload = self._coerce_mapping(response.json())
        page_items = self._extract_page_items(payload)
        
        records.extend(page_items)
        total_batches += 1
        
        if limit is not None and len(records) >= limit:
            break
    
    return pd.DataFrame.from_records(records)
```

### 6.3 Field Extraction and Flattening

Pipeline извлекает более 100 полей и разворачивает несколько вложенных JSON структур из ответа ChEMBL:

**Группы полей:**

1. **ChEMBL core fields** (59 полей): `molregno`, `pref_name`, `parent_chembl_id`, `therapeutic_flag`, `molecule_type`, `max_phase`, `first_approval`, и др.
2. **ChEMBL property fields** (24 поля): `mw_freebase`, `alogp`, `hba`, `hbd`, `psa`, `rtb`, `ro3_pass`, `num_ro5_violations`, `acd_logp`, `acd_logd`, `full_mwt`, `aromatic_rings`, `heavy_atoms`, `qed_weighted`, и др.
3. **ChEMBL structure fields** (3 поля): `standardized_smiles`, `standard_inchi`, `standard_inchi_key`
4. **ChEMBL JSON fields** (11 полей): `molecule_hierarchy`, `molecule_properties`, `molecule_structures`, `molecule_synonyms`, `atc_classifications`, `cross_references`, `biotherapeutic`, `chemical_probe`, `orphan`, `veterinary`, `helm_notation`
5. **ChEMBL text fields** (1 поле): `all_names` (агрегированные синонимы)

**Flattening правил:**

- **`molecule_hierarchy`**: Разворачивается в `parent_chembl_id` и `parent_molregno`, исходный JSON сохраняется в `molecule_hierarchy`
- **`molecule_properties`**: Разворачивается в ~24 отдельных физико-химических свойства, исходный JSON сохраняется в `molecule_properties`
- **`molecule_structures`**: Разворачивается в `standardized_smiles`, `standard_inchi`, `standard_inchi_key`, исходный JSON сохраняется в `molecule_structures`
- **`molecule_synonyms`**: Агрегируется в `all_names` (для поиска), исходный JSON сохраняется в `molecule_synonyms`
- **Другие вложенные объекты**: `atc_classifications`, `cross_references`, `biotherapeutic`, и др. сохраняются как канонические JSON строки

## 7. Схемы данных

### 7.1 TestItemSchema

**Модуль:** `src/bioetl/schemas/chembl_testitem.py`

**Класс:** `TestItemSchema` (наследует `FallbackMetadataMixin`, `BaseSchema`)

**Версия схемы:** Регистрируется в `schema_registry` как `"testitem"` версия `"1.0.0"`

**Группы полей:**

1. **ChEMBL core fields** (59 полей):
   - Идентификаторы: `molecule_chembl_id`, `molregno`, `parent_chembl_id`, `parent_molregno`
   - Названия: `pref_name`, `pref_name_key`
   - Флаги: `therapeutic_flag`, `dosed_ingredient`, `direct_interaction`, `molecular_mechanism`, `oral`, `parenteral`, `topical`, `black_box_warning`, `natural_product`, `first_in_class`, `prodrug`, `inorganic_flag`, `polymer_flag`, `withdrawn_flag`
   - Типы: `structure_type`, `molecule_type`, `molecule_type_chembl`, `chirality`, `chirality_chembl`
   - Клинические данные: `max_phase`, `first_approval`, `availability_type`, `mechanism_of_action`
   - USAN данные: `usan_year`, `usan_stem`, `usan_substem`, `usan_stem_definition`
   - Индикации: `indication_class`
   - Отзывы: `withdrawn_year`, `withdrawn_country`, `withdrawn_reason`
   - Drug данные: `drug_chembl_id`, `drug_name`, `drug_type`, и флаги `drug_*_flag` (9 полей)

2. **ChEMBL property fields** (24 поля):
   - Молекулярная масса: `mw_freebase`, `full_mwt`, `mw_monoisotopic`
   - Липофильность: `alogp`, `acd_logp`, `acd_logd`
   - Водородные связи: `hba`, `hbd`, `hba_lipinski`, `hbd_lipinski`
   - Поверхность: `psa`
   - Гибкость: `rtb` (rotatable bonds)
   - Кольца: `aromatic_rings`, `heavy_atoms`
   - Правила: `ro3_pass`, `num_ro5_violations`, `lipinski_ro5_pass`, `num_lipinski_ro5_violations`, `lipinski_ro5_violations`
   - pKa: `acd_most_apka`, `acd_most_bpka`
   - Прочее: `molecular_species`, `full_molformula`, `qed_weighted`

3. **ChEMBL structure fields** (3 поля):
   - `standardized_smiles`: Стандартизированные SMILES
   - `standard_inchi`: Стандартный InChI
   - `standard_inchi_key`: Стандартный InChI ключ

4. **ChEMBL JSON fields** (11 полей):
   - `molecule_hierarchy`, `molecule_properties`, `molecule_structures`, `molecule_synonyms`
   - `atc_classifications`, `cross_references`, `biotherapeutic`, `chemical_probe`, `orphan`, `veterinary`, `helm_notation`

5. **ChEMBL text fields** (1 поле):
   - `all_names`: Агрегированные синонимы для поиска

6. **Fallback fields** (8 полей):
   - `fallback_reason`, `fallback_error_type`, `fallback_error_code`, `fallback_http_status`, `fallback_retry_after_sec`, `fallback_attempt`, `fallback_error_message`, `fallback_timestamp`

**COLUMN_ORDER:**

Порядок колонок фиксирован в `TestItemSchema._column_order` и включает ~150 колонок в следующем порядке:

1. Метаданные: `index`, `hash_row`, `hash_business_key`, `pipeline_version`, `run_id`, `source_system`, `chembl_release`, `extracted_at`
2. Fallback метаданные
3. Бизнес-ключ: `molecule_chembl_id`
4. ChEMBL core fields
5. ChEMBL property fields
6. ChEMBL structure fields
7. ChEMBL text fields
8. ChEMBL JSON fields

**Валидация:**

- `strict = True`: все колонки должны быть определены в схеме
- `coerce = True`: автоматическое приведение типов
- `ordered = True`: порядок колонок строго соблюдается
- Regex валидация для `molecule_chembl_id`: `^CHEMBL\d+$`
- Nullable policy: большинство полей nullable, только `molecule_chembl_id` NOT NULL
- Range checks: `ge=1` для `molregno`, `parent_molregno`, `ge=0` для числовых полей

## 8. Выходные данные

### 8.1 Формат вывода

**Основной CSV:** `testitem_{date}.csv`

- Содержит все поля из `TestItemSchema` в порядке `COLUMN_ORDER`
- Сортировка по `molecule_chembl_id` (детерминизм)
- Атомарная запись через временные файлы

**QC отчет:** `testitem_{date}_quality_report.csv`

- Метрики качества данных
- Распределения ключевых полей
- Пропуски и уникальности
- Результаты referential integrity checks

**Meta YAML:** `testitem_{date}_meta.yaml` (в extended режиме)

Структура:

```yaml
pipeline_version: "1.0.0"
run_id: "a1b2c3d4e5f6"
git_commit: "abc123..."
config_hash: "def456..."
python_version: "3.11.5"
deps_fingerprint: "ghi789..."

chembl_release: "CHEMBL_36"
chembl_base_url: "https://www.ebi.ac.uk/chembl/api/data"

extracted_at: "2025-10-28T12:00:00Z"
processing_time_s: 123.45

row_count: 1234

# Metrics
metrics:
  total_molecules: 1234
  success_count: 1200
  fallback_count: 24
  error_count: 10
  success_rate: 0.992
  api_calls: 45
  cache_hits: 1189

# QC summary
qc:
  passed: true
  issues: []
  warnings:
    - type: "referential_integrity_loss"
      count: 5

# Output artifacts
output_files:
  csv: "testitem_20251028.csv"
  qc: "testitem_20251028_quality_report.csv"
  meta: "testitem_20251028_meta.yaml"

checksums:
  csv: sha256: "abc123..."
  qc: sha256: "def456..."

column_order:
  - "index"
  - "hash_row"
  # ... полный список из TestItemSchema._column_order
```

### 8.2 Атомарная запись

**Механизм:** Временный файл в run_id-scoped директории + atomic rename

```python
def _atomic_write(
    self,
    content: bytes,
    target_path: Path,
    run_id: str
) -> Path:
    """Atomic write with run_id-scoped temp directory."""
    
    # Temp directory per run
    temp_dir = self.output_dir / ".tmp" / run_id
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Temp file
    temp_path = temp_dir / f"{target_path.name}.tmp"
    
    # Write
    temp_path.write_bytes(content)
    
    # Atomic rename (Windows: os.replace() instead of shutil.move())
    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(temp_path), str(target_path))
    
    return target_path
```

## 9. Key Identifiers

- **Business Key**: `molecule_chembl_id`
- **Sort Key**: `molecule_chembl_id`

## 10. Детерминизм

**Sort keys:** `["molecule_chembl_id"]`

TestItem pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются по `molecule_chembl_id` перед записью (определяется в `determinism.sort.by`)
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`, `extracted_at`)
  - `hash_business_key`: хеш бизнес-ключа (`molecule_chembl_id`)
- **Canonicalization:** Все значения нормализуются перед хешированием:
  - JSON с `sort_keys=True`, `separators=(',', ':')`
  - ISO8601 UTC для всех datetimes
  - Float формат: `%.6f`
  - Empty/None значения: `""` (пустая строка)
  - Column order: строго по `TestItemSchema._column_order`
- **Column order:** Фиксированный порядок колонок из `TestItemSchema._column_order` (~150 колонок)
- **Meta.yaml:** Содержит `pipeline_version`, `chembl_release`, `row_count`, checksums, `hash_algo`, `hash_policy_version`

**Guarantees:**

- Бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации
- Стабильный порядок строк и колонок
- Идентичные хеши для идентичных данных

**Каноническая сериализация для хеширования:**

```python
def _canonicalize_row_for_hash(row: dict, column_order: list[str]) -> str:
    """
    Canonical serialization for deterministic hashing.
    
    Rules:
    1. JSON with sort_keys=True, separators=(',', ':')
    2. ISO8601 UTC for all datetimes
    3. Float format: %.6f
    4. Empty/None values: "" (empty string)
    5. Column order: строго по column_order
    """
    
    canonical = {}
    
    for col in column_order:
        value = row.get(col)
        
        # Convert to canonical representation
        if pd.isna(value):
            canonical[col] = ""
        elif isinstance(value, float):
            canonical[col] = f"{value:.6f}"
        elif isinstance(value, datetime):
            canonical[col] = value.isoformat() + "Z"
        elif isinstance(value, (dict, list)):
            canonical[col] = json.dumps(value, sort_keys=True, separators=(',', ':'))
        else:
            canonical[col] = str(value)
    
    # JSON serialization with strict format
    return json.dumps(canonical, sort_keys=True, separators=(',', ':'))
```

For detailed policy, see [Determinism Policy](../determinism/00-determinism-policy.md).

## 11. QC/QA

**Ключевые метрики успеха:**

| Метрика | TestItem | Критичность |
|---------|----------|-------------|
| **ChEMBL coverage** | 100% идентификаторов | HIGH |
| **Duplicate ratio** | 0% (дубликаты недопустимы) | CRITICAL |
| **Fallback ratio** | ≤20% fallback записей | MEDIUM |
| **Parent referential integrity** | 100% (parent_chembl_id должен существовать) | HIGH |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**

1. **`testitem.duplicate_ratio`**: Дубликаты по `molecule_chembl_id`
   - Порог: `0.0` (критично)
   - Метрика: количество дубликатов / общее количество строк
   - Действие: удаление дубликатов при превышении порога

2. **`testitem.fallback_ratio`**: Процент fallback записей
   - Порог: `0.2` (20%)
   - Метрика: количество fallback записей / общее количество строк
   - Действие: предупреждение при превышении порога

3. **`testitem.parent_missing_ratio`**: Referential integrity для `parent_chembl_id`
   - Порог: `0.0` (критично)
   - Метрика: количество `parent_chembl_id`, которые не существуют в наборе `molecule_chembl_id` / общее количество ссылок
   - Действие: ошибка при превышении порога

4. **Валидация форматов идентификаторов**: Regex для `molecule_chembl_id`
   - Паттерн: `^CHEMBL\d+$`
   - Действие: ошибка при несоответствии

5. **Schema validation**: Соответствие схеме Pandera
   - Проверка типов, nullable, диапазонов, regex
   - Действие: ошибка при нарушении схемы

**Пороги качества:**

- `testitem.duplicate_ratio` должен быть `0.0` (критично)
- `testitem.fallback_ratio` должен быть `≤ 0.2` (средний приоритет)
- `testitem.parent_missing_ratio` должен быть `0.0` (критично)

**QC отчеты:**

- Генерируется `testitem_{date}_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с распределениями
- Все метрики записываются в `meta.yaml` в секции `qc`

**Referential Integrity Check:**

```python
def _check_referential_integrity(self, df: pd.DataFrame) -> None:
    """Ensure parent_chembl_id values resolve to known molecules."""
    
    required_columns = {"molecule_chembl_id", "parent_chembl_id"}
    if df.empty or not required_columns.issubset(df.columns):
        return
    
    parent_series = df["parent_chembl_id"].dropna()
    molecule_ids = df["molecule_chembl_id"].unique()
    known_ids = set(molecule_ids)
    
    missing_mask = ~parent_series.isin(known_ids)
    missing_count = int(missing_mask.sum())
    missing_ratio = missing_count / len(parent_series) if len(parent_series) else 0.0
    
    threshold = float(self.config.qc.thresholds.get("testitem.parent_missing_ratio", 0.0))
    
    if missing_ratio > threshold:
        raise ValueError(
            "Referential integrity violation: parent_chembl_id references missing molecules"
        )
```

For detailed QC metrics and policies, see [QC Overview](../qc/00-qc-overview.md).

## 12. Логирование и трассировка

TestItem pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**

- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`chembl_testitem`)
- `duration`: Время выполнения стадии в секундах
- `row_count`: Количество обработанных строк

**Структурированные события:**

- `pipeline_initialized`: Инициализация пайплайна с параметрами конфигурации
- `pipeline_started`: Начало выполнения пайплайна
- `reading_input`: Чтение входного файла
- `input_file_not_found`: Предупреждение при отсутствии входного файла
- `extraction_completed`: Завершение стадии извлечения с метриками
- `molecule_fetch_summary`: Сводка по извлечению молекул (requested, fetched, cache_hits, api_success_count, fallback_count)
- `transform_started`: Начало стадии трансформации
- `transform_completed`: Завершение стадии трансформации
- `validation_started`: Начало валидации
- `validation_completed`: Завершение валидации с количеством issues
- `qc_metric`: Логирование QC метрик (metric, value, threshold, severity, count, details)
- `referential_integrity_passed`: Успешная проверка referential integrity
- `referential_integrity_failure`: Ошибка referential integrity (relation, missing_count, missing_ratio, threshold, severity)
- `identifier_format_error`: Ошибка формата идентификатора (column, invalid_count, sample_values)
- `write_started`: Начало записи результатов
- `write_completed`: Завершение записи результатов
- `pipeline_completed`: Успешное завершение пайплайна
- `pipeline_failed`: Ошибка выполнения с деталями

**Примеры JSON-логов:**

```json
{
  "event": "pipeline_initialized",
  "run_id": "a1b2c3d4e5f6g7h8",
  "pipeline": "chembl_testitem",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "molecule_fetch_summary",
  "run_id": "a1b2c3d4e5f6g7h8",
  "requested": 1250,
  "fetched": 1250,
  "cache_hits": 1189,
  "api_success_count": 61,
  "fallback_count": 0,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}

{
  "event": "qc_metric",
  "run_id": "a1b2c3d4e5f6g7h8",
  "metric": "testitem.duplicate_ratio",
  "value": 0.0,
  "threshold": 0.0,
  "severity": "info",
  "count": 0,
  "timestamp": "2025-01-15T10:31:30.456789Z"
}

{
  "event": "validation_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "rows": 1250,
  "issues": 0,
  "timestamp": "2025-01-15T10:31:45.567890Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "duration": 120.5,
  "row_count": 1250,
  "timestamp": "2025-01-15T10:32:00.678901Z"
}
```

**Формат вывода:**

- Консоль: текстовый формат для удобства чтения
- Файлы: JSON формат для машинной обработки и анализа
- Ротация: автоматическая ротация лог-файлов (10MB × 10 файлов)

**Трассировка:**

- Все операции связаны через `run_id` для отслеживания полного жизненного цикла пайплайна
- Каждая стадия логирует начало и завершение с метриками производительности
- Ошибки логируются с полным контекстом и stack trace
- QC метрики логируются с деталями для анализа качества данных

For detailed logging configuration and API, see [Logging Overview](../logging/00-overview.md).
