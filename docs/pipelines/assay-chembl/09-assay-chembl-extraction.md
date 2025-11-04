# 5. Извлечение данных для assay из ChEMBL

## Обзор

Документ описывает спецификацию извлечения данных ассаев (assay) из ChEMBL API с детерминированностью, полной воспроизводимостью результатов и защитой от потери данных.

## Архитектура

```text

AssayPipeline
├── Extract Stage
│   ├── ChEMBLClient (batch_size=25, URL limit)
│   ├── CacheManager (TTL, release-scoped invalidation)
│   ├── CircuitBreaker (failure tracking, recovery)
│   └── FallbackManager (extended error metadata)
├── Transform Stage
│   ├── NestedStructureExpansion (long format)
│   │   ├── AssayParametersExplode
│   │   ├── VariantSequencesExplode
│   │   └── AssayClassificationsExplode
│   ├── Enrichment (whitelist only)
│   │   ├── TargetEnrichment (7 fields whitelist)
│   │   └── AssayClassEnrichment (7 fields whitelist)
│   └── Normalization (strict NA policy)
├── Validate Stage
│   ├── PanderaSchema (strict=True, nullable dtypes)
│   ├── ReferentialIntegrityCheck
│   └── QualityProfile (fail thresholds)
└── Write Stage
    ├── AtomicWriter (run_id-scoped temp dirs)
    ├── CanonicalSerialization (hash generation)
    └── MetadataBuilder (full provenance)

```

## 1. Входные данные

### 1.1 Формат входных данных

**Файл:** CSV или DataFrame

**Обязательные поля:**

- `assay_chembl_id` (StringDtype, NOT NULL): ChEMBL идентификатор ассая в формате `CHEMBL\d+`

**Опциональные поля:**

- `target_chembl_id` (StringDtype): для фильтрации по целевому белку

**Схема валидации:**

```python

# src/library/schemas/assay_schema.py

class AssayInputSchema(pa.DataFrameModel):
    assay_chembl_id: Series[str] = pa.Field(description="ChEMBL assay identifier")
    target_chembl_id: Series[str] = pa.Field(nullable=True, description="ChEMBL target identifier")

    class Config:
        strict = True
        coerce = True

```

### 1.2 Конфигурация

**Базовый стандарт:** см. `docs/configs/00-typed-configs-and-profiles.md`.

**Профильный файл:** `configs/pipelines/assay.yaml`, который объявляет `extends: "../base.yaml"` и проходит валидацию `PipelineConfig`.

**Критические переопределения:**

| Путь | Значение по умолчанию | Ограничение | Комментарий |
|------|-----------------------|-------------|-------------|
| `sources.chembl.batch_size` | 25 | `≤ 25` (жёсткое требование ChEMBL URL) | Проверяется на этапе валидации конфигурации. |
| `sources.chembl.max_url_length` | 2000 | `≤ 2000` | Используется для предиктивного троттлинга запросов. |
| `cache.namespace` | `"chembl"` | Не пусто | Обеспечивает release-scoped invalidation. |
| `determinism.sort.by` | `['assay_chembl_id', 'row_subtype', 'row_index']` | Первый ключ — `assay_chembl_id` | Сортировка применяется до агрегации; итоговый CSV следует `AssaySchema.Config.column_order`. |
| `determinism.column_order` | `AssaySchema.Config.column_order` (71 колонка) | Полный список обязателен | Нарушение приводит к падению `PipelineConfig`. |

| `enrichment.target_fields` | см. стандарт | Только whitelisted поля | Дополнение новых полей требует обновления документации. |

Секреты (API ключи) прокидываются через переменные окружения `BIOETL_SOURCES__CHEMBL__API_KEY` и `BIOETL_HTTP__GLOBAL__HEADERS__AUTHORIZATION`. Для быстрой настройки допускается использование CLI-переопределений, например `--set sources.chembl.batch_size=20`, однако изменения должны сопровождаться обоснованием в run metadata.

## 2. Процесс извлечения (Extract)

### 2.1 Инициализация пайплайна

**Класс:** `AssayPipeline` (`src/library/assay/pipeline.py`)

**Наследование:** `PipelineBase[AssayConfig]`

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

4. Кэш-ключи **ОБЯЗАТЕЛЬНО** содержат release: `assay:{release}:{assay_chembl_id}`

### 2.2 Батчевое извлечение из ChEMBL API

**Метод:** `AssayPipeline._extract_from_chembl()`

**Эндпоинт ChEMBL:** `/assay.json?assay_chembl_id__in={ids}`

**Размер батча:**

- **Конфиг:** `sources.chembl.batch_size: 25` (ОБЯЗАТЕЛЬНО)

- **Причина:** Жесткое ограничение длины URL в ChEMBL API (~2000 символов)

- **Валидация конфига:**

```python

if batch_size > 25:
      raise ConfigValidationError(
          "sources.chembl.batch_size must be <= 25 due to ChEMBL API URL length limit"
      )

```

**Алгоритм:**

```python

def _extract_from_chembl(self, data: pd.DataFrame) -> pd.DataFrame:
    """Extract assay data with 25-item batching."""

    # Счетчики метрик

    success_count = 0
    fallback_count = 0
    error_count = 0
    api_calls = 0
    cache_hits = 0

    # Жесткий размер батча

    BATCH_SIZE = 25

    assay_ids = data["assay_chembl_id"].tolist()

    # Батчевое извлечение

    for i in range(0, len(assay_ids), BATCH_SIZE):
        batch_ids = assay_ids[i:i + BATCH_SIZE]

        try:

            # Проверка кэша с release-scoping

            cached = self._check_cache(batch_ids, self.chembl_release)
            if cached:
                batch_data = cached
                cache_hits += len(batch_ids)
            else:
                batch_data = chembl_client.fetch_assays_batch(batch_ids)
                api_calls += 1
                self._store_cache(batch_data, self.chembl_release)

            # Обработка ответов

            for assay_id in batch_ids:
                assay_data = batch_data.get(assay_id)
                if assay_data and "error" not in assay_data:
                    if assay_data.get("source_system") == "ChEMBL_FALLBACK":
                        fallback_count += 1
                    else:
                        success_count += 1
                else:
                    error_count += 1

                    # Fallback с расширенными полями

                    assay_data = self._create_fallback_record(assay_id)

        except CircuitBreakerOpenError:

            # Fallback для всего батча

            for assay_id in batch_ids:
                assay_data = self._create_fallback_record(assay_id)
                fallback_count += 1

        except Exception as e:
            error_count += len(batch_ids)
            logger.error(f"Batch failed: {e}")

    # Логирование статистики

    logger.info({
        "total_assays": len(assay_ids),
        "success_count": success_count,
        "fallback_count": fallback_count,
        "error_count": error_count,
        "success_rate": (success_count + fallback_count) / len(assay_ids),
        "api_calls": api_calls,
        "cache_hits": cache_hits
    })

    return extracted_dataframe

```

### 2.3 Fallback механизм

**Условия активации:**

- HTTP 5xx ошибки

- Таймауты (ReadTimeout, ConnectTimeout)

- Circuit Breaker в состоянии OPEN

- 429/503 с `Retry-After` header (если exceed max retries)

**Расширенная запись fallback:**

```python

def _create_fallback_record(self, assay_id: str, error: Exception = None) -> dict:
    """Create fallback record with extended error metadata."""

    return {
        "assay_chembl_id": assay_id,
        "source_system": "ChEMBL_FALLBACK",
        "extracted_at": datetime.utcnow().isoformat() + "Z",

        # Расширенные поля ошибки

        "error_code": error.code if hasattr(error, 'code') else None,
        "http_status": error.status if hasattr(error, 'status') else None,
        "error_message": str(error) if error else "Fallback: API unavailable",
        "retry_after_sec": error.retry_after if hasattr(error, 'retry_after') else None,
        "attempt": error.attempt if hasattr(error, 'attempt') else None,
        "run_id": self.run_id,

        # Все остальные поля: None или "" (см. NA policy)

        **{field: None for field in EXPECTED_FIELDS}
    }

```

### 2.4 Развертывание вложенных структур

**ВАЖНО:** Текущая реализация "берет первый параметр" **НЕВЕРНА** и приводит к потере данных. Требуется рефакторинг.

#### 2.4.1 Assay Parameters (ПРАВИЛЬНАЯ СПЕЦИФИКАЦИЯ)

**Источник:** Поле `assay_parameters` (массив объектов)

**Проблема:** Текущий код берет только `first_param = params[0]` - **ПОТЕРЯ ДАННЫХ**

**Корректное решение:**

**Вариант A: Длинный формат (Long Format)** - **РЕКОМЕНДУЕТСЯ**

```python

def _expand_assay_parameters_long(self, assay_data: dict) -> pd.DataFrame:
    """Expand assay_parameters to long format with param_index."""

    params = assay_data.get("assay_parameters", [])
    assay_chembl_id = assay_data["assay_chembl_id"]

    if not params:
        return pd.DataFrame({
            "assay_chembl_id": [assay_chembl_id],
            "param_index": [None],
            "param_type": [None],
            "param_relation": [None],
            "param_value": [None],
            "param_units": [None],
            "param_text_value": [None],
            "param_standard_type": [None],
            "param_standard_value": [None],
            "param_standard_units": [None]
        })

    # Explode до длинного формата

    records = []
    for idx, param in enumerate(params):
        records.append({
            "assay_chembl_id": assay_chembl_id,
            "param_index": idx,  # Индекс для детерминизма

            "param_type": param.get("type"),
            "param_relation": param.get("relation"),
            "param_value": param.get("value"),
            "param_units": param.get("units"),
            "param_text_value": param.get("text_value"),
            "param_standard_type": param.get("standard_type"),
            "param_standard_value": param.get("standard_value"),
            "param_standard_units": param.get("standard_units")
        })

    return pd.DataFrame(records)

# В основном DataFrame добавляем признак типа строки

df["row_subtype"] = "assay"  # или "param" при explode

df["row_index"] = df.groupby("assay_chembl_id").cumcount()

```

**Вариант B: Широкий формат с индексацией** (опционально)

```python

def _expand_assay_parameters_wide(self, assay_data: dict, max_params: int = 5) -> dict:
    """Expand to wide format with deterministic column names."""

    params = assay_data.get("assay_parameters", [])

    result = {}
    for idx in range(min(len(params), max_params)):
        param = params[idx]
        prefix = f"assay_param_{idx}_"
        result[f"{prefix}type"] = param.get("type")
        result[f"{prefix}relation"] = param.get("relation")
        result[f"{prefix}value"] = param.get("value")

        # ... остальные поля

    return result

```

**Рекомендация:** Использовать **Вариант A (long format)** как основной, с опциональным pivot в wide при необходимости.

**Инвариант G7:** Расширение вложенных массивов только в long-format (parameters, variant_sequences, classifications); при невозможности — error; включить RI-чек "assay→target".

#### 2.4.2 Variant Sequences (ПРАВИЛЬНАЯ СПЕЦИФИКАЦИЯ)

**Источник:** Поле `variant_sequence` (может быть объект ИЛИ массив)

**Проблема:** Код предполагает только объект - **Риск потери данных при наличии списка**

**Корректное решение:**

```python

def _expand_variant_sequences(self, assay_data: dict) -> pd.DataFrame:
    """Expand variant_sequence(s) to long format."""

    variant = assay_data.get("variant_sequence")
    assay_chembl_id = assay_data["assay_chembl_id"]

    if not variant:
        return pd.DataFrame({
            "assay_chembl_id": [assay_chembl_id],
            "variant_index": [None],
            "variant_id": [None],
            "variant_base_accession": [None],
            "variant_mutation": [None],
            "variant_sequence": [None],
            "variant_accession_reported": [None]
        })

    # Поддержка и объекта, и списка

    variants = [variant] if isinstance(variant, dict) else variant

    if not isinstance(variants, list):
        variants = []

    records = []
    for idx, var in enumerate(variants):
        records.append({
            "assay_chembl_id": assay_chembl_id,
            "variant_index": idx,
            "variant_id": var.get("variant_id"),
            "variant_base_accession": var.get("accession") or var.get("base_accession"),
            "variant_mutation": var.get("mutation"),
            "variant_sequence": var.get("sequence"),
            "variant_accession_reported": var.get("accession")
        })

    return pd.DataFrame(records)

```

#### 2.4.3 Assay Classifications

**Источник:** Поле `assay_classifications` (JSON строка с массивом)

**Текущая логика:** Извлекается **только первый** assay_class_id

**Корректное решение:**

```python

def _expand_assay_classifications(self, assay_data: dict) -> pd.DataFrame:
    """Extract all assay_class_id from classifications."""

    classifications = assay_data.get("assay_classifications")
    assay_chembl_id = assay_data["assay_chembl_id"]

    if not classifications:
        return pd.DataFrame({
            "assay_chembl_id": [assay_chembl_id],
            "class_index": [None],
            "assay_class_id": [None]
        })

    try:
        class_data = json.loads(classifications)
        if isinstance(class_data, list):

            # Извлечь все class_id

            records = []
            for idx, class_item in enumerate(class_data):
                if isinstance(class_item, dict) and "assay_class_id" in class_item:
                    records.append({
                        "assay_chembl_id": assay_chembl_id,
                        "class_index": idx,
                        "assay_class_id": class_item["assay_class_id"]
                    })
            return pd.DataFrame(records)
    except (json.JSONDecodeError, TypeError):
        pass

    return pd.DataFrame()

```

### 2.5 Обогащение данными (Enrichment)

**См. также**: Детали реализации см. в [Pipeline Contract](../etl_contract/01-pipeline-contract.md).

#### 2.5.1 Обогащение Target данными

**Метод:** `AssayPipeline._enrich_with_target_data()`

**Эндпоинт:** `/target/{target_chembl_id}`

**ВАЖНО:** Текущий код "тащит всё из /target" - **это раздувает схему**

**Корректное решение с Whitelist:**

```python

TARGET_ENRICHMENT_WHITELIST = [
    "target_chembl_id",
    "pref_name",
    "organism",
    "target_type",
    "species_group_flag",
    "tax_id",
    "component_count"
]

def _enrich_with_target_data(self, chembl_client, target_ids: list[str]) -> pd.DataFrame:
    """Enrich with whitelisted target fields only."""

    target_records = []
    enriched_ids = set()

    for target_id in target_ids:
        try:

            # Fetch from API

            target_data = chembl_client.fetch_by_target_id(target_id)

            if target_data and "error" not in target_data:

                # Extract only whitelisted fields

                enriched_record = {
                    field: target_data.get(field)
                    for field in TARGET_ENRICHMENT_WHITELIST
                }
                target_records.append(enriched_record)
                enriched_ids.add(target_id)
        except Exception as e:
            logger.warning(f"Failed to fetch target {target_id}: {e}")

    # Referential integrity check

    missing_ids = set(target_ids) - enriched_ids
    if missing_ids:
        logger.warning(f"Missing targets in enrichment: {len(missing_ids)} targets")

        # Запись в quality report

    return pd.DataFrame(target_records)

# Merge с whitelist validation

def _merge_target_data(self, assay_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """Merge with validation of whitelisted fields."""

    if target_df.empty:
        return assay_df

    # Проверка на неразрешенные поля

    unexpected_fields = set(target_df.columns) - set(TARGET_ENRICHMENT_WHITELIST)
    if unexpected_fields and self.config.enrichment.strict_enrichment:
        raise ValueError(f"Unexpected target fields: {unexpected_fields}")

    # Merge

    result = assay_df.merge(
        target_df,
        on="target_chembl_id",
        how="left",
        suffixes=("", "_target")
    )

    # Report join losses

    join_loss = result["target_chembl_id"].notna() & result["pref_name"].isna()
    if join_loss.any():
        logger.warning(f"Join losses: {join_loss.sum()} assays without target data")

    return result

```

**Инвариант G8:** --strict-enrichment + schema check; whitelist-enrichment обязателен; запрет лишних полей при enrichment.

#### 2.5.2 Обогащение Assay Class данными

**Метод:** `AssayPipeline._enrich_with_assay_classes()`

**Эндпоинт:** `/assay_class/{assay_class_id}`

**Whitelist:**

```python

ASSAY_CLASS_ENRICHMENT_WHITELIST = [
    "assay_class_id",
    "bao_id",  # Maps to assay_class_bao_id in output

    "class_type",
    "l1",
    "l2",
    "l3",
    "description"
]

# Аналогичная логика с whitelist и RI check

```

## 3. Нормализация данных (Normalize)

### 3.1 Каноническая сериализация для хеширования

**Метод:** `_canonicalize_row_for_hash()` (НОВЫЙ)

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

### 3.2 Хеширование

```python

def _calculate_hashes(self, df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Calculate hash_row and hash_business_key."""

    hash_row = df.apply(
        lambda row: sha256(
            self._canonicalize_row_for_hash(row.to_dict(), self.column_order)
            .encode('utf-8')
        ).hexdigest(),
        axis=1
    )

    hash_bk = df["assay_chembl_id"].apply(
        lambda x: sha256(x.encode('utf-8')).hexdigest()
    )

    return hash_row, hash_bk

```

### 3.3 Системные метаданные

```python

def _add_system_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
    """Add system metadata fields."""

    # Run metadata

    df["run_id"] = self.run_id
    df["git_commit"] = self.git_commit
    df["config_hash"] = self.config_hash
    df["python_version"] = self.python_version
    df["chembl_base_url"] = self.chembl_base_url

    # Pipeline metadata

    df["pipeline_version"] = self.config.pipeline.version
    df["source_system"] = "ChEMBL"  # or "ChEMBL_FALLBACK"

    df["chembl_release"] = self.chembl_release
    df["extracted_at"] = datetime.utcnow().isoformat() + "Z"

    # Hashes

    df["hash_row"], df["hash_business_key"] = self._calculate_hashes(df)

    # Index

    df["index"] = pd.Int64Dtype()(range(len(df)))

    return df

```

### 3.4 Настройки типов данных (Dtypes)

**КРИТИЧЕСКИ:** Использовать nullable dtypes, никаких `object`

```python

DTYPES_CONFIG = {
    "assay_chembl_id": pd.StringDtype(),
    "target_chembl_id": pd.StringDtype(),
    "assay_type": pd.StringDtype(),
    "confidence_score": pd.Int64Dtype(),
    "assay_tax_id": pd.Int64Dtype(),
    "assay_param_value": pd.Float64Dtype(),
    "variant_id": pd.Int64Dtype(),
    "index": pd.Int64Dtype(),

    # ... все поля с явными nullable dtypes

}

```

## 4. Валидация и QC

### 4.1 Referential Integrity Check

```python

def _check_referential_integrity(self, df: pd.DataFrame) -> dict:
    """Check referential integrity and report losses."""

    issues = []

    # Check target_chembl_id

    if "target_chembl_id" in df.columns:
        enriched_targets = set(df[df["pref_name"].notna()]["target_chembl_id"].unique())
        all_targets = set(df["target_chembl_id"].dropna().unique())
        missing = all_targets - enriched_targets

        if missing:
            issues.append({
                "type": "referential_integrity",
                "field": "target_chembl_id",
                "missing_count": len(missing),
                "missing_ids": list(missing)[:10]  # Sample

            })

    # Check assay_class_id

    if "assay_class_id" in df.columns:
        enriched_classes = set(df[df["assay_class_bao_id"].notna()]["assay_class_id"].unique())
        all_classes = set(df["assay_class_id"].dropna().unique())
        missing = all_classes - enriched_classes

        if missing:
            issues.append({
                "type": "referential_integrity",
                "field": "assay_class_id",
                "missing_count": len(missing),
                "missing_ids": list(missing)[:10]
            })

    return {
        "total_issues": len(issues),
        "issues": issues
    }

```

### 4.2 QC Profile

```python

qc_profile = {
    "checks": [
        {
            "name": "missing_assay_chembl_id",
            "threshold": 0.0,
            "severity": "ERROR",
            "metric": lambda df: df["assay_chembl_id"].isna().sum() / len(df)
        },
        {
            "name": "duplicate_primary_keys",
            "threshold": 0.0,
            "severity": "ERROR",
            "metric": lambda df: df["assay_chembl_id"].duplicated().sum()
        },
        {
            "name": "invalid_chembl_id_pattern",
            "threshold": 0.05,
            "severity": "ERROR",
            "metric": lambda df: ~df["assay_chembl_id"].str.match(r'^CHEMBL\d+$').sum()
        },
        {
            "name": "referential_integrity_loss",
            "threshold": 0.1,
            "severity": "WARNING",
            "metric": lambda df: ...  # из RI check

        }
    ]
}

```

## 5. Запись результатов (Write)

### 5.1 Atomic Writes

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

    # Validate (optional)

    # self._validate_written_file(temp_path)

    # Atomic rename

    # Windows: os.replace() instead of shutil.move()

    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(str(temp_path), str(target_path))

    # Cleanup temp dir

    try:
        temp_dir.rmdir()  # Only if empty

    except OSError:
        pass

    return target_path

```

### 5.2 Metadata Builder

```yaml

# assay_{date_tag}_meta.yaml

pipeline_version: "2.0.0"
run_id: "a1b2c3d4e5f6"
git_commit: "abc123..."
config_hash: "def456..."
python_version: "3.11.5"
deps_fingerprint: "ghi789..."

chembl_release: "CHEMBL_36"
chembl_base_url: "<https://www.ebi.ac.uk/chembl/api/data>"

extracted_at: "2025-10-28T12:00:00Z"
processing_time_s: 123.45

row_count: 1234

# Metrics

metrics:
  total_assays: 1234
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
  csv: "assay_20251028.csv"
  qc: "assay_20251028_qc.csv"
  quality_report: "assay_20251028_quality_report.csv"
  meta: "assay_20251028_meta.yaml"

checksums:
  csv: sha256: "abc123..."
  qc: sha256: "def456..."
  quality_report: sha256: "ghi789..."

```

## 6. Корреляционный анализ

**ВАЖНО:** Корреляционный анализ **НЕ часть ETL** и должен быть **опциональным**

```yaml

# configs/pipelines/assay.yaml

postprocess:
  correlation:
    enabled: false  # По умолчанию ВЫКЛЮЧЕН

    steps:

      - name: "correlation_analysis"

        enabled: true  # Включается только явно

```

**Причина:** Гарантировать бит-в-бит идентичность с/без корреляций практически невозможно из-за non-deterministic алгоритмов.

## 7. Конфигурация пайплайна

### 7.1 Обзор

Assay pipeline использует декларативный YAML-конфигурационный файл, который отделяет логику пайплайна от его поведения. Конфигурация валидируется на этапе выполнения через типизированные Pydantic-модели (`PipelineConfig`). Подробности о системе конфигурации см. в [Typed Configurations](../configs/00-typed-configs-and-profiles.md).

**Расположение конфига:** `configs/pipelines/chembl/assay.yaml`

**Профили по умолчанию:** Конфигурация наследует от `configs/profiles/base.yaml` и `configs/profiles/determinism.yaml` через `extends`.

### 7.2 Структура конфигурации

Ниже приведена полная структура конфигурационного файла для assay pipeline с описанием всех секций:

```yaml
# configs/pipelines/chembl/assay.yaml

# Наследование базовых профилей
extends:
  - ../profiles/base.yaml      # Общие настройки для всех пайплайнов
  - ../profiles/determinism.yaml  # Стандартные настройки детерминизма
  - ../profiles/network.yaml     # Опционально: сетевые настройки

# -----------------------------------------------------------------------------
# Метаданные пайплайна
# -----------------------------------------------------------------------------
pipeline:
  name: "assay"
  version: "2.0.0"
  owner: "chembl-team"
  description: "Extract and normalize ChEMBL assay metadata"

# -----------------------------------------------------------------------------
# HTTP-конфигурация
# -----------------------------------------------------------------------------
http:
  default:
    timeout_sec: 60.0
    connect_timeout_sec: 15.0
    read_timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 60.0
      statuses: [408, 429, 500, 502, 503, 504]
    rate_limit:
      max_calls: 10
      period: 1.0
    rate_limit_jitter: true
    headers:
      User-Agent: "BioETL/1.0 (UnifiedAPIClient)"
      Accept: "application/json"
  
  # Именованный профиль для ChEMBL
  profiles:
    chembl:
      timeout_sec: 30.0
      retries:
        total: 7
      rate_limit:
        max_calls: 15
        period: 1.0

# -----------------------------------------------------------------------------
# Кэширование
# -----------------------------------------------------------------------------
cache:
  enabled: true
  directory: "http_cache"
  ttl: 86400  # 24 часа
  namespace: "chembl"  # Обеспечивает release-scoped invalidation

# -----------------------------------------------------------------------------
# Пути
# -----------------------------------------------------------------------------
paths:
  input_root: "data/input"
  output_root: "data/output"
  cache_root: ".cache"

# -----------------------------------------------------------------------------
# Источники данных
# -----------------------------------------------------------------------------
sources:
  chembl:
    enabled: true
    description: "ChEMBL Data Web Services API"
    http_profile: "chembl"  # Ссылка на именованный HTTP-профиль
    batch_size: 25  # КРИТИЧЕСКИ: <= 25 (жёсткое ограничение URL длины)
    max_url_length: 2000  # Максимальная длина URL для предиктивного троттлинга
    parameters:
      endpoint: "/assay.json"
      base_url: "https://www.ebi.ac.uk/chembl/api/data"

# -----------------------------------------------------------------------------
# Детерминизм
# -----------------------------------------------------------------------------
determinism:
  enabled: true
  hash_policy_version: "1.0.0"
  float_precision: 6
  datetime_format: "iso8601"
  column_validation_ignore_suffixes: ["_scd", "_temp", "_meta", "_tmp"]
  
  # Ключи сортировки (обязательно: первый ключ - assay_chembl_id)
  sort:
    by: ["assay_chembl_id", "row_subtype", "row_index"]
    ascending: [true, true, true]
    na_position: "last"
  
  # Фиксированный порядок колонок (71 колонка из AssaySchema)
  column_order:
    - "assay_chembl_id"
    - "target_chembl_id"
    - "assay_type"
    - "confidence_score"
    # ... остальные 67 колонок в порядке из AssaySchema.Config.column_order
  
  # Хеширование
  hashing:
    algorithm: "sha256"
    row_fields: []  # Все колонки из column_order (кроме exclude_fields)
    business_key_fields: ["assay_chembl_id"]
    exclude_fields: ["generated_at", "run_id"]
  
  # Сериализация
  serialization:
    csv:
      separator: ","
      quoting: "ALL"
      na_rep: ""
    booleans: ["True", "False"]
    nan_rep: "NaN"
  
  # Окружение
  environment:
    timezone: "UTC"
    locale: "C"
  
  # Запись
  write:
    strategy: "atomic"
  
  # Метаданные
  meta:
    location: "sibling"
    include_fields: []
    exclude_fields: []

# -----------------------------------------------------------------------------
# Валидация
# -----------------------------------------------------------------------------
validation:
  schema_in: "bioetl.schemas.chembl.assay.AssayInputSchema"  # Опционально
  schema_out: "bioetl.schemas.chembl.assay.AssayOutputSchema"  # Обязательно
  strict: true  # Строгая проверка порядка колонок
  coerce: true  # Приведение типов в Pandera

# -----------------------------------------------------------------------------
# Обогащение данными (Enrichment)
# -----------------------------------------------------------------------------
enrichment:
  enabled: true
  strict_enrichment: true  # Запрет неразрешенных полей
  
  # Whitelist для Target enrichment
  target_fields:
    - "target_chembl_id"
    - "pref_name"
    - "organism"
    - "target_type"
    - "species_group_flag"
    - "tax_id"
    - "component_count"
  
  # Whitelist для Assay Class enrichment
  assay_class_fields:
    - "assay_class_id"
    - "bao_id"
    - "class_type"
    - "l1"
    - "l2"
    - "l3"
    - "description"

# -----------------------------------------------------------------------------
# Материализация
# -----------------------------------------------------------------------------
materialization:
  root: "data/output"
  default_format: "parquet"
  pipeline_subdir: "assay"
  filename_template: "assay_{date_tag}.{format}"

# -----------------------------------------------------------------------------
# Fallback механизмы
# -----------------------------------------------------------------------------
fallbacks:
  enabled: true
  max_depth: null  # Без ограничения глубины

# -----------------------------------------------------------------------------
# Постобработка (опционально)
# -----------------------------------------------------------------------------
postprocess:
  correlation:
    enabled: false  # По умолчанию выключен
    steps:
      - name: "correlation_analysis"
        enabled: false
```

### 7.3 Критические параметры

| Параметр | Значение | Обоснование | Валидация |
|----------|----------|------------|-----------|
| `sources.chembl.batch_size` | `25` | Жёсткое ограничение длины URL в ChEMBL API (~2000 символов) | `if batch_size > 25: raise ConfigValidationError` |
| `sources.chembl.max_url_length` | `2000` | Максимальная длина URL для предиктивного троттлинга | `<= 2000` |
| `determinism.sort.by[0]` | `"assay_chembl_id"` | Первый ключ сортировки должен быть бизнес-ключом | Обязательно |
| `determinism.column_order` | `71 колонка` | Полный список колонок из `AssaySchema.Config.column_order` | Проверяется на соответствие схеме |
| `validation.schema_out` | `"bioetl.schemas.chembl.assay.AssayOutputSchema"` | Обязательная ссылка на Pandera-схему | Должен существовать и быть импортируемым |
| `enrichment.strict_enrichment` | `true` | Запрет неразрешенных полей при enrichment | Проверка whitelist |

### 7.4 Валидация конфигурации

Конфигурация валидируется через Pydantic-модель `PipelineConfig` при загрузке:

1. **Типобезопасность:** Все значения проверяются на соответствие типам
2. **Обязательные поля:** Отсутствие обязательных полей приводит к ошибке
3. **Неизвестные ключи:** Неизвестные ключи запрещены (`extra="forbid"`)
4. **Кросс-полевые инварианты:** Проверка согласованности (например, длина `sort.by` и `sort.ascending`)

**Пример ошибки валидации:**

```text
1 validation error for PipelineConfig
sources.chembl.batch_size
  Value error, sources.chembl.batch_size must be <= 25 due to ChEMBL API URL length limit
```

### 7.5 Переопределения через CLI

Параметры конфигурации могут быть переопределены через CLI флаг `--set`:

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --set sources.chembl.batch_size=20 \
  --set determinism.sort.by='["assay_chembl_id"]' \
  --set enrichment.strict_enrichment=false
```

### 7.6 Переменные окружения

Наивысший приоритет имеют переменные окружения (формат: `BIOETL__<SECTION>__<KEY>__<SUBKEY>`):

```bash
export BIOETL__SOURCES__CHEMBL__BATCH_SIZE=25
export BIOETL__HTTP__DEFAULT__TIMEOUT_SEC=90
export BIOETL__DETERMINISM__FLOAT_PRECISION=4
```

### 7.7 Пример полного конфига

Полный пример конфигурационного файла для assay pipeline доступен в `configs/pipelines/chembl/assay.yaml`. Конфигурация включает все необходимые секции для работы пайплайна с детерминизмом, валидацией и обогащением данными.

## 8. Pandera схема валидации

### 8.1 Обзор

Assay pipeline использует Pandera для строгой валидации данных перед записью. Схема валидации определяет структуру, типы данных, порядок колонок и ограничения для всех записей. Подробности о политике Pandera схем см. в [Pandera Schema Policy](../schemas/00-pandera-policy.md).

**Расположение схемы:** `src/bioetl/schemas/chembl/assay/assay_output_schema.py`

**Ссылка в конфиге:** `validation.schema_out: "bioetl.schemas.chembl.assay.AssayOutputSchema"`

**Версионирование:** Схема имеет семантическую версию (`MAJOR.MINOR.PATCH`), которая фиксируется в `meta.yaml` для каждой записи пайплайна.

### 8.2 Требования к схеме

Схема валидации для assay pipeline должна соответствовать следующим требованиям:

1. **Строгость:** `strict=True` - все колонки должны быть явно определены
2. **Приведение типов:** `coerce=True` - автоматическое приведение типов данных
3. **Порядок колонок:** `ordered=True` - фиксированный порядок колонок (71 колонка)
4. **Nullable dtypes:** Использование nullable dtypes (`pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`) вместо `object`
5. **Бизнес-ключ:** Валидация уникальности `assay_chembl_id`

### 8.3 Структура схемы

Ниже приведена структура Pandera схемы для assay pipeline:

```python
# src/bioetl/schemas/chembl/assay/assay_output_schema.py

import pandera as pa
from pandera.typing import Series, DateTime, String, Int64, Float64
from typing import Optional

# Версия схемы
SCHEMA_VERSION = "2.0.0"

class AssayOutputSchema(pa.DataFrameModel):
    """Pandera schema for ChEMBL assay output data."""
    
    # Бизнес-ключ (обязательное поле, NOT NULL)
    assay_chembl_id: Series[str] = pa.Field(
        description="ChEMBL assay identifier",
        nullable=False,
        regex="^CHEMBL\\d+$"
    )
    
    # Основные поля assay
    target_chembl_id: Series[str] = pa.Field(
        description="ChEMBL target identifier",
        nullable=True
    )
    assay_type: Series[str] = pa.Field(
        description="Type of assay",
        nullable=True
    )
    confidence_score: Series[Int64] = pa.Field(
        description="Confidence score",
        nullable=True,
        ge=0,
        le=9
    )
    assay_tax_id: Series[Int64] = pa.Field(
        description="Taxonomy ID",
        nullable=True
    )
    
    # Параметры assay (long format)
    param_index: Series[Int64] = pa.Field(
        description="Index of assay parameter",
        nullable=True
    )
    param_type: Series[str] = pa.Field(
        description="Type of parameter",
        nullable=True
    )
    param_value: Series[Float64] = pa.Field(
        description="Parameter value",
        nullable=True
    )
    # ... остальные поля параметров
    
    # Variant sequences (long format)
    variant_index: Series[Int64] = pa.Field(
        description="Index of variant sequence",
        nullable=True
    )
    variant_id: Series[Int64] = pa.Field(
        description="Variant ID",
        nullable=True
    )
    # ... остальные поля variant
    
    # Assay classifications (long format)
    class_index: Series[Int64] = pa.Field(
        description="Index of assay classification",
        nullable=True
    )
    assay_class_id: Series[str] = pa.Field(
        description="Assay class identifier",
        nullable=True
    )
    
    # Enrichment fields (Target whitelist)
    pref_name: Series[str] = pa.Field(
        description="Preferred target name",
        nullable=True
    )
    organism: Series[str] = pa.Field(
        description="Target organism",
        nullable=True
    )
    # ... остальные enrichment поля
    
    # Системные метаданные
    run_id: Series[str] = pa.Field(
        description="Pipeline run ID",
        nullable=False
    )
    git_commit: Series[str] = pa.Field(
        description="Git commit SHA",
        nullable=False
    )
    config_hash: Series[str] = pa.Field(
        description="Configuration hash",
        nullable=False
    )
    pipeline_version: Series[str] = pa.Field(
        description="Pipeline version",
        nullable=False
    )
    source_system: Series[str] = pa.Field(
        description="Source system (ChEMBL or ChEMBL_FALLBACK)",
        nullable=False,
        isin=["ChEMBL", "ChEMBL_FALLBACK"]
    )
    chembl_release: Series[str] = pa.Field(
        description="ChEMBL release version",
        nullable=False
    )
    extracted_at: Series[DateTime] = pa.Field(
        description="Extraction timestamp (UTC)",
        nullable=False
    )
    
    # Хеши
    hash_row: Series[str] = pa.Field(
        description="SHA256 hash of entire row",
        nullable=False,
        regex="^[a-f0-9]{64}$"
    )
    hash_business_key: Series[str] = pa.Field(
        description="SHA256 hash of business key",
        nullable=False,
        regex="^[a-f0-9]{64}$"
    )
    
    # Индекс
    index: Series[Int64] = pa.Field(
        description="Row index",
        nullable=False
    )
    
    # Порядок колонок (71 колонка)
    class Config:
        strict = True
        coerce = True
        ordered = True
        column_order = [
            "assay_chembl_id",
            "target_chembl_id",
            "assay_type",
            "confidence_score",
            # ... остальные 67 колонок в фиксированном порядке
            "run_id",
            "git_commit",
            "config_hash",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "extracted_at",
            "hash_row",
            "hash_business_key",
            "index"
        ]
    
    # Валидация уникальности бизнес-ключа
    @pa.check("assay_chembl_id")
    def check_unique_assay_id(cls, series: Series[str]) -> Series[bool]:
        """Validate uniqueness of assay_chembl_id."""
        return ~series.duplicated()
```

### 8.4 Версионирование схемы

Схема версионируется по семантическому версионированию (`MAJOR.MINOR.PATCH`):

- **PATCH:** Обновления документации или корректировки, не влияющие на логику валидации
- **MINOR:** Обратно совместимые расширения (добавление nullable колонок с дефолтами, ослабление ограничений)
- **MAJOR:** Breaking changes (переименование/удаление колонок, изменение типов, изменение порядка колонок)

**Инвариант:** Версия схемы фиксируется в `meta.yaml` для каждой записи пайплайна:

```yaml
schema_version: "2.0.0"
```

### 8.5 Процесс валидации

Валидация выполняется в стадии `validate` пайплайна (`PipelineBase.validate()`):

1. **Загрузка схемы:** Динамическая загрузка схемы из `validation.schema_out`
2. **Lazy validation:** Выполнение `schema.validate(df, lazy=True)` для сбора всех ошибок
3. **Проверка порядка колонок:** Применение `ensure_column_order()` для соответствия `column_order`
4. **Запись версии:** Фиксация `schema_version` в `meta.yaml`

**Режимы валидации:**

- **Fail-closed (по умолчанию):** Пайплайн завершается при первой ошибке валидации
- **Fail-open (опционально):** Ошибки логируются как предупреждения, `schema_valid: false` в `meta.yaml`

### 8.6 Golden-тесты

Golden-артефакты обеспечивают регрессионное покрытие для поведения схемы:

1. **Хранение:** Golden CSV/Parquet и `meta.yaml` находятся в `tests/golden/assay/`
2. **Триггеры регенерации:**
   - Изменение версии схемы (любой уровень)
   - Изменение политики детерминизма
   - Обновление правил сортировки или хеширования
3. **Процесс:**
   - Запуск пайплайна с `--golden` для получения свежих артефактов
   - Выполнение тестов схемы
   - Проверка хешей и порядка колонок
   - Коммит обновленных golden-файлов вместе с изменениями версии схемы

### 8.7 Обработка schema drift

Schema drift — любое отклонение между runtime-данными и замороженной схемой:

- **Fail-closed (по умолчанию):** Пайплайн завершается при первой ошибке валидации
- **Fail-open (опционально):** Ошибки логируются как предупреждения, `schema_valid: false` в `meta.yaml`

**CLI контроль:**

- `--fail-on-schema-drift` (по умолчанию): Падение при отклонении схемы
- `--allow-schema-drift`: Разрешить отклонения (только для отладки)

**Операционные рекомендации:**

1. Production пайплайны **должны** работать в fail-closed режиме
2. Fail-open режим разрешен только для отладки и не должен писать в shared production buckets
3. Любое событие drift требует пересмотра схемы, обновления golden-артефактов и возможного изменения версии

### 8.8 Изменение схемы

Workflow для изменения схемы:

1. **Предложение изменения:** RFC с указанием затронутых пайплайнов и downstream потребителей
2. **Реализация:** Обновление схемы, изменение версии, регенерация golden-артефактов, обновление документации
3. **PR:** Включает:
   - Обновленный модуль схемы и константу версии
   - Регенерированные golden-снимки
   - Тестовые доказательства из `pytest -m schema`
4. **Релиз:** Release management тегирует пайплайн с новой версией схемы и координирует окна деплоя

## 9. CLI команды и параметры

### 9.1 Обзор

Assay pipeline использует унифицированный CLI интерфейс через Typer. Все команды запускаются через `python -m bioetl.cli.main assay`. Подробности архитектуры CLI см. в [CLI Overview](../cli/00-cli-overview.md).

**Инвокация:**

```bash
python -m bioetl.cli.main assay [OPTIONS]
```

**Назначение:** Извлечение и нормализация метаданных ассаев из ChEMBL `/assay.json` с детерминированным выводом и полной воспроизводимостью.

### 9.2 Глобальные опции

Все опции доступны для всех pipeline команд. Опции, помеченные как **required**, обязательны; остальные имеют безопасные значения по умолчанию.

| Опция | Короткая форма | Обязательна | Описание | Значение по умолчанию |
|-------|----------------|-------------|----------|----------------------|
| `--config PATH` | | **Да** | Путь к YAML конфигурации пайплайна | — |
| `--output-dir PATH` | `-o` | **Да** | Директория для записи артефактов | — |
| `--input-file PATH` | `-i` | Нет | Опциональный входной файл (CSV с `assay_chembl_id`) | `None` |
| `--dry-run` | `-d` | Нет | Загрузить и валидировать конфигурацию без выполнения | `False` |
| `--limit N` | | Нет | Обработать максимум N строк (для smoke-тестов) | `None` |
| `--sample N` | | Нет | Случайная выборка N строк; использует детерминированный seed | `None` |
| `--golden PATH` | | Нет | Сравнить вывод с golden-файлом для проверки детерминизма | `None` |
| `--mode NAME` | | Нет | Режим выполнения (например, `full`, `minimal`) | `None` |
| `--set KEY=VALUE` | `-S` | Нет | Переопределить значение конфигурации. Повторяемый | `[]` |
| `--fail-on-schema-drift / --allow-schema-drift` | | Нет | Завершить выполнение при отклонении схемы | `--fail-on-schema-drift` |
| `--validate-columns / --no-validate-columns` | | Нет | Контроль валидации колонок в post-processing | `--validate-columns` |
| `--extended / --no-extended` | | Нет | Включить расширенные QC-артефакты | `--no-extended` |
| `--verbose` | `-v` | Нет | Включить детальное логирование (DEBUG уровень) | `False` |

### 9.3 Приоритет конфигурации

CLI загружает конфигурацию в следующем порядке приоритета (от низкого к высокому):

1. **Базовые профили** из `extends` (обычно `configs/profiles/base.yaml`, `configs/profiles/determinism.yaml`)
2. **Pipeline конфиг** из `--config`
3. **CLI переопределения** через `--set`
4. **Переменные окружения** (например, `BIOETL__SOURCES__CHEMBL__BATCH_SIZE=25`)

Переменные окружения имеют наивысший приоритет и переопределяют все остальные источники.

### 9.4 Команда `assay`

**Сигнатура:** `python -m bioetl.cli.main assay [OPTIONS]`

**Обязательные опции:** `--config`, `--output-dir`

**Опциональные опции:** `--dry-run`, `--limit`, `--sample`, `--golden`, `--set`, `--input-file`

**Профили по умолчанию:** `base.yaml` и `determinism.yaml`, опционально `network.yaml` через pipeline конфиг

**Детерминированный вывод:** Сортировка по `assay_chembl_id`, `row_subtype`, `row_index`; SHA256 хеши для `hash_row` и `hash_business_key`; фиксированный порядок колонок из `AssaySchema.Config.column_order` (71 колонка)

### 9.5 Примеры использования

**Базовый запуск:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay
```

**Проверка конфигурации (dry-run):**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --dry-run
```

**Ограничение количества записей (smoke test):**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --limit 100
```

**Случайная выборка с детерминированным seed:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --sample 500 \
  --set determinism.sample_seed=42
```

**Сравнение с golden-файлом:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --golden tests/golden/assay/assay_20250115.csv
```

**Переопределение параметров ChEMBL:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --set sources.chembl.batch_size=20 \
  --set sources.chembl.max_url_length=2000
```

**Строгая валидация схемы:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --fail-on-schema-drift \
  --validate-columns
```

**С входным файлом:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --input-file data/input/assay_ids.csv
```

**Детальное логирование:**

```bash
python -m bioetl.cli.main assay \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --verbose
```

### 9.6 Коды завершения

CLI использует стандартные коды завершения:

- `0`: Успешное выполнение
- `1`: Ошибка валидации конфигурации
- `2`: Ошибка выполнения пайплайна
- `3`: Ошибка валидации схемы (schema drift)
- `4`: Ошибка QC (quality checks failed)

Подробности см. в [CLI Exit Codes](../cli/02-cli-exit_codes.md).

## 10. Критические исправления (To-Do)

### A. Batch size

- [ ] Исправить `configs/pipelines/assay.yaml`: `batch_size: 25`

- [ ] Добавить валидацию в `AssayConfig`: `if batch_size > 25: raise`

- [ ] Обновить документацию с объяснением URL limit

### B. Assay parameters

- [ ] Переписать `_expand_assay_parameters()` на long format

- [ ] Использовать `param_index` для детерминизма

- [ ] Добавить `row_subtype` в DataFrame

### C. Variant sequences

- [ ] Поддержка и объекта, и списка в `_expand_variant_sequence()`

- [ ] Добавить `variant_index` для детерминизма

### D. Enrichment whitelist

- [ ] Явно перечислить поля в `TARGET_ENRICHMENT_WHITELIST`

- [ ] Явно перечислить поля в `ASSAY_CLASS_ENRICHMENT_WHITELIST`

- [ ] Добавить `strict_enrichment: true` в конфиг

- [ ] Обновить Pandera схему с whitelist

### E. Хеширование

- [ ] Реализовать `_canonicalize_row_for_hash()` с JSON/ISO8601

- [ ] Единая NA policy: "" вместо None

- [ ] Float формат: %.6f

### F. Fallback

- [ ] Расширить запись полями: `error_code`, `http_status`, `retry_after_sec`, `attempt`, `run_id`

### G. QC

- [ ] Добавить referential integrity check

- [ ] Отчет о потерях join в quality_report

### H. Корреляции

- [ ] Вынести в отдельный шаг `postprocess.correlation`

- [ ] По умолчанию `enabled: false`

### I. Chembl release

- [ ] Один снимок `/status` в начале

- [ ] Запись в `run_config.yaml`

- [ ] Прокидывание в кэш-ключи

- [ ] Блокировка при смене release

### J. Atomic writes

- [ ] Использовать `os.replace()` вместо `rename()`

- [ ] Temp dir: `output_dir/.tmp/{run_id}/`

- [ ] Документировать Windows behavior

### K. Dtypes

- [ ] Принудительно `pd.StringDtype()`, `pd.Int64Dtype()`, `pd.Float64Dtype()`

- [ ] Никаких `object` dtypes

### L. Метаданные

- [ ] Добавить `run_id`, `git_commit`, `config_hash` в все лог-сообщения

- [ ] Добавить OpenTelemetry tags для стадий

- [ ] Расширить `meta.yaml` полной provenance

### M. Документация

- [ ] Заменить "строки 128-293" на имена методов

- [ ] Добавить тест-кейсы как якоря

### N. Фильтры CLI

- [ ] Формализовать контракт фильтра (pure function DataFrame -> DataFrame)

- [ ] Документировать предикаты

## 11. Логирование и трассировка

### 11.1 Обзор

Assay pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста. Система логирования построена на `structlog` и обеспечивает детерминированный, машиночитаемый вывод в формате JSON. Подробности о системе логирования см. в [Logging Overview](../logging/00-overview.md).

### 11.2 Уровни логирования

Система логирования поддерживает следующие уровни (от низкого к высокому):

- **DEBUG:** Детальная информация для отладки (включается через `--verbose`)
- **INFO:** Общая информация о выполнении пайплайна (по умолчанию)
- **WARNING:** Предупреждения о потенциальных проблемах
- **ERROR:** Ошибки выполнения, требующие внимания

**Настройка уровня:** Уровень логирования настраивается через `LoggerConfig` при инициализации:

```python
from bioetl.core.logger import UnifiedLogger, LoggerConfig

config = LoggerConfig(
    level="INFO",  # или "DEBUG", "WARNING", "ERROR"
    console_format="text",  # "text" для разработки, "json" для продакшена
    file_enabled=True,
    file_path=Path("logs/assay_pipeline.log"),
    file_format="json"  # Всегда JSON для файлов
)

UnifiedLogger.configure(config)
```

### 11.3 Обязательные поля в логах

Каждая запись лога, независимо от уровня или происхождения, гарантированно содержит следующие базовые поля согласно контракту логирования (см. [Structured Events](../logging/02-structured-events-and-context.md)):

| Поле | Тип | Описание |
|------|-----|----------|
| `run_id` | `str` | Уникальный идентификатор запуска пайплайна |
| `stage` | `str` | Текущая стадия выполнения (`bootstrap`, `extract`, `transform`, `validate`, `write`) |
| `actor` | `str` | Сущность, инициировавшая запуск (например, `scheduler`, `<username>`) |
| `source` | `str` | Источник данных (`chembl`) |
| `generated_at` | `str(ISO)` | UTC timestamp в формате ISO 8601 |
| `level` | `str` | Уровень лога (`debug`, `info`, `warning`, `error`) |
| `message` | `str` | Основное, человекочитаемое описание события |

### 11.4 Контекст-специфичные поля

Определенные события, особенно связанные с внешними вызовами (например, API запросы), должны включать дополнительный контекст:

| Поле | Тип | Описание |
|------|-----|----------|
| `endpoint` | `str` | URL или путь API эндпоинта |
| `params` | `dict` | (Условно) Параметры запроса, отправленные с запросом |
| `attempt` | `int` | Номер попытки запроса (например, `1` для первой попытки) |
| `duration_ms` | `int` | Общая длительность вызова в миллисекундах |
| `trace_id` | `str` | (Условно) OpenTelemetry trace ID для этого запроса |

### 11.5 Структурированные события

Assay pipeline логирует следующие структурированные события на разных стадиях выполнения:

#### 11.5.1 Жизненный цикл пайплайна

- **`pipeline_started`:** Начало выполнения пайплайна
- **`pipeline_completed`:** Успешное завершение пайплайна
- **`pipeline_failed`:** Ошибка выполнения с деталями

#### 11.5.2 Стадия извлечения (Extract)

- **`extract_started`:** Начало стадии извлечения
- **`extract_completed`:** Завершение стадии извлечения с метриками
- **`batch_fetch`:** Запрос батча ассаев из ChEMBL API
- **`cache_hit`:** Попадание в кэш
- **`cache_miss`:** Промах кэша
- **`fallback_activated`:** Активация fallback-механизма

#### 11.5.3 Стадия трансформации (Transform)

- **`transform_started`:** Начало стадии трансформации
- **`transform_completed`:** Завершение стадии трансформации
- **`enrichment_started`:** Начало обогащения данными (Target/AssayClass)
- **`enrichment_completed`:** Завершение обогащения с метриками
- **`normalization_started`:** Начало нормализации данных
- **`normalization_completed`:** Завершение нормализации

#### 11.5.4 Стадия валидации (Validate)

- **`validate_started`:** Начало валидации
- **`validate_completed`:** Завершение валидации
- **`schema_validation_passed`:** Успешная валидация схемы
- **`schema_validation_failed`:** Ошибка валидации схемы
- **`referential_integrity_check`:** Проверка референциальной целостности

#### 11.5.5 Стадия записи (Write)

- **`write_started`:** Начало записи результатов
- **`write_completed`:** Завершение записи результатов
- **`atomic_write`:** Атомарная запись файла
- **`metadata_generated`:** Генерация метаданных (meta.yaml)

### 11.6 Примеры JSON-логов

#### 11.6.1 Начало пайплайна

```json
{
  "event": "pipeline_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:30:00.123456Z",
  "level": "info",
  "message": "Assay pipeline started",
  "config_hash": "def456...",
  "pipeline_version": "2.0.0",
  "chembl_release": "CHEMBL_36"
}
```

#### 11.6.2 Запрос к ChEMBL API

```json
{
  "event": "batch_fetch",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:30:15.234567Z",
  "level": "info",
  "message": "Fetching batch of assays from ChEMBL API",
  "endpoint": "https://www.ebi.ac.uk/chembl/api/data/assay.json",
  "params": {
    "assay_chembl_id__in": ["CHEMBL123", "CHEMBL456", "..."]
  },
  "attempt": 1,
  "batch_size": 25,
  "duration_ms": 1234
}
```

#### 11.6.3 Попадание в кэш

```json
{
  "event": "cache_hit",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:30:16.345678Z",
  "level": "info",
  "message": "Cache hit for assay batch",
  "cache_key": "assay:CHEMBL_36:CHEMBL123",
  "cache_ttl": 86400
}
```

#### 11.6.4 Завершение стадии извлечения

```json
{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:30:45.345678Z",
  "level": "info",
  "message": "Extraction stage completed",
  "duration": 45.2,
  "row_count": 1250,
  "api_calls": 50,
  "cache_hits": 1189,
  "cache_misses": 61,
  "success_count": 1200,
  "fallback_count": 24,
  "error_count": 26,
  "success_rate": 0.992
}
```

#### 11.6.5 Обогащение данными

```json
{
  "event": "enrichment_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "transform",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:31:00.456789Z",
  "level": "info",
  "message": "Starting target enrichment",
  "enrichment_type": "target",
  "target_count": 850
}
```

#### 11.6.6 Ошибка валидации схемы

```json
{
  "event": "schema_validation_failed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "validate",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:32:00.567890Z",
  "level": "error",
  "message": "Schema validation failed",
  "schema_version": "2.0.0",
  "error_count": 5,
  "errors": [
    {
      "column": "assay_chembl_id",
      "error": "Value 'INVALID' does not match regex '^CHEMBL\\d+$'"
    }
  ]
}
```

#### 11.6.7 Успешное завершение пайплайна

```json
{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:32:00.678901Z",
  "level": "info",
  "message": "Assay pipeline completed successfully",
  "duration": 120.5,
  "row_count": 1250,
  "output_files": {
    "csv": "assay_20250115.csv",
    "qc": "assay_20250115_qc.csv",
    "quality_report": "assay_20250115_quality_report.csv",
    "meta": "assay_20250115_meta.yaml"
  },
  "checksums": {
    "csv": "sha256:abc123...",
    "meta": "sha256:def456..."
  }
}
```

### 11.7 Форматы вывода

Система логирования поддерживает два формата вывода:

#### 11.7.1 Консольный формат

- **Формат:** `"text"` (по умолчанию для разработки)
- **Рендерер:** `structlog.dev.KeyValueRenderer`
- **Особенности:** Цветной, удобочитаемый формат `key=value` пар
- **Использование:** Локальная разработка (не подходит для продакшена)

**Пример консольного вывода:**

```text
2025-01-15T10:30:00.123Z [info     ] Extraction complete.         run_id=a1b2c3d4e5f6g7h8 stage=extract rows=5000
```

#### 11.7.2 JSON формат

- **Формат:** `"json"` (для файлов и продакшена)
- **Рендерер:** `structlog.processors.JSONRenderer`
- **Особенности:** Один минифицированный JSON объект на запись лога
- **Использование:** Контейнеризованные среды (Docker, Kubernetes), файлы логов

**Пример JSON вывода (форматирован для читаемости):**

```json
{
  "generated_at": "2025-01-15T10:30:00.123456Z",
  "level": "info",
  "message": "Extraction complete.",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "rows": 5000
}
```

### 11.8 Ротация логов

Система логирования включает автоматическую ротацию лог-файлов:

- **Максимальный размер файла:** 10 MB (по умолчанию)
- **Количество резервных копий:** 10 файлов (по умолчанию)
- **Именование:** `assay_pipeline.log`, `assay_pipeline.log.1`, `assay_pipeline.log.2`, ...

### 11.9 Трассировка и контекст

Все операции связаны через `run_id` для отслеживания полного жизненного цикла пайплайна:

- **Глобальный контекст:** `run_id`, `stage`, `actor`, `source` устанавливаются один раз в начале выполнения через `set_run_context()`
- **Автоматическое обогащение:** Все последующие логи автоматически обогащаются контекстом через `ContextVar`
- **OpenTelemetry интеграция:** При включенной телеметрии логи автоматически обогащаются `trace_id` и `span_id`

### 11.10 Обработка ошибок

Ошибки логируются с полным контекстом и stack trace:

```json
{
  "event": "pipeline_failed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "assay",
  "actor": "user",
  "source": "chembl",
  "generated_at": "2025-01-15T10:30:30.789012Z",
  "level": "error",
  "message": "Pipeline failed with error",
  "error_type": "HTTPError",
  "error_message": "Connection timeout after 30 seconds",
  "stack_trace": "...",
  "endpoint": "https://www.ebi.ac.uk/chembl/api/data/assay.json",
  "attempt": 5
}
```

### 11.11 Конфигурация логирования

Подробности о настройке и использовании системы логирования см. в:

- [Logging Overview](../logging/00-overview.md) - обзор системы логирования
- [Public API and Configuration](../logging/01-public-api-and-configuration.md) - API и конфигурация
- [Structured Events and Context](../logging/02-structured-events-and-context.md) - структурированные события и контекст
- [Output Formats and Determinism](../logging/03-output-formats-and-determinism.md) - форматы вывода и детерминизм

## Заключение

Данная спецификация исправляет критические недостатки в текущей реализации:

1. **Размер батча:** Жестко 25, с валидацией конфига

2. **Потеря данных:** Long format для nested structures вместо "первого элемента"

3. **Enrichment whitelist:** Только разрешенные поля, строгая валидация

4. **Хеширование:** Каноническая сериализация с детерминизмом

5. **Метаданные:** Полная provenance с run_id/git_commit/config_hash

6. **QC:** Referential integrity checks и отчеты о потерях

7. **Корреляции:** Опциональный шаг, не часть ETL

8. **Atomic writes:** Run-scoped temp dirs, os.replace() для Windows compatibility

Все изменения направлены на обеспечение **детерминизма**, **воспроизводимости** и **полной прослеживаемости** данных.
