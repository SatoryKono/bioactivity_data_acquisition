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
└── Load Stage
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

**Базовый стандарт:** см. `docs/requirements/10-configuration.md` (§2–§6).

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

**См. также**: [gaps.md](../gaps.md) (G6, G7, G8), [acceptance-criteria.md](../acceptance-criteria.md) (AC7, AC8), [implementation-examples.md](../implementation-examples.md) (патч 2).

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

## 5. Запись результатов (Load)

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

## 7. CLI Дополнения

**Унифицированный интерфейс**: Все пайплайны используют единую команду `bioetl pipeline run`. См. стандарт в [10-configuration.md](10-configuration.md#53-cli-interface-specification-aud-4).

```bash

# Golden compare для детерминизма

bioetl pipeline run --config configs/pipelines/assay.yaml \
  --golden golden_assay.csv

# Sample с ограничением

bioetl pipeline run --config configs/pipelines/assay.yaml \
  --sample 100

# Контроль API параметров

bioetl pipeline run --config configs/pipelines/assay.yaml \
  --set sources.chembl.max_url_length=2000 \
  --set sources.chembl.batch_size=25

# Strict validation

bioetl pipeline run --config configs/pipelines/assay.yaml \
  --fail-on-schema-drift \
  --set qc.severity_threshold=error

```

**Новые CLI параметры:**

- `--golden PATH`: Путь к golden файлу для сравнения (бит-в-бит проверка)

- `--sample N`: Обработать только N случайных записей

- `--sample-seed INT`: Seed для воспроизводимости sample

- `--max-url-length INT`: Макс. длина URL (по умолчанию 2000)

- `--page-size INT`: Размер страницы для пагинации

- `--strict-enrichment`: Запретить неразрешенные поля из enrichment

- `--fail-on-schema-drift`: Падение при изменениях схемы/column_order

## 8. Критические исправления (To-Do)

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
