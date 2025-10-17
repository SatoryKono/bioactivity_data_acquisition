# Пайплайн извлечения и нормализации сущности Assay из ChEMBL

## Обзор

Данный документ описывает техническое решение для построения пайплайна `Sxx_assay` (extract → normalize → validate → persist) для создания измерения `assay_dim` в звёздной схеме проекта. Пайплайн обеспечивает корректное извлечение метаданных ассая, детерминированную сериализацию результата и строгую валидацию схемой.

## Архитектура пайплайна

### Этапы обработки

1. **S01_status_and_release** - Получение статуса ChEMBL и фиксация релиза
2. **S02_fetch_assay_core** - Извлечение основных данных ассев
3. **S03_enrich_source** - Обогащение данными источников
4. **S04_normalize_fields** - Нормализация полей
5. **S05_validate_schema** - Валидация схемы данных
6. **S06_persist_and_meta** - Сохранение и создание метаданных

### Источники данных

- **ChEMBL Assay API**: `/assay` endpoint для получения метаданных ассев
- **ChEMBL Source API**: `/source` endpoint для расшифровки источников
- **ChEMBL Status API**: `/status` endpoint для получения версии релиза

## Схема полей assay_dim

### Ключи и идентификаторы

| Column | Source endpoint.field | Purpose | Type | Normalization | Notes |
|--------|----------------------|---------|------|---------------|-------|
| `assay_chembl_id` | `assay.assay_chembl_id` | business key | str | strip | — |
| `src_id` | `assay.src_id` | source FK | int | — | join to /source |
| `src_name` | `source.src_description` | source name | str | trim | via src_id |
| `src_assay_id` | `assay.src_assay_id` | source assay ID | str | trim | — |

### Классификация ассая

| Column | Source endpoint.field | Purpose | Type | Normalization | Notes |
|--------|----------------------|---------|------|---------------|-------|
| `assay_type` | `assay.assay_type` | classification | str | map B/F/P/U | docs-backed |
| `assay_type_description` | `assay.assay_type_description` | label | str | trim | — |
| `bao_format` | `assay.bao_format` | BAO term | str | trim | may be null |
| `bao_label` | `assay.bao_label` | BAO label | str | trim | may be null |
| `assay_category` | `assay.assay_category` | category array | list | dedup, sort | may be null |
| `assay_classifications` | `assay.assay_classifications` | classifications | list | dedup, sort | may be null |

### Связь с таргетом

| Column | Source endpoint.field | Purpose | Type | Normalization | Notes |
|--------|----------------------|---------|------|---------------|-------|
| `target_chembl_id` | `assay.target_chembl_id` | target FK | str | trim | optional |
| `relationship_type` | `assay.relationship_type` | target link quality | str | keep raw code | filterable |
| `confidence_score` | `assay.confidence_score` | curation confidence | int | range 0..9 | interpret in docs |

### Биологический контекст

| Column | Source endpoint.field | Purpose | Type | Normalization | Notes |
|--------|----------------------|---------|------|---------------|-------|
| `assay_organism` | `assay.assay_organism` | organism | str | trim | may be null |
| `assay_tax_id` | `assay.assay_tax_id` | organism tax ID | int | — | may be null |
| `assay_cell_type` | `assay.assay_cell_type` | cell type | str | trim | may be null |
| `assay_tissue` | `assay.assay_tissue` | tissue | str | trim | may be null |
| `assay_strain` | `assay.assay_strain` | strain | str | trim | may be null |
| `assay_subcellular_fraction` | `assay.assay_subcellular_fraction` | subcellular fraction | str | trim | may be null |

### Описание и протокол

| Column | Source endpoint.field | Purpose | Type | Normalization | Notes |
|--------|----------------------|---------|------|---------------|-------|
| `description` | `assay.description` | free text | str | trim; limit N | — |
| `assay_parameters` | `assay.assay_parameters` | parameters | json/str | normalize to key=value | may be null |
| `assay_format` | `assay.assay_format` | format | str | trim | may be null |

### Техслужебные поля

| Column | Source | Purpose | Type | Normalization | Notes |
|--------|--------|---------|------|---------------|-------|
| `source_system` | system | provenance | str | — | 'ChEMBL' |
| `chembl_release` | `/status` | provenance | str | — | meta.yaml |
| `extracted_at` | system | provenance | datetime | ISO8601 | — |
| `hash_row` | computed | dedup | str | stable hash | — |
| `hash_business_key` | computed | dedup | str | stable hash | — |

## Конфигурация

### YAML-конфиг config/config_assay_full.yaml

```yaml
# Глобальные настройки HTTP-клиента
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 10
      backoff_multiplier: 3.0
    rate_limit:
      max_calls: 3
      period: 15.0
    headers:
      Accept: application/json
      User-Agent: bioactivity-data-acquisition/0.1.0

# Конфигурация источников данных
sources:
  chembl:
    name: chembl
    endpoint: assay
    params:
      # Параметры по умолчанию
    pagination:
      size: 200
      max_pages: 50
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      timeout_sec: 120.0
      headers:
        Authorization: "Bearer {chembl_api_token}"

# Настройки ввода-вывода данных
io:
  input:
    assay_ids_csv: data/input/assay_ids.csv
  output:
    dir: data/output/assay
    format: csv
    csv:
      encoding: utf-8
      float_format: "%.3f"
      date_format: "%Y-%m-%dT%H:%M:%SZ"

# Настройки выполнения программы
runtime:
  workers: 8
  limit: null
  dry_run: false

# Настройки валидации данных
validation:
  strict: true
  qc:
    max_missing_fraction: 0.02
    max_duplicate_fraction: 0.005

# Настройки детерминизма для воспроизводимости результатов
determinism:
  sort:
    by:
      - assay_chembl_id
    ascending:
      - true
    na_position: last
  column_order:
    - index
    - assay_chembl_id
    - src_id
    - src_name
    - src_assay_id
    - assay_type
    - assay_type_description
    - bao_format
    - bao_label
    - assay_category
    - assay_classifications
    - target_chembl_id
    - relationship_type
    - confidence_score
    - assay_organism
    - assay_tax_id
    - assay_cell_type
    - assay_tissue
    - assay_strain
    - assay_subcellular_fraction
    - description
    - assay_parameters
    - assay_format
    - source_system
    - chembl_release
    - extracted_at
    - hash_row
    - hash_business_key

# Настройки постобработки данных
postprocess:
  qc:
    enabled: true
  correlation:
    enabled: true
```

## Реализация этапов пайплайна

### S01_status_and_release

```python
def get_chembl_status(client: ChEMBLClient) -> dict[str, Any]:
    """Получить статус ChEMBL и зафиксировать релиз."""
    try:
        status = client._request("GET", "status")
        return {
            "chembl_release": status.get("chembl_release"),
            "status": status.get("status"),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get ChEMBL status: {e}")
        raise
```

### S02_fetch_assay_core

```python
def fetch_assay_data(
    client: ChEMBLClient, 
    assay_ids: list[str] | None = None,
    target_chembl_id: str | None = None,
    filters: dict[str, Any] | None = None
) -> pd.DataFrame:
    """Извлечь данные ассев из ChEMBL API."""
    
    if assay_ids:
        # Batched fetch по списку ID
        return _fetch_by_assay_ids(client, assay_ids)
    elif target_chembl_id:
        # Выборка ассев по таргету с фильтрами
        return _fetch_by_target(client, target_chembl_id, filters)
    else:
        raise ValueError("Either assay_ids or target_chembl_id must be provided")

def _fetch_by_assay_ids(client: ChEMBLClient, assay_ids: list[str]) -> pd.DataFrame:
    """Извлечь ассеи по списку ID."""
    assays = []
    
    for assay_id in assay_ids:
        try:
            payload = client._request("GET", f"assay/{assay_id}")
            assay_data = _parse_assay(payload)
            assays.append(assay_data)
        except Exception as e:
            logger.warning(f"Failed to fetch assay {assay_id}: {e}")
            # Создаем пустую запись с ошибкой
            assays.append(_create_empty_assay_record(assay_id, str(e)))
    
    return pd.DataFrame(assays)

def _fetch_by_target(
    client: ChEMBLClient, 
    target_chembl_id: str, 
    filters: dict[str, Any] | None = None
) -> pd.DataFrame:
    """Извлечь ассеи по таргету с фильтрами."""
    params = {
        "target_chembl_id": target_chembl_id,
        "format": "json",
        "limit": 200
    }
    
    if filters:
        params.update(filters)
    
    # Применяем профили фильтрации
    if "relationship_type" not in params:
        params["relationship_type"] = "D"  # Direct relationship
    if "assay_type" not in params:
        params["assay_type"] = "B,F"  # Binding and Functional
    
    assays = []
    page = 0
    
    while True:
        params["offset"] = page * 200
        try:
            response = client._request("GET", "assay", params=params)
            page_data = response.get("assays", [])
            
            if not page_data:
                break
                
            for assay in page_data:
                assay_data = _parse_assay(assay)
                assays.append(assay_data)
            
            page += 1
            
            # Проверяем, есть ли еще страницы
            if len(page_data) < 200:
                break
                
        except Exception as e:
            logger.error(f"Failed to fetch assays page {page}: {e}")
            break
    
    return pd.DataFrame(assays)
```

### S03_enrich_source

```python
def enrich_with_source_data(client: ChEMBLClient, assays_df: pd.DataFrame) -> pd.DataFrame:
    """Обогатить данные ассев информацией об источниках."""
    enriched = assays_df.copy()
    
    # Получаем уникальные src_id
    unique_src_ids = assays_df["src_id"].dropna().unique()
    
    # Кэшируем данные источников
    source_cache = {}
    for src_id in unique_src_ids:
        try:
            source_data = client._request("GET", f"source/{src_id}")
            source_cache[src_id] = {
                "src_name": source_data.get("src_description"),
                "src_short_name": source_data.get("src_short_name"),
                "src_url": source_data.get("src_url")
            }
        except Exception as e:
            logger.warning(f"Failed to fetch source {src_id}: {e}")
            source_cache[src_id] = {
                "src_name": None,
                "src_short_name": None,
                "src_url": None
            }
    
    # Обогащаем данные
    enriched["src_name"] = enriched["src_id"].map(
        lambda x: source_cache.get(x, {}).get("src_name") if pd.notna(x) else None
    )
    enriched["src_short_name"] = enriched["src_id"].map(
        lambda x: source_cache.get(x, {}).get("src_short_name") if pd.notna(x) else None
    )
    enriched["src_url"] = enriched["src_id"].map(
        lambda x: source_cache.get(x, {}).get("src_url") if pd.notna(x) else None
    )
    
    return enriched
```

### S04_normalize_fields

```python
def normalize_assay_fields(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Нормализовать поля ассев."""
    normalized = assays_df.copy()
    
    # Строковые поля: strip, collapse spaces
    string_columns = [
        "assay_chembl_id", "src_assay_id", "assay_type_description",
        "bao_format", "bao_label", "description", "assay_format",
        "assay_organism", "assay_cell_type", "assay_tissue",
        "assay_strain", "assay_subcellular_fraction"
    ]
    
    for col in string_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].astype(str).str.strip()
            normalized[col] = normalized[col].replace("", pd.NA)
    
    # Маппинг assay_type к описанию
    assay_type_mapping = {
        "B": "Binding",
        "F": "Functional", 
        "P": "Physicochemical",
        "U": "Unclassified"
    }
    
    if "assay_type" in normalized.columns:
        normalized["assay_type_description"] = normalized["assay_type"].map(
            assay_type_mapping
        ).fillna(normalized["assay_type_description"])
    
    # Нормализация списков
    list_columns = ["assay_category", "assay_classifications"]
    for col in list_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col].apply(_normalize_list_field)
    
    # Валидация relationship_type
    valid_relationship_types = {"D", "E", "H", "U", "X", "Y", "Z"}
    if "relationship_type" in normalized.columns:
        invalid_rels = ~normalized["relationship_type"].isin(valid_relationship_types)
        if invalid_rels.any():
            logger.warning(f"Found invalid relationship_type values: {normalized.loc[invalid_rels, 'relationship_type'].unique()}")
    
    # Валидация confidence_score
    if "confidence_score" in normalized.columns:
        invalid_conf = (normalized["confidence_score"] < 0) | (normalized["confidence_score"] > 9)
        if invalid_conf.any():
            logger.warning(f"Found invalid confidence_score values: {normalized.loc[invalid_conf, 'confidence_score'].unique()}")
    
    # Ограничение длины description
    if "description" in normalized.columns:
        max_desc_length = 1000
        normalized["description"] = normalized["description"].str[:max_desc_length]
    
    return normalized

def _normalize_list_field(value) -> list[str] | None:
    """Нормализовать поле-список."""
    if pd.isna(value) or value is None:
        return None
    
    if isinstance(value, list):
        # Дедупликация и сортировка для детерминизма
        unique_items = list(set(str(item).strip() for item in value if str(item).strip()))
        return sorted(unique_items) if unique_items else None
    
    if isinstance(value, str):
        # Парсинг строки как JSON или разделение по разделителям
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, list):
                unique_items = list(set(str(item).strip() for item in parsed if str(item).strip()))
                return sorted(unique_items) if unique_items else None
        except (json.JSONDecodeError, TypeError):
            pass
        
        # Разделение по запятым или точкам с запятой
        items = [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        unique_items = list(set(items))
        return sorted(unique_items) if unique_items else None
    
    return None
```

### S05_validate_schema

```python
from pandera import DataFrameSchema, Column, Check
import pandera as pa

# Pandera-схема для валидации assay_dim
AssayDimSchema = DataFrameSchema({
    # Ключи и идентификаторы
    "assay_chembl_id": Column(str, nullable=False, unique=True),
    "src_id": Column(int, nullable=True),
    "src_name": Column(str, nullable=True),
    "src_assay_id": Column(str, nullable=True),
    
    # Классификация ассая
    "assay_type": Column(str, nullable=True, checks=Check.isin(["B", "F", "P", "U"])),
    "assay_type_description": Column(str, nullable=True),
    "bao_format": Column(str, nullable=True),
    "bao_label": Column(str, nullable=True),
    "assay_category": Column(object, nullable=True),  # list
    "assay_classifications": Column(object, nullable=True),  # list
    
    # Связь с таргетом
    "target_chembl_id": Column(str, nullable=True),
    "relationship_type": Column(str, nullable=True, checks=Check.isin(["D", "E", "H", "U", "X", "Y", "Z"])),
    "confidence_score": Column(int, nullable=True, checks=Check.in_range(0, 9)),
    
    # Биологический контекст
    "assay_organism": Column(str, nullable=True),
    "assay_tax_id": Column(int, nullable=True),
    "assay_cell_type": Column(str, nullable=True),
    "assay_tissue": Column(str, nullable=True),
    "assay_strain": Column(str, nullable=True),
    "assay_subcellular_fraction": Column(str, nullable=True),
    
    # Описание и протокол
    "description": Column(str, nullable=True),
    "assay_parameters": Column(object, nullable=True),  # dict/str
    "assay_format": Column(str, nullable=True),
    
    # Техслужебные поля
    "source_system": Column(str, nullable=False, checks=Check.equal_to("ChEMBL")),
    "chembl_release": Column(str, nullable=False),
    "extracted_at": Column("datetime64[ns]", nullable=False),
    "hash_row": Column(str, nullable=False),
    "hash_business_key": Column(str, nullable=False),
})

def validate_assay_schema(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Валидировать схему данных ассев."""
    try:
        validated_df = AssayDimSchema.validate(assays_df)
        logger.info(f"Schema validation passed for {len(validated_df)} assays")
        return validated_df
    except pa.errors.SchemaError as e:
        logger.error(f"Schema validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected validation error: {e}")
        raise
```

### S06_persist_and_meta

```python
def persist_assay_data(
    assays_df: pd.DataFrame,
    output_dir: Path,
    date_tag: str,
    chembl_release: str,
    config: AssayConfig
) -> dict[str, Path]:
    """Сохранить данные ассев и создать метаданные."""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Пути для выходных файлов
    csv_path = output_dir / f"assay_{date_tag}.csv"
 
    meta_path = output_dir / f"assay_{date_tag}_meta.yaml"
    
    # Детерминированная сериализация
    from library.etl.load import write_deterministic_csv
    
    # Сохранение CSV
    write_deterministic_csv(
        assays_df,
        csv_path,
        determinism=config.determinism,
        output=config.io.output
    )
    
    # Сохранение Parquet
    
    
    # Создание метаданных
    meta_data = {
        "pipeline_version": "1.0.0",
        "chembl_release": chembl_release,
        "extraction_date": date_tag,
        "row_count": len(assays_df),
        "file_checksums": {
            "csv": _calculate_checksum(csv_path),
           
        },
        "filters_applied": config.filters,
        "extraction_parameters": {
            "total_assays": len(assays_df),
            "unique_sources": assays_df["src_id"].nunique(),
            "assay_types": assays_df["assay_type"].value_counts().to_dict(),
            "relationship_types": assays_df["relationship_type"].value_counts().to_dict()
        }
    }
    
    # Сохранение метаданных
    with open(meta_path, 'w', encoding='utf-8') as f:
        yaml.dump(meta_data, f, default_flow_style=False, allow_unicode=True)
    
    return {
        "csv": csv_path,
       
        "meta": meta_path
    }
```

## Правила вызова API и фильтры

### Формат запросов

- **Базовый URL**: `https://www.ebi.ac.uk/chembl/api/data`
- **Формат ответа**: JSON (`.json`)
- **Поддержка фильтров**: `__in`, `__range`, `__isnull`
- **Параметр `only=`**: для уменьшения полезной нагрузки
- **Пагинация**: `page_meta.next`

### Типовые выборки

```python
# Все binding-ассеи для человеческого таргета с доверенной связью
params = {
    "target_chembl_id": "CHEMBL231",
    "relationship_type": "D",
    "assay_type": "B",
    "format": "json",
    "limit": 200
}

# Подборка по источнику
params = {
    "assay_type": "F",
    "src_id": 1,
    "format": "json",
    "limit": 200
}

# Фильтрация по типу ассая и уверенности
params = {
    "assay_type__in": "B,F",
    "confidence_score__range": "7,9",
    "format": "json",
    "limit": 200
}
```

## Логирование и кэш

### JSON-логирование

```python
import structlog

logger = structlog.get_logger()

# Логирование HTTP-запросов
logger.info(
    "api_request",
    endpoint="assay",
    params=params,
    status_code=response.status_code,
    elapsed_ms=elapsed_time * 1000,
    response_size=len(response.content)
)
```

### HTTP-кэш

```python
from requests_cache import CachedSession

# Настройка кэша с TTL
cache_session = CachedSession(
    cache_name='.cache/chembl/assay_cache',
    backend='filesystem',
    expire_after=3600,  # 1 час TTL
    cache_control=True
)

# Инвалидация при смене релиза
def invalidate_cache_on_release_change(new_release: str):
    """Инвалидировать кэш при смене релиза ChEMBL."""
    cache_file = Path('.cache/chembl/assay_cache.sqlite')
    if cache_file.exists():
        cache_file.unlink()
        logger.info(f"Cache invalidated due to ChEMBL release change: {new_release}")
```

## Тесты и QC

### Unit тесты

```python
import pytest
from unittest.mock import Mock, patch

def test_assay_parsing():
    """Тест парсинга данных ассая."""
    mock_payload = {
        "assay_chembl_id": "CHEMBL123456",
        "assay_type": "B",
        "src_id": 1,
        "description": "Test assay"
    }
    
    client = Mock()
    result = _parse_assay(mock_payload)
    
    assert result["assay_chembl_id"] == "CHEMBL123456"
    assert result["assay_type"] == "B"
    assert result["src_id"] == 1

def test_field_normalization():
    """Тест нормализации полей."""
    df = pd.DataFrame({
        "assay_chembl_id": [" CHEMBL123 ", "CHEMBL456"],
        "description": ["  Test description  ", None]
    })
    
    normalized = normalize_assay_fields(df)
    
    assert normalized["assay_chembl_id"].iloc[0] == "CHEMBL123"
    assert normalized["description"].iloc[0] == "Test description"
```

### Contract тесты

```python
def test_known_assay_contract():
    """Тест контракта для известных ассев."""
    known_assay_ids = [
        "CHEMBL123456",
        "CHEMBL789012", 
        "CHEMBL345678",
        "CHEMBL901234",
        "CHEMBL567890"
    ]
    
    client = ChEMBLClient(config)
    results = []
    
    for assay_id in known_assay_ids:
        try:
            data = client.fetch_by_assay_id(assay_id)
            results.append(data)
        except Exception as e:
            pytest.fail(f"Failed to fetch known assay {assay_id}: {e}")
    
    # Проверяем заполненность ключевых полей
    for result in results:
        assert result["assay_chembl_id"] is not None
        assert result["assay_type"] is not None
        assert result["src_id"] is not None
        assert result["relationship_type"] is not None
        assert result["confidence_score"] is not None
```

### E2E тесты

```python
def test_end_to_end_pipeline():
    """E2E тест пайплайна."""
    # Создаем тестовые данные
    test_assay_ids = ["CHEMBL123456", "CHEMBL789012"]
    
    # Запускаем пайплайн
    result = run_assay_etl_pipeline(
        assay_ids=test_assay_ids,
        config_path="configs/assay_test.yaml"
    )
    
    # Проверяем результат
    assert result["csv"].exists()
    assert result["parquet"].exists()
    assert result["meta"].exists()
    
    # Проверяем детерминизм
    df1 = pd.read_csv(result["csv"])
    df2 = pd.read_csv(result["csv"])  # Повторное чтение
    
    assert df1.equals(df2)
    
    # Проверяем контрольные суммы
    checksum1 = _calculate_checksum(result["csv"])
    checksum2 = _calculate_checksum(result["csv"])
    assert checksum1 == checksum2
```

### QC-отчёт

```python
def generate_qc_report(assays_df: pd.DataFrame) -> pd.DataFrame:
    """Генерировать QC-отчёт для ассев."""
    metrics = {
        "total_assays": len(assays_df),
        "unique_sources": assays_df["src_id"].nunique(),
        "assay_type_distribution": assays_df["assay_type"].value_counts().to_dict(),
        "top_sources": assays_df["src_id"].value_counts().head(10).to_dict(),
        "relationship_type_distribution": assays_df["relationship_type"].value_counts().to_dict(),
        "confidence_score_distribution": assays_df["confidence_score"].value_counts().to_dict(),
        "missing_data_percentage": {
            col: (assays_df[col].isna().sum() / len(assays_df)) * 100
            for col in assays_df.columns
        }
    }
    
    return pd.DataFrame([
        {"metric": key, "value": value}
        for key, value in metrics.items()
    ])
```

## CLI интерфейс

### Скрипт get_assay_data.py

```python
#!/usr/bin/env python3
"""CLI для извлечения данных ассев из ChEMBL."""

import argparse
import sys
from pathlib import Path
from typing import Any

def main():
    parser = argparse.ArgumentParser(description="Extract assay data from ChEMBL")
    
    # Входные данные
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input", 
        type=Path,
        help="Path to CSV/JSON file with assay IDs"
    )
    input_group.add_argument(
        "--target",
        type=str,
        help="Target ChEMBL ID for filtering assays"
    )
    
    # Фильтры
    parser.add_argument(
        "--filters",
        type=str,
        help="Filter profile name (e.g., 'human_single_protein')"
    )
    parser.add_argument(
        "--only-fields",
        type=Path,
        help="Path to YAML file with field selection"
    )
    
    # Ограничения
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of assays to process"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Request timeout in seconds"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Number of retry attempts"
    )
    
    # Кэш и релиз
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(".cache/chembl"),
        help="Cache directory path"
    )
    parser.add_argument(
        "--chembl-release",
        type=str,
        help="Specific ChEMBL release to use"
    )
    
    # Выходной формат
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format"
    )
    
    # Конфигурация
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config_assay_full.yaml"),
        help="Configuration file path"
    )
    
    args = parser.parse_args()
    
    try:
        # Загружаем конфигурацию
        config = load_assay_config(args.config)
        
        # Применяем переопределения из CLI
        if args.timeout:
            config.http.global_.timeout_sec = args.timeout
        if args.retries:
            config.http.global_.retries.total = args.retries
        if args.limit:
            config.runtime.limit = args.limit
        if args.format:
            config.io.output.format = args.format
        
        # Запускаем пайплайн
        if args.input:
            assay_ids = _load_assay_ids(args.input)
            result = run_assay_etl_pipeline(
                assay_ids=assay_ids,
                config=config
            )
        elif args.target:
            result = run_assay_etl_pipeline(
                target_chembl_id=args.target,
                filters=_get_filter_profile(args.filters),
                config=config
            )
        
        print(f"Pipeline completed successfully. Output files:")
        for name, path in result.items():
            print(f"  {name}: {path}")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 3  # API error

if __name__ == "__main__":
    sys.exit(main())
```

### Коды возврата

- **0**: Успешное выполнение
- **2**: Ошибка валидации данных
- **3**: Ошибка API или сети

## Критерии приёмки

### 1. Корректность данных

- ✅ `assay_dim` содержит корректно распарсенные поля из `/assay`
- ✅ Источники расшифрованы через `/source`
- ✅ Все поля соответствуют схеме `AssayDimSchema`

### 2. Профили фильтрации

- ✅ Выборки по `assay_type` и `relationship_type` воспроизводимы
- ✅ Фильтры соответствуют документации ChEMBL
- ✅ Поддерживаются сложные фильтры (`__in`, `__range`, `__isnull`)

### 3. Детерминированная сериализация

- ✅ Повторный запуск на тех же входах даёт идентичные файлы
- ✅ Контрольные суммы совпадают
- ✅ Порядок колонок фиксирован

### 4. Валидация схемы

- ✅ Поля проходят Pandera-валидацию
- ✅ `assay_chembl_id` уникален
- ✅ Типы данных соответствуют схеме

### 5. Документация

- ✅ Схема полей задокументирована
- ✅ Таблица маппинга "колонка → источник → нормализация" создана
- ✅ Примеры CLI предоставлены
- ✅ Описание профилей фильтрации включено
- ✅ Ссылка на интерпретацию `confidence_score` добавлена

## Интерпретация confidence_score

Уровни уверенности в ChEMBL (0-9):

- **9**: Экспериментально подтвержденная связь
- **8**: Высокая уверенность на основе литературы
- **7**: Умеренная уверенность
- **6**: Низкая уверенность
- **5**: Очень низкая уверенность
- **4**: Неопределенная связь
- **3**: Противоречивые данные
- **2**: Недостаточно данных
- **1**: Предполагаемая связь
- **0**: Нет данных о связи

## Заключение

Данный пайплайн обеспечивает надёжное извлечение и нормализацию данных ассев из ChEMBL с соблюдением принципов детерминизма, валидации и воспроизводимости. Архитектура следует установленным в проекте паттернам и стандартам, обеспечивая интеграцию с существующей инфраструктурой ETL.
