# План тестирования
Покрытие тестами для validation правок из [gaps.md](gaps.md) и [acceptance-criteria.md](acceptance-criteria.md).

## Категории тестов
### 1. Юнит-тесты
**Назначение**: Проверка изолированных компонентов (функций, классов).

#### 1.1 Нормализаторы
```python

# tests/unit/test_normalizers.py

def test_canonicalize_row_for_hash():
    """AC3: hash_row стабилен."""
    from library.common.hash_utils import canonicalize_row_for_hash

    row1 = {"a": 1.23456789, "b": datetime(2024, 1, 1), "c": None}
    row2 = {"c": None, "a": 1.23456789, "b": datetime(2024, 1, 1)}

    assert canonicalize_row_for_hash(row1) == canonicalize_row_for_hash(row2)

    # Float precision

    row = {"value": 3.141592653589793238}
    expected = '{"value":3.141593}'
    assert canonicalize_row_for_hash(row) == expected

def test_precision_map():
    """AC2: precision_map инвариант."""
    from library.schemas.precision_map import get_precision

    assert get_precision("standard_value") == 2
    assert get_precision("activity_id") is None  # string

```
**Покрытие**: AC3, G5

#### 1.2 Schema drift detection
```python

# tests/unit/test_schema_drift.py

def test_schema_drift_major_incompatible():
    """AC10: Fail-fast на несовместимой major версии."""
    from library.schemas.registry import check_schema_compatibility

    schema_v1 = ActivitySchema(version="1.0.0")
    schema_v2 = ActivitySchema(version="2.0.0")  # major bump

    with pytest.raises(SchemaDriftError):
        check_schema_compatibility(schema_v1, schema_v2)

def test_schema_drift_minor_compatible():
    """Minor версии совместимы."""
    schema_v1 = ActivitySchema(version="1.0.0")
    schema_v1_1 = ActivitySchema(version="1.1.0")

    assert check_schema_compatibility(schema_v1, schema_v1_1) is None

```
**Покрытие**: AC10, G4

#### 1.3 Column order validation
```python

# tests/unit/test_column_order.py

def test_column_order_from_schema():
    """AC2: column_order из схемы."""
    schema = ActivitySchema()
    df = pd.DataFrame(columns=schema.column_order)

    df_validated = schema.validate(df)
    assert list(df_validated.columns) == schema.column_order

```
**Покрытие**: AC2, G4

### 2. Интеграционные тесты
**Назначение**: Проверка взаимодействия компонентов.

#### 2.1 Offset-only pagination
```python

# tests/integration/test_pagination.py

def test_activity_offset_only():
    """G2: Только offset-пагинация, запрет смешивания."""
    from library.clients.chembl import ChEMBLClient

    config = APIConfig(
        name="chembl",
        pagination_strategy="offset",  # только offset

        max_limit=5000
    )

    client = ChEMBLClient(config)

    # Должен пройти

    client.fetch_activities(offset=0, limit=100)

    # Должен fail

    with pytest.raises(ValueError, match="Cannot mix pagination strategies"):
        client.fetch_activities(offset=0, cursor="abc123")  # смешивание

```
**Покрытие**: G2

#### 2.2 Mock 429 Retry-After
```python

# tests/integration/test_retry_after.py

def test_respect_retry_after(mocker):
    """AC5: Respect Retry-After."""
    from library.clients.unified_client import UnifiedAPIClient

    import time

    mock_response = Mock(status_code=429, headers={'Retry-After': '2'})
    mocker.patch('requests.get', return_value=mock_response)

    client = UnifiedAPIClient(config)

    start = time.time()
    with pytest.raises(RateLimitError):
        client.request("https://example.com/api/test")
    elapsed = time.time() - start

    assert elapsed >= 2.0, f"Did not wait Retry-After, elapsed={elapsed}"

def test_no_retry_on_4xx_except_429():
    """G11: Не ретраить на 4xx (кроме 429)."""
    from library.clients.unified_client import RateLimitError

    client = UnifiedAPIClient(config)

    # 400 — no retry

    mock_400 = Mock(status_code=400)
    mocker.patch('requests.get', return_value=mock_400)

    with pytest.raises(ValueError):
        client.request("https://example.com/api/test")

    # Не должно быть ретраев

    assert requests.get.call_count == 1

```
**Покрытие**: AC5, G11

#### 2.3 POST+X-HTTP-Method-Override
```python

# tests/integration/test_chembl_post.py

def test_chembl_post_override():
    """ChEMBL POST с X-HTTP-Method-Override для длинных списков."""
    client = ChEMBLClient(config)

    # Большой список assay_chembl_ids (>100)

    assay_ids = [f"CHEMBL{i}" for i in range(150)]

    response = client.fetch_activities(assay_chembl_ids=assay_ids, method="post")

    # Проверка, что использовался POST

    assert response.request.headers.get("X-HTTP-Method-Override") == "GET"

```
**Покрытие**: [06-activity-data-extraction.md](requirements/06-activity-data-extraction.md)

### 3. Golden-run тесты
**Назначение**: Бит-в-бит детерминизм.

#### 3.1 Bit-exact сравнение
```python

# tests/golden/test_golden_run.py

def test_golden_run_activity():
    """AC1: Бит-в-бит идентичность."""
    from library.pipelines.activity import ActivityPipeline

    config = ActivityConfig(...)

    # Run 1

    pipeline = ActivityPipeline(config)
    output1 = pipeline.run()

    # Run 2 (те же параметры)

    pipeline = ActivityPipeline(config)
    output2 = pipeline.run()

    # Byte-by-byte comparison

    assert files_identical(output1.csv, output2.csv)
    assert files_identical(output1.meta, output2.meta)

    # Checksums

    assert output1.checksum_dataset == output2.checksum_dataset

def test_golden_run_documents():
    """Аналогично для documents."""

    #

def test_golden_run_assay():
    """Аналогично для assay."""

    # (continued)

```
**CLI команда**:

```bash

python -m pipeline run --golden data/golden/activity.csv

```
**Покрытие**: AC1, G1, G13

### 4. Нагрузочные тесты и лимиты
**Назначение**: Определение "безопасных" лимитов API.

#### 4.1 Бинарный поиск limit
```python

# tests/load/test_limit_search.py

def test_binary_search_limit():
    """G3: Поиск максимального безопасного limit для activity."""
    from library.clients.chembl import ChEMBLClient

    def try_limit(limit):
        try:
            client.fetch_activities(limit=limit, timeout=10)
            return True
        except (HTTPError, URITooLong) as e:
            return False

    # Бинарный поиск

    low, high = 100, 10000
    while high - low > 1:
        mid = (low + high) // 2
        if try_limit(mid):
            low = mid
        else:
            high = mid

    safe_limit = low

    # Логирование результата

    print(f"Safe limit: {safe_limit}")
    assert safe_limit >= 1000

    # Фиксация в конфиге

    assert ActivityConfig.max_limit == safe_limit

```
**Покрытие**: G3

#### 4.2 Метрики частоты 429
```python

# tests/load/test_rate_limit_metrics.py

def test_track_429_rate():
    """Мониторинг частоты 429 во время выгрузки."""
    from library.clients.unified_client import UnifiedAPIClient

    client = UnifiedAPIClient(config)
    metrics = client.metrics

    # Выгрузка большого объема

    for i in range(1000):
        try:
            client.request("https://chembl.example.com/activity")
        except RateLimitError:
            pass

    # Проверка метрик

    assert metrics.rate_limit_errors < 50  # менее 5% 429

    assert metrics.avg_retry_after > 0

    # Сохранение в отчет

    report = {
        "total_requests": metrics.total_requests,
        "rate_limit_errors": metrics.rate_limit_errors,
        "avg_retry_after": metrics.avg_retry_after
    }

```
**Покрытие**: G11

## Тестовые сценарии
### Сценарий 1: Полный пайплайн Activity
```python

def test_full_activity_pipeline():
    """End-to-end тест пайплайна activity."""

    # 1. Extract

    activities = extract_activities(config)

    # 2. Validate pagination

    assert_all_offsets_covered(activities)

    # 3. Sort

    activities = activities.sort_values("activity_id")

    # 4. QC

    assert activities["activity_id"].duplicated().sum() == 0  # AC9

    # 5. Write atomic

    output = write_atomic(activities, run_id)

    # 6. Validate artifacts

    assert output.csv.exists()
    assert output.meta.exists()
    assert not is_partial_file(output.csv)

```
**Покрытие**: AC6, AC9, AC4

### Сценарий 2: Assay с long-format
```python

def test_assay_long_format():
    """AC8: Long-format для nested data."""
    assays = extract_assays(config)

    # Expand nested

    parameters = expand_assay_parameters_long(assays)
    variants = expand_variant_sequences_long(assays)

    # Проверка: ни одна запись не потеряна

    assert len(parameters) > 0)
    assert len(variants) > 0

    # RI check

    assert all(p["assay_chembl_id"] in assays["assay_chembl_id"].values
              for p in parameters.itertuples())

```
**Покрытие**: AC8, G7

### Сценарий 3: Atomic write failure recovery
```python

def test_atomic_write_failure():
    """AC4: Нет partial artifacts при ошибке."""
    mock_error = OSError("Disk full")
    mocker.patch('Path.write_bytes', side_effect=mock_error)

    with pytest.raises(RuntimeError):
        write_atomic(df, path, run_id)

    # Проверка cleanup

    assert not path.exists()
    assert not (path.parent / ".tmp" / run_id / f"{path.name}.tmp").exists()

```
**Покрытие**: AC4, G1

## Метрики покрытия
- **Юнит-тесты**: >90% для нормализаторов, схем, hash utils

- **Интеграционные**: >80% для пагинации, retry, clients

- **Golden-run**: 100% для критичных пайплайнов (activity, assay, documents)

- **Нагрузочные**: 100% для limit discovery

## Инструменты
- **pytest**: основная платформа

- **pytest-mock**: mock-ing зависимостей

- **pytest-benchmark**: измерение производительности

- **pytest-cov**: измерение покрытия кода

## CI Integration
```yaml

# .github/workflows/test.yml

- name: Run tests
  run: |
    pytest tests/unit/ -v --cov=src
    pytest tests/integration/ -v
    pytest tests/golden/ -v --golden-dir=../data/golden
    pytest tests/load/ -v --slow

```
## Связи с документами
- [acceptance-criteria.md](acceptance-criteria.md) — критерии для валидации

- [gaps.md](gaps.md) — пробелы, которые закрывают тесты

- [02-io-system.md](requirements/02-io-system.md) — тесты atomic write

- [03-data-extraction.md](requirements/03-data-extraction.md) — тесты pagination и retry

- [04-normalization-validation.md](requirements/04-normalization-validation.md) — тесты schema drift

- [06-activity-data-extraction.md](requirements/06-activity-data-extraction.md) — тесты activity пайплайна
