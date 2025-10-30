# Acceptance Criteria
Матрица проверок инвариантов архитектуры проекта.

## Таблица критериев
| ac_id | инвариант | способ проверки | команда/псевдокод | порог/ожидаемое | артефакт |
|---|---|---|---|---|---|
| AC1 | Бит-в-бит детерминизм | Golden-run | --golden path/to/golden.csv | байтовая идентичность | diff отчёт [ref: 02-io-system.md](requirements/02-io-system.md#ac-01-golden-compare-детерминизма) |
| AC2 | column_order=схеме | Сравнить df.columns с schema.column_order | assert list(df.columns)==schema.column_order | 100% совпадение | лог теста [ref: 04-normalization-validation.md](requirements/04-normalization-validation.md#ac-03-column-order-validation) |
| AC3 | hash_row стабилен | Канонизация + хэш | см. патч в implementation-examples.md | идентичность на повторном запуске | checksum |

| AC4 | Нет partial artifacts | Проверка наличия/размеров | см. 02-io протокол | ни одного пустого/пропавшего | валидатор [ref: 02-io-system.md](requirements/02-io-system.md#ac-02-запрет-частичных-артефактов) |
| AC5 | Respect Retry-After | Mock 429 | см. AC-07 клиента | ожидание ≥ Retry-After | логи [ref: 03-data-extraction.md](requirements/03-data-extraction.md#ac-07-respect-retry-after-429) |
| AC6 | Activity сортировка | Перед записью | df.sort_values("activity_id") | устойчивый порядок | meta.yaml note [ref: 06-activity-data-extraction.md](requirements/06-activity-data-extraction.md#детерминизм) |
| AC7 | Assay batch≤25 | Конфиг-валидация | assert cfg.batch_size<=25 | fail-fast при >25 | лог [ref: 00-architecture-overview.md](requirements/00-architecture-overview.md) |
| AC8 | Long-format nested | Трансформация | expand_assay_parameters_long(...) | ни одной потерянной записи | RI-отчёт [ref: 00-architecture-overview.md](requirements/00-architecture-overview.md) |
| AC9 | QC duplicates=0 | Проверка | df["activity_id"].duplicated().sum()==0 | 0 | qc_summary [ref: 06-activity-data-extraction.md](requirements/06-activity-data-extraction.md#11-quality-control) |
| AC10 | Schema drift fail-fast | Запуск с несовместимой major | --fail-on-schema-drift | exit!=0 | лог [ref: 04-normalization-validation.md](requirements/04-normalization-validation.md#ac-08-schema-drift-detection) |

## Новые AC (AUD-5)
### AC11: Обязательные поля логов
**Цель:** Все логи содержат минимальный набор полей для трассируемости.

```python

# Проверка обязательных полей

def test_mandatory_log_fields():
    with capture_logs() as logs:
        logger.info("test message")

    log = logs[0]
    mandatory = ["run_id", "stage", "actor", "source", "generated_at"]

    for field in mandatory:
        assert field in log, f"Missing field: {field}"

```
**Артефакт:** Логи должны проходить статическую проверку обязательных полей.

### AC12-AC16: QC пороги по пайплайнам
**AC12 (Assay):** duplicates=0, referential_integrity_violations=0

**AC13 (Activity):** duplicates_activity_id=0, max_ic50_missing ≤ 5%

**AC14 (Testitem):** duplicates_molecule_chembl_id=0, pubchem_enrichment ≥ 60%

**AC15 (Target):** duplicates_target_chembl_id=0, uniprot_coverage ≥ 80%

**AC16 (Document):** duplicates_document_chembl_id=0, s2_access_denied ≤ 5%

**Артефакт:** QC отчеты с явными порогами для каждого пайплайна.

## Детализация по категориям
### IO и детерминизм (AC1, AC3, AC4, AC6)
#### AC1: Бит-в-бит детерминизм
```python

# Проверка golden-run

def test_deterministic_output():
    run1 = generate_output(config)
    run2 = generate_output(config)
    assert files_identical(run1.output_file, run2.output_file)

    # CLI

    # python -m pipeline run --golden data/golden/assay.csv

```
#### AC3: hash_row стабилен
```python

def canonicalize_row_for_hash(row: dict) -> str:
    """JSON c sort_keys=True, ISO8601 для дат, float формат %.6f, NA-policy: None→null."""
    def _normalize(v):
        if isinstance(v, float):
            return float(f"{v:.6f}")
        if isinstance(v, (datetime.date, datetime.datetime)):
            return v.isoformat()
        return v
    return json.dumps({k: _normalize(v) for k, v in sorted(row.items())},
                      sort_keys=True, separators=(",", ":"))

```
#### AC4: Нет partial artifacts
```python

# Проверка после записи

assert output_file.exists()
assert meta_file.exists()
assert not is_partial_file(output_file)  # по размеру, заголовкам

```
#### AC6: Activity сортировка
```python

df_final = df.sort_values(["activity_id"], kind="mergesort")

# mergesort гарантирует стабильность

```
### Схемы и валидация (AC2, AC10)
#### AC2: column_order=схеме
```python

schema = get_schema("ActivitySchema")
df_validated = schema.validate(df)
assert list(df_validated.columns) == schema.column_order

```
#### AC10: Schema drift fail-fast
**Важно:** В production режиме default=True обязателен для предотвращения незамеченных breaking changes.

```python

# CLI (continued 1)

# python -m pipeline run --fail-on-schema-drift  # default=True в production

# Runtime

current_schema = schema_registry.get("ActivitySchema", "1.0.0")
new_schema = schema_registry.get("ActivitySchema", "2.0.0")  # major bump

if not current_schema.compatible_with(new_schema):
    raise SchemaDriftError(f"Incompatible schema: {current_schema} -> {new_schema}")

```
### API клиенты и отказоустойчивость (AC5, AC11)
#### AC5: Respect Retry-After
```python

if response.status_code == 429:
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        wait = min(int(retry_after), 60)
        logger.warning("Rate limited by API", code=429, retry_after=wait, endpoint=endpoint, attempt=attempt)
        time.sleep(wait)
    raise RateLimitError("Rate limited")

# Test

def test_respect_retry_after():
    mock_response = Mock(status_code=429, headers={'Retry-After': '5'})
    with patch('requests.get', return_value=mock_response):
        start = time.time()
        client.request(url)
        elapsed = time.time() - start
        assert elapsed >= 5.0
        assert elapsed <= 60.0  # Cap инвариант

```
### Assay и трансформации (AC7, AC8)
#### AC7: Assay batch≤25
```python

@dataclass
class AssayConfig:
    batch_size: int = field(default=25)

    def __post_init__(self):
        if self.batch_size > 25:
            raise ValueError(f"batch_size must be ≤ 25, got {self.batch_size}")

```
#### AC8: Long-format nested
```python

def expand_assay_parameters_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Превращает массив parameters в long-format с полями:
    assay_chembl_id, param_index, param_name, param_value, row_subtype="parameter".
    """
    rows = []
    for _, r in df.iterrows():
        for i, p in enumerate(r.get("assay_parameters") or []):
            rows.append({
                "assay_chembl_id": r["assay_chembl_id"],
                "param_index": i,
                "param_name": p.get("name"),
                "param_value": p.get("value"),
                "row_subtype": "parameter",
            })
    return pd.DataFrame(rows, columns=["assay_chembl_id","param_index","param_name","param_value","row_subtype"])

```
### QC и качество (AC9)
#### AC9: QC duplicates=0
```python

duplicate_count = df["activity_id"].duplicated().sum()
assert duplicate_count == 0, f"Found {duplicate_count} duplicate activity_id entries"

# QC report должен содержать

qc_report = {
    "duplicates_activity_id": duplicate_count,
    "threshold": 0,
    "passed": duplicate_count == 0
}

```
## Связи с Gap-листом
| AC ID | Покрывает gaps | Закрытые риски |
|-------|----------------|----------------|
| AC1 | G1, G13 | |
| AC2 | G4 | |
| AC3 | G5 | **R1** (NA-policy) ✅ |
| AC4 | G1, G15 | |
| AC5 | G11 | |
| AC6 | G9 | |
| AC7 | G6 | |
| AC8 | G7 | |
| AC9 | G10 | |
| AC10 | G4, G8 | **R2** (meta.yaml lineage) ✅ |
| AC11 | G12 | **R4** (обязательные поля логов) ✅ |
| AC12 | - | |
| AC13 | - | |
| AC14 | - | |
| AC15 | - | |
| AC16 | - | |

**Дополнительные закрытые риски:**

- **R4** (AUD-5): Обязательные поля логов формализованы в [01-logging-system.md](requirements/01-logging-system.md#acceptance-criteria-aud-5) ✅

- **R3** (major): Протокол requeue для PartialFailure формализован в [03-data-extraction.md](requirements/03-data-extraction.md#протокол-повторной-постановки-requeue-для-partialfailure) ✅

## Инструменты проверки
- **AC1**: `diff -u golden.csv actual.csv`, `sha256sum`

- **AC2-AC10**: pytest тесты в `tests/acceptance/`

- **AC5**: mock test с time assertion

- **AC7**: config validation при инициализации

- **AC9**: QC generator в OutputWriter

## Метрики успеха
После внедрения всех AC:

- Средний балл по ISO/IEC 25010 ≥ 4.0

- All High-priority gaps закрыты

- Зелёный CI pipeline

- Golden-run проходит бит-в-бит
