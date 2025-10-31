# Примеры реализации (Unified Diff)

Унифицированные патчи для закрытия gaps из [gaps.md](gaps.md).

## Патч 1: Канонизация для хэширования (AC3, G5)

**Зачем**: Детерминизм hash_row, устранение различий из-за порядка и формата.

**Файл**: `src/library/common/hash_utils.py`

```diff

--- a/src/library/common/hash_utils.py
+++ b/src/library/common/hash_utils.py

@@
-def serialize_row(row: dict) -> str:

-    return json.dumps(row)

+def canonicalize_row_for_hash(row: dict) -> str:

+    """

+    JSON c sort_keys=True, ISO8601 для дат, float формат %.6f, NA-policy: None→null.

+    """

+    def _normalize(v):

+        if isinstance(v, float):

+            return float(f"{v:.6f}")

+        if isinstance(v, (datetime.date, datetime.datetime)):

+            return v.isoformat()

+        if v is None:

+            return None  # будет serialized как null

+        return v

+    return json.dumps({k: _normalize(v) for k, v in sorted(row.items())},

+                      sort_keys=True, separators=(",", ":"))

@@
-def hash_row(row: dict) -> str:

-    return sha256(serialize_row(row).encode()).hexdigest()

+def hash_row(row: dict) -> str:

+    return sha256(canonicalize_row_for_hash(row).encode()).hexdigest()

```

**Особенности**:

- `sort_keys=True` для детерминированного порядка

- ISO8601 для дат и datetime (единообразно)

- Фиксированная точность float (%.6f)

- `separators=(",", ":")` без пробелов

- NA-policy: None→null

**См. также**: [00-architecture-overview.md → Каноническая сериализация](requirements/00-architecture-overview.md)

---

## Патч 2: Long-format для assay parameters (AC8, G7)

**Зачем**: Предотвращение потери вложенных данных, обязательный long-format для nested структур.

**Файл**: `src/library/pipelines/assay/transform.py`

```diff

--- a/src/library/pipelines/assay/transform.py
+++ b/src/library/pipelines/assay/transform.py

@@
-def expand_assay_parameters(df: pd.DataFrame) -> pd.DataFrame:

-    # Старая реализация с потерей данных

-    ...

+def expand_assay_parameters_long(df: pd.DataFrame) -> pd.DataFrame:

+    """

+    Превращает массив parameters в long-format с полями:

+    assay_chembl_id, param_index, param_name, param_value, row_subtype="parameter".

+

+    Инвариант: ни одна запись не теряется.

+    """

+    rows = []

+    for _, r in df.iterrows():

+        for i, p in enumerate(r.get("assay_parameters") or []):

+            rows.append({

+                "assay_chembl_id": r["assay_chembl_id"],

+                "param_index": i,

+                "param_name": p.get("name"),

+                "param_value": p.get("value"),

+                "row_subtype": "parameter",

+            })

+

+    if not rows:

+        # Возвращаем пустой DataFrame с правильными колонками

+        return pd.DataFrame(columns=["assay_chembl_id","param_index","param_name","param_value","row_subtype"])

+

+    return pd.DataFrame(rows, columns=["assay_chembl_id","param_index","param_name","param_value","row_subtype"])

+
+# Аналогично для variant_sequences и classifications

+def expand_variant_sequences_long(df: pd.DataFrame) -> pd.DataFrame:

+    """Тот же подход для variant_sequences."""

+    rows = []

+    for _, r in df.iterrows():

+        for i, vs in enumerate(r.get("variant_sequences") or []):

+            rows.append({

+                "assay_chembl_id": r["assay_chembl_id"],

+                "varseq_index": i,

+                "variant_name": vs.get("name"),

+                "sequence": vs.get("sequence"),

+                "row_subtype": "variant_sequence",

+            })

+    return pd.DataFrame(rows if rows else [],

+                       columns=["assay_chembl_id","varseq_index","variant_name","sequence","row_subtype"])

+
+def expand_classifications_long(df: pd.DataFrame) -> pd.DataFrame:

+    """Тот же подход для assay_classifications."""

+    rows = []

+    for _, r in df.iterrows():

+        for i, ac in enumerate(r.get("assay_classifications") or []):

+            rows.append({

+                "assay_chembl_id": r["assay_chembl_id"],

+                "class_index": i,

+                "class_type": ac.get("type"),

+                "class_value": ac.get("value"),

+                "row_subtype": "classification",

+            })

+    return pd.DataFrame(rows if rows else [],

+                       columns=["assay_chembl_id","class_index","class_type","class_value","row_subtype"])

```

**Использование**:

```python

# В пайплайне assay

df_parameters = expand_assay_parameters_long(df)
df_variants = expand_variant_sequences_long(df)
df_classes = expand_classifications_long(df)

# Объединение

df_long = pd.concat([df_parameters, df_variants, df_classes], ignore_index=True)

```

**См. также**: [00-architecture-overview.md → Long format](requirements/00-architecture-overview.md)

---

## Патч 3: TokenBucket + Retry-After логирование (AC5, G11)

**Зачем**: Наблюдаемость ретраев, respect Retry-After инвариант.

**Файл**: `src/library/clients/rate_limit.py`

```diff

--- a/src/library/clients/rate_limit.py
+++ b/src/library/clients/rate_limit.py

@@
 class TokenBucketLimiter:
     def __init__(self, max_calls:int, period:float, jitter:bool=True): ...

     def acquire(self) -> None:

-        pass

+        # Блокирующее ожидание следующего токена, с опциональным джиттером

+        now = time.time()

+

+        # Пополнение bucket

+        elapsed = now - self.last_refill

+        tokens_to_add = int(elapsed / self.period * self.max_calls)

+        self.tokens = min(self.max_calls, self.tokens + tokens_to_add)

+        self.last_refill = now

+

+        # Если токенов нет, ждём

+        if self.tokens < 1:

+            wait_time = self.period / self.max_calls

+            if self.jitter:

+                wait_time += random.uniform(0, wait_time * 0.1)

+            time.sleep(wait_time)

+            self.tokens -= 1

+        else:

+            self.tokens -= 1

```

**Файл**: `src/library/clients/unified_client.py`

```diff

--- a/src/library/clients/unified_client.py
+++ b/src/library/clients/unified_client.py

@@ def request(...):
     if response.status_code == 429:
         retry_after = response.headers.get('Retry-After')
         if retry_after:

-            time.sleep(min(int(retry_after), 60))

+            wait = min(int(retry_after), 60)

+            logger.warning("Rate limited by API",

+                          code=429,

+                          retry_after=wait,

+                          endpoint=endpoint,

+                          attempt=attempt,

+                          run_id=context.run_id)

+            time.sleep(wait)

         raise RateLimitError("Rate limited")

```

**Тест**:

```python

def test_respect_retry_after(mocker, caplog):
    mock_response = Mock(status_code=429, headers={'Retry-After': '5'})
    mocker.patch('requests.get', return_value=mock_response)

    start = time.time()
    with pytest.raises(RateLimitError):
        client.request(url)
    elapsed = time.time() - start

    assert elapsed >= 5.0, f"Did not wait Retry-After, elapsed={elapsed}"

    # Проверка лога

    assert any("Rate limited by API" in rec.message and rec.levelname == "WARNING"
               for rec in caplog.records)

```

**См. также**: [03-data-extraction.md → AC-07 Retry-After](requirements/03-data-extraction.md#ac-07-respect-retry-after-429)

---

## Патч 4: CLI-флаги строгих режимов (AC10, G8)

**Зачем**: Fail-fast на schema drift, whitelist enrichment контроль.

**Файл**: `src/cli/main.py`

```diff

--- a/src/cli/main.py
+++ b/src/cli/main.py

@@
-parser.add_argument("--fail-on-schema-drift", action="store_true", default=False)
+parser.add_argument("--fail-on-schema-drift", action="store_true", default=True,

+                    help="Fail-fast при несовместимой major версии схемы")

+parser.add_argument("--strict-enrichment", action="store_true", default=True,

+                    help="Запрет лишних полей при enrichment (whitelist)")

```

**Использование**:

```bash

# Строгий режим (default)

python -m pipeline run --fail-on-schema-drift --strict-enrichment

# Либеральный режим (для debugging)

python -m pipeline run --no-fail-on-schema-drift --no-strict-enrichment

```

**См. также**: [04-normalization-validation.md → Schema drift](requirements/04-normalization-validation.md#ac-08-schema-drift-detection)

---

## Патч 5: Atomic writer с os.replace (AC1, AC4, G1)

**Зачем**: Гарантированная атомарность записи, отсутствие partial artifacts.

**Файл**: `src/library/io/writer.py`

```diff

--- a/src/library/io/writer.py
+++ b/src/library/io/writer.py

@@
-def write_bytes_atomic(path: Path, content: bytes) -> None:

-    path.write_bytes(content)

+def write_bytes_atomic(path: Path, content: bytes, run_id: str) -> None:

+    """

+    Атомарная запись через run-scoped temp dir и os.replace.

+

+    Протокол:

+    1. Создать temp dir: {parent}/.tmp/{run_id}/

+    2. Записать во временный файл: {name}.tmp

+    3. os.replace(tmp_path, final_path) — атомарная операция

+    4. Cleanup temp files при любой ошибке

+

+    Инвариант: либо файл записан полностью, либо не записан вообще.

+    """

+    tmpdir = path.parent / f".tmp_run_{run_id}"

+    tmpdir.mkdir(parents=True, exist_ok=True)

+

+    tmppath = tmpdir / f"{path.name}.tmp"

+

+    try:

+        # Запись

+        tmppath.write_bytes(content)

+

+        # Валидация (опционально)

+        # checksum = compute_checksum(tmppath)

+

+        # Атомарный rename

+        path.parent.mkdir(parents=True, exist_ok=True)

+        os.replace(str(tmppath), str(path))

+

+    except Exception as e:

+        # Cleanup при ошибке

+        tmppath.unlink(missing_ok=True)

+        raise RuntimeError(f"Failed to write atomic file: {path}") from e

+

+    finally:

+        # Cleanup temp dir если пустая

+        try:

+            if tmpdir.exists():

+                remaining = list(tmpdir.glob("*"))

+                if not remaining:

+                    tmpdir.rmdir()

+        except OSError:

+            pass

+
+def write_dataframe_atomic(df: pd.DataFrame, path: Path, run_id: str, format: str = "csv") -> None:

+    """Обертка для DataFrame с атомарной записью."""

+    if format == "csv":

+        content = df.to_csv(index=False).encode("utf-8")

+    elif format == "parquet":

+        content = df.to_parquet(index=False)

+    else:

+        raise ValueError(f"Unsupported format: {format}")

+

+    write_bytes_atomic(path, content, run_id)

```

**Проверка**:

```python

def test_atomic_write_no_partial(mocker):
    """Проверка, что при ошибке не остаётся partial файла."""
    run_id = "test_run_123"
    path = Path("/tmp/test.csv")

    # Mock ошибку записи

    mocker.patch.object(Path, 'write_bytes', side_effect=OSError("Disk full"))

    with pytest.raises(RuntimeError):
        write_bytes_atomic(path, b"content", run_id)

    # Финальный файл не должен существовать

    assert not path.exists()

    # Temp файл должен быть удалён

    tmpdir = path.parent / ".tmp" / run_id
    assert not (tmpdir / f"{path.name}.tmp").exists()

```

**См. также**: [02-io-system.md → Atomic Write](requirements/02-io-system.md#протокол-atomic-write)

---

## Резюме патчей

| Патч | Файл(ы) | AC | Gap(s) | Приоритет |
|------|---------|----|--------|-----------|
| 1. Канонизация | `hash_utils.py` | AC3 | G5 | Med |

| 2. Long-format | `transform.py` | AC8 | G7 | High |

| 3. Retry-After | `rate_limit.py`, `unified_client.py` | AC5 | G11 | High |

| 4. CLI флаги | `main.py` | AC10 | G8 | Med |

| 5. Atomic writer | `writer.py` | AC1, AC4 | G1 | High |

## Порядок применения

1. **Патч 5** (Atomic writer) — основа детерминизма

2. **Патч 1** (Канонизация) — для стабильных хешей

3. **Патч 3** (Retry-After) — для отказоустойчивости

4. **Патч 2** (Long-format) — для assay quality

5. **Патч 4** (CLI флаги) — для строгости

## Связи с другими документами

- [gaps.md](gaps.md) — описание проблем, которые решают патчи

- [acceptance-criteria.md](acceptance-criteria.md) — критерии проверки патчей

- [02-io-system.md](requirements/02-io-system.md) — протокол atomic write

- [03-data-extraction.md](requirements/03-data-extraction.md) — Retry-After стратегия

- [04-normalization-validation.md](requirements/04-normalization-validation.md) — schema drift
