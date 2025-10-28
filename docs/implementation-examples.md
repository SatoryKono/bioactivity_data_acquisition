# Примеры реализации

Унифицированные патчи для закрытия gaps и выполнения acceptance criteria.

## Обзор

Данный документ содержит конкретные изменения кода (unified diff формат) для:
- Канонизации хеширования (AC3)
- Long-format для вложенных данных (AC8)
- Rate limiting и Retry-After (AC5)
- CLI флаги строгих режимов (AC10)
- Atomic writer (AC1, AC4)

## Патч 1: Канонизация для хэширования

**Файл:** `src/library/common/hash_utils.py`

**Зачем:** AC3, детерминизм hash_row

**Ссылка:** [00-architecture-overview.md](requirements/00-architecture-overview.md#каноническая-сериализация)

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
+        return v
+    return json.dumps({k: _normalize(v) for k, v in sorted(row.items())},
+                      sort_keys=True, separators=(",", ":"))
@@
-def hash_row(row: dict) -> str:
-    return sha256(serialize_row(row).encode()).hexdigest()
+def hash_row(row: dict) -> str:
+    return sha256(canonicalize_row_for_hash(row).encode()).hexdigest()
```

**Тест:**
```python
def test_hash_row_deterministic():
    row = {"id": "A001", "value": 12.3456789, "date": date(2025, 1, 28)}
    hash1 = hash_row(row)
    hash2 = hash_row(row)
    assert hash1 == hash2  # Детерминированность
    
    # Проверка форматирования
    canonical = canonicalize_row_for_hash(row)
    assert '"date":"2025-01-28"' in canonical
    assert '"value":12.345679' in canonical  # округлено до 6 знаков
```

## Патч 2: Long-format для assay parameters

**Файл:** `src/library/pipelines/assay/transform.py`

**Зачем:** AC8, предотвращение потерь вложенных данных

**Ссылка:** [00-architecture-overview.md](requirements/00-architecture-overview.md#long-format)

```diff
--- a/src/library/pipelines/assay/transform.py
+++ b/src/library/pipelines/assay/transform.py
@@
-def expand_assay_parameters(df: pd.DataFrame) -> pd.DataFrame:
-    ...
+def expand_assay_parameters_long(df: pd.DataFrame) -> pd.DataFrame:
+    """
+    Превращает массив parameters в long-format с полями:
+    assay_chembl_id, param_index, param_name, param_value, row_subtype="parameter".
+    
+    Args:
+        df: DataFrame с колонкой assay_parameters (list[dict])
+        
+    Returns:
+        DataFrame в long-format по одному параметру на строку
+    """
+    rows = []
+    for _, r in df.iterrows():
+        params = r.get("assay_parameters") or []
+        for i, p in enumerate(params):
+            rows.append({
+                "assay_chembl_id": r["assay_chembl_id"],
+                "param_index": i,
+                "param_name": p.get("name"),
+                "param_value": p.get("value"),
+                "row_subtype": "parameter",
+            })
+    if not rows:
+        return pd.DataFrame(columns=["assay_chembl_id","param_index","param_name","param_value","row_subtype"])
+    return pd.DataFrame(rows, columns=["assay_chembl_id","param_index","param_name","param_value","row_subtype"])
```

**Использование:**
```python
# Старый подход (потеря данных)
df_flat = df.explode("assay_parameters")  # ❌ Не гарантирует сохранность

# Новый подход (long-format)
df_params = expand_assay_parameters_long(df)  # ✅ Все параметры сохранены
```

## Патч 3: TokenBucket + Retry-After логирование

**Файл 1:** `src/library/clients/rate_limit.py`

**Файл 2:** `src/library/clients/unified_client.py`

**Зачем:** AC5, наблюдаемость rate limiting

**Ссылка:** [03-data-extraction.md → AC-07 Retry-After](requirements/03-data-extraction.md#ac-07-respect-retry-after-429)

```diff
--- a/src/library/clients/rate_limit.py
+++ b/src/library/clients/rate_limit.py
@@
 class TokenBucketLimiter:
     def __init__(self, max_calls:int, period:float, jitter:bool=True): 
         self.max_calls = max_calls
         self.period = period
         self.jitter = jitter
         self._tokens = float(max_calls)
         self._last_update = time.time()
         
     def acquire(self) -> None:
-        pass
+        """Блокирующее ожидание следующего токена, с опциональным джиттером."""
+        now = time.time()
+        elapsed = now - self._last_update
+        
+        # Refill tokens
+        self._tokens = min(self.max_calls, self._tokens + (elapsed / self.period) * self.max_calls)
+        
+        if self._tokens >= 1.0:
+            self._tokens -= 1.0
+            self._last_update = now
+        else:
+            # Wait for next token
+            wait_time = self.period / self.max_calls
+            if self.jitter:
+                wait_time *= random.uniform(0.8, 1.2)
+            time.sleep(wait_time)
+            self._tokens = 0.0
+            self._last_update = time.time()

--- a/src/library/clients/unified_client.py
+++ b/src/library/clients/unified_client.py
@@
 def request(...):
     for attempt in range(max_retries):
         try:
             response = session.get(endpoint, params=params, timeout=timeout)
             
             if response.status_code == 429:
-                retry_after = response.headers.get('Retry-After')
+                retry_after = response.headers.get('Retry-After')
                 if retry_after:
-                    time.sleep(min(int(retry_after), 60))
+                    wait = min(int(retry_after), 60)
+                    logger.warning(
+                        "Rate limited by API",
+                        code=429,
+                        retry_after=wait,
+                        endpoint=endpoint,
+                        attempt=attempt
+                    )
+                    time.sleep(wait)
                 raise RateLimitError("Rate limited")
```

**Лог пример:**
```json
{
  "event": "Rate limited by API",
  "code": 429,
  "retry_after": 5,
  "endpoint": "/api/data/activity",
  "attempt": 2,
  "timestamp": "2025-01-28T14:23:15.123Z"
}
```

## Патч 4: CLI-флаги строгих режимов

**Файл:** `src/cli/main.py`

**Зачем:** AC10 и gap G8

**Ссылка:** [04-normalization-validation.md → Schema drift](requirements/04-normalization-validation.md#ac-08-schema-drift-detection)

```diff
--- a/src/cli/main.py
+++ b/src/cli/main.py
@@
 parser = argparse.ArgumentParser(description="Bioactivity Data Acquisition Pipeline")
-parser.add_argument("--fail-on-schema-drift", action="store_true", default=False)
+parser.add_argument("--fail-on-schema-drift", action="store_true", default=True,
+                    help="Fail-fast при несовместимой major версии схемы")
+parser.add_argument("--strict-enrichment", action="store_true", default=True,
+                    help="Запрет лишних полей при enrichment (whitelist)")
 parser.add_argument("--golden", type=str, help="Path to golden output for comparison")

+# Обработка флагов
 if args.fail_on_schema_drift:
+    config.schema_validation = "strict"
     schema = schema_registry.get(args.schema_name, args.schema_version)
     if not is_compatible(schema):
         logger.error("Schema drift detected", schema=args.schema_name, version=args.schema_version)
         sys.exit(1)

+if args.strict_enrichment:
+    config.enrichment_policy = "whitelist"  # Запрет неподдерживаемых полей
```

**Примеры использования:**
```bash
# Строгий режим по умолчанию
python -m pipeline run --extract assay

# Мягкий режим для разработки
python -m pipeline run --extract assay --no-fail-on-schema-drift --no-strict-enrichment
```

## Патч 5: Atomic writer с os.replace

**Файл:** `src/library/io/writer.py`

**Зачем:** AC1/AC4, гарантия атомарности

**Ссылка:** [02-io-system.md → Atomic Write](requirements/02-io-system.md#протокол-atomic-write)

```diff
--- a/src/library/io/writer.py
+++ b/src/library/io/writer.py
@@
-def write_bytes_atomic(path: Path, content: bytes) -> None:
-    path.write_bytes(content)
+def write_bytes_atomic(path: Path, content: bytes, run_id: str) -> None:
+    """
+    Атомарная запись через run-scoped временную директорию с os.replace.
+    
+    Args:
+        path: Финальный путь к файлу
+        content: Байты для записи
+        run_id: UUID run-scope для изоляции временных файлов
+    """
+    # Create run-scoped temp directory
+    tmpdir = path.parent / ".tmp" / run_id
+    tmpdir.mkdir(parents=True, exist_ok=True)
+    
+    # Write to temporary file
+    tmppath = tmpdir / (path.name + ".tmp")
+    tmppath.write_bytes(content)
+    
+    # Atomic replace
+    output_parent = path.parent
+    output_parent.mkdir(parents=True, exist_ok=True)
+    os.replace(str(tmppath), str(path))  # Atomic на POSIX и Windows
+    
+    # Cleanup temp file (not dir, для других потенциальных файлов в run)
+    tmppath.unlink(missing_ok=True)


+@contextmanager
+def atomic_writer_context(path: Path, run_id: str):
+    """Context manager для гарантированного cleanup."""
+    tmpdir = path.parent / ".tmp" / run_id
+    try:
+        yield tmpdir
+    finally:
+        # Cleanup всех .tmp файлов в run-scoped dir
+        if tmpdir.exists():
+            for temp_file in tmpdir.glob("*.tmp"):
+                temp_file.unlink(missing_ok=True)
+            try:
+                tmpdir.rmdir()
+            except OSError:
+                pass
```

**Использование:**
```python
run_id = generate_run_id()

# CSV
with atomic_writer_context(output_path, run_id):
    temp_path = tmpdir / f"{output_path.name}.tmp"
    df.to_csv(temp_path, index=False)
    os.replace(str(temp_path), str(output_path))

# Metadata
meta_content = yaml.dump(metadata)
write_bytes_atomic(meta_path, meta_content.encode(), run_id)
```

## Интеграционное использование

**Пример полного пайплайна assay с применением всех патчей:**

```python
from src.library.common.hash_utils import hash_row
from src.library.pipelines.assay.transform import expand_assay_parameters_long
from src.library.io.writer import atomic_writer_context
from src.library.clients.rate_limit import TokenBucketLimiter

def assay_pipeline(config: AssayConfig, run_id: str):
    # AC7: валидация batch
    assert config.batch_size <= 25, f"batch_size must be ≤ 25, got {config.batch_size}"
    
    limiter = TokenBucketLimiter(max_calls=10, period=1.0)
    
    # Extract
    df_raw = client.fetch_assays(assay_ids, batch_size=config.batch_size)
    
    # AC8: Long-format
    df_params = expand_assay_parameters_long(df_raw)
    
    # Transform & validate
    df_normalized = normalize_assays(df_raw)
    
    # AC6: Sort
    df_final = df_normalized.sort_values("assay_chembl_id")
    
    # AC3: Hash
    df_final["hash_row"] = df_final.apply(hash_row, axis=1)
    
    # AC1/AC4: Atomic write
    with atomic_writer_context(output_path, run_id):
        temp_path = tmpdir / f"{output_path.name}.tmp"
        df_final.to_csv(temp_path, index=False)
        os.replace(str(temp_path), str(output_path))
    
    logger.info("Pipeline completed", run_id=run_id, rows=len(df_final))
```

## Связь с gaps и AC

| Патч | Gaps | AC |
|------|------|-----|
| 1. Канонизация хеша | G5 | AC3 |
| 2. Long-format | G7 | AC8 |
| 3. Retry-After | G11 | AC5 |
| 4. CLI флаги | G4, G8 | AC10 |
| 5. Atomic writer | G1, G15 | AC1, AC4 |

## Примечания

- Все патчи применяются последовательно согласно [pr-plan.md](pr-plan.md)
- Предварительно нужно прогнать тесты из [test-plan.md](test-plan.md)
- При возникновении проблем см. [gaps.md](gaps.md) для связей с исходными проблемами

