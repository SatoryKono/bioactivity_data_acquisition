# 2. Система ввода-вывода (UnifiedOutputWriter)

## Обзор

UnifiedOutputWriter — детерминированная система записи данных, объединяющая:
- **Атомарную запись** через временные файлы (bioactivity_data_acquisition5)
- **Трехфайловую систему** с QC отчетами (ChEMBL_data_acquisition6)
- **Автоматическую валидацию** через Pandera
- **Run manifests** для отслеживания пайплайнов

## Архитектура

```text
UnifiedOutputWriter
├── Validation Layer
│   └── PanderaSchemaValidator
├── Format Layer
│   ├── CSVHandler (deterministic sorting)
│   └── ParquetHandler (compression, column types)
├── Quality Layer
│   ├── QualityReportGenerator
│   └── CorrelationReportGenerator
├── Metadata Layer
│   ├── OutputMetadata
│   └── ManifestWriter
└── Atomic Write Layer
    └── AtomicWriter (temporary files + rename)
```

## Компоненты

### 1. OutputArtifacts (dataclass)

Стандартизированные пути к выходным артефактам:

```python
@dataclass(frozen=True)
class OutputArtifacts:
    """Пути к стандартным выходным артефактам."""
    
    dataset: Path  # Основной датасет
    quality_report: Path  # QC метрики
    correlation_report: Path  # Корреляционный анализ
    metadata: Path | None  # Метаданные (опционально)
    manifest: Path | None  # Run manifest (опционально)
```

**Формат имен**:

```text
output.{table_name}_{date_tag}.csv
output.{table_name}_{date_tag}_quality_report_table.csv
output.{table_name}_{date_tag}_data_correlation_report_table.csv
output.{table_name}_{date_tag}.meta.yaml  # если extended
run_manifest_{timestamp}.json  # если extended
```

### 2. AtomicWriter

Безопасная атомарная запись через run-scoped временные директории с использованием `os.replace` и явным управлением `run_id`:

```python
import os
from pathlib import Path

class AtomicWriter:
    """Атомарная запись с защитой от corruption."""

    def __init__(self, run_id: str):
        self.run_id = run_id
    
    def write(self, data: pd.DataFrame, path: Path, **kwargs):
        """Записывает data в path атомарно через run-scoped temp directory."""
        
        # Run-scoped temp directory
        temp_dir = path.parent / f".tmp_run_{self.run_id}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Temp file path
        temp_path = temp_dir / f"{path.name}.tmp"
        
        try:
            # Запись во временный файл
            self._write_to_file(data, temp_path, **kwargs)
            
            # Атомарный rename через os.replace (Windows-compatible)
            path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(temp_path), str(path))
            
        except Exception as e:
            # Cleanup временного файла при ошибке
            temp_path.unlink(missing_ok=True)
            raise
        
        finally:
            # Cleanup temp directory (если пуста)
            try:
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
                elif temp_dir.exists():
                    # Удаляем temp файлы в любом случае
                    for temp_file in temp_dir.glob("*.tmp"):
                        temp_file.unlink(missing_ok=True)
            except OSError:
                pass
```

**Ключевые принципы атомарной записи:**
- **os.replace() вместо Path.rename()**: гарантирует атомарность на POSIX и Windows
- **Run-scoped temp directories**: `.tmp_run_{run_id}/` изолируют временные файлы между запусками
- **Guaranteed cleanup**: finally блок удаляет partial файлы даже при сбоях
- **Windows-compatibility**: os.replace() является атомарной операцией на всех ОС

**Передача run_id**:
- В CLI run_id создаётся на старте пайплайна и передаётся в `AtomicWriter` явным аргументом конструктора.
- Допускается DI через контекст исполнения (`contextvars` или context manager), но **инициализация всегда явная**, чтобы соблюдать инвариант детерминизма из [00-architecture-overview.md](00-architecture-overview.md#2-%D0%94%D0%B5%D1%82%D0%B5%D1%80%D0%BC%D0%B8%D0%BD%D0%B8%D0%B7%D0%BC).
- Run-scoped временные директории `.tmp_run_{run_id}` обеспечивают принцип «всё или ничего» из AC (см. раздел *Standard (2 файла, без correlation по умолчанию)* в [00-architecture-overview.md](00-architecture-overview.md#2-unifiedoutputwriter-%E2%80%94-%D0%A1%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0-%D0%B2%D0%B2%D0%BE%D0%B4%D0%B0-%D0%B2%D1%8B%D0%B2%D0%BE%D0%B4%D0%B0)).

### 3. OutputMetadata (dataclass)

Метаданные экспорта для воспроизводимости:

```python
@dataclass(frozen=True)
class OutputMetadata:
    """Метаданные выходного файла."""
    
    pipeline_version: str
    source_system: str
    chembl_release: str | None
    generated_at: str  # UTC ISO8601
    row_count: int
    column_count: int
    column_order: list[str]
    checksums: dict[str, str]  # {"dataset": "sha256:...", "quality": "sha256:..."}
    git_commit: str | None
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, column_order: list[str]):
        """Создает метаданные из DataFrame."""
        return cls(
            pipeline_version=get_version(),
            source_system="unified",
            chembl_release=get_chembl_release(),
            generated_at=datetime.now(UTC).isoformat(),
            row_count=len(df),
            column_count=len(df.columns),
            column_order=column_order,
            checksums={},
            git_commit=get_git_sha()
        )
```

### 4. FormatHandler

Универсальный обработчик форматов:

```python
class FormatHandler:
    """Обработчик различных форматов вывода."""
    
    def write_csv(
        self, 
        df: pd.DataFrame, 
        path: Path, 
        *,
        encoding: str = "utf-8",
        float_format: str = "%.3f",
        column_order: list[str] | None = None
    ):
        """Детерминированная запись CSV."""
        if column_order:
            df = df[column_order]
        
        df.to_csv(
            path,
            index=False,
            encoding=encoding,
            float_format=float_format
        )
    
    def write_parquet(
        self,
        df: pd.DataFrame,
        path: Path,
        *,
        compression: str = "snappy",
        engine: str = "pyarrow",
        column_order: list[str] | None = None
    ):
        """Оптимизированная запись Parquet."""
        if column_order:
            df = df[column_order]
        
        df.to_parquet(
            path,
            index=False,
            compression=compression,
            engine=engine
        )
```

### 5. QualityReportGenerator

Автоматическая генерация QC метрик:

```python
class QualityReportGenerator:
    """Генератор quality report."""
    
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создает QC отчет."""
        metrics = []
        
        for column in df.columns:
            null_count = df[column].isna().sum()
            null_fraction = null_count / len(df) if len(df) > 0 else 0
            unique_count = df[column].nunique()
            duplicate_count = df.duplicated(subset=[column]).sum()
            
            metrics.append({
                "column": column,
                "dtype": str(df[column].dtype),
                "null_count": null_count,
                "null_fraction": f"{null_fraction:.4f}",
                "unique_count": unique_count,
                "duplicate_count": duplicate_count,
                "min": df[column].min() if pd.api.types.is_numeric_dtype(df[column]) else None,
                "max": df[column].max() if pd.api.types.is_numeric_dtype(df[column]) else None,
            })
        
        return pd.DataFrame(metrics)
```

### 6. CorrelationReportGenerator

Корреляционный анализ:

```python
class CorrelationReportGenerator:
    """Генератор correlation report."""
    
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создает корреляционный отчет."""
        # Только числовые колонки
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return pd.DataFrame()  # Пустой если недостаточно числовых колонок
        
        corr_matrix = df[numeric_cols].corr()
        
        # Преобразуем в long format
        correlations = []
        for i, col1 in enumerate(corr_matrix.columns):
            for j, col2 in enumerate(corr_matrix.columns):
                if i <= j:  # Избегаем дубликатов
                    correlations.append({
                        "column_1": col1,
                        "column_2": col2,
                        "pearson_correlation": f"{corr_matrix.loc[col1, col2]:.4f}"
                    })

        return pd.DataFrame(correlations)
```

### 6.1 Условная генерация корреляций

Согласно инвариантам режима *Standard* из [00-architecture-overview.md](00-architecture-overview.md#2-unifiedoutputwriter-%E2%80%94-%D0%A1%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0-%D0%B2%D0%B2%D0%BE%D0%B4%D0%B0-%D0%B2%D1%8B%D0%B2%D0%BE%D0%B4%D0%B0), корреляционные отчёты выключены по умолчанию, чтобы сохранить детерминизм и минимальный AC-профиль (нет лишних файлов, нет нестабильных статистик). Генерация привязана к конфигурации `postprocess.correlation.enabled` и должна явно ветвиться в коде:

```python
def maybe_write_correlation(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    correlation_writer: CorrelationReportGenerator,
    atomic_writer: AtomicWriter,
    correlation_path: Path,
    run_logger: BoundLogger,
):
    """Опционально создаёт корреляционный отчёт."""

    if not config.postprocess.correlation.enabled:
        run_logger.info(
            "skip_correlation_report",
            reason="disabled_in_config",
            invariant="determinism"
        )
        return None

    correlation_df = correlation_writer.generate(df)

    if correlation_df.empty:
        run_logger.info("skip_correlation_report", reason="no_numeric_columns")
        return None

    atomic_writer.write(
        correlation_df,
        correlation_path,
        float_format="%.6f",  # соблюдаем форматирование из инвариантов детерминизма
    )
    return correlation_path
```

Такое ветвление делает зависимость от конфигурации явной, подчёркивая влияние на детерминизм (константный набор артефактов) и соответствие AC «всё или ничего»: если отчёт выключен, он не появляется вовсе, что предотвращает дрейф структуры выпуска.

### 7. ManifestWriter

Запись run manifest для отслеживания:

```python
@dataclass
class RunManifest:
    """Manifest выполнения пайплайна."""
    
    run_id: str
    timestamp: str  # UTC ISO8601
    exit_code: int
    duration_sec: float
    steps: list[StepManifest]
    config: dict  # Resolved configuration
    
@dataclass
class StepManifest:
    """Manifest отдельного шага."""
    
    step_name: str
    status: str  # success, failed, skipped
    exit_code: int
    duration_sec: float
    artifacts: list[str]  # Paths to output files
    stats: dict  # row_count, column_count, etc.

class ManifestWriter:
    """Записывает run manifest."""
    
    def write(self, manifest: RunManifest, path: Path):
        """Записывает manifest в JSON."""
        with path.open("w") as f:
            json.dump(asdict(manifest), f, indent=2, ensure_ascii=False)
```

## Основной класс: UnifiedOutputWriter

```python
class UnifiedOutputWriter:
    """Универсальный writer для пайплайнов."""
    
    def __init__(
        self,
        *,
        schema: pa.DataFrameModel | None = None,
        column_order: list[str] | None = None,
        key_columns: list[str] | None = None,
        format: str = "csv",  # csv или parquet
        mode: str = "standard",  # standard или extended
        output_dir: Path = Path("data/output")
    ):
        self.schema = schema
        self.column_order = column_order
        self.key_columns = key_columns or []
        self.format = format
        self.mode = mode
        self.output_dir = output_dir
        
        self.quality_generator = QualityReportGenerator()
        self.correlation_generator = CorrelationReportGenerator()
        self.format_handler = FormatHandler()
        self.atomic_writer = AtomicWriter()
        self.manifest_writer = ManifestWriter()
    
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        date_tag: str
    ) -> OutputArtifacts:
        """Записывает датасет со всеми артефактами."""
        
        # Валидация
        if self.schema:
            df = self.schema.validate(df, lazy=True)
        
        # Детерминированная сортировка
        df_sorted = self._deterministic_sort(df)
        
        # Генерация путей
        artifacts = self._make_artifacts(table_name, date_tag)
        
        # Запись основного датасета
        self._write_dataset(df_sorted, artifacts.dataset)
        
        # Генерация и запись QC отчета
        quality_df = self.quality_generator.generate(df)
        self._write_dataset(quality_df, artifacts.quality_report)
        
        # Генерация и запись correlation отчета
        correlation_df = self.correlation_generator.generate(df)
        self._write_dataset(correlation_df, artifacts.correlation_report)
        
        # Дополнительные артефакты в extended режиме
        if self.mode == "extended":
            self._write_metadata(df, artifacts.metadata)
            self._write_manifest(artifacts)
        
        return artifacts
    
    def _deterministic_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        """Детерминированная сортировка."""
        if not self.key_columns or not df.empty:
            # Используем первый доступный столбец если key_columns не заданы
            sort_cols = [col for col in self.key_columns if col in df.columns]
            if not sort_cols:
                sort_cols = [df.columns[0]]
            
            return df.sort_values(
                sort_cols,
                ascending=True,
                na_position='last'
            ).reset_index(drop=True)
        
        return df
```

## Использование

### Basic Usage

```python
from unified_output import UnifiedOutputWriter
from schemas import DocumentSchema

writer = UnifiedOutputWriter(
    schema=DocumentSchema,
    column_order=["document_chembl_id", "title", "doi", "journal"],
    key_columns=["document_chembl_id"],
    format="csv",
    mode="standard"
)

artifacts = writer.write(df, table_name="documents", date_tag="20250128")

print(f"Dataset: {artifacts.dataset}")
print(f"Quality: {artifacts.quality_report}")
print(f"Correlation: {artifacts.correlation_report}")
```

### Extended Mode с метаданными

```python
writer = UnifiedOutputWriter(
    schema=DocumentSchema,
    column_order=["document_chembl_id", "title", "doi", "journal"],
    key_columns=["document_chembl_id"],
    format="csv",
    mode="extended"  # Включает metadata и manifest
)

artifacts = writer.write(df, table_name="documents", date_tag="20250128")

print(f"Metadata: {artifacts.metadata}")
print(f"Manifest: {artifacts.manifest}")
```

### Parquet формат

```python
writer = UnifiedOutputWriter(
    schema=ActivitySchema,
    format="parquet",  # Использует Parquet вместо CSV
    output_dir=Path("data/output/parquet")
)

artifacts = writer.write(df, table_name="activities", date_tag="20250128")
```

## Deterministic Output

Детерминированный вывод обеспечивает воспроизводимость результатов.

### Column Order

**Критический принцип:** Источник истины для `column_order` — только Schema Registry (Pandera схема).

`meta.yaml` содержит **копию** `column_order` из схемы для справки и воспроизводимости, но **не является** источником истины.

```python
# schema.py - ИСТОЧНИК ИСТИНЫ
class DocumentSchema(BaseSchema):
    """Схема определяет column_order."""
    ...
    schema_id = "document.chembl"
    schema_version = "2.1.0"
    column_order = [  # ← Источник истины
        "document_chembl_id", "title", "doi", "journal",
        "hash_business_key", "hash_row"
    ]
```

При экспорте:
```python
# Берем column_order из схемы
df = df[schema.column_order]  # Используется порядок из схемы
```

В `meta.yaml` (только для справки):
```yaml
# meta.yaml - КОПИЯ для справки
column_order:  # Копируется из схемы
  - "document_chembl_id"
  - "title"
  - "doi"
  # ...

# Валидация консистентности
column_order_source: "schema_registry"
schema_id: "document.chembl"
schema_version: "2.1.0"
```

**Acceptance Criteria AC-03:** При валидации `df.columns.tolist() == schema.column_order` должно быть истиной.

### NA Policy

Правила для пропущенных значений:
- **Строки**: `NA` → `""`
- **Числа**: `NaN` → `null` (в JSON) или пустое (в CSV)

```python
df = df.fillna({
    "string_columns": "",  # Пустая строка
    "numeric_columns": None  # null в JSON
})
```

### Точность чисел

Настраиваемая по типам метрик:

| Тип метрики | Знаков после запятой | Пример |
|-------------|----------------------|--------|
| pIC50, Ki | 3 | `7.234` |
| molecular_weight, LogP | 2 | `456.78` |
| correlation, coefficient | 4 | `0.8234` |
| score, ratio | 2 | `0.95` |

```python
# В схеме
class ActivitySchema(BaseSchema):
    pic50: float = pa.Field(precision=3)  # 3 знака
    molecular_weight: float = pa.Field(precision=2)  # 2 знака
```

### Сортировка

Стандартный порядок для фактовых таблиц:
```python
sort_order = [
    "document_id",
    "target_id",
    "assay_id",
    "testitem_id",
    "activity_id",
    "created_at_utc",
    "id"  # Последний ключ для tie-breaking
]

df = df.sort_values(sort_order, ascending=True, na_position='last')
```

### Хеши

Для отслеживания изменений строк:

**hash_business_key**: хеш от business key
```python
import hashlib
bk = "|".join([row.document_id, row.target_id, row.testitem_id])
hash_bk = hashlib.sha256(bk.encode()).hexdigest()
```

**hash_row**: хеш от всей строки (ordered, typed, normalized)
```python
row_str = "|".join([str(v) for v in df.loc[idx].values])
hash_row = hashlib.sha256(row_str.encode()).hexdigest()
```

Правила:
- Колонки в фиксированном порядке (`column_order`)
- Типы приведены (float → str с точностью)
- Значения нормализованы (trim, NA policy)
- Всегда SHA256 (64 hex символа)

**Каноническая сериализация для hash_row:**

```python
def canonicalize_row_for_hash(row: dict[str, Any], column_order: list[str]) -> str:
    """
    Каноническая сериализация строки для детерминированного хеширования.
    
    Правила:
    1. JSON с sort_keys=True, separators=(',', ':')
    2. ISO8601 UTC для всех datetime с суффиксом 'Z'
    3. Float формат: %.6f
    4. NA-policy: строки → "", числа → null
    5. Column order: строго по column_order
    """
    from datetime import datetime, timezone
    import json
    import pandas as pd
    
    canonical = {}
    
    for col in column_order:
        value = row.get(col)
        
        # Применяем NA-policy
        if pd.isna(value):
            if isinstance(row.get(col, None), str):
                canonical[col] = ""  # Пустая строка для NA в строковом поле
            else:
                canonical[col] = None  # null для числовых NA
        elif isinstance(value, float):
            canonical[col] = float(f"{value:.6f}")  # Фиксированная точность
        elif isinstance(value, datetime):
            canonical[col] = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(value, (dict, list)):
            canonical[col] = json.loads(json.dumps(value, sort_keys=True))  # Нормализация
        else:
            canonical[col] = value
    
    return json.dumps(canonical, sort_keys=True, ensure_ascii=False)
```

## Manifest & Atomic Write

Схема `meta.yaml` и протокол записи.

### Обязательные поля meta.yaml

```yaml
# meta.yaml
pipeline_version: "2.1.0"
chembl_release: "33"
row_count: 12345
column_count: 42
column_order:
  - "document_chembl_id"
  - "title"
  # ... все колонки
checksums:
  dataset: "sha256:abc123..."
  quality: "sha256:def456..."
git_commit: "a1b2c3d"
generated_at: "2025-01-28T14:23:15.123Z"
lineage:
  source_files:
    - "input/documents.csv"
  transformations:
    - "normalize_titles"
    - "validate_dois"
```

### Протокол Atomic Write

Протокол гарантирует отсутствие partial артефактов:

1. **Запись во временный файл в run-scoped директории**

```python
# Run-scoped temp directory
temp_dir = output_path.parent / f".tmp_run_{run_id}"
temp_dir.mkdir(parents=True, exist_ok=True)
temp_path = temp_dir / f"{output_path.name}.tmp"

# Запись
df.to_csv(temp_path, index=False)
```

2. **Валидация checksums (опционально, для критичных данных)**

```python
checksum = compute_checksum(temp_path)
# Запись checksum в метаданные
```

3. **Атомарный replace**

```python
# os.replace() гарантирует атомарность на POSIX и Windows
output_path.parent.mkdir(parents=True, exist_ok=True)
os.replace(str(temp_path), str(output_path))  # Atomic операция
```

4. **Guaranteed cleanup при любой ошибке**

```python
try:
    # Write + replace
    write_data()
except Exception as e:
    # Cleanup temp files
    temp_path.unlink(missing_ok=True)
    raise
finally:
    # Cleanup temp directory
    if temp_dir.exists():
        for temp_file in temp_dir.glob("*.tmp"):
            temp_file.unlink(missing_ok=True)
        try:
            temp_dir.rmdir()
        except OSError:
            pass
```

**Инвариант атомарности:** Либо все артефакты записаны, либо ни один (нет partial файлов в финальной директории).

### Единый протокол записи (норматив)

**Обязательное требование для всех пайплайнов**: Запись только через run-scoped temp dir; валидация checksum до rename; `os.replace/rename` атомарен; при исключении temp удаляется; запрет частичных артефактов обязателен.

**Все пайплайны обязаны ссылаться на этот раздел и не переопределять поведение.**

**См. также**: [gaps.md](../gaps.md) (G1, G13, G15), [acceptance-criteria.md](../acceptance-criteria.md) (AC1, AC4), [implementation-examples.md](../implementation-examples.md) (патч 5).

### Запрет частичных артефактов

**Критический инвариант:** Частичные артефакты в финальной директории недопустимы.

**Недопустимо:**
- CSV с неполными данными
- `meta.yaml` без checksums или lineage
- Пустые файлы (размер = 0)
- Артефакты без соответствующих QC отчетов (в standard/extended режиме)

**Протокол валидации после записи:**

```python
def validate_artifacts_completeness(artifacts: OutputArtifacts, mode: str) -> None:
    """
    Проверяет полноту всех артефактов после записи.
    
    Raises:
        IOError: при обнаружении частичных артефактов
    """
    # Обязательные артефакты для любого режима
    required = [artifacts.dataset, artifacts.quality_report]
    
    # Extended режим добавляет metadata
    if mode == "extended":
        if artifacts.metadata:
            required.append(artifacts.metadata)
    
    # Проверка существования и размера
    for artifact in required:
        if not artifact.exists():
            raise IOError(f"Partial artifact: {artifact} missing")
        
        if artifact.stat().st_size == 0:
            raise IOError(f"Empty artifact: {artifact}")
    
    # Проверка checksums в meta.yaml (если extended)
    if mode == "extended" and artifacts.metadata:
        import yaml
        meta = yaml.safe_load(artifacts.metadata.read_text())
        if "checksums" not in meta or not meta["checksums"]:
            raise IOError(f"Missing checksums in {artifacts.metadata}")
```

**Acceptance Criteria AC-02:** При вызове этой функции после записи не должно быть частичных артефактов.

## CLI & Scenarios

Интерфейс командной строки и сценарии запуска.

### Флаги

```bash
# Dry run (валидация без записи)
python pipeline.py --dry-run

# Ограничение количества записей
python pipeline.py --limit 1000

# Сэмплирование данных
python pipeline.py --sample 0.1  # 10% данных

# Golden comparison
python pipeline.py --golden data/output/golden/documents_20250115.csv

# Формат вывода
python pipeline.py --output-format parquet  # csv или parquet
```

### Сценарий: Golden Run

Контрольный прогон для проверки детерминизма.

**Этап 1: Создание golden**

```bash
python pipeline.py --input data/input/documents.csv \
                   --output data/output/golden/
```

**Этап 2: Сравнение с golden**

```bash
python pipeline.py --input data/input/documents.csv \
                   --golden data/output/golden/documents_20250115.csv
```

**Результат:**

- При совпадении: `✅ Deterministic output confirmed`
- При расхождении: отчет diff.txt с различиями

**Формат отчета diff.txt:**

```text
Row 42: Column 'title' differs
  golden: "Target Discovery"
  actual: "Target Discovery "  # Extra space

Row 123: Hash mismatch
  golden_hash: "abc123..."
  actual_hash: "def456..."
```

### Сценарий: Sampling для тестов

Быстрая проверка на малой выборке:
```bash
python pipeline.py --sample 0.01 --limit 100  # 1% или 100 строк
```

## Режимы работы

### Correlation Analysis Configuration

**Критическое изменение:** Корреляционный анализ является **опциональным** и по умолчанию **выключен**.

```yaml
# Конфигурация корреляций
postprocess:
  correlation:
    enabled: false  # По умолчанию ВЫКЛЮЧЕН
    steps:
      - name: "correlation_analysis"
        enabled: false
```

**Обоснование:** Корреляции не являются частью ETL-контракта и создают риск недетерминизма из-за численных алгоритмов.

**Включение корреляций:** Только явным флагом `postprocess.correlation.enabled: true` или параметром `--correlation` в CLI.

### Standard (без correlation по умолчанию)

```text
# Создает основные артефакты без correlation:
output.documents_20250128.csv
output.documents_20250128_quality_report_table.csv
```

**При включении correlation (опционально):**
```text
output.documents_20250128_data_correlation_report_table.csv
```

**Инварианты:**
- Checksums стабильны при одинаковом вводе (SHA256)
- Порядок строк фиксирован (deterministic sort)
- Column order из Schema Registry
- NA-policy: `""` для строк, `null` для чисел
- Correlation опциональный (по умолчанию выключен)

### Extended (+ metadata и manifest)

```text
# Добавляет метаданные и manifest:
output.documents_20250128.meta.yaml
reports/run_manifest_20250128142315.json
```

**Инварианты:**
- `meta.yaml` валиден по YAML schema
- `lineage` содержит все трансформации
- `checksums` вычислены для всех артефактов
- `git_commit` присутствует в production
- Нет частичных артефактов (все или ничего)
- `column_order` копируется из Schema Registry (не источник истины)

## Acceptance Criteria

Матрица проверяемых инвариантов для всех пайплайнов:

### AC-01: Golden Compare Детерминизма

**Цель:** Проверка бит-в-бит воспроизводимости вывода.

**Команда:**
```bash
python pipeline.py --input X --golden data/output/golden/Y.csv
```

**Ожидаемый результат:** "✅ Deterministic output confirmed" или пустой `diff.txt`.

**Артефакт:** Отчет `diff.txt` с различиями (если есть).

### AC-02: Запрет Частичных Артефактов

**Цель:** Гарантировать, что все файлы записаны полностью или не записаны вообще.

**Проверка:**
```python
# После write()
for artifact in [artifacts.dataset, artifacts.quality_report, ...]:
    assert artifact.exists(), f"Missing: {artifact}"
    assert artifact.stat().st_size > 0, f"Empty: {artifact}"
```

**Порог:** Нет частичных файлов в финальной директории.

### AC-03: Column Order из Схемы

**Цель:** Гарантировать, что порядок колонок соответствует Schema Registry.

**Проверка:**
```python
assert df.columns.tolist() == schema.column_order
```

**Ожидаемое:** Полное совпадение порядка.

### AC-05: NA-Policy в Сериализации

**Цель:** Проверка применения канонической политики NA.

**Тестовые данные:**
- Строковое NA → `""`
- Числовое NaN → `null` в JSON, пустое в CSV

**Проверка:**
```python
# После канонической сериализации
serialized_row = canonicalize_row_for_hash(row, column_order)
assert '"string_field": ""' in serialized_row  # пустая строка
assert '"numeric_field": null' in serialized_row  # null
```

## Best Practices

1.  **Всегда используйте schema**: валидация перед записью предотвращает ошибки
2.  **Фиксируйте column_order**: для воспроизводимости
3.  **Задавайте key_columns**: для детерминированной сортировки
4.  **Используйте standard mode для production**: уменьшает размер вывода
5.  **Используйте extended mode для debugging**: полные метаданные
6.  **CSV для человекочитаемости**: Parquet для производительности и размера
7.  **Проверяйте artifacts перед использованием**: существование файлов

## Обработка ошибок

```python
try:
    artifacts = writer.write(df, table_name="documents", date_tag="20250128")
except pa.errors.SchemaErrors as e:
    logger.error("Schema validation failed", errors=e.failure_cases)
    raise
except IOError as e:
    logger.error("IO error during write", error=str(e))
    raise
```

## Интеграция с UnifiedLogger

```python
writer = UnifiedOutputWriter(...)

with bind_stage(logger, "write_output"):
    artifacts = writer.write(df, table_name="documents", date_tag="20250128")
    logger.info(
        "Output written",
        artifacts=len(artifacts),
        row_count=len(df),
        checksum=compute_checksum(artifacts.dataset)
    )
```

## Миграция

### Из bioactivity_data_acquisition5

```python
# Было
from library.io.read_write import write_publications
write_publications(df, path)

# Стало
from unified_output import UnifiedOutputWriter
writer = UnifiedOutputWriter()
writer.write(df, table_name="documents", date_tag="20250128")
```

### Из ChEMBL_data_acquisition6

```python
# Было
from library.io.output_writer import save_standard_outputs
save_standard_outputs(df, qc_df, corr_df, "documents", "20250128")

# Стало
from unified_output import UnifiedOutputWriter
writer = UnifiedOutputWriter()  # Автоматически генерирует QC и correlation
writer.write(df, table_name="documents", date_tag="20250128")
```

---

**Следующий раздел**: [03-data-extraction.md](03-data-extraction.md)
