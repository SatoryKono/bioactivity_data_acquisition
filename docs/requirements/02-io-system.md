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

    correlation_report: Path | None  # Корреляционный анализ (опционально)

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

> **Материализация:** `MaterializationManager` всегда использует `UnifiedOutputWriter.atomic_writer` для записи как CSV, так и Parquet. Если пайплайн не передал фабрику `output_writer_factory`, менеджер создаёт `AtomicWriter` на лету, повторно используя текущий `run_id` и `DeterminismConfig`. Это гарантирует единое поведение и одинаковые гарантии атомарности для обоих форматов.

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
        float_format: str = "%.6f",
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

### 7. Run manifest

Run manifest фиксирует состав и контрольные суммы артефактов одного запуска. Файл сохраняется как `run_manifest.json` в каталоге
запуска.

**Структура JSON:**

```json
{
  "run_id": "chembl-targets-2024-02-01",
  "artifacts": {
    "dataset": ".../datasets/targets.csv",
    "quality_report": ".../qc/targets_quality_report.csv",
    "metadata": ".../targets_meta.yaml",
    "qc": {
      "correlation_report": ".../qc/targets_correlation_report.csv",
      "summary_statistics": ".../qc/targets_summary_statistics.csv"
    },
    "additional_datasets": {
      "normalized": {
        "csv": ".../normalized.csv",
        "parquet": ".../normalized.parquet"
      }
    },
    "debug_dataset": ".../targets_debug.json"
  },
  "checksums": {
    "targets.csv": "88ff…",
    "targets_quality_report.csv": "0b1c…"
  },
  "schema": {
    "id": "targets",
    "version": "1.4.0"
  }
}
```

- `run_id` совпадает с идентификатором запуска пайплайна.
- `artifacts` всегда содержит пути к `dataset`, `quality_report`, `metadata`; дополнительные ключи (`qc`, `additional_datasets`,
  `debug_dataset`) появляются только при наличии соответствующих файлов.
- `checksums` — словарь SHA256 по каждому сгенерированному файлу (ключ — имя файла без пути).
- `schema` описывает привязанную Pandera-схему (`null`, если идентификатор/версия отсутствуют).

**Процедура записи:**

```python
manifest_payload = {
    "run_id": run_id,
    "artifacts": artifacts_map,
    "checksums": checksums,
    "schema": {"id": schema_id, "version": schema_version},
}
atomic_writer.write_json(manifest_payload, run_dir / "run_manifest.json")
```

`AtomicWriter.write_json` использует ту же временную директорию, что и запись датасетов, обеспечивая атомарность и восстановление
после сбоев.

## Основной класс: UnifiedOutputWriter

```python

class UnifiedOutputWriter:
    """Универсальный writer для пайплайнов."""

    def __init__(
        self,
        *,
        run_id: str,
        schema: pa.DataFrameModel | None = None,
        column_order: list[str] | None = None,
        key_columns: list[str] | None = None,
        format: str = "csv",  # csv или parquet

        mode: str = "standard",  # standard или extended

        output_dir: Path = Path("data/output")
    ):
        self.run_id = run_id
        self.schema = schema
        self.column_order = column_order
        self.key_columns = key_columns or []
        self.format = format
        self.mode = mode
        self.output_dir = output_dir

        self.quality_generator = QualityReportGenerator()
        self.correlation_generator = CorrelationReportGenerator()
        self.format_handler = FormatHandler()
        self.atomic_writer = AtomicWriter(run_id)
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

        # Условная генерация correlation отчета (только при явном включении)

        if hasattr(self, 'config') and getattr(self.config, 'postprocess', None) and self.config.postprocess.correlation.enabled:
            correlation_df = self.correlation_generator.generate(df)
            if not correlation_df.empty:
                self._write_dataset(correlation_df, artifacts.correlation_report)
            else:
                logger.info("skip_correlation_report", reason="no_numeric_columns")
        else:
            logger.info("skip_correlation_report", reason="disabled_in_config", invariant="determinism")

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
    run_id=generate_run_id(),
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
    run_id=generate_run_id(),
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

**Единый источник:** Подробности политики column_order и NA-policy описаны в [04-normalization-validation.md](04-normalization-validation.md#централизованная-политика-na-policy-и-precision-policy-aud-2) как часть AUD-2 исправления.

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

  #

# Валидация консистентности

column_order_source: "schema_registry"
schema_id: "document.chembl"
schema_version: "2.1.0"

```

**Acceptance Criteria AC-03:** При валидации `df.columns.tolist() == schema.column_order` должно быть истиной.

### Централизованная политика NA/precision {#na-precision-policy}

**Назначение:** единая спецификация обработки пропусков и числовой точности для всех пайплайнов. Политика обязательна к применению в `extract → normalize → validate → write` и контролируется Pandera-схемами.

#### Область действия

- **Источник истины:** Pandera OutputSchema/MetaSchema.
- **Артефакты:** CSV, canonical JSON, QC отчёты, `meta.yaml`, `run_manifest.json`.
- **Контроль:** `UnifiedOutputWriter` + тесты AC-05/AC-10 + `tests/integration/pipelines/test_extended_mode_outputs.py`.

#### NA Policy {#na-policy}

**Инвариант:** тип Pandera определяет представление пропусков. Любые отступления трактуются как нарушение схемы.

| Тип данных Pandera | Значение в DataFrame | CSV/Parquet | Canonical JSON | Применение |
| --- | --- | --- | --- | --- |
| `StringDtype` | `""` (пустая строка) | `""` | `""` | Все текстовые поля, идентификаторы |
| `Int64Dtype`/`Float64Dtype` | `pd.NA` | Пустая ячейка | `null` | Целочисленные и вещественные показатели |
| `BooleanDtype` | `pd.NA` | Пустая ячейка | `null` | Логические флаги |
| `Datetime64[ns, UTC]` | `pd.NaT` | Пустая ячейка | `null` (значения сериализуются в ISO8601) | Временные метки |
| `JSON`/`dict` поля | `None` или `{}` | Canonical JSON | Canonical JSON | Вложенные структуры |

```python
string_columns = {
    name
    for name, field in schema.__fields__.items()
    if pd.api.types.is_string_dtype(field.dtype)
}
df[string_columns] = df[string_columns].fillna("")

non_string = df.columns.difference(string_columns)
df[non_string] = df[non_string].astype(schema.to_schema().dtype)
```

**Ключевые проверки:**

- `schema.validate(..., lazy=True)` обеспечивает наличие nullable типов.
- Snapshot тесты (`tests/golden/**`) фиксируют, что `""` используется только для строк.
- `meta.yaml` копирует NA-policy и precision-policy для аудита (AUD-2).

#### Каноническая сериализация

- JSON: `sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`.
- Даты: `datetime.isoformat()` в UTC.
- Коллекции: `json.dumps(value, sort_keys=True, separators=(",", ":"))`.
- Хеширование использует нормализованную проекцию (`hash_row`).

#### Precision Policy {#precision-policy}

**Инвариант:** числовые поля сериализуются детерминированно по карте точности.

| Категория полей | Формат | Примеры | Обоснование |
| --- | --- | --- | --- |
| Фармакологические метрики | `%.6f` | `standard_value`, `pic50`, `activity_value` | Научная точность, стабильные сравнения |
| Логарифмические показатели | `%.2f` | `pchembl_value`, `selectivity_score` | Совместимость с отчётами ChEMBL |
| Проценты/доли | `%.4f` | `missing_fraction`, `duplicate_ratio` | Читаемость QC |
| Прочие float поля | `%.6f` | `molecular_weight`, `confidence_score` | Единый default |

**Применение:**

- `schema.field_precision` → `PrecisionFormatter` в `UnifiedOutputWriter`.
- Тесты `tests/unit/test_output_writer.py::test_unified_output_writer_writes_extended_metadata` проверяют точность.
- Любое новое поле требует явного precision в схеме или документа валидации (AUD-3).

**См. также:** [00-architecture-overview.md](00-architecture-overview.md#инварианты-канонизации) для общей канонической сериализации.

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

> **Determinism guarantee**: ``finalize_pipeline_output`` теперь использует ``pandas.util.hash_pandas_object`` поверх отсортированной проекции колонок для вычисления стабильного базового значения, которое затем хешируется в SHA256.  Такой подход устраняет зависимость от ``DataFrame.apply(axis=1)`` и гарантирует идентичные ``hash_row`` значения при повторных запусках на любых платформах при условии одинакового содержимого таблицы.

Правила:

- Колонки в фиксированном порядке (`column_order`)

- Типы приведены (float → str с точностью)

- Значения нормализованы (trim, NA policy)

- Всегда SHA256 (64 hex символа)

**Каноническая сериализация для hash_row:**

```python

def canonicalize_row_for_hash(
    row: dict[str, Any],
    column_order: list[str],
    *,
    string_columns: Collection[str],
) -> str:
    """
    Каноническая сериализация строки для детерминированного хеширования.

    Правила:
    1. JSON с sort_keys=True, separators=(',', ':')
    2. ISO8601 UTC для всех datetime с суффиксом 'Z'
    3. Float формат: %.6f
    4. NA-policy: строковые → "", остальные → None
    5. Column order: строго по column_order
    """
    from collections.abc import Collection
    from datetime import datetime, timezone
    import json
    import pandas as pd

    canonical = {}

    for col in column_order:
        value = row.get(col)

        # Применяем NA-policy: строковые → "", остальные → None

        if pd.isna(value):
            canonical[col] = "" if col in string_columns else None
            continue

        elif isinstance(value, float):
            canonical[col] = float(f"{value:.6f}")  # Фиксированная точность

        elif isinstance(value, datetime):
            canonical[col] = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(value, (dict, list)):
            canonical[col] = json.loads(json.dumps(value, sort_keys=True))  # Нормализация

        else:
            canonical[col] = value

    return json.dumps(canonical, sort_keys=True, ensure_ascii=False)

# Использование (continued 1)

STRING_COLUMNS = {
    name
    for name, field in schema.fields.items()
    if pd.api.types.is_string_dtype(field.dtype)
}
canonicalize_row_for_hash(row, schema.column_order, string_columns=STRING_COLUMNS)

# Здесь `schema` — Pandera DataFrameModel; `string_columns` используется при вычислении `hash_row`

```

## Manifest & Atomic Write

Схема `meta.yaml` и протокол записи.

### Обязательные поля meta.yaml

```yaml

# meta.yaml

run_id: "abc123"
pipeline_version: "2.1.0"
config_hash: "sha256:deadbeef..."
config_snapshot:
  path: "configs/pipelines/document.yaml"
  sha256: "sha256:d1c2..."
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
  correlation: "sha256:ghi789..."  # Опционально, только если correlation enabled
checksum_algorithm: "sha256"
quantitative_metrics:
  row_count: 12345
  column_count: 42
  duplicate_rows: 12
stage_durations:
  extract: 1.23
  transform: 0.87
  validate: 0.44
  load: 2.65
sort_keys: []
sort_directions: []
pii_secrets_policy:
  pii_expected: false
  pii_controls: "Derived from public ChEMBL records; personal identifiers are removed upstream."
  secret_management: "API credentials via env / secrets manager"
  pii_review: "Schema registry audits enforce absence of personal identifiers."
git_commit: "a1b2c3d"
generated_at: "2025-01-28T14:23:15.123Z"
lineage:
  source_files:

    - "input/documents.csv"

  transformations:

    - "normalize_titles"
    - "validate_dois"

```

**Обязательные поля lineage конфигурации:**

- `run_id`: уникальный идентификатор запуска (UUID8 или timestamp-based)
- `config_hash`: SHA256 хеш конфигурации (после резолва переменных окружения)
- `config_snapshot`: путь и хеш исходного файла конфигурации
- `checksum_algorithm`: алгоритм, которым рассчитаны `file_checksums`
- `quantitative_metrics`: числовые показатели качества (объём, дубликаты, пропуски)
- `stage_durations`: длительность стадий ETL (extract/transform/validate/load)
- `sort_keys`/`sort_directions`: применённая детерминированная сортировка
- `pii_secrets_policy`: политика обращения с PII и секретами для трассируемости аудита

**Обоснование:** Обеспечивает полную воспроизводимость и аудит запусков, закрывает требование R2 из gap-анализа.

**См. также**: [09-document-chembl-extraction.md](09-document-chembl-extraction.md) — примеры использования `config_hash`.

### Протокол Atomic Write

Протокол гарантирует отсутствие partial артефактов:

1. **Запись во временный файл в run-scoped директории**

```python

# Run-scoped temp directory (continued 1)

temp_dir = output_path.parent / f".tmp_run_{run_id}"
temp_dir.mkdir(parents=True, exist_ok=True)
temp_path = temp_dir / f"{output_path.name}.tmp"

# Запись

df.to_csv(temp_path, index=False)

```

1. **Валидация checksums (опционально, для критичных данных)**

```python

checksum = compute_checksum(temp_path)

# Запись checksum в метаданные

```

1. **Атомарный replace**

```python

# os.replace() гарантирует атомарность на POSIX и Windows

output_path.parent.mkdir(parents=True, exist_ok=True)
os.replace(str(temp_path), str(output_path))  # Atomic операция

```

1. **Guaranteed cleanup при любой ошибке**

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

### Этап 1: Создание golden

```bash

python pipeline.py --input data/input/documents.csv \
                   --output data/output/golden/

```

### Этап 2: Сравнение с golden

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

### Репозиторий golden-артефактов

- Каноничные результаты golden-тестов хранятся в `artifacts/baselines/golden_tests/`. В каталоге лежат журналы запуска pytest и итоговый отчёт покрытия (`coverage_summary.txt`), подтверждающие, что текущая версия пайплайна детерминирована и проходит smoke/golden сценарии без расхождений.
- При обновлении эталонов выполняйте golden-прогон локально: `pytest tests/golden/ -v -m golden --cov=src/bioetl --cov-report=term | tee artifacts/baselines/golden_tests/pytest_integration.log`. После завершения сохраните текстовый отчёт покрытия с помощью `coverage report > artifacts/baselines/golden_tests/coverage_summary.txt`.
- Перед коммитом убедитесь, что обновлённые файлы в каталоге `artifacts/baselines/golden_tests/` отражают фактический прогон (включая дату, контрольные значения покрытия и ключевые логи), затем повторно запустите `pytest tests/golden/ -v -m golden` для валидации.

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

# Создает основные артефакты без correlation

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

# Добавляет метаданные и manifest

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

**Момент проверки:** Проверка выполняется **после** Pandera валидации схемы, **перед** атомарной записью.

**Проверка:**

```python

# После Pandera validation

df = schema.validate(df, lazy=True)

# Применяем column_order из схемы (источник истины)

df = df[schema.column_order]

# Проверка перед записью

assert list(df.columns) == schema.column_order

```

**Ожидаемое:** Полное совпадение порядка колонок.

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

1. **Всегда используйте schema**: валидация перед записью предотвращает ошибки

1. **Фиксируйте column_order**: для воспроизводимости

1. **Задавайте key_columns**: для детерминированной сортировки

1. **Используйте standard mode для production**: уменьшает размер вывода

1. **Используйте extended mode для debugging**: полные метаданные

1. **CSV для человекочитаемости**: Parquet для производительности и размера

1. **Проверяйте artifacts перед использованием**: существование файлов

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

# Было (continued 1)

from library.io.output_writer import save_standard_outputs
save_standard_outputs(df, qc_df, corr_df, "documents", "20250128")

# Стало (continued 1)

from unified_output import UnifiedOutputWriter

# Standard режим: dataset + quality_report

writer = UnifiedOutputWriter(
    run_id=generate_run_id(),
    schema=DocumentSchema,
    mode="standard"
)
artifacts = writer.write(df, table_name="documents", date_tag="20250128")

# artifacts.correlation_report будет None, если postprocess.correlation.enabled=false

# Для включения correlation

# В конфигурации: postprocess.correlation.enabled: true

# Или через CLI: --set postprocess.correlation.enabled=true

```

**Важно**: UnifiedOutputWriter генерирует correlation отчёт только при явном `config.postprocess.correlation.enabled=true`. По умолчанию создаются только 2 файла (dataset + quality_report), что обеспечивает детерминизм и минимальный AC-профиль.

---

**Следующий раздел**: [03-data-extraction.md](03-data-extraction.md)

