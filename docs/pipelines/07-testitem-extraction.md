# ChEMBL TestItem Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document specifies the `testitem` pipeline, which extracts molecule data from the ChEMBL API and optionally enriches it with data from PubChem.

## 1. Overview

The `testitem` pipeline is responsible for fetching detailed information about chemical compounds (molecules) from the ChEMBL database. It flattens nested structures from the ChEMBL API response to create a comprehensive, flat record for each molecule.

-   **Primary Source**: ChEMBL API `/molecule.json` endpoint.
-   **Optional Enrichment**: PubChem PUG REST API for additional properties and identifiers.

## 2. CLI Command

The pipeline is executed via the `testitem` CLI command.

**Usage:**
```bash
python -m bioetl.cli.main testitem [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main testitem \
  --config configs/pipelines/chembl/testitem.yaml \
  --output-dir data/output/testitem
```

## 3. ChEMBL Data Extraction

### 3.1. Batch Extraction

The pipeline extracts data in batches to comply with the ChEMBL API's URL length limitations.
-   **Endpoint**: `/molecule.json?molecule_chembl_id__in={ids}`
-   **Batch Size**: Configurable via `sources.chembl.batch_size`, typically `25`.

### 3.2. Field Extraction and Flattening

The pipeline extracts over 80 fields and flattens several nested JSON structures from the ChEMBL response:
-   **`molecule_hierarchy`**: Flattened to `parent_chembl_id` and `parent_molregno`.
-   **`molecule_properties`**: Flattened into ~22 distinct physicochemical properties (e.g., `mw_freebase`, `alogp`, `hba`).
-   **`molecule_structures`**: Flattened to `canonical_smiles`, `standard_inchi`, and `standard_inchi_key`.
-   **`molecule_synonyms`**: Aggregated into an `all_names` field (for search) and also preserved as a JSON string.
-   Other nested objects like `atc_classifications` and `cross_references` are stored as canonical JSON strings.

## 4. PubChem Enrichment (Optional)

The pipeline can optionally enrich the ChEMBL data with information from PubChem. This feature is controlled by the `sources.pubchem.enabled` flag in the configuration.

### 4.1. CID Resolution

The core of the enrichment process is resolving a PubChem Compound ID (CID) for each molecule. A cascaded strategy is used, prioritizing the most reliable methods first:
1.  **Cache Lookup**: Check a persistent local cache for a known CID.
2.  **Direct CID**: Use a CID if already present in ChEMBL cross-references.
3.  **InChIKey Lookup**: Use the molecule's `standard_inchi_key`. This is the most reliable method.
4.  **SMILES Lookup**: Use the `canonical_smiles` as a fallback.
5.  **Name Lookup**: Use the `pref_name` as a last resort.

### 4.2. Batch Property Fetching

Once CIDs are resolved, the pipeline fetches properties from PubChem in batches (typically 100 CIDs per request) for efficiency.
-   **Endpoint**: `/compound/cid/{cids}/property/{properties}/JSON`
-   **Properties**: `MolecularFormula`, `MolecularWeight`, `CanonicalSMILES`, `InChIKey`, etc.

### 4.3. Caching and Resilience

-   **Multi-Level Caching**: A combination of an in-memory TTL cache and a persistent file-based cache is used to minimize redundant API calls.
-   **Graceful Degradation**: The entire PubChem enrichment process is designed to be optional. Any failure in fetching data from PubChem will be logged, but it will **not** stop the main ChEMBL pipeline from completing.

## 5. Component Architecture

| Component | Implementation |
|---|---|
| **Client** | `[ref: repo:src/bioetl/sources/chembl/testitem/client/testitem_client.py@refactoring_001]` |
| **Parser** | `[ref: repo:src/bioetl/sources/chembl/testitem/parser/testitem_parser.py@refactoring_001]` |
| **Normalizer** | `[ref: repo:src/bioetl/sources/chembl/testitem/normalizer/testitem_normalizer.py@refactoring_001]` |
| **Schema** | `[ref: repo:src/bioetl/schemas/chembl_testitem.py@refactoring_001]` |

## 6. Key Identifiers

-   **Business Key**: `molecule_chembl_id`
-   **Sort Key**: `molecule_chembl_id`

## 7. Детерминизм

**Sort keys:** `["testitem_id"]`

TestItem pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются по `testitem_id` (или `molecule_chembl_id`) перед записью
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`)
  - `hash_business_key`: хеш бизнес-ключа (`molecule_chembl_id`)
- **Canonicalization:** Все значения нормализуются перед хешированием (trim whitespace, lowercase identifiers, fixed precision numbers, UTC timestamps)
- **Column order:** Фиксированный порядок колонок из Pandera схемы
- **Meta.yaml:** Содержит `pipeline_version`, `chembl_release`, `row_count`, checksums, `hash_algo`, `hash_policy_version`

**Guarantees:**
- Бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации
- Стабильный порядок строк и колонок
- Идентичные хеши для идентичных данных

For detailed policy, see [Determinism Policy](docs/determinism/01-determinism-policy.md).

## 8. QC/QA

**Ключевые метрики успеха:**

| Метрика | TestItem | Критичность |
|---------|----------|-------------|
| **ChEMBL coverage** | 100% идентификаторов | HIGH |
| **PubChem enrichment rate** | ≥70% для молекул с InChIKey | HIGH |
| **SMILES validity** | ≥95% валидных SMILES | MEDIUM |
| **InChI validity** | ≥95% валидных InChI | MEDIUM |
| **Property completeness** | ≥85% молекул с полным набором свойств | MEDIUM |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**
- Покрытие ChEMBL: процент успешно извлеченных molecule_chembl_id
- Покрытие PubChem: процент молекул с успешным обогащением через PubChem
- Валидность SMILES: процент валидных SMILES строк
- Валидность InChI: процент валидных InChI строк
- Полнота свойств: процент молекул с полным набором физико-химических свойств
- Валидность данных: соответствие схеме Pandera и референциальная целостность

**Пороги качества:**
- ChEMBL coverage должен быть 100% (критично)
- PubChem enrichment rate ≥70% для молекул с InChIKey (высокий приоритет)
- SMILES validity ≥95% (средний приоритет)
- InChI validity ≥95% (средний приоритет)

**QC отчеты:**
- Генерируется `testitem_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с распределениями

For detailed QC metrics and policies, see [QC Overview](docs/qc/00-qc-overview.md).

## 9. Логирование и трассировка

TestItem pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**
- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`testitem`)
- `duration`: Время выполнения стадии в секундах
- `row_count`: Количество обработанных строк

**Структурированные события:**
- `pipeline_started`: Начало выполнения пайплайна
- `extract_started`: Начало стадии извлечения
- `extract_completed`: Завершение стадии извлечения с метриками
- `transform_started`: Начало стадии трансформации
- `transform_completed`: Завершение стадии трансформации
- `validate_started`: Начало валидации
- `validate_completed`: Завершение валидации
- `write_started`: Начало записи результатов
- `write_completed`: Завершение записи результатов
- `pipeline_completed`: Успешное завершение пайплайна
- `pipeline_failed`: Ошибка выполнения с деталями

**Примеры JSON-логов:**

```json
{
  "event": "pipeline_started",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "testitem",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "testitem",
  "duration": 45.2,
  "row_count": 1250,
  "pubchem_enrichment_rate": 72.5,
  "smiles_validity": 96.2,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "testitem",
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
- PubChem enrichment операции логируются с метриками успешности и покрытия

For detailed logging configuration and API, see [Logging Overview](docs/logging/00-overview.md).