# ChEMBL Target Extraction Pipeline

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

This document describes the `target` pipeline, which is responsible for extracting and processing target data from the ChEMBL database.

## 1. Overview

The `target` pipeline extracts information about macromolecular targets of bioactive compounds. This data is essential for understanding drug-target interactions and mechanisms of action. The pipeline enriches the core ChEMBL target data with information from external sources like UniProt and IUPHAR/BPS Guide to PHARMACOLOGY.

## 2. CLI Command

The pipeline is executed via the `target` CLI command.

**Usage:**
```bash
python -m bioetl.cli.main target [OPTIONS]
```

**Example:**
```bash
python -m bioetl.cli.main target \
  --config configs/pipelines/chembl/target.yaml \
  --output-dir data/output/target
```

## 3. Configuration

The pipeline's behavior is controlled by a YAML configuration file, typically located at `configs/pipelines/chembl/target.yaml`. This file specifies the data sources, extraction parameters, and enrichment options.

-   **Primary Source**: ChEMBL API `/target.json` endpoint.
-   **Enrichment Sources**: UniProt, IUPHAR.

## 4. Component Architecture

The `target` pipeline follows the standard source architecture, utilizing a stack of specialized components for its operation.

| Component | Implementation |
|---|---|
| **Client** | `[ref: repo:src/bioetl/sources/chembl/target/client/target_client.py@refactoring_001]` |
| **Parser** | `[ref: repo:src/bioetl/sources/chembl/target/parser/target_parser.py@refactoring_001]` |
| **Normalizer** | `[ref: repo:src/bioetl/sources/chembl/target/normalizer/target_normalizer.py@refactoring_001]` |
| **Schema** | `[ref: repo:src/bioetl/schemas/chembl_target.py@refactoring_001]` |

## 5. Key Identifiers

-   **Business Key**: `target_chembl_id`
-   **Sort Key**: `target_chembl_id`

## 6. Детерминизм

**Sort keys:** `["target_id"]`

Target pipeline обеспечивает детерминированный вывод через стабильную сортировку и хеширование:

- **Sort keys:** Строки сортируются по `target_id` (или `target_chembl_id`) перед записью
- **Hash policy:** Используется SHA256 для генерации `hash_row` и `hash_business_key`
  - `hash_row`: хеш всей строки (кроме полей `generated_at`, `run_id`)
  - `hash_business_key`: хеш бизнес-ключа (`target_chembl_id`)
- **Canonicalization:** Все значения нормализуются перед хешированием (trim whitespace, lowercase identifiers, fixed precision numbers, UTC timestamps)
- **Column order:** Фиксированный порядок колонок из Pandera схемы
- **Meta.yaml:** Содержит `pipeline_version`, `chembl_release`, `row_count`, checksums, `hash_algo`, `hash_policy_version`

**Guarantees:**
- Бит-в-бит воспроизводимость при одинаковых входных данных и конфигурации
- Стабильный порядок строк и колонок
- Идентичные хеши для идентичных данных

For detailed policy, see [Determinism Policy](docs/determinism/01-determinism-policy.md).

## 7. QC/QA

**Ключевые метрики успеха:**

| Метрика | Target | Критичность |
|---------|--------|-------------|
| **ChEMBL coverage** | 100% идентификаторов | HIGH |
| **UniProt enrichment rate** | ≥80% для protein targets | HIGH |
| **IUPHAR coverage** | ≥60% для receptors | MEDIUM |
| **Accession resolution** | ≥90% согласованность | HIGH |
| **Component completeness** | ≥85% последовательностей | MEDIUM |
| **Pipeline failure rate** | 0% (graceful degradation) | CRITICAL |
| **Детерминизм** | Бит-в-бит воспроизводимость | CRITICAL |

**QC метрики:**
- Покрытие ChEMBL: процент успешно извлеченных target_chembl_id
- Покрытие UniProt: процент protein targets с успешным обогащением через UniProt
- Покрытие IUPHAR: процент targets с фармакологической классификацией
- Разрешение accession: согласованность между ChEMBL и UniProt accession
- Полнота компонентов: процент targets с полными последовательностями
- Валидность данных: соответствие схеме Pandera и референциальная целостность

**Пороги качества:**
- ChEMBL coverage должен быть 100% (критично)
- UniProt enrichment rate ≥80% для protein targets (высокий приоритет)
- IUPHAR coverage ≥60% для receptors (средний приоритет)
- Accession resolution ≥90% (высокий приоритет)

**QC отчеты:**
- Генерируется `target_quality_report.csv` с метриками покрытия и валидности
- При использовании `--extended` режима дополнительно создается подробный отчет с распределениями

For detailed QC metrics and policies, see [QC Overview](docs/qc/00-qc-overview.md).

## 8. Логирование и трассировка

Target pipeline использует `UnifiedLogger` для структурированного логирования всех операций с обязательными полями контекста.

**Обязательные поля в логах:**
- `run_id`: Уникальный идентификатор запуска пайплайна
- `stage`: Текущая стадия выполнения (`extract`, `transform`, `validate`, `write`)
- `pipeline`: Имя пайплайна (`target`)
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
  "pipeline": "target",
  "timestamp": "2025-01-15T10:30:00.123456Z"
}

{
  "event": "extract_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "extract",
  "pipeline": "target",
  "duration": 45.2,
  "row_count": 1250,
  "uniprot_enrichment_rate": 82.5,
  "iuphar_coverage": 65.3,
  "timestamp": "2025-01-15T10:30:45.345678Z"
}

{
  "event": "pipeline_completed",
  "run_id": "a1b2c3d4e5f6g7h8",
  "stage": "bootstrap",
  "pipeline": "target",
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

For detailed logging configuration and API, see [Logging Overview](docs/logging/00-overview.md).