# Публичные контракты пайплайнов {#pipelines-contracts}

## Интерфейс стадий {#stage-interface}
| Метод | Вход | Выход | Инварианты |
| --- | --- | --- | --- |
| `extract(input_file: Path | None)` | CSV из `data/input/*.csv` или путь из CLI | `pd.DataFrame` | MUST вызывать `read_input_table()` для детерминизма ([ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]). |
| `normalize(df)` / `enrich(df)` | DataFrame после extract | DataFrame | SHOULD возвращать столбцы в порядке `determinism.column_order`. |
| `validate(df)` | DataFrame после normalize | None | MUST валидировать Pandera-схемами и накапливать `validation_issues`. |
| `write(df)` | Проверенный DataFrame | `OutputArtifacts` | MUST использовать `OutputWriter` и соблюдать `SortConfig`. |
| `run()` | Конфиг, run_id | Результаты материализации | Оборачивает стадии, обновляет QC и метаданные. |

## Минимальный пример конфига {#config-example}

```yaml
extends:
  - ../base.yaml
  - ../includes/determinism.yaml
pipeline:
  name: activity
  entity: activity
sources:
  chembl:
    batch_size: 20
```
Конфигурация валидируется моделью
[`PipelineConfig`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32],
неизвестные ключи запрещены.

## Activity {#pipeline-activity}
- Вход: `activity.csv` со списком `activity_id`.
- Выход: основной CSV, QC JSON, fallback таблица (если применимо).
- Бизнес-ключи: поля из
  [`ACTIVITY_FALLBACK_BUSINESS_COLUMNS`][ref: repo:src/bioetl/sources/chembl/activity/parser/activity_parser.py@test_refactoring_32].
- Дополнительные таблицы: `activity_fallback.csv` через
  [`ActivityOutputWriter`][ref: repo:src/bioetl/sources/chembl/activity/output/activity_output.py@test_refactoring_32].
- CLI: `python -m bioetl.cli.main activity --config ...`
  ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).
- Тесты покрытия: unit и integration в
  [`tests/unit/test_pipelines.py`][ref: repo:tests/unit/test_pipelines.py@test_refactoring_32]
  и [`tests/integration/pipelines/test_activity_pipeline.py`][ref: repo:tests/integration/pipelines/test_activity_pipeline.py@test_refactoring_32].

## Assay {#pipeline-assay}
- Вход: `assay.csv` с `assay_chembl_id`.
- Нормализация: комбинирует ChEMBL payload и fallback из
  [`AssayMergeService`][ref: repo:src/bioetl/sources/chembl/assay/merge/assay_merge.py@test_refactoring_32].
- QC: `duplicates` MUST быть 0 согласно конфигу
  [`assay.yaml`][ref: repo:src/bioetl/configs/pipelines/assay.yaml@test_refactoring_32].
- CLI: `python -m bioetl.cli.main assay ...`.
- Дополнительно: материализует отчёт о пропусках `qc_assay_missing.csv`.

## Target {#pipeline-target}
- Вход: `target.csv` со столбцами `target_chembl_id`, `uniprot_accession?`.
- Стадии обогащения:
  - `uniprot` — добавляет белковую информацию через
    [`TargetPipeline._enrich_uniprot`][ref: repo:src/bioetl/sources/chembl/target/pipeline.py@test_refactoring_32].
  - `iuphar` — добавляет Guide to Pharmacology данные.
- Выходы: основной CSV, `target_uniprot.csv`, `target_iuphar.csv`.
- Бизнес-ключ: `target_chembl_id` + `accession` (если обогащено).
- Доп. инвариант: gold-таблица MUST быть отсортирована по `target_chembl_id`.

## Document {#pipeline-document}
- Поддерживает режимы (`extended`, `baseline`, `enrichment-only`) через CLI
  ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).
- Extract: читает `document.csv` и обращается к ChEMBL в
  [`DocumentPipeline.extract`][ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32].
- Enrich: стадии `pubmed`, `crossref`, `openalex`, `semantic_scholar` из
  [`DocumentPipeline._prepare_enrichment_adapters`][ref: repo:src/bioetl/sources/chembl/document/pipeline.py@test_refactoring_32].
- Validate: применяет `DocumentSchema` и проверяет полноту DOI/PMID.
- Write: материализует `document.csv`, `document_conflicts.csv`, `document_qc.json`.
- Инварианты:
  - DOI/PMID coverage MUST соответствовать порогам
    [`qc.thresholds`][ref: repo:src/bioetl/configs/pipelines/document.yaml@test_refactoring_32].
  - При несовпадении данных внешних источников соответствующие `*_source` поля MUST
    фиксировать источник.

## TestItem {#pipeline-testitem}
- Вход: `testitem.csv` с `chembl_id`.
- Extract: вызывает ChEMBL `/molecule.json`.
- Enrich: PubChem свойства через
  [`TestItemPipeline._enrich_pubchem`][ref: repo:src/bioetl/sources/chembl/testitem/pipeline.py@test_refactoring_32].
- Validate: `TestItemSchema` из
  [`src/bioetl/schemas/testitem.py`][ref: repo:src/bioetl/schemas/testitem.py@test_refactoring_32].
- Write: основной CSV + `testitem_properties.json`.

## Standalone энричеры {#pipeline-standalone}
### PubChem {#pipeline-pubchem}
- CLI: `python -m bioetl.cli.main pubchem`.
- Extract: CID из CSV, запросы через
  [`PubChemClient`][ref: repo:src/bioetl/sources/pubchem/client.py@test_refactoring_32].
- Validate: Pandera схема `PubChemSchema` (см.
  [`tests/sources/pubchem/test_pipeline_e2e.py`][ref: repo:tests/sources/pubchem/test_pipeline_e2e.py@test_refactoring_32]).

### UniProt {#pipeline-uniprot}
- Extract: `UniProtClient.iter_query`.
- Нормализация: маппинг полей в `UniProtPipeline._normalize_entry`.
- Выход: `uniprot.csv` + QC метрики.

### GtP IUPHAR {#pipeline-iuphar}
- Требует `IUPHAR_API_KEY` (валидация в
  [`Source.resolve_contact_secrets`][ref: repo:src/bioetl/configs/models.py@test_refactoring_32]).
- Валидация: Pandera схема `IupharSchema`.
- Write: `gtp_iuphar.csv`.

## Инварианты QC {#pipeline-qc}
- Каждый пайплайн MUST вызывать `update_summary_metrics` перед финализацией
  ([ref: repo:src/bioetl/utils/qc.py@test_refactoring_32]).
- Дополнительные таблицы SHOULD документироваться в `OutputMetadata.additional_tables`.
- При ошибках схемы пайплайн MUST поднимать исключение `SchemaValidationError`
  ([ref: repo:src/bioetl/utils/validation.py@test_refactoring_32]).
