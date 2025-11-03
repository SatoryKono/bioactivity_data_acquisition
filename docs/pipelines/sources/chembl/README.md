# ChEMBL pipelines

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Maintainers

- `@SatoryKono` — глобальный код-оунер и ответственный за документацию репозитория, поэтому он утверждает изменения по пайплайнам ChEMBL и сопутствующим артефактам.【F:.github/CODEOWNERS†L5-L41】

## Требования к содержанию карточки источника

- `MODULE_RULES.md` предписывает, что для каждого источника в `docs/pipelines/sources/<source>/README.md` необходимо фиксировать публичный API, ключи конфигурации, merge-policy и артефакты тестирования, поддерживая структуру модульных слоёв.【F:refactoring/MODULE_RULES.md†L3-L42】【F:refactoring/MODULE_RULES.md†L146-L149】
- Стандарт «карточки источника» из `DATA_SOURCES.md` требует указывать каталог источника, публичный API, `config_keys` и merge-политику — эти поля считаются обязательными при изменении пайплайна.【F:refactoring/DATA_SOURCES.md†L63-L76】

## Public API

- `bioetl.pipelines.chembl_activity.ActivityPipeline` — основной ETL по активности ChEMBL (загрузка `/activity`, управление fallback и валидацией по `ActivitySchema`).【F:src/bioetl/pipelines/chembl_activity.py†L1-L210】
- `bioetl.pipelines.chembl_assay.AssayPipeline` — извлечение ассайев ChEMBL с учётом лимитов URL, кешей и статистики fallback.【F:src/bioetl/pipelines/chembl_assay.py†L1-L210】
- `bioetl.sources.chembl.document.pipeline.DocumentPipeline` — выгрузка документов ChEMBL и оркестрация обогащения PubMed/Crossref/OpenAlex/Semantic Scholar по режимам `chembl`/`all`.【F:src/bioetl/sources/chembl/document/pipeline.py†L66-L140】
- `bioetl.sources.chembl.target.pipeline.TargetPipeline` — многостадийный таргет-пайплайн: ChEMBL → UniProt → IUPHAR + постобработка в `target_gold`.

【F:src/bioetl/sources/chembl/target/pipeline.py†L69-L160】

- `bioetl.sources.chembl.testitem.pipeline.TestItemPipeline` — загрузка молекул (test items) с полями из `/molecule` и поддержкой PubChem-обогащения.【F:src/bioetl/sources/chembl/testitem/pipeline.py†L59-L139】

## Module layout

- Код пайплайнов `activity` и `assay` перенесён в модули `src/bioetl/pipelines/chembl_<entity>.py`, а в `src/bioetl/sources/chembl/<entity>/pipeline.py` остались прокси-импорты для обратной совместимости CLI и тестов.【F:src/bioetl/pipelines/chembl_activity.py†L1-L210】【F:src/bioetl/pipelines/chembl_assay.py†L1-L210】

## Configuration keys

- Общие параметры ChEMBL берутся из include `configs/includes/chembl_source.yaml`: `base_url`, `batch_size`, `max_url_length`, заголовки и флаг `rate_limit_jitter`. Эти ключи наследуются всеми профильными конфигами ChEMBL.【F:src/bioetl/configs/includes/chembl_source.yaml†L1-L11】
- `configs/pipelines/activity.yaml` задаёт собственный `batch_size`, заголовок `User-Agent` и QC-политику дубликатов/единиц; сортировка фиксирована по `activity_id`.【F:src/bioetl/configs/pipelines/activity.yaml†L1-L86】
- `configs/pipelines/assay.yaml` использует такой же include, но переопределяет сортировку (`assay_chembl_id`, `row_subtype`) и ключевые поля BAO для записи; QC следит за `fallback_usage_rate`.【F:src/bioetl/configs/pipelines/assay.yaml†L1-L95】
- `configs/pipelines/document.yaml` подключает внешние источники (`sources.pubmed`, `sources.crossref`, `sources.openalex`, `sources.semantic_scholar`) с их лимитами, переменными окружения (`PUBMED_TOOL`, `PUBMED_EMAIL`, `CROSSREF_MAILTO`, `SEMANTIC_SCHOLAR_API_KEY`) и расширенным QC по покрытию DOI/PMID и конфликтам заголовков.【F:src/bioetl/configs/pipelines/document.yaml†L1-L194】
- `configs/pipelines/target.yaml` объявляет профили HTTP/источников для ChEMBL, UniProt (поиск, idmapping, orthologs) и IUPHAR; QC контролирует успех обогащений и fallback-стратегии.【F:src/bioetl/configs/pipelines/target.yaml†L1-L200】
- `configs/pipelines/testitem.yaml` описывает postprocess-обогащение `pubchem_dataset` и фиксирует порядок колонок для выдачи ChEMBL + PubChem, а QC отслеживает дубликаты и долю fallback.【F:src/bioetl/configs/pipelines/testitem.yaml†L1-L151】

## Merge policy highlights

- Документы: консолидация DOI/PMID/метаданных в порядке Crossref → PubMed → OpenAlex → ChEMBL с фиксацией источника в `*_source` колонках; реализация опирается на приоритеты из `FIELD_PRECEDENCE` и merge-хелперы.【F:refactoring/DATA_SOURCES.md†L33-L39】【F:src/bioetl/sources/document/merge/policy.py†L17-L200】
- Таргеты: ChEMBL даёт каркас, UniProt побеждает по именам/генам, IUPHAR — по фармакологической классификации; итоговая политика описана в матрице источников.【F:refactoring/DATA_SOURCES.md†L35-L37】
- Ассайы: конфликтующие тип/формат нормализуются через BAO, приоритет за словарём BAO относительно raw ChEMBL.【F:refactoring/DATA_SOURCES.md†L37-L38】
- Test items: поля идентичности и синонимы берутся из PubChem, ChEMBL служит fallback; расхождения помечаются для QC.【F:refactoring/DATA_SOURCES.md†L38-L39】
- Активности: ChEMBL — единственный источник, ключ — комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, `value`, `units`) с выбором записи по корректности единиц.【F:refactoring/DATA_SOURCES.md†L39-L39】

## Tests

- `tests/sources/chembl/test_client.py`, `test_parser.py`, `test_normalizer.py`, `test_schema.py`, `test_pipeline_e2e.py` покрывают клиентские хелперы, парсинг, нормализацию и контроль колонок Activity/Assay/TestItem.【F:tests/sources/chembl/test_client.py†L1-L23】【F:tests/sources/chembl/test_pipeline_e2e.py†L1-L12】
- `tests/unit/test_utils_chembl.py` проверяет утилиты и защиту от регрессий в общих функциях ChEMBL.【F:tests/unit/test_utils_chembl.py†L1-L44】
