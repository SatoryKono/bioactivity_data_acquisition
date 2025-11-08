# AUDIT_FILETREE

## Дерево директорий

```
_audit_src_extract/
└── src
    └── bioetl
        ├── cli
        │   ├── tools
        │   │   ├── __init__.py
        │   │   ├── audit_docs.py
        │   │   ├── build_vocab_store.py
        │   │   ├── catalog_code_symbols.py
        │   │   ├── check_comments.py
        │   │   ├── check_output_artifacts.py
        │   │   ├── create_matrix_doc_code.py
        │   │   ├── determinism_check.py
        │   │   ├── doctest_cli.py
        │   │   ├── inventory_docs.py
        │   │   ├── link_check.py
        │   │   ├── remove_type_ignore.py
        │   │   ├── run_test_report.py
        │   │   ├── schema_guard.py
        │   │   ├── semantic_diff.py
        │   │   └── vocab_audit.py
        │   ├── __init__.py
        │   ├── app.py
        │   ├── command.py
        │   ├── main.py
        │   └── registry.py
        ├── clients
        │   ├── activity
        │   │   ├── __init__.py
        │   │   └── chembl_activity.py
        │   ├── assay
        │   │   ├── __init__.py
        │   │   ├── chembl_assay.py
        │   │   └── chembl_assay_entity.py
        │   ├── document
        │   │   ├── __init__.py
        │   │   ├── chembl_document.py
        │   │   └── chembl_document_entity.py
        │   ├── target
        │   │   ├── __init__.py
        │   │   └── chembl_target.py
        │   ├── testitem
        │   │   ├── __init__.py
        │   │   └── chembl_testitem.py
        │   ├── __init__.py
        │   ├── chembl.py
        │   ├── chembl_base.py
        │   ├── chembl_entities.py
        │   └── chembl_iterator.py
        ├── config
        │   ├── activity
        │   │   └── __init__.py
        │   ├── assay
        │   │   └── __init__.py
        │   ├── document
        │   │   └── __init__.py
        │   ├── models
        │   │   ├── __init__.py
        │   │   ├── base.py
        │   │   ├── cache.py
        │   │   ├── cli.py
        │   │   ├── determinism.py
        │   │   ├── fallbacks.py
        │   │   ├── http.py
        │   │   ├── io.py
        │   │   ├── logging.py
        │   │   ├── paths.py
        │   │   ├── postprocess.py
        │   │   ├── runtime.py
        │   │   ├── source.py
        │   │   ├── telemetry.py
        │   │   ├── transform.py
        │   │   └── validation.py
        │   ├── target
        │   │   └── __init__.py
        │   ├── testitem
        │   │   └── __init__.py
        │   ├── __init__.py
        │   ├── environment.py
        │   ├── loader.py
        │   ├── models.py
        │   └── utils.py
        ├── core
        │   ├── __init__.py
        │   ├── api_client.py
        │   ├── client_factory.py
        │   ├── hashing.py
        │   ├── load_meta_store.py
        │   ├── logger.py
        │   ├── normalizers.py
        │   ├── output.py
        │   ├── serialization.py
        │   └── test_report_artifacts.py
        ├── etl
        │   └── vocab_store.py
        ├── pipelines
        │   ├── activity
        │   │   ├── __init__.py
        │   │   ├── activity.py
        │   │   ├── activity_enrichment.py
        │   │   └── join_molecule.py
        │   ├── assay
        │   │   ├── __init__.py
        │   │   ├── assay.py
        │   │   ├── assay_enrichment.py
        │   │   └── assay_transform.py
        │   ├── common
        │   │   └── validation.py
        │   ├── document
        │   │   ├── __init__.py
        │   │   ├── document.py
        │   │   └── document_enrich.py
        │   ├── target
        │   │   ├── __init__.py
        │   │   ├── target.py
        │   │   └── target_transform.py
        │   ├── testitem
        │   │   ├── __init__.py
        │   │   ├── testitem.py
        │   │   └── testitem_transform.py
        │   ├── __init__.py
        │   ├── base.py
        │   └── chembl_base.py
        ├── qc
        │   ├── __init__.py
        │   ├── metrics.py
        │   └── report.py
        ├── schemas
        │   ├── activity
        │   │   ├── __init__.py
        │   │   ├── activity_chembl.py
        │   │   └── enrichment.py
        │   ├── assay
        │   │   ├── __init__.py
        │   │   ├── assay_chembl.py
        │   │   └── enrichment.py
        │   ├── document
        │   │   ├── __init__.py
        │   │   ├── document_chembl.py
        │   │   └── enrichment.py
        │   ├── target
        │   │   ├── __init__.py
        │   │   └── target_chembl.py
        │   ├── testitem
        │   │   ├── __init__.py
        │   │   └── testitem_chembl.py
        │   ├── __init__.py
        │   ├── activity_chembl.py
        │   ├── base.py
        │   ├── common.py
        │   ├── load_meta.py
        │   └── vocab.py
        ├── tools
        │   ├── __init__.py
        │   ├── audit_docs.py
        │   ├── build_vocab_store.py
        │   ├── catalog_code_symbols.py
        │   ├── check_comments.py
        │   ├── check_output_artifacts.py
        │   ├── chembl_stub.py
        │   ├── create_matrix_doc_code.py
        │   ├── determinism_check.py
        │   ├── doctest_cli.py
        │   ├── inventory_docs.py
        │   ├── link_check.py
        │   ├── remove_type_ignore.py
        │   ├── run_test_report.py
        │   ├── schema_guard.py
        │   ├── semantic_diff.py
        │   └── vocab_audit.py
        ├── __init__.py
        └── py.typed
```

## Сводная статистика

**Файлы по расширениям:**

- .py: 136
- .typed: 1

**Распределение по директориям (топ-20):**

- src/bioetl/tools: 17 файлов
- src/bioetl/cli/tools: 16 файлов
- src/bioetl/config/models: 16 файлов
- src/bioetl/core: 10 файлов
- src/bioetl/schemas: 6 файлов
- src/bioetl/cli: 5 файлов
- src/bioetl/clients: 5 файлов
- src/bioetl/config: 5 файлов
- src/bioetl/pipelines/activity: 4 файлов
- src/bioetl/pipelines/assay: 4 файлов
- src/bioetl/clients/assay: 3 файлов
- src/bioetl/clients/document: 3 файлов
- src/bioetl/pipelines: 3 файлов
- src/bioetl/pipelines/document: 3 файлов
- src/bioetl/pipelines/target: 3 файлов
- src/bioetl/pipelines/testitem: 3 файлов
- src/bioetl/qc: 3 файлов
- src/bioetl/schemas/activity: 3 файлов
- src/bioetl/schemas/assay: 3 файлов
- src/bioetl/schemas/document: 3 файлов

**Топ-10 крупнейших директорий (по байтам):**

- src/bioetl: 929842 байт
- src: 929842 байт
- .: 929842 байт
- src/bioetl/pipelines: 476530 байт
- src/bioetl/pipelines/activity: 207989 байт
- src/bioetl/tools: 103643 байт
- src/bioetl/config: 97089 байт
- src/bioetl/core: 75793 байт
- src/bioetl/clients: 70081 байт
- src/bioetl/pipelines/assay: 61394 байт

**Топ-20 крупнейших файлов (по байтам):**

- src/bioetl/pipelines/activity/activity.py: 148388 байт; строки: 3321
- src/bioetl/pipelines/base.py: 65131 байт; строки: 1661
- src/bioetl/pipelines/activity/activity_enrichment.py: 38308 байт; строки: 898
- src/bioetl/pipelines/assay/assay.py: 37463 байт; строки: 899
- src/bioetl/pipelines/target/target.py: 35084 байт; строки: 801
- src/bioetl/pipelines/testitem/testitem.py: 31408 байт; строки: 769
- src/bioetl/pipelines/document/document.py: 25781 байт; строки: 633
- src/bioetl/pipelines/chembl_base.py: 23173 байт; строки: 618
- src/bioetl/core/api_client.py: 21359 байт; строки: 570
- src/bioetl/pipelines/activity/join_molecule.py: 21163 байт; строки: 525
- src/bioetl/config/models.py: 20577 байт; строки: 553
- src/bioetl/clients/chembl.py: 17876 байт; строки: 455
- src/bioetl/config/loader.py: 16228 байт; строки: 456
- src/bioetl/clients/chembl_entities.py: 15387 байт; строки: 413
- src/bioetl/core/output.py: 14231 байт; строки: 381
- src/bioetl/pipelines/assay/assay_enrichment.py: 13964 байт; строки: 353
- src/bioetl/tools/vocab_audit.py: 13525 байт; строки: 406
- src/bioetl/tools/audit_docs.py: 12564 байт; строки: 389
- src/bioetl/cli/command.py: 12231 байт; строки: 349
- src/bioetl/clients/chembl_iterator.py: 11617 байт; строки: 316

**Orphan-директории без .py:**


## Аномалии именований (высокий приоритет)
