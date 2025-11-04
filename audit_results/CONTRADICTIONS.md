# Противоречия в документации

| Тип | Файл/раздел | Формулировка №1 | Формулировка №2 | Почему конфликт | ref_1 | ref_2 | Критичность |
|-----|-------------|-----------------|-----------------|-----------------|-------|-------|-------------|
| A | docs/pipelines/10-chembl-pipelines-catalog.md | determinism.sort.by: ['assay_id', 'testitem_id', 'activity_id'] | determinism.sort.by: ['activity_id'] (возможная интерпретация) | Каталог указывает 3 ключа, но в determinism policy может быть другая интерпретация. | [ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001] | [ref: repo:docs/determinism/00-determinism-policy.md@refactoring_001] | MEDIUM |
| B | .lychee.toml | docs/architecture/00-architecture-overview.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует в репозитории. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| D | docs/pipelines/10-chembl-pipelines-catalog.md | testitem | test_item (возможный вариант) | Используется testitem без подчеркивания, но возможны варианты написания. | [ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001] | [ref: repo:docs/pipelines/07-testitem-chembl-extraction.md@refactoring_001] | LOW |
| B | .lychee.toml | docs/architecture/00-architecture-overview.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| B | .lychee.toml | docs/architecture/03-data-sources-and-spec.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| B | .lychee.toml | docs/pipelines/PIPELINES.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| B | .lychee.toml | docs/configs/CONFIGS.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| B | .lychee.toml | docs/cli/CLI.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
| B | .lychee.toml | docs/qc/QA_QC.md объявлен | Файл отсутствует | Файл объявлен в .lychee.toml, но физически отсутствует. | [ref: repo:.lychee.toml@refactoring_001] | N/A | CRITICAL |
