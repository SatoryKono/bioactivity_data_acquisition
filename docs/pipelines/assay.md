<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: извлечение и нормализация данных об ассаях ChEMBL, сохранение детерминированных артефактов.
- Вход: список `assay_chembl_id` или `target_chembl_id` + фильтры.
- Выход: `data/output/assay/assays_{date_tag}.csv` + QC + meta.

## S01 Extract
- Источники: ChEMBL — батчевые запросы, fallback на одиночные (см. `library.assay.pipeline._extract_assay_data*`).
- Параметры/лимиты/ретраи — через `AssayConfig.sources.chembl.http`.

## S02 Raw-Schema
- Входные требования: `assay_chembl_id` обязательный — `library.assay.pipeline._normalise_columns`.

## S03 Normalize
- Нормализация строк/списков, сопоставления `assay_type` → описание, ограничения диапазонов — `library.assay.pipeline._normalize_assay_fields`.

## S04 Validate
- Pandera: `library.schemas.assay_schema.AssayNormalizedSchema.validate` + бизнес-правила.

## S05 QC
- Метрики: `row_count`, `enabled_sources`, `chembl_records`.

## S06 Persist
- CSV + YAML meta; детерминированная запись `library.etl.load.write_deterministic_csv`.

## S07 CLI
- Использование через общую команду `pipeline` с соответствующим конфигом; отдельной подкоманды нет.

## S08 Ops/CI
- Аналогично другим пайплайнам: логи, артефакты, воспроизводимость.

