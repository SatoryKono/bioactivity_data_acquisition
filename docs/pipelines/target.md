<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: извлечение таргетов ChEMBL, нормализация и обогащение UniProt/IUPHAR, детерминированный экспорт.
- Вход: `data/input/target.csv` (см. `configs/config_target_full.yaml`).
- Выход: `data/output/target/target_{date_tag}.csv` + QC + meta.

## S01 Extract
- Источники: ChEMBL (ядро), UniProt (обогащение), IUPHAR (классификация) — см. `configs/config_target_full.yaml`.
- Лимиты/ретраи: разделы `http.*`, `sources.*.http.retries`, `sources.*.rate_limit`.

## S02 Raw-Schema
- Pandera-схемы: явно не выделены; проверки на уровне постобработки и выравнивания колонок.

## S03 Normalize
- Выравнивание и нормализация полей: `library.pipelines.target.postprocessing.align_target_columns`, `finalise_targets`.

## S04 Validate
- Проверки уникальности/обязательных полей: `finalise_targets` + валидация входных колонок.

## S05 QC
- Метрики и отчёты — по аналогии с другими пайплайнами; пути в `data/output/target/`.

## S06 Persist
- CSV + YAML meta; порядок колонок согласно `library.schemas.targets.TARGETS_COLUMN_ORDER`.

## S07 CLI
- Использование через общую команду `pipeline` с конфигом `configs/config_target_full.yaml`.

## S08 Ops/CI
- Воспроизводимость и логи — общие правила проекта.

