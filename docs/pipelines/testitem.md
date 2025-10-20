<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: извлечение и обогащение молекулярных данных (ChEMBL + PubChem), нормализация и детерминированный экспорт.
- Вход: CSV с идентификаторами молекул (`molecule_chembl_id` и/или `molregno`).
- Выход: `data/output/testitem/testitems_{date_tag}.csv` + QC + meta.

## S01 Extract
- Источники: ChEMBL (обязательно), PubChem (опционально) — см. `library.testitem.pipeline` и конфиг `testitem`.
- Лимиты/ретраи: через `http.global` и overrides источников.

## S02 Raw-Schema
- Проверки входа: наличие хотя бы одного идентификатора — `library.testitem.pipeline._normalise_columns`.

## S03 Normalize
- Правила: нормализация строковых/числовых/булевых/списковых полей; вычисление `hash_*` — `library.testitem.pipeline._normalize_testitem_data`.

## S04 Validate
- Бизнес-правила: консистентность идентификаторов, диапазоны, длины — `library.testitem.pipeline._validate_*`.

## S05 QC
- Метрики: `row_count`, счётчики источников, PubChem coverage, ошибки.

## S06 Persist
- CSV + YAML meta; детерминизм через `library.etl.load.write_deterministic_csv`.

## S07 CLI
- Команда: `bioactivity-data-acquisition testitem-run` (см. `library.cli:testitem_run`).
- Ключевые параметры: `--config`, `--input`, `--output`, `--cache-dir`, `--pubchem-cache-dir`, `--timeout`, `--retries`, `--limit`, `--disable-pubchem`, `--dry-run`.

## S08 Ops/CI
- Публикация артефактов в CI, детерминизм и логирование аналогично documents.

