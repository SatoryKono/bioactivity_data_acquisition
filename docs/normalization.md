## Normalization Rules

### Documents
- `_normalise_columns`: трим строк, обязательные поля, маппинг устаревших колонок.

### Testitem
- `_normalize_testitem_data`: нормализация строк/чисел/булевых/списков, вычисление `hash_*`.

### Assay
- `_normalize_assay_fields`: строки, списки, маппинги типов, ограничения значений.

### Target
- `postprocessing.align_target_columns` и `finalise_targets`: выравнивание имен и приоритетов, объединения списков `|`.

