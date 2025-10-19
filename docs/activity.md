## Activity: обзор пайплайна

Пайплайн `activity` реализован по образцу `documents` и предоставляет:

- чтение входного CSV с идентификаторами (assay/target/document);
- извлечение сырых записей активности из ChEMBL (`endpoint: activity`);
- нормализацию единиц и полей (`normalize_bioactivity_data`);
- генерацию QC-метрик и метаданных;
- детерминированную запись результатов в `data/output/activity`.

Основные файлы:

- `library/activity/config.py` — модели конфигурации и загрузчик `load_activity_config`;
- `library/activity/pipeline.py` — orchestration: `run_activity_etl`, `write_activity_outputs`;
- `library/schemas/activity_schema.py` — схемы Pandera для raw/normalized.
