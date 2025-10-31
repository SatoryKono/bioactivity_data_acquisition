# Risk Register

| Risk | Description | Monitoring & Detection | Mitigation |
| --- | --- | --- | --- |
| API contract divergence | Источники (ChEMBL, CrossRef и т.д.) меняют схемы/эндпоинты без обратной совместимости. | Мониторинг статусов HTTP/JSON-schema в интеграционных тестах; ежедневные smoke-запуски с валидацией Pandera (`tests/integration/pipelines/*`). | Версионирование коннекторов, feature flags на уровне конфигурации, fallback `partial_retry`, оперативное обновление схем регистри. |
| Cursor drift / pagination gaps | Потеря или дублирование записей при инкрементальной выгрузке. | `meta.yaml` фиксирует `stage_durations_ms`, `sort_keys` и курсор (при наличии); QC отчёты (`qc_missing_mappings.csv`) отслеживают разрывы, алерты в логах `cursor_mismatch`. | Сравнение с предыдущим снапшотом (hash-сводки), атомарная запись курсоров, fail-fast при несостыковке количества строк. |
| Unstable units / flaky enrichment | Нестабильные вспомогательные таблицы или enrichment-стадии нарушают детерминизм. | Еженедельные повторные прогоны golden-тестов, structlog фильтр `stage="enrichment"`, метрики `dataset_metrics.csv` для дрейфа. | Фиксация входов в S3-кассеты, idempotent enrichment stages, авто-disable через feature flag при повторных сбоях. |
| Write failures / partial artefacts | Сбой при записи CSV/QC ведёт к битым файлам. | Структурные логи `atomic_write` + алерты по отсутствию `manifest`/`meta.yaml`; проверка atomic write в `tests/unit/test_output_writer.py`. | Использование `AtomicWriter` (`os.replace`), повторный запуск пайплайна, чистка `.tmp_run_*` директорий и валидация checksum. |

> Риски пересматриваются ежемесячно на операционных ревью. Каждый риск закреплён за владельцем (Pipeline On-Call). Обновления фиксируются в `CHANGELOG.md`.
