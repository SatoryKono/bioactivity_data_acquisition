# Acceptance Criteria

## Delivery governance

- ✅ Все изменения поставляются через короткоживущие ветки поверх trunk.
  Pull Request закрывается в течение одного рабочего дня, без длительных
  "feature"-веток.
- ✅ Версии артефактов и API ведутся по SemVer. Каждое изменение, попадающее
  в `main`, добавляет заметку в `CHANGELOG.md` и обновляет номер версии
  пайплайна.
- ✅ Управление устаревшими CLI/конфигурационными параметрами следует PEP 387:
  объявление, предупреждение, удаление в согласованные релизы.

## Acceptance checklist

- [ ] Структурированные логи фиксируют `stage` и `duration_ms` для стадий
  `extract`, `transform`, `validate`, `load`.
- [ ] `meta.yaml` содержит количественные метрики (`row_count`, `column_count`),
  детерминированные `file_checksums`, `config_hash`, `config_version`,
  длительности стадий (`stage_durations_ms`), сортировочные ключи (`sort_keys`)
  и политику по PII/секретам (`pii_secrets_policy`).
- [ ] Повторный запуск с теми же входами → бит-идентичные CSV/JSON и QC, что
  проверяется golden-тестами (`tests/golden/*`).
- [ ] Итоговые CSV детерминированы: стабильная сортировка, нормализованные
  типы, канонические NaN; различия в размерах файлов по сравнению с исходным
  выгрузкой ≥ 30 % в сторону уменьшения.
- [ ] Успешный прогон pytest, mypy, ruff и проверок pre-commit в CI.

<a id="pii-and-secrets"></a>

## PII and secrets policy

- Секреты и PII не попадают в логи, артефакты и `meta.yaml`; все чувствительные
  значения приходят из переменных окружения/secret store.
- `pii_secrets_policy` в `meta.yaml` фиксирует режимы редактирования
  (`redaction: enabled`, `artifact_secrets: forbidden`) и ссылку на данный
  документ (`#pii-and-secrets`).
- QC и ревью проверяют, что debug-дампы и дополнительные артефакты проходят
  ту же политику редактирования.

## Verification of acceptance

- **Structural logs** — `tests/integration/pipelines/test_extended_mode_outputs.py`
  проверяет наличие `duration_ms` и stage-контекстов.
- **Determinism** — `tests/golden/test_cli_golden.py` и `tests/integration/pipelines/
  test_bit_identical_output.py` выполняют битовые сравнения.
- **Metadata contract** — `tests/unit/test_output_writer.py` и `tests/integration/
  pipelines/test_extended_mode_outputs.py` валидируют наличие новых полей
  `stage_durations_ms`, `sort_keys`, `pii_secrets_policy` и `config_version`.
