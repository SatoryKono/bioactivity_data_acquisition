# Отчёт синхронизации схем и конфигураций

## Executive Summary

- **Entities проверено**: 5/5

## Таблица несоответствий

| Entity | Config Path | Check Type | Result | Details | Priority | Recommended Action |
|--------|-------------|------------|--------|---------|----------|-------------------|
| activities | configs/config_activities.yaml | columns | OK | OK | P2 | No action needed |
| assays | configs/config_assays.yaml | columns | OK | OK | P2 | No action needed |
| documents | configs/config_documents.yaml | columns | OK | OK | P2 | No action needed |
| targets | configs/config_targets.yaml | columns | FAIL | Extra: 3 cols, Order mismatch | P2 | Remove/map extra, Fix column order |
| testitem | configs/config_testitem.yaml | columns | OK | OK | P2 | No action needed |

## Детальный анализ по сущностям

### Activities

- **YAML колонок**: 33
- **Фактических колонок**: 33
- **Порядок совпадает**: YES

### Assays

- **YAML колонок**: 46
- **Фактических колонок**: 46
- **Порядок совпадает**: YES

### Documents

- **YAML колонок**: 89
- **Фактических колонок**: 89
- **Порядок совпадает**: YES

### Targets

- **YAML колонок**: 131
- **Фактических колонок**: 133
- **Порядок совпадает**: NO
- **Лишние в выходе**: pipeline_version.1, pipeline_version.2, pipeline_version.3

### Testitem

- **YAML колонок**: 104
- **Фактических колонок**: 104
- **Порядок совпадает**: YES
