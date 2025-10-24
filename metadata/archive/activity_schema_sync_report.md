# Отчёт синхронизации activity конфигурации со схемами и выходами

## Резюме

Проведена полная инвентаризация несоответствий между конфигурацией activity пайплайна, Pandera-схемами и фактическими выходными данными. Обнаружены критические расхождения, требующие немедленного исправления.

## Сводная таблица несоответствий

| Entity | Check Type | Status | Details | Priority | Recommended Action |
|--------|-----------|--------|---------|----------|-------------------|
| activity | columns_missing | **FAIL** | `saltform_id` в CSV, но отсутствует в YAML column_order | **P1** | Добавить в column_order |
| activity | columns_count | **FAIL** | YAML: 27 колонок, CSV: 34 колонки | **P1** | Синхронизировать количество |
| activity | schema_extra_fields | **WARN** | `extraction_errors`, `validation_errors`, `extraction_status` в Pandera, но не в YAML | P2 | Удалить из схем или добавить в YAML |
| activity | schema_missing_fields | **FAIL** | `saltform_id` отсутствует в Pandera ActivityNormalizedSchema | **P1** | Добавить в схему |
| activity | dtype_mismatch | **OK** | Все основные типы соответствуют | - | - |
| activity | format_datetime | **WARN** | ISO 8601 с микросекундами vs Z-suffix в YAML | P2 | Унифицировать формат |
| activity | format_float | **OK** | float_format "%.6f" применяется корректно | - | - |
| activity | format_null | **OK** | na_rep "" применяется корректно | - | - |
| activity | column_order | **FAIL** | Порядок не полностью соответствует YAML | **P1** | Пересобрать пайплайн |

## Детальный анализ

### 1. Инвентаризация колонок

**Фактический CSV** (`data/output/activities/activities_20251023.csv`):
```
1. activity_chembl_id
2. assay_chembl_id  
3. document_chembl_id
4. target_chembl_id
5. molecule_chembl_id
6. saltform_id                    # ❌ ОТСУТСТВУЕТ в YAML
7. activity_type
8. activity_value
9. activity_unit
10. pchembl_value
11. data_validity_comment
12. activity_comment
13. lower_bound
14. upper_bound
15. is_censored
16. published_type
17. published_relation
18. published_value
19. published_units
20. standard_type
21. standard_relation
22. standard_value
23. standard_units
24. standard_flag
25. bao_endpoint
26. bao_format
27. bao_label
28. index
29. pipeline_version
30. source_system
31. chembl_release
32. extracted_at
33. hash_row
34. hash_business_key
```

**YAML column_order** (`configs/config_activity.yaml:88-124`):
- Определено: **27 колонок**
- **Отсутствует**: `saltform_id` (позиция 6 в CSV)

### 2. Трассировка источника saltform_id

**Источник**: `data/input/activity.csv` (строка 1)
```csv
activity_chembl_id,assay_chembl_id,document_chembl_id,molecule_chembl_id,saltform_id,target_chembl_id
```

**Пайплайн обработки**:
1. **Входные данные**: `saltform_id` присутствует в `data/input/activity.csv`
2. **Извлечение**: `src/library/activity/client.py:296-332` - НЕ добавляет `saltform_id`
3. **Нормализация**: `src/library/activity/normalize.py:52-96` - НЕ обрабатывает `saltform_id`
4. **Запись**: `src/library/common/writer_base.py:95-105` - применяет column_order, но `saltform_id` отсутствует в YAML

**Вывод**: `saltform_id` проходит через пайплайн без изменений из входного файла, но не задокументирован в конфигурации.

### 3. Проверка Pandera-схем

**ActivityNormalizedSchema** (`src/library/schemas/activity_schema.py:151-373`):

**Отсутствующие поля**:
- `saltform_id` - **критично**

**Лишние поля** (есть в схеме, но не в YAML column_order):
- `extraction_errors` (строка 363)
- `validation_errors` (строка 364) 
- `extraction_status` (строки 365-372)

**Соответствие типов**:
| Поле | YAML dtype | Pandera dtype | CSV образец | Статус |
|------|-----------|---------------|-------------|--------|
| activity_chembl_id | STRING | pa.String | CHEMBL33279 | ✅ |
| activity_value | DECIMAL | pa.Float | 833.0 | ✅ |
| pchembl_value | DECIMAL | pa.Float | (пусто) | ✅ nullable |
| is_censored | BOOL | pa.Bool | False | ✅ |
| extracted_at | TIMESTAMP | pa.DateTime | 2025-10-23T19:06:58.750152Z | ⚠️ формат |

### 4. Проверка форматов

**YAML настройки** (`configs/config_activity.yaml:54-56`):
```yaml
csv:
  float_format: "%.6f"              # ✅ Применяется корректно
  date_format: "%Y-%m-%dT%H:%M:%SZ" # ⚠️ Фактически: с микросекундами
  na_rep: ""                        # ✅ Применяется корректно
```

**Фактические форматы в CSV**:
- **Float**: `833.0`, `0.6` - соответствует `%.6f`
- **DateTime**: `2025-10-23T19:06:58.750152Z` - содержит микросекунды, не соответствует YAML `%Y-%m-%dT%H:%M:%SZ`
- **NULL**: пустые значения - соответствует `na_rep: ""`

### 5. Промежуточные DataFrame

**Трассировка изменений колонок**:

1. **Входные данные** (`data/input/activity.csv`): 6 колонок
2. **После извлечения** (`_parse_activity`): +20 колонок из ChEMBL API
3. **После нормализации** (`normalize_activities`): +7 вычисляемых полей, -4 служебных поля
4. **После записи** (`_write_deterministic_csv`): применяется column_order, исключаются служебные поля

**Итоговый результат**: 34 колонки в CSV

## YAML-патчи

### configs/config_activity.yaml

```yaml
# Добавить в determinism.column_order после molecule_chembl_id:
determinism:
  column_order:
    - "activity_chembl_id"
    - "assay_chembl_id"
    - "document_chembl_id"
    - "target_chembl_id"
    - "molecule_chembl_id"
    - "saltform_id"              # ДОБАВИТЬ: FK к saltform в testitem, источник: input CSV, тип: STRING, nullable
    - "activity_type"
    # ... остальные поля без изменений ...
```

### src/library/schemas/activity_schema.py

```python
# Добавить в ActivityNormalizedSchema.get_schema() после molecule_chembl_id:
"molecule_chembl_id": Column(
    pa.String,
    checks=[
        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL molecule ID format"),
        Check(lambda x: x.notna())
    ],
    nullable=False,
    description="ChEMBL ID молекулы"
),
"saltform_id": Column(                    # ДОБАВИТЬ
    pa.String,
    checks=[
        Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL saltform ID format")
    ],
    nullable=True,
    description="ChEMBL ID saltform"
),
```

## Рекомендации по унификации

### 1. BaseSchema для общих проверок

```python
# src/library/schemas/base_schema.py
class BaseChEMBLSchema:
    @staticmethod
    def chembl_id_column(name: str, nullable: bool = False) -> Column:
        return Column(
            pa.String,
            checks=[
                Check.str_matches(r'^CHEMBL\d+$', error=f"Invalid ChEMBL {name} ID format"),
                Check(lambda x: x.notna()) if not nullable else Check(lambda x: True)
            ],
            nullable=nullable,
            description=f"ChEMBL ID {name}"
        )
```

### 2. BaseNormalizer

```python
# src/library/normalizers/base_normalizer.py
class BaseNormalizer:
    def normalize_chembl_id(self, series: pd.Series) -> pd.Series:
        """Нормализация ChEMBL ID: strip, uppercase"""
        return series.str.strip().str.upper()
    
    def normalize_datetime_iso8601(self, series: pd.Series) -> pd.Series:
        """Нормализация дат: coerce to UTC, format as ISO 8601"""
        return pd.to_datetime(series, utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
```

### 3. Детерминизм вывода

**Строгий режим**: column_order должен включать ВСЕ выходные колонки
**float_format**: применяется через pandas to_csv с параметрами из YAML
**Фиксированная сортировка**: по determinism.sort.by

## Приоритизация исправлений

### P1 (критичные - блокируют сборку)
1. **Добавить `saltform_id` в YAML column_order** - отсутствие поля в конфигурации
2. **Добавить `saltform_id` в Pandera ActivityNormalizedSchema** - схема не соответствует данным
3. **Синхронизировать количество колонок** (27 vs 34) - расхождение в документации

### P2 (воспроизводимость)
1. **Унифицировать формат datetime** - микросекунды vs Z-suffix
2. **Удалить лишние поля из Pandera схем** - `extraction_errors`, `validation_errors`, `extraction_status`
3. **Применить BaseSchema/BaseNormalizer** - единообразие проверок

### P3 (техдолг)
1. **Добавить комментарии в YAML** - описания полей
2. **Создать валидатор синхронизации** - автоматическая проверка соответствия

## Критерии успеха

- [ ] Все 34 колонки CSV задокументированы в column_order
- [ ] Pandera ActivityNormalizedSchema включает все поля из column_order
- [ ] Типы данных YAML ↔ Pandera ↔ CSV синхронизированы
- [ ] Специальные форматы (datetime, float) применяются единообразно
- [ ] Сводная таблица показывает только OK/WARN статусы

## Следующие шаги

1. **Немедленно**: Применить YAML-патч для `saltform_id`
2. **В течение дня**: Обновить Pandera-схему
3. **В течение недели**: Внедрить BaseSchema/BaseNormalizer
4. **Постоянно**: Добавить валидатор синхронизации в CI/CD

---
*Отчёт сгенерирован: 2025-01-27*
*Пайплайн: activity*
*Версия конфигурации: 2.0.0*
