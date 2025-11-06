# 18 Activity ChEMBL Field Mapping

**Version:** 1.0.0
**Date:** 2025-01-29
**Author:** Data Acquisition Team

## Purpose

Исчерпывающее описание правил заполнения таблицы активностей в пайплайне активностей. Для каждого поля описано:

- Как запрашиваются данные из ChEMBL API
- Как нормализуются значения
- Как валидируются перед записью

## Общая схема извлечения данных

### Запрос к ChEMBL API

Данные запрашиваются через эндпоинт `/activity.json` с параметрами:

- `activity_id__in`: список ID активностей (через запятую, батчами до 25)
- `only`: список полей для запроса (по умолчанию `API_ACTIVITY_FIELDS`)

### Этапы обработки

1. **Extract**: извлечение сырых данных из ChEMBL API
2. **Transform**: нормализация и обогащение данных
3. **Validate**: валидация по Pandera схеме

## Описание полей

### 1. activity_id

**Источник:** Прямое поле из ChEMBL API (`activity_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`
- Используется как ключ для батчинга запросов

**Нормализация:**

- Приведение к `int64` (не nullable)
- Сортировка по `activity_id` после извлечения

**Валидация:**

- Тип: `int64`, не nullable, `>= 1`, уникальное
- Проверка уникальности перед валидацией схемы

---

### 2. row_subtype

**Источник:** Генерируется в пайплайне

**Запрос:** Не запрашивается из ChEMBL

**Нормализация:**

- Всегда устанавливается в `"activity"` в методе `_add_row_metadata()`

**Валидация:**

- Тип: `string`, не nullable
- Значение: `"activity"`

---

### 3. row_index

**Источник:** Генерируется в пайплайне

**Запрос:** Не запрашивается из ChEMBL

**Нормализация:**

- Последовательный индекс начиная с 0 в методе `_add_row_metadata()`
- Приводится к `Int64` (nullable integer)

**Валидация:**

- Тип: `Int64`, не nullable
- Последовательные значения от 0

---

### 4. assay_chembl_id

**Источник:** Прямое поле из ChEMBL API (`assay_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, не nullable
- Формат: `CHEMBL\d+` (regex)
- Проверка foreign key integrity

---

### 5. assay_type

**Источник:** Прямое поле из ChEMBL API (`assay_type`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 6. assay_description

**Источник:** Прямое поле из ChEMBL API (`assay_description`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 7. assay_organism

**Источник:**

1. Прямое поле из ChEMBL API (`assay_organism`)
2. Обогащение через `/assay.json` (если включено)

**Запрос:**

- Прямое поле: из `/activity.json`
- Обогащение: через `enrich_with_assay()` → `/assay.json` с полями `assay_organism`, `assay_tax_id`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Title case: `.str.title()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 8. assay_tax_id

**Источник:**

1. Прямое поле из ChEMBL API (`assay_tax_id`)
2. Обогащение через `/assay.json` (если включено)

**Запрос:**

- Прямое поле: из `/activity.json`
- Обогащение: через `enrich_with_assay()` → `/assay.json` с полями `assay_organism`, `assay_tax_id`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Приведение к `Int64` (nullable integer)
- Значения должны быть `>= 1` (иначе → `None`)

**Валидация:**

- Тип: `Int64`, nullable, `>= 1`

---

### 9. testitem_chembl_id

**Источник:** Прямое поле из ChEMBL API (`testitem_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`
- Если отсутствует, копируется из `molecule_chembl_id`

**Валидация:**

- Тип: `string`, не nullable
- Формат: `CHEMBL\d+` (regex)
- Проверка foreign key integrity
- Проверка соответствия с `molecule_chembl_id` (warning при несоответствии)

---

### 10. molecule_chembl_id

**Источник:** Прямое поле из ChEMBL API (`molecule_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, не nullable
- Формат: `CHEMBL\d+` (regex)
- Проверка foreign key integrity

---

### 11. parent_molecule_chembl_id

**Источник:** Прямое поле из ChEMBL API (`parent_molecule_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, nullable
- Формат: `CHEMBL\d+` (regex, если не NULL)
- Проверка foreign key integrity

---

### 12. molecule_pref_name

**Источник:**

1. Прямое поле из ChEMBL API (`molecule_pref_name`)
2. Вложенный объект `molecule.pref_name` (извлекается в `_extract_nested_fields()`)

**Запрос:**

- Прямое поле: из `/activity.json`
- Вложенный объект: из `molecule.pref_name` (если доступен)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 13. target_chembl_id

**Источник:** Прямое поле из ChEMBL API (`target_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, nullable
- Формат: `CHEMBL\d+` (regex, если не NULL)
- Проверка foreign key integrity

---

### 14. target_pref_name

**Источник:** Прямое поле из ChEMBL API (`target_pref_name`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 15. document_chembl_id

**Источник:** Прямое поле из ChEMBL API (`document_chembl_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^CHEMBL\d+$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, nullable
- Формат: `CHEMBL\d+` (regex, если не NULL)
- Проверка foreign key integrity

---

### 16. record_id

**Источник:** Прямое поле из ChEMBL API (`record_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Приведение к `Int64` (nullable integer)
- Значения должны быть `>= 1` (иначе → `None`)

**Валидация:**

- Тип: `Int64`, nullable, `>= 1`

---

### 17. src_id

**Источник:** Прямое поле из ChEMBL API (`src_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Приведение к `Int64` (nullable integer)
- Значения должны быть `>= 1` (иначе → `None`)

**Валидация:**

- Тип: `Int64`, nullable, `>= 1`

---

### 18. type

**Источник:** Прямое поле из ChEMBL API (`type`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 19. relation

**Источник:** Прямое поле из ChEMBL API (`relation`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Замена Unicode символов на ASCII:
  - `≤` → `<=`

  - `≥` → `>=`

  - `≠` → `~`

- Валидация по whitelist: `{"=", ">", "<", ">=", "<=", "~"}`
- Невалидные значения → `None`

**Валидация:**

- Тип: `string`, nullable
- Допустимые значения: `{"=", ">", "<", ">=", "<=", "~"}`

---

### 20. value

**Источник:** Прямое поле из ChEMBL API (`value`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Сохраняется как `object` (mixed types)
- Не приводится к числовому типу (может быть строкой или числом)

**Валидация:**

- Тип: `object`, nullable
- Может содержать числа или строки

---

### 21. units

**Источник:** Прямое поле из ChEMBL API (`units`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 22. standard_type

**Источник:** Прямое поле из ChEMBL API (`standard_type`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Валидация по whitelist: `{"IC50", "EC50", "XC50", "AC50", "Ki", "Kd", "Potency", "ED50"}`
- Невалидные значения → `None`

**Валидация:**

- Тип: `string`, nullable
- Допустимые значения: `{"IC50", "EC50", "XC50", "AC50", "Ki", "Kd", "Potency", "ED50"}`

---

### 23. standard_relation

**Источник:** Прямое поле из ChEMBL API (`standard_relation`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Замена Unicode символов на ASCII:
  - `≤` → `<=`

  - `≥` → `>=`

  - `≠` → `~`

- Валидация по whitelist: `{"=", ">", "<", ">=", "<=", "~"}`
- Невалидные значения → `None`

**Валидация:**

- Тип: `string`, nullable
- Допустимые значения: `{"=", ">", "<", ">=", "<=", "~"}`

---

### 24. standard_value

**Источник:** Прямое поле из ChEMBL API (`standard_value`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Преобразование в строку: `.astype(str).str.strip()`
- Удаление пробелов и запятых: `.str.replace(r"[,\s]", "", regex=True)`
- Извлечение первого числового значения из диапазонов: `.str.extract(r"([+-]?\d*\.?\d+)", expand=False)`
- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Отрицательные значения → `None` (должно быть `>= 0`)
- Приведение к `float64`

**Валидация:**

- Тип: `float64`, nullable, `>= 0`

---

### 25. standard_upper_value

**Источник:**

1. Прямое поле из ChEMBL API (`standard_upper_value`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Преобразование в строку: `.astype(str).str.strip()`
- Удаление пробелов и запятых: `.str.replace(r"[,\s]", "", regex=True)`
- Извлечение первого числового значения: `.str.extract(r"([+-]?\d*\.?\d+)", expand=False)`
- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Отрицательные значения → `None` (должно быть `>= 0`)
- Приведение к `float64`

**Валидация:**

- Тип: `float64`, nullable, `>= 0`

---

### 26. standard_units

**Источник:** Прямое поле из ChEMBL API (`standard_units`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Нормализация синонимов единиц измерения:
  - `nanomolar`, `nmol`, `nm`, `NM` → `nM`

  - `µM`, `uM`, `UM`, `micromolar`, `microM`, `umol` → `μM`

  - `millimolar`, `milliM`, `mmol`, `MM` → `mM`

  - `percent`, `pct` → `%`

  - `ratios` → `ratio`

- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 27. standard_text_value

**Источник:**

1. Прямое поле из ChEMBL API (`standard_text_value`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 28. standard_flag

**Источник:** Прямое поле из ChEMBL API (`standard_flag`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Приведение к `Int64` (nullable integer)
- Валидация значений: только `0` или `1` (иначе → `None`)

**Валидация:**

- Тип: `Int64`, nullable
- Допустимые значения: `{0, 1}`

---

### 29. upper_value

**Источник:**

1. Прямое поле из ChEMBL API (`upper_value`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Отрицательные значения → `None` (должно быть `>= 0`)
- Приведение к `float64`

**Валидация:**

- Тип: `float64`, nullable, `>= 0`

---

### 30. lower_value

**Источник:**

1. Прямое поле из ChEMBL API (`lower_value`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Отрицательные значения → `None` (должно быть `>= 0`)
- Приведение к `float64`

**Валидация:**

- Тип: `float64`, nullable, `>= 0`

---

### 31. pchembl_value

**Источник:** Прямое поле из ChEMBL API (`pchembl_value`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Отрицательные значения → `None` (должно быть `>= 0`)
- Приведение к `float64`

**Валидация:**

- Тип: `float64`, nullable, `>= 0`

---

### 32. uo_units

**Источник:** Прямое поле из ChEMBL API (`uo_units`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 33. qudt_units

**Источник:** Прямое поле из ChEMBL API (`qudt_units`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 34. text_value

**Источник:**

1. Прямое поле из ChEMBL API (`text_value`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 35. activity_comment

**Источник:**

1. Прямое поле из ChEMBL API (`activity_comment`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 36. bao_endpoint

**Источник:** Прямое поле из ChEMBL API (`bao_endpoint`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^BAO_\d{7}$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, nullable
- Формат: `BAO_\d{7}` (regex, если не NULL)

---

### 37. bao_format

**Источник:** Прямое поле из ChEMBL API (`bao_format`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к верхнему регистру: `.str.upper().str.strip()`
- Валидация формата через regex: `^BAO_\d{7}$`
- Невалидные значения устанавливаются в `None`

**Валидация:**

- Тип: `string`, nullable
- Формат: `BAO_\d{7}` (regex, если не NULL)

---

### 38. bao_label

**Источник:** Прямое поле из ChEMBL API (`bao_label`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Ограничение длины: максимум 128 символов
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable
- Максимальная длина: 128 символов

---

### 39. canonical_smiles

**Источник:** Прямое поле из ChEMBL API (`canonical_smiles`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 40. ligand_efficiency

**Источник:** Прямое поле из ChEMBL API (`ligand_efficiency`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Сериализация в JSON строку: `json.dumps(..., ensure_ascii=False, sort_keys=True)`
- Если не сериализуется → `None`
- Приведение к типу `object` (string)

**Валидация:**

- Тип: `string` (nullable), должен быть валидным JSON

---

### 41. target_organism

**Источник:** Прямое поле из ChEMBL API (`target_organism`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Title case: `.str.title()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 42. target_tax_id

**Источник:** Прямое поле из ChEMBL API (`target_tax_id`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к числовому типу: `pd.to_numeric(..., errors="coerce")`
- Приведение к `Int64` (nullable integer)
- Значения должны быть `>= 1` (иначе → `None`)

**Валидация:**

- Тип: `Int64`, nullable, `>= 1`

---

### 43. data_validity_comment

**Источник:**

1. Прямое поле из ChEMBL API (`data_validity_comment`)
2. Из массива `activity_properties` (fallback, если отсутствует в основном ответе)

**Запрос:**

- Прямое поле: из `/activity.json`
- Fallback: из `activity_properties` (извлекается в `_extract_activity_properties_fields()`)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable
- Soft enum: проверка по whitelist из конфига (warning при несоответствии, не блокирует валидацию)

---

### 44. data_validity_description

**Источник:** Обогащение через `/data_validity_lookup.json`

**Запрос:**

- Отдельный запрос в `extract()` через `_extract_data_validity_descriptions()`
- Собираются уникальные непустые значения `data_validity_comment`
- Запрос к `/data_validity_lookup.json` с параметром `data_validity_comment__in`
- LEFT JOIN обратно к DataFrame по `data_validity_comment`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`
- Проверка инварианта: `data_validity_description` не должно быть заполнено при `data_validity_comment = NULL` (warning)

**Валидация:**

- Тип: `string`, nullable
- Инвариант: если `data_validity_description` заполнено, то `data_validity_comment` должно быть заполнено

---

### 45. potential_duplicate

**Источник:** Прямое поле из ChEMBL API (`potential_duplicate`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`

**Нормализация:**

- Приведение к boolean: `.astype("boolean")`
- Сохранение `NA` значений

**Валидация:**

- Тип: `boolean`, nullable

---

### 46. activity_properties

**Источник:** Прямое поле из ChEMBL API (`activity_properties`)

**Запрос:**

- Запрашивается напрямую из `/activity.json`
- Может быть массивом объектов или JSON строкой

**Нормализация:**

- Парсинг JSON строки (если строка)
- Нормализация элементов массива:
  - Каждый элемент должен содержать только ключи из `ACTIVITY_PROPERTY_KEYS`: `["type", "relation", "units", "value", "text_value", "result_flag"]`

  - `result_flag`: приведение `int` (0/1) → `bool`

- Сериализация в канонический JSON: `json.dumps(..., ensure_ascii=False, sort_keys=True)`
- Если не сериализуется → `None`

**Валидация:**

- Тип: `string` (nullable), должен быть валидным JSON массивом
- Каждый элемент массива должен соответствовать структуре `ACTIVITY_PROPERTY_KEYS`
- Валидация через функцию `_is_valid_activity_properties()`

---

### 47. compound_key

**Источник:** Обогащение через `fetch_compound_records_by_pairs()`

**Запрос:**

- Обогащение в `transform()` через `enrich_with_compound_record()`
- Запрос по парам `(molecule_chembl_id, document_chembl_id)`
- Извлекается из `COMPOUND_STRUCTURES.STANDARD_INCHI_KEY`

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 48. compound_name

**Источник:** Обогащение через `fetch_compound_records_by_pairs()`

**Запрос:**

- Обогащение в `transform()` через `enrich_with_compound_record()`
- Запрос по парам `(molecule_chembl_id, document_chembl_id)`
- Извлекается из `MOLECULE_DICTIONARY.PREF_NAME` (fallback: `CHEMBL_ID`)

**Нормализация:**

- Trim пробелов: `.str.strip()`
- Пустые строки → `None`
- Приведение к типу `string`

**Валидация:**

- Тип: `string`, nullable

---

### 49. curated

**Источник:**

1. Прямое поле из ChEMBL API (`curated_by`)
2. Обогащение через `fetch_compound_records_by_pairs()` (если включено)

**Запрос:**

- Прямое поле: из `/activity.json` (`curated_by`)
- Обогащение: через `enrich_with_compound_record()` (если включено)

**Нормализация:**

- Извлечение из `curated_by`: `(curated_by IS NOT NULL) -> True, иначе -> False`
- Приведение к boolean: `.astype("boolean")`
- Сохранение `NA` значений

**Валидация:**

- Тип: `boolean`, nullable

---

### 50. removed

**Источник:** Обогащение через `fetch_compound_records_by_pairs()`

**Запрос:**

- Обогащение в `transform()` через `enrich_with_compound_record()`
- Запрос по парам `(molecule_chembl_id, document_chembl_id)`

**Нормализация:**

- Всегда устанавливается в `None` (не извлекается из ChEMBL)
- Приведение к boolean: `.astype("boolean")`

**Валидация:**

- Тип: `boolean`, nullable
- Всегда `NULL` (не используется в ChEMBL)

---

## Специальные обработки

### Извлечение вложенных полей

Метод `_extract_nested_fields()` извлекает поля из вложенных объектов:

- `assay.organism` → `assay_organism`
- `assay.tax_id` → `assay_tax_id`
- `molecule.pref_name` → `molecule_pref_name`
- `curated_by` → `curated` (boolean)

### Извлечение полей из activity_properties

Метод `_extract_activity_properties_fields()` извлекает поля из массива `activity_properties` только если они отсутствуют в основном ответе API:

- `upper_value`
- `lower_value`
- `standard_upper_value`
- `text_value`
- `standard_text_value`
- `activity_comment`
- `data_validity_comment`

### Обогащение данных

Обогащение выполняется в этапе `transform()`:

1. **enrich_with_assay()**: обогащает `assay_organism`, `assay_tax_id` из `/assay.json`
2. **enrich_with_compound_record()**: обогащает `compound_name`, `compound_key`, `curated`, `removed` из `fetch_compound_records_by_pairs()`
3. **enrich_with_data_validity()**: обогащает `data_validity_description` из `/data_validity_lookup.json`

## Порядок обработки

1. **Extract**:
   - Запрос к `/activity.json` с батчингом по `activity_id`

   - Извлечение вложенных полей (`_extract_nested_fields()`)

   - Извлечение полей из `activity_properties` (`_extract_activity_properties_fields()`)

   - Запрос `data_validity_description` через `_extract_data_validity_descriptions()`

2. **Transform**:

   - Нормализация идентификаторов (`_normalize_identifiers()`)

   - Нормализация измерений (`_normalize_measurements()`)

   - Нормализация строковых полей (`_normalize_string_fields()`)

   - Нормализация вложенных структур (`_normalize_nested_structures()`)

   - Добавление метаданных строк (`_add_row_metadata()`)

   - Нормализация типов данных (`_normalize_data_types()`)

   - Обогащение данными из других источников (assay, compound_record, data_validity)

   - Финализация колонок (`_finalize_identifier_columns()`, `_finalize_output_columns()`)

   - Фильтрация невалидных строк (`_filter_invalid_required_fields()`)

3. **Validate**:

   - Проверка уникальности `activity_id`

   - Проверка foreign key integrity

   - Soft enum валидация `data_validity_comment`

   - Валидация по Pandera схеме `ActivitySchema`

## Связанная документация

- [09-activity-chembl-extraction.md](09-activity-chembl-extraction.md) — Этап извлечения
- [10-activity-chembl-transformation.md](10-activity-chembl-transformation.md) — Этап трансформации
- [11-activity-chembl-validation.md](11-activity-chembl-validation.md) — Этап валидации
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Обзор пайплайна
