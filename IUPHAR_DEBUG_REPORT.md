# Отчет об отладке IUPHAR обогащения

## Дата: 2025-10-21

## Резюме проблемы

Все записи в target pipeline получали одинаковые IUPHAR значения (target_id=1386, family_id=271, name=12R-LOX) вместо уникальных значений для каждого target.

## Выявленные проблемы

### 1. Неправильные названия колонок в lookup индексе

**Проблема**: В функции `_build_iuphar_lookup_index` использовались неправильные названия колонок для чтения данных из словаря IUPHAR.

**Исходный код** (строки 300-302):
```python
uniprot_id = str(row.get("uniprot_id", "")).strip()
gene_symbol = str(row.get("gene_symbol", "")).strip()
name = str(row.get("name", "")).strip()
```

**Проблема**: В словаре IUPHAR колонки называются:
- `swissprot` (не `uniprot_id`)
- `gene_name` (не `gene_symbol`)
- `target_name` (не `name`)

**Результат**: Lookup индекс показывал 0 записей в `by_uniprot`, 0 в `by_gene`, что делало маппинг по UniProt ID и gene symbol невозможным.

**Исправление**:
```python
uniprot_id = str(row.get("swissprot", "")).strip()  # В словаре IUPHAR колонка называется swissprot
gene_symbol = str(row.get("gene_name", "")).strip()  # В словаре IUPHAR колонка называется gene_name
name = str(row.get("target_name", "")).strip()  # В словаре IUPHAR колонка называется target_name
```

**Результат после исправления**:
- `by_target_id`: 3099 записей
- `by_uniprot`: 7661 записей (было 0)
- `by_gene`: 3150 записей (было 0)
- `by_name`: 3087 записей

### 2. Неправильные названия колонок в _parse_iuphar_target_from_csv

**Проблема**: В функции `_parse_iuphar_target_from_csv` использовались неправильные названия колонок.

**Исходный код** (строка 407):
```python
result["iuphar_name"] = csv_data.get("name", "")
```

**Исправление** (строка 407):
```python
result["iuphar_name"] = csv_data.get("target_name", "")  # В словаре IUPHAR колонка называется target_name
```

**Аналогично** для дополнительных полей (строки 469-472):
```python
result["gene_symbol"] = csv_data.get("gene_name", "")  # В словаре IUPHAR колонка называется gene_name
result["uniprot_id_primary"] = csv_data.get("swissprot", "")  # В словаре IUPHAR колонка называется swissprot
```

### 3. Несоответствие форматов family_id

**Обнаружено**: В словаре IUPHAR:
- В `_IUPHAR_family.csv`: family_id имеет формат с ведущими нулями (0001, 0002, 0271, и т.д.)
- В `_IUPHAR_target.csv`: family_id тоже имеет формат с ведущими нулями (0248, 0271, и т.д.)

**Исправление**: В `iuphar_local.py` уже добавлена логика нормализации в функции `_get_family_info` (строки 127-160), которая:
- Пробует найти точное совпадение
- Если не найдено, добавляет ведущие нули с помощью `zfill(4)`
- Если все еще не найдено, убирает ведущие нули с помощью `lstrip('0')`

## Структура словарей IUPHAR

### Словари идентичны в обоих проектах

Сравнение с референсным проектом `ChEMBL_data_acquisition6` показало, что словари IUPHAR полностью идентичны:
- `_IUPHAR_family.csv`: 856 записей
- `_IUPHAR_target.csv`: 24174 записи

### Колонки в словарях

**_IUPHAR_target.csv**:
- `target_id` - ID таргета в IUPHAR
- `target_name` - Название таргета
- `swissprot` - UniProt ID
- `gene_name` - Название гена
- `family_id` - ID семейства (с ведущими нулями)
- `type` - Тип таргета
- И другие колонки

**_IUPHAR_family.csv**:
- `family_id` - ID семейства (с ведущими нулями)
- `family_name` - Название семейства
- `parent_family_id` - ID родительского семейства
- `type` - Тип семейства
- И другие колонки

## Измененные файлы

1. `src/library/target/iuphar_adapter.py`:
   - Строки 300-302: Исправлены названия колонок в `_build_iuphar_lookup_index`
   - Строка 407: Исправлено название колонки в `_parse_iuphar_target_from_csv`
   - Строки 469-472: Исправлены названия колонок для дополнительных полей

2. `src/library/pipelines/target/iuphar_local.py`:
   - Строки 127-160: Добавлена логика нормализации family_id в `_get_family_info`
   - Строки 163-185: Добавлена функция `_build_family_chain`
   - Строки 224-265: Обновлена логика построения `iuphar_full_id_path` и `iuphar_full_name_path`

## Рекомендации

### Следующие шаги

1. **Запустить пайплайн заново** с исправлениями:
   ```bash
   python src\scripts\get_target_data.py --config configs\config_target_full.yaml --limit 5 --input data\input\target.csv
   ```

2. **Проверить результаты**:
   - Убедиться, что IUPHAR поля теперь содержат уникальные значения для каждого target
   - Проверить корректность заполнения `iuphar_full_id_path` и `iuphar_full_name_path`

3. **Запустить тесты**:
   ```bash
   pytest tests/test_iuphar_fields_sync.py -v
   ```

### Потенциальные проблемы

1. **UniProt IDs из ChEMBL могут отсутствовать в словаре IUPHAR**
   - Это нормально, не все targets из ChEMBL имеют соответствия в IUPHAR
   - Система должна корректно обрабатывать такие случаи через fallback механизмы

2. **Batch processing для UniProt не работает**
   - Это нормальное поведение - batch jobs могут истекать
   - Система корректно переключается на индивидуальные запросы

## Заключение

Основная проблема была в неправильных названиях колонок при чтении данных из словаря IUPHAR, что приводило к пустым lookup индексам и невозможности маппинга targets. После исправления названий колонок система должна корректно обогащать данные IUPHAR для каждого target.
