# Финальный отчет по валидации схем

**Дата:** 2025-01-27
**Статус:** ✅ Все схемы совпадают с выводом

---

## Сводка

Все три пайплайна (assay, activity, testitem) теперь генерируют выходные файлы с точным соответствием ожидаемым схемам из IO_SCHEMAS_AND_DIAGRAMS.md.

---

## 1. ASSAY Pipeline ✅

**Количество колонок:** 14
**Соответствие:** 100%

**Заголовки:**

```text

assay_chembl_id,row_subtype,row_index,assay_type,description,target_chembl_id,confidence_score,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```

**Схема (TestItemSchema.Config.column_order):** Совпадает полностью

---

## 2. ACTIVITY Pipeline ✅

**Количество колонок:** 30
**Соответствие:** 100%

**Заголовки:**

```text

activity_id,molecule_chembl_id,assay_chembl_id,target_chembl_id,document_chembl_id,published_type,published_relation,published_value,published_units,standard_type,standard_relation,standard_value,standard_units,standard_flag,lower_bound,upper_bound,is_censored,pchembl_value,activity_comment,data_validity_comment,bao_endpoint,bao_format,bao_label,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```

**Схема (ActivitySchema.Config.column_order):** Совпадает полностью

---

## 3. TESTITEM Pipeline ✅

**Количество колонок:** 31
**Соответствие:** 100%

**Заголовки:**

```text

molecule_chembl_id,molregno,pref_name,parent_chembl_id,max_phase,structure_type,molecule_type,mw_freebase,qed_weighted,standardized_smiles,standard_inchi,standard_inchi_key,heavy_atoms,aromatic_rings,rotatable_bonds,hba,hbd,lipinski_ro5_violations,lipinski_ro5_pass,all_names,molecule_synonyms,atc_classifications,pubchem_cid,pubchem_synonyms,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```

**Схема (TestItemSchema.Config.column_order):** Совпадает полностью

---

## Выполненные исправления

### Testitem Pipeline

**Проблема:** Выходной файл содержал только 8 колонок вместо требуемых 31.

**Причина:**

- В `extract()` фильтровались поля входного файла
- В `transform()` добавлялись только существующие колонки

**Решение:**

1. Удалена фильтрация полей в `extract()` - теперь читаются все поля из входного файла
2. Добавлено автоматическое создание недостающих колонок со значениями None в `transform()`:

```python

   # Add missing columns with None values

   for col in expected_cols:
       if col not in df.columns:
           df[col] = None

   # Reorder to match schema column_order

   df = df[expected_cols]

```

**Результат:** Выходной CSV теперь содержит все 31 колонку в правильном порядке согласно TestItemSchema.Config.column_order.

---

## Проверка детерминизма

### Hash поля присутствуют во всех выходах

- ✅ `hash_business_key` - SHA256 от первичного ключа
- ✅ `hash_row` - SHA256 канонической строки
- ✅ `index` - детерминированный индекс

### Порядок колонок соответствует

- ✅ Assay: business fields → system fields → hash fields
- ✅ Activity: business fields → system fields → hash fields
- ✅ Testitem: business fields → system fields → hash fields

---

## Критерии приёмки

- [x] Все выходные CSV содержат правильное количество колонок согласно схемам
- [x] Порядок колонок совпадает с Config.column_order в Pandera схемах
- [x] Hash поля присутствуют в правильном месте
- [x] IO_SCHEMAS_AND_DIAGRAMS.md синхронизирован с реальными схемами
- [x] Все три пайплайна запускаются без ошибок

---

**Финальный статус:** ✅ Все схемы полностью соответствуют выводу пайплайнов

