# Финальные результаты запуска пайплайнов

**Дата:** 2025-10-28 18:08  
**Статус:** ✅ Все пайплайны успешно выполнены

---

## 1. ASSAY Pipeline ✅

**Файл:** `assay_20251028_20251028.csv`  
**Размер:** 3,310 bytes  
**Время:** 18:08:26

**Статистика:**

- Колонок: **14** ✅
- Строк данных: **10** ✅
- Hash поля: ✅ присутствуют

**Заголовки:**

```text

assay_chembl_id,row_subtype,row_index,assay_type,description,target_chembl_id,confidence_score,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```text

**Соответствие схеме:** ✅ 100% - полностью совпадает с AssaySchema.Config.column_order

---

## 2. ACTIVITY Pipeline ✅

**Файл:** `activity_20251028_20251028.csv`  
**Размер:** 1,609 bytes  
**Время:** 18:08:31

**Статистика:**

- Колонок: **30** ✅
- Строк данных: **4** ✅ (входной файл содержал 4 записи)
- Hash поля: ✅ присутствуют

**Заголовки:**

```text

activity_id,molecule_chembl_id,assay_chembl_id,target_chembl_id,document_chembl_id,published_type,published_relation,published_value,published_units,standard_type,standard_relation,standard_value,standard_units,standard_flag,lower_bound,upper_bound,is_censored,pchembl_value,activity_comment,data_validity_comment,bao_endpoint,bao_format,bao_label,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```text

**Соответствие схеме:** ✅ 100% - полностью совпадает с ActivitySchema.Config.column_order

---

## 3. TESTITEM Pipeline ✅

**Файл:** `testitem_20251028_20251028.csv`  
**Размер:** 2,595 bytes  
**Время:** 18:08:36

**Статистика:**

- Колонок: **31** ✅
- Строк данных: **10** ✅
- Hash поля: ✅ присутствуют

**Заголовки:**

```text

molecule_chembl_id,molregno,pref_name,parent_chembl_id,max_phase,structure_type,molecule_type,mw_freebase,qed_weighted,standardized_smiles,standard_inchi,standard_inchi_key,heavy_atoms,aromatic_rings,rotatable_bonds,hba,hbd,lipinski_ro5_violations,lipinski_ro5_pass,all_names,molecule_synonyms,atc_classifications,pubchem_cid,pubchem_synonyms,pipeline_version,source_system,chembl_release,extracted_at,hash_business_key,hash_row,index

```text

**Соответствие схеме:** ✅ 100% - полностью совпадает с TestItemSchema.Config.column_order

---

## Таблица соответствия

| Pipeline | Ожидаемых колонок | Фактических колонок | Соответствие | Hash поля | Строк данных |
|----------|-------------------|---------------------|--------------|-----------|--------------|
| **Assay** | 14 | 14 | ✅ 100% | ✅ | 10 |
| **Activity** | 30 | 30 | ✅ 100% | ✅ | 4 |
| **Testitem** | 31 | 31 | ✅ 100% | ✅ | 10 |

---

## Выводы

✅ **Все три пайплайна генерируют выходные файлы с точным соответствием ожидаемым схемам**

✅ **Все выходные файлы содержат обязательные hash поля:**

- `hash_business_key` - SHA256 от первичного ключа
- `hash_row` - SHA256 канонической строки
- `index` - детерминированный индекс

✅ **Порядок колонок соответствует Config.column_order в Pandera схемах**

✅ **IO_SCHEMAS_AND_DIAGRAMS.md синхронизирован с реальными выходами**

---

**Финальный статус:** ✅ Полное соответствие схем достигнуто для всех трех пайплайнов

