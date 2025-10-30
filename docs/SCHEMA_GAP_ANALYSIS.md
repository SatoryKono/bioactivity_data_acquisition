# Schema Gap Analysis

## Цель
Сравнить текущее состояние схем пайплайнов с требованиями IO_SCHEMAS_AND_DIAGRAMS.md и выявить все расхождения.

**Дата анализа:** 2025-01-03  
**Спецификация:** IO_SCHEMAS_AND_DIAGRAMS.md (lines 1-1330)

---

## 1. BaseSchema - System Fields

### Текущее состояние
**Файл:** `src/bioetl/schemas/base.py`

```python
class BaseSchema(pa.DataFrameModel):
    pipeline_version: str
    source_system: str
    chembl_release: str | None
    extracted_at: str
    Config.ordered = False
```

### Требуется по спецификации
- `pipeline_version: str` ✓
- `source_system: str` ✓
- `chembl_release: str` ✓
- `extracted_at: str` ✓
- `index: Series[int]` - детерминированный индекс (>=0, nullable=False) ✗ **ОТСУТСТВУЕТ**
- `hash_row: Series[str]` - SHA256 (regex: `^[0-9a-f]{64}$`, nullable=False) ✗ **ОТСУТСТВУЕТ**
- `hash_business_key: Series[str]` - SHA256 (regex: `^[0-9a-f]{64}$`, nullable=False) ✗ **ОТСУТСТВУЕТ**
- `Config.ordered = True` ✗ **НАСТРОЙКА**

### Чеклист изменений
- [ ] Добавить `index: Series[int]` (>=0, nullable=False)
- [ ] Добавить `hash_row: Series[str]` (regex constraint, nullable=False)
- [ ] Добавить `hash_business_key: Series[str]` (regex constraint, nullable=False)
- [ ] Изменить `Config.ordered = True`

---

## 2. AssaySchema - Full Specification

### Текущее состояние
**Файл:** `src/bioetl/schemas/assay.py`

**Текущие поля (5):**
1. `assay_chembl_id: Series[str]` ✓
2. `assay_type: Series[str]` - частично
3. `description: Series[str]` - частично
4. `target_chembl_id: Series[str]` ✓
5. `confidence_score: Series[int]` - частично

**Primary Key:** отсутствует ✗  
**Column Order:** отсутствует ✗

### Требуется по спецификации (lines 48-124)
**Primary Key:** `[assay_chembl_id, row_subtype, row_index]`

**Основные поля (обязательно):**
- `assay_chembl_id` ✓
- `row_subtype: Series[str]` - allowed: ["assay", "param", "variant"] ✗ **КЛЮЧЕВОЕ**
- `row_index: Series[int]` - >=0 ✗ **КЛЮЧЕВОЕ**
- `pref_name: Series[str]` ✗
- `hash_row: Series[str]` ✗ (из BaseSchema)
- `hash_business_key: Series[str]` ✗ (из BaseSchema)

**Остальные поля (из примечания line 136):**
- `src_id, src_name, assay_organism, assay_tax_id, assay_tissue, assay_cell_type`
- `assay_category, assay_organism, assay_type, assay_description`
- `assay_tissue, assay_cell_type, assay_subcellular_fraction`
- `target_chembl_id, target_pref_name, relationship_type`
- `relationship_type, confidence_score, activity_type`
- И другие из требований docs/requirements/05-assay-extraction.md

**Column Order:** `[assay_chembl_id, row_subtype, row_index, pref_name, hash_row, hash_business_key, chembl_release]`

### Чеклист изменений
- [ ] Добавить `row_subtype: Series[str]` (allowed: ["assay", "param", "variant"], nullable=False)
- [ ] Добавить `row_index: Series[int]` (>=0, nullable=False)
- [ ] Добавить `pref_name: Series[str]` (nullable=True)
- [ ] Добавить `assay_class_id: Series[int]` (nullable=True, FK на assay_class)
- [ ] Добавить все остальные поля из полной спецификации
- [ ] Установить Primary Key: `[assay_chembl_id, row_subtype, row_index]`
- [ ] Установить `Config.column_order` согласно спецификации
- [ ] Установить `Config.ordered = True`

### Pipeline Changes (assay.py)
- [ ] Реализовать explode nested structures (params, variants)
- [ ] Генерация `row_subtype`, `row_index` для каждой exploded строки
- [ ] Генерация `hash_row`, `hash_business_key`
- [ ] Сортировка по `[assay_chembl_id, row_subtype, row_index]`

---

## 3. ActivitySchema - Complete Fields

### Текущее состояние
**Файл:** `src/bioetl/schemas/activity.py`

**Текущие поля (16):**
1. `activity_id: Series[int]` ✓
2. `molecule_chembl_id: Series[str]` ✓
3. `assay_chembl_id: Series[str]` ✓
4. `target_chembl_id: Series[str]` ✓
5. `document_chembl_id: Series[str]` ✓
6. `standard_type: Series[str]` ✓
7. `standard_relation: Series[str]` ✓
8. `standard_value: Series[float]` ✓
9. `standard_units: Series[str]` ✓
10. `pchembl_value: Series[float]` ✓
11. `bao_endpoint: Series[str]` ✓
12. `bao_format: Series[str]` ✓
13. `bao_label: Series[str]` ✓
14. `canonical_smiles: Series[str]` ✗ **ЛИШНЕЕ** (должно быть в TestItem)
15. `target_organism: Series[str]` ✗ **ЛИШНЕЕ** (должно быть в Target)
16. `target_tax_id: Series[int]` ✗ **ЛИШНЕЕ** (должно быть в Target)
17. `data_validity_comment: Series[str]` ✓
18. `activity_properties: Series[str]` ✗ **ЛИШНЕЕ** (JSON string)

### Требуется по спецификации (lines 189-474)

**Primary Key:** `[activity_id]` ✓  
**Column Order:** `[activity_id, molecule_chembl_id, assay_chembl_id, target_chembl_id, document_chembl_id, published_type, published_relation, published_value, published_units, standard_type, standard_relation, standard_value, standard_units, standard_flag, lower_bound, upper_bound, is_censored, pchembl_value, activity_comment, data_validity_comment, bao_endpoint, bao_format, bao_label, extracted_at, hash_business_key, hash_row, index, source_system, chembl_release]`

**Отсутствующие поля:**
- `published_type: Series[str]` ✗
- `published_relation: Series[str]` ✗
- `published_value: Series[float]` (>=0) ✗
- `published_units: Series[str]` ✗
- `standard_flag: Series[int]` (0/1) ✗
- `lower_bound: Series[float]` ✗
- `upper_bound: Series[float]` ✗
- `is_censored: Series[bool]` ✗
- `activity_comment: Series[str]` ✗
- `hash_row, hash_business_key, index` ✗ (из BaseSchema)

### Чеклист изменений
- [ ] Удалить `canonical_smiles`
- [ ] Удалить `target_organism`
- [ ] Удалить `target_tax_id`
- [ ] Удалить `activity_properties`
- [ ] Добавить `published_type: Series[str]` (nullable=True)
- [ ] Добавить `published_relation: Series[str]` (nullable=True)
- [ ] Добавить `published_value: Series[float]` (>=0, nullable=True)
- [ ] Добавить `published_units: Series[str]` (nullable=True)
- [ ] Добавить `standard_flag: Series[int]` (0/1, nullable=True)
- [ ] Добавить `lower_bound: Series[float]` (nullable=True)
- [ ] Добавить `upper_bound: Series[float]` (nullable=True)
- [ ] Добавить `is_censored: Series[bool]` (nullable=True)
- [ ] Добавить `activity_comment: Series[str]` (nullable=True)
- [ ] Установить `Config.column_order` согласно спецификации
- [ ] Установить `Config.ordered = True`

### Pipeline Changes (activity.py)
- [ ] Генерация `hash_row`, `hash_business_key`, `index`
- [ ] Mapping всех `published_*` полей
- [ ] Сортировка по `activity_id`

---

## 4. TestItemSchema - Extended Fields

### Текущее состояние
**Файл:** `src/bioetl/schemas/testitem.py`

**Текущие поля (~17):**
1. `molecule_chembl_id: Series[str]` ✓
2. `molregno: Series[int]` ✓
3. `parent_chembl_id: Series[str]` ✓
4. `canonical_smiles: Series[str]` - нужно переименовать ✗
5. `standard_inchi: Series[str]` ✓
6. `standard_inchi_key: Series[str]` ✓
7. `molecular_weight: Series[float]` - нужно переименовать ✗
8. `heavy_atoms, aromatic_rings, rotatable_bonds, hba, hbd: Series[int]` ✓
9. `lipinski_ro5_violations: Series[int]` ✓
10. `lipinski_ro5_pass: Series[bool]` ✓
11. `all_names, molecule_synonyms: Series[str]` ✓
12. `atc_classifications: Series[str]` ✓
13. `pubchem_cid: Series[int]` ✓
14. `pubchem_synonyms: Series[str]` ✓

### Требуется по спецификации (lines 550-675 + примечание line 667)

**Primary Key:** `[molecule_chembl_id]` ✓  
**Примечание:** "Testitem schema содержит ~81 поле" (line 667)

**Отсутствующие ключевые поля:**
- `pref_name: Series[str]` ✗
- `max_phase: Series[int]` (>=0) ✗
- `structure_type: Series[str]` ✗
- `molecule_type: Series[str]` ✗
- `mw_freebase: Series[float]` (>=0) - вместо `molecular_weight` ✗
- `qed_weighted: Series[float]` ✗
- `standardized_smiles: Series[str]` - вместо `canonical_smiles` ✗

**Остальные поля из ~81:**
- Все физико-химические свойства, флаги, drug_* поля
- Полный список см. в docs/requirements/07a-testitem-extraction.md

**Column Order:** `[molecule_chembl_id, molregno, pref_name, parent_chembl_id, max_phase, structure_type, molecule_type, mw_freebase, qed_weighted, pubchem_cid, standardized_smiles, hash_row, hash_business_key, chembl_release]`

### Чеклист изменений
- [ ] Переименовать `canonical_smiles` → `standardized_smiles`
- [ ] Переименовать `molecular_weight` → `mw_freebase`
- [ ] Добавить `pref_name: Series[str]` (nullable=True)
- [ ] Добавить `max_phase: Series[int]` (>=0, nullable=True)
- [ ] Добавить `structure_type: Series[str]` (nullable=True)
- [ ] Добавить `molecule_type: Series[str]` (nullable=True)
- [ ] Добавить `qed_weighted: Series[float]` (nullable=True)
- [ ] Добавить все остальные поля из полной спецификации (~81 поле всего)
- [ ] Установить `Config.column_order` согласно спецификации
- [ ] Установить `Config.ordered = True`

### Pipeline Changes (testitem.py)
- [ ] Генерация `hash_row`, `hash_business_key`
- [ ] Flatten всех nested structures (molecule_properties)
- [ ] Сортировка по `molecule_chembl_id`

---

## 5. TargetSchema - Multi-Table Enrichment

### Текущее состояние
**Файл:** `src/bioetl/schemas/target.py`

**Текущие схемы (4):**
1. `TargetSchema`
2. `TargetComponentSchema`
3. `ProteinClassSchema`
4. `XrefSchema`

#### TargetSchema
**Текущие поля:**
- `target_chembl_id: Series[str]` ✓
- `pref_name: Series[str]` ✓
- `target_type: Series[str]` ✓
- `organism: Series[str]` ✓
- `taxonomy: Series[int]` - нужно переименовать ✗
- `hgnc_id: Series[str]` ✓
- `uniprot_accession: Series[str]` - нужно переименовать ✗
- `iuphar_type, iuphar_class, iuphar_subclass: Series[str]` - удалить ✗

**Требуется (lines 738-805):**
- `tax_id: Series[int]` - переименовать `taxonomy` ✗
- `uniprot_id_primary: Series[str]` - переименовать `uniprot_accession`, regex: `^[A-Z0-9]{6,10}(-[0-9]+)?$` ✗
- `uniprot_ids_all: Series[str]` - JSON array ✗
- `gene_symbol: Series[str]` ✗
- `protein_class_pred_L1: Series[str]` ✗
- `isoform_count: Series[int]` (>=0) ✗
- Удалить `iuphar_*` поля ✗

#### TargetComponentSchema
**Текущие поля:**
- `target_chembl_id: Series[str]` ✓
- `component_id: Series[int]` ✓
- `accession: Series[str]` ✓
- `gene_symbol: Series[str]` ✓
- `sequence_length: Series[int]` - нужно заменить ✗
- `is_canonical: Series[bool]` ✓

**Требуется (lines 813-843):**
- `component_type: Series[str]` ✗
- `sequence: Series[str]` - заменить `sequence_length` ✗
- `isoform_variant: Series[str]` ✗
- `data_origin: Series[str]` - allowed: ["chembl", "uniprot", "ortholog", "fallback"] ✗
- Удалить `sequence_length` ✗

#### ProteinClassSchema
**Текущие поля:**
- `target_chembl_id: Series[str]` ✓
- `protein_class_id: Series[int]` - удалить ✗
- `l1, l2, l3, l4: Series[str]` - заменить ✗

**Требуется (lines 852-871):**
- `class_level: Series[int]` (>=1) ✗
- `class_name: Series[str]` ✗
- `full_path: Series[str]` ✗

#### XrefSchema
**Текущие поля:**
- `target_chembl_id: Series[str]` ✓
- `xref_id: Series[int]` - удалить ✗
- `xref_src_db: Series[str]` - nullable=True ✗
- `xref_src_id: Series[str]` ✓

**Требуется (lines 880-897):**
- Удалить `xref_id` (int) ✗
- `xref_src_db, xref_src_id: Series[str]` (nullable=False) ✗

### Чеклист изменений

**TargetSchema:**
- [ ] Переименовать `taxonomy` → `tax_id`
- [ ] Переименовать `uniprot_accession` → `uniprot_id_primary` (regex constraint)
- [ ] Добавить `uniprot_ids_all: Series[str]`
- [ ] Добавить `gene_symbol: Series[str]`
- [ ] Добавить `protein_class_pred_L1: Series[str]`
- [ ] Добавить `isoform_count: Series[int]`
- [ ] Удалить `iuphar_type, iuphar_class, iuphar_subclass`

**TargetComponentSchema:**
- [ ] Добавить `component_type: Series[str]`
- [ ] Заменить `sequence_length` → `sequence: Series[str]`
- [ ] Добавить `isoform_variant: Series[str]`
- [ ] Добавить `data_origin: Series[str]` (allowed values)

**ProteinClassSchema:**
- [ ] Удалить `protein_class_id, l1, l2, l3, l4`
- [ ] Добавить `class_level: Series[int]` (>=1)
- [ ] Добавить `class_name: Series[str]`
- [ ] Добавить `full_path: Series[str]`

**XrefSchema:**
- [ ] Удалить `xref_id: Series[int]`
- [ ] Сделать `xref_src_db, xref_src_id` nullable=False

### Pipeline Changes (target.py)
- [ ] Генерация `hash_row`, `hash_business_key` для всех 4 таблиц
- [ ] Multi-stage enrichment (ChEMBL → UniProt → IUPHAR)
- [ ] Priority merge (chembl > uniprot > iuphar > ortholog)
- [ ] Isoform extraction
- [ ] Protein class hierarchy flatten в `class_level/class_name/full_path`

---

## 6. DocumentSchema - Multi-Source Unified

### Текущее состояние
**Файл:** `src/bioetl/schemas/document.py`

**Текущие схемы (2):**
1. `ChEMBLDocumentSchema` - 6 полей
2. `PubMedDocumentSchema` - 7 полей

### Требуется по спецификации (lines 953-1329)

**Одна unified schema:** `DocumentSchema` с ~70 полями

**Primary Key:** `[document_chembl_id]`  
**Column Order:** `[index, extracted_at, hash_business_key, hash_row, document_chembl_id, document_pubmed_id, document_classification, referenses_on_previous_experiments, original_experimental_document, document_citation, pubmed_mesh_descriptors, pubmed_mesh_qualifiers, pubmed_chemical_list, crossref_subject, chembl_pmid, openalex_pmid, pubmed_pmid, semantic_scholar_pmid, chembl_title, crossref_title, openalex_title, pubmed_article_title, semantic_scholar_title, chembl_abstract, pubmed_abstract, chembl_authors, crossref_authors, openalex_authors, pubmed_authors, semantic_scholar_authors, chembl_doi, crossref_doi, openalex_doi, pubmed_doi, semantic_scholar_doi, chembl_doc_type, crossref_doc_type, openalex_doc_type, openalex_crossref_doc_type, pubmed_doc_type, semantic_scholar_doc_type, openalex_issn, pubmed_issn, semantic_scholar_issn, chembl_journal, pubmed_journal, semantic_scholar_journal, chembl_year, openalex_year, chembl_volume, pubmed_volume, chembl_issue, pubmed_issue, pubmed_first_page, pubmed_last_page, crossref_error, openalex_error, pubmed_error, semantic_scholar_error, pubmed_year_completed, pubmed_month_completed, pubmed_day_completed, pubmed_year_revised, pubmed_month_revised, pubmed_day_revised, publication_date, document_sortorder, valid_doi, valid_journal, valid_year, valid_volume, valid_issue, invalid_doi, invalid_journal, invalid_year, invalid_volume, invalid_issue]`

**Отсутствующие поля группированные по префиксам:**

**Core Fields (5):**
- `document_pubmed_id: Series[int]` ✗
- `document_classification: Series[str]` ✗
- `referenses_on_previous_experiments: Series[str]` ✗
- `original_experimental_document: Series[str]` ✗
- `document_citation: Series[str]` ✗

**PMID (4):**
- `chembl_pmid: Series[int]` ✗
- `openalex_pmid: Series[int]` ✗
- `pubmed_pmid: Series[int]` ✗
- `semantic_scholar_pmid: Series[int]` ✗

**Title (5):**
- `chembl_title: Series[str]` - переименовать `title` ✗
- `crossref_title: Series[str]` ✗
- `openalex_title: Series[str]` ✗
- `pubmed_article_title: Series[str]` ✗
- `semantic_scholar_title: Series[str]` ✗

**Abstract (2):**
- `chembl_abstract: Series[str]` - добавить ✗
- `pubmed_abstract: Series[str]` ✗

**Authors (5):**
- `chembl_authors: Series[str]` - добавить ✗
- `crossref_authors: Series[str]` ✗
- `openalex_authors: Series[str]` ✗
- `pubmed_authors: Series[str]` - частично `authors` ✗
- `semantic_scholar_authors: Series[str]` ✗

**DOI (5):**
- `chembl_doi: Series[str]` - переименовать `doi` ✗
- `crossref_doi: Series[str]` ✗
- `openalex_doi: Series[str]` ✗
- `pubmed_doi: Series[str]` ✗
- `semantic_scholar_doi: Series[str]` ✗

**Doc Type (6):**
- `chembl_doc_type: Series[str]` ✗
- `crossref_doc_type: Series[str]` ✗
- `openalex_doc_type: Series[str]` ✗
- `openalex_crossref_doc_type: Series[str]` ✗
- `pubmed_doc_type: Series[str]` ✗
- `semantic_scholar_doc_type: Series[str]` ✗

**Journal (3):**
- `chembl_journal: Series[str]` - переименовать `journal` ✗
- `pubmed_journal: Series[str]` ✗
- `semantic_scholar_journal: Series[str]` ✗

**Year (2):**
- `chembl_year: Series[int]` (1800-2100) - переименовать `year` ✗
- `openalex_year: Series[int]` ✗

**Volume/Issue (4):**
- `chembl_volume: Series[str]` ✗
- `pubmed_volume: Series[str]` ✗
- `chembl_issue: Series[str]` ✗
- `pubmed_issue: Series[str]` ✗

**Pages (2):**
- `pubmed_first_page: Series[str]` ✗
- `pubmed_last_page: Series[str]` ✗

**ISSN (3):**
- `openalex_issn: Series[str]` ✗
- `pubmed_issn: Series[str]` ✗
- `semantic_scholar_issn: Series[str]` ✗

**PubMed Metadata (9):**
- `pubmed_mesh_descriptors: Series[str]` ✗
- `pubmed_mesh_qualifiers: Series[str]` ✗
- `pubmed_chemical_list: Series[str]` ✗
- `pubmed_year_completed, pubmed_month_completed, pubmed_day_completed: Series[int]` ✗ (3)
- `pubmed_year_revised, pubmed_month_revised, pubmed_day_revised: Series[int]` ✗ (3)

**Crossref Metadata (1):**
- `crossref_subject: Series[str]` ✗

**Error Fields (4):**
- `crossref_error, openalex_error, pubmed_error, semantic_scholar_error: Series[str]` ✗ (4)

**Validation Fields (10):**
- `valid_doi, valid_journal, valid_year, valid_volume, valid_issue: Series[bool]` ✗ (5)
- `invalid_doi, invalid_journal, invalid_year, invalid_volume, invalid_issue: Series[str]` ✗ (5)

**Derived Fields (2):**
- `publication_date: Series[str]` ✗
- `document_sortorder: Series[int]` ✗

### Чеклист изменений
- [ ] Удалить `ChEMBLDocumentSchema`
- [ ] Удалить `PubMedDocumentSchema`
- [ ] Создать `DocumentSchema` с ~70 полями
- [ ] Переименовать поля из ChEMBLDocumentSchema: `title` → `chembl_title`, `journal` → `chembl_journal`, `year` → `chembl_year`, `doi` → `chembl_doi`
- [ ] Добавить все префиксные поля для 5 источников
- [ ] Добавить Error fields для 4 источников
- [ ] Добавить Validation fields (10)
- [ ] Добавить PubMed metadata (9 полей)
- [ ] Добавить Derived fields (2)
- [ ] Установить `Config.column_order` согласно спецификации
- [ ] Установить `Config.ordered = True`

### Pipeline Changes (document.py)
- [ ] Multi-source merge с приоритетами полей
- [ ] Генерация `hash_row`, `hash_business_key`, `index`
- [ ] Validation fields logic
- [ ] Error tracking для каждого адаптера

---

## 7. Config Files - Determinism Settings

### Текущее состояние
**Файлы:** `configs/pipelines/*.yaml`

**activity.yaml:**
- `determinism.sort.by: [activity_id]` ✓ частично

### Требуется

**Добавить в каждый config:**

```yaml
determinism:
  hash_algorithm: "sha256"
  float_precision: 6
  datetime_format: "iso8601"
  sort:
    by: [<primary_key_fields>]
```

**Конкретно:**
- `assay.yaml`: `sort.by: [assay_chembl_id, row_subtype, row_index]`
- `activity.yaml`: `sort.by: [activity_id]` + добавить determinism.*
- `testitem.yaml`: добавить `determinism.*`, `sort.by: [molecule_chembl_id]`
- `target.yaml`: добавить `determinism.*`, `sort.by: [target_chembl_id]`
- `document.yaml`: добавить `determinism.*`, `sort.by: [document_chembl_id]`

### Чеклист изменений
- [ ] Добавить `determinism.hash_algorithm: "sha256"` во все configs
- [ ] Добавить `determinism.float_precision: 6`
- [ ] Добавить `determinism.datetime_format: "iso8601"`
- [ ] Добавить `determinism.sort.by` для каждого пайплайна

---

## Summary

### Статистика расхождений

| Pipeline | Схем | Текущих полей | Требуется полей | Критических изменений | Status |
|----------|------|--------------|-----------------|----------------------|--------|
| BaseSchema | 1 | 4 | 7 | +3 system fields | ⚠️ |
| Assay | 1 | 5 | ~30 | +row_subtype/row_index, explode | 🔴 |
| Activity | 1 | 18 | 29 | +published_*, -4 лишних | 🟡 |
| TestItem | 1 | ~17 | ~81 | +64 поля | 🔴 |
| Target | 4 | ~30 | ~40 | переименования, restructuring | 🟡 |
| Document | 2 | ~13 | ~70 | unified multi-source | 🔴 |
| **Total** | 10 | ~93 | ~257 | ~164 изменения | |

### Критичность изменений

**🔴 Высокая:**
- BaseSchema: system fields обязательны для всех
- Assay: explode functionality
- TestItem: 64 поля (~400% роста)
- Document: unified multi-source (~400% роста)

**🟡 Средняя:**
- Activity: добавление published_* полей
- Target: restructuring 4 схем

**🟢 Низкая:**
- Configs: добавление determinism настроек

### Next Steps

1. ✅ Gap Analysis завершен
2. ⏭️ Создать модуль хеширования
3. ⏭️ Обновить BaseSchema
4. ⏭️ Последовательно обновить все пайплайны
5. ⏭️ Обновить configs
6. ⏭️ Тесты и документация
