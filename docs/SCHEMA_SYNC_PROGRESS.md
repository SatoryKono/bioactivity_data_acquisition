# Schema Synchronization Progress

**Дата начала:** 2025-01-03  
**Цель:** Привести схемы всех пайплайнов в 100% соответствие с IO_SCHEMAS_AND_DIAGRAMS.md

---

## Выполнено ✅

### 1. Gap Analysis

- ✅ Создан `docs/SCHEMA_GAP_ANALYSIS.md` с детальным анализом расхождений
- ✅ Выявлено ~164 критических изменений для 6 схем
- ✅ Статистика: 10 схем → 257 полей требуемых

### 2. Модуль хеширования

- ✅ Создан `src/bioetl/core/hashing.py`
- ✅ Функции: `generate_hash_row()`, `generate_hash_business_key()`
- ✅ Детерминированная канонизация (JSON sort_keys, ISO8601, %.6f)
- ✅ Тесты: 14/14 пройдено, coverage 82%

### 3. BaseSchema

- ✅ Добавлены системные поля: `index`, `hash_row`, `hash_business_key`
- ✅ `Config.ordered = True` для enforcement column order
- ✅ Аннотации типов и descriptions

### 4. ActivitySchema

- ✅ Удалены лишние поля: `canonical_smiles`, `target_organism`, `target_tax_id`, `activity_properties`
- ✅ Добавлены published fields: `published_type`, `published_relation`, `published_value`, `published_units`
- ✅ Добавлены boundary fields: `lower_bound`, `upper_bound`, `is_censored`, `standard_flag`
- ✅ Добавлен `activity_comment`
- ✅ `Config.column_order` установлен согласно спецификации (29 полей)

### 5. ActivityPipeline

- ✅ Генерация `hash_row`, `hash_business_key`, `index`
- ✅ Сортировка по `activity_id` для детерминизма
- ✅ Column order enforcement
- ✅ Normalization для `published_units`

### 6. Config Files

- ✅ Обновлены все 5 config файлов:
  - `assay.yaml`: determinism settings + sort by [assay_chembl_id, row_subtype, row_index]
  - `activity.yaml`: determinism settings + sort by [activity_id]
  - `testitem.yaml`: determinism settings + sort by [molecule_chembl_id]
  - `target.yaml`: determinism settings + sort by [target_chembl_id]
  - `document.yaml`: determinism settings + sort by [document_chembl_id]
- ✅ Обновлена `DeterminismConfig` модель в `src/bioetl/config/models.py`:
  - Добавлены поля: `hash_algorithm`, `float_precision`, `datetime_format`
  - Значения по умолчанию: sha256, 6, iso8601

### 7. Tests

- ✅ Создан `tests/unit/test_hashing.py` (14 тестов)
- ✅ Покрытие: determinism, canonical serialization, hash length, type handling

---

## В процессе ⏳

### 8. AssaySchema ✅

- ✅ Добавить `row_subtype` (nullable=False)
- ✅ Добавить `row_index` (>=0, nullable=False)
- ✅ Добавить поля: pref_name, assay_class_id, src_id, src_name, assay_organism, assay_tax_id
- ✅ Primary Key: [assay_chembl_id, row_subtype, row_index]
- ✅ Column order enforcement
- ✅ Pipeline explode functionality с row_subtype="assay" и row_index=0
- ✅ Hash generation, sorting

### 9. TestItemSchema ✅

- ✅ Переименовать `canonical_smiles` → `standardized_smiles`
- ✅ Переименовать `molecular_weight` → `mw_freebase`
- ✅ Добавить: pref_name, max_phase, structure_type, molecule_type, qed_weighted
- ✅ Column order enforcement
- ✅ Pipeline: hash generation, field renaming, sorting

### 10. Target schemas (pending)

- [ ] 4 схемы: TargetSchema, TargetComponentSchema, ProteinClassSchema, XrefSchema
- [ ] Переименования полей
- [ ] Enrichment fields (UniProt, IUPHAR)
- [ ] Protein class restructuring

### 11. DocumentSchema ✅

- ✅ Unified multi-source schema (~70 полей с префиксами)
- ✅ Удалены старые схемы (ChEMBLDocument, PubMedDocument)
- ✅ Префиксные поля для 5 источников (ChEMBL, PubMed, Crossref, OpenAlex, Semantic Scholar)
- ✅ Error tracking и validation fields добавлены
- ✅ Column order enforcement
- ✅ Pipeline: multi-source rename, hash generation, sorting

### 12. Pipeline updates ✅

- ✅ AssayPipeline: explode functionality
- ✅ TestItemPipeline: hash generation, flatten
- ✅ DocumentPipeline: multi-source merge, hash generation
- ⏳ TargetPipeline: multi-stage enrichment (pending)

### 13. Additional tests (pending)

- [ ] `tests/unit/test_schemas.py`
- [ ] `tests/integration/test_pipelines_e2e.py`
- [ ] Обновить `tests/unit/test_pipelines.py`

### 14. Documentation (pending)

- [ ] `docs/SCHEMA_IMPLEMENTATION_GUIDE.md`
- [ ] Обновить `docs/SCHEMA_COMPLIANCE_REPORT.md`

---

## Статистика

### Compliance

| Pipeline | Текущее состояние | Целевое состояние |
|----------|-------------------|-------------------|
| BaseSchema | 100% ✅ | 100% ✅ |
| Activity | 100% ✅ | 100% ✅ |
| Assay | 100% ✅ | 100% ✅ |
| TestItem | 100% ✅ | 100% ✅ |
| Document | 100% ✅ | 100% ✅ |
| Target | 0% ❌ | 100% |
| **Overall** | **~80%** | **100%** |

### Тесты

- ✅ 14 тестов пройдено
- ✅ 82% coverage для hashing.py
- ⏳ Тесты для схем и пайплайнов (pending)

---

## Next Steps

1. ✅ Завершить AssaySchema и Pipeline (explode functionality)
2. ✅ Завершить TestItemSchema (переименования и поля)
3. ⏳ Завершить Target schemas (4 таблицы)
4. ✅ Завершить DocumentSchema (unified multi-source)
5. ⏳ Интеграционные тесты
6. ⏳ Документация (SCHEMA_IMPLEMENTATION_GUIDE)

---

## Заметки

- ✅ Hashing module работает детерминированно (14 тестов пройдено)
- ✅ Column order enforcement включен (Config.ordered=True)
- ✅ Configs готовы для детерминистической генерации (models.py обновлен)
- ✅ Activity pipeline полностью синхронизирован
- ✅ DeterminismConfig модель расширена для поддержки новых параметров
- ⏳ Остальные схемы требуют обновления (Assay, TestItem, Target, Document)

