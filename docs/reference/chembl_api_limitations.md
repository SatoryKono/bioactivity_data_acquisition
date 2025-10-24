# ChEMBL API Limitations

## Обзор

Данный документ описывает ограничения ChEMBL API v33+ и объясняет, почему некоторые поля в пайплайне assay остаются пустыми (nullable).

## Недоступные поля в ChEMBL API

### BAO (BioAssay Ontology) расширенные поля

Следующие поля, связанные с расширенной классификацией BAO, **недоступны** в ChEMBL API:

- `bao_endpoint` - BAO endpoint классификация
- `bao_assay_format` - BAO assay format
- `bao_assay_type` - BAO assay type
- `bao_assay_type_label` - BAO assay type label
- `bao_assay_type_uri` - BAO assay type URI
- `bao_assay_format_uri` - BAO assay format URI
- `bao_assay_format_label` - BAO assay format label
- `bao_endpoint_uri` - BAO endpoint URI
- `bao_endpoint_label` - BAO endpoint label

**Причина:** ChEMBL API v33+ не включает эти поля в ответ `/assay/{id}` endpoint.

**Доступные BAO поля:**
- `bao_format` - базовый BAO format (доступен)
- `bao_label` - базовый BAO label (доступен)

### Вариантные данные

Следующие поля, связанные с вариантами белков, **недоступны** в ChEMBL API:

- `variant_id` - ID варианта
- `is_variant` - флаг наличия вариантных данных
- `variant_accession` - accession варианта
- `variant_sequence_accession` - accession последовательности варианта
- `variant_sequence_mutation` - мутация в последовательности варианта
- `variant_mutations` - описание мутаций варианта
- `variant_text` - текстовое описание варианта
- `variant_sequence_id` - ID последовательности варианта
- `variant_organism` - организм варианта

**Причина:** ChEMBL API не предоставляет endpoint для вариантных данных.

**Доступное поле:**
- `variant_sequence` - последовательность варианта (доступно, но часто null)

### Дополнительные недоступные поля

- `assay_parameters_json` - параметры ассая в JSON формате
- `assay_format` - формат ассая

**Причина:** Эти поля не включены в ChEMBL API response.

## Поля, обогащаемые через /target endpoint

Следующие поля недоступны в `/assay/{id}` endpoint, но **доступны** через `/target/{id}` endpoint:

- `target_organism` - организм мишени
- `target_tax_id` - таксономический ID мишени
- `target_uniprot_accession` - UniProt accession таргета
- `target_isoform` - изоформа таргета

**Решение:** Пайплайн автоматически обогащает данные через запросы к `/target` endpoint.

## Поля, часто null в API

Следующие поля доступны в ChEMBL API, но часто содержат null значения:

- `assay_category` - категория ассая
- `assay_parameters` - параметры ассая (legacy)

**Причина:** Это нормальное поведение - не все ассаи имеют эти данные.

## Технические детали

### Endpoint mapping

| Поле | Endpoint | Статус |
|------|----------|--------|
| `target_organism` | `/target/{id}` | ✅ Доступен |
| `target_tax_id` | `/target/{id}` | ✅ Доступен |
| `target_uniprot_accession` | `/target/{id}` → `target_components[].accession` | ✅ Доступен |
| `target_isoform` | `/target/{id}` → `target_components[].component_description` | ✅ Доступен |
| `bao_endpoint` | N/A | ❌ Недоступен |
| `variant_id` | N/A | ❌ Недоступен |
| `assay_parameters_json` | N/A | ❌ Недоступен |

### Реализация обогащения

```python
# В AssayPipeline._extract_from_chembl()
# 1. Извлекаем assay данные
assay_data = chembl_client.fetch_by_assay_id(assay_id)

# 2. Собираем уникальные target_chembl_id
target_ids = set(assay_data["target_chembl_id"] for assay_data in extracted_records)

# 3. Обогащаем через /target endpoint
target_data = chembl_client.fetch_by_target_id(target_id)

# 4. Объединяем данные
enriched_data = assay_data.merge(target_data, on="target_chembl_id")
```

## Альтернативные источники

В будущем возможно рассмотреть альтернативные источники для недоступных полей:

- **BAO расширенные поля:** Прямое обращение к BAO API
- **Вариантные данные:** UniProt API, dbSNP, ClinVar
- **assay_parameters_json:** Парсинг из `assay_parameters` поля

## Мониторинг

Пайплайн логирует информацию о недоступных полях:

```
INFO: Fields unavailable in ChEMBL API: ['bao_endpoint', 'bao_assay_format', ...]
INFO: These fields are documented as unavailable in ChEMBL API v33+
```

## Заключение

Пустые значения в пайплайне assay являются результатом ограничений ChEMBL API, а не ошибок в коде. Все недоступные поля документированы и помечены как nullable в schema. Поля, доступные через `/target` endpoint, автоматически обогащаются пайплайном.
