# Схемы данных и валидация

Схемы описаны в Pandera для двух основных наборов:

- Сырьё (RawBioactivitySchema) — `src/library/schemas/input_schema.py`
- Нормализованные данные (NormalizedBioactivitySchema) — `src/library/schemas/output_schema.py`

## Сырьё: RawBioactivitySchema (фрагмент)

| поле | тип | nullable | источник |
|---|---|---|---|
| target_pref_name | str | yes | ChEMBL |
| standard_value | float | yes | ChEMBL (activity value) |
| standard_units | str | yes | ChEMBL (activity units) |
| canonical_smiles | str | yes | ChEMBL |
| source | str | no | клиент источника |
| retrieved_at | datetime | no | время выборки |
| activity_id | int | yes | ChEMBL |
| assay_chembl_id | str | yes | ChEMBL |
| document_chembl_id | str | yes | ChEMBL |

Поведение: `strict = False`, `coerce = True` (доп.колонки допускаются, типы приводятся).

## Нормализация: NormalizedBioactivitySchema (фрагмент)

| поле | тип | nullable | примечание |
|---|---|---|---|
| target | str | yes | нормализованное имя таргета |
| activity_value | float | yes | допускает NULL |
| activity_unit | str | yes | допускает различные ед. |
| source | str | no | источник |
| retrieved_at | datetime | no | время выборки |
| smiles | str | yes | канонический SMILES |

Поведение: `strict = False`, `coerce = True`.

## Примеры валид/инвалид

Валидный CSV (фрагмент):

```csv
target,activity_value,activity_unit,source,retrieved_at,smiles
Protein X,12.3,nM,chembl,2024-01-01T00:00:00Z,C1=CC=CC=C1
```

Невалидный CSV (тип не приводится):

```csv
target,activity_value,activity_unit,source,retrieved_at,smiles
Protein X,not_a_number,nM,chembl,2024-01-01T00:00:00Z,C1=CC=CC=C1
```

Ошибки Pandera содержат название поля и ожидаемый тип/инварианты (см. `tests/test_validation.py`).
