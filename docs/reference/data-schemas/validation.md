# Validation

Проверки и схемы Pandera для таблиц биоактивностей и документов.

## Bioactivity схемы

- Сырьё: см. `src/library/schemas/input_schema.py` (RawBioactivitySchema)
- Нормализованные: см. `src/library/schemas/output_schema.py` (NormalizedBioactivitySchema)

Ключевые поля и поведение:

- `strict = False`, допускаются дополнительные колонки
- `coerce = True`, авто‑приведение типов

## Документы

См. `docs/document_schemas.md` — обязательные колонки, нормализация входа, QC‑метрики.

## Ошибки валидации

- Ошибки Pandera содержат имя поля и ожидаемый тип/инвариант
- Падение валидации останавливает соответствующий этап пайплайна
