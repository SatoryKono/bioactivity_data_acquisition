## Validation (Pandera)

### Документы

- Входная проверка: `library.documents.pipeline._normalise_columns`
- Полевая валидация: `library.tools.data_validator.validate_all_fields`

### Assay

- Pandera: `library.schemas.assay_schema.AssayNormalizedSchema.validate`

### Testitem

- Схема Pandera отключена (валидаторы бизнес-правил): `library.testitem.pipeline._validate_*`

### Инструкции по запуску

- Локально: `pytest -q`
- В CI: см. `pyproject.toml [tool.pytest.ini_options]` (coverage ≥90%)
