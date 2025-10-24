"""
Базовый класс для всех Pandera схем.

Предоставляет общие настройки и поля для всех схем с единой политикой
валидации и нормализации.
"""

import pandera as pa


def add_normalization_metadata(column: pa.Column, functions: list[str]) -> pa.Column:
    """Добавляет метаданные нормализации к колонке.
    
    Args:
        column: Колонка Pandera
        functions: Список функций нормализации
        
    Returns:
        Колонка с добавленными метаданными
    """
    metadata = column.metadata or {}
    metadata["normalization_functions"] = functions
    return pa.Column(
        column.dtype,
        checks=column.checks,
        nullable=column.nullable,
        description=column.description,
        metadata=metadata
    )


class BaseSchema(pa.DataFrameModel):
    """Базовый класс для всех Pandera схем с общими настройками."""
    
    # Системные метаданные (присутствуют во всех сущностях)
    index: int = pa.Field(
        ge=0, 
        nullable=False, 
        coerce=True,
        description="Порядковый номер записи"
    )
    pipeline_version: str = pa.Field(
        nullable=False, 
        coerce=True,
        description="Версия пайплайна"
    )
    source_system: str = pa.Field(
        nullable=False, 
        coerce=True,
        description="Система-источник данных"
    )
    chembl_release: str | None = pa.Field(
        nullable=True, 
        coerce=True,
        description="Версия ChEMBL"
    )
    extracted_at: str = pa.Field(
        nullable=False, 
        coerce=True,
        description="Время извлечения данных (ISO 8601)"
    )
    hash_row: str = pa.Field(
        nullable=False, 
        coerce=True, 
        str_length=64,
        description="SHA256 хеш строки"
    )
    hash_business_key: str = pa.Field(
        nullable=False, 
        coerce=True, 
        str_length=64,
        description="SHA256 хеш бизнес-ключа"
    )
    
    class Config:
        strict = True  # Строгая типизация
        coerce = True  # Автоматическая коэрсия типов
        ordered = True  # Сохранение порядка колонок
        
    @classmethod
    def validate_dataframe(cls, df):
        """Валидация DataFrame с подробным отчётом."""
        try:
            return cls.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            print(f"Schema validation failed:\n{e.failure_cases}")
            raise


class BaseInputSchema(pa.DataFrameModel):
    """Базовый класс для входных данных."""
    
    class Config:
        strict = False  # Более мягкая валидация для входных данных
        coerce = True
        ordered = False


class BaseOutputSchema(pa.DataFrameModel):
    """Базовый класс для выходных данных."""
    
    class Config:
        strict = True  # Строгая валидация для выходных данных
        coerce = True
        ordered = True


# Общие поля для нормализации
def create_chembl_id_field(description: str, nullable: bool = False) -> pa.Column:
    """Создаёт поле для ChEMBL ID с стандартной валидацией."""
    return add_normalization_metadata(
        pa.Column(
            pa.String,
            checks=[
                pa.Check.str_matches(r'^CHEMBL\d+$', error="Invalid ChEMBL ID format"),
                pa.Check(lambda x: x.notna()) if not nullable else pa.Check(lambda x: True)
            ],
            nullable=nullable,
            description=description
        ),
        ["normalize_string_strip", "normalize_string_upper", "normalize_chembl_id"]
    )


def create_doi_field(description: str, nullable: bool = True) -> pa.Column:
    """Создаёт поле для DOI с стандартной валидацией."""
    return add_normalization_metadata(
        pa.Column(
            pa.String,
            checks=[
                pa.Check.str_matches(r'^10\.\d+/.+$', error="Invalid DOI format") if not nullable else pa.Check(lambda x: True)
            ],
            nullable=nullable,
            description=description
        ),
        ["normalize_string_strip", "normalize_string_lower", "normalize_doi"]
    )


def create_pmid_field(description: str, nullable: bool = True) -> pa.Column:
    """Создаёт поле для PMID с стандартной валидацией."""
    return add_normalization_metadata(
        pa.Column(
            pa.String,
            checks=[
                pa.Check.str_matches(r'^\d+$', error="Invalid PMID format") if not nullable else pa.Check(lambda x: True)
            ],
            nullable=nullable,
            description=description
        ),
        ["normalize_string_strip", "normalize_pmid"]
    )


def create_datetime_field(description: str, nullable: bool = False) -> pa.Column:
    """Создаёт поле для даты/времени с стандартной валидацией."""
    return add_normalization_metadata(
        pa.Column(
            pa.String,
            checks=[
                pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$', error="Invalid ISO 8601 format") if not nullable else pa.Check(lambda x: True)
            ],
            nullable=nullable,
            description=description
        ),
        ["normalize_datetime_iso8601"]
    )


def create_boolean_field(description: str, nullable: bool = True) -> pa.Column:
    """Создаёт поле для boolean с стандартной валидацией."""
    return add_normalization_metadata(
        pa.Column(
            pa.Bool,
            nullable=nullable,
            description=description
        ),
        ["normalize_boolean"]
    )


def create_float_field(description: str, nullable: bool = True, ge: float | None = None, le: float | None = None) -> pa.Column:
    """Создаёт поле для float с стандартной валидацией."""
    checks = []
    if ge is not None:
        checks.append(pa.Check.ge(ge))
    if le is not None:
        checks.append(pa.Check.le(le))
    
    return add_normalization_metadata(
        pa.Column(
            pa.Float,
            checks=checks,
            nullable=nullable,
            description=description
        ),
        ["normalize_float"]
    )


def create_string_field(description: str, nullable: bool = True, max_length: int | None = None) -> pa.Column:
    """Создаёт поле для строки с стандартной валидацией."""
    checks = []
    if max_length is not None:
        checks.append(pa.Check.str_length(max_value=max_length))
    
    return add_normalization_metadata(
        pa.Column(
            pa.String,
            checks=checks,
            nullable=nullable,
            description=description
        ),
        ["normalize_string_strip", "normalize_string_nfc", "normalize_string_whitespace"]
    )