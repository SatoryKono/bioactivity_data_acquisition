from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import pandas as pd
import pandera as pa
from pandera.errors import SchemaError, SchemaErrors

from bioetl.core.pipeline.validation.interfaces import (
    SchemaProviderABC,
    ValidationError,
    ValidationResult,
    ValidatorABC,
)

DEFAULT_RECORD_SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(pa.Int, nullable=False, checks=pa.Check.ge(1)),
        "name": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "value": pa.Column(pa.Float, nullable=True),
    }
)


@dataclass
class PanderaSchemaProvider(SchemaProviderABC):
    """Поставщик схемы Pandera."""

    _schema: pa.DataFrameSchema

    def schema(self) -> pa.DataFrameSchema:
        return self._schema


class PanderaValidator(ValidatorABC):
    """Валидатор, использующий схему Pandera для проверки записей."""

    def __init__(self, schema_provider: SchemaProviderABC, *, lazy: bool = True) -> None:
        self._schema_provider = schema_provider
        self._lazy = lazy

    def validate(self, data: Sequence[Mapping[str, object]]) -> ValidationResult:
        schema = self._schema_provider.schema()
        dataframe = pd.DataFrame(list(data))
        try:
            schema.validate(dataframe, lazy=self._lazy)
            return ValidationResult(is_valid=True)
        except SchemaErrors as exc:
            errors = _map_failure_cases(exc.failure_cases)
            return ValidationResult(is_valid=False, errors=tuple(errors))
        except SchemaError as exc:
            error = ValidationError(message=str(exc), context={"schema_context": exc.schema})
            return ValidationResult(is_valid=False, errors=(error,))


def _map_failure_cases(failure_cases: pd.DataFrame) -> Iterable[ValidationError]:
    for _, row in failure_cases.iterrows():
        field = row.get("column") if "column" in failure_cases.columns else None
        try:
            row_index = int(row.get("index")) if pd.notna(row.get("index")) else None
        except (TypeError, ValueError):
            row_index = None
        context = {
            "check": row.get("check"),
            "check_code": row.get("check_code"),
        }
        yield ValidationError(
            message=str(row.get("failure_case")),
            field=str(field) if field is not None else None,
            row_index=row_index,
            context={k: v for k, v in context.items() if pd.notna(v)},
        )
