# ValidatorABC

**Назначение:** Выполняет проверку записи по схеме и возвращает ValidationResult.

```python
from typing import Generic, TypeVar

RecordT = TypeVar("RecordT")
SchemaT = TypeVar("SchemaT")

class ValidatorABC(Generic[RecordT, SchemaT]):
    def validate(self, record: RecordT, schema: SchemaT) -> "ValidationResult[RecordT]":
        raise NotImplementedError
```