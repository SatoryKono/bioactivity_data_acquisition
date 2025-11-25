# TransformerABC

**Назначение:** Преобразует одну запись RecordT в нормализованную форму ValidatedRecordT.

```python
from typing import Generic, TypeVar

RecordT = TypeVar("RecordT")
ValidatedRecordT = TypeVar("ValidatedRecordT")

class TransformerABC(Generic[RecordT, ValidatedRecordT]):
    def transform(self, record: RecordT) -> ValidatedRecordT:
        raise NotImplementedError
```