# MergeStrategyABC

**Назначение:** Объединяет дубликаты с одним бизнес-ключом в итоговую запись.

```python
from typing import Generic, TypeVar, Iterable

RecordT = TypeVar("RecordT")
BusinessKeyT = TypeVar("BusinessKeyT")

class MergeStrategyABC(Generic[RecordT, BusinessKeyT]):
    def merge(self, records: Iterable[RecordT], business_key: BusinessKeyT) -> RecordT:
        raise NotImplementedError
```